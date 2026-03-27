#!/usr/bin/env python3
"""
Discovery watcher: scan text posts in configured watch-subreddits for
r/SubredditName mentions and auto-add them to the scrape list.

Reads watch_subreddits from [discovery] in config.ini.
Tracks already-seen post IDs in a local state file to avoid re-processing.

Usage:
    # Run once
    python src/reddit_downloader/utils/discovery_watcher.py

    # Run as a daemon (every hour)
    python src/reddit_downloader/utils/discovery_watcher.py --daemon
"""

import re
import sys
import time
import json
import logging
import argparse
import configparser
from pathlib import Path

import praw
import mysql.connector
from prawcore.exceptions import Forbidden, NotFound

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
)
logger = logging.getLogger(__name__)

# Subreddit name pattern: r/Name (3–21 chars, alphanumeric + underscore)
SUBREDDIT_RE = re.compile(r'\br/([A-Za-z0-9][A-Za-z0-9_]{2,20})\b')

STATE_FILE = Path(__file__).parent.parent.parent.parent / 'discovery_state.json'
INTERVAL_SECONDS = 3600  # 1 hour


# ── config ──────────────────────────────────────────────────────────────────

def _load_config(path='config.ini'):
    """Parse config.ini, skipping list-style sections that break ConfigParser."""
    cfg = configparser.ConfigParser()
    skip = {'scrape_list', 'user_scrape_list'}
    lines, skipping = [], False
    with open(path, encoding='utf-8') as f:
        for line in f:
            s = line.strip()
            if s in {f'[{k}]' for k in skip}:
                skipping = True
                continue
            if skipping and s.startswith('[') and s.endswith(']'):
                skipping = False
            if not skipping:
                lines.append(line)
    tmp = Path('_tmp_disc_cfg.ini')
    tmp.write_text(''.join(lines) + '\n', encoding='utf-8')
    try:
        cfg.read(str(tmp))
    finally:
        tmp.unlink(missing_ok=True)
    return cfg


def _get_db(cfg):
    return mysql.connector.connect(
        host=cfg.get('mysql', 'host', fallback='localhost'),
        port=cfg.getint('mysql', 'port', fallback=3306),
        user=cfg.get('mysql', 'user', fallback='root'),
        password=cfg.get('mysql', 'password', fallback=''),
        database=cfg.get('mysql', 'database', fallback='reddit_images'),
    )


def _get_reddit(cfg):
    kwargs = dict(
        client_id=cfg.get('reddit', 'client_id'),
        client_secret=cfg.get('reddit', 'client_secret'),
        user_agent=cfg.get('reddit', 'user_agent', fallback='discovery_watcher/1.0'),
    )
    u = cfg.get('reddit', 'username', fallback=None)
    p = cfg.get('reddit', 'password', fallback=None)
    if u and p:
        kwargs.update(username=u, password=p)
    return praw.Reddit(**kwargs)


# ── state (seen post IDs) ────────────────────────────────────────────────────

def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {}   # { subreddit_name: [post_id, ...] }


def _save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ── core logic ───────────────────────────────────────────────────────────────

def _extract_subreddits(text: str) -> set:
    """Extract all r/Name mentions from a block of text."""
    return {m.group(1) for m in SUBREDDIT_RE.finditer(text or '')}


def _add_to_scrape_list(conn, name: str, source_sub: str, post_url: str) -> bool:
    """
    Insert subreddit into scrape_lists.
    Returns True if newly added, False if already existed.
    """
    description = f'Discovered in r/{source_sub} — {post_url}'
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO scrape_lists (type, name, status, media_types, description)
            VALUES ('subreddit', %s, 'enabled', 'image,video', %s)
            """,
            (name, description),
        )
        conn.commit()
        return True
    except mysql.connector.IntegrityError:
        # Already in list — don't overwrite description
        return False
    finally:
        cursor.close()


def run_once(config_path: str = 'config.ini') -> int:
    """
    One pass: fetch posts from all watch-subreddits, find r/ mentions,
    add new ones to the scrape list.
    Returns the number of newly added subreddits.
    """
    cfg = _load_config(config_path)

    watch_raw = cfg.get('discovery', 'watch_subreddits', fallback='')
    watch_subs = [s.strip() for s in watch_raw.split(',') if s.strip()]
    if not watch_subs:
        logger.warning('No watch_subreddits configured in [discovery]')
        return 0

    fetch_limit = cfg.getint('discovery', 'fetch_limit', fallback=100)

    reddit = _get_reddit(cfg)
    conn = _get_db(cfg)
    state = _load_state()
    total_added = 0

    for watch_sub in watch_subs:
        logger.info(f'Scanning r/{watch_sub} (limit={fetch_limit})…')
        seen_ids: list = state.get(watch_sub, [])
        seen_set = set(seen_ids)
        new_seen = []

        try:
            sub = reddit.subreddit(watch_sub)
            posts = list(sub.new(limit=fetch_limit))
        except (Forbidden, NotFound) as e:
            logger.error(f'Cannot access r/{watch_sub}: {e}')
            continue
        except Exception as e:
            logger.error(f'Error fetching r/{watch_sub}: {e}')
            continue

        for post in posts:
            if not post.is_self:
                continue   # only text posts have subreddit mentions worth parsing

            if post.id in seen_set:
                continue   # already processed

            new_seen.append(post.id)
            post_url = f'https://www.reddit.com{post.permalink}'

            # Scan title + selftext
            text = f'{post.title}\n{post.selftext}'
            found = _extract_subreddits(text)
            # Remove the watch subreddit itself and very generic names
            found.discard(watch_sub)
            found.discard(watch_sub.lower())

            for name in sorted(found):
                added = _add_to_scrape_list(conn, name, watch_sub, post_url)
                if added:
                    logger.info(f'  + Added r/{name}  (from: {post_url})')
                    total_added += 1
                else:
                    logger.debug(f'  ~ r/{name} already in list')

        # Persist seen IDs (keep last 2000 to cap file size)
        seen_ids.extend(new_seen)
        state[watch_sub] = seen_ids[-2000:]

    _save_state(state)
    conn.close()
    logger.info(f'Done. {total_added} new subreddit(s) added.')
    return total_added


# ── entry point ──────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description='Auto-discover subreddits from watch-subs')
    ap.add_argument('--config', default='config.ini', help='Path to config.ini')
    ap.add_argument('--daemon', action='store_true',
                    help=f'Run continuously, sleeping {INTERVAL_SECONDS}s between runs')
    args = ap.parse_args()

    if not Path(args.config).exists():
        logger.error(f'Config not found: {args.config}')
        sys.exit(1)

    if args.daemon:
        logger.info(f'Starting daemon mode (interval={INTERVAL_SECONDS}s)')
        while True:
            try:
                run_once(args.config)
            except Exception as e:
                logger.error(f'Run failed: {e}')
            logger.info(f'Sleeping {INTERVAL_SECONDS}s…')
            time.sleep(INTERVAL_SECONDS)
    else:
        run_once(args.config)


if __name__ == '__main__':
    main()
