#!/usr/bin/env python3
"""
Sync Reddit posts: fetch latest comments, detect Reddit-deletions, update scores.

Modes
-----
  weekly   — last 10 000 posts  (run via cron weekly)
  monthly  — last 100 000 posts (run via cron monthly)
  full     — all posts          (manual / initial run)

How it works
------------
1. Batch 100 post IDs at a time into reddit.info() to detect deletions and
   get current scores — one API call per 100 posts.
2. For each non-deleted post, fetch top-level comments individually.
3. Merge new comments with old ones (preserve disappeared comments, mark
   them "(deleted)").
4. Write updated comments + score + is_deleted flag back to MySQL.

Progress output
---------------
When --progress-json is set every status update is emitted as a single JSON
line to stdout so the Flask web UI can parse it:
  {"message": "...", "progress": 42, "total": 1000, ...}
"""

import json
import re
import sys
import time
import argparse
import configparser
from pathlib import Path

from loguru import logger
import psycopg2
import praw

logger.remove()
logger.add(sys.stdout, colorize=True, format="<lvl>{message}</lvl>")

# ── constants ──────────────────────────────────────────────────────────────
INFO_BATCH   = 100   # reddit.info() accepts up to 100 fullnames
COMMENT_LIMIT = 100  # top comments to keep per post

MODE_LIMITS = {
    'weekly':  10_000,
    'monthly': 100_000,
    'full':    None,
}

# ── config helpers ─────────────────────────────────────────────────────────

def _load_mysql(config_path):
    cfg = configparser.ConfigParser()
    cfg.read(config_path)
    return {
        'host':     cfg.get('mysql', 'host',     fallback='localhost'),
        'port':     cfg.getint('mysql', 'port',  fallback=3306),
        'user':     cfg.get('mysql', 'user',     fallback='root'),
        'password': cfg.get('mysql', 'password', fallback=''),
        'database': cfg.get('mysql', 'database', fallback='reddit_images'),
    }


def _parse_reddit_config(config_path):
    """Read config.ini skipping list-style sections that break ConfigParser."""
    cfg = configparser.ConfigParser()
    skip = {'scrape_list', 'user_scrape_list'}
    lines, skipping = [], False
    with open(config_path, encoding='utf-8') as f:
        for line in f:
            s = line.strip()
            if s in {f'[{k}]' for k in skip}:
                skipping = True
                continue
            if skipping and s.startswith('[') and s.endswith(']'):
                skipping = False
            if not skipping:
                lines.append(line)
    tmp = Path('_tmp_update_cfg.ini')
    tmp.write_text(''.join(lines) + '\n', encoding='utf-8')
    try:
        cfg.read(str(tmp))
    finally:
        tmp.unlink(missing_ok=True)
    return cfg


def get_reddit(config_path):
    cfg = _parse_reddit_config(config_path)
    kwargs = dict(
        client_id     = cfg.get('reddit', 'client_id'),
        client_secret = cfg.get('reddit', 'client_secret'),
        user_agent    = cfg.get('reddit', 'user_agent', fallback='reddit_sync/1.0'),
        requestor_kwargs = {'timeout': 15},
    )
    u = cfg.get('reddit', 'username', fallback=None)
    p = cfg.get('reddit', 'password', fallback=None)
    if u and p:
        kwargs.update(username=u, password=p)
    reddit = praw.Reddit(**kwargs)
    if u and p:
        try:
            logger.success(f"Authenticated as u/{reddit.user.me()}")
        except Exception:
            logger.warning("Auth check failed, continuing")
    else:
        logger.info("Connected (read-only)")
    return reddit


# ── progress emitter ───────────────────────────────────────────────────────

def emit(enabled, progress, total, message, **extra):
    if enabled:
        print(json.dumps({'progress': progress, 'total': total,
                          'message': message, **extra}), flush=True)


# ── core batch processor ───────────────────────────────────────────────────

def _merge_comments(old_json, new_comments):
    """Merge new comment list with old, preserving comments that disappeared."""
    try:
        old = json.loads(old_json) if old_json else []
    except Exception:
        old = []

    new_keys = {(c.get('author', ''), c.get('body', '')) for c in new_comments}
    merged = []
    for oc in old:
        key = (oc.get('author', ''), oc.get('body', ''))
        if key not in new_keys:
            mc = dict(oc)
            a = mc.get('author', '')
            b = mc.get('body', '')
            if a not in ('[deleted]', '[removed]') and not a.endswith(' (deleted)'):
                mc['author'] = a + ' (deleted)'
            if b not in ('[deleted]', '[removed]') and not b.endswith(' (deleted)'):
                mc['body'] = b + ' (deleted)'
            merged.append(mc)
    merged.extend(new_comments)
    return merged


def process_batch(reddit, conn, rows, skip_comments=False):
    """
    Process one batch of up to INFO_BATCH posts.
    Returns (updated, deleted, skipped, errors).

    skip_comments=True — only update score + is_deleted (100x faster,
    no per-post API call); used by the web UI sync.
    """
    valid = [(r['id'], r['reddit_id'], r['comments'])
             for r in rows if r.get('reddit_id')]
    if not valid:
        return 0, 0, len(rows), 0

    # Step 1 — batch status/score check via reddit.info()
    id_map    = {rid: (db_id, comments) for db_id, rid, comments in valid}
    fullnames = [f't3_{rid}' for rid in id_map]

    found = {}
    try:
        for sub in reddit.info(fullnames=fullnames):
            found[sub.id] = sub
    except Exception as e:
        logger.error(f"reddit.info error: {e}")
        return 0, 0, 0, len(valid)

    cursor = conn.cursor()
    updated = deleted = errors = 0

    for rid, (db_id, old_comments_json) in id_map.items():
        if rid not in found:
            # Post completely absent from Reddit API (very rare)
            cursor.execute(
                "UPDATE posts SET is_deleted=1, removed_by_category='unknown' WHERE id=%s",
                [db_id]
            )
            deleted += 1
            continue

        sub = found[rid]
        score = getattr(sub, 'score', None)

        # reddit.info() still returns deleted/removed posts — check their attributes.
        # removed_by_category is set when a mod, Reddit, or the author removed the post.
        # author is None in PRAW when the user deleted their own post.
        removed_by = getattr(sub, 'removed_by_category', None)
        author     = getattr(sub, 'author', None)
        if removed_by is not None or author is None:
            # Use 'deleted' as category when author is gone but no explicit category
            category = removed_by or 'deleted'
            if score is not None:
                cursor.execute(
                    "UPDATE posts SET is_deleted=1, removed_by_category=%s, score=%s WHERE id=%s",
                    [category, score, db_id]
                )
            else:
                cursor.execute(
                    "UPDATE posts SET is_deleted=1, removed_by_category=%s WHERE id=%s",
                    [category, db_id]
                )
            deleted += 1
            logger.debug(f"Marked deleted: t3_{rid} "
                         f"(removed_by={removed_by}, author={author})")
            continue

        if skip_comments:
            # Fast path — score + deletion only, no per-post API call
            if score is not None:
                cursor.execute(
                    "UPDATE posts SET score=%s, is_deleted=0 WHERE id=%s",
                    [score, db_id]
                )
            else:
                cursor.execute("UPDATE posts SET is_deleted=0 WHERE id=%s", [db_id])
            updated += 1
            continue

        # Step 2 — fetch comments for this post (one API call each)
        try:
            sub.comments.replace_more(limit=0)
            new_comments = [
                {
                    'author':      str(c.author) if c.author else '[deleted]',
                    'body':        c.body,
                    'score':       c.score,
                    'created_utc': c.created_utc,
                }
                for c in sub.comments[:COMMENT_LIMIT]
            ]
        except Exception as e:
            logger.error(f"Comment fetch error for {rid}: {e}")
            if score is not None:
                cursor.execute(
                    "UPDATE posts SET score=%s, is_deleted=0 WHERE id=%s",
                    [score, db_id]
                )
            errors += 1
            continue

        merged = _merge_comments(old_comments_json, new_comments)
        if score is not None:
            cursor.execute(
                "UPDATE posts SET comments=%s, score=%s, is_deleted=0 WHERE id=%s",
                [json.dumps(merged), score, db_id]
            )
        else:
            cursor.execute(
                "UPDATE posts SET comments=%s, is_deleted=0 WHERE id=%s",
                [json.dumps(merged), db_id]
            )
        updated += 1

    conn.commit()
    cursor.close()
    return updated, deleted, len(rows) - len(valid), errors


# ── main entry point ───────────────────────────────────────────────────────

def run(config_path, mode='weekly', progress_json=False, skip_comments=False):
    limit = MODE_LIMITS.get(mode)
    mysql_cfg = _load_mysql(config_path)
    reddit    = get_reddit(config_path)

    conn   = mysql.connector.connect(**mysql_cfg)
    cursor = conn.cursor(dictionary=True)

    # Exclude posts already marked deleted and posts from banned subreddits
    base_where = """
        WHERE p.is_deleted = 0
          AND p.reddit_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM scrape_lists sl
              WHERE sl.name = p.subreddit
                AND sl.type = 'subreddit'
                AND sl.status = 'banned'
          )
    """
    if limit:
        cursor.execute(
            f"SELECT p.id, p.reddit_id, p.comments FROM posts p {base_where}"
            "ORDER BY p.created_utc DESC LIMIT %s",
            [limit]
        )
    else:
        cursor.execute(
            f"SELECT p.id, p.reddit_id, p.comments FROM posts p {base_where}"
            "ORDER BY p.created_utc DESC"
        )
    rows = cursor.fetchall()
    cursor.close()

    total = len(rows)
    if total == 0:
        logger.warning("No posts found")
        emit(progress_json, 0, 0, "No posts found")
        conn.close()
        return

    mode_label = f"{mode} (scores+deletions only)" if skip_comments else mode
    logger.info(f"Mode={mode_label}  posts={total} (skipping already-deleted and banned-subreddit posts)")
    emit(progress_json, 0, total, f"Starting {mode_label} update for {total} posts…")

    done = updated_total = deleted_total = error_total = 0
    start = time.time()

    for i in range(0, total, INFO_BATCH):
        batch = rows[i:i + INFO_BATCH]

        # Emit a "checking batch…" line immediately so the UI doesn't look stuck
        emit(progress_json, done, total,
             f"Checking posts {i+1}–{min(i+INFO_BATCH, total)} of {total}…",
             updated=updated_total, deleted=deleted_total, errors=error_total)

        u, d, _s, e = process_batch(reddit, conn, batch, skip_comments=skip_comments)
        done          += len(batch)
        updated_total += u
        deleted_total += d
        error_total   += e

        elapsed = time.time() - start
        rate    = done / elapsed if elapsed else 0
        eta     = int((total - done) / rate) if rate else 0
        msg = (f"[{done}/{total}] updated={updated_total} "
               f"deleted={deleted_total} errors={error_total} "
               f"eta={eta}s")
        logger.info(msg)
        emit(progress_json, done, total, msg,
             updated=updated_total, deleted=deleted_total, errors=error_total)

    conn.close()
    summary = (f"Done — {updated_total} updated, "
               f"{deleted_total} marked deleted, {error_total} errors.")
    logger.success(summary)
    emit(progress_json, total, total, summary,
         updated=updated_total, deleted=deleted_total, errors=error_total)


def main():
    ap = argparse.ArgumentParser(
        description='Sync Reddit post comments, scores, and deletion status',
        epilog="""
Modes:
  weekly   — last 10 000 posts (run weekly via cron)
  monthly  — last 100 000 posts (run monthly via cron)
  full     — all posts (initial / on-demand run)
        """
    )
    ap.add_argument('--mode', choices=['weekly', 'monthly', 'full'],
                    default='weekly',
                    help='Update scope (default: weekly = last 10k posts)')
    ap.add_argument('--config', default='config.ini',
                    help='Path to config.ini (default: config.ini)')
    ap.add_argument('--progress-json', action='store_true',
                    help='Emit JSON progress lines for web UI consumption')
    ap.add_argument('--skip-comments', action='store_true',
                    help='Only update score + is_deleted, skip per-post comment fetch (much faster)')
    args = ap.parse_args()

    cfg = Path(args.config)
    if not cfg.exists():
        logger.error(f"Config not found: {cfg}")
        sys.exit(1)

    try:
        run(str(cfg), mode=args.mode, progress_json=args.progress_json,
            skip_comments=args.skip_comments)
    except KeyboardInterrupt:
        logger.warning("Cancelled")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal: {e}")
        raise


if __name__ == '__main__':
    main()
