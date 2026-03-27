#!/usr/bin/env python3
"""
Check all 'enabled' subreddits in the scrape list and mark them 'banned'
if Reddit returns 403/Forbidden (quarantined, banned, or private).

Usage:
    python src/reddit_downloader/utils/check_banned_subreddits.py
"""
import sys
import logging
import configparser
import mysql.connector
import praw
from praw.exceptions import RedditAPIException
from prawcore.exceptions import Forbidden, NotFound, Redirect

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def load_config(path='config.ini'):
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg


def get_db(cfg):
    return mysql.connector.connect(
        host=cfg.get('mysql', 'host', fallback='localhost'),
        port=cfg.getint('mysql', 'port', fallback=3306),
        user=cfg.get('mysql', 'user', fallback='root'),
        password=cfg.get('mysql', 'password', fallback=''),
        database=cfg.get('mysql', 'database', fallback='reddit_images'),
    )


def get_reddit(cfg):
    return praw.Reddit(
        client_id=cfg.get('reddit', 'client_id'),
        client_secret=cfg.get('reddit', 'client_secret'),
        username=cfg.get('reddit', 'username', fallback=None),
        password=cfg.get('reddit', 'password', fallback=None),
        user_agent=cfg.get('reddit', 'user_agent', fallback='check_banned_subreddits/1.0'),
    )


def check_subreddit(reddit, name):
    """Return True if accessible, False if banned/private/forbidden."""
    try:
        sub = reddit.subreddit(name)
        # Accessing .id triggers the API call
        _ = sub.id
        return True
    except (Forbidden, NotFound, Redirect):
        return False
    except RedditAPIException as e:
        logger.warning(f"r/{name}: Reddit API error: {e}")
        return False
    except Exception as e:
        msg = str(e).lower()
        if '403' in msg or 'forbidden' in msg or '404' in msg or 'banned' in msg:
            return False
        logger.warning(f"r/{name}: Unexpected error: {e}")
        return True  # Don't mark banned on unknown errors


def main():
    cfg = load_config()
    reddit = get_reddit(cfg)
    conn = get_db(cfg)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id, name FROM scrape_lists WHERE type = 'subreddit' AND status != 'banned' ORDER BY name"
    )
    rows = cursor.fetchall()
    logger.info(f"Checking {len(rows)} subreddit(s) (enabled + disabled)...")

    newly_banned = []
    for item_id, name in rows:
        accessible = check_subreddit(reddit, name)
        status = "OK" if accessible else "BANNED"
        logger.info(f"  r/{name}: {status}")
        if not accessible:
            cursor.execute(
                "UPDATE scrape_lists SET status = 'banned' WHERE id = %s",
                (item_id,)
            )
            newly_banned.append(name)

    conn.commit()
    conn.close()

    if newly_banned:
        logger.info(f"\nMarked {len(newly_banned)} subreddit(s) as banned:")
        for name in newly_banned:
            logger.info(f"  - r/{name}")
    else:
        logger.info("\nAll subreddits are accessible.")


if __name__ == '__main__':
    main()
