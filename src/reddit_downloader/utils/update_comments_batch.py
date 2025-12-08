#!/usr/bin/env python3
"""
Update comments for the last N posts in the database using Reddit API (PRAW).

This script fetches the latest comments from Reddit for posts in the database
and updates them, preserving deleted comments by marking them.
"""
import json
import praw
import re
import os
import sys
import argparse
import time
from configparser import ConfigParser
from pathlib import Path
import logging
import mysql.connector
import configparser
from loguru import logger

logger.remove()
logger.add(sys.stdout, colorize=True, format="<lvl>{message}</lvl>")

# Load MySQL config
mysql_config = None
try:
    config = configparser.ConfigParser()
    config.read('config.ini')
    mysql_config = {
        'host': config.get('mysql', 'host', fallback='localhost'),
        'port': config.getint('mysql', 'port', fallback=3306),
        'user': config.get('mysql', 'user', fallback='root'),
        'password': config.get('mysql', 'password', fallback=''),
        'database': config.get('mysql', 'database', fallback='reddit_images')
    }
except Exception as e:
    logger.error(f"Error loading MySQL config: {e}")
    sys.exit(1)

def parse_config_file(config_file: str) -> ConfigParser:
    """Parse config file handling list sections properly."""
    config = ConfigParser()
    try:
        temp_config = []
        skip_sections = ['scrape_list', 'user_scrape_list']
        skipping = False
        with open(config_file, 'r', encoding='utf-8') as f:
            for line in f:
                line_stripped = line.strip()
                if line_stripped in [f'[{s}]' for s in skip_sections]:
                    skipping = True
                    continue
                if skipping and line_stripped.startswith('[') and line_stripped.endswith(']'):
                    skipping = False
                elif skipping:
                    continue
                temp_config.append(line)
        temp_config.append('\n')
        temp_content = ''.join(temp_config)
        temp_file = 'temp_config.ini'
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(temp_content)
        try:
            config.read(temp_file)
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
    except Exception as e:
        raise Exception(f"⚠️  Config parsing error: {e}")

    return config

def get_reddit_instance(config_path):
    """Initialize and return a Reddit API instance."""
    config = parse_config_file(str(config_path))
    client_id = config.get('reddit', 'client_id', fallback=None)
    client_secret = config.get('reddit', 'client_secret', fallback=None)
    user_agent = config.get('reddit', 'user_agent', fallback='reddit_image_downloader')
    username = config.get('reddit', 'username', fallback=None)
    password = config.get('reddit', 'password', fallback=None)
    
    if not client_id or not client_secret:
        raise ValueError("Reddit API credentials (client_id, client_secret) are required")
    
    if username and password:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            username=username,
            password=password
        )
        # Test authentication
        try:
            user = reddit.user.me()
            logger.success(f"✓ Authenticated as: u/{user}")
        except Exception as e:
            logger.warning(f"⚠️  Authentication test failed: {e}")
    else:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        logger.info("✓ Connected with client credentials (read-only mode)")
    
    return reddit

def extract_post_id(permalink, url=""):
    """Extract Reddit post ID from permalink or URL."""
    # Try to extract post id from permalink
    if permalink:
        match = re.search(r'/comments/([a-z0-9]+)', permalink)
        if match:
            return match.group(1)
    # Try to extract from gallery or post url
    if url:
        match = re.search(r'reddit.com/(?:gallery|comments)/([a-z0-9]+)', url)
        if match:
            return match.group(1)
    return None

def fetch_comments(reddit, post_id, limit=100):
    """Fetch comments from a Reddit post.
    
    Args:
        reddit: PRAW Reddit instance
        post_id: Reddit post ID
        limit: Maximum number of comments to fetch (default: 100)
    
    Returns:
        List of comment dictionaries
    """
    try:
        submission = reddit.submission(id=post_id)
        submission.comments.replace_more(limit=0)
        comments = []
        for c in submission.comments[:limit]:
            comments.append({
                'author': str(c.author) if c.author else '[deleted]',
                'body': c.body,
                'score': c.score,
                'created_utc': c.created_utc
            })
        return comments
    except Exception as e:
        logger.error(f"Error fetching comments for post {post_id}: {e}")
        return []

def update_comments(config_path, limit=10000, batch_size=100, delay=1.0):
    """Update comments for the last N posts in the database.
    
    Args:
        config_path: Path to config.ini file
        limit: Number of posts to update (default: 10000)
        batch_size: Number of posts to process before committing (default: 100)
        delay: Delay between API calls in seconds (default: 1.0)
    """
    logger.info(f"🚀 Starting comment update for last {limit} posts...")
    
    reddit = get_reddit_instance(config_path)
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()
    
    # Fetch posts ordered by ID descending (newest first)
    logger.info(f"📥 Fetching {limit} posts from database...")
    cursor.execute("""
        SELECT id, reddit_id, permalink, comments 
        FROM posts 
        ORDER BY id DESC 
        LIMIT %s
    """, (limit,))
    rows = cursor.fetchall()
    total = len(rows)
    
    if total == 0:
        logger.warning("❌ No posts found in database")
        conn.close()
        return
    
    logger.info(f"✓ Found {total} posts to update")
    
    updated = 0
    skipped = 0
    errors = 0
    start_time = time.time()
    
    for idx, row in enumerate(rows, 1):
        post_db_id, reddit_id, permalink, old_comments_json = row
        
        # Use reddit_id if available, otherwise try to extract from permalink
        post_id = reddit_id
        if not post_id:
            post_id = extract_post_id(permalink, "")
        
        if not post_id:
            logger.warning(f"[{idx}/{total}] ⚠️  Could not extract post id for entry {post_db_id}")
            skipped += 1
            continue
        
        try:
            # Fetch new comments from Reddit
            logger.info(f"[{idx}/{total}] 🔍 Fetching comments for post {post_id} (db id {post_db_id})")
            new_comments = fetch_comments(reddit, post_id, limit=100)
            
            # Parse old comments
            try:
                old_comments = json.loads(old_comments_json) if old_comments_json else []
            except Exception:
                old_comments = []
            
            # Merge comments: preserve deleted ones, add new ones
            merged_comments = []
            # Use (author, body) as identity for matching
            new_comment_keys = set((c.get('author', ''), c.get('body', '')) for c in new_comments)
            
            # Mark old comments that are no longer present as deleted
            for old in old_comments:
                key = (old.get('author', ''), old.get('body', ''))
                if key not in new_comment_keys:
                    # Mark as deleted if not already marked
                    deleted_comment = dict(old)
                    author = deleted_comment.get('author', '')
                    body = deleted_comment.get('body', '')
                    
                    if not author.endswith(' (deleted)') and author != '[deleted]':
                        deleted_comment['author'] = author + ' (deleted)'
                    if not body.endswith(' (deleted)'):
                        deleted_comment['body'] = body + ' (deleted)'
                    
                    merged_comments.append(deleted_comment)
            
            # Add new comments
            merged_comments.extend(new_comments)
            
            # Update database
            comments_json = json.dumps(merged_comments)
            cursor.execute("UPDATE posts SET comments = %s WHERE id = %s", (comments_json, post_db_id))
            updated += 1
            
            # Commit in batches
            if idx % batch_size == 0:
                conn.commit()
                logger.info(f"💾 Committed batch: {idx}/{total} processed")
            
            # Rate limiting delay
            if delay > 0 and idx < total:
                time.sleep(delay)
            
            # Progress update every 50 posts
            if idx % 50 == 0:
                elapsed = time.time() - start_time
                rate = idx / elapsed if elapsed > 0 else 0
                remaining = (total - idx) / rate if rate > 0 else 0
                logger.info(f"📊 Progress: {idx}/{total} ({idx*100//total}%) | "
                          f"Updated: {updated} | Skipped: {skipped} | Errors: {errors} | "
                          f"Rate: {rate:.1f}/s | ETA: {remaining:.0f}s")
        
        except Exception as e:
            logger.error(f"[{idx}/{total}] ❌ Error processing post {post_db_id}: {e}")
            errors += 1
            continue
    
    # Final commit
    conn.commit()
    conn.close()
    
    elapsed = time.time() - start_time
    logger.success(f"\n✅ Comment update complete!")
    logger.info(f"📊 Summary:")
    logger.info(f"   Total posts: {total}")
    logger.info(f"   Updated: {updated}")
    logger.info(f"   Skipped: {skipped}")
    logger.info(f"   Errors: {errors}")
    logger.info(f"   Time elapsed: {elapsed:.1f}s")
    logger.info(f"   Average rate: {total/elapsed:.2f} posts/s" if elapsed > 0 else "")

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Update comments for posts in the database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    # Update last 10000 posts (default)
  %(prog)s --limit 5000       # Update last 5000 posts
  %(prog)s --limit 20000      # Update last 20000 posts
  %(prog)s --delay 0.5        # Use 0.5s delay between requests
  %(prog)s --batch-size 50    # Commit every 50 posts
        """
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=10000,
        help='Number of posts to update (default: 10000)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Number of posts to process before committing (default: 100)'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=1.0,
        help='Delay between API calls in seconds (default: 1.0)'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config.ini',
        help='Path to config.ini file (default: config.ini)'
    )
    
    args = parser.parse_args()
    
    config_path = Path(args.config)
    if not config_path.exists():
        logger.error(f"❌ Config file not found: {config_path}")
        logger.info("   Run with --setup to create a default config file.")
        sys.exit(1)
    
    try:
        update_comments(
            config_path=config_path,
            limit=args.limit,
            batch_size=args.batch_size,
            delay=args.delay
        )
    except KeyboardInterrupt:
        logger.warning("\n⏹️  Update cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

