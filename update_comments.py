#!/usr/bin/env python3
"""
Update comments for all entries in metadata.db using Reddit API (PRAW).
"""
import sqlite3
import json
import praw
import re
import os
from configparser import ConfigParser
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def parse_config_file(config_file: str) -> ConfigParser:
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
    config = parse_config_file(str(config_path))
    client_id = config.get('reddit', 'client_id', fallback=None)
    client_secret = config.get('reddit', 'client_secret', fallback=None)
    user_agent = config.get('reddit', 'user_agent', fallback='reddit_image_downloader')
    username = config.get('reddit', 'username', fallback=None)
    password = config.get('reddit', 'password', fallback=None)
    if username and password:
        reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent, username=username, password=password)
    else:
        reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)
    return reddit

def extract_post_id(permalink, url):
    # Try to extract post id from permalink
    if permalink:
        match = re.search(r'/comments/(\w+)', permalink)
        if match:
            return match.group(1)
    # Try to extract from gallery or post url
    match = re.search(r'reddit.com/(?:gallery|comments)/(\w+)', url)
    if match:
        return match.group(1)
    return None

def fetch_comments(reddit, post_id, limit=10):
    try:
        submission = reddit.submission(id=post_id)
        submission.comments.replace_more(limit=0)
        comments = []
        for c in submission.comments[:limit]:
            comments.append({
                'author': str(c.author) if c.author else '',
                'body': c.body,
                'score': c.score,
                'created_utc': c.created_utc
            })
        return comments
    except Exception as e:
        print(f"Error fetching comments for post {post_id}: {e}")
        return []

def update_comments(db_path, config_path):
    reddit = get_reddit_instance(config_path)
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT id, url, permalink, comments FROM images")
    rows = cursor.fetchall()
    total = len(rows)
    updated = 0
    logging.info(f"Starting comment update for {total} entries.")
    for idx, row in enumerate(rows, 1):
        img_id, url, permalink, old_comments_json = row
        post_id = extract_post_id(permalink, url)
        if not post_id:
            logging.warning(f"[{idx}/{total}] Could not extract post id for entry {img_id}")
            continue
        logging.info(f"[{idx}/{total}] Fetching comments for post id {post_id} (db id {img_id})")
        new_comments = fetch_comments(reddit, post_id)
        # Parse old comments
        try:
            old_comments = json.loads(old_comments_json) if old_comments_json else []
        except Exception:
            old_comments = []
        # Mark deleted comments
        merged_comments = []
        # Use (author, body) as identity for matching
        new_comment_keys = set((c['author'], c['body']) for c in new_comments)
        for old in old_comments:
            key = (old.get('author', ''), old.get('body', ''))
            if key not in new_comment_keys:
                # Mark as deleted
                deleted_comment = dict(old)
                if not deleted_comment['author'].endswith(' (deleted)'):
                    deleted_comment['author'] += ' (deleted)'
                if not deleted_comment['body'].endswith(' (deleted)'):
                    deleted_comment['body'] += ' (deleted)'
                merged_comments.append(deleted_comment)
        # Add new comments
        merged_comments.extend(new_comments)
        comments_json = json.dumps(merged_comments)
        cursor.execute("UPDATE images SET comments = ? WHERE id = ?", (comments_json, img_id))
        updated += 1
        if idx % 10 == 0:
            logging.info(f"Progress: {idx}/{total} processed.")
        conn.commit()
    conn.close()
    logging.info(f"Comments updated for {updated} entries out of {total}.")

if __name__ == "__main__":
    db_path = Path("reddit_downloads/metadata.db")
    config_path = Path("config.ini")
    update_comments(db_path, config_path)
