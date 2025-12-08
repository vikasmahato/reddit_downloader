#!/usr/bin/env python3
"""
Reddit Image Downloader

A comprehensive script to download images from Reddit, including content that requires login.
Supports various image formats and handles authentication through PRAW (Python Reddit API Wrapper).
"""

import os
import sys
import json
import argparse
import requests
from pathlib import Path
from urllib.parse import urlparse, unquote
from datetime import datetime
import praw
from configparser import ConfigParser
import mimetypes
import re
import mysql.connector
import configparser
import hashlib
from typing import List, Dict, Optional
import time
from loguru import logger
from prawcore.exceptions import Forbidden


logger.remove()
logger.add(sys.stdout, colorize=True, format="<lvl>{message}</lvl>")


class SubredditAccessError(RuntimeError):
    """Raised when a subreddit cannot be accessed due to permission issues."""

    def __init__(self, subreddit: str, status_code: int = 403, message: str = "Forbidden"):
        self.subreddit = subreddit
        self.status_code = status_code
        self.message = message
        super().__init__(f"r/{subreddit} returned status {status_code}: {message}")


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


class RedditImageDownloader:
    def __init__(self, config_file: str = "config.ini"):
        """Initialize the Reddit Image Downloader."""
        self.config = ConfigParser()
        self.config_file = config_file
        
        # Create a clean config parser that handles list sections properly
        self._parse_config_file(config_file)
        
        self.session = requests.Session()
        self.reddit = None
        self.download_folder = Path(self.config.get('general', 'download_folder', fallback='downloads'))
        
        # Create download folder if it doesn't exist
        self.download_folder.mkdir(exist_ok=True)
        
        # Setup headers for requests
        self.session.headers.update({
            'User-Agent': self.config.get('reddit', 'user_agent', 
                        fallback='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        })
        
        self._setup_reddit_auth()

    def _parse_config_file(self, config_file: str):
        """Parse config file handling list sections properly."""
        try:
            # Create temporary file without list sections
            temp_config = []
            skip_sections = ['scrape_list', 'user_scrape_list']
            skipping = False
            
            with open(config_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line_stripped = line.strip()
                    
                    # Check if we're entering a skip section
                    if line_stripped in [f'[{s}]' for s in skip_sections]:
                        skipping = True
                        continue
                    
                    # Check if we're leaving a skip section
                    if skipping and line_stripped.startswith('[') and line_stripped.endswith(']'):
                        skipping = False
                    elif skipping:
                        continue
                    
                    temp_config.append(line)
            
            # Parse the cleaned config
            temp_content = ''.join(temp_config)
            temp_file = 'temp_config.ini'
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write(temp_content)
            
            try:
                self.config.read(temp_file)
            finally:
                # Clean up temp file
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            
        except Exception as e:
            logger.warning(f"⚠️  Config parsing error: {e}")
            logger.info("   Using defaults...")
            # Fallback to minimal config
            self.config.read_string("""
                [reddit]
                client_id = 
                client_secret = 
                user_agent = reddit_image_downloader

                [general]
                download_folder = downloads
                max_images_per_subreddit = 25
                """)

    def _setup_reddit_auth(self):
        """Setup Reddit authentication using PRAW."""
        client_id = self.config.get('reddit', 'client_id', fallback=None)
        client_secret = self.config.get('reddit', 'client_secret', fallback=None)
        
        if not client_id or not client_secret:
            logger.warning("⚠️  No Reddit API credentials found. Using anonymous access only.")
            self.reddit = None
            return
        
        try:
            # Check if username and password are provided for full authentication
            username = self.config.get('reddit', 'username', fallback=None)
            password = self.config.get('reddit', 'password', fallback=None)
            
            if username and password:
                # Full authentication with user credentials
                self.reddit = praw.Reddit(
                    client_id=client_id,
                    client_secret=client_secret,
                    user_agent=self.config.get('reddit', 'user_agent', 
                                fallback='reddit_image_downloader'),
                    username=username,
                    password=password
                )
                # Test authentication
                user = self.reddit.user.me()
                logger.success(f"✓ Authenticated as: u/{user}")
                
            else:
                # Client credentials only (read-only, public content)
                self.reddit = praw.Reddit(
                    client_id=client_id,
                    client_secret=client_secret,
                    user_agent=self.config.get('reddit', 'user_agent', 
                                fallback='reddit_image_downloader')
                )
                logger.success("✓ Connected with client credentials (read-only mode)")
                
        except Exception as e:
            logger.error(f"⚠️  Reddit connection failed: {e}")
            logger.info("   You'll still be able to download directly from URLs.")
            self.reddit = None

    def download_image(self, url: str, filename: str = None, subreddit: str = "", 
                       post_data: Dict = None) -> bool:
        """Download a single image from URL with enhanced organization and metadata. Efficient for large files."""
        try:
            prev_record = self._get_image_record(url)
            response = self.session.get(url, stream=True, timeout=60)
            response.raise_for_status()
            # Determine filename if not provided
            if not filename:
                parsed_url = urlparse(url)
                filename = unquote(parsed_url.path.split('/')[-1])
                if prev_record and prev_record['filename']:
                    filename = prev_record['filename']
                else:
                    temp_path = self.download_folder / filename
                    if temp_path.exists():
                        name, ext = os.path.splitext(filename)
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f"{name}_{timestamp}{ext}"
            folder = self.download_folder
            if subreddit:
                folder = folder / self._sanitize_folder_name(subreddit)
                folder.mkdir(parents=True, exist_ok=True)
            filepath = folder / filename
            # Write image to file efficiently
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            file_hash = hashlib.md5()
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        file_hash.update(chunk)
                        downloaded += len(chunk)
                        if total_size > 0 and downloaded % (1024*1024) == 0:
                            logger.info(f"Downloaded {downloaded//(1024*1024)}MB / {total_size//(1024*1024)}MB...")
            # GIF to MP4 conversion and size reporting using ffmpeg
            if filepath.suffix.lower() == '.gif':
                import subprocess
                import os
                gif_size = os.path.getsize(filepath)
                mp4_path = filepath.with_suffix('.mp4')
                logger.info(f"Converting {filepath} to {mp4_path} using ffmpeg...")
                cmd = [
                    'ffmpeg', '-y', '-i', str(filepath),
                    '-movflags', 'faststart', '-pix_fmt', 'yuv420p', '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
                    str(mp4_path)
                ]
                try:
                    result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    mp4_size = os.path.getsize(mp4_path)
                    percent_diff = ((gif_size - mp4_size) / gif_size) * 100 if gif_size else 0
                    logger.info(f"GIF size: {gif_size/1024:.2f} KB, MP4 size: {mp4_size/1024:.2f} KB, Size reduced by: {percent_diff:.2f}%")
                    # Remove GIF file
                    os.remove(filepath)
                    logger.info(f"Deleted original GIF: {filepath}")
                    # Save ffmpeg details (stderr output)
                    ffmpeg_details = result.stderr.decode(errors='ignore')
                    # Update DB with MP4 details
                    filepath = mp4_path
                    filename = mp4_path.name
                    downloaded = mp4_size
                    # Save metadata with ffmpeg details
                    self._save_image_metadata(url, filename, subreddit, post_data, filepath, file_hash.hexdigest(), downloaded)
                    # Optionally, save ffmpeg details in a new DB column if available
                    # try:
                    #     conn = mysql.connector.connect(**mysql_config)
                    #     cursor = conn.cursor()
                    #     cursor.execute('UPDATE images SET ffmpeg_details=%s WHERE filename=%s', (ffmpeg_details, filename))
                    #     conn.commit()
                    #     conn.close()
                    # except Exception as e:
                    #     logger.error(f"Failed to save ffmpeg details: {e}")
                except Exception as conv_err:
                    logger.error(f"GIF to MP4 conversion failed: {conv_err}")
            else:
                # Save metadata for non-GIF files
                
                # Deduplication Check
                existing_image = self._get_image_by_hash(file_hash.hexdigest())
                if existing_image:
                    logger.info(f"♻️  Duplicate file found (Hash: {file_hash.hexdigest()}). Using existing file.")
                    # Delete the newly downloaded file
                    try:
                        os.remove(filepath)
                    except OSError:
                        pass
                    # Use existing file details
                    filepath = Path(existing_image['file_path'])
                    filename = existing_image['filename']
                    downloaded = existing_image['file_size']
                    
                self._save_image_metadata(url, filename, subreddit, post_data, filepath, file_hash.hexdigest(), downloaded)
            if prev_record and prev_record.get('is_deleted'):
                if '_deleted' in filename:
                    new_filename = filename.replace('_deleted', '')
                    new_filepath = filepath.parent / new_filename
                    filepath.rename(new_filepath)
                    self._update_file_path_in_db(url, str(new_filepath))
                    logger.success(f"✓ Restored: {new_filename}")
                else:
                    logger.success(f"✓ Re-downloaded: {filename}")
            else:
                logger.success(f"✓ Downloaded: {filename}")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to download {url}: {e}")
            return False
    
    def _sanitize_folder_name(self, name: str) -> str:
        """Sanitize folder names to be filesystem-safe."""
        # Replace invalid characters with underscores
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            name = name.replace(char, '_')
        
        # Remove leading/trailing dots and spaces
        name = name.strip('. ')
        
        # Limit length
        return name[:100] if name else 'unknown'

    def _get_image_record(self, url: str) -> Optional[Dict]:
        """Get image record from metadata database."""
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor(dictionary=True)
        # Join post_images and images to get full record
        query = '''
            SELECT i.*, pi.url 
            FROM post_images pi 
            JOIN images i ON pi.image_id = i.id 
            WHERE pi.url = %s
        '''
        cursor.execute(query, (url,))
        result = cursor.fetchone()
        conn.close()
        return result

    def _get_image_by_hash(self, file_hash: str) -> Optional[Dict]:
        """Get image record by file hash."""
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute('SELECT * FROM images WHERE file_hash = %s', (file_hash,))
        result = cursor.fetchone()
        conn.close()
        return result

    def _save_image_metadata(self, url: str, filename: str, subreddit: str, 
                            post_data: Dict, filepath: Path, file_hash: str, file_size: int):
        """Save image metadata to MySQL database using normalized schema."""
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        
        # 1. Insert/Update Post
        post_id = None
        if post_data:
            author = post_data.get('author', '')
            title = post_data.get('title', '')
            permalink = post_data.get('permalink', '')
            post_username = post_data.get('post_username', '')
            comments = post_data.get('comments', '')
            reddit_id = None
            if permalink:
                # Try to extract reddit_id from permalink
                match = re.search(r'/comments/([a-z0-9]+)/', permalink)
                if match:
                    reddit_id = match.group(1)

            cursor.execute('''
                INSERT INTO posts (reddit_id, title, author, subreddit, permalink, created_utc, score, post_username, comments)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    title=VALUES(title), 
                    score=VALUES(score), 
                    comments=VALUES(comments),
                    id=LAST_INSERT_ID(id)
            ''', (reddit_id, title, author, subreddit, permalink, 
                  post_data.get('created_utc', 0), post_data.get('score', 0), 
                  post_username, comments))
            post_id = cursor.lastrowid
        
        # 2. Insert/Update Image
        # Check if image exists by hash (handled in download_image, but we ensure here)
        cursor.execute('''
            INSERT INTO images (file_hash, filename, file_path, file_size, download_date, download_time)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                file_path=VALUES(file_path),
                id=LAST_INSERT_ID(id)
        ''', (file_hash, filename, str(filepath), file_size, 
              datetime.now().strftime("%Y-%m-%d"), datetime.now().strftime("%H:%M:%S")))
        image_id = cursor.lastrowid
        
        # 3. Link Post and Image
        if post_id and image_id:
            cursor.execute('''
                INSERT IGNORE INTO post_images (post_id, image_id, url)
                VALUES (%s, %s, %s)
            ''', (post_id, image_id, url))
        
        conn.commit()
        conn.close()

    def _update_file_path_in_db(self, url: str, new_filepath: str):
        """Update file path in MySQL database."""
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        # Update in images table based on join with post_images? 
        # Or just update images table directly if we know the file path?
        # But we only have URL here.
        cursor.execute('''
            UPDATE images i 
            JOIN post_images pi ON i.id = pi.image_id 
            SET i.file_path = %s 
            WHERE pi.url = %s
        ''', (new_filepath, url))
        conn.commit()
        conn.close()

    def _mark_image_as_deleted(self, url: str):
        """Mark an image as deleted in MySQL database by setting is_deleted to True."""
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE images i
            JOIN post_images pi ON i.id = pi.image_id
            SET i.is_deleted = 1 
            WHERE pi.url = %s
        ''', (url,))
        conn.commit()
        conn.close()
        logger.info(f"Marked as deleted: {url}")

    def check_deleted_images(self, subreddit: str = None) -> List[Dict]:
        """Check which previously downloaded images are now deleted."""
        deleted_images = []
        if not self.reddit:
            logger.error("❌ Reddit connection required to check for deleted images")
            return deleted_images
        
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        if subreddit:
            cursor.execute('''
                SELECT pi.url, i.filename, i.file_path 
                FROM post_images pi
                JOIN posts p ON pi.post_id = p.id
                JOIN images i ON pi.image_id = i.id
                WHERE p.subreddit = %s AND i.is_deleted = 0
            ''', (subreddit,))
        else:
            cursor.execute('''
                SELECT pi.url, i.filename, i.file_path 
                FROM post_images pi
                JOIN images i ON pi.image_id = i.id
                WHERE i.is_deleted = 0
            ''')
        images = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        conn.close()
        for img_data in images:
            img_dict = dict(zip(columns, img_data))
            url = img_dict['url']
            try:
                response = self.session.head(url, timeout=10)
                if response.status_code == 404:
                    deleted_images.append({
                        'url': url,
                        'filename': img_dict['filename'],
                        'file_path': img_dict.get('file_path')
                    })
            except Exception:
                deleted_images.append({
                    'url': url,
                    'filename': img_dict['filename'],
                    'file_path': img_dict.get('file_path')
                })
        for img in deleted_images:
            self._mark_image_as_deleted(img['url'])
            logger.info(f"📝 Marked as deleted in DB: {img['filename']}")
        return deleted_images


    def get_scrape_lists_from_db(self, list_type: str, backoff_threshold: int = 3) -> List[str]:
        """Get subreddits or users from the database, ordered by oldest scraped first.
        Applies backoff for items with too many consecutive zero results.
        
        Args:
            list_type: 'subreddit' or 'user'
            backoff_threshold: Number of consecutive zero results before skipping (default: 3)
        
        Returns:
            List of names (subreddit names or usernames), ordered by last_scraped_at ASC (NULL first)
        """
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        
        # Ensure zero_result_count column exists
        self._ensure_zero_result_count_column()
        
        # Get items with zero_result_count
        try:
            cursor.execute("""
                SELECT name, COALESCE(zero_result_count, 0) as zero_result_count 
                FROM scrape_lists
                WHERE type = %s AND enabled = TRUE
                ORDER BY last_scraped_at ASC, name ASC
            """, (list_type,))
        except mysql.connector.Error:
            # Fallback if column doesn't exist yet
            cursor.execute("""
                SELECT name FROM scrape_lists
                WHERE type = %s AND enabled = TRUE
                ORDER BY last_scraped_at ASC, name ASC
            """, (list_type,))
            results = cursor.fetchall()
            items = [row[0] for row in results]
            conn.close()
            return items
        
        results = cursor.fetchall()
        items = []
        skipped_count = 0
        
        for row in results:
            if len(row) == 2:
                name, zero_count = row
                if zero_count >= backoff_threshold:
                    skipped_count += 1
                    logger.debug(f"⏭️  Skipping {list_type} '{name}' (backoff: {zero_count} consecutive zero results)")
                    continue
            else:
                name = row[0]
            items.append(name)
        
        if skipped_count > 0:
            logger.info(f"⏭️  Skipped {skipped_count} {list_type}(s) due to backoff")
        
        conn.close()
        return items

    def update_last_scraped_at(self, list_type: str, name: str):
        """Update the last_scraped_at timestamp for a subreddit or user.
        
        Args:
            list_type: 'subreddit' or 'user'
            name: The subreddit or user name
        """
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE scrape_lists
            SET last_scraped_at = CURRENT_TIMESTAMP
            WHERE type = %s AND name = %s
        """, (list_type, name))
        
        conn.commit()
        conn.close()

    def _ensure_zero_result_count_column(self):
        """Ensure the zero_result_count column exists in scrape_lists table."""
        try:
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor()
            # Check if column exists
            cursor.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = 'scrape_lists' 
                AND COLUMN_NAME = 'zero_result_count'
            """)
            exists = cursor.fetchone()[0] > 0
            
            if not exists:
                cursor.execute("""
                    ALTER TABLE scrape_lists 
                    ADD COLUMN zero_result_count INT DEFAULT 0
                """)
                conn.commit()
            conn.close()
        except mysql.connector.Error as e:
            # Column might already exist or other error
            logger.debug(f"Column check: {e}")

    def get_zero_result_count(self, list_type: str, name: str) -> int:
        """Get the current zero result count for a subreddit or user.
        
        Args:
            list_type: 'subreddit' or 'user'
            name: The subreddit or user name
        
        Returns:
            Current zero result count (0 if not found or column doesn't exist)
        """
        try:
            self._ensure_zero_result_count_column()
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COALESCE(zero_result_count, 0) 
                FROM scrape_lists
                WHERE type = %s AND name = %s
            """, (list_type, name))
            result = cursor.fetchone()
            conn.close()
            return result[0] if result else 0
        except mysql.connector.Error:
            return 0

    def increment_zero_result_count(self, list_type: str, name: str):
        """Increment the zero result count for a subreddit or user.
        
        Args:
            list_type: 'subreddit' or 'user'
            name: The subreddit or user name
        """
        try:
            self._ensure_zero_result_count_column()
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE scrape_lists
                SET zero_result_count = COALESCE(zero_result_count, 0) + 1
                WHERE type = %s AND name = %s
            """, (list_type, name))
            conn.commit()
            conn.close()
        except mysql.connector.Error as e:
            logger.debug(f"Error incrementing zero result count: {e}")

    def reset_zero_result_count(self, list_type: str, name: str):
        """Reset the zero result count for a subreddit or user (when results are found).
        
        Args:
            list_type: 'subreddit' or 'user'
            name: The subreddit or user name
        """
        try:
            self._ensure_zero_result_count_column()
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE scrape_lists
                SET zero_result_count = 0
                WHERE type = %s AND name = %s
            """, (list_type, name))
            conn.commit()
            conn.close()
        except mysql.connector.Error as e:
            logger.debug(f"Error resetting zero result count: {e}")

    def parse_scrape_list(self, section: str) -> List[str]:
        """Parse a config section for scraping lists.
        
        DEPRECATED: This method is kept for backward compatibility.
        Use get_scrape_lists_from_db() instead.
        """
        items = []
        try:
            config_file_path = Path(self.config_file)
            
            if not config_file_path.exists():
                logger.warning(f"⚠️  Config file not found: {config_file_path}")
                return items
            
            # Read the raw config file to handle multiple values in a section
            reading_section = False
            with open(config_file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    
                    # Start reading when we hit the target section
                    if line == f'[{section}]':
                        reading_section = True
                        continue
                    
                    # Stop reading when we hit another section
                    if reading_section:
                        if line.startswith('[') and line.endswith(']'):
                            break
                        
                        # Skip empty lines and comments
                        if line and not line.startswith('#'):
                            # Remove quotes and clean up the name
                            clean_name = line.strip('"\'')
                            if clean_name:  # Only add if not empty after cleaning
                                items.append(clean_name)
        
        except Exception as e:
            logger.warning(f"⚠️  Warning: Could not parse {section} list: {e}")
        
        return items

    def scrape_from_config_list(self, scrape_type: str = "all"):
        """Scrape images from configured lists."""
        if not self.reddit:
            logger.error("❌ Reddit connection required for batch scraping")
            return
        
        # Get backoff threshold from config (default: 3)
        backoff_threshold = self.config.getint('general', 'backoff_threshold', fallback=3)
        
        total_downloads = 0
        subreddit_counts: Dict[str, int] = {}
        forbidden_subreddits: List[str] = []
        backoff_skipped: List[str] = []
        
        # Scrape subreddits
        if scrape_type in ["all", "subreddits"]:
            subreddits = self.get_scrape_lists_from_db('subreddit', backoff_threshold)
            if subreddits:
                logger.info(f"\n📂 Found {len(subreddits)} subreddits in database (backoff threshold: {backoff_threshold})")
                for subreddit in subreddits:
                    # Name is already cleaned from database
                    clean_name = subreddit.strip()
                    logger.info(f"\n🔍 Scraping r/{clean_name}...")
                    
                    limit = self.config.getint('general', 'max_images_per_subreddit', fallback=25)
                    try:
                        downloaded = self.download_from_subreddit(clean_name, limit)
                        subreddit_counts[clean_name] = downloaded
                        total_downloads += 1
                        
                        # Update last_scraped_at timestamp
                        self.update_last_scraped_at('subreddit', clean_name)
                        
                        # Track zero results for backoff
                        if downloaded == 0:
                            self.increment_zero_result_count('subreddit', clean_name)
                            zero_count = self.get_zero_result_count('subreddit', clean_name)
                            logger.warning(f"⚠️  r/{clean_name}: No images found (consecutive zero results: {zero_count})")
                        else:
                            # Reset zero result count when images are found
                            self.reset_zero_result_count('subreddit', clean_name)
                            
                    except SubredditAccessError as err:
                        forbidden_subreddits.append(clean_name)
                        logger.warning(f"🚫 Skipping r/{clean_name}: {err}")
        
        # Scrape user posts
        if scrape_type in ["all", "users"]:
            users = self.get_scrape_lists_from_db('user', backoff_threshold)
            if users:
                logger.info(f"\n👤 Found {len(users)} users in database (backoff threshold: {backoff_threshold})")
                for username in users:
                    # Name is already cleaned from database
                    clean_name = username.strip()
                    logger.info(f"\n🔍 Scraping u/{clean_name}...")
                    
                    limit = self.config.getint('general', 'max_images_per_subreddit', fallback=25)
                    try:
                        downloaded = self.download_from_user(clean_name, limit)
                        total_downloads += 1
                        
                        # Update last_scraped_at timestamp
                        self.update_last_scraped_at('user', clean_name)
                        
                        # Track zero results for backoff
                        if downloaded == 0:
                            self.increment_zero_result_count('user', clean_name)
                            zero_count = self.get_zero_result_count('user', clean_name)
                            logger.warning(f"⚠️  u/{clean_name}: No images found (consecutive zero results: {zero_count})")
                        else:
                            # Reset zero result count when images are found
                            self.reset_zero_result_count('user', clean_name)
                    except Exception as e:
                        logger.error(f"❌ Error scraping u/{clean_name}: {e}")
        
        logger.success(f"\n✅ Batch scraping complete! Scraped from {total_downloads} sources.")
        
        if subreddit_counts:
            logger.info("\n📊 Subreddit download summary:")
            for name, count in sorted(subreddit_counts.items()):
                logger.info(f"   r/{name}: {count} images downloaded")
        
        if forbidden_subreddits:
            logger.warning("\n🚫 Subreddits skipped due to 403/banned status:")
            for name in sorted(set(forbidden_subreddits)):
                logger.warning(f"   r/{name}")

    def download_from_user(self, username: str, limit: int = 25):
        """Download images from a specific user's posts.
        
        Returns:
            Number of images downloaded
        """
        if not self.reddit:
            logger.error("❌ Reddit connection required to access user posts")
            return 0
        
        try:
            # Remove u/ prefix if present
            username = username.replace('u/', '').strip()
            
            user = self.reddit.redditor(username)
            post_data_list = []
            
            logger.info(f"🔍 Fetching posts from u/{username}...")
            
            submissions = user.submissions.new(limit=limit)
            
            for submission in submissions:
                if submission.is_self:
                    continue

                gallery_urls = self._extract_gallery_urls(submission)
                has_gallery = bool(gallery_urls)
                if not has_gallery and not self._is_image_url(submission.url):
                    continue

                # Fetch comments for each post
                comments_list = []
                try:
                    submission.comments.replace_more(limit=0)
                    for c in submission.comments[:10]:
                        comments_list.append({
                            'author': str(c.author) if c.author else '',
                            'body': c.body,
                            'score': c.score,
                            'created_utc': c.created_utc
                        })
                except Exception:
                    comments_list = []

                post_entry = {
                    'title': submission.title,
                    'url': gallery_urls[0] if has_gallery else submission.url,
                    'author': str(submission.author),
                    'subreddit': str(submission.subreddit),
                    'permalink': submission.permalink,
                    'created_utc': submission.created_utc,
                    'score': submission.score,
                    'comments': json.dumps(comments_list)
                }
                if has_gallery:
                    post_entry['all_urls'] = ','.join(gallery_urls)
                post_data_list.append(post_entry)
            
            if not post_data_list:
                logger.warning(f"❌ No image posts found for u/{username}")
                return 0
            
            logger.info(f"📸 Found {len(post_data_list)} image posts from u/{username}")
            
            urls = [post['url'] for post in post_data_list]
            return self.download_from_urls(urls, username, post_data_list)
            
        except Exception as e:
            logger.error(f"❌ Error fetching posts from u/{username}: {e}")
            return 0

    def _extract_gallery_urls(self, post) -> List[str]:
        """Extract all direct image URLs from a Reddit gallery post."""
        all_urls: List[str] = []
        if hasattr(post, 'gallery_data') and post.gallery_data and hasattr(post, 'media_metadata') and post.media_metadata:
            for item in post.gallery_data.get('items', []):
                media_id = item.get('media_id')
                if not media_id:
                    continue
                meta = post.media_metadata.get(media_id)
                if not meta or meta.get('status') != 'valid':
                    continue
                source = meta.get('s') or {}
                url = source.get('u') or source.get('gif')
                if url:
                    all_urls.append(url.replace('&amp;', '&'))
        return all_urls

    def get_image_urls_from_subreddit(self, subreddit: str, limit: int = 25, 
                                    time_filter: str = 'all') -> List[Dict]:
        """Get image URLs from a subreddit, saving gallery posts as a single record with all image URLs comma-separated."""
        if not self.reddit:
            logger.error("❌ Authentication required to access subreddit content")
            return []
        try:
            sub = self.reddit.subreddit(subreddit)
            posts = sub.new(limit=limit)
            image_posts = []
            for post in posts:
                if not post.is_self:
                    # Handle gallery posts
                    all_urls = self._extract_gallery_urls(post)
                    if all_urls:
                        post_username = str(post.author) if post.author else ''
                        comments_list = []
                        try:
                            post.comments.replace_more(limit=0)
                            for c in post.comments[:10]:
                                comments_list.append({
                                    'author': str(c.author) if c.author else '',
                                    'body': c.body,
                                    'score': c.score,
                                    'created_utc': c.created_utc
                                })
                        except Exception:
                            comments_list = []
                        if all_urls:
                            image_posts.append({
                                'title': post.title,
                                'url': all_urls[0],
                                'all_urls': ','.join(all_urls),
                                'author': str(post.author),
                                'score': post.score,
                                'permalink': post.permalink,
                                'created_utc': post.created_utc,
                                'post_username': post_username,
                                'comments': json.dumps(comments_list)
                            })
                        continue  # Skip normal image handling for gallery posts
                    # Normal image handling
                    url = post.url
                    if self._is_image_url(url):
                        if self._get_image_record(url):
                            logger.warning(f"🛑 Already downloaded: {url}. Stopping further scraping for r/{subreddit}.")
                            break
                        post_username = str(post.author) if post.author else ''
                        comments_list = []
                        try:
                            post.comments.replace_more(limit=0)
                            for c in post.comments[:10]:
                                comments_list.append({
                                    'author': str(c.author) if c.author else '',
                                    'body': c.body,
                                    'score': c.score,
                                    'created_utc': c.created_utc
                                })
                        except Exception:
                            comments_list = []
                        image_posts.append({
                            'title': post.title,
                            'url': url,
                            'author': str(post.author),
                            'score': post.score,
                            'permalink': post.permalink,
                            'created_utc': post.created_utc,
                            'post_username': post_username,
                            'comments': json.dumps(comments_list)
                        })
            return image_posts
        except Forbidden as e:
            status_code = getattr(getattr(e, 'response', None), 'status_code', 403)
            raise SubredditAccessError(subreddit, status_code, "Access forbidden (possibly banned)") from e
        except Exception as e:
            logger.error(f"❌ Error accessing subreddit {subreddit}: {e}")
            return []

    def _is_image_url(self, url: str) -> bool:
        """Check if URL points to an image."""
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.webm']
        parsed_url = urlparse(url)
        
        # Check file extension
        path = parsed_url.path.lower()
        if any(path.endswith(ext) for ext in image_extensions):
            return True
        
        # Check for imgur, reddit image, i.redd.it
        image_domains = ['imgur.com', 'i.imgur.com', 'i.redd.it', 'preview.redd.it']
        return any(domain in parsed_url.netloc for domain in image_domains)

    def download_from_urls(self, urls: List[str], subreddit: str = "", url_data: List[Dict] = None):
        """Download images from a list of URLs."""
        successful = 0
        total = len(urls)
        
        logger.info(f"\n📥 Downloading {total} images...")
        
        for i, url in enumerate(urls, 1):
            logger.info(f"[{i}/{total}] {url}")
            post_data = url_data[i-1] if url_data and i <= len(url_data) else None
            
            # Check if this is a gallery post with multiple URLs
            if post_data and post_data.get('all_urls'):
                # Download all images from the gallery
                gallery_urls = [u.strip() for u in post_data['all_urls'].split(',') if u.strip()]
                logger.info(f"📸 Gallery post detected with {len(gallery_urls)} images")
                for gallery_url in gallery_urls:
                    if self.download_image(gallery_url, subreddit=subreddit, post_data=post_data):
                        successful += 1
            else:
                # Single image post
                if self.download_image(url, subreddit=subreddit, post_data=post_data):
                    successful += 1
        logger.success(f"\n✅ Download complete: {successful}/{total} images downloaded")
        return successful

    def download_from_subreddit(self, subreddit: str, limit: int = 25):
        """Download images from a subreddit."""
        logger.info(f"\n🔍 Fetching images from r/{subreddit}...")
        image_posts = self.get_image_urls_from_subreddit(subreddit, limit)
        
        if not image_posts:
            logger.warning("❌ No images found")
            return 0
        
        logger.info(f"📸 Found {len(image_posts)} image posts")
        
        # For gallery posts, we'll pass the full image_posts list to download_from_urls
        # which will handle downloading all images from each gallery
        urls = [post['url'] for post in image_posts]
        return self.download_from_urls(urls, subreddit, image_posts)

    def get_user_saved_posts(self, limit: int = 25) -> List[Dict]:
        """Get saved posts from authenticated user."""
        if not self.reddit:
            logger.error("❌ Reddit connection required to access saved posts")
            return []
        
        try:
            # Check if we have user authentication
            if not hasattr(self.reddit.user, 'me') or self.reddit.user.me() is None:
                logger.error("❌ User authentication required to access saved posts")
                logger.info("   Add username and password to config.ini for this feature")
                return []
                
            saved_posts = []
            for post in self.reddit.user.me().saved(limit=limit):
                if post.is_self:
                    continue

                gallery_urls = self._extract_gallery_urls(post)
                has_gallery = bool(gallery_urls)
                if not has_gallery and not self._is_image_url(post.url):
                    continue

                post_entry = {
                    'title': post.title,
                    'url': gallery_urls[0] if has_gallery else post.url,
                    'author': str(post.author),
                    'subreddit': str(post.subreddit),
                    'permalink': post.permalink,
                    'created_utc': post.created_utc,
                    'score': post.score
                }
                if has_gallery:
                    post_entry['all_urls'] = ','.join(gallery_urls)
                saved_posts.append(post_entry)
            
            return saved_posts
            
        except Exception as e:
            logger.error(f"❌ Error fetching saved posts: {e}")
            return []

    def resolve_imgur_url(self, url: str) -> str:
        """Resolve imgur URLs to direct image links."""
        if 'imgur.com' in url and not url.endswith(('.jpg', '.png', '.gif', '.webp')):
            # Add .jpg extension if missing
            if not url.endswith('/'):
                url += '/'
            return url + '.jpg'
        return url


def create_default_config():
    """Create a default configuration file."""
    config = ConfigParser()
    
    config['reddit'] = {
        'client_id': 'your_client_id',
        'client_secret': 'your_client_secret',
        'username': 'your_username',
        'password': 'your_password',
        'user_agent': 'reddit_image_downloader by u/your_username'
    }
    
    config['general'] = {
        'download_folder': 'downloads',
        'max_images_per_subreddit': '25'
    }
    
    with open('config.ini', 'w') as f:
        config.write(f)
    
    logger.info("📝 Created config.ini file. Please edit it with your Reddit credentials.")
    logger.info("   Get Reddit API credentials at: https://www.reddit.com/prefs/apps")


def main():
    parser = argparse.ArgumentParser(description='Download images from Reddit with organization and metadata tracking')
    parser.add_argument('--urls', nargs='+', help='Direct image URLs to download')
    parser.add_argument('--subreddit', help='Subreddit to download images from')
    parser.add_argument('--user', help='Download images from a specific user (with or without u/ prefix)')
    parser.add_argument('--limit', type=int, default=25, help='Number of images to download')
    parser.add_argument('--saved', action='store_true', help='Download from saved posts')
    parser.add_argument('--scrape-all', action='store_true', help='Scrape all subreddits and users from config')
    parser.add_argument('--scrape-subreddits', action='store_true', help='Scrape only subreddits from config')
    parser.add_argument('--scrape-users', action='store_true', help='Scrape only users from config')
    parser.add_argument('--check-deleted', help='Check for deleted images (specify subreddit or "all"')
    parser.add_argument('--list-metadata', action='store_true', help='List metadata for downloaded images')
    parser.add_argument('--config', default='config.ini', help='Config file path')
    parser.add_argument('--setup', action='store_true', help='Create default config file')
    parser.add_argument('--loop', action='store_true', help='Run in a loop every 5 minutes with --scrape-all')

    args = parser.parse_args()
    
    if args.setup:
        create_default_config()
        return
    
    if not os.path.exists(args.config):
        logger.error("❌ Config file not found. Run with --setup to create one.")
        return

    # Loop mode: run --scrape-all every 5 minutes
    if args.loop:
        while True:
            logger.info("\n⏳ Running batch scrape (--scrape-all)...")
            try:
                downloader = RedditImageDownloader(args.config)
                downloader.scrape_from_config_list("all")
            except KeyboardInterrupt:
                logger.warning("\n⏹️  Download cancelled by user")
                break
            except Exception as e:
                logger.error(f"❌ Error: {e}")
            logger.info("🕒 Sleeping for 5 minutes...")
            time.sleep(300)
        return

    try:
        downloader = RedditImageDownloader(args.config)
        
        if args.saved:
            logger.info("📖 Fetching saved posts...")
            saved_posts = downloader.get_user_saved_posts(args.limit)
            if saved_posts:
                urls = [post['url'] for post in saved_posts]
                downloader.download_from_urls(urls, "saved_posts", saved_posts)
            else:
                logger.warning("❌ No saved image posts found")
        
        elif args.scrape_all:
            logger.info("📋 Scraping all sources from config...")
            downloader.scrape_from_config_list("all")
        
        elif args.scrape_subreddits:
            logger.info("📂 Scraping subreddits from config...")
            downloader.scrape_from_config_list("subreddits")
        
        elif args.scrape_users:
            logger.info("👤 Scraping users from config...")
            downloader.scrape_from_config_list("users")
        
        elif args.user:
            username = args.user.replace('u/', '').strip()
            downloader.download_from_user(username, args.limit)
        
        elif args.subreddit:
            downloader.download_from_subreddit(args.subreddit, args.limit)
        
        elif args.urls:
            downloader.download_from_urls(args.urls)
        
        elif args.check_deleted:
            if args.check_deleted.lower() == 'all':
                deleted = downloader.check_deleted_images()
            else:
                deleted = downloader.check_deleted_images(args.check_deleted)
            
            if deleted:
                logger.info(f"\n📝 Found {len(deleted)} marked/moved deleted images")
            else:
                logger.success("\n✅ No deleted images found")
        
        elif args.list_metadata:
            pass  # TODO: Implement metadata listing
        
        else:
            parser.print_help()
            
    except KeyboardInterrupt:
        logger.warning("\n⏹️  Download cancelled by user")
    except Exception as e:
        logger.error(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
