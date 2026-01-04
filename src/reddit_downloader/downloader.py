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
        self.thumbs_folder = Path(self.config.get('general', 'thumbs_folder', fallback='reddit_downloads_thumbs'))
        
        # Create download folder if it doesn't exist
        self.download_folder.mkdir(exist_ok=True)
        self.thumbs_folder.mkdir(exist_ok=True)
        
        # Setup headers for requests
        self.session.headers.update({
            'User-Agent': self.config.get('reddit', 'user_agent', 
                        fallback='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        })
        
        self._setup_reddit_auth()

    def _parse_config_file(self, config_file: str):
        """Parse config file handling list sections properly."""
        try:
            # Create temporary file without list sections and strip inline comments
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
                    
                    # Strip inline comments (everything after # that's not in quotes)
                    if '#' in line and not line.strip().startswith('#'):
                        # Split on # and take the first part, but preserve the line structure
                        parts = line.split('#', 1)
                        if len(parts) > 1:
                            # Check if # is inside quotes (simple check)
                            before_hash = parts[0]
                            quote_count = before_hash.count('"') + before_hash.count("'")
                            if quote_count % 2 == 0:  # Even number means # is not in quotes
                                line = parts[0].rstrip() + '\n'
                    
                    # Strip inline comments (everything after # that's not in quotes)
                    if '#' in line and not line.strip().startswith('#'):
                        # Split on # and take the first part
                        parts = line.split('#', 1)
                        if len(parts) > 1:
                            # Simple check: if # is not inside quotes, strip the comment
                            before_hash = parts[0]
                            # Count quotes - if even, # is outside quotes
                            quote_count = before_hash.count('"') + before_hash.count("'")
                            if quote_count % 2 == 0:  # Even means # is not in quotes
                                line = parts[0].rstrip() + '\n'
                    
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

    def _get_config_int(self, section: str, key: str, fallback: int = 0) -> int:
        """Get integer value from config, handling inline comments.
        
        Args:
            section: Config section name
            key: Config key name
            fallback: Default value if not found or invalid
        
        Returns:
            Integer value from config or fallback
        """
        try:
            value = self.config.get(section, key, fallback=str(fallback))
            # Strip any inline comments and whitespace
            if '#' in value:
                value = value.split('#')[0]
            value = value.strip()
            return int(value)
        except (ValueError, TypeError):
            return fallback

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
                    # If filename doesn't have an extension, try to get it from Content-Type
                    if not os.path.splitext(filename)[1]:
                        content_type = response.headers.get('Content-Type', '')
                        if content_type:
                            ext = mimetypes.guess_extension(content_type.split(';')[0].strip())
                            if ext:
                                filename = filename + ext
                        # If still no extension and it's a video URL, default to .mp4
                        if not os.path.splitext(filename)[1] and self._is_video_url(url):
                            filename = filename + '.mp4'
                    
                    # Check if filename is generic (like DASH_1080.mp4, DASH_720.mp4, etc.)
                    # or if it's too short/doesn't contain unique identifiers
                    name, ext = os.path.splitext(filename)
                    is_generic = False
                    
                    # Check for common generic Reddit video filenames
                    generic_patterns = ['DASH_', 'DASHPlaylist', 'audio', 'video']
                    if any(pattern in name for pattern in generic_patterns):
                        is_generic = True
                    
                    # If filename is generic, create a unique one using post data or URL
                    if is_generic or len(name) < 5:
                        unique_id = None
                        
                        # Try to get post ID from post_data
                        if post_data:
                            permalink = post_data.get('permalink', '')
                            if permalink:
                                # Extract post ID from permalink (e.g., /r/subreddit/comments/abc123/title/)
                                match = re.search(r'/comments/([a-z0-9]+)/', permalink)
                                if match:
                                    unique_id = match.group(1)
                        
                        # If no post ID, try to extract from URL (for v.redd.it videos)
                        if not unique_id:
                            # Extract video ID from v.redd.it URLs (e.g., v.redd.it/ni0u4jnovm8c1/DASH_1080.mp4)
                            path_parts = parsed_url.path.strip('/').split('/')
                            if len(path_parts) >= 2 and 'v.redd.it' in parsed_url.netloc:
                                unique_id = path_parts[0]  # The video ID part
                        
                        # If still no unique ID, use a hash of the URL
                        if not unique_id:
                            unique_id = hashlib.md5(url.encode()).hexdigest()[:12]
                        
                        # Create unique filename: original_name_uniqueid.ext
                        filename = f"{name}_{unique_id}{ext}"
                    
                    # Check if file already exists in the target folder
                    temp_folder = self.download_folder
                    if subreddit:
                        temp_folder = temp_folder / self._sanitize_folder_name(subreddit)
                    temp_path = temp_folder / filename
                    if temp_path.exists():
                        # File exists, add timestamp to make it unique
                        name, ext = os.path.splitext(filename)
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f"{name}_{timestamp}{ext}"
            
            # Determine final folder and filepath
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
                    # Use existing file details
                    existing_filepath = Path(existing_image['file_path'])
                    # Resolve the path - handle both absolute and relative paths
                    if not existing_filepath.is_absolute():
                        # Try relative to download folder
                        existing_filepath = self.download_folder / existing_filepath
                    
                    # Check if existing file actually exists
                    if existing_filepath.exists():
                        logger.info(f"♻️  Duplicate file found (Hash: {file_hash.hexdigest()}). Using existing file.")
                        # Delete the newly downloaded file
                        try:
                            os.remove(filepath)
                        except OSError:
                            pass
                        # Use existing file details
                        filepath = existing_filepath
                        filename = existing_image['filename']
                        downloaded = existing_image['file_size']
                    else:
                        # Existing file doesn't exist, but hash matches - file was moved/deleted
                        # Keep the new file and update the database with the new path
                        logger.warning(f"⚠️  Duplicate hash found but existing file missing at {existing_image['file_path']}")
                        logger.info(f"📥 Re-saving file to database with new path: {filepath}")
                        # filepath, filename, downloaded already set above - keep the newly downloaded file
                        # Verify the new file exists before proceeding
                        if not filepath.exists():
                            logger.error(f"✗ Newly downloaded file also missing at {filepath}")
                            return False
                    
                self._save_image_metadata(url, filename, subreddit, post_data, filepath, file_hash.hexdigest(), downloaded)
            
            # Generate thumbnail only if file exists (filepath should point to the actual file location)
            if filepath.exists():
                try:
                    thumb_path = self._generate_thumbnail(filepath, subreddit)
                    if thumb_path:
                        logger.debug(f"✓ Thumbnail generated: {thumb_path.name}")
                except Exception as e:
                    logger.warning(f"⚠️  Thumbnail generation failed for {filepath}: {e}")
            else:
                logger.error(f"✗ Cannot generate thumbnail: file does not exist at {filepath}")
                # This shouldn't happen if logic is correct, but log as error for debugging
            
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

    def _is_post_downloaded(self, permalink: str) -> bool:
        """Check if a post is already downloaded by checking its permalink in the permalinks table."""
        if not permalink:
            return False
        try:
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor()
            cursor.execute('SELECT permalink FROM permalinks WHERE permalink = %s', (permalink,))
            result = cursor.fetchone()
            conn.close()
            return result is not None
        except Exception as e:
            logger.debug(f"Error checking if post is downloaded: {e}")
            return False

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

            # Convert created_utc from Unix timestamp to datetime if needed
            created_utc = post_data.get('created_utc', 0)
            if created_utc and isinstance(created_utc, (int, float)):
                # Convert Unix timestamp to datetime
                created_utc_dt = datetime.fromtimestamp(created_utc)
            elif created_utc:
                # Already a datetime or other format
                created_utc_dt = created_utc
            else:
                # Default to current time if not provided
                created_utc_dt = datetime.now()
            
            cursor.execute('''
                INSERT INTO posts (reddit_id, title, author, subreddit, permalink, created_utc, score, post_username, comments)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    title=VALUES(title), 
                    score=VALUES(score), 
                    comments=VALUES(comments)
            ''', (reddit_id, title, author, subreddit, permalink, 
                  created_utc_dt, post_data.get('score', 0), 
                  post_username, comments))
            
            # Save permalink to permalinks table to prevent redownloads of deleted posts
            if permalink:
                cursor.execute('''
                    INSERT IGNORE INTO permalinks (permalink)
                    VALUES (%s)
                ''', (permalink,))
            
            # Get the post_id - either from lastrowid (if inserted) or by querying (if updated)
            post_id = cursor.lastrowid
            if not post_id or post_id == 0:
                # Post already existed, fetch its ID
                if permalink:
                    cursor.execute('SELECT id FROM posts WHERE permalink = %s', (permalink,))
                elif reddit_id:
                    cursor.execute('SELECT id FROM posts WHERE reddit_id = %s', (reddit_id,))
                else:
                    cursor.execute('SELECT id FROM posts WHERE subreddit = %s AND title = %s AND author = %s LIMIT 1', 
                                  (subreddit, title, author))
                result = cursor.fetchone()
                if result:
                    post_id = result[0]
        
        # 2. Insert/Update Image
        # Check if image exists by hash (handled in download_image, but we ensure here)
        cursor.execute('''
            INSERT INTO images (file_hash, filename, file_path, file_size, download_date, download_time)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                file_path=VALUES(file_path)
        ''', (file_hash, filename, str(filepath), file_size, 
              datetime.now().strftime("%Y-%m-%d"), datetime.now().strftime("%H:%M:%S")))
        
        # Get the image_id - either from lastrowid (if inserted) or by querying (if updated)
        image_id = cursor.lastrowid
        if not image_id or image_id == 0:
            # Image already existed, fetch its ID by hash
            cursor.execute('SELECT id FROM images WHERE file_hash = %s', (file_hash,))
            result = cursor.fetchone()
            if result:
                image_id = result[0]
        
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
            #if len(row) == 2:
            #    name, zero_count = row
            #    if zero_count >= backoff_threshold:
            #        skipped_count += 1
            #        logger.debug(f"⏭️  Skipping {list_type} '{name}' (backoff: {zero_count} consecutive zero results)")
            #        continue
            #else:
            name = row[0]
            items.append(name)
        
        if skipped_count > 0:
            logger.info(f"⏭️  Skipped {skipped_count} {list_type}(s) due to backoff")
        
        conn.close()
        return items

    def update_last_scraped_at(self, list_type: str, name: str):
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE scrape_lists
            SET last_scraped_at = CURRENT_TIMESTAMP
            WHERE type = %s AND name = %s
        """, (list_type, name))
        
        conn.commit()
        conn.close()


    def get_zero_result_count(self, list_type: str, name: str) -> int:
        try:
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
        try:
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
        try:
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

    def scrape_from_config_list(self, scrape_type: str = "all"):
        """Scrape images from configured lists."""
        if not self.reddit:
            logger.error("❌ Reddit connection required for batch scraping")
            return
        
        # Get backoff threshold from config (default: 3)
        backoff_threshold = self._get_config_int('general', 'backoff_threshold', fallback=3)
        
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

                # Check if post is already downloaded by checking the permalink of the post in the database
                if self._is_post_downloaded(submission.permalink):
                    logger.debug(f"⏭️  Post already downloaded (permalink: {submission.permalink}), skipping...")
                    continue

                gallery_urls = self._extract_gallery_urls(submission)
                has_gallery = bool(gallery_urls)
                
                # Check for video posts
                video_url = self._extract_video_url(submission)
                has_video = bool(video_url)
                
                # Skip if not gallery, not image, and not video
                if not has_gallery and not self._is_image_url(submission.url) and not has_video:
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

                # Determine the URL to use
                if has_gallery:
                    post_url = gallery_urls[0]
                elif has_video:
                    post_url = video_url
                else:
                    post_url = submission.url

                post_entry = {
                    'title': submission.title,
                    'url': post_url,
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
                    # Check if post is already downloaded by checking the permalink of the post in the database
                    if self._is_post_downloaded(post.permalink):
                        logger.debug(f"⏭️  Post already downloaded (permalink: {post.permalink}), skipping...")
                        continue
                    
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
                        continue  # Skip normal image/video handling for gallery posts
                    
                    # Handle video posts
                    video_url = self._extract_video_url(post)
                    if video_url:
                        if self._get_image_record(video_url):
                            logger.warning(f"🛑 Already downloaded: {video_url}. Stopping further scraping for r/{subreddit}.")
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
                            'url': video_url,
                            'author': str(post.author),
                            'score': post.score,
                            'permalink': post.permalink,
                            'created_utc': post.created_utc,
                            'post_username': post_username,
                            'comments': json.dumps(comments_list)
                        })
                        continue  # Skip image handling for video posts
                    
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
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']
        parsed_url = urlparse(url)
        
        # Check file extension
        path = parsed_url.path.lower()
        if any(path.endswith(ext) for ext in image_extensions):
            return True
        
        # Check for imgur, reddit image, i.redd.it
        image_domains = ['imgur.com', 'i.imgur.com', 'i.redd.it', 'preview.redd.it']
        return any(domain in parsed_url.netloc for domain in image_domains)

    def _is_video_url(self, url: str) -> bool:
        """Check if URL points to a video."""
        video_extensions = ['.mp4', '.webm', '.mov', '.avi', '.mkv', '.flv', '.wmv']
        parsed_url = urlparse(url)
        
        # Check file extension
        path = parsed_url.path.lower()
        if any(path.endswith(ext) for ext in video_extensions):
            return True
        
        # Check for Reddit video domains
        video_domains = ['v.redd.it', 'reddit.com/video']
        netloc = parsed_url.netloc.lower()
        path_lower = parsed_url.path.lower()
        if any(domain in netloc for domain in video_domains):
            return True
        if '/video/' in path_lower:
            return True
        
        return False

    def _extract_video_url(self, post) -> Optional[str]:
        """Extract video URL from a Reddit post."""
        # Check for Reddit video in media.reddit_video (most common case)
        try:
            if hasattr(post, 'media') and post.media:
                # media can be a dict or an object
                if isinstance(post.media, dict):
                    reddit_video = post.media.get('reddit_video', {})
                    if isinstance(reddit_video, dict):
                        fallback_url = reddit_video.get('fallback_url')
                        if fallback_url:
                            return fallback_url
                else:
                    # media is an object
                    if hasattr(post.media, 'reddit_video'):
                        reddit_video = post.media.reddit_video
                        if isinstance(reddit_video, dict):
                            fallback_url = reddit_video.get('fallback_url')
                            if fallback_url:
                                return fallback_url
                        elif hasattr(reddit_video, 'fallback_url') and reddit_video.fallback_url:
                            return reddit_video.fallback_url
        except Exception as e:
            logger.debug(f"Error extracting video from media: {e}")
        
        # Check if post has a direct video URL
        if hasattr(post, 'url') and post.url:
            if self._is_video_url(post.url):
                return post.url
        
        # Check media_metadata for video
        try:
            if hasattr(post, 'media_metadata') and post.media_metadata:
                for media_id, meta in post.media_metadata.items():
                    if isinstance(meta, dict) and meta.get('status') == 'valid':
                        # Check for RedditVideo type
                        if meta.get('e') == 'RedditVideo':
                            # Check for direct video URL in the metadata
                            if 's' in meta:
                                source = meta['s']
                                if isinstance(source, dict):
                                    # Try different possible video URL fields
                                    video_url = source.get('mp4') or source.get('gif') or source.get('u')
                                    if video_url and self._is_video_url(video_url):
                                        return video_url.replace('&amp;', '&')
        except Exception as e:
            logger.debug(f"Error extracting video from media_metadata: {e}")
        
        # Check if URL is a Reddit video post link (v.redd.it)
        if hasattr(post, 'url') and post.url:
            if 'v.redd.it' in post.url or 'reddit.com/video' in post.url:
                # For v.redd.it links, try to get the actual video URL
                # The URL format is usually: https://v.redd.it/{id}
                # We might need to fetch the post details to get the actual video URL
                # For now, return the URL and let the download handle it
                return post.url
        
        return None

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

    def resolve_imgur_url(self, url: str) -> str:
        """Resolve imgur URLs to direct image links."""
        if 'imgur.com' in url and not url.endswith(('.jpg', '.png', '.gif', '.webp')):
            # Add .jpg extension if missing
            if not url.endswith('/'):
                url += '/'
            return url + '.jpg'
        return url

    def _generate_thumbnail(self, source_path: Path, subreddit: str = "") -> Optional[Path]:
        """Generate a thumbnail for an image or video file.
        
        Args:
            source_path: Path to source file
            subreddit: Subreddit name for folder structure
        
        Returns:
            Path to thumbnail if successful, None otherwise
        """
        try:
            from PIL import Image
            
            # Calculate relative path from download folder
            try:
                rel_path = source_path.relative_to(self.download_folder)
            except ValueError:
                # File is not under download folder, use filename only
                rel_path = Path(source_path.name)
            
            # Create corresponding thumbnail path
            thumb_folder = self.thumbs_folder
            if subreddit:
                thumb_folder = thumb_folder / self._sanitize_folder_name(subreddit)
                thumb_folder.mkdir(parents=True, exist_ok=True)
            
            thumb_path = thumb_folder / rel_path
            thumb_path = thumb_path.with_suffix('.jpg')  # Always save as JPEG
            
            # Skip if thumbnail already exists and is newer
            if thumb_path.exists():
                if thumb_path.stat().st_mtime >= source_path.stat().st_mtime:
                    return thumb_path
            
            # Create parent directory
            thumb_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Check if it's a video file
            video_extensions = {'.mp4', '.webm', '.mov', '.avi', '.mkv'}
            if source_path.suffix.lower() in video_extensions:
                return self._generate_video_thumbnail(source_path, thumb_path)
            
            # Process image
            with Image.open(source_path) as img:
                # Convert RGBA to RGB if necessary
                if img.mode in ('RGBA', 'LA', 'P'):
                    background = Image.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'P':
                        img = img.convert('RGBA')
                    background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
                    img = background
                elif img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # Create thumbnail maintaining aspect ratio
                img.thumbnail((300, 300), Image.Resampling.LANCZOS)
                
                # Save thumbnail
                img.save(thumb_path, 'JPEG', quality=85, optimize=True)
            
            return thumb_path
        except Exception as e:
            logger.debug(f"Error generating thumbnail for {source_path}: {e}")
            return None

    def _generate_video_thumbnail(self, video_path: Path, thumb_path: Path) -> Optional[Path]:
        """Generate a thumbnail from a video file using ffmpeg.
        
        Args:
            video_path: Path to source video
            thumb_path: Path to save thumbnail
        
        Returns:
            Path to thumbnail if successful, None otherwise
        """
        try:
            import subprocess
            
            # Use ffmpeg to extract a frame
            cmd = [
                'ffmpeg', '-y', '-i', str(video_path),
                '-vf', 'scale=300:300:force_original_aspect_ratio=decrease,pad=300:300:(ow-iw)/2:(oh-ih)/2',
                '-frames:v', '1',
                '-q:v', '2',
                str(thumb_path)
            ]
            
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
            
            if result.returncode == 0 and thumb_path.exists():
                return thumb_path
            else:
                return None
        except FileNotFoundError:
            logger.debug(f"ffmpeg not found, skipping video thumbnail: {video_path}")
            return None
        except Exception as e:
            logger.debug(f"Error generating video thumbnail: {e}")
            return None


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
        
        if args.scrape_all:
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
