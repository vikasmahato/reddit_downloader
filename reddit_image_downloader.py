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
    print(f"Error loading MySQL config: {e}")


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
            print(f"⚠️  Config parsing error: {e}")
            print("   Using defaults...")
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
            print("⚠️  No Reddit API credentials found. Using anonymous access only.")
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
                print(f"✓ Authenticated as: u/{user}")
                
            else:
                # Client credentials only (read-only, public content)
                self.reddit = praw.Reddit(
                    client_id=client_id,
                    client_secret=client_secret,
                    user_agent=self.config.get('reddit', 'user_agent', 
                                fallback='reddit_image_downloader')
                )
                print("✓ Connected with client credentials (read-only mode)")
                
        except Exception as e:
            print(f"⚠️  Reddit connection failed: {e}")
            print("   You'll still be able to download directly from URLs.")
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
                            print(f"Downloaded {downloaded//(1024*1024)}MB / {total_size//(1024*1024)}MB...")
            # GIF to MP4 conversion and size reporting using ffmpeg
            if filepath.suffix.lower() == '.gif':
                import subprocess
                import os
                gif_size = os.path.getsize(filepath)
                mp4_path = filepath.with_suffix('.mp4')
                print(f"Converting {filepath} to {mp4_path} using ffmpeg...")
                cmd = [
                    'ffmpeg', '-y', '-i', str(filepath),
                    '-movflags', 'faststart', '-pix_fmt', 'yuv420p', '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
                    str(mp4_path)
                ]
                try:
                    result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    mp4_size = os.path.getsize(mp4_path)
                    percent_diff = ((gif_size - mp4_size) / gif_size) * 100 if gif_size else 0
                    print(f"GIF size: {gif_size/1024:.2f} KB, MP4 size: {mp4_size/1024:.2f} KB, Size reduced by: {percent_diff:.2f}%")
                    # Remove GIF file
                    os.remove(filepath)
                    print(f"Deleted original GIF: {filepath}")
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
                    #     print(f"Failed to save ffmpeg details: {e}")
                except Exception as conv_err:
                    print(f"GIF to MP4 conversion failed: {conv_err}")
            else:
                # Save metadata for non-GIF files
                self._save_image_metadata(url, filename, subreddit, post_data, filepath, file_hash.hexdigest(), downloaded)
            if prev_record and prev_record.get('is_deleted'):
                if '_deleted' in filename:
                    new_filename = filename.replace('_deleted', '')
                    new_filepath = filepath.parent / new_filename
                    filepath.rename(new_filepath)
                    self._update_file_path_in_db(url, str(new_filepath))
                    print(f"✓ Restored: {new_filename}")
                else:
                    print(f"✓ Re-downloaded: {filename}")
            else:
                print(f"✓ Downloaded: {filename}")
            return True
        except Exception as e:
            print(f"✗ Failed to download {url}: {e}")
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
        try:
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM images WHERE url = %s', (url,))
            result = cursor.fetchone()
            conn.close()
            if result:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, result))
        except Exception as e:
            print(f"⚠️  Warning: Could not query metadata database: {e}")
        return None

    def _save_image_metadata(self, url: str, filename: str, subreddit: str, 
                            post_data: Dict, filepath: Path, file_hash: str, file_size: int):
        """Save image metadata to MySQL database."""
        try:
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor()
            now = datetime.now()
            download_date = now.strftime("%Y-%m-%d")
            download_time = now.strftime("%H:%M:%S")
            author = post_data.get('author', '') if post_data else ''
            title = post_data.get('title', '') if post_data else ''
            permalink = post_data.get('permalink', '') if post_data else ''
            post_username = post_data.get('post_username', '') if post_data else ''
            comments = post_data.get('comments', '') if post_data else ''
            url_field = post_data.get('all_urls', url) if post_data else url
            cursor.execute('''
                INSERT INTO images (url, filename, subreddit, username, author, title, permalink, 
                 download_date, download_time, file_hash, file_size, file_path, post_username, comments)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE filename=VALUES(filename), subreddit=VALUES(subreddit), username=VALUES(username), author=VALUES(author), title=VALUES(title), permalink=VALUES(permalink), download_date=VALUES(download_date), download_time=VALUES(download_time), file_hash=VALUES(file_hash), file_size=VALUES(file_size), file_path=VALUES(file_path), post_username=VALUES(post_username), comments=VALUES(comments)
            ''', (url_field, filename, subreddit, author, author, title, permalink,
                  download_date, download_time, file_hash, file_size, str(filepath), post_username, comments))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"⚠️  Warning: Could not save metadata: {e}")

    def _update_file_path_in_db(self, url: str, new_filepath: str):
        """Update file path in MySQL database."""
        try:
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor()
            cursor.execute('UPDATE images SET file_path = %s WHERE url = %s', (new_filepath, url))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"⚠️  Warning: Could not update file path: {e}")

    def _mark_image_as_deleted(self, url: str):
        """Mark an image as deleted in MySQL database by setting is_deleted to True."""
        try:
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor()
            cursor.execute('UPDATE images SET is_deleted = 1 WHERE url = %s', (url,))
            conn.commit()
            conn.close()
            print(f"Marked as deleted: {url}")
        except Exception as e:
            print(f"Error marking image as deleted: {e}")

    def check_deleted_images(self, subreddit: str = None) -> List[Dict]:
        """Check which previously downloaded images are now deleted."""
        deleted_images = []
        if not self.reddit:
            print("❌ Reddit connection required to check for deleted images")
            return deleted_images
        try:
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor()
            if subreddit:
                cursor.execute('SELECT * FROM images WHERE subreddit = %s', (subreddit,))
            else:
                cursor.execute('SELECT * FROM images WHERE is_deleted = 0')
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
                print(f"📝 Marked as deleted in DB: {img['filename']}")
        except Exception as e:
            print(f"❌ Error checking deleted images: {e}")
        return deleted_images


    def parse_scrape_list(self, section: str) -> List[str]:
        """Parse a config section for scraping lists."""
        items = []
        try:
            config_file_path = Path(self.config_file)
            
            if not config_file_path.exists():
                print(f"⚠️  Config file not found: {config_file_path}")
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
            print(f"⚠️  Warning: Could not parse {section} list: {e}")
        
        return items

    def scrape_from_config_list(self, scrape_type: str = "all"):
        """Scrape images from configured lists."""
        if not self.reddit:
            print("❌ Reddit connection required for batch scraping")
            return
        
        total_downloads = 0
        
        # Scrape subreddits
        if scrape_type in ["all", "subreddits"]:
            #subreddits = self.parse_scrape_list('scrape_list')
            subreddits = [line.strip() for line in config['scrape_list']['list'].splitlines() if line.strip()]
            if subreddits:
                print(f"\n📂 Found {len(subreddits)} subreddits in config")
                for subreddit in subreddits:
                    # Clean subreddit name (remove r/ if present)
                    clean_name = subreddit.replace('r/', '').strip()
                    print(f"\n🔍 Scraping r/{clean_name}...")
                    
                    limit = self.config.getint('general', 'max_images_per_subreddit', fallback=25)
                    self.download_from_subreddit(clean_name, limit)
                    total_downloads += 1
        
        # Scrape user posts
        if scrape_type in ["all", "users"]:
            users = self.parse_scrape_list('user_scrape_list')
            if users:
                print(f"\n👤 Found {len(users)} users in config")
                for username in users:
                    # Clean username (remove u/ if present)
                    clean_name = username.replace('u/', '').strip()
                    print(f"\n🔍 Scraping u/{clean_name}...")
                    
                    limit = self.config.getint('general', 'max_images_per_subreddit', fallback=25)
                    self.download_from_user(clean_name, limit)
                    total_downloads += 1
        
        print(f"\n✅ Batch scraping complete! Scraped from {total_downloads} sources.")

    def download_from_user(self, username: str, limit: int = 25):
        """Download images from a specific user's posts."""
        if not self.reddit:
            print("❌ Reddit connection required to access user posts")
            return
        
        try:
            # Remove u/ prefix if present
            username = username.replace('u/', '').strip()
            
            user = self.reddit.redditor(username)
            post_data_list = []
            
            print(f"🔍 Fetching posts from u/{username}...")
            
            submissions = user.submissions.new(limit=limit)
            
            for submission in submissions:
                if not submission.is_self and self._is_image_url(submission.url):
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
                    post_data_list.append({
                        'title': submission.title,
                        'url': submission.url,
                        'author': str(submission.author),
                        'subreddit': str(submission.subreddit),
                        'permalink': submission.permalink,
                        'created_utc': submission.created_utc,
                        'score': submission.score,
                        'comments': json.dumps(comments_list)
                    })
            
            if not post_data_list:
                print(f"❌ No image posts found for u/{username}")
                return
            
            print(f"📸 Found {len(post_data_list)} image posts from u/{username}")
            
            urls = [post['url'] for post in post_data_list]
            self.download_from_urls(urls, username, post_data_list)
            
        except Exception as e:
            print(f"❌ Error fetching posts from u/{username}: {e}")

    def get_image_urls_from_subreddit(self, subreddit: str, limit: int = 25, 
                                    time_filter: str = 'all') -> List[Dict]:
        """Get image URLs from a subreddit, saving gallery posts as a single record with all image URLs comma-separated."""
        if not self.reddit:
            print("❌ Authentication required to access subreddit content")
            return []
        try:
            sub = self.reddit.subreddit(subreddit)
            posts = sub.new(limit=limit)
            image_posts = []
            for post in posts:
                if not post.is_self:
                    # Handle gallery posts
                    if hasattr(post, 'gallery_data') and post.gallery_data and hasattr(post, 'media_metadata') and post.media_metadata:
                        gallery_items = post.gallery_data['items']
                        all_urls = []
                        for item in gallery_items:
                            media_id = item['media_id']
                            meta = post.media_metadata.get(media_id)
                            if meta and meta.get('status') == 'valid' and 's' in meta and 'u' in meta['s']:
                                img_url = meta['s']['u'].replace('&amp;', '&')
                                all_urls.append(img_url)
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
                            print(f"🛑 Already downloaded: {url}. Stopping further scraping for r/{subreddit}.")
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
        except Exception as e:
            print(f"❌ Error accessing subreddit {subreddit}: {e}")
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
        
        print(f"\n📥 Downloading {total} images...")
        
        for i, url in enumerate(urls, 1):
            print(f"[{i}/{total}] {url}")
            post_data = url_data[i-1] if url_data and i <= len(url_data) else None
            if self.download_image(url, subreddit=subreddit, post_data=post_data):
                successful += 1
        
        print(f"\n✅ Download complete: {successful}/{total} images downloaded")

    def download_from_subreddit(self, subreddit: str, limit: int = 25):
        """Download images from a subreddit."""
        print(f"\n🔍 Fetching images from r/{subreddit}...")
        image_posts = self.get_image_urls_from_subreddit(subreddit, limit)
        
        if not image_posts:
            print("❌ No images found")
            return
        
        print(f"📸 Found {len(image_posts)} image posts")
        
        urls = [post['url'] for post in image_posts]
        self.download_from_urls(urls, subreddit, image_posts)

    def get_user_saved_posts(self, limit: int = 25) -> List[Dict]:
        """Get saved posts from authenticated user."""
        if not self.reddit:
            print("❌ Reddit connection required to access saved posts")
            return []
        
        try:
            # Check if we have user authentication
            if not hasattr(self.reddit.user, 'me') or self.reddit.user.me() is None:
                print("❌ User authentication required to access saved posts")
                print("   Add username and password to config.ini for this feature")
                return []
                
            saved_posts = []
            for post in self.reddit.user.me().saved(limit=limit):
                if not post.is_self and self._is_image_url(post.url):
                    saved_posts.append({
                        'title': post.title,
                        'url': post.url,
                        'author': str(post.author),
                        'subreddit': str(post.subreddit),
                        'permalink': post.permalink,
                        'created_utc': post.created_utc,
                        'score': post.score
                    })
            
            return saved_posts
            
        except Exception as e:
            print(f"❌ Error fetching saved posts: {e}")
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
    
    print("📝 Created config.ini file. Please edit it with your Reddit credentials.")
    print("   Get Reddit API credentials at: https://www.reddit.com/prefs/apps")


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
        print("❌ Config file not found. Run with --setup to create one.")
        return

    # Loop mode: run --scrape-all every 5 minutes
    if args.loop:
        while True:
            print("\n⏳ Running batch scrape (--scrape-all)...")
            try:
                downloader = RedditImageDownloader(args.config)
                downloader.scrape_from_config_list("all")
            except KeyboardInterrupt:
                print("\n⏹️  Download cancelled by user")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
            print("🕒 Sleeping for 5 minutes...")
            time.sleep(300)
        return

    try:
        downloader = RedditImageDownloader(args.config)
        
        if args.saved:
            print("📖 Fetching saved posts...")
            saved_posts = downloader.get_user_saved_posts(args.limit)
            if saved_posts:
                urls = [post['url'] for post in saved_posts]
                downloader.download_from_urls(urls, "saved_posts", saved_posts)
            else:
                print("❌ No saved image posts found")
        
        elif args.scrape_all:
            print("📋 Scraping all sources from config...")
            downloader.scrape_from_config_list("all")
        
        elif args.scrape_subreddits:
            print("📂 Scraping subreddits from config...")
            downloader.scrape_from_config_list("subreddits")
        
        elif args.scrape_users:
            print("👤 Scraping users from config...")
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
                print(f"\n📝 Found {len(deleted)} marked/moved deleted images")
            else:
                print("\n✅ No deleted images found")
        
        elif args.list_metadata:
            pass  # TODO: Implement metadata listing
        
        else:
            parser.print_help()
            
    except KeyboardInterrupt:
        print("\n⏹️  Download cancelled by user")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
