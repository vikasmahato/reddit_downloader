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
from typing import List, Dict, Optional


class RedditImageDownloader:
    def __init__(self, config_file: str = "config.ini"):
        """Initialize the Reddit Image Downloader."""
        self.config = ConfigParser()
        self.config.read(config_file)
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

    def download_image(self, url: str, filename: str = None, subreddit: str = "") -> bool:
        """Download a single image from URL."""
        try:
            response = self.session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Determine filename if not provided
            if not filename:
                parsed_url = urlparse(url)
                filename = unquote(parsed_url.path.split('/')[-1])
                
                # Add timestamp if filename exists
                if os.path.exists(self.download_folder / filename):
                    name, ext = os.path.splitext(filename)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"{name}_{timestamp}{ext}"
            
            # Create subreddit-specific folder
            folder = self.download_folder
            if subreddit:
                folder = self.download_folder / subreddit
                folder.mkdir(exist_ok=True)
            
            filepath = folder / filename
            
            # Write image to file
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"✓ Downloaded: {filename}")
            return True
            
        except Exception as e:
            print(f"✗ Failed to download {url}: {e}")
            return False

    def get_image_urls_from_subreddit(self, subreddit: str, limit: int = 25, 
                                    time_filter: str = 'all') -> List[Dict]:
        """Get image URLs from a subreddit."""
        if not self.reddit:
            print("❌ Authentication required to access subreddit content")
            return []
        
        try:
            sub = self.reddit.subreddit(subreddit)
            posts = sub.hot(limit=limit)
            
            image_posts = []
            for post in posts:
                if not post.is_self:
                    url = post.url
                    if self._is_image_url(url):
                        image_posts.append({
                            'title': post.title,
                            'url': url,
                            'author': str(post.author),
                            'score': post.score,
                            'permalink': post.permalink,
                            'created_utc': post.created_utc
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

    def download_from_urls(self, urls: List[str], subreddit: str = ""):
        """Download images from a list of URLs."""
        successful = 0
        total = len(urls)
        
        print(f"\n📥 Downloading {total} images...")
        
        for i, url in enumerate(urls, 1):
            print(f"[{i}/{total}] {url}")
            if self.download_image(url):
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
        self.download_from_urls(urls, subreddit)

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
                        'permalink': post.permalink
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
    parser = argparse.ArgumentParser(description='Download images from Reddit')
    parser.add_argument('--urls', nargs='+', help='Direct image URLs to download')
    parser.add_argument('--subreddit', help='Subreddit to download images from')
    parser.add_argument('--limit', type=int, default=25, help='Number of images to download')
    parser.add_argument('--saved', action='store_true', help='Download from saved posts')
    parser.add_argument('--config', default='config.ini', help='Config file path')
    parser.add_argument('--setup', action='store_true', help='Create default config file')
    
    args = parser.parse_args()
    
    if args.setup:
        create_default_config()
        return
    
    if not os.path.exists(args.config):
        print("❌ Config file not found. Run with --setup to create one.")
        return
    
    try:
        downloader = RedditImageDownloader(args.config)
        
        if args.saved:
            print("📖 Fetching saved posts...")
            saved_posts = downloader.get_user_saved_posts(args.limit)
            if saved_posts:
                urls = [post['url'] for post in saved_posts]
                downloader.download_from_urls(urls, "saved_posts")
            else:
                print("❌ No saved image posts found")
        
        elif args.subreddit:
            downloader.download_from_subreddit(args.subreddit, args.limit)
        
        elif args.urls:
            downloader.download_from_urls(args.urls)
        
        else:
            parser.print_help()
            
    except KeyboardInterrupt:
        print("\n⏹️  Download cancelled by user")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
