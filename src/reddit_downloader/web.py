#!/usr/bin/env python3
"""
Web UI for Reddit Image Downloader

A Flask-based web interface to browse downloaded images and metadata.
Provides search, filtering, and gallery view capabilities.
"""

import os
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests as http_requests
from flask import Flask, render_template, request, jsonify, send_file, url_for, send_from_directory, redirect
from pathlib import Path
import json
from datetime import datetime
import mimetypes
import hashlib
from PIL import Image, ExifTags
import mysql.connector
from mysql.connector import pooling
import configparser

# File-backed cache for related subreddits (name_lower -> list of related names).
# Loaded from disk at startup; written back whenever new entries are fetched.
# Reddit API is only called for subreddits NOT already in the cache file.
_RELATED_CACHE_FILE = Path.cwd() / 'subreddit_map_cache.json'
_related_cache_lock = threading.Lock()

def _load_related_cache() -> dict:
    try:
        if _RELATED_CACHE_FILE.exists():
            return json.loads(_RELATED_CACHE_FILE.read_text(encoding='utf-8'))
    except Exception:
        pass
    return {}

def _save_related_cache(cache: dict) -> None:
    try:
        _RELATED_CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False), encoding='utf-8')
    except Exception:
        pass

_related_subreddits_cache: dict = _load_related_cache()

_MAP_HEADERS = {'User-Agent': 'subreddit-map-viewer/1.0'}

# ── Anvaka map-of-reddit position cache ───────────────────────────────────
# Stores pre-computed x/y from anvaka's graph.svg (community-overlap layout).
# None value means the sub was looked up but not found in anvaka's dataset.
_ANVAKA_CACHE_FILE = Path.cwd() / 'subreddit_anvaka_cache.json'
_anvaka_cache_lock = threading.Lock()

def _load_anvaka_cache() -> dict:
    try:
        if _ANVAKA_CACHE_FILE.exists():
            return json.loads(_ANVAKA_CACHE_FILE.read_text(encoding='utf-8'))
    except Exception:
        pass
    return {}

def _save_anvaka_cache(cache: dict) -> None:
    try:
        _ANVAKA_CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False), encoding='utf-8')
    except Exception:
        pass

_anvaka_position_cache: dict = _load_anvaka_cache()

def _fetch_related_sub(name: str) -> tuple:
    """Fetch related subreddits for one sub. Returns (name, rel_names, was_new_fetch).
    Checks the in-memory cache first; only hits Reddit API on a miss."""
    name_lower = name.lower()
    with _related_cache_lock:
        if name_lower in _related_subreddits_cache:
            return name, _related_subreddits_cache[name_lower], False
    try:
        resp = http_requests.get(
            f'https://www.reddit.com/subreddits/search.json?q={name}&limit=20&include_over_18=1',
            headers=_MAP_HEADERS,
            timeout=5,
        )
        if resp.status_code == 200:
            children = resp.json().get('data', {}).get('children', [])
            rel_names = [
                c['data']['display_name'] for c in children
                if c.get('data', {}).get('display_name', '').lower() != name_lower
                and c.get('data', {}).get('display_name')
            ]
            with _related_cache_lock:
                _related_subreddits_cache[name_lower] = rel_names
            return name, rel_names, True
    except Exception:
        pass
    # Cache the miss too so we don't retry until the user clears cache
    with _related_cache_lock:
        _related_subreddits_cache[name_lower] = []
    return name, [], True

# Get the directory where this file is located
_current_dir = Path(__file__).parent
_template_dir = _current_dir / 'templates'
_static_folder = Path.cwd() / 'reddit_downloads'

app = Flask(__name__, 
            template_folder=str(_template_dir),
            static_url_path='/reddit_downloads', 
            static_folder=str(_static_folder))

# Setup thumbs folder
_thumbs_folder = Path.cwd() / 'reddit_downloads_thumbs'
try:
    config = configparser.ConfigParser()
    config.read('config.ini')
    thumbs_folder = config.get('general', 'thumbs_folder', fallback='reddit_downloads_thumbs')
    _thumbs_folder = Path(thumbs_folder).resolve()
except Exception:
    pass

# Load MySQL config
config = configparser.ConfigParser()
config.read('config.ini')
mysql_config = {
    'host': config.get('mysql', 'host', fallback='localhost'),
    'port': config.getint('mysql', 'port', fallback=3306),
    'user': config.get('mysql', 'user', fallback='root'),
    'password': config.get('mysql', 'password', fallback=''),
    'database': config.get('mysql', 'database', fallback='reddit_images')
}

# Initialize MySQL connection pool (optional)
mysql_pool = None
try:
    pool_size = config.getint('mysql', 'pool_size', fallback=5)
    mysql_pool = pooling.MySQLConnectionPool(pool_name='web_pool', pool_size=pool_size, **mysql_config)
    print(f"MySQL connection pool created (size={pool_size})")
except Exception as e:
    # If pool creation fails, we'll fall back to direct connections
    print(f"Warning: Could not create MySQL connection pool: {e}")


def _get_db_connection():
    """Return a MySQL connection from the pool if available, otherwise open a new connection.
    Caller must close the connection when done.
    """
    try:
        if mysql_pool:
            return mysql_pool.get_connection()
        return mysql.connector.connect(**mysql_config)
    except Exception:
        # Final fallback
        return mysql.connector.connect(**mysql_config)


def _ensure_blocked_users_table():
    """Create the blocked_users table if it doesn't exist."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS blocked_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(255) NOT NULL UNIQUE,
                blocked_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Warning: Could not ensure blocked_users table: {e}")

_ensure_blocked_users_table()


def _ensure_favourite_column():
    """Add is_favourite column to images table if it doesn't exist."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE images ADD COLUMN is_favourite TINYINT(1) NOT NULL DEFAULT 0")
        conn.commit()
        conn.close()
    except Exception:
        # Column already exists — ignore
        pass

_ensure_favourite_column()


def _ensure_posts_deleted_column():
    """Add is_deleted column to posts table if it doesn't exist."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "ALTER TABLE posts ADD COLUMN is_deleted TINYINT(1) NOT NULL DEFAULT 0"
        )
        conn.commit()
        conn.close()
    except Exception:
        pass  # already exists

_ensure_posts_deleted_column()

# Add Python built-ins to template context
@app.context_processor
def inject_template_globals():
    import builtins
    template_globals = {
        'min': min,
        'max': max,
        'enumerate': enumerate,
        'zip': zip,
    }
    return template_globals

# Register a custom Jinja2 filter for JSON loading
@app.template_filter('loads')
def jinja_json_loads(s):
    try:
        return json.loads(s) if s else []
    except Exception:
        return []

# Register a custom Jinja2 filter for formatting Unix timestamps
@app.template_filter('format_datetime')
def jinja_format_datetime(value):
    try:
        ts = float(value)
        dt = datetime.utcfromtimestamp(ts)
        # Format: day-Mon-year hour:minute am/pm (e.g., 03-Oct-2025 02:15 PM)
        return dt.strftime('%d-%b-%Y %I:%M %p')
    except Exception:
        return ''

class RedditImageUI:
    def __init__(self, download_folder="reddit_downloads"):
        # store an absolute resolved download folder for reliable relative-path computation
        self.download_folder = Path(download_folder).resolve()
        # Get thumbs folder from config
        try:
            config = configparser.ConfigParser()
            config.read('config.ini')
            thumbs_folder = config.get('general', 'thumbs_folder', fallback='reddit_downloads_thumbs')
            self.thumbs_folder = Path(thumbs_folder).resolve()
        except Exception:
            self.thumbs_folder = Path('reddit_downloads_thumbs').resolve()

    def make_web_path(self, file_path):
        """Return a path relative to the download_folder suitable for use in /image/<web_path>.
        If it cannot be computed, return None.
        """
        if not file_path:
            return None
        try:
            fp = Path(file_path).resolve()
        except Exception:
            try:
                fp = Path(file_path)
            except Exception:
                return None
        # Try direct relative_to with resolved paths
        try:
            rel = fp.relative_to(self.download_folder)
            return str(rel).replace('\\', '/')
        except Exception:
            # Fallback: look for the download folder name in parts and build relative path
            parts = list(fp.parts)
            try:
                idx = parts.index(self.download_folder.name)
                rel_parts = parts[idx+1:]
                if rel_parts:
                    return str(Path(*rel_parts)).replace('\\', '/')
            except Exception:
                return None
        return None

    def make_thumb_path(self, file_path):
        """Return a path to thumbnail relative to thumbs_folder.
        If it cannot be computed, return None.
        """
        if not file_path:
            return None
        try:
            fp = Path(file_path).resolve()
        except Exception:
            try:
                fp = Path(file_path)
            except Exception:
                return None
        
        # Calculate relative path from download folder
        try:
            rel = fp.relative_to(self.download_folder)
        except Exception:
            # Fallback: use filename
            rel = Path(fp.name)
        
        # Convert to thumbnail path (always .jpg)
        thumb_rel = rel.with_suffix('.jpg')
        
        # Check if thumbnail exists
        thumb_path = self.thumbs_folder / thumb_rel
        if thumb_path.exists():
            return str(thumb_rel).replace('\\', '/')
        return None

    def get_all_images(self, limit=100, offset=0, subreddit=None, username=None, search=None, user=None, deleted=None):
        """
        Paginate on posts, then fetch all images for those posts.
        Each returned item represents one post with a post_images list.
        """
        try:
            conn = _get_db_connection()
            cursor = conn.cursor(dictionary=True)

            # Map provided username/user param
            effective_username = username or user
            # Prepare search placeholders
            search_param = search if search else None
            search_like = f"%{search}%" if search else None

            query = """
            SELECT
                p.id AS post_id,
                p.title, p.author, p.subreddit, p.permalink, p.created_utc,
                p.score, p.post_username, p.comments, p.flair,

                i.id AS image_id, i.file_hash, i.file_path, i.filename,
                i.file_size, i.download_date, i.download_time, i.is_deleted,
                pi.url
            FROM (
                SELECT id
                FROM posts
                WHERE (%s IS NULL OR subreddit = %s)
                AND (%s IS NULL OR author = %s)
                AND (%s IS NULL OR title LIKE %s OR author LIKE %s)
                AND (author IS NULL OR author NOT IN (SELECT username FROM blocked_users))
                ORDER BY created_utc DESC
                LIMIT %s OFFSET %s
            ) paged_posts
            JOIN posts p ON p.id = paged_posts.id
            LEFT JOIN post_images pi ON pi.post_id = p.id
            LEFT JOIN images i ON i.id = pi.image_id
            WHERE (%s IS NULL OR i.is_deleted = %s)
            ORDER BY p.created_utc DESC, i.id ASC
            """

            params = [
                subreddit, subreddit,
                effective_username, effective_username,
                search_param, search_like, search_like,
                limit, offset,
                deleted, deleted
            ]

            cursor.execute(query, params)
            results = cursor.fetchall()

            posts = {}

            for row in results:
                post_id = row["post_id"]

                if post_id not in posts:
                    # Count comments once per post
                    try:
                        comments = json.loads(row["comments"]) if row["comments"] else []
                        comment_count = len(comments)
                    except Exception:
                        comment_count = 0

                    posts[post_id] = {
                        "post_id": post_id,
                        "title": row["title"],
                        "author": row["author"],
                        "subreddit": row["subreddit"],
                        "permalink": row["permalink"],
                        "created_utc": row["created_utc"],
                        "score": row["score"],
                        "post_username": row["post_username"],
                        "comments": row["comments"],
                        "comment_count": comment_count,
                        "flair": row.get("flair"),
                        "post_images": []
                    }

                if row["image_id"]:
                    img = {
                        "id": row["image_id"],
                        "file_hash": row["file_hash"],
                        "file_path": row["file_path"],
                        "filename": row["filename"],
                        "file_size": row["file_size"],
                        "download_date": row["download_date"],
                        "download_time": row["download_time"],
                        "is_deleted": row["is_deleted"],
                        "url": row["url"]
                    }

                    if img["file_path"]:
                        web = self.make_web_path(img["file_path"])
                        if web:
                            img["web_path"] = web

                        thumb = self.make_thumb_path(img["file_path"])
                        if thumb:
                            img["thumb_path"] = thumb

                    posts[post_id]["post_images"].append(img)

            # Add image_count per post
            for post in posts.values():
                post["image_count"] = len(post["post_images"])

            # Ensure compatibility with templates: expose first image fields at top-level for each post
            for post in posts.values():
                # sane defaults to avoid Jinja2 UndefinedError
                post.setdefault('filename', None)
                post.setdefault('file_path', None)
                post.setdefault('web_path', None)
                post.setdefault('thumb_path', None)
                post.setdefault('url', None)

                if post.get('post_images'):
                    first_img = post['post_images'][0]
                    # top-level filename/file_path
                    if first_img.get('filename'):
                        post['filename'] = first_img.get('filename')
                    if first_img.get('file_path'):
                        post['file_path'] = first_img.get('file_path')
                    # web_path / thumb_path: prefer the image's values, else compute from file_path
                    if first_img.get('web_path'):
                        post['web_path'] = first_img.get('web_path')
                    elif post.get('file_path'):
                        wp = self.make_web_path(post['file_path'])
                        if wp:
                            post['web_path'] = wp

                    if first_img.get('thumb_path'):
                        post['thumb_path'] = first_img.get('thumb_path')
                    elif post.get('file_path'):
                        tp = self.make_thumb_path(post['file_path'])
                        if tp:
                            post['thumb_path'] = tp

                    if first_img.get('url'):
                        post['url'] = first_img.get('url')

            conn.close()
            return list(posts.values())

        except Exception as e:
            print(f"Database error: {e}")
            return []


    def get_stats(self):
        """Get download statistics from MySQL."""
        try:
            conn = _get_db_connection()
            cursor = conn.cursor()
            # Total images (count distinct images, not posts)
            cursor.execute("SELECT COUNT(*) FROM images")
            total_images = cursor.fetchone()[0]
            # Images by subreddit (optimized query with LIMIT)
            cursor.execute("""SELECT subreddit, COUNT(1) as cnt
                FROM posts GROUP BY subreddit 
                ORDER BY cnt DESC
                LIMIT 20""")
            subreddit_counts = dict(cursor.fetchall())
            # Top authors (for display) - optimized
            cursor.execute("""SELECT author, COUNT(1) as cnt
                FROM posts GROUP BY subreddit 
                ORDER BY cnt DESC
                LIMIT 20""")
            user_counts = dict(cursor.fetchall())
            # All unique authors (for stats) - optimized
            cursor.execute("""SELECT COUNT(DISTINCT p.author) 
                FROM posts p
                WHERE p.author IS NOT NULL AND p.author != ''""")
            total_users = cursor.fetchone()[0]
            # File size stats - optimized
            cursor.execute("SELECT COALESCE(SUM(file_size), 0) FROM images WHERE file_size > 0")
            total_size = cursor.fetchone()[0] or 0
            conn.close()
            return {
                'total_images': total_images,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'subreddit_counts': subreddit_counts,
                'top_users': user_counts,
                'total_subreddits': len(subreddit_counts),
                'total_users': total_users
            }
        except Exception as e:
            print(f"Stats error: {e}")
            return {}

    def get_subreddits(self, only_enabled=True):
        """Get list of subreddits from scrape_lists table for fast loading.
        
        Args:
            only_enabled: If True, only return enabled subreddits. If False, return all subreddits.
        """
        try:
            conn = _get_db_connection()
            cursor = conn.cursor()
            if only_enabled:
                cursor.execute("SELECT name FROM scrape_lists WHERE type = 'subreddit' AND enabled = TRUE ORDER BY name")
            else:
                cursor.execute("SELECT name FROM scrape_lists WHERE type = 'subreddit' ORDER BY enabled DESC, name")
            results = [row[0] for row in cursor.fetchall()]
            conn.close()
            return results
        except Exception as e:
            print(f"Subreddits error: {e}")
            return []

    def get_users(self):
        """Get list of unique users from MySQL."""
        try:
            conn = _get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT author FROM posts WHERE author IS NOT NULL AND author != '' ORDER BY author")
            results = [row[0] for row in cursor.fetchall()]
            conn.close()
            return results
        except Exception as e:
            print(f"Users error: {e}")
            return []

# Initialize the UI handler
ui_handler = RedditImageUI()

def extract_exif_data(image_path):
    try:
        from PIL.TiffImagePlugin import IFDRational
        with Image.open(image_path) as img:
            exif_data = img._getexif()
            if not exif_data:
                return None
            exif = {}
            for tag, value in exif_data.items():
                tag_name = ExifTags.TAGS.get(tag, tag)
                # Convert bytes to string for JSON serialization
                if isinstance(value, bytes):
                    try:
                        value = value.decode('utf-8', errors='replace')
                    except Exception:
                        value = value.hex()
                # Convert IFDRational to float
                elif 'IFDRational' in str(type(value)):
                    try:
                        value = float(value)
                    except Exception:
                        value = str(value)
                exif[tag_name] = value
            return exif
    except Exception:
        return None

@app.route('/')
def index():
    """Main gallery page."""
    page = int(request.args.get('page', 1))
    search = request.args.get('search', '')
    subreddit = request.args.get('subreddit', '')
    user = request.args.get('user', '')
    deleted = request.args.get('deleted', '')
    sort = request.args.get('sort', '')
    hidden_users = request.args.getlist('hidden_users')
    only_enabled = request.args.get('only_enabled', '1') == '1'  # Default to showing only enabled
    deleted_filter = None
    if deleted == '1':
        deleted_filter = True
    elif deleted == '0':
        deleted_filter = False
    per_page = 200
    offset = (page - 1) * per_page
    images = ui_handler.get_all_images(
        limit=per_page,
        offset=offset,
        search=search if search else None,
        subreddit=subreddit if subreddit else None,
        user=user if user else None,
        deleted=deleted_filter
    )
    for img in images:
        if img.get('file_path'):
            img['exif'] = extract_exif_data(img['file_path'])
    stats = ui_handler.get_stats()
    subreddits = ui_handler.get_subreddits(only_enabled=only_enabled)
    users = ui_handler.get_users()
    return render_template('index.html',
                         images=images,
                         stats=stats,
                         subreddits=subreddits,
                         only_enabled=only_enabled,
                         users=users,
                         current_page=page,
                         search=search,
                         filter_subreddit=subreddit,
                         filter_user=user,
                         filter_deleted=deleted,
                         sort=sort,
                         hidden_users=hidden_users)

@app.route('/api/images')
def api_images():
    """API endpoint for AJAX image loading."""
    page = int(request.args.get('page', 1))
    search = request.args.get('search', '')
    subreddit = request.args.get('subreddit', '')
    user = request.args.get('user', '')
    deleted = request.args.get('deleted', '')
    deleted_filter = None
    if deleted == '1':
        deleted_filter = True
    elif deleted == '0':
        deleted_filter = False

    per_page = 20
    offset = (page - 1) * per_page
    
    images = ui_handler.get_all_images(
        limit=per_page, 
        offset=offset, 
        search=search if search else None,
        subreddit=subreddit if subreddit else None,
        user=user if user else None,
        deleted=deleted_filter
    )
    
    return jsonify(images)

@app.route('/api/stats')
def api_stats():
    """API endpoint for statistics."""
    return jsonify(ui_handler.get_stats())

@app.route('/image/<path:filename>')
def serve_image(filename):
    # Legacy route: redirect to the static file URL so clients request /reddit_downloads/... directly.
    try:
        return redirect(url_for('static', filename=filename))
    except Exception:
        # Fallback to serving directly from the downloads directory
        download_dir = os.path.join(os.getcwd(), 'reddit_downloads')
        return send_from_directory(download_dir, filename)

@app.route('/thumbs/<path:filename>')
def serve_thumbnail(filename):
    """Serve thumbnail images."""
    try:
        return send_from_directory(str(_thumbs_folder), filename)
    except Exception:
        return "Thumbnail not found", 404

@app.route('/details/<int:post_id>')
def image_details(post_id):
    """Show detailed information for a specific post."""
    try:
        # Validate post_id
        if not post_id or post_id <= 0:
            return f"Invalid post_id: {post_id}", 400
        
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        # Get post information
        cursor.execute("""SELECT 
            p.id as post_id, p.title, p.author, p.subreddit, p.permalink, p.created_utc, 
            p.score, p.post_username, p.comments, p.reddit_id, p.flair
        FROM posts p
        WHERE p.id = %s""", (post_id,))
        post = cursor.fetchone()
        if not post:
            # Check if there are any images linked to this post_id (orphaned links)
            cursor.execute("""SELECT COUNT(*) as cnt FROM post_images WHERE post_id = %s""", (post_id,))
            orphan_check = cursor.fetchone()
            conn.close()
            if orphan_check and orphan_check['cnt'] > 0:
                return f"Post not found (post_id: {post_id}), but {orphan_check['cnt']} image(s) are still linked to it. This indicates orphaned data.", 404
            return f"Post not found (post_id: {post_id})", 404
        
        if post:
            post_dict = dict(post)
            # Convert datetime/timedelta fields to string for JSON serialization
            from datetime import timedelta, datetime, date, time
            for k, v in post_dict.items():
                if isinstance(v, (timedelta, datetime, date, time)):
                    post_dict[k] = str(v)
            
            # Get all images from this post
            cursor.execute("""SELECT 
                i.id, i.file_hash, i.file_path, i.filename, i.file_size, 
                i.download_date, i.download_time, i.is_deleted,
                pi.url
            FROM images i
            LEFT JOIN post_images pi ON i.id = pi.image_id
            WHERE pi.post_id = %s
            ORDER BY i.id ASC""", (post_id,))
            all_post_images = cursor.fetchall()
            post_images_list = []
            for post_img in all_post_images:
                post_img_dict = dict(post_img)
                # Convert datetime/timedelta fields to string
                for k, v in post_img_dict.items():
                    if isinstance(v, (timedelta, datetime, date, time)):
                        post_img_dict[k] = str(v)
                if post_img_dict.get('file_path'):
                    web = ui_handler.make_web_path(post_img_dict['file_path'])
                    if web:
                        post_img_dict['web_path'] = web
                    # Get thumbnail path
                    thumb = ui_handler.make_thumb_path(post_img_dict['file_path'])
                    if thumb:
                        post_img_dict['thumb_path'] = thumb
                post_images_list.append(post_img_dict)
            
            # Use first image for EXIF and other image-specific data
            first_image = post_images_list[0] if post_images_list else None
            if first_image:
                # Extract EXIF data from first image
                exif = extract_exif_data(first_image['file_path'])
                post_dict['exif'] = exif
                # Add first image file_path and web_path for compatibility
                post_dict['file_path'] = first_image['file_path']
                post_dict['web_path'] = first_image.get('web_path')
                post_dict['thumb_path'] = first_image.get('thumb_path')
                post_dict['filename'] = first_image.get('filename')
            
            # Check if post has any images
            if not post_images_list:
                conn.close()
                return f"Post found but has no images linked to it (post_id: {post_id})", 404
            
            post_dict['post_images'] = post_images_list
            post_dict['current_image_index'] = 0
            post_dict['image_count'] = len(post_images_list)
            
            # Get previous and next post IDs for navigation
            cursor.execute("SELECT id FROM posts WHERE id < %s ORDER BY id DESC LIMIT 1", (post_id,))
            prev_result = cursor.fetchone()
            post_dict['prev_post_id'] = prev_result['id'] if prev_result else None
            
            cursor.execute("SELECT id FROM posts WHERE id > %s ORDER BY id ASC LIMIT 1", (post_id,))
            next_result = cursor.fetchone()
            post_dict['next_post_id'] = next_result['id'] if next_result else None
            
            conn.close()
            # Pass stats, subreddits, users for template compatibility
            stats = ui_handler.get_stats()
            subreddits = ui_handler.get_subreddits()
            users = ui_handler.get_users()
            return render_template('details.html', image=post_dict, stats=stats, subreddits=subreddits, users=users)
        else:
            conn.close()
            return f"Post not found (post_id: {post_id})", 404
    except Exception as e:
        return f"Error: {e}", 500

@app.route('/api/post_comment', methods=['POST'])
def post_comment():
    """Post a comment to Reddit and save it locally in MySQL."""
    import json
    data = request.get_json()
    post_id = data.get('post_id')
    comment_text = data.get('comment', '').strip()
    if not post_id or not comment_text:
        return jsonify({'success': False, 'error': 'Missing post ID or comment.'}), 400
    # Get post info from MySQL
    conn = _get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""SELECT 
        id as post_id, reddit_id, permalink, comments
    FROM posts
    WHERE id = %s""", (post_id,))
    post = cursor.fetchone()
    if not post:
        conn.close()
        return jsonify({'success': False, 'error': 'Post not found.'}), 404
    # Get Reddit post ID or permalink
    reddit_post_id = post.get('reddit_id')
    permalink = post.get('permalink')
    if not reddit_post_id and not permalink:
        conn.close()
        return jsonify({'success': False, 'error': 'No Reddit post info.'}), 400
    # Post comment to Reddit
    try:
        from reddit_downloader.downloader import RedditImageDownloader
        rid = RedditImageDownloader()
        reddit = rid.reddit
        submission = None
        if reddit_post_id:
            submission = reddit.submission(id=reddit_post_id)
        elif permalink:
            import re
            m = re.search(r'/comments/([a-z0-9]+)/', permalink)
            if m:
                submission = reddit.submission(id=m.group(1))
        if not submission:
            conn.close()
            return jsonify({'success': False, 'error': 'Could not resolve Reddit submission.'}), 400
        reddit_comment = submission.reply(comment_text)
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': f'Reddit error: {e}'}), 500
    # Save comment locally in MySQL (in posts table)
    try:
        comments_json = post.get('comments', '[]')
        comments = json.loads(comments_json) if comments_json else []
        new_comment = {
            'author': reddit_comment.author.name if reddit_comment.author else 'You',
            'body': reddit_comment.body,
            'score': reddit_comment.score,
            'created_utc': reddit_comment.created_utc
        }
        comments.insert(0, new_comment)
        cursor.execute("UPDATE posts SET comments = %s WHERE id = %s", (json.dumps(comments), post_id))
        conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': f'Local save error: {e}'}), 500
    finally:
        conn.close()
    return jsonify({'success': True, 'comment': new_comment})

@app.route('/api/comments/<int:post_id>')
def get_comments(post_id):
    """Return latest comments for a post from MySQL."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        # Get comments from posts table
        cursor.execute("""SELECT comments FROM posts WHERE id = %s""", (post_id,))
        row = cursor.fetchone()
        conn.close()
        if row and row.get('comments'):
            import json
            comments = json.loads(row['comments'])
            return jsonify({'success': True, 'comments': comments})
        else:
            return jsonify({'success': True, 'comments': []})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/delete-post/<int:post_id>', methods=['DELETE'])
def delete_post(post_id):
    """Delete a post from the database. If image is not linked to other posts, delete image and move file to /deleted folder."""
    conn = None
    try:
        
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Get image_id and file path from the first image linked to this post
        image_id = None
        file_path = None
        cursor.execute("SELECT image_id FROM post_images WHERE post_id = %s LIMIT 1", (post_id,))
        result = cursor.fetchone()
        if result:
            image_id = result['image_id']
            cursor.execute("SELECT file_path FROM images WHERE id = %s", (image_id,))
            img_result = cursor.fetchone()
            if img_result:
                file_path = img_result['file_path']
        
        # 1. Delete from post_images (the link between post and image)
        cursor.execute("DELETE FROM post_images WHERE post_id = %s", (post_id,))
        deleted_links = cursor.rowcount
        
        # 2. Check if image is linked to any other posts
        image_deleted = False
        file_moved = False
        if image_id:
            cursor.execute("SELECT COUNT(*) as count FROM post_images WHERE image_id = %s", (image_id,))
            remaining_links = cursor.fetchone()['count']
            
            # 3. If image is not linked to any other posts, delete image and move file
            if remaining_links == 0 and file_path:
                # Move file to /deleted folder
                try:
                    from pathlib import Path
                    import shutil
                    
                    source_path = Path(file_path)
                    if source_path.exists():
                        # Create deleted folder inside the download folder
                        download_folder = ui_handler.download_folder
                        deleted_folder = download_folder / 'deleted'
                        deleted_folder.mkdir(parents=True, exist_ok=True)
                        
                        # Move file to deleted folder
                        dest_path = deleted_folder / source_path.name
                        # Handle filename conflicts
                        counter = 1
                        while dest_path.exists():
                            stem = source_path.stem
                            suffix = source_path.suffix
                            dest_path = deleted_folder / f"{stem}_{counter}{suffix}"
                            counter += 1

                        shutil.move(str(source_path), str(dest_path))
                        file_moved = True

                        # Also move MP4 if it's a GIF that was converted
                        if source_path.suffix.lower() == '.mp4':
                            # Check if there was an original file
                            pass
                        elif source_path.suffix.lower() == '.gif':
                            # Check if there's a corresponding MP4
                            mp4_path = source_path.with_suffix('.mp4')
                            if mp4_path.exists():
                                mp4_dest = deleted_folder / mp4_path.name
                                counter = 1
                                while mp4_dest.exists():
                                    stem = mp4_path.stem
                                    suffix = mp4_path.suffix
                                    mp4_dest = deleted_folder / f"{stem}_{counter}{suffix}"
                                    counter += 1
                                shutil.move(str(mp4_path), str(mp4_dest))
                except Exception as move_error:
                    # Log error but continue with deletion
                    print(f"Error moving file: {move_error}")
                
                # Delete from images table
                cursor.execute("DELETE FROM images WHERE id = %s", (image_id,))
                image_deleted = cursor.rowcount > 0
        
        # 4. Delete from posts table
        cursor.execute("DELETE FROM posts WHERE id = %s", (post_id,))
        post_deleted = cursor.rowcount > 0
        
        if not post_deleted:
            conn.rollback()
            conn.close()
            return jsonify({'success': False, 'error': 'Post not found'}), 404
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Post deleted successfully',
            'image_deleted': image_deleted,
            'file_moved': file_moved
        })
        
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/delete-posts-batch', methods=['DELETE'])
def delete_posts_batch():
    """Delete multiple posts from the database. If images are not linked to other posts, delete images and move files to /deleted folder."""
    conn = None
    try:
        data = request.get_json()
        post_ids = data.get('post_ids', [])
        
        if not post_ids or not isinstance(post_ids, list):
            return jsonify({'success': False, 'error': 'Invalid post_ids provided'}), 400
        
        if len(post_ids) == 0:
            return jsonify({'success': False, 'error': 'No post IDs provided'}), 400
        
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        deleted_count = 0
        errors = []
        
        # Process each post
        for post_id in post_ids:
            try:
                # Get all image_ids linked to this post
                cursor.execute("SELECT image_id FROM post_images WHERE post_id = %s", (post_id,))
                image_results = cursor.fetchall()
                image_ids = [row['image_id'] for row in image_results if row['image_id']]
                
                # For each image linked to this post, check if it's linked to other posts BEFORE deleting links
                images_to_delete = []
                for image_id in image_ids:
                    cursor.execute("SELECT COUNT(*) as count FROM post_images WHERE image_id = %s AND post_id != %s", (image_id, post_id))
                    remaining_links = cursor.fetchone()['count']
                    
                    # If image is not linked to any other posts, mark for deletion
                    if remaining_links == 0:
                        images_to_delete.append(image_id)
                
                # Delete from post_images (the link between post and image)
                cursor.execute("DELETE FROM post_images WHERE post_id = %s", (post_id,))
                
                # Process images that should be deleted
                for image_id in images_to_delete:
                    cursor.execute("SELECT file_path FROM images WHERE id = %s", (image_id,))
                    img_result = cursor.fetchone()
                    
                    if img_result and img_result['file_path']:
                        file_path = img_result['file_path']
                        try:
                            from pathlib import Path
                            import shutil
                            
                            source_path = Path(file_path)
                            if source_path.exists():
                                # Create deleted folder inside the download folder
                                download_folder = ui_handler.download_folder
                                deleted_folder = download_folder / 'deleted'
                                deleted_folder.mkdir(parents=True, exist_ok=True)
                                
                                # Move file to deleted folder
                                dest_path = deleted_folder / source_path.name
                                # Handle filename conflicts
                                counter = 1
                                while dest_path.exists():
                                    stem = source_path.stem
                                    suffix = source_path.suffix
                                    dest_path = deleted_folder / f"{stem}_{counter}{suffix}"
                                    counter += 1
                                
                                shutil.move(str(source_path), str(dest_path))
                                
                                # Also move MP4 if it's a GIF that was converted
                                if source_path.suffix.lower() == '.gif':
                                    mp4_path = source_path.with_suffix('.mp4')
                                    if mp4_path.exists():
                                        mp4_dest = deleted_folder / mp4_path.name
                                        counter = 1
                                        while mp4_dest.exists():
                                            stem = mp4_path.stem
                                            suffix = mp4_path.suffix
                                            mp4_dest = deleted_folder / f"{stem}_{counter}{suffix}"
                                            counter += 1
                                        shutil.move(str(mp4_path), str(mp4_dest))
                        except Exception as move_error:
                            # Log error but continue with deletion
                            print(f"Error moving file for post {post_id}: {move_error}")
                    
                    # Delete from images table
                    cursor.execute("DELETE FROM images WHERE id = %s", (image_id,))
                
                # Delete from posts table
                cursor.execute("DELETE FROM posts WHERE id = %s", (post_id,))
                if cursor.rowcount > 0:
                    deleted_count += 1
                    
            except Exception as post_error:
                errors.append(f"Error deleting post {post_id}: {str(post_error)}")
                print(f"Error deleting post {post_id}: {post_error}")
        
        if deleted_count == 0 and errors:
            conn.rollback()
            conn.close()
            return jsonify({'success': False, 'error': 'Failed to delete any posts', 'details': errors}), 400
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'{deleted_count} post(s) deleted successfully',
            'deleted_count': deleted_count,
            'errors': errors if errors else None
        })
        
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/scrape-lists')
def scrape_lists():
    """Page for managing scrape lists."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT sl.id, sl.type, sl.name, sl.enabled, sl.created_at, sl.updated_at, sl.last_scraped_at,
                   COUNT(DISTINCT p.id) as post_count
            FROM scrape_lists sl
            LEFT JOIN posts p ON sl.name = p.subreddit AND sl.type = 'subreddit'
            GROUP BY sl.id, sl.type, sl.name, sl.enabled, sl.created_at, sl.updated_at, sl.last_scraped_at
            ORDER BY sl.type, sl.enabled DESC, sl.name
        """)
        
        items = cursor.fetchall()
        conn.close()
        
        # Convert datetime objects to strings for template
        for item in items:
            for key in ['created_at', 'updated_at', 'last_scraped_at']:
                if item.get(key):
                    item[key] = item[key].strftime('%Y-%m-%d %H:%M:%S') if hasattr(item[key], 'strftime') else str(item[key])
            # Ensure post_count is an integer
            item['post_count'] = int(item.get('post_count', 0)) if item.get('post_count') is not None else 0
        
        stats = ui_handler.get_stats()
        subreddits = ui_handler.get_subreddits()
        users = ui_handler.get_users()
        return render_template('scrape_lists.html', 
                             items=items, 
                             stats=stats, 
                             subreddits=subreddits, 
                             users=users)
    except Exception as e:
        return f"Error: {e}", 500

@app.route('/api/scrape-lists', methods=['GET'])
def api_get_scrape_lists():
    """API endpoint to get all scrape lists."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT sl.id, sl.type, sl.name, sl.enabled, sl.created_at, sl.updated_at, sl.last_scraped_at,
                   COUNT(DISTINCT p.id) as post_count
            FROM scrape_lists sl
            LEFT JOIN posts p ON sl.name = p.subreddit AND sl.type = 'subreddit'
            GROUP BY sl.id, sl.type, sl.name, sl.enabled, sl.created_at, sl.updated_at, sl.last_scraped_at
            ORDER BY sl.type, sl.enabled DESC, sl.name
        """)
        
        items = cursor.fetchall()
        conn.close()
        
        # Convert datetime objects to strings
        for item in items:
            for key in ['created_at', 'updated_at', 'last_scraped_at']:
                if item.get(key):
                    item[key] = item[key].strftime('%Y-%m-%d %H:%M:%S') if hasattr(item[key], 'strftime') else str(item[key])
            # Ensure post_count is an integer
            item['post_count'] = int(item.get('post_count', 0)) if item.get('post_count') is not None else 0
        
        return jsonify({'success': True, 'items': items})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/scrape-lists', methods=['POST'])
def api_add_scrape_list():
    """API endpoint to add a new scrape list item."""
    try:
        data = request.get_json()
        list_type = data.get('type')
        name = data.get('name', '').strip()
        
        if not list_type or list_type not in ['subreddit', 'user']:
            return jsonify({'success': False, 'error': 'Invalid type. Must be "subreddit" or "user"'}), 400
        
        if not name:
            return jsonify({'success': False, 'error': 'Name is required'}), 400
        
        # Clean name (remove r/ or u/ prefix if present)
        name = name.replace('r/', '').replace('u/', '').strip()
        
        conn = _get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO scrape_lists (type, name, enabled)
                VALUES (%s, %s, TRUE)
            """, (list_type, name))
            conn.commit()
            item_id = cursor.lastrowid
            conn.close()
            return jsonify({'success': True, 'id': item_id, 'message': 'Item added successfully'})
        except mysql.connector.IntegrityError:
            conn.rollback()
            conn.close()
            return jsonify({'success': False, 'error': 'Item already exists'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/scrape-lists/<int:item_id>', methods=['PUT'])
def api_update_scrape_list(item_id):
    """API endpoint to update a scrape list item."""
    try:
        data = request.get_json()
        name = data.get('name', '').strip()
        enabled = data.get('enabled')
        
        if not name:
            return jsonify({'success': False, 'error': 'Name is required'}), 400
        
        # Clean name (remove r/ or u/ prefix if present)
        name = name.replace('r/', '').replace('u/', '').strip()
        
        conn = _get_db_connection()
        cursor = conn.cursor()
        
        # Build update query dynamically
        updates = ['name = %s']
        params = [name]
        
        if enabled is not None:
            updates.append('enabled = %s')
            params.append(bool(enabled))
        
        params.append(item_id)
        
        cursor.execute(f"""
            UPDATE scrape_lists
            SET {', '.join(updates)}
            WHERE id = %s
        """, params)
        
        if cursor.rowcount == 0:
            conn.close()
            return jsonify({'success': False, 'error': 'Item not found'}), 404
        
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Item updated successfully'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/scrape-lists/<int:item_id>', methods=['DELETE'])
def api_delete_scrape_list(item_id):
    """API endpoint to delete a scrape list item."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM scrape_lists WHERE id = %s", (item_id,))
        
        if cursor.rowcount == 0:
            conn.close()
            return jsonify({'success': False, 'error': 'Item not found'}), 404
        
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Item deleted successfully'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/scrape-lists/<int:item_id>/toggle', methods=['POST'])
def api_toggle_scrape_list(item_id):
    """API endpoint to toggle enabled status of a scrape list item."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT enabled FROM scrape_lists WHERE id = %s", (item_id,))
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            return jsonify({'success': False, 'error': 'Item not found'}), 404
        
        new_enabled = not result['enabled']
        cursor.execute("UPDATE scrape_lists SET enabled = %s WHERE id = %s", (new_enabled, item_id))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'enabled': new_enabled, 'message': 'Status updated successfully'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/subreddit-map')
def subreddit_map():
    """Page showing a visual network map of tracked subreddits and related ones."""
    stats = ui_handler.get_stats()
    subreddits = ui_handler.get_subreddits()
    users = ui_handler.get_users()
    return render_template('subreddit_map.html', stats=stats, subreddits=subreddits, users=users)


@app.route('/api/subreddit-map-cache/clear', methods=['POST'])
def api_clear_subreddit_map_cache():
    """Clear the related-subreddits cache (memory + file) so the next load refetches from Reddit."""
    with _related_cache_lock:
        _related_subreddits_cache.clear()
    try:
        if _RELATED_CACHE_FILE.exists():
            _RELATED_CACHE_FILE.unlink()
    except Exception:
        pass
    return jsonify({'success': True, 'message': 'Cache cleared'})


@app.route('/api/subreddit-map-data')
def api_subreddit_map_data():
    """Returns nodes and links for the subreddit map visualization."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT sl.id, sl.name, sl.enabled,
                   COUNT(DISTINCT p.id) as post_count
            FROM scrape_lists sl
            LEFT JOIN posts p ON sl.name = p.subreddit AND sl.type = 'subreddit'
            WHERE sl.type = 'subreddit'
            GROUP BY sl.id, sl.name, sl.enabled
            ORDER BY sl.name
        """)
        scraped = cursor.fetchall()
        conn.close()

        scraped_names_lower = {s['name'].lower() for s in scraped}

        related_map: dict = {}
        newly_fetched = False

        # ── Depth-1: parallel fetch for all tracked subs ──────────────────
        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = {ex.submit(_fetch_related_sub, sub['name']): sub['name'] for sub in scraped}
            for future in as_completed(futures):
                name, rel_names, was_new = future.result()
                related_map[name] = rel_names
                if was_new:
                    newly_fetched = True

        # ── Depth-2: fetch related-of-related for the most connected subs ─
        # Count how many tracked subs link to each depth-1 sub
        depth1_freq: dict = {}
        depth1_display: dict = {}   # lowercase → display_name
        for rel_list in related_map.values():
            for rel in rel_list:
                rl = rel.lower()
                depth1_freq[rl] = depth1_freq.get(rl, 0) + 1
                depth1_display[rl] = rel

        # Top 40 most-connected depth-1 subs (excluding already-tracked)
        top_d1_lower = sorted(
            [k for k in depth1_freq if k not in scraped_names_lower],
            key=lambda x: depth1_freq[x], reverse=True
        )[:40]

        # Load any already-cached depth-2 results
        for k in top_d1_lower:
            with _related_cache_lock:
                if k in _related_subreddits_cache:
                    display = depth1_display.get(k, k)
                    if display not in related_map:
                        related_map[display] = _related_subreddits_cache[k]

        # Fetch remaining uncached depth-1 subs in parallel
        to_fetch_d2 = [
            depth1_display.get(k, k)
            for k in top_d1_lower
            if k not in _related_subreddits_cache
        ]
        if to_fetch_d2:
            with ThreadPoolExecutor(max_workers=6) as ex:
                futures = {ex.submit(_fetch_related_sub, name): name for name in to_fetch_d2}
                for future in as_completed(futures):
                    name, rel_names, was_new = future.result()
                    if rel_names:
                        related_map[name] = rel_names
                    if was_new:
                        newly_fetched = True

        if newly_fetched:
            with _related_cache_lock:
                _save_related_cache(_related_subreddits_cache)

        # ── Build node list ────────────────────────────────────────────────
        # Determine which subs are directly connected to tracked subs (depth 1)
        depth1_names_lower: set = set()
        for sub in scraped:
            for rel in related_map.get(sub['name'], []):
                depth1_names_lower.add(rel.lower())

        nodes = []
        for sub in scraped:
            nodes.append({
                'id': sub['name'],
                'name': sub['name'],
                'status': 'enabled' if sub['enabled'] else 'disabled',
                'db_id': sub['id'],
                'post_count': int(sub.get('post_count') or 0),
                'in_list': True,
                'depth': 0,
            })

        added_related: set = set()
        for sub_name, rel_list in related_map.items():
            for rel in rel_list:
                rel_lower = rel.lower()
                if rel_lower not in scraped_names_lower and rel_lower not in added_related:
                    nodes.append({
                        'id': rel,
                        'name': rel,
                        'status': 'related',
                        'db_id': None,
                        'post_count': 0,
                        'in_list': False,
                        'depth': 1 if rel_lower in depth1_names_lower else 2,
                    })
                    added_related.add(rel_lower)

        # ── Build links ────────────────────────────────────────────────────
        node_ids_lower = {n['id'].lower() for n in nodes}
        links = []
        for sub_name, rel_list in related_map.items():
            for rel in rel_list:
                if rel.lower() in node_ids_lower:
                    links.append({'source': sub_name, 'target': rel})

        return jsonify({'success': True, 'nodes': nodes, 'links': links})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/subreddit-anvaka-positions')
def api_subreddit_anvaka_positions():
    """
    Return x/y positions for subreddits from anvaka's pre-computed Reddit map layout.
    Positions are based on Jaccard similarity of user overlap (community geography).
    Streams graph.svg from anvaka's CDN and caches results locally.
    """
    import re as _re

    names_param = request.args.get('names', '')
    names = [n.strip() for n in names_param.split(',') if n.strip()]
    if not names:
        return jsonify({'success': True, 'positions': {}, 'viewBox': None})

    result = {}
    needed_lower: dict = {}  # lower -> original name

    with _anvaka_cache_lock:
        for name in names:
            nl = name.lower()
            if nl in _anvaka_position_cache:
                result[name] = _anvaka_position_cache[nl]
            else:
                needed_lower[nl] = name

    viewBox = None

    if needed_lower:
        SVG_URL = 'https://anvaka.github.io/map-of-reddit-data/v3/graph.svg'
        # Circles: <circle id="_name" cx="x" cy="y" r="r"/>  (id has _ prefix in SVG)
        circle_re = _re.compile(r'<circle\b([^>]*?)/?>', _re.IGNORECASE | _re.DOTALL)
        attr_re   = _re.compile(r'\b(id|cx|cy)\s*=\s*"([^"]*)"')
        vbox_re   = _re.compile(r'<svg\b[^>]*\bviewBox="([^"]+)"', _re.IGNORECASE)

        buffer = ''
        found_count = 0
        try:
            resp = http_requests.get(SVG_URL, headers=_MAP_HEADERS, stream=True, timeout=60)
            for raw in resp.iter_content(chunk_size=65536):
                chunk = raw.decode('utf-8', errors='ignore')
                buffer += chunk

                if viewBox is None:
                    vm = vbox_re.search(buffer)
                    if vm:
                        viewBox = vm.group(1)

                for cm in circle_re.finditer(buffer):
                    attrs = dict(attr_re.findall(cm.group(1)))
                    raw_id = attrs.get('id', '')
                    sub_id = raw_id.lstrip('_').lower()
                    if sub_id not in needed_lower:
                        continue
                    try:
                        pos = {'x': float(attrs['cx']), 'y': float(attrs['cy'])}
                    except (KeyError, ValueError):
                        continue
                    orig = needed_lower[sub_id]
                    result[orig] = pos
                    with _anvaka_cache_lock:
                        _anvaka_position_cache[sub_id] = pos
                    found_count += 1
                    if found_count >= len(needed_lower):
                        resp.close()
                        break

                if found_count >= len(needed_lower):
                    break

                # Keep only the tail so we don't miss tags split across chunks
                last_lt = buffer.rfind('<')
                if last_lt > 8192:
                    buffer = buffer[last_lt:]

            try:
                resp.close()
            except Exception:
                pass
        except Exception:
            pass

        # Mark not-found subs to avoid re-querying
        with _anvaka_cache_lock:
            for nl, orig in needed_lower.items():
                if nl not in _anvaka_position_cache:
                    _anvaka_position_cache[nl] = None
                    result[orig] = None
            _save_anvaka_cache(_anvaka_position_cache)

    return jsonify({'success': True, 'positions': result, 'viewBox': viewBox})


@app.route('/api/scrape-lists/by-name/<name>', methods=['POST'])
def api_add_scrape_list_by_name(name):
    """Add a subreddit to the scrape list by name."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO scrape_lists (type, name, enabled) VALUES ('subreddit', %s, TRUE)",
                (name,)
            )
            conn.commit()
            item_id = cursor.lastrowid
            conn.close()
            return jsonify({'success': True, 'id': item_id})
        except mysql.connector.IntegrityError:
            conn.rollback()
            conn.close()
            return jsonify({'success': False, 'error': 'Already in list'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/scrape-lists/toggle-by-name/<name>', methods=['POST'])
def api_toggle_scrape_list_by_name(name):
    """Toggle enabled status of a subreddit in the scrape list by name."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT id, enabled FROM scrape_lists WHERE name = %s AND type = 'subreddit'",
            (name,)
        )
        result = cursor.fetchone()
        if not result:
            conn.close()
            return jsonify({'success': False, 'error': 'Not found in scrape list'}), 404
        new_enabled = not result['enabled']
        cursor.execute("UPDATE scrape_lists SET enabled = %s WHERE id = %s", (new_enabled, result['id']))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'enabled': new_enabled, 'id': result['id']})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/scrape-lists/remove-by-name/<name>', methods=['DELETE'])
def api_remove_scrape_list_by_name(name):
    """Remove a subreddit from the scrape list by name."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM scrape_lists WHERE name = %s AND type = 'subreddit'",
            (name,)
        )
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/delete-posts-by-user/<username>', methods=['DELETE'])
def delete_posts_by_user(username):
    """Delete all posts by a specific user. If images are not linked to other posts, delete images and move files to /deleted folder."""
    conn = None
    try:
        if not username:
            return jsonify({'success': False, 'error': 'Username is required'}), 400
        
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Get all post IDs for this user
        cursor.execute("SELECT id FROM posts WHERE author = %s", (username,))
        post_results = cursor.fetchall()
        post_ids = [row['id'] for row in post_results]
        
        if not post_ids:
            conn.close()
            return jsonify({'success': True, 'message': f'No posts found for user {username}', 'deleted_count': 0})
        
        deleted_count = 0
        errors = []
        
        # Process each post (reuse logic from delete_posts_batch)
        for post_id in post_ids:
            try:
                # Get all image_ids linked to this post
                cursor.execute("SELECT image_id FROM post_images WHERE post_id = %s", (post_id,))
                image_results = cursor.fetchall()
                image_ids = [row['image_id'] for row in image_results if row['image_id']]
                
                # For each image linked to this post, check if it's linked to other posts BEFORE deleting links
                images_to_delete = []
                for image_id in image_ids:
                    cursor.execute("SELECT COUNT(*) as count FROM post_images WHERE image_id = %s AND post_id != %s", (image_id, post_id))
                    remaining_links = cursor.fetchone()['count']
                    
                    # If image is not linked to any other posts, mark for deletion
                    if remaining_links == 0:
                        images_to_delete.append(image_id)
                
                # Delete from post_images (the link between post and image)
                cursor.execute("DELETE FROM post_images WHERE post_id = %s", (post_id,))
                
                # Process images that should be deleted
                for image_id in images_to_delete:
                    cursor.execute("SELECT file_path FROM images WHERE id = %s", (image_id,))
                    img_result = cursor.fetchone()
                    
                    if img_result and img_result['file_path']:
                        file_path = img_result['file_path']
                        try:
                            from pathlib import Path
                            import shutil
                            
                            source_path = Path(file_path)
                            if source_path.exists():
                                # Create deleted folder inside the download folder
                                download_folder = ui_handler.download_folder
                                deleted_folder = download_folder / 'deleted'
                                deleted_folder.mkdir(parents=True, exist_ok=True)
                                
                                # Move file to deleted folder
                                dest_path = deleted_folder / source_path.name
                                # Handle filename conflicts
                                counter = 1
                                while dest_path.exists():
                                    stem = source_path.stem
                                    suffix = source_path.suffix
                                    dest_path = deleted_folder / f"{stem}_{counter}{suffix}"
                                    counter += 1
                                
                                shutil.move(str(source_path), str(dest_path))
                                
                                # Also move MP4 if it's a GIF that was converted
                                if source_path.suffix.lower() == '.gif':
                                    mp4_path = source_path.with_suffix('.mp4')
                                    if mp4_path.exists():
                                        mp4_dest = deleted_folder / mp4_path.name
                                        counter = 1
                                        while mp4_dest.exists():
                                            stem = mp4_path.stem
                                            suffix = mp4_path.suffix
                                            mp4_dest = deleted_folder / f"{stem}_{counter}{suffix}"
                                            counter += 1
                                        shutil.move(str(mp4_path), str(mp4_dest))
                        except Exception as move_error:
                            # Log error but continue with deletion
                            print(f"Error moving file for post {post_id}: {move_error}")
                    
                    # Delete from images table
                    cursor.execute("DELETE FROM images WHERE id = %s", (image_id,))
                
                # Delete from posts table
                cursor.execute("DELETE FROM posts WHERE id = %s", (post_id,))
                if cursor.rowcount > 0:
                    deleted_count += 1
                    
            except Exception as post_error:
                errors.append(f"Error deleting post {post_id}: {str(post_error)}")
                print(f"Error deleting post {post_id}: {post_error}")
        
        if deleted_count == 0 and errors:
            conn.rollback()
            conn.close()
            return jsonify({'success': False, 'error': 'Failed to delete any posts', 'details': errors}), 400
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'{deleted_count} post(s) deleted successfully for user {username}',
            'deleted_count': deleted_count,
            'errors': errors if errors else None
        })
        
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500

# ═══════════════════════════════════════════════════════════════════════════
# BLOCKED USERS
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/blocked-users')
def blocked_users_page():
    """Page for managing blocked users."""
    stats = ui_handler.get_stats()
    subreddits = ui_handler.get_subreddits()
    users = ui_handler.get_users()
    try:
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT bu.username, bu.blocked_at,
                   COUNT(DISTINCT p.id) as post_count
            FROM blocked_users bu
            LEFT JOIN posts p ON p.author = bu.username
            GROUP BY bu.username, bu.blocked_at
            ORDER BY bu.blocked_at DESC
        """)
        blocked = cursor.fetchall()
        conn.close()
        for row in blocked:
            if row.get('blocked_at') and hasattr(row['blocked_at'], 'strftime'):
                row['blocked_at'] = row['blocked_at'].strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        blocked = []
    return render_template('blocked_users.html', blocked=blocked, stats=stats, subreddits=subreddits, users=users)


@app.route('/api/blocked-users', methods=['GET'])
def api_get_blocked_users():
    """Return list of blocked users."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT username, blocked_at FROM blocked_users ORDER BY blocked_at DESC")
        rows = cursor.fetchall()
        conn.close()
        for row in rows:
            if row.get('blocked_at') and hasattr(row['blocked_at'], 'strftime'):
                row['blocked_at'] = row['blocked_at'].strftime('%Y-%m-%d %H:%M:%S')
        return jsonify({'success': True, 'blocked_users': rows})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/block-user/<username>', methods=['POST'])
def api_block_user(username):
    """Block a user. Optionally delete their existing posts."""
    if not username:
        return jsonify({'success': False, 'error': 'Username required'}), 400
    data = request.get_json() or {}
    delete_posts = data.get('delete_posts', False)
    conn = None
    try:
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                "INSERT INTO blocked_users (username) VALUES (%s)",
                (username,)
            )
            conn.commit()
        except mysql.connector.IntegrityError:
            pass  # already blocked — that's fine

        deleted_count = 0
        if delete_posts:
            # Reuse delete logic: get all post IDs for this user
            cursor.execute("SELECT id FROM posts WHERE author = %s", (username,))
            post_ids = [row['id'] for row in cursor.fetchall()]
            for post_id in post_ids:
                try:
                    cursor.execute("SELECT image_id FROM post_images WHERE post_id = %s", (post_id,))
                    image_ids = [r['image_id'] for r in cursor.fetchall() if r['image_id']]
                    images_to_delete = []
                    for image_id in image_ids:
                        cursor.execute(
                            "SELECT COUNT(*) as count FROM post_images WHERE image_id = %s AND post_id != %s",
                            (image_id, post_id)
                        )
                        if cursor.fetchone()['count'] == 0:
                            images_to_delete.append(image_id)
                    cursor.execute("DELETE FROM post_images WHERE post_id = %s", (post_id,))
                    for image_id in images_to_delete:
                        cursor.execute("SELECT file_path FROM images WHERE id = %s", (image_id,))
                        img_result = cursor.fetchone()
                        if img_result and img_result['file_path']:
                            try:
                                import shutil
                                source_path = Path(img_result['file_path'])
                                if source_path.exists():
                                    deleted_folder = ui_handler.download_folder / 'deleted'
                                    deleted_folder.mkdir(parents=True, exist_ok=True)
                                    dest_path = deleted_folder / source_path.name
                                    counter = 1
                                    while dest_path.exists():
                                        dest_path = deleted_folder / f"{source_path.stem}_{counter}{source_path.suffix}"
                                        counter += 1
                                    shutil.move(str(source_path), str(dest_path))
                                    if source_path.suffix.lower() == '.gif':
                                        mp4_path = source_path.with_suffix('.mp4')
                                        if mp4_path.exists():
                                            mp4_dest = deleted_folder / mp4_path.name
                                            c2 = 1
                                            while mp4_dest.exists():
                                                mp4_dest = deleted_folder / f"{mp4_path.stem}_{c2}{mp4_path.suffix}"
                                                c2 += 1
                                            shutil.move(str(mp4_path), str(mp4_dest))
                            except Exception as move_err:
                                print(f"Error moving file: {move_err}")
                        cursor.execute("DELETE FROM images WHERE id = %s", (image_id,))
                    cursor.execute("DELETE FROM posts WHERE id = %s", (post_id,))
                    if cursor.rowcount > 0:
                        deleted_count += 1
                except Exception as pe:
                    print(f"Error deleting post {post_id}: {pe}")
            conn.commit()

        conn.close()
        return jsonify({
            'success': True,
            'message': f'User {username} blocked' + (f' and {deleted_count} post(s) deleted' if delete_posts else ''),
            'deleted_count': deleted_count
        })
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/unblock-user/<username>', methods=['DELETE'])
def api_unblock_user(username):
    """Unblock a user."""
    if not username:
        return jsonify({'success': False, 'error': 'Username required'}), 400
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM blocked_users WHERE username = %s", (username,))
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        if affected == 0:
            return jsonify({'success': False, 'error': 'User not found in block list'}), 404
        return jsonify({'success': True, 'message': f'User {username} unblocked'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# DELETED FOLDER
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/deleted')
def deleted_folder_page():
    """Browse files in the deleted folder."""
    stats = ui_handler.get_stats()
    subreddits = ui_handler.get_subreddits()
    users = ui_handler.get_users()
    return render_template('deleted_folder.html', stats=stats, subreddits=subreddits, users=users)


@app.route('/api/deleted-files', methods=['GET'])
def api_deleted_files():
    """List files in the deleted folder with pagination."""
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    deleted_folder = ui_handler.download_folder / 'deleted'
    if not deleted_folder.exists():
        return jsonify({'success': True, 'files': [], 'total': 0, 'page': page, 'per_page': per_page})
    MEDIA_EXT = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.mp4', '.webm', '.mov', '.avi', '.mkv'}
    try:
        all_files = sorted(
            [f for f in deleted_folder.iterdir() if f.is_file() and f.suffix.lower() in MEDIA_EXT],
            key=lambda f: f.stat().st_mtime,
            reverse=True
        )
        total = len(all_files)
        start = (page - 1) * per_page
        page_files = all_files[start:start + per_page]
        result = []
        for f in page_files:
            stat = f.stat()
            rel = f.relative_to(ui_handler.download_folder)
            web_path = str(rel).replace('\\', '/')
            thumb_path = ui_handler.make_thumb_path(str(f))
            result.append({
                'filename': f.name,
                'web_path': web_path,
                'thumb_path': thumb_path,
                'file_size': stat.st_size,
                'mtime': stat.st_mtime,
                'ext': f.suffix.lower()
            })
        return jsonify({'success': True, 'files': result, 'total': total, 'page': page, 'per_page': per_page})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/deleted-files/<path:filename>', methods=['DELETE'])
def api_delete_file_permanently(filename):
    """Permanently delete a specific file from the deleted folder."""
    deleted_folder = ui_handler.download_folder / 'deleted'
    # Security: ensure the resolved path stays inside deleted_folder
    try:
        target = (deleted_folder / filename).resolve()
        if not str(target).startswith(str(deleted_folder.resolve())):
            return jsonify({'success': False, 'error': 'Invalid path'}), 400
        if not target.exists():
            return jsonify({'success': False, 'error': 'File not found'}), 404
        target.unlink()
        return jsonify({'success': True, 'message': f'{filename} permanently deleted'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/deleted-files', methods=['DELETE'])
def api_purge_deleted_folder():
    """Permanently delete ALL files in the deleted folder."""
    deleted_folder = ui_handler.download_folder / 'deleted'
    if not deleted_folder.exists():
        return jsonify({'success': True, 'deleted_count': 0})
    MEDIA_EXT = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.mp4', '.webm', '.mov', '.avi', '.mkv'}
    try:
        count = 0
        for f in list(deleted_folder.iterdir()):
            if f.is_file() and f.suffix.lower() in MEDIA_EXT:
                f.unlink()
                count += 1
        return jsonify({'success': True, 'deleted_count': count, 'message': f'{count} file(s) permanently deleted'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# COMPRESS IMAGES
# ═══════════════════════════════════════════════════════════════════════════

_compress_state: dict = {
    'running': False, 'message': '', 'progress': 0, 'total': 0,
    'error': None, 'logs': [], 'saved_bytes': 0,
}
_compress_lock = threading.Lock()
_compress_proc = None


def _run_compression(folder_name: str, min_size_kb: int, quality: int):
    """Background thread: run compress_images.py as a subprocess."""
    import sys as _sys

    script = Path(__file__).parent.parent.parent / 'compress_images.py'
    if not script.exists():
        script = Path.cwd() / 'compress_images.py'

    if folder_name:
        folder_path = str(Path.cwd() / 'reddit_downloads' / folder_name)
    else:
        folder_path = str(Path.cwd() / 'reddit_downloads')

    cmd = [
        _sys.executable, str(script),
        '--folder', folder_path,
        '--min-size-kb', str(min_size_kb),
        '--quality', str(quality),
        '--progress-json',
    ]

    global _compress_proc

    def _set_proc(p):
        global _compress_proc
        with _compress_lock:
            _compress_proc = p

    def _on_complete(state, lock):
        # Parse saved_bytes from the last JSON log line that has it
        with lock:
            for line in reversed(state['logs']):
                try:
                    ev = json.loads(line)
                    if 'saved_bytes' in ev:
                        state['saved_bytes'] = ev['saved_bytes']
                        break
                except Exception:
                    pass

    with _compress_lock:
        _compress_state['logs'] = []
        _compress_state['saved_bytes'] = 0

    _run_subprocess_with_state(cmd, _compress_state, _compress_lock, _set_proc, _on_complete)


@app.route('/compress')
def compress_page():
    """Image compression tool page."""
    stats = ui_handler.get_stats()
    subreddits = ui_handler.get_subreddits(only_enabled=False)
    users = ui_handler.get_users()
    return render_template('compress.html', stats=stats, subreddits=subreddits, users=users)


@app.route('/api/compress/start', methods=['POST'])
def api_compress_start():
    data = request.get_json() or {}
    folder_name  = data.get('folder') or None
    min_size_kb  = int(data.get('min_size_kb', 1024))
    quality      = int(data.get('quality', 85))
    with _compress_lock:
        if _compress_state['running']:
            return jsonify({'success': False, 'error': 'Compression already running'})
        _compress_state.update({
            'running': True, 'message': 'Starting…', 'progress': 0,
            'total': 0, 'error': None, 'logs': [], 'saved_bytes': 0,
        })
    threading.Thread(
        target=_run_compression,
        args=(folder_name, min_size_kb, quality),
        daemon=True,
    ).start()
    return jsonify({'success': True})


@app.route('/api/compress/status')
def api_compress_status():
    with _compress_lock:
        state = dict(_compress_state)
        state.pop('logs', None)
    return jsonify(state)


@app.route('/api/compress/logs')
def api_compress_logs():
    offset = int(request.args.get('offset', 0))
    with _compress_lock:
        logs = _compress_state['logs'][offset:]
    return jsonify({'logs': logs, 'offset': offset + len(logs)})


@app.route('/api/compress/stop', methods=['POST'])
def api_compress_stop():
    global _compress_proc
    with _compress_lock:
        proc = _compress_proc
    if proc:
        try:
            proc.terminate()
        except Exception:
            pass
    return jsonify({'success': True})


# ═══════════════════════════════════════════════════════════════════════════
# CLEANUP ORPHANS
# ═══════════════════════════════════════════════════════════════════════════

_cleanup_state: dict = {
    'running': False, 'message': '', 'progress': 0, 'total': 0,
    'error': None, 'logs': [], 'phase': '',
}
_cleanup_lock = threading.Lock()
_cleanup_proc = None


def _run_cleanup_job(dry_run: bool):
    """Background thread: run cleanup_orphans.py as a subprocess."""
    import sys as _sys

    script = Path(__file__).parent.parent.parent / 'cleanup_orphans.py'
    if not script.exists():
        script = Path.cwd() / 'cleanup_orphans.py'

    cmd = [_sys.executable, str(script), '--progress-json']
    if dry_run:
        cmd.append('--dry-run')

    global _cleanup_proc

    def _set_proc(p):
        global _cleanup_proc
        with _cleanup_lock:
            _cleanup_proc = p

    def _on_complete(state, lock):
        with lock:
            for line in reversed(state['logs']):
                try:
                    ev = json.loads(line)
                    if ev.get('message', '').startswith('Done in'):
                        state['message'] = ev['message']
                        break
                except Exception:
                    pass

    with _cleanup_lock:
        _cleanup_state['logs'] = []
        _cleanup_state['phase'] = ''

    _run_subprocess_with_state(cmd, _cleanup_state, _cleanup_lock, _set_proc, _on_complete)


@app.route('/cleanup')
def cleanup_page():
    """Orphan cleanup tool page."""
    stats = ui_handler.get_stats()
    subreddits = ui_handler.get_subreddits(only_enabled=False)
    users = ui_handler.get_users()
    return render_template('cleanup.html', stats=stats, subreddits=subreddits, users=users)


@app.route('/api/cleanup/start', methods=['POST'])
def api_cleanup_start():
    data    = request.get_json() or {}
    dry_run = bool(data.get('dry_run', False))
    with _cleanup_lock:
        if _cleanup_state['running']:
            return jsonify({'success': False, 'error': 'Cleanup already running'})
        _cleanup_state.update({
            'running': True, 'message': 'Starting…', 'progress': 0,
            'total': 0, 'error': None, 'logs': [], 'phase': '',
        })
    threading.Thread(target=_run_cleanup_job, args=(dry_run,), daemon=True).start()
    return jsonify({'success': True})


@app.route('/api/cleanup/status')
def api_cleanup_status():
    with _cleanup_lock:
        state = dict(_cleanup_state)
        state.pop('logs', None)
    return jsonify(state)


@app.route('/api/cleanup/logs')
def api_cleanup_logs():
    offset = int(request.args.get('offset', 0))
    with _cleanup_lock:
        logs = _cleanup_state['logs'][offset:]
    return jsonify({'logs': logs, 'offset': offset + len(logs)})


@app.route('/api/cleanup/stop', methods=['POST'])
def api_cleanup_stop():
    global _cleanup_proc
    with _cleanup_lock:
        proc = _cleanup_proc
    if proc:
        try:
            proc.terminate()
        except Exception:
            pass
    return jsonify({'success': True})


# ═══════════════════════════════════════════════════════════════════════════
# FLAIR BROWSER
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/flairs')
def flairs_page():
    stats = ui_handler.get_stats()
    subreddits = ui_handler.get_subreddits()
    users = ui_handler.get_users()
    return render_template('flairs.html', stats=stats, subreddits=subreddits, users=users)


@app.route('/api/flairs')
def api_flairs():
    """Return all distinct flairs with post/image counts, optionally filtered by subreddit."""
    subreddit = request.args.get('subreddit', '').strip() or None
    try:
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        if subreddit:
            cursor.execute("""
                SELECT p.flair,
                       COUNT(DISTINCT p.id)  AS post_count,
                       COUNT(pi.image_id)    AS image_count
                FROM posts p
                LEFT JOIN post_images pi ON pi.post_id = p.id
                WHERE p.flair IS NOT NULL AND p.flair != ''
                  AND p.subreddit = %s
                GROUP BY p.flair
                ORDER BY post_count DESC
            """, (subreddit,))
        else:
            cursor.execute("""
                SELECT p.flair,
                       COUNT(DISTINCT p.id)  AS post_count,
                       COUNT(pi.image_id)    AS image_count
                FROM posts p
                LEFT JOIN post_images pi ON pi.post_id = p.id
                WHERE p.flair IS NOT NULL AND p.flair != ''
                GROUP BY p.flair
                ORDER BY post_count DESC
            """)
        rows = cursor.fetchall()
        conn.close()
        return jsonify({'success': True, 'flairs': rows})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/flairs/posts')
def api_flair_posts():
    """Return paginated posts for a given flair."""
    flair   = request.args.get('flair', '').strip()
    subreddit = request.args.get('subreddit', '').strip() or None
    page    = max(1, int(request.args.get('page', 1)))
    per_page = min(100, max(12, int(request.args.get('per_page', 48))))
    if not flair:
        return jsonify({'success': False, 'error': 'flair required'}), 400
    offset = (page - 1) * per_page
    try:
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        params_count = [flair]
        params_rows  = [flair]
        sr_clause = ''
        if subreddit:
            sr_clause = ' AND p.subreddit = %s'
            params_count.append(subreddit)
            params_rows.append(subreddit)

        cursor.execute(
            f"SELECT COUNT(*) AS c FROM posts p WHERE p.flair = %s{sr_clause}",
            params_count,
        )
        total = cursor.fetchone()['c']

        params_rows += [per_page, offset]
        cursor.execute(f"""
            SELECT p.id AS post_id, p.title, p.author, p.subreddit, p.flair,
                   p.score, p.created_utc, p.permalink,
                   MIN(i.file_path) AS file_path,
                   COUNT(pi2.image_id) AS image_count
            FROM posts p
            LEFT JOIN post_images pi2 ON pi2.post_id = p.id
            LEFT JOIN images i ON i.id = pi2.image_id
            WHERE p.flair = %s{sr_clause}
            GROUP BY p.id, p.title, p.author, p.subreddit, p.flair, p.score, p.created_utc, p.permalink
            ORDER BY p.created_utc DESC
            LIMIT %s OFFSET %s
        """, params_rows)
        rows = cursor.fetchall()
        conn.close()

        result = []
        for row in rows:
            fp = row.get('file_path') or ''
            thumb = ui_handler.make_thumb_path(fp) if fp else None
            result.append({
                'post_id':     row['post_id'],
                'title':       row.get('title') or '',
                'author':      row.get('author') or '',
                'subreddit':   row.get('subreddit') or '',
                'flair':       row.get('flair') or '',
                'score':       row.get('score') or 0,
                'created_utc': row['created_utc'].isoformat() if row.get('created_utc') and hasattr(row['created_utc'], 'isoformat') else str(row.get('created_utc') or ''),
                'permalink':   row.get('permalink') or '',
                'image_count': row.get('image_count') or 0,
                'thumb':       thumb,
                'file_path':   fp,
            })
        return jsonify({'success': True, 'posts': result, 'total': total, 'page': page, 'per_page': per_page})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# EXPLICIT CONTENT DETECTION
# ═══════════════════════════════════════════════════════════════════════════

_explicit_state: dict = {
    'running': False, 'message': '', 'progress': 0, 'total': 0,
    'error': None, 'logs': [], 'flagged_count': 0, 'done': False,
}
_explicit_lock = threading.Lock()
_explicit_proc = None
_EXPLICIT_FLAGGED_FILE = Path.cwd() / 'explicit_flagged.json'


def _run_explicit_scan(folder_name: str, threshold: float):
    """Background thread: run detect_explicit.py as a subprocess."""
    import sys as _sys

    script = Path(__file__).parent.parent.parent / 'detect_explicit.py'
    if not script.exists():
        script = Path.cwd() / 'detect_explicit.py'

    cmd = [_sys.executable, str(script), '--progress-json',
           '--threshold', str(threshold)]
    if folder_name:
        cmd += ['--folder', folder_name]

    global _explicit_proc

    def _set_proc(p):
        global _explicit_proc
        with _explicit_lock:
            _explicit_proc = p

    def _on_complete(state, lock):
        with lock:
            for line in reversed(state['logs']):
                try:
                    ev = json.loads(line)
                    if ev.get('done'):
                        state['flagged_count'] = ev.get('flagged_count', 0)
                        break
                except Exception:
                    pass

    with _explicit_lock:
        _explicit_state['logs'] = []
        _explicit_state['flagged_count'] = 0
        _explicit_state['done'] = False

    _run_subprocess_with_state(cmd, _explicit_state, _explicit_lock, _set_proc, _on_complete)


@app.route('/explicit')
def explicit_page():
    flagged = []
    if _EXPLICIT_FLAGGED_FILE.exists():
        try:
            flagged = json.loads(_EXPLICIT_FLAGGED_FILE.read_text())
        except Exception:
            flagged = []
    stats = ui_handler.get_stats()
    subreddits = ui_handler.get_subreddits()
    users = ui_handler.get_users()
    return render_template('explicit.html', flagged=flagged,
                           stats=stats, subreddits=subreddits, users=users)


@app.route('/api/explicit/start', methods=['POST'])
def api_explicit_start():
    with _explicit_lock:
        if _explicit_state['running']:
            return jsonify({'success': False, 'error': 'Already running'}), 409
    data = request.get_json() or {}
    folder = data.get('folder', '')
    threshold = float(data.get('threshold', 0.5))
    t = threading.Thread(target=_run_explicit_scan, args=(folder, threshold), daemon=True)
    t.start()
    return jsonify({'success': True})


@app.route('/api/explicit/status')
def api_explicit_status():
    with _explicit_lock:
        state = dict(_explicit_state)
    state.pop('logs', None)
    return jsonify(state)


@app.route('/api/explicit/logs')
def api_explicit_logs():
    offset = int(request.args.get('offset', 0))
    with _explicit_lock:
        logs = _explicit_state['logs'][offset:]
        new_offset = len(_explicit_state['logs'])
    return jsonify({'logs': logs, 'offset': new_offset})


@app.route('/api/explicit/stop', methods=['POST'])
def api_explicit_stop():
    global _explicit_proc
    with _explicit_lock:
        proc = _explicit_proc
    if proc:
        try:
            proc.terminate()
        except Exception:
            pass
    return jsonify({'success': True})


@app.route('/api/explicit/results')
def api_explicit_results():
    """Return the current flagged results file."""
    if not _EXPLICIT_FLAGGED_FILE.exists():
        return jsonify({'success': True, 'flagged': []})
    try:
        flagged = json.loads(_EXPLICIT_FLAGGED_FILE.read_text())
        return jsonify({'success': True, 'flagged': flagged})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/explicit/delete', methods=['POST'])
def api_explicit_delete():
    """Move selected flagged files to the deleted folder."""
    data = request.get_json() or {}
    paths = data.get('paths', [])
    if not paths:
        return jsonify({'success': False, 'error': 'No paths provided'}), 400

    import shutil
    deleted_folder = ui_handler.download_folder / 'deleted'
    deleted_folder.mkdir(parents=True, exist_ok=True)

    moved = 0
    errors = []
    conn = None
    try:
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)

        for file_path_str in paths:
            try:
                source = Path(file_path_str)
                if not source.exists():
                    continue

                # Move file to deleted folder
                dest = deleted_folder / source.name
                counter = 1
                while dest.exists():
                    dest = deleted_folder / f"{source.stem}_{counter}{source.suffix}"
                    counter += 1
                shutil.move(str(source), str(dest))
                moved += 1

                # Remove DB record
                cursor.execute("SELECT id FROM images WHERE file_path = %s", (file_path_str,))
                img_row = cursor.fetchone()
                if img_row:
                    image_id = img_row['id']
                    cursor.execute(
                        "SELECT post_id FROM post_images WHERE image_id = %s", (image_id,)
                    )
                    post_ids = [r['post_id'] for r in cursor.fetchall()]
                    cursor.execute("DELETE FROM images WHERE id = %s", (image_id,))
                    for pid in post_ids:
                        cursor.execute(
                            "SELECT COUNT(*) as c FROM post_images WHERE post_id = %s", (pid,)
                        )
                        if cursor.fetchone()['c'] == 0:
                            cursor.execute("DELETE FROM posts WHERE id = %s", (pid,))

                # Delete thumbnail
                try:
                    thumb = ui_handler.make_thumb_path(file_path_str)
                    if thumb:
                        tp = Path(thumb.lstrip('/'))
                        if not tp.is_absolute():
                            tp = Path.cwd() / tp
                        if tp.exists():
                            tp.unlink()
                except Exception:
                    pass

            except Exception as e:
                errors.append(f'{Path(file_path_str).name}: {e}')

        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

    # Remove deleted paths from the flagged file
    if _EXPLICIT_FLAGGED_FILE.exists():
        try:
            flagged = json.loads(_EXPLICIT_FLAGGED_FILE.read_text())
            paths_set = set(paths)
            flagged = [f for f in flagged if f['file_path'] not in paths_set]
            _EXPLICIT_FLAGGED_FILE.write_text(json.dumps(flagged, indent=2))
        except Exception:
            pass

    return jsonify({'success': True, 'moved': moved, 'errors': errors})


@app.route('/api/explicit/dismiss', methods=['POST'])
def api_explicit_dismiss():
    """Remove paths from the flagged list without deleting files (false positives)."""
    data = request.get_json() or {}
    paths = data.get('paths', [])
    if not _EXPLICIT_FLAGGED_FILE.exists():
        return jsonify({'success': True})
    try:
        flagged = json.loads(_EXPLICIT_FLAGGED_FILE.read_text())
        paths_set = set(paths)
        flagged = [f for f in flagged if f['file_path'] not in paths_set]
        _EXPLICIT_FLAGGED_FILE.write_text(json.dumps(flagged, indent=2))
        return jsonify({'success': True, 'remaining': len(flagged)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# UPDATE POSTS (comments + deletion status)
# ═══════════════════════════════════════════════════════════════════════════

_update_posts_state: dict = {
    'running': False, 'message': '', 'progress': 0, 'total': 0,
    'error': None, 'logs': [], 'updated': 0, 'deleted': 0, 'errors': 0,
}
_update_posts_lock = threading.Lock()
_update_posts_proc = None


def _run_update_posts_job(mode: str):
    import sys as _sys
    script = Path(__file__).parent / 'utils' / 'update_comments_batch.py'
    cmd = [_sys.executable, str(script),
           '--mode', mode, '--progress-json', '--skip-comments', '--config', 'config.ini']

    global _update_posts_proc

    def _set_proc(p):
        global _update_posts_proc
        with _update_posts_lock:
            _update_posts_proc = p

    def _on_complete(state, lock):
        with lock:
            for line in reversed(state['logs']):
                try:
                    ev = json.loads(line)
                    if 'updated' in ev:
                        state['updated']  = ev['updated']
                        state['deleted']  = ev.get('deleted', 0)
                        state['errors']   = ev.get('errors', 0)
                        break
                except Exception:
                    pass

    with _update_posts_lock:
        _update_posts_state.update({
            'running': True, 'message': 'Starting…', 'progress': 0, 'total': 0,
            'error': None, 'logs': [], 'updated': 0, 'deleted': 0, 'errors': 0,
        })

    _run_subprocess_with_state(cmd, _update_posts_state, _update_posts_lock,
                               _set_proc, _on_complete)

    with _update_posts_lock:
        _update_posts_state['running'] = False


@app.route('/update-posts')
def update_posts_page():
    return render_template('update_posts.html')


@app.route('/api/update-posts/start', methods=['POST'])
def api_update_posts_start():
    data = request.get_json() or {}
    mode = data.get('mode', 'weekly')
    if mode not in ('weekly', 'monthly', 'full'):
        return jsonify({'error': 'Invalid mode'}), 400
    with _update_posts_lock:
        if _update_posts_state['running']:
            return jsonify({'error': 'Already running'}), 400
    threading.Thread(target=_run_update_posts_job, args=(mode,), daemon=True).start()
    return jsonify({'success': True, 'mode': mode})


@app.route('/api/update-posts/status')
def api_update_posts_status():
    with _update_posts_lock:
        state = dict(_update_posts_state)
        state.pop('logs', None)
    return jsonify(state)


@app.route('/api/update-posts/logs')
def api_update_posts_logs():
    offset = int(request.args.get('offset', 0))
    with _update_posts_lock:
        logs = _update_posts_state['logs'][offset:]
    return jsonify({'logs': logs, 'offset': offset + len(logs)})


@app.route('/api/update-posts/stop', methods=['POST'])
def api_update_posts_stop():
    global _update_posts_proc
    with _update_posts_lock:
        proc = _update_posts_proc
    if proc:
        try:
            proc.terminate()
        except Exception:
            pass
    return jsonify({'success': True})


# ── Reddit-deleted posts page ──────────────────────────────────────────────

@app.route('/reddit-deleted')
def reddit_deleted_page():
    page     = int(request.args.get('page', 1))
    per_page = 50
    offset   = (page - 1) * per_page
    subreddit = request.args.get('subreddit', '')
    search    = request.args.get('search', '')

    try:
        conn   = _get_db_connection()
        cursor = conn.cursor(dictionary=True)

        where_parts = ["p.is_deleted = 1"]
        params = []
        if subreddit:
            where_parts.append("p.subreddit = %s")
            params.append(subreddit)
        if search:
            where_parts.append("(p.title LIKE %s OR p.author LIKE %s)")
            params.extend([f'%{search}%', f'%{search}%'])
        where = ' AND '.join(where_parts)

        cursor.execute(f"SELECT COUNT(*) AS total FROM posts p WHERE {where}", params)
        total = cursor.fetchone()['total']

        cursor.execute(f"""
            SELECT p.id AS post_id, p.title, p.author, p.subreddit,
                   p.permalink, p.created_utc, p.score, p.flair,
                   i.id AS image_id, i.file_path, i.filename, i.file_size
            FROM (
                SELECT id FROM posts p WHERE {where}
                ORDER BY created_utc DESC LIMIT %s OFFSET %s
            ) paged
            JOIN posts p ON p.id = paged.id
            LEFT JOIN post_images pi ON pi.post_id = p.id
            LEFT JOIN images i ON i.id = pi.image_id
            ORDER BY p.created_utc DESC, i.id ASC
        """, params + [per_page, offset])
        rows = cursor.fetchall()

        # Subreddits for filter dropdown
        cursor.execute(
            "SELECT DISTINCT subreddit FROM posts WHERE is_deleted=1 "
            "ORDER BY subreddit LIMIT 200"
        )
        subreddits = [r['subreddit'] for r in cursor.fetchall() if r['subreddit']]
        conn.close()

        # Group into posts
        posts = {}
        for row in rows:
            pid = row['post_id']
            if pid not in posts:
                posts[pid] = {
                    'post_id': pid, 'title': row['title'], 'author': row['author'],
                    'subreddit': row['subreddit'], 'permalink': row['permalink'],
                    'created_utc': row['created_utc'], 'score': row['score'],
                    'flair': row.get('flair'), 'post_images': [],
                }
            if row.get('image_id'):
                img = {'id': row['image_id'], 'file_path': row['file_path'],
                       'filename': row['filename'], 'file_size': row['file_size']}
                if img['file_path']:
                    wp = ui_handler.make_web_path(img['file_path'])
                    if wp:
                        img['web_path'] = wp
                        img['thumb_url'] = '/thumbs/' + str(Path(wp).with_suffix('.jpg')).replace('\\', '/')
                posts[pid]['post_images'].append(img)

        post_list = list(posts.values())
        for p in post_list:
            if p['post_images']:
                first = p['post_images'][0]
                p['web_path']   = first.get('web_path')
                p['thumb_url']  = first.get('thumb_url')
                p['filename']   = first.get('filename')

        total_pages = max(1, (total + per_page - 1) // per_page)
        return render_template('reddit_deleted.html',
                               posts=post_list, total=total,
                               current_page=page, total_pages=total_pages,
                               subreddits=subreddits,
                               filter_subreddit=subreddit, search=search)
    except Exception as e:
        print(f"Reddit deleted page error: {e}")
        return render_template('reddit_deleted.html',
                               posts=[], total=0, current_page=1, total_pages=1,
                               subreddits=[], filter_subreddit='', search='')


@app.route('/api/posts/<int:post_id>/restore', methods=['POST'])
def restore_post(post_id):
    try:
        conn   = _get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE posts SET is_deleted = 0 WHERE id = %s", [post_id])
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


_IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.avif', '.tiff'}
_VIDEO_EXTS = {'.mp4', '.webm', '.mov', '.avi', '.mkv', '.gifv'}
_COMPRESSIBLE_EXTS = {'.jpg', '.jpeg', '.png', '.webp', '.bmp', '.tiff', '.avif'}

_batch_compress_state: dict = {'running': False, 'total': 0, 'done': 0, 'errors': 0,
                                'saved_bytes': 0, 'message': ''}
_batch_compress_lock = threading.Lock()

# ── Per-file video compression ─────────────────────────────────────────────

_video_compress_jobs: dict = {}   # image_id -> {running, done, error, web_path, saved_bytes}
_video_compress_lock = threading.Lock()


def _run_video_compression(image_id: int, crf: int):
    import subprocess

    with _video_compress_lock:
        _video_compress_jobs[image_id] = {
            'running': True, 'done': False, 'error': None,
            'web_path': None, 'saved_bytes': 0
        }

    try:
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT file_path, file_size FROM images WHERE id = %s", [image_id])
        row = cursor.fetchone()
        conn.close()

        if not row or not row['file_path']:
            raise ValueError("File not found in DB")

        file_path = Path(row['file_path'])
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        if file_path.suffix.lower() not in _VIDEO_EXTS:
            raise ValueError(f"Not a video: {file_path.suffix}")

        original_size = file_path.stat().st_size
        tmp_path = file_path.with_suffix('.compressed_tmp.mp4')
        out_path = file_path.with_suffix('.mp4')

        cmd = [
            'ffmpeg', '-y', '-i', str(file_path),
            '-c:v', 'libx264', '-crf', str(crf),
            '-preset', 'medium',
            '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart',
            str(tmp_path)
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=600)
        if result.returncode != 0:
            tmp_path.unlink(missing_ok=True)
            raise RuntimeError(f"ffmpeg failed: {result.stderr.decode()[-500:]}")

        if file_path != out_path:
            file_path.unlink(missing_ok=True)
        tmp_path.rename(out_path)

        new_size = out_path.stat().st_size
        saved = max(0, original_size - new_size)

        conn = _get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE images SET file_size = %s, file_path = %s WHERE id = %s",
            [new_size, str(out_path), image_id]
        )
        conn.commit()
        conn.close()

        wp = ui_handler.make_web_path(str(out_path))
        with _video_compress_lock:
            _video_compress_jobs[image_id].update({
                'running': False, 'done': True,
                'web_path': wp, 'saved_bytes': saved
            })

    except Exception as e:
        print(f"Video compress error for {image_id}: {e}")
        with _video_compress_lock:
            _video_compress_jobs[image_id].update({
                'running': False, 'done': True, 'error': str(e)
            })


@app.route('/api/files/compress-video/<int:image_id>', methods=['POST'])
def compress_video_file(image_id):
    data = request.get_json() or {}
    crf = max(18, min(51, int(data.get('crf', 28))))
    with _video_compress_lock:
        job = _video_compress_jobs.get(image_id)
        if job and job.get('running'):
            return jsonify({'error': 'Already compressing'}), 400
    threading.Thread(target=_run_video_compression, args=(image_id, crf), daemon=True).start()
    return jsonify({'success': True})


@app.route('/api/files/compress-video/<int:image_id>/status')
def compress_video_status(image_id):
    with _video_compress_lock:
        job = _video_compress_jobs.get(image_id)
    if not job:
        return jsonify({'running': False, 'done': False})
    return jsonify(job)


def _run_batch_compression(image_ids: list, quality: int):
    from PIL import Image as PILImage

    with _batch_compress_lock:
        _batch_compress_state.update({
            'running': True, 'total': len(image_ids), 'done': 0,
            'errors': 0, 'saved_bytes': 0, 'message': 'Starting…'
        })

    saved_total = 0
    errors = 0

    for i, image_id in enumerate(image_ids):
        try:
            conn = _get_db_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT file_path, file_size FROM images WHERE id = %s", [image_id])
            row = cursor.fetchone()
            conn.close()

            if not row or not row['file_path']:
                errors += 1
                continue

            file_path = Path(row['file_path'])
            if not file_path.exists():
                errors += 1
                continue

            ext = file_path.suffix.lower()
            if ext not in _COMPRESSIBLE_EXTS:
                continue  # skip videos / unsupported

            original_size = file_path.stat().st_size

            with PILImage.open(file_path) as img:
                if img.mode in ('RGBA', 'LA', 'P'):
                    bg = PILImage.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'P':
                        img = img.convert('RGBA')
                    mask = img.split()[-1] if img.mode in ('RGBA', 'LA') else None
                    bg.paste(img, mask=mask)
                    img = bg
                elif img.mode != 'RGB':
                    img = img.convert('RGB')
                img.save(file_path.with_suffix('.jpg'), 'JPEG', quality=quality, optimize=True)

            new_path = file_path.with_suffix('.jpg')
            if ext not in {'.jpg', '.jpeg'}:
                file_path.unlink(missing_ok=True)
            file_path = new_path

            new_size = file_path.stat().st_size
            saved = max(0, original_size - new_size)
            saved_total += saved

            conn = _get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE images SET file_size = %s, file_path = %s WHERE id = %s",
                [new_size, str(file_path), image_id]
            )
            conn.commit()
            conn.close()

        except Exception as e:
            print(f"Batch compress error on {image_id}: {e}")
            errors += 1

        with _batch_compress_lock:
            _batch_compress_state.update({
                'done': i + 1, 'errors': errors, 'saved_bytes': saved_total,
                'message': f'Compressing {i + 1}/{len(image_ids)}…'
            })

    with _batch_compress_lock:
        saved_mb = round(saved_total / 1048576, 1)
        _batch_compress_state.update({
            'running': False,
            'message': f'Done — saved {saved_mb} MB across {len(image_ids) - errors} files.'
        })


@app.route('/api/files/compress-batch', methods=['POST'])
def compress_batch_files():
    data = request.get_json() or {}
    image_ids = [int(x) for x in data.get('image_ids', [])]
    quality = max(10, min(85, int(data.get('quality', 35))))
    if not image_ids:
        return jsonify({'error': 'No images selected'}), 400
    with _batch_compress_lock:
        if _batch_compress_state['running']:
            return jsonify({'error': 'Compression already running'}), 400
    threading.Thread(target=_run_batch_compression, args=(image_ids, quality), daemon=True).start()
    return jsonify({'success': True, 'total': len(image_ids)})


@app.route('/api/files/compress-batch/status')
def compress_batch_status():
    with _batch_compress_lock:
        return jsonify(dict(_batch_compress_state))


@app.route('/files')
def file_browser():
    """Browse all downloaded files with filter by type and sort by size."""
    file_type = request.args.get('type', 'all')   # all | image | video
    sort      = request.args.get('sort', 'size_desc')
    page      = int(request.args.get('page', 1))
    per_page  = 10
    offset    = (page - 1) * per_page

    sort_map = {
        'size_desc': 'i.file_size DESC',
        'size_asc':  'i.file_size ASC',
        'name_asc':  'i.filename ASC',
        'name_desc': 'i.filename DESC',
        'date_desc': 'i.download_date DESC, i.download_time DESC',
        'date_asc':  'i.download_date ASC, i.download_time ASC',
    }
    order_by = sort_map.get(sort, 'i.file_size DESC')

    if file_type == 'image':
        type_clause = ("AND (LOWER(i.filename) LIKE '%.jpg' OR LOWER(i.filename) LIKE '%.jpeg'"
                       " OR LOWER(i.filename) LIKE '%.png' OR LOWER(i.filename) LIKE '%.gif'"
                       " OR LOWER(i.filename) LIKE '%.webp' OR LOWER(i.filename) LIKE '%.bmp'"
                       " OR LOWER(i.filename) LIKE '%.avif' OR LOWER(i.filename) LIKE '%.tiff')")
    elif file_type == 'video':
        type_clause = ("AND (LOWER(i.filename) LIKE '%.mp4' OR LOWER(i.filename) LIKE '%.webm'"
                       " OR LOWER(i.filename) LIKE '%.mov' OR LOWER(i.filename) LIKE '%.avi'"
                       " OR LOWER(i.filename) LIKE '%.mkv' OR LOWER(i.filename) LIKE '%.gifv')")
    else:
        type_clause = ""

    try:
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute(f"""
            SELECT COUNT(*) as total FROM images i
            WHERE (i.is_deleted = 0 OR i.is_deleted IS NULL) {type_clause}
        """)
        total = cursor.fetchone()['total']

        cursor.execute(f"""
            SELECT i.id, i.filename, i.file_path, i.file_size,
                   i.download_date, i.file_hash, i.is_favourite,
                   (SELECT p2.id FROM post_images pi2
                    JOIN posts p2 ON p2.id = pi2.post_id
                    WHERE pi2.image_id = i.id LIMIT 1) AS post_id,
                   (SELECT p2.title FROM post_images pi2
                    JOIN posts p2 ON p2.id = pi2.post_id
                    WHERE pi2.image_id = i.id LIMIT 1) AS title,
                   (SELECT COUNT(*) FROM post_images pi2
                    WHERE pi2.image_id = i.id) AS post_count
            FROM images i
            WHERE (i.is_deleted = 0 OR i.is_deleted IS NULL) {type_clause}
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
        """, [per_page, offset])
        files = cursor.fetchall()
        conn.close()

        for f in files:
            ext = Path(f['filename']).suffix.lower() if f.get('filename') else ''
            f['is_video'] = ext in _VIDEO_EXTS
            f['is_image'] = ext in _IMAGE_EXTS
            if f.get('file_path'):
                wp = ui_handler.make_web_path(f['file_path'])
                if wp:
                    f['web_path'] = wp
                    # Always emit a thumb_url; browser onerror will fall back to full image
                    f['thumb_url'] = '/thumbs/' + str(Path(wp).with_suffix('.jpg')).replace('\\', '/')

        total_pages = max(1, (total + per_page - 1) // per_page)
        return render_template('files.html',
                               files=files, file_type=file_type, sort=sort,
                               current_page=page, total_pages=total_pages, total=total)
    except Exception as e:
        print(f"File browser error: {e}")
        return render_template('files.html', files=[], file_type=file_type, sort=sort,
                               current_page=1, total_pages=1, total=0)


@app.route('/favourites')
def favourites():
    """Posts that have at least one favourited image."""
    page     = int(request.args.get('page', 1))
    per_page = 100
    offset   = (page - 1) * per_page

    try:
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT COUNT(DISTINCT p.id) AS total
            FROM posts p
            JOIN post_images pi ON pi.post_id = p.id
            JOIN images i ON i.id = pi.image_id
            WHERE i.is_favourite = 1
        """)
        total = cursor.fetchone()['total']

        cursor.execute("""
            SELECT p.id AS post_id, p.title, p.author, p.subreddit,
                   p.permalink, p.created_utc, p.score, p.flair,
                   i.id AS image_id, i.file_path, i.filename, i.file_size,
                   i.is_favourite, pi.url
            FROM (
                SELECT DISTINCT p2.id FROM posts p2
                JOIN post_images pi2 ON pi2.post_id = p2.id
                JOIN images i2 ON i2.id = pi2.image_id
                WHERE i2.is_favourite = 1
                ORDER BY p2.created_utc DESC
                LIMIT %s OFFSET %s
            ) paged
            JOIN posts p ON p.id = paged.id
            JOIN post_images pi ON pi.post_id = p.id
            JOIN images i ON i.id = pi.image_id
            WHERE i.is_favourite = 1
            ORDER BY p.created_utc DESC, i.id ASC
        """, [per_page, offset])
        rows = cursor.fetchall()
        conn.close()

        posts = {}
        for row in rows:
            pid = row['post_id']
            if pid not in posts:
                posts[pid] = {
                    'post_id': pid, 'title': row['title'], 'author': row['author'],
                    'subreddit': row['subreddit'], 'permalink': row['permalink'],
                    'created_utc': row['created_utc'], 'score': row['score'],
                    'flair': row.get('flair'), 'post_images': []
                }
            img = {
                'id': row['image_id'], 'file_path': row['file_path'],
                'filename': row['filename'], 'file_size': row['file_size'],
                'is_favourite': row['is_favourite'], 'url': row['url']
            }
            if img['file_path']:
                wp = ui_handler.make_web_path(img['file_path'])
                if wp:
                    img['web_path'] = wp
                tp = ui_handler.make_thumb_path(img['file_path'])
                if tp:
                    img['thumb_path'] = tp
            posts[pid]['post_images'].append(img)

        post_list = list(posts.values())
        for p in post_list:
            if p['post_images']:
                first = p['post_images'][0]
                p['web_path']   = first.get('web_path')
                p['thumb_path'] = first.get('thumb_path')
                p['filename']   = first.get('filename')

        total_pages = max(1, (total + per_page - 1) // per_page)
        return render_template('favourites.html',
                               posts=post_list, current_page=page,
                               total_pages=total_pages, total=total)
    except Exception as e:
        print(f"Favourites error: {e}")
        return render_template('favourites.html', posts=[],
                               current_page=1, total_pages=1, total=0)


@app.route('/api/favourite/<int:image_id>', methods=['POST'])
def toggle_favourite(image_id):
    """Toggle is_favourite flag for an image."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT is_favourite FROM images WHERE id = %s", [image_id])
        row = cursor.fetchone()
        if not row:
            conn.close()
            return jsonify({'error': 'Image not found'}), 404
        new_val = 0 if row[0] else 1
        cursor.execute("UPDATE images SET is_favourite = %s WHERE id = %s", [new_val, image_id])
        conn.commit()
        conn.close()
        return jsonify({'is_favourite': bool(new_val)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/files/delete/<int:image_id>', methods=['DELETE'])
def delete_file_physical(image_id):
    """Delete file from disk, mark as deleted, and delete all linked posts."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT file_path FROM images WHERE id = %s", [image_id])
        row = cursor.fetchone()
        if not row:
            conn.close()
            return jsonify({'error': 'Image not found'}), 404

        # Delete physical file
        file_path = row['file_path']
        if file_path:
            try:
                p = Path(file_path)
                if p.exists():
                    p.unlink()
            except Exception:
                pass

        # Find all posts linked to this image
        cursor.execute(
            "SELECT post_id FROM post_images WHERE image_id = %s", [image_id]
        )
        post_ids = [r['post_id'] for r in cursor.fetchall()]

        # Delete linked posts (post_images cascade via FK)
        if post_ids:
            fmt = ','.join(['%s'] * len(post_ids))
            cursor.execute(f"DELETE FROM posts WHERE id IN ({fmt})", post_ids)

        # Mark image as deleted
        cursor.execute("UPDATE images SET is_deleted = 1 WHERE id = %s", [image_id])
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'posts_deleted': len(post_ids)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def main():
    """Main entry point for the web UI."""
    app.run(debug=True, host='0.0.0.0', port=4000)

# ═══════════════════════════════════════════════════════════════════════════
# DUPLICATES
# ═══════════════════════════════════════════════════════════════════════════

_DUPES_DB = Path.cwd() / 'duplicates.db'

_DUPES_SCHEMA = """
CREATE TABLE IF NOT EXISTS scan_info (
    id INTEGER PRIMARY KEY, scanned_at TEXT, scan_duration_sec REAL,
    total_files_scanned INTEGER, total_groups INTEGER, total_wasted_bytes INTEGER,
    threshold INTEGER, hash_size INTEGER, is_partial INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS folder_scan_info (
    folder TEXT PRIMARY KEY, scanned_at TEXT, scan_duration_sec REAL,
    total_files_scanned INTEGER, total_groups INTEGER, total_wasted_bytes INTEGER,
    threshold INTEGER, hash_size INTEGER, is_partial INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS dup_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT, file_hash TEXT NOT NULL,
    file_count INTEGER NOT NULL, total_size INTEGER NOT NULL, wasted_size INTEGER NOT NULL,
    min_distance INTEGER NOT NULL DEFAULT 0, folder TEXT
);
CREATE TABLE IF NOT EXISTS dup_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER NOT NULL,
    file_path TEXT NOT NULL, file_size INTEGER NOT NULL,
    phash TEXT,
    image_id INTEGER, post_id INTEGER, reddit_id TEXT, post_title TEXT,
    subreddit TEXT, permalink TEXT, score INTEGER, is_deleted INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (group_id) REFERENCES dup_groups(id)
);
CREATE INDEX IF NOT EXISTS idx_df_group   ON dup_files(group_id);
CREATE INDEX IF NOT EXISTS idx_df_deleted ON dup_files(group_id, is_deleted);
CREATE INDEX IF NOT EXISTS idx_df_imgid   ON dup_files(image_id);
CREATE TABLE IF NOT EXISTS phash_cache (
    path  TEXT    PRIMARY KEY,
    mtime REAL    NOT NULL,
    size  INTEGER NOT NULL,
    phash TEXT
);
"""

_MEDIA_EXT = {'.jpg','.jpeg','.png','.gif','.bmp','.webp','.mp4','.webm','.mov','.avi','.mkv'}

_scan_state: dict = {'running': False, 'folder': None, 'message': '', 'progress': 0, 'total': 0, 'error': None, 'logs': [], 'partial': False}
_scan_lock = threading.Lock()
_scan_proc = None  # subprocess.Popen reference for stop support

_hash_state: dict = {'running': False, 'folder': None, 'message': '', 'progress': 0, 'total': 0, 'error': None, 'logs': []}
_hash_lock = threading.Lock()
_hash_proc = None


def _get_dupes_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DUPES_DB), timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')   # allow concurrent reads during writes
    conn.executescript(_DUPES_SCHEMA)
    conn.commit()
    # Migrate dup_groups: add folder column and remove UNIQUE constraint
    try:
        cols = [row[1] for row in conn.execute('PRAGMA table_info(dup_groups)').fetchall()]
        if 'folder' not in cols:
            conn.executescript("""
                BEGIN;
                CREATE TABLE dup_groups_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, file_hash TEXT NOT NULL,
                    file_count INTEGER NOT NULL, total_size INTEGER NOT NULL,
                    wasted_size INTEGER NOT NULL, min_distance INTEGER NOT NULL DEFAULT 0,
                    folder TEXT
                );
                INSERT INTO dup_groups_new
                    SELECT id, file_hash, file_count, total_size, wasted_size, min_distance, NULL
                    FROM dup_groups;
                DROP TABLE dup_groups;
                ALTER TABLE dup_groups_new RENAME TO dup_groups;
                COMMIT;
            """)
    except Exception:
        pass
    return conn


def _format_bytes(n: int) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if n < 1024:
            return f'{n:.1f} {unit}'
        n /= 1024
    return f'{n:.1f} TB'


def _file_to_url(file_path: str) -> str | None:
    """Convert an absolute file path to a web-accessible URL path."""
    return ui_handler.make_web_path(file_path)


def _file_to_thumb(file_path: str) -> str | None:
    return ui_handler.make_thumb_path(file_path)


def _run_subprocess_with_state(cmd, state, lock, proc_ref_setter, on_complete_db_query=None):
    """Shared helper: runs a subprocess, streams JSON progress into `state`."""
    import subprocess as _sp
    import json as _json
    import threading as _threading

    try:
        with lock:
            state['logs'] = []

        proc = _sp.Popen(cmd, stdout=_sp.PIPE, stderr=_sp.PIPE,
                         text=True, bufsize=1, cwd=str(Path.cwd()))
        proc_ref_setter(proc)

        def _drain_stderr():
            for line in proc.stderr:
                line = line.rstrip()
                if line:
                    with lock:
                        state['logs'].append('[stderr] ' + line)

        _threading.Thread(target=_drain_stderr, daemon=True).start()

        last_msg = 'Starting…'
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            with lock:
                state['logs'].append(line)
            try:
                ev = _json.loads(line)
                msg = ev.get('message', last_msg)
                cur = ev.get('progress', 0)
                tot = ev.get('total', 0)
                last_msg = msg
                with lock:
                    state['message']  = msg
                    state['progress'] = cur
                    state['total']    = tot
            except _json.JSONDecodeError:
                pass

        proc.wait()
        proc_ref_setter(None)

        if proc.returncode not in (0, -15):
            with lock:
                state['error'] = f'Process exited with code {proc.returncode} — check logs'
            return False

        if on_complete_db_query:
            on_complete_db_query(state, lock)

        return True

    except Exception as exc:
        with lock:
            state['error'] = str(exc)
        proc_ref_setter(None)
        return False
    finally:
        with lock:
            state['running'] = False
        proc_ref_setter(None)


def _run_hash_computation(folder_name: str):
    """Background thread: compute hashes for one folder via compute_hashes.py."""
    import sys as _sys

    script = Path(__file__).parent.parent.parent / 'compute_hashes.py'
    if not script.exists():
        script = Path.cwd() / 'compute_hashes.py'

    folder_path = str(Path.cwd() / 'reddit_downloads' / folder_name)

    cmd = [_sys.executable, str(script),
           '--folder', folder_path,
           '--progress-json']

    global _hash_proc

    def _set_proc(p):
        global _hash_proc
        with _hash_lock:
            _hash_proc = p

    def _on_complete(state, lock):
        with lock:
            logs = state['logs']
            # Extract final "Done. N hashes computed" message
            for l in reversed(logs):
                if 'Done.' in l or 'up to date' in l:
                    state['message'] = l
                    break

    with _hash_lock:
        _hash_state['logs'] = []

    _run_subprocess_with_state(cmd, _hash_state, _hash_lock, _set_proc, _on_complete)


def _run_duplicate_scan(threshold: int = 10, hash_size: int = 8, folder_name: str = None):
    """Background thread: delegates to scan_duplicates.py via subprocess."""
    import sys as _sys

    script = Path(__file__).parent.parent.parent / 'scan_duplicates.py'
    if not script.exists():
        script = Path.cwd() / 'scan_duplicates.py'

    cmd = [_sys.executable, str(script),
           '--threshold', str(threshold),
           '--hash-size', str(hash_size),
           '--progress-json']

    if folder_name:
        folder_path = str(Path.cwd() / 'reddit_downloads' / folder_name)
        cmd += ['--folder', folder_path, '--use-cache-only']

    global _scan_proc

    def _set_proc(p):
        global _scan_proc
        with _scan_lock:
            _scan_proc = p

    def _on_complete(state, lock):
        try:
            sdb = _get_dupes_db()
            if folder_name:
                row = sdb.execute(
                    'SELECT * FROM folder_scan_info WHERE folder = ?', (folder_name,)
                ).fetchone()
            else:
                row = sdb.execute('SELECT * FROM scan_info WHERE id = 1').fetchone()
            sdb.close()
            if row:
                wasted  = row['total_wasted_bytes'] or 0
                groups  = row['total_groups'] or 0
                elapsed = row['scan_duration_sec'] or 0
                with lock:
                    state['message']  = (
                        f'Done in {elapsed:.1f}s — {groups:,} groups, '
                        f'{_format_bytes(wasted)} wasted'
                    )
                    state['progress'] = row['total_files_scanned'] or 0
                    state['total']    = row['total_files_scanned'] or 0
        except Exception:
            pass

        with lock:
            logs  = state['logs']
            is_partial = any('partial' in l.lower() for l in logs[-5:])
            state['partial'] = is_partial
        if is_partial:
            try:
                sdb2 = _get_dupes_db()
                if folder_name:
                    sdb2.execute(
                        'UPDATE folder_scan_info SET is_partial=1 WHERE folder=?', (folder_name,)
                    )
                else:
                    sdb2.execute('UPDATE scan_info SET is_partial=1 WHERE id=1')
                sdb2.commit()
                sdb2.close()
            except Exception:
                pass

    with _scan_lock:
        _scan_state['logs']    = []
        _scan_state['partial'] = False
        _scan_state['folder']  = folder_name

    _run_subprocess_with_state(cmd, _scan_state, _scan_lock, _set_proc, _on_complete)


# ── Routes ────────────────────────────────────────────────────────────────

@app.route('/duplicates')
def duplicates_page():
    stats = ui_handler.get_stats()
    return render_template('duplicates.html', stats=stats)


@app.route('/api/duplicates/scan', methods=['POST'])
def api_start_duplicate_scan():
    data        = request.get_json() or {}
    threshold   = int(data.get('threshold', 10))
    hash_size   = int(data.get('hash_size', 8))
    folder_name = data.get('folder') or None
    with _scan_lock:
        if _scan_state['running']:
            return jsonify({'success': False, 'error': 'Scan already running'})
        _scan_state.update({'running': True, 'folder': folder_name,
                            'message': 'Starting…', 'progress': 0,
                            'total': 0, 'error': None, 'logs': [], 'partial': False})
    threading.Thread(target=_run_duplicate_scan,
                     args=(threshold, hash_size, folder_name),
                     daemon=True).start()
    return jsonify({'success': True})


@app.route('/api/duplicates/scan/status')
def api_duplicate_scan_status():
    with _scan_lock:
        state = dict(_scan_state)
        state.pop('logs', None)
        return jsonify(state)


@app.route('/api/duplicates/scan/stop', methods=['POST'])
def api_stop_duplicate_scan():
    global _scan_proc
    with _scan_lock:
        if not _scan_state['running']:
            return jsonify({'success': False, 'error': 'No scan running'})
        proc = _scan_proc
    if proc:
        try:
            proc.terminate()
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})
    return jsonify({'success': True})


@app.route('/api/duplicates/scan/logs')
def api_duplicate_scan_logs():
    offset = int(request.args.get('offset', 0))
    with _scan_lock:
        logs = _scan_state['logs']
        return jsonify({'lines': logs[offset:], 'total': len(logs)})


# ── Hash computation endpoints ─────────────────────────────────────────────

@app.route('/api/duplicates/hash', methods=['POST'])
def api_start_hash_computation():
    data        = request.get_json() or {}
    folder_name = data.get('folder')
    if not folder_name:
        return jsonify({'success': False, 'error': 'folder is required'}), 400
    folder_path = Path.cwd() / 'reddit_downloads' / folder_name
    if not folder_path.exists():
        return jsonify({'success': False, 'error': f'Folder not found: {folder_name}'}), 404
    with _hash_lock:
        if _hash_state['running']:
            return jsonify({'success': False, 'error': 'Hash computation already running'})
        _hash_state.update({'running': True, 'folder': folder_name,
                            'message': 'Starting…', 'progress': 0,
                            'total': 0, 'error': None, 'logs': []})
    threading.Thread(target=_run_hash_computation, args=(folder_name,), daemon=True).start()
    return jsonify({'success': True})


@app.route('/api/duplicates/hash/status')
def api_hash_status():
    with _hash_lock:
        state = dict(_hash_state)
        state.pop('logs', None)
        return jsonify(state)


@app.route('/api/duplicates/hash/stop', methods=['POST'])
def api_hash_stop():
    global _hash_proc
    with _hash_lock:
        if not _hash_state['running']:
            return jsonify({'success': False, 'error': 'No hash computation running'})
        proc = _hash_proc
    if proc:
        try:
            proc.terminate()
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})
    return jsonify({'success': True})


@app.route('/api/duplicates/hash/logs')
def api_hash_logs():
    offset = int(request.args.get('offset', 0))
    with _hash_lock:
        logs = _hash_state['logs']
        return jsonify({'lines': logs[offset:], 'total': len(logs)})


# ── Folder listing ─────────────────────────────────────────────────────────

@app.route('/api/duplicates/folders')
def api_duplicate_folders():
    import os as _os
    dl_dir = Path.cwd() / 'reddit_downloads'
    if not dl_dir.exists():
        return jsonify({'success': False, 'error': 'reddit_downloads not found'}), 404
    try:
        folder_dirs = sorted(
            [f for f in dl_dir.iterdir() if f.is_dir()],
            key=lambda x: x.name.lower()
        )

        sdb = _get_dupes_db()

        # ── Single bulk query: group phash_cache rows by immediate subfolder name.
        # Extract the subfolder name by stripping the downloads prefix and taking
        # the first path component.  Works on both Linux ('/') and Windows ('\\').
        dl_prefix = str(dl_dir.resolve()) + _os.sep
        dl_prefix_len = len(dl_prefix)
        sep = _os.sep

        hash_rows = sdb.execute("""
            SELECT
                CASE
                    WHEN instr(substr(path, :skip), :sep) > 0
                    THEN substr(path, :skip, instr(substr(path, :skip), :sep) - 1)
                    ELSE substr(path, :skip)
                END AS folder_name,
                COUNT(*) AS total,
                SUM(CASE WHEN phash IS NOT NULL THEN 1 ELSE 0 END) AS hashed
            FROM phash_cache
            WHERE substr(path, 1, :plen) = :prefix
            GROUP BY folder_name
        """, {'skip': dl_prefix_len + 1, 'sep': sep,
              'plen': dl_prefix_len, 'prefix': dl_prefix}).fetchall()
        hash_by_folder = {r['folder_name']: r for r in hash_rows}

        # ── Single query for all folder scan results.
        scan_rows = sdb.execute('SELECT * FROM folder_scan_info').fetchall()
        scan_by_folder = {r['folder']: r for r in scan_rows}

        sdb.close()

        result = []
        for folder in folder_dirs:
            hr = hash_by_folder.get(folder.name)
            sr = scan_by_folder.get(folder.name)
            result.append({
                'name':           folder.name,
                'cached_count':   hr['hashed'] if hr else 0,
                'total_in_cache': hr['total']  if hr else 0,
                'dup_groups':     sr['total_groups']      if sr else 0,
                'wasted_fmt':     _format_bytes(sr['total_wasted_bytes'] or 0) if sr else None,
                'scanned_at':     sr['scanned_at']        if sr else None,
                'threshold':      sr['threshold']         if sr else None,
            })

        return jsonify({'success': True, 'folders': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/duplicates/folder_stats')
def api_folder_stats():
    folder_name = request.args.get('folder')
    if not folder_name:
        return jsonify({'success': False, 'error': 'folder param required'}), 400
    if not _DUPES_DB.exists():
        return jsonify({'success': True, 'has_data': False})
    try:
        sdb = _get_dupes_db()
        row = sdb.execute(
            'SELECT * FROM folder_scan_info WHERE folder = ?', (folder_name,)
        ).fetchone()
        sdb.close()
        if not row:
            return jsonify({'success': True, 'has_data': False})
        return jsonify({
            'success': True, 'has_data': True,
            'scanned_at':          row['scanned_at'],
            'total_files_scanned': row['total_files_scanned'],
            'total_groups':        row['total_groups'],
            'total_wasted_bytes':  row['total_wasted_bytes'],
            'total_wasted_fmt':    _format_bytes(row['total_wasted_bytes'] or 0),
            'threshold':           row['threshold'],
            'partial':             bool(row['is_partial']),
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/duplicates/stats')
def api_duplicate_stats():
    if not _DUPES_DB.exists():
        return jsonify({'success': True, 'has_data': False})
    try:
        sdb = _get_dupes_db()
        row = sdb.execute('SELECT * FROM scan_info WHERE id = 1').fetchone()
        sdb.close()
        if not row:
            return jsonify({'success': True, 'has_data': False})
        return jsonify({
            'success': True, 'has_data': True,
            'scanned_at':          row['scanned_at'],
            'scan_duration_sec':   row['scan_duration_sec'],
            'total_files_scanned': row['total_files_scanned'],
            'total_groups':        row['total_groups'],
            'total_wasted_bytes':  row['total_wasted_bytes'],
            'total_wasted_fmt':    _format_bytes(row['total_wasted_bytes'] or 0),
            'threshold':           row['threshold'],
            'hash_size':           row['hash_size'],
            'partial':             bool(row['is_partial']),
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/duplicates/groups')
def api_duplicate_groups():
    if not _DUPES_DB.exists():
        return jsonify({'success': True, 'groups': [], 'total': 0})
    try:
        sort        = request.args.get('sort', 'wasted_size')
        page        = max(1, int(request.args.get('page', 1)))
        per_p       = min(50, max(5, int(request.args.get('per_page', 20))))
        folder_name = request.args.get('folder') or None

        order  = 'wasted_size DESC' if sort == 'wasted_size' else 'file_count DESC'
        offset = (page - 1) * per_p

        sdb = _get_dupes_db()

        if folder_name:
            total_row = sdb.execute(
                'SELECT COUNT(*) FROM dup_groups WHERE file_count > 1 AND folder = ?',
                (folder_name,),
            ).fetchone()[0]
            groups_raw = sdb.execute(
                f'SELECT * FROM dup_groups WHERE file_count > 1 AND folder = ? '
                f'ORDER BY {order} LIMIT ? OFFSET ?',
                (folder_name, per_p, offset),
            ).fetchall()
        else:
            total_row = sdb.execute(
                'SELECT COUNT(*) FROM dup_groups WHERE file_count > 1'
            ).fetchone()[0]
            groups_raw = sdb.execute(
                f'SELECT * FROM dup_groups WHERE file_count > 1 ORDER BY {order} LIMIT ? OFFSET ?',
                (per_p, offset),
            ).fetchall()

        groups = []
        for g in groups_raw:
            files_raw = sdb.execute(
                'SELECT * FROM dup_files WHERE group_id = ? AND is_deleted = 0 ORDER BY file_size DESC',
                (g['id'],),
            ).fetchall()
            files = []
            for f in files_raw:
                fp = f['file_path']
                files.append({
                    'id':         f['id'],
                    'file_path':  fp,
                    'file_size':  f['file_size'],
                    'file_size_fmt': _format_bytes(f['file_size']),
                    'filename':   Path(fp).name,
                    'phash':      f['phash'],
                    'image_id':   f['image_id'],
                    'post_id':    f['post_id'],
                    'reddit_id':  f['reddit_id'],
                    'post_title': f['post_title'],
                    'subreddit':  f['subreddit'],
                    'permalink':  f['permalink'],
                    'score':      f['score'],
                    'web_url':    _file_to_url(fp),
                    'thumb_url':  _file_to_thumb(fp),
                })
            groups.append({
                'id':           g['id'],
                'file_hash':    g['file_hash'],
                'file_count':   g['file_count'],
                'total_size':   g['total_size'],
                'wasted_size':  g['wasted_size'],
                'total_size_fmt':  _format_bytes(g['total_size']),
                'wasted_size_fmt': _format_bytes(g['wasted_size']),
                'min_distance': g['min_distance'],
                'files':        files,
            })
        sdb.close()
        return jsonify({'success': True, 'groups': groups, 'total': total_row,
                        'page': page, 'per_page': per_p})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/duplicates/delete', methods=['POST'])
def api_delete_duplicates():
    data     = request.get_json() or {}
    file_ids = [int(x) for x in data.get('file_ids', [])]
    if not file_ids:
        return jsonify({'success': False, 'error': 'No file IDs provided'}), 400

    deleted, errors = 0, []
    sdb = _get_dupes_db()
    ph  = ','.join(['?'] * len(file_ids))
    files = sdb.execute(
        f'SELECT * FROM dup_files WHERE id IN ({ph}) AND is_deleted = 0', file_ids
    ).fetchall()

    my_conn = _get_db_connection()
    my_cur  = my_conn.cursor()

    for f in files:
        try:
            fp = Path(f['file_path'])

            # 1. Delete from filesystem
            if fp.exists():
                fp.unlink()

            # 2. Mark deleted in SQLite
            sdb.execute('UPDATE dup_files SET is_deleted = 1 WHERE id = ?', (f['id'],))

            # 3. Prod DB: only delete the images row when this is the last surviving
            #    copy in the group (all remaining active files share the same image_id
            #    because file_hash is UNIQUE in the images table).
            if f['image_id']:
                remaining = sdb.execute(
                    'SELECT COUNT(*) FROM dup_files '
                    'WHERE group_id = ? AND image_id = ? AND is_deleted = 0 AND id != ?',
                    (f['group_id'], f['image_id'], f['id']),
                ).fetchone()[0]

                if remaining == 0:
                    # Collect post_ids before cascade removes post_images
                    my_cur.execute(
                        'SELECT post_id FROM post_images WHERE image_id = %s', (f['image_id'],)
                    )
                    post_ids = [r[0] for r in my_cur.fetchall()]

                    my_cur.execute('DELETE FROM images WHERE id = %s', (f['image_id'],))

                    # Delete orphaned posts (no remaining images)
                    for pid in post_ids:
                        if pid is None:
                            continue
                        my_cur.execute(
                            'SELECT COUNT(*) FROM post_images WHERE post_id = %s', (pid,)
                        )
                        if my_cur.fetchone()[0] == 0:
                            my_cur.execute('DELETE FROM posts WHERE id = %s', (pid,))

                    my_conn.commit()

            deleted += 1
        except Exception as exc:
            errors.append(f'{Path(f["file_path"]).name}: {exc}')

    # 4. Refresh group counters in SQLite
    group_ids = list({f['group_id'] for f in files})
    for gid in group_ids:
        row = sdb.execute(
            'SELECT COUNT(*) AS cnt, COALESCE(SUM(file_size),0) AS ts, '
            'COALESCE(MIN(file_size),0) AS ms '
            'FROM dup_files WHERE group_id = ? AND is_deleted = 0', (gid,)
        ).fetchone()
        cnt, ts, ms = row['cnt'], row['ts'], row['ms']
        if cnt <= 1:
            sdb.execute('DELETE FROM dup_groups WHERE id = ?', (gid,))
            sdb.execute('DELETE FROM dup_files WHERE group_id = ?', (gid,))
        else:
            sdb.execute(
                'UPDATE dup_groups SET file_count=?, total_size=?, wasted_size=? WHERE id=?',
                (cnt, ts, ts - ms, gid),
            )

    sdb.commit()
    sdb.close()
    my_cur.close()
    my_conn.close()

    return jsonify({'success': True, 'deleted': deleted, 'errors': errors})


if __name__ == '__main__':
    main()

