#!/usr/bin/env python3
"""
Web UI for Reddit Image Downloader

A Flask-based web interface to browse downloaded images and metadata.
Provides search, filtering, and gallery view capabilities.
"""

import os
import sqlite3
from flask import Flask, render_template, request, jsonify, send_file, url_for, send_from_directory, redirect
from pathlib import Path
import json
from datetime import datetime
import mimetypes
import hashlib
from PIL import Image, ExifTags
import mysql.connector
import configparser

# Get the directory where this file is located
_current_dir = Path(__file__).parent
_template_dir = _current_dir / 'templates'
_static_folder = Path.cwd() / 'reddit_downloads'

app = Flask(__name__, 
            template_folder=str(_template_dir),
            static_url_path='/reddit_downloads', 
            static_folder=str(_static_folder))

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

    def get_all_images(self, limit=100, offset=0, search=None, subreddit=None, user=None, deleted=None, sort=None, hidden_users=None):
        """Get images from MySQL database with filtering, including deleted filter, sorting, and hidden users."""
        try:
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor(dictionary=True)
            query = "SELECT * FROM images WHERE 1=1 AND author != 'BusPsychological3243'"
            params = []
            if search:
                query += " AND (title LIKE %s OR author LIKE %s OR filename LIKE %s)"
                search_term = f"%{search}%"
                params.extend([search_term, search_term, search_term])
            if subreddit:
                query += " AND subreddit LIKE %s"
                params.append(f"%{subreddit}%")
            if user:
                query += " AND author LIKE %s"
                params.append(f"%{user}%")
            if deleted is not None:
                if deleted:
                    query += " AND is_deleted = 1"
                else:
                    query += " AND (is_deleted = 0 OR is_deleted IS NULL)"
            # Sorting logic
            if sort == 'comments':
                order_by = ''  # Will sort in Python after fetch
            elif sort == 'filesize':
                order_by = ' ORDER BY file_size DESC'
            else:
                order_by = ' ORDER BY download_date DESC, download_time DESC'
            query += order_by
            query += f" LIMIT {limit} OFFSET {offset}"
            cursor.execute(query, params)
            results = cursor.fetchall()
            images = []
            for row in results:
                img_dict = dict(row)
                if img_dict.get('file_path'):
                    web = self.make_web_path(img_dict['file_path'])
                    if web:
                        img_dict['web_path'] = web
                # Count comments
                try:
                    comments = json.loads(img_dict.get('comments', '[]')) if img_dict.get('comments') else []
                    img_dict['comment_count'] = len(comments)
                except Exception:
                    img_dict['comment_count'] = 0
                images.append(img_dict)
            conn.close()
            # Filter out hidden users
            if hidden_users:
                images = [img for img in images if img.get('author') not in hidden_users]
            # Sort by comment count if requested
            if sort == 'comments':
                images.sort(key=lambda x: x.get('comment_count', 0), reverse=True)
            return images
        except Exception as e:
            print(f"Database error: {e}")
            return []

    def get_stats(self):
        """Get download statistics from MySQL."""
        try:
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor()
            # Total images
            cursor.execute("SELECT COUNT(*) FROM images")
            total_images = cursor.fetchone()[0]
            # Images by subreddit
            cursor.execute("SELECT subreddit, COUNT(*) FROM images GROUP BY subreddit ORDER BY COUNT(*) DESC")
            subreddit_counts = dict(cursor.fetchall())
            # Top authors (for display)
            cursor.execute("SELECT author, COUNT(*) FROM images WHERE author != '' GROUP BY author ORDER BY COUNT(*) DESC LIMIT 10")
            user_counts = dict(cursor.fetchall())
            # All unique authors (for stats)
            cursor.execute("SELECT COUNT(DISTINCT author) FROM images WHERE author != ''")
            total_users = cursor.fetchone()[0]
            # File size stats
            cursor.execute("SELECT SUM(file_size) FROM images WHERE file_size > 0")
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

    def get_subreddits(self):
        """Get list of unique subreddits from MySQL."""
        try:
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT subreddit FROM images WHERE subreddit != '' ORDER BY subreddit")
            results = [row[0] for row in cursor.fetchall()]
            conn.close()
            return results
        except Exception as e:
            print(f"Subreddits error: {e}")
            return []

    def get_users(self):
        """Get list of unique users from MySQL."""
        try:
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT author FROM images WHERE author != '' ORDER BY author")
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
        deleted=deleted_filter,
        sort=sort if sort else None,
        hidden_users=hidden_users if hidden_users else None
    )
    for img in images:
        if img.get('file_path'):
            img['exif'] = extract_exif_data(img['file_path'])
    stats = ui_handler.get_stats()
    subreddits = ui_handler.get_subreddits()
    users = ui_handler.get_users()
    return render_template('index.html',
                         images=images,
                         stats=stats,
                         subreddits=subreddits,
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

@app.route('/details/<int:image_id>')
def image_details(image_id):
    """Show detailed information for a specific image."""
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM images WHERE id = %s", (image_id,))
        image = cursor.fetchone()
        if image:
            image_dict = dict(image)
            # Convert timedelta fields to string for JSON serialization
            from datetime import timedelta
            for k, v in image_dict.items():
                if isinstance(v, timedelta):
                    image_dict[k] = str(v)
            if image_dict.get('file_path'):
                web = ui_handler.make_web_path(image_dict['file_path'])
                if web:
                    image_dict['web_path'] = web
            # Extract EXIF data
            exif = extract_exif_data(image_dict['file_path'])
            image_dict['exif'] = exif
            conn.close()
            # Pass stats, subreddits, users for template compatibility
            stats = ui_handler.get_stats()
            subreddits = ui_handler.get_subreddits()
            users = ui_handler.get_users()
            return render_template('details.html', image=image_dict, stats=stats, subreddits=subreddits, users=users)
        else:
            conn.close()
            return "Image not found", 404
    except Exception as e:
        return f"Error: {e}", 500

@app.route('/api/post_comment', methods=['POST'])
def post_comment():
    """Post a comment to Reddit and save it locally in MySQL."""
    import json
    data = request.get_json()
    image_id = data.get('image_id')
    comment_text = data.get('comment', '').strip()
    if not image_id or not comment_text:
        return jsonify({'success': False, 'error': 'Missing image ID or comment.'}), 400
    # Get image info from MySQL
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM images WHERE id = %s", (image_id,))
    image = cursor.fetchone()
    if not image:
        conn.close()
        return jsonify({'success': False, 'error': 'Image not found.'}), 404
    # Get Reddit post ID or permalink
    reddit_post_id = image.get('reddit_id')
    permalink = image.get('permalink')
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
    # Save comment locally in MySQL
    try:
        comments_json = image.get('comments', '[]')
        comments = json.loads(comments_json) if comments_json else []
        new_comment = {
            'author': reddit_comment.author.name if reddit_comment.author else 'You',
            'body': reddit_comment.body,
            'score': reddit_comment.score,
            'created_utc': reddit_comment.created_utc
        }
        comments.insert(0, new_comment)
        cursor.execute("UPDATE images SET comments = %s WHERE id = %s", (json.dumps(comments), image_id))
        conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': f'Local save error: {e}'}), 500
    finally:
        conn.close()
    return jsonify({'success': True, 'comment': new_comment})

@app.route('/api/comments/<int:image_id>')
def get_comments(image_id):
    """Return latest comments for an image from MySQL."""
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT comments FROM images WHERE id = %s", (image_id,))
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

def main():
    """Main entry point for the web UI."""
    app.run(debug=True, host='0.0.0.0', port=4000)

if __name__ == '__main__':
    main()