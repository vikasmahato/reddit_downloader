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
        """Get images from MySQL database with filtering, including deleted filter, sorting, and hidden users.
        Returns images grouped by post_id, with each image having a 'post_images' list containing all images from the same post."""
        try:
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor(dictionary=True)
            # Join posts, images, and post_images tables
            query = """SELECT 
                i.id, i.file_hash, i.file_path, i.filename, i.file_size, 
                i.download_date, i.download_time, i.is_deleted,
                p.id as post_id, p.title, p.author, p.subreddit, p.permalink, p.created_utc, 
                p.score, p.post_username, p.comments,
                pi.url
            FROM images i
            LEFT JOIN post_images pi ON i.id = pi.image_id
            LEFT JOIN posts p ON pi.post_id = p.id
            WHERE 1=1"""
            params = []
            # Filter out hidden user
            query += " AND (p.author IS NULL OR p.author != 'BusPsychological3243')"
            if search:
                query += " AND (p.title LIKE %s OR p.author LIKE %s OR i.filename LIKE %s)"
                search_term = f"%{search}%"
                params.extend([search_term, search_term, search_term])
            if subreddit:
                query += " AND p.subreddit LIKE %s"
                params.append(f"%{subreddit}%")
            if user:
                query += " AND p.author LIKE %s"
                params.append(f"%{user}%")
            if deleted is not None:
                if deleted:
                    query += " AND i.is_deleted = 1"
                else:
                    query += " AND (i.is_deleted = 0 OR i.is_deleted IS NULL)"
            # Sorting logic
            if sort == 'comments':
                order_by = ''  # Will sort in Python after fetch
            elif sort == 'filesize':
                order_by = ' ORDER BY i.file_size DESC'
            else:
                order_by = ' ORDER BY i.download_date DESC, i.download_time DESC'
            query += order_by
            query += f" LIMIT {limit * 5} OFFSET {offset}"  # Fetch more to account for grouping
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Group images by post_id
            post_images_map = {}
            images = []
            seen_post_ids = set()
            
            for row in results:
                img_dict = dict(row)
                post_id = img_dict.get('post_id')
                
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
                
                # Group by post_id
                if post_id:
                    if post_id not in post_images_map:
                        post_images_map[post_id] = []
                    post_images_map[post_id].append(img_dict)
                    
                    # Only add the first image of each post to the main list
                    if post_id not in seen_post_ids:
                        seen_post_ids.add(post_id)
                        images.append(img_dict)
                else:
                    # Images without post_id are added individually
                    images.append(img_dict)
            
            # Add post_images list to each image
            for img in images:
                post_id = img.get('post_id')
                if post_id and post_id in post_images_map:
                    # Get all images for this post, sorted by id
                    all_post_images = sorted(post_images_map[post_id], key=lambda x: x.get('id', 0))
                    img['post_images'] = all_post_images
                    img['image_count'] = len(all_post_images)
                else:
                    img['post_images'] = [img]
                    img['image_count'] = 1
            
            conn.close()
            # Filter out hidden users
            if hidden_users:
                images = [img for img in images if img.get('author') not in hidden_users]
            # Sort by comment count if requested
            if sort == 'comments':
                images.sort(key=lambda x: x.get('comment_count', 0), reverse=True)
            
            # Limit to requested number after grouping
            return images[:limit]
        except Exception as e:
            print(f"Database error: {e}")
            return []

    def get_stats(self):
        """Get download statistics from MySQL."""
        try:
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor()
            # Total images (count distinct images, not posts)
            cursor.execute("SELECT COUNT(*) FROM images")
            total_images = cursor.fetchone()[0]
            # Images by subreddit (optimized query with LIMIT)
            cursor.execute("""SELECT p.subreddit, COUNT(DISTINCT i.id) as cnt
                FROM images i
                LEFT JOIN post_images pi ON i.id = pi.image_id
                LEFT JOIN posts p ON pi.post_id = p.id
                WHERE p.subreddit IS NOT NULL AND p.subreddit != ''
                GROUP BY p.subreddit 
                ORDER BY cnt DESC
                LIMIT 20""")
            subreddit_counts = dict(cursor.fetchall())
            # Top authors (for display) - optimized
            cursor.execute("""SELECT p.author, COUNT(DISTINCT i.id) as cnt
                FROM images i
                LEFT JOIN post_images pi ON i.id = pi.image_id
                LEFT JOIN posts p ON pi.post_id = p.id
                WHERE p.author IS NOT NULL AND p.author != ''
                GROUP BY p.author 
                ORDER BY cnt DESC 
                LIMIT 10""")
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

    def get_subreddits(self):
        """Get list of unique subreddits from MySQL."""
        try:
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT subreddit FROM posts WHERE subreddit IS NOT NULL AND subreddit != '' ORDER BY subreddit")
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
        # Join posts, images, and post_images tables
        cursor.execute("""SELECT 
            i.id, i.file_hash, i.file_path, i.filename, i.file_size, 
            i.download_date, i.download_time, i.is_deleted,
            p.id as post_id, p.title, p.author, p.subreddit, p.permalink, p.created_utc, 
            p.score, p.post_username, p.comments, p.reddit_id,
            pi.url
        FROM images i
        LEFT JOIN post_images pi ON i.id = pi.image_id
        LEFT JOIN posts p ON pi.post_id = p.id
        WHERE i.id = %s""", (image_id,))
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
            
            # Get all images from the same post
            post_id = image_dict.get('post_id')
            if post_id:
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
                current_image_index = 0
                for idx, post_img in enumerate(all_post_images):
                    post_img_dict = dict(post_img)
                    if post_img_dict.get('file_path'):
                        web = ui_handler.make_web_path(post_img_dict['file_path'])
                        if web:
                            post_img_dict['web_path'] = web
                    post_images_list.append(post_img_dict)
                    if post_img_dict['id'] == image_id:
                        current_image_index = idx
                image_dict['post_images'] = post_images_list
                image_dict['current_image_index'] = current_image_index
                image_dict['image_count'] = len(post_images_list)
            else:
                image_dict['post_images'] = [image_dict]
                image_dict['current_image_index'] = 0
                image_dict['image_count'] = 1
            
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
    # Get image and post info from MySQL
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor(dictionary=True)
    # Join to get post information
    cursor.execute("""SELECT 
        i.id, p.id as post_id, p.reddit_id, p.permalink, p.comments
    FROM images i
    LEFT JOIN post_images pi ON i.id = pi.image_id
    LEFT JOIN posts p ON pi.post_id = p.id
    WHERE i.id = %s""", (image_id,))
    image = cursor.fetchone()
    if not image:
        conn.close()
        return jsonify({'success': False, 'error': 'Image not found.'}), 404
    # Get Reddit post ID or permalink
    reddit_post_id = image.get('reddit_id')
    permalink = image.get('permalink')
    post_id = image.get('post_id')
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
        comments_json = image.get('comments', '[]')
        comments = json.loads(comments_json) if comments_json else []
        new_comment = {
            'author': reddit_comment.author.name if reddit_comment.author else 'You',
            'body': reddit_comment.body,
            'score': reddit_comment.score,
            'created_utc': reddit_comment.created_utc
        }
        comments.insert(0, new_comment)
        if post_id:
            cursor.execute("UPDATE posts SET comments = %s WHERE id = %s", (json.dumps(comments), post_id))
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
        # Join to get comments from posts table
        cursor.execute("""SELECT p.comments
        FROM images i
        LEFT JOIN post_images pi ON i.id = pi.image_id
        LEFT JOIN posts p ON pi.post_id = p.id
        WHERE i.id = %s""", (image_id,))
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
        data = request.get_json()
        image_id = data.get('image_id') if data else None
        
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor(dictionary=True)
        
        # Get image_id if not provided
        if not image_id:
            cursor.execute("SELECT image_id FROM post_images WHERE post_id = %s LIMIT 1", (post_id,))
            result = cursor.fetchone()
            if result:
                image_id = result['image_id']
        
        # Get file path before deletion
        file_path = None
        if image_id:
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

@app.route('/scrape-lists')
def scrape_lists():
    """Page for managing scrape lists."""
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT id, type, name, enabled, created_at, updated_at, last_scraped_at
            FROM scrape_lists
            ORDER BY type, name
        """)
        
        items = cursor.fetchall()
        conn.close()
        
        # Convert datetime objects to strings for template
        for item in items:
            for key in ['created_at', 'updated_at', 'last_scraped_at']:
                if item.get(key):
                    item[key] = item[key].strftime('%Y-%m-%d %H:%M:%S') if hasattr(item[key], 'strftime') else str(item[key])
        
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
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT id, type, name, enabled, created_at, updated_at, last_scraped_at
            FROM scrape_lists
            ORDER BY type, name
        """)
        
        items = cursor.fetchall()
        conn.close()
        
        # Convert datetime objects to strings
        for item in items:
            for key in ['created_at', 'updated_at', 'last_scraped_at']:
                if item.get(key):
                    item[key] = item[key].strftime('%Y-%m-%d %H:%M:%S') if hasattr(item[key], 'strftime') else str(item[key])
        
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
        
        conn = mysql.connector.connect(**mysql_config)
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
        
        conn = mysql.connector.connect(**mysql_config)
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
        conn = mysql.connector.connect(**mysql_config)
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
        conn = mysql.connector.connect(**mysql_config)
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

def main():
    """Main entry point for the web UI."""
    app.run(debug=True, host='0.0.0.0', port=4000)

if __name__ == '__main__':
    main()