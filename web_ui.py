#!/usr/bin/env python3
"""
Web UI for Reddit Image Downloader

A Flask-based web interface to browse downloaded images and metadata.
Provides search, filtering, and gallery view capabilities.
"""

import os
import sqlite3
from flask import Flask, render_template, request, jsonify, send_file, url_for
from pathlib import Path
import json
from datetime import datetime
import mimetypes
import hashlib

app = Flask(__name__)

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

@app.template_filter('format_datetime')
def format_datetime_filter(value):
    try:
        return datetime.fromtimestamp(float(value)).strftime('%Y-%m-%d %H:%M')
    except Exception:
        return ''

@app.template_filter('loads')
def loads_filter(value):
    try:
        return json.loads(value)
    except Exception:
        return []

class RedditImageUI:
    def __init__(self, download_folder="reddit_downloads", metadata_db="metadata.db"):
        self.download_folder = Path(download_folder)
        self.metadata_db = self.download_folder / metadata_db
        
    def get_all_images(self, limit=100, offset=0, search=None, subreddit=None, user=None, deleted=None):
        """Get images from database with filtering, including deleted filter."""
        try:
            conn = sqlite3.connect(str(self.metadata_db))
            conn.row_factory = sqlite3.Row  # Enable column access by name
            cursor = conn.cursor()
            
            query = "SELECT * FROM images WHERE 1=1"
            params = []
            
            if search:
                query += " AND (title LIKE ? OR author LIKE ? OR filename LIKE ?)"
                search_term = f"%{search}%"
                params.extend([search_term, search_term, search_term])
            
            if subreddit:
                query += " AND subreddit LIKE ?"
                params.append(f"%{subreddit}%")
            
            if user:
                query += " AND author LIKE ?"
                params.append(f"%{user}%")
            
            if deleted is not None:
                if deleted:
                    query += " AND is_deleted = 1"
                else:
                    query += " AND (is_deleted = 0 OR is_deleted IS NULL)"

            query += " ORDER BY download_date DESC, download_time DESC"
            query += f" LIMIT {limit} OFFSET {offset}"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            images = []
            for row in results:
                img_dict = dict(row)
                # Convert paths to relative paths for web access
                if img_dict['file_path']:
                    relative_path = Path(img_dict['file_path']).relative_to(self.download_folder)
                    img_dict['web_path'] = str(relative_path).replace('\\', '/')
                images.append(img_dict)
            
            conn.close()
            return images
            
        except Exception as e:
            print(f"Database error: {e}")
            return []
    
    def get_stats(self):
        """Get download statistics."""
        try:
            conn = sqlite3.connect(str(self.metadata_db))
            cursor = conn.cursor()
            
            # Total images
            cursor.execute("SELECT COUNT(*) FROM images")
            total_images = cursor.fetchone()[0]
            
            # Images by subreddit
            cursor.execute("SELECT subreddit, COUNT(*) FROM images GROUP BY subreddit ORDER BY COUNT(*) DESC")
            subreddit_counts = dict(cursor.fetchall())
            
            # Images by user
            cursor.execute("SELECT author, COUNT(*) FROM images WHERE author != '' GROUP BY author ORDER BY COUNT(*) DESC LIMIT 10")
            user_counts = dict(cursor.fetchall())
            
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
                'total_users': len(user_counts)
            }
            
        except Exception as e:
            print(f"Stats error: {e}")
            return {}
    
    def get_subreddits(self):
        """Get list of unique subreddits."""
        try:
            conn = sqlite3.connect(str(self.metadata_db))
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT subreddit FROM images WHERE subreddit != '' ORDER BY subreddit")
            results = [row[0] for row in cursor.fetchall()]
            conn.close()
            return results
        except Exception as e:
            print(f"Subreddits error: {e}")
            return []
    
    def get_users(self):
        """Get list of unique users."""
        try:
            conn = sqlite3.connect(str(self.metadata_db))
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

@app.route('/')
def index():
    """Main gallery page."""
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
                         filter_deleted=deleted)

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

@app.route('/image/<path:layout>')
def serve_image(layout):
    """Serve image files."""
    try:
        image_path = ui_handler.download_folder / layout.replace('/', os.sep)
        if image_path.exists():
            return send_file(str(image_path))
        else:
            return "Image not found", 404
    except Exception as e:
        return f"Error: {e}", 500

@app.route('/details/<int:image_id>')
def image_details(image_id):
    """Show detailed information for a specific image."""
    try:
        conn = sqlite3.connect(str(ui_handler.metadata_db))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM images WHERE id = ?", (image_id,))
        image = cursor.fetchone()
        
        if image:
            image_dict = dict(image)
            if image_dict['file_path']:
                relative_path = Path(image_dict['file_path']).relative_to(ui_handler.download_folder)
                image_dict['web_path'] = str(relative_path).replace('\\', '/')
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

if __name__ == '__main__':
    # Check if metadata database exists
    if not ui_handler.metadata_db.exists():
        print("❌ Metadata database not found!")
        print(f"   Expected location: {ui_handler.metadata_db}")
        print("   Please run the downloader script first to create the database.")
        exit(1)
    
    print("🚀 Starting Reddit Image Browser UI...")
    print(f"📁 Download folder: {ui_handler.download_folder}")
    print(f"🗄️  Database: {ui_handler.metadata_db}")
    print("\n🌐 Access the UI at:")
    print("   http://localhost:5000")
    print("\n💡 Features:")
    print("   - Browse images in gallery view")
    print("   - Search by title, author, or filename")
    print("   - Filter by subreddit or user")
    print("   - View detailed metadata")
    print("   - Statistics dashboard")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
