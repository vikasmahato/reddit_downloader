import mysql.connector
import configparser
import hashlib
import os
import sys
from datetime import datetime
import json
import re

def load_config():
    config = configparser.ConfigParser()
    # Handle the list sections that configparser doesn't like
    try:
        with open('config.ini', 'r') as f:
            content = f.read()
        
        # Remove lines that look like list items (quoted strings) in specific sections
        # Or just extract the mysql section manually
        lines = content.splitlines()
        clean_lines = []
        in_bad_section = False
        for line in lines:
            if line.strip().startswith('[') and line.strip().endswith(']'):
                section = line.strip()[1:-1]
                if section in ['scrape_list', 'user_scrape_list']:
                    in_bad_section = True
                else:
                    in_bad_section = False
            
            if in_bad_section and (line.strip().startswith('"') or line.strip().startswith("'")):
                continue
            clean_lines.append(line)
            
        config.read_string('\n'.join(clean_lines))
    except Exception as e:
        print(f"Error reading config: {e}")
        return None

    return {
        'host': config.get('mysql', 'host', fallback='localhost'),
        'port': config.getint('mysql', 'port', fallback=3306),
        'user': config.get('mysql', 'user', fallback='root'),
        'password': config.get('mysql', 'password', fallback=''),
        'database': config.get('mysql', 'database', fallback='reddit_images')
    }

def get_db_connection(config):
    return mysql.connector.connect(**config)

def create_tables(cursor):
    print("Creating new tables...")
    
    # Posts table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        reddit_id VARCHAR(255) UNIQUE,
        title TEXT,
        author VARCHAR(255),
        subreddit VARCHAR(255),
        permalink VARCHAR(512) UNIQUE,
        created_utc FLOAT,
        score INT,
        post_username VARCHAR(255),
        comments text,
        INDEX idx_subreddit (subreddit),
        INDEX idx_author (author)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """)

    # Images table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS images_new (
        id INT AUTO_INCREMENT PRIMARY KEY,
        file_hash VARCHAR(32) UNIQUE,
        file_path TEXT,
        filename VARCHAR(255),
        file_size BIGINT,
        download_date DATE,
        download_time TIME,
        is_deleted BOOLEAN DEFAULT 0,
        INDEX idx_hash (file_hash)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """)

    # Post-Images Association table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS post_images (
        id INT AUTO_INCREMENT PRIMARY KEY,
        post_id INT,
        image_id INT,
        url TEXT,
        FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
        FOREIGN KEY (image_id) REFERENCES images_new(id) ON DELETE CASCADE,
        UNIQUE KEY unique_post_image (post_id, image_id)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """)
    print("Tables created.")

def migrate_data(conn, cursor):
    print("Starting migration...")
    
    # Check if old images table exists
    cursor.execute("SHOW TABLES LIKE 'images'")
    if not cursor.fetchone():
        print("No 'images' table found. Skipping migration.")
        return

    # Get all existing images
    cursor.execute("SELECT * FROM images")
    old_images = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    
    print(f"Found {len(old_images)} images to migrate.")
    
    migrated_count = 0
    skipped_count = 0
    
    for row in old_images:
        img_data = dict(zip(columns, row))
        
        # 1. Handle Post
        permalink = img_data.get('permalink')
        reddit_id = None
        if permalink:
            # Try to extract reddit_id from permalink
            match = re.search(r'/comments/([a-z0-9]+)/', permalink)
            if match:
                reddit_id = match.group(1)
        
        # If no permalink/reddit_id, we might have orphan images or direct downloads.
        # We'll use the URL or filename as a fallback unique identifier for the "post" if needed,
        # but ideally we group by permalink.
        
        post_id = None
        if permalink:
            try:
                cursor.execute("""
                    INSERT INTO posts (reddit_id, title, author, subreddit, permalink, created_utc, score, post_username, comments)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID()
                """, (
                    reddit_id,
                    img_data.get('title'),
                    img_data.get('author'),
                    img_data.get('subreddit'),
                    permalink,
                    img_data.get('created_utc', 0), # Assuming created_utc might not be in old DB, check schema
                    img_data.get('score', 0),
                    img_data.get('post_username'),
                    img_data.get('comments')
                ))
                post_id = cursor.lastrowid
            except mysql.connector.Error as err:
                print(f"Error inserting post {permalink}: {err}")
                # Try to fetch existing if insert failed (though ON DUPLICATE KEY UPDATE should handle it)
                cursor.execute("SELECT id FROM posts WHERE permalink = %s", (permalink,))
                res = cursor.fetchone()
                if res:
                    post_id = res[0]

        # 2. Handle Image
        file_hash = img_data.get('file_hash')
        file_path = img_data.get('file_path')
        
        # If hash is missing, calculate it from file
        if not file_hash and file_path and os.path.exists(file_path):
            try:
                with open(file_path, "rb") as f:
                    file_hash = hashlib.md5(f.read()).hexdigest()
            except Exception as e:
                print(f"Could not calculate hash for {file_path}: {e}")
        
        if not file_hash:
            # Fallback for missing file/hash: use md5 of url
            file_hash = hashlib.md5(img_data['url'].encode()).hexdigest()

        image_id = None
        try:
            cursor.execute("""
                INSERT INTO images_new (file_hash, file_path, filename, file_size, download_date, download_time, is_deleted)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(), is_deleted=VALUES(is_deleted)
            """, (
                file_hash,
                file_path,
                img_data.get('filename'),
                img_data.get('file_size'),
                img_data.get('download_date'),
                img_data.get('download_time'),
                img_data.get('is_deleted', 0)
            ))
            image_id = cursor.lastrowid
        except mysql.connector.Error as err:
             print(f"Error inserting image {img_data.get('filename')}: {err}")
             cursor.execute("SELECT id FROM images_new WHERE file_hash = %s", (file_hash,))
             res = cursor.fetchone()
             if res:
                 image_id = res[0]

        # 3. Link Post and Image
        if post_id and image_id:
            try:
                cursor.execute("""
                    INSERT IGNORE INTO post_images (post_id, image_id, url)
                    VALUES (%s, %s, %s)
                """, (post_id, image_id, img_data['url']))
                migrated_count += 1
            except mysql.connector.Error as err:
                print(f"Error linking post {post_id} and image {image_id}: {err}")
        else:
            # If no post (e.g. direct url download without reddit metadata), we might want to create a dummy post or just skip?
            # For now, if we have an image but no post, we can't link them in post_images if post_id is required.
            # But wait, direct downloads usually don't have permalinks.
            # Let's create a "Direct Download" post or similar if permalink is missing.
            if not post_id and image_id:
                 # Create a dummy post for this download
                 dummy_permalink = f"direct_download_{hashlib.md5(img_data['url'].encode()).hexdigest()}"
                 try:
                    cursor.execute("""
                        INSERT INTO posts (reddit_id, title, author, subreddit, permalink, created_utc, score, post_username, comments)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID()
                    """, (
                        None,
                        img_data.get('title') or 'Direct Download',
                        img_data.get('author') or 'Unknown',
                        img_data.get('subreddit') or 'Direct',
                        dummy_permalink,
                        datetime.now().timestamp(),
                        0,
                        None,
                        None
                    ))
                    post_id = cursor.lastrowid
                    cursor.execute("""
                        INSERT IGNORE INTO post_images (post_id, image_id, url)
                        VALUES (%s, %s, %s)
                    """, (post_id, image_id, img_data['url']))
                    migrated_count += 1
                 except Exception as e:
                     print(f"Failed to handle orphan image: {e}")
                     skipped_count += 1
            else:
                skipped_count += 1

    print(f"Migration finished. Migrated: {migrated_count}, Skipped/Failed: {skipped_count}")
    
    # Rename tables
    print("Renaming tables...")
    try:
        cursor.execute("RENAME TABLE images TO images_old")
        cursor.execute("RENAME TABLE images_new TO images")
        print("Tables renamed successfully.")
    except mysql.connector.Error as err:
        print(f"Error renaming tables: {err}")

def main():
    config = load_config()
    conn = None
    try:
        conn = get_db_connection(config)
        conn.autocommit = False
        cursor = conn.cursor()
        
        create_tables(cursor)
        migrate_data(conn, cursor)
        
        conn.commit()
        print("Migration committed successfully.")
        
    except Exception as e:
        print(f"Migration failed: {e}")
        if conn:
            conn.rollback()
            print("Changes rolled back.")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
