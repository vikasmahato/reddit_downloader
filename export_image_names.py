#!/usr/bin/env python3
"""
Export all image filenames (without extensions) from the images table to a file.
"""

import mysql.connector
import configparser
from pathlib import Path

def load_mysql_config():
    """Load MySQL configuration from config.ini"""
    config = configparser.ConfigParser()
    config.read('config.ini')
    return {
        'host': config.get('mysql', 'host', fallback='localhost'),
        'port': config.getint('mysql', 'port', fallback=3306),
        'user': config.get('mysql', 'user', fallback='root'),
        'password': config.get('mysql', 'password', fallback=''),
        'database': config.get('mysql', 'database', fallback='reddit_images')
    }

def main():
    mysql_config = load_mysql_config()
    output_file = 'image_names_no_extensions.txt'
    
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        
        # Get all filenames from images table
        cursor.execute("SELECT DISTINCT filename FROM images WHERE filename IS NOT NULL AND filename != ''")
        results = cursor.fetchall()
        
        # Extract filenames without extensions
        filenames_no_ext = []
        for (filename,) in results:
            if filename:
                # Remove extension
                name_without_ext = Path(filename).stem
                filenames_no_ext.append(name_without_ext)
        
        conn.close()
        
        # Sort and remove duplicates
        filenames_no_ext = sorted(set(filenames_no_ext))
        
        # Write to file
        with open(output_file, 'w', encoding='utf-8') as f:
            for name in filenames_no_ext:
                f.write(name + '\n')
        
        print(f"✓ Exported {len(filenames_no_ext)} unique filenames (without extensions) to {output_file}")
        
    except mysql.connector.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()

