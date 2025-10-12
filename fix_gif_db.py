import os
import mysql.connector
import configparser
from pathlib import Path

def get_mysql_config():
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
    mysql_config = get_mysql_config()
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT COUNT(*) FROM images WHERE filename LIKE '%.gif'")
    initial_count = cursor.fetchone()
    print(f"Rows with GIF filenames before update: {initial_count}")
    cursor.execute("SELECT id, filename, file_path FROM images WHERE filename LIKE '%.gif'")
    rows = cursor.fetchall()
    updated = 0
    for row in rows:
        gif_filename = row['filename']
        gif_path = row['file_path']
        mp4_filename = gif_filename[:-4] + '.mp4'
        mp4_path = str(Path(gif_path).with_suffix('.mp4'))
        if os.path.exists(mp4_path):
            mp4_size = os.path.getsize(mp4_path)
            cursor.execute(
                "UPDATE images SET filename=%s, file_size=%s, file_path=%s WHERE id=%s",
                (mp4_filename, mp4_size, mp4_path, row['id'])
            )
            print(f"Updated DB for id={row['id']}: {gif_filename} -> {mp4_filename}, size={mp4_size} bytes")
            updated += 1
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM images WHERE filename LIKE '%.gif'")
    remaining_count = cursor.fetchone()
    conn.close()
    print(f"\nTotal rows updated: {updated}")
    print(f"Rows still with GIF filenames after update: {remaining_count}")

if __name__ == "__main__":
    main()
