import sys
import os
import subprocess
import mysql.connector
import configparser

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

def update_db_with_mp4(gif_filename, mp4_filename, mp4_size):
    try:
        mysql_config = get_mysql_config()
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        # Update the record where filename matches the GIF filename
        cursor.execute(
            "UPDATE images SET filename=%s, file_size=%s WHERE filename=%s",
            (mp4_filename, mp4_size, gif_filename)
        )
        conn.commit()
        conn.close()
        print(f"Database updated: {gif_filename} -> {mp4_filename}, size={mp4_size} bytes")
    except Exception as e:
        print(f"Failed to update database: {e}")

def gif_to_mp4(gif_path):
    if not os.path.isfile(gif_path):
        print(f"File not found: {gif_path}")
        return 0, 0
    if not gif_path.lower().endswith('.gif'):
        print("Input file must be a GIF.")
        return 0, 0
    mp4_path = os.path.splitext(gif_path)[0] + '.mp4'
    if os.path.exists(mp4_path):
        print(f"MP4 already exists for {gif_path}, skipping.")
        return 0, 0
    print(f"Converting {gif_path} to {mp4_path} using ffmpeg...")
    cmd = [
        'ffmpeg', '-y', '-i', gif_path,
        '-movflags', 'faststart', '-pix_fmt', 'yuv420p', '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
        mp4_path
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print(f"ffmpeg conversion failed: {e}")
        return 0, 0
    gif_size = os.path.getsize(gif_path)
    mp4_size = os.path.getsize(mp4_path)
    percent_diff = ((gif_size - mp4_size) / gif_size) * 100 if gif_size else 0
    print(f"GIF size: {gif_size/1024:.2f} KB, MP4 size: {mp4_size/1024:.2f} KB, Compression: {percent_diff:.2f}%")
    # Delete original GIF
    try:
        os.remove(gif_path)
        print(f"Deleted original GIF: {gif_path}")
    except Exception as e:
        print(f"Failed to delete GIF: {e}")
    # Update DB
    update_db_with_mp4(os.path.basename(gif_path), os.path.basename(mp4_path), mp4_size)
    return gif_size, mp4_size

def compress_gifs_in_folder(folder_path):
    total_gif_size = 0
    total_mp4_size = 0
    file_count = 0
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith('.gif'):
                gif_path = os.path.join(root, file)
                gif_size, mp4_size = gif_to_mp4(gif_path)
                if gif_size > 0 and mp4_size > 0:
                    total_gif_size += gif_size
                    total_mp4_size += mp4_size
                    file_count += 1
    if file_count > 0:
        overall_percent = ((total_gif_size - total_mp4_size) / total_gif_size) * 100 if total_gif_size else 0
        print(f"\nProcessed {file_count} GIFs.")
        print(f"Total GIF size: {total_gif_size/1024:.2f} KB, Total MP4 size: {total_mp4_size/1024:.2f} KB")
        print(f"Overall compression: {overall_percent:.2f}%")
    else:
        print("No GIFs were processed.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python gif_to_mp4.py <folder_path>")
    else:
        compress_gifs_in_folder(sys.argv[1])
