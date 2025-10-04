import os
from pathlib import Path
from PIL import Image, ExifTags
import sqlite3

def get_exif_data(image_path):
    try:
        with Image.open(image_path) as img:
            exif_data = img._getexif()
            if not exif_data:
                return None
            return {ExifTags.TAGS.get(tag, tag): value for tag, value in exif_data.items()}
    except Exception:
        return None

def get_db_id(image_path, db_path):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM images WHERE file_path = ?", (str(image_path),))
        result = cursor.fetchone()
        conn.close()
        if result:
            return result[0]
    except Exception:
        pass
    return None

def scan_folder_for_exif(folder, db_path):
    image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff'}
    for root, _, files in os.walk(folder):
        for file in files:
            ext = Path(file).suffix.lower()
            if ext in image_extensions:
                image_path = os.path.join(root, file)
                exif = get_exif_data(image_path)
                print(f"Image: {image_path}")
                if exif:
                    db_id = get_db_id(image_path, db_path)
                    if db_id:
                        print(f"DB ID: {db_id}")
                    else:
                        print("DB ID: Not found")
                    print("EXIF Information:")
                    print("{:<30} | {}".format("Key", "Value"))
                    print("-"*60)
                    for key, value in exif.items():
                        print("{:<30} | {}".format(str(key), str(value)))
                else:
                    print("No EXIF data found.")
                print("\n" + "="*60 + "\n")

def main():
    downloads_folder = Path('reddit_downloads')
    db_path = str(downloads_folder / 'metadata.db')
    if not downloads_folder.exists():
        print(f"Folder not found: {downloads_folder}")
        return
    if not os.path.exists(db_path):
        print(f"Database not found: {db_path}")
        return
    scan_folder_for_exif(downloads_folder, db_path)

if __name__ == "__main__":
    main()
