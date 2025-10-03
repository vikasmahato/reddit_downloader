#!/usr/bin/env python3
"""
Migration script to update gallery posts in metadata.db so their url field contains all image URLs, comma-separated.
"""
import sqlite3
import json
from pathlib import Path

def migrate_gallery_urls(db_path):
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT id, url, comments FROM images")
    rows = cursor.fetchall()
    updated = 0
    for row in rows:
        img_id, url, comments = row
        # Try to detect gallery posts by comments field containing a JSON list of dicts with 'gallery_urls'
        try:
            if comments and '[' in comments and 'gallery_urls' in comments:
                data = json.loads(comments)
                gallery_urls = []
                for c in data:
                    if 'gallery_urls' in c:
                        gallery_urls.extend(c['gallery_urls'])
                if gallery_urls:
                    new_url = ','.join(gallery_urls)
                    cursor.execute("UPDATE images SET url = ? WHERE id = ?", (new_url, img_id))
                    updated += 1
        except Exception:
            continue
    conn.commit()
    conn.close()
    print(f"Migration complete. Updated {updated} gallery records.")

if __name__ == "__main__":
    db_path = Path("reddit_downloads/metadata.db")
    migrate_gallery_urls(db_path)

