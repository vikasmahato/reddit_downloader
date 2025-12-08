#!/usr/bin/env python3
"""
Move files from reddit_downloads to /deleted that are not present in image_names_no_extensions.txt
"""

import os
import shutil
from pathlib import Path
import configparser

def load_config():
    """Load configuration from config.ini"""
    config = configparser.ConfigParser()
    config.read('config.ini')
    return {
        'download_folder': config.get('general', 'download_folder', fallback='reddit_downloads')
    }

def main():
    config = load_config()
    download_folder = Path(config['download_folder'])
    deleted_folder = download_folder / 'deleted'
    names_file = Path('image_names_no_extensions.txt')
    
    # Check if names file exists
    if not names_file.exists():
        print(f"❌ Error: {names_file} not found!")
        print("   Please run export_image_names.py first to generate the file.")
        return
    
    # Read the list of valid filenames (without extensions)
    print(f"📖 Reading {names_file}...")
    valid_names = set()
    with open(names_file, 'r', encoding='utf-8') as f:
        for line in f:
            name = line.strip()
            if name:
                valid_names.add(name)
    
    print(f"✓ Found {len(valid_names)} valid image names in the list")
    
    # Check if download folder exists
    if not download_folder.exists():
        print(f"❌ Error: Download folder {download_folder} does not exist!")
        return
    
    # Create deleted folder if it doesn't exist
    deleted_folder.mkdir(parents=True, exist_ok=True)
    print(f"📁 Deleted folder: {deleted_folder}")
    
    # Scan all files in download folder (recursively)
    print(f"\n🔍 Scanning {download_folder} for files...")
    files_to_move = []
    total_files = 0
    
    for file_path in download_folder.rglob('*'):
        if file_path.is_file() and file_path.parent != deleted_folder:
            total_files += 1
            # Get filename without extension
            filename_no_ext = file_path.stem
            
            # Check if this file is in the valid list
            if filename_no_ext not in valid_names:
                files_to_move.append(file_path)
    
    print(f"✓ Found {total_files} total files")
    print(f"📦 Found {len(files_to_move)} files to move to /deleted")
    
    if not files_to_move:
        print("\n✅ No orphaned files found. All files are in the database!")
        return
    
    # Ask for confirmation
    print(f"\n⚠️  About to move {len(files_to_move)} files to {deleted_folder}")
    response = input("Continue? (yes/no): ").strip().lower()
    
    if response not in ['yes', 'y']:
        print("❌ Cancelled.")
        return
    
    # Move files
    moved_count = 0
    error_count = 0
    
    print(f"\n📦 Moving files...")
    for file_path in files_to_move:
        try:
            # Create destination path
            dest_path = deleted_folder / file_path.name
            
            # Handle filename conflicts
            counter = 1
            while dest_path.exists():
                stem = file_path.stem
                suffix = file_path.suffix
                dest_path = deleted_folder / f"{stem}_{counter}{suffix}"
                counter += 1
            
            # Move the file
            shutil.move(str(file_path), str(dest_path))
            moved_count += 1
            
            if moved_count % 100 == 0:
                print(f"   Moved {moved_count}/{len(files_to_move)} files...")
                
        except Exception as e:
            error_count += 1
            print(f"   ⚠️  Error moving {file_path.name}: {e}")
    
    print(f"\n✅ Done!")
    print(f"   Moved: {moved_count} files")
    if error_count > 0:
        print(f"   Errors: {error_count} files")
    
    # Optionally remove empty directories
    print(f"\n🧹 Cleaning up empty directories...")
    removed_dirs = 0
    for dir_path in sorted(download_folder.rglob('*'), reverse=True):
        if dir_path.is_dir() and dir_path != download_folder and dir_path != deleted_folder:
            try:
                if not any(dir_path.iterdir()):
                    dir_path.rmdir()
                    removed_dirs += 1
            except Exception:
                pass
    
    if removed_dirs > 0:
        print(f"   Removed {removed_dirs} empty directories")

if __name__ == "__main__":
    main()

