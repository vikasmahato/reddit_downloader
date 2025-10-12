import os
import shutil
from pathlib import Path
import subprocess

def compress_gif_with_gifsicle(input_path, output_path):
    try:
        # Use gifsicle for lossy compression
        result = subprocess.run([
            'gifsicle', '--optimize=3', '--lossy=80', input_path, '-o', output_path
        ], capture_output=True)
        if result.returncode != 0:
            print(f"gifsicle error: {result.stderr.decode().strip()}")
            return False
    except FileNotFoundError:
        print("gifsicle not found. Please install gifsicle and ensure it's in your PATH.")
        return False
    except Exception as e:
        print(f"Error compressing {input_path}: {e}")
        return False
    return True

def get_size(path):
    return os.path.getsize(path)

def print_size_stats(old_path, new_path):
    old_size = get_size(old_path)
    new_size = get_size(new_path)
    percent = 100 * (old_size - new_size) / old_size if old_size else 0
    print(f"{os.path.basename(new_path)}: Old size: {old_size/1024:.2f} KB, New size: {new_size/1024:.2f} KB, Saved: {percent:.2f}%")

def main():
    base_dir = Path('reddit_downloads')
    backup_dir = base_dir / 'backup'
    backup_dir.mkdir(exist_ok=True)

    for root, dirs, files in os.walk(base_dir):
        # Skip backup folder
        if backup_dir in [Path(root)]:
            continue
        for file in files:
            if file.lower().endswith('.gif'):
                gif_path = Path(root) / file
                rel_path = gif_path.relative_to(base_dir)
                backup_path = backup_dir / rel_path
                backup_path.parent.mkdir(parents=True, exist_ok=True)
                print(f"Processing: {gif_path}")
                # Move original to backup
                shutil.move(str(gif_path), str(backup_path))
                # Compress backup gif and save to original location using gifsicle
                success = compress_gif_with_gifsicle(str(backup_path), str(gif_path))
                if success:
                    print_size_stats(str(backup_path), str(gif_path))
                else:
                    print(f"Failed to compress {gif_path}")

if __name__ == "__main__":
    main()
