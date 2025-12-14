#!/usr/bin/env python3
"""
Generate thumbnails for all images in the download folder.

This script scans the download folder, generates thumbnails for all images,
and saves them in a parallel directory structure in the thumbs folder.
"""
import os
import sys
import argparse
from pathlib import Path
from PIL import Image
import configparser
from loguru import logger

logger.remove()
logger.add(sys.stdout, colorize=True, format="<lvl>{message}</lvl>")

# Default thumbnail size
THUMBNAIL_SIZE = (300, 300)
THUMBNAIL_QUALITY = 85

def get_download_folder(config_path: str = "config.ini") -> Path:
    """Get download folder from config."""
    try:
        config = configparser.ConfigParser()
        config.read(config_path)
        download_folder = config.get('general', 'download_folder', fallback='downloads')
        return Path(download_folder).resolve()
    except Exception as e:
        logger.error(f"Error reading config: {e}")
        return Path('downloads').resolve()

def generate_thumbnail(source_path: Path, thumb_path: Path, size: tuple = THUMBNAIL_SIZE, quality: int = THUMBNAIL_QUALITY) -> bool:
    """Generate a thumbnail from an image file.
    
    Args:
        source_path: Path to source image
        thumb_path: Path to save thumbnail
        size: Thumbnail size (width, height)
        quality: JPEG quality (1-100)
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Skip if thumbnail already exists and is newer
        if thumb_path.exists():
            if thumb_path.stat().st_mtime >= source_path.stat().st_mtime:
                return True
        
        # Create parent directory if it doesn't exist
        thumb_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Open and process image
        with Image.open(source_path) as img:
            # Convert RGBA to RGB if necessary (for JPEG)
            if img.mode in ('RGBA', 'LA', 'P'):
                # Create a white background
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'P':
                    img = img.convert('RGBA')
                background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
                img = background
            elif img.mode != 'RGB':
                img = img.convert('RGB')
            
            # Create thumbnail maintaining aspect ratio
            img.thumbnail(size, Image.Resampling.LANCZOS)
            
            # Save thumbnail
            thumb_path.parent.mkdir(parents=True, exist_ok=True)
            img.save(thumb_path, 'JPEG', quality=quality, optimize=True)
            
        return True
    except Exception as e:
        logger.error(f"Error generating thumbnail for {source_path}: {e}")
        return False

def generate_thumbnail_for_video(video_path: Path, thumb_path: Path, size: tuple = THUMBNAIL_SIZE) -> bool:
    """Generate a thumbnail from a video file using ffmpeg.
    
    Args:
        video_path: Path to source video
        thumb_path: Path to save thumbnail
        size: Thumbnail size (width, height)
    
    Returns:
        True if successful, False otherwise
    """
    try:
        import subprocess
        
        # Skip if thumbnail already exists and is newer
        if thumb_path.exists():
            if thumb_path.stat().st_mtime >= video_path.stat().st_mtime:
                return True
        
        # Create parent directory
        thumb_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Use ffmpeg to extract a frame
        cmd = [
            'ffmpeg', '-y', '-i', str(video_path),
            '-vf', f'scale={size[0]}:{size[1]}:force_original_aspect_ratio=decrease,pad={size[0]}:{size[1]}:(ow-iw)/2:(oh-ih)/2',
            '-frames:v', '1',
            '-q:v', '2',
            str(thumb_path)
        ]
        
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        
        if result.returncode == 0 and thumb_path.exists():
            return True
        else:
            logger.warning(f"ffmpeg failed for {video_path}: {result.stderr.decode()}")
            return False
    except FileNotFoundError:
        logger.warning(f"ffmpeg not found, skipping video thumbnail: {video_path}")
        return False
    except Exception as e:
        logger.error(f"Error generating video thumbnail for {video_path}: {e}")
        return False

def is_image_file(path: Path) -> bool:
    """Check if file is an image."""
    image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
    return path.suffix.lower() in image_extensions

def is_video_file(path: Path) -> bool:
    """Check if file is a video."""
    video_extensions = {'.mp4', '.webm', '.mov', '.avi', '.mkv'}
    return path.suffix.lower() in video_extensions

def generate_all_thumbnails(download_folder: Path, thumbs_folder: Path, size: tuple = THUMBNAIL_SIZE, 
                            quality: int = THUMBNAIL_QUALITY, force: bool = False):
    """Generate thumbnails for all images in download folder.
    
    Args:
        download_folder: Source folder containing images
        thumbs_folder: Destination folder for thumbnails
        size: Thumbnail size (width, height)
        quality: JPEG quality (1-100)
        force: Force regeneration of existing thumbnails
    """
    if not download_folder.exists():
        logger.error(f"Download folder does not exist: {download_folder}")
        return
    
    logger.info(f"📁 Scanning: {download_folder}")
    logger.info(f"💾 Thumbnails: {thumbs_folder}")
    logger.info(f"📐 Size: {size[0]}x{size[1]}")
    
    # Find all image and video files
    image_files = []
    video_files = []
    
    for root, dirs, files in os.walk(download_folder):
        for file in files:
            file_path = Path(root) / file
            if is_image_file(file_path):
                image_files.append(file_path)
            elif is_video_file(file_path):
                video_files.append(file_path)
    
    total = len(image_files) + len(video_files)
    logger.info(f"📸 Found {len(image_files)} images and {len(video_files)} videos")
    
    if total == 0:
        logger.warning("No images or videos found")
        return
    
    # Process images
    processed = 0
    skipped = 0
    errors = 0
    
    for idx, image_path in enumerate(image_files, 1):
        # Calculate relative path from download folder
        try:
            rel_path = image_path.relative_to(download_folder)
        except ValueError:
            # File is not under download folder
            continue
        
        # Create corresponding thumbnail path
        thumb_path = thumbs_folder / rel_path
        thumb_path = thumb_path.with_suffix('.jpg')  # Always save as JPEG
        
        # Skip if exists and not forcing
        if not force and thumb_path.exists():
            if thumb_path.stat().st_mtime >= image_path.stat().st_mtime:
                skipped += 1
                if idx % 100 == 0:
                    logger.info(f"Progress: {idx}/{len(image_files)} images, {processed} processed, {skipped} skipped, {errors} errors")
                continue
        
        if generate_thumbnail(image_path, thumb_path, size, quality):
            processed += 1
        else:
            errors += 1
        
        if idx % 100 == 0:
            logger.info(f"Progress: {idx}/{len(image_files)} images, {processed} processed, {skipped} skipped, {errors} errors")
    
    # Process videos
    video_processed = 0
    video_errors = 0
    
    for idx, video_path in enumerate(video_files, 1):
        try:
            rel_path = video_path.relative_to(download_folder)
        except ValueError:
            continue
        
        thumb_path = thumbs_folder / rel_path
        thumb_path = thumb_path.with_suffix('.jpg')
        
        if not force and thumb_path.exists():
            if thumb_path.stat().st_mtime >= video_path.stat().st_mtime:
                skipped += 1
                continue
        
        if generate_thumbnail_for_video(video_path, thumb_path, size):
            video_processed += 1
        else:
            video_errors += 1
        
        if (len(image_files) + idx) % 100 == 0:
            logger.info(f"Progress: {len(image_files) + idx}/{total} files, {processed + video_processed} processed, {skipped} skipped, {errors + video_errors} errors")
    
    logger.success(f"\n✅ Thumbnail generation complete!")
    logger.info(f"📊 Summary:")
    logger.info(f"   Images processed: {processed}")
    logger.info(f"   Videos processed: {video_processed}")
    logger.info(f"   Skipped (up to date): {skipped}")
    logger.info(f"   Errors: {errors + video_errors}")

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Generate thumbnails for all images in the download folder',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--download-folder',
        type=str,
        help='Download folder path (default: from config.ini)'
    )
    parser.add_argument(
        '--thumbs-folder',
        type=str,
        default='reddit_downloads_thumbs',
        help='Thumbnails folder path (default: reddit_downloads_thumbs)'
    )
    parser.add_argument(
        '--size',
        type=int,
        nargs=2,
        default=[300, 300],
        metavar=('WIDTH', 'HEIGHT'),
        help='Thumbnail size in pixels (default: 300 300)'
    )
    parser.add_argument(
        '--quality',
        type=int,
        default=85,
        help='JPEG quality 1-100 (default: 85)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force regeneration of existing thumbnails'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config.ini',
        help='Path to config.ini (default: config.ini)'
    )
    
    args = parser.parse_args()
    
    # Get download folder
    if args.download_folder:
        download_folder = Path(args.download_folder).resolve()
    else:
        download_folder = get_download_folder(args.config)
    
    thumbs_folder = Path(args.thumbs_folder).resolve()
    
    if not download_folder.exists():
        logger.error(f"Download folder does not exist: {download_folder}")
        sys.exit(1)
    
    generate_all_thumbnails(
        download_folder=download_folder,
        thumbs_folder=thumbs_folder,
        size=tuple(args.size),
        quality=args.quality,
        force=args.force
    )

if __name__ == "__main__":
    main()

