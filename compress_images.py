#!/usr/bin/env python3
"""
compress_images.py — Compress images larger than a size threshold.

Scans a folder (or the whole reddit_downloads tree), compresses oversized JPEGs
and PNGs in-place using Pillow, then updates file_size in the MySQL images table
and invalidates stale entries in the phash_cache (duplicates.db) so that the
next phash run re-hashes touched files.

Usage:
    # All subreddit folders, files > 1 MB, JPEG quality 85:
    python compress_images.py

    # Specific folder, 500 KB threshold, JPEG quality 80:
    python compress_images.py --folder reddit_downloads/pics --min-size-kb 500 --quality 80

    # Machine-readable JSON progress (used by web UI):
    python compress_images.py --progress-json

Cron example (daily at 3 AM):
    0 3 * * * cd /path/to/reddit_downloader && .venv/bin/python compress_images.py >> logs/compress.log 2>&1
"""

import argparse
import json
import os
import signal
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional

# ── Stop flag ─────────────────────────────────────────────────────────────
_stop_requested = False


def _handle_stop(signum, frame):
    global _stop_requested
    _stop_requested = True


signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)

# ── Constants ─────────────────────────────────────────────────────────────
DEFAULT_MIN_SIZE_KB = 1024          # 1 MB
DEFAULT_QUALITY     = 85            # JPEG quality (1-95)
DEFAULT_FOLDER      = 'reddit_downloads'
DUPES_DB            = Path('duplicates.db')

IMAGE_EXT   = {'.jpg', '.jpeg'}     # formats we re-encode
PNG_EXT     = {'.png'}              # lossless-optimised separately
SKIP_DIRS   = {'deleted', 'thumbs'} # never touch these sub-folders


# ── Helpers ───────────────────────────────────────────────────────────────

def _fmt(n: int) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if n < 1024:
            return f'{n:.1f} {unit}'
        n /= 1024
    return f'{n:.1f} TB'


def _emit(msg: str, cur: int = 0, tot: int = 0,
          saved: int = 0, as_json: bool = False):
    if as_json:
        print(json.dumps({'message': msg, 'progress': cur, 'total': tot,
                          'saved_bytes': saved}), flush=True)
    else:
        bar = f'[{cur}/{tot}] ' if tot else ''
        print(f'{bar}{msg}', flush=True)


# ── MySQL update ──────────────────────────────────────────────────────────

def _update_db_filesize(file_path: str, new_size: int) -> bool:
    """Update file_size for the given path in the MySQL images table."""
    try:
        import configparser
        import mysql.connector
        cfg = configparser.ConfigParser()
        cfg.read('config.ini')
        conn = mysql.connector.connect(
            host    =cfg.get('mysql', 'host',     fallback='localhost'),
            port    =cfg.getint('mysql', 'port',  fallback=3306),
            user    =cfg.get('mysql', 'user',     fallback='root'),
            password=cfg.get('mysql', 'password', fallback=''),
            database=cfg.get('mysql', 'database', fallback='reddit_images'),
        )
        cur = conn.cursor()
        cur.execute(
            'UPDATE images SET file_size = %s WHERE file_path = %s',
            (new_size, file_path)
        )
        conn.commit()
        conn.close()
        return cur.rowcount > 0
    except Exception as e:
        print(f'[warn] DB update failed for {file_path}: {e}', file=sys.stderr)
        return False


# ── phash cache invalidation ──────────────────────────────────────────────

def _invalidate_phash(file_path: str) -> None:
    """Remove or update the phash_cache entry so it gets re-hashed next run."""
    if not DUPES_DB.exists():
        return
    try:
        conn = sqlite3.connect(str(DUPES_DB), timeout=10)
        conn.execute('DELETE FROM phash_cache WHERE path = ?', (file_path,))
        conn.commit()
        conn.close()
    except Exception:
        pass


# ── Compression ───────────────────────────────────────────────────────────

def compress_file(path: Path, quality: int) -> Optional[int]:
    """
    Compress a single image file in-place.
    Returns bytes saved (positive) or None if skipped / failed.
    """
    try:
        from PIL import Image
    except ImportError:
        print('Pillow not installed — run: pip install Pillow', file=sys.stderr)
        sys.exit(1)

    original_size = path.stat().st_size
    ext = path.suffix.lower()

    try:
        with Image.open(path) as img:
            fmt = img.format  # 'JPEG', 'PNG', …

            if ext in IMAGE_EXT or fmt == 'JPEG':
                # Convert palette / RGBA → RGB before saving as JPEG
                if img.mode in ('RGBA', 'P', 'LA'):
                    bg = Image.new('RGB', img.size, (255, 255, 255))
                    mask = img.split()[-1] if img.mode in ('RGBA', 'LA') else None
                    if mask:
                        bg.paste(img.convert('RGB'), mask=mask)
                    else:
                        bg.paste(img.convert('RGB'))
                    img_out = bg
                else:
                    img_out = img.convert('RGB') if img.mode != 'RGB' else img

                img_out.save(path, 'JPEG', quality=quality, optimize=True,
                             progressive=True)

            elif ext in PNG_EXT or fmt == 'PNG':
                img.save(path, 'PNG', optimize=True)

            else:
                return None  # unsupported format

        new_size = path.stat().st_size
        saved = original_size - new_size
        return saved  # may be negative if already well-compressed

    except Exception as e:
        print(f'[warn] Could not compress {path}: {e}', file=sys.stderr)
        return None


# ── Core ──────────────────────────────────────────────────────────────────

def run_compress(
    folder: Path,
    min_size_bytes: int = DEFAULT_MIN_SIZE_KB * 1024,
    quality: int = DEFAULT_QUALITY,
    progress_json: bool = False,
    stats_file: Optional[Path] = None,
) -> dict:
    """
    Compress all oversized images under `folder`.
    Returns a summary dict.
    """
    import datetime

    global _stop_requested
    _stop_requested = False

    def emit(msg, cur=0, tot=0, saved=0):
        _emit(msg, cur, tot, saved, as_json=progress_json)

    folder = folder.resolve()
    COMPRESSIBLE = IMAGE_EXT | PNG_EXT

    # Collect candidate files
    emit(f'Scanning {folder} …')
    candidates = []
    for root, dirs, files in os.walk(folder):
        # Skip special sub-directories
        dirs[:] = [d for d in dirs if d.lower() not in SKIP_DIRS]
        for fname in files:
            fp = Path(root) / fname
            if fp.suffix.lower() not in COMPRESSIBLE:
                continue
            try:
                if fp.stat().st_size >= min_size_bytes:
                    candidates.append(fp)
            except OSError:
                pass

    total = len(candidates)
    emit(f'Found {total:,} image(s) above {_fmt(min_size_bytes)} threshold.', 0, total)

    if total == 0:
        emit('Nothing to compress.', 0, 0)
        return {'total': 0, 'compressed': 0, 'skipped': 0, 'saved_bytes': 0,
                'size_before_bytes': 0, 'size_after_bytes': 0}

    # ── Before stats ──────────────────────────────────────────────────────
    size_before = sum(fp.stat().st_size for fp in candidates)
    emit(f'Before: {total:,} files, {_fmt(size_before)} total')

    compressed = skipped = 0
    total_saved = 0
    t0 = time.time()

    for i, fp in enumerate(candidates, 1):
        if _stop_requested:
            emit(f'Stopped at {i-1}/{total}.', i - 1, total, total_saved)
            break

        saved = compress_file(fp, quality)

        if saved is None:
            skipped += 1
            emit(f'[{i}/{total}] Skipped (unsupported): {fp.name}', i, total, total_saved)
            continue

        if saved <= 0:
            skipped += 1
            emit(f'[{i}/{total}] Already optimal: {fp.name}', i, total, total_saved)
            continue

        compressed += 1
        total_saved += saved
        new_size = fp.stat().st_size
        emit(
            f'[{i}/{total}] {fp.name}  −{_fmt(saved)}  → {_fmt(new_size)}',
            i, total, total_saved,
        )

        # Keep DB in sync
        _update_db_filesize(str(fp), new_size)
        _invalidate_phash(str(fp))

    elapsed = time.time() - t0

    # ── After stats ───────────────────────────────────────────────────────
    size_after = sum(fp.stat().st_size for fp in candidates)
    actual_saved = size_before - size_after

    summary_lines = [
        f'',
        f'=== Compression Stats ===',
        f'Run at      : {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        f'Folder      : {folder}',
        f'Threshold   : {_fmt(min_size_bytes)}',
        f'Quality     : {quality}',
        f'',
        f'BEFORE',
        f'  Files     : {total:,}',
        f'  Disk used : {_fmt(size_before)}  ({size_before:,} bytes)',
        f'',
        f'AFTER',
        f'  Files     : {total:,}',
        f'  Disk used : {_fmt(size_after)}  ({size_after:,} bytes)',
        f'',
        f'RESULT',
        f'  Compressed: {compressed:,}',
        f'  Skipped   : {skipped:,}',
        f'  Saved     : {_fmt(actual_saved)}  ({actual_saved:,} bytes)',
        f'  Reduction : {100 * actual_saved / size_before:.1f}%' if size_before else '  Reduction : 0%',
        f'  Time      : {elapsed:.1f}s',
        f'',
    ]

    for line in summary_lines:
        emit(line)

    if stats_file:
        try:
            stats_file.parent.mkdir(parents=True, exist_ok=True)
            with open(stats_file, 'a', encoding='utf-8') as f:
                f.write('\n'.join(summary_lines) + '\n')
            emit(f'Stats saved to {stats_file}')
        except Exception as e:
            print(f'[warn] Could not write stats file: {e}', file=sys.stderr)

    return {
        'total': total,
        'compressed': compressed,
        'skipped': skipped,
        'saved_bytes': actual_saved,
        'elapsed_sec': round(elapsed, 1),
        'size_before_bytes': size_before,
        'size_after_bytes': size_after,
    }


# ── CLI ───────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description='Compress images larger than a size threshold.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument(
        '--folder', default=DEFAULT_FOLDER,
        help='Root folder to scan (default: reddit_downloads)',
    )
    ap.add_argument(
        '--min-size-kb', type=int, default=DEFAULT_MIN_SIZE_KB,
        help='Only compress files larger than this (KB)',
    )
    ap.add_argument(
        '--quality', type=int, default=DEFAULT_QUALITY,
        help='JPEG quality 1-95 (PNG always uses lossless optimisation)',
    )
    ap.add_argument(
        '--progress-json', action='store_true',
        help='Emit JSON progress lines (used by the web UI)',
    )
    ap.add_argument(
        '--stats-file', default=None,
        help='Append before/after stats to this file (e.g. logs/compress_stats.txt)',
    )
    args = ap.parse_args()

    folder = Path(args.folder).resolve()
    if not folder.exists():
        print(f'Error: folder not found: {folder}', file=sys.stderr)
        sys.exit(1)

    run_compress(
        folder=folder,
        min_size_bytes=args.min_size_kb * 1024,
        quality=args.quality,
        progress_json=args.progress_json,
        stats_file=Path(args.stats_file) if args.stats_file else None,
    )


if __name__ == '__main__':
    main()
