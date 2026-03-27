#!/usr/bin/env python3
"""
png_to_jpg.py — Convert every .png on disk to .jpg (< 1 MB) and update the DB.

For each PNG found:
  1. Open with Pillow, composite transparency onto white.
  2. Encode as progressive JPEG at --quality (default 85).
  3. If result >= 1 MB, downscale progressively until it fits.
  4. Write <name>.jpg next to the original, delete the .png.
  5. UPDATE images SET file_path, filename, file_hash, file_size WHERE file_path = <old>.

DB writes use a single connection from the pool; rows are committed in batches
of --batch-size (default 100) to avoid long open transactions.

Usage:
    uv run python png_to_jpg.py                  # dry-run first to see counts
    uv run python png_to_jpg.py --dry-run
    uv run python png_to_jpg.py --folder reddit_downloads --quality 85
"""

import argparse
import hashlib
import io
import os
import signal
import sys
import time
from pathlib import Path

try:
    from PIL import Image, ImageFile
    ImageFile.LOAD_TRUNCATED_IMAGES = True
except ImportError:
    print("Pillow not installed. Run: pip install Pillow", file=sys.stderr)
    sys.exit(1)

import configparser
import mysql.connector
from mysql.connector import pooling

# ── Constants ──────────────────────────────────────────────────────────────
DEFAULT_FOLDER     = 'reddit_downloads'
DEFAULT_QUALITY    = 85
DEFAULT_BATCH_SIZE = 100
MAX_BYTES          = 1 * 1024 * 1024        # 1 MB hard limit for output
RESIZE_STEPS       = [1920, 1440, 1080, 720, 480, 360]
FILE_TIMEOUT_SECS  = 30
SKIP_DIRS          = {'deleted', 'thumbs', '__pycache__'}

# ── Graceful stop ──────────────────────────────────────────────────────────
_stop = False

def _on_stop(sig, frame):
    global _stop
    _stop = True
    print('\nStop requested — finishing current file…', flush=True)

signal.signal(signal.SIGINT,  _on_stop)
signal.signal(signal.SIGTERM, _on_stop)


# ── Helpers ────────────────────────────────────────────────────────────────

def _fmt(n: int) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if abs(n) < 1024:
            return f'{n:.1f} {unit}'
        n /= 1024
    return f'{n:.1f} TB'


def _md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def _to_rgb(img: Image.Image) -> Image.Image:
    """Flatten alpha/palette onto white, return RGB image."""
    if img.mode in ('RGBA', 'LA'):
        bg = Image.new('RGB', img.size, (255, 255, 255))
        bg.paste(img.convert('RGB'), mask=img.split()[-1])
        return bg
    if img.mode != 'RGB':
        return img.convert('RGB')
    return img


# ── Conversion ─────────────────────────────────────────────────────────────

def convert_png(src: Path, quality: int) -> 'Path | None':
    """
    Convert src (.png) → dst (.jpg) next to it.
    Returns dst Path on success, None on failure or if dst already exists.
    """
    dst = src.with_suffix('.jpg')
    if dst.exists():
        print(f'  skip — .jpg already exists: {dst.name}', flush=True)
        return None

    def _alarm(sig, frame):
        raise TimeoutError(f'PIL timed out on {src.name}')

    try:
        signal.signal(signal.SIGALRM, _alarm)
        signal.alarm(FILE_TIMEOUT_SECS)

        with Image.open(src) as img:
            rgb = _to_rgb(img)

        # First encode attempt
        buf = io.BytesIO()
        rgb.save(buf, 'JPEG', quality=quality, optimize=True, progressive=True)

        # Progressively downscale until under 1 MB
        if buf.tell() >= MAX_BYTES:
            for max_dim in RESIZE_STEPS:
                w, h = rgb.size
                if max(w, h) > max_dim:
                    scale = max_dim / max(w, h)
                    rgb = rgb.resize(
                        (max(1, int(w * scale)), max(1, int(h * scale))),
                        Image.LANCZOS,
                    )
                buf = io.BytesIO()
                rgb.save(buf, 'JPEG', quality=quality, optimize=True, progressive=True)
                if buf.tell() < MAX_BYTES:
                    break

        signal.alarm(0)
        dst.write_bytes(buf.getvalue())
        return dst

    except TimeoutError as e:
        signal.alarm(0)
        print(f'  warn  timeout: {src.name}', file=sys.stderr, flush=True)
    except Exception as e:
        signal.alarm(0)
        if 'cannot identify' not in str(e):
            print(f'  warn  {src.name}: {e}', file=sys.stderr, flush=True)

    if dst.exists():
        dst.unlink(missing_ok=True)
    return None


# ── DB pool ────────────────────────────────────────────────────────────────

def _build_pool(cfg: configparser.ConfigParser) -> pooling.MySQLConnectionPool:
    return pooling.MySQLConnectionPool(
        pool_name='png2jpg_pool',
        pool_size=2,                        # only 1 conn used; 2 for safety
        host    =cfg.get('mysql', 'host',     fallback='localhost'),
        port    =cfg.getint('mysql', 'port',  fallback=3306),
        user    =cfg.get('mysql', 'user',     fallback='root'),
        password=cfg.get('mysql', 'password', fallback=''),
        database=cfg.get('mysql', 'database', fallback='reddit_images'),
        connection_timeout=10,
    )


# ── Core ───────────────────────────────────────────────────────────────────

def run(folder: Path, quality: int, batch_size: int, dry_run: bool) -> None:
    global _stop

    # Scan
    print(f'Scanning {folder} for PNG files …', flush=True)
    candidates: list[Path] = []
    for root, dirs, files in os.walk(folder):
        dirs[:] = [d for d in dirs if d.lower() not in SKIP_DIRS]
        for fname in files:
            if fname.lower().endswith('.png'):
                candidates.append(Path(root) / fname)

    total = len(candidates)
    total_bytes = sum(p.stat().st_size for p in candidates)
    print(f'Found {total:,} PNG files  ({_fmt(total_bytes)} total)', flush=True)

    if total == 0 or dry_run:
        if dry_run:
            print('[dry-run] No changes made.', flush=True)
        return

    # DB
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    pool   = _build_pool(cfg)
    conn   = pool.get_connection()
    cursor = conn.cursor()
    print('DB pool ready, single connection checked out.', flush=True)

    converted = skipped = failed = db_updated = db_missed = 0
    total_saved = 0
    pending = 0
    t0 = time.time()

    try:
        for i, src in enumerate(candidates, 1):
            if _stop:
                print(f'\nStopped at {i - 1}/{total}.', flush=True)
                break

            old_size = src.stat().st_size
            print(f'[{i}/{total}] {src.name}  {_fmt(old_size)}', end=' … ', flush=True)

            dst = convert_png(src, quality)

            if dst is None:
                skipped += 1
                print('skipped', flush=True)
                continue

            new_size  = dst.stat().st_size
            new_hash  = _md5(dst)
            saved     = old_size - new_size

            print(f'→ {dst.name}  {_fmt(new_size)}  (−{_fmt(saved)})', flush=True)

            # Update DB row
            try:
                cursor.execute(
                    """
                    UPDATE images
                       SET file_path = %s,
                           filename  = %s,
                           file_hash = %s,
                           file_size = %s
                     WHERE file_path = %s
                    """,
                    (str(dst), dst.name, new_hash, new_size, str(src)),
                )
                if cursor.rowcount > 0:
                    db_updated += 1
                else:
                    db_missed += 1
                    print(f'  warn  no DB row matched for {src}', file=sys.stderr, flush=True)
                pending += 1
            except mysql.connector.errors.IntegrityError as e:
                # Duplicate hash — two PNGs that produce identical JPEG bytes
                print(f'  warn  duplicate hash for {dst.name}: {e}', file=sys.stderr, flush=True)
                dst.unlink(missing_ok=True)
                failed += 1
                continue
            except Exception as e:
                print(f'  warn  DB error for {dst.name}: {e}', file=sys.stderr, flush=True)

            # Remove original PNG
            try:
                src.unlink()
            except Exception as e:
                print(f'  warn  could not delete {src.name}: {e}', file=sys.stderr, flush=True)

            converted += 1
            total_saved += saved

            # Batch commit
            if pending >= batch_size:
                conn.commit()
                pending = 0

    finally:
        if pending:
            conn.commit()
        cursor.close()
        conn.close()

    elapsed = time.time() - t0
    print(f'\n{"=" * 40}')
    print(f'Converted  : {converted:,}')
    print(f'Skipped    : {skipped:,}')
    print(f'Failed     : {failed:,}')
    print(f'DB updated : {db_updated:,}')
    print(f'DB missed  : {db_missed:,}  (files on disk but not in DB)')
    print(f'Disk saved : {_fmt(total_saved)}')
    print(f'Time       : {elapsed:.1f}s')


# ── CLI ────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description='Convert all PNG files to JPEG (< 1 MB) and update the DB.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument('--folder',     default=DEFAULT_FOLDER,
                    help='Root folder to scan')
    ap.add_argument('--quality',    type=int, default=DEFAULT_QUALITY,
                    help='JPEG quality 1-95')
    ap.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE,
                    help='Commit DB updates every N files')
    ap.add_argument('--dry-run',    action='store_true',
                    help='Count PNGs without converting anything')
    args = ap.parse_args()

    folder = Path(args.folder).resolve()
    if not folder.exists():
        print(f'Error: folder not found: {folder}', file=sys.stderr)
        sys.exit(1)

    run(folder, args.quality, args.batch_size, args.dry_run)


if __name__ == '__main__':
    main()
