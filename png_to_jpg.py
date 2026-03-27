#!/usr/bin/env python3
"""
png_to_jpg.py — Convert every .png on disk to .jpg (< 1 MB) and update the DB.

For each PNG found:
  1. Open with Pillow, composite transparency onto white.
  2. Encode as progressive JPEG at --quality (default 85).
  3. If result >= 1 MB, downscale progressively until it fits.
  4. Write <name>.jpg next to the original, delete the .png.
  5. UPDATE images SET file_path, filename, file_hash, file_size WHERE file_path = <old_relative>.

DB writes reuse a single connection from the pool; rows are committed in
batches of --batch-size (default 100) to avoid long open transactions.

--repair   Fix DB rows for files already converted (jpg on disk, DB still has
           .png path). Run this after an interrupted previous run.

Usage:
    cd ~/hdd/reddit_downloader
    uv run python png_to_jpg.py --dry-run          # count only
    uv run python png_to_jpg.py --repair           # fix stale DB rows first
    uv run python png_to_jpg.py                    # convert remaining PNGs
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

# Scripts are run from the project root (e.g. ~/hdd/reddit_downloader/).
# The DB stores paths relative to that root, e.g. "reddit_downloads/sub/file.png".
_CWD = Path.cwd()

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


def _rel(path: Path) -> str:
    """Return path relative to cwd as a forward-slash string (matches DB format)."""
    try:
        return str(path.relative_to(_CWD)).replace('\\', '/')
    except ValueError:
        return str(path).replace('\\', '/')


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
    Returns dst Path on success, None on failure or collision.
    """
    dst = src.with_suffix('.jpg')
    if dst.exists():
        return None   # already converted; caller handles DB repair

    def _alarm(sig, frame):
        raise TimeoutError()

    try:
        signal.signal(signal.SIGALRM, _alarm)
        signal.alarm(FILE_TIMEOUT_SECS)

        with Image.open(src) as img:
            rgb = _to_rgb(img)

        buf = io.BytesIO()
        rgb.save(buf, 'JPEG', quality=quality, optimize=True, progressive=True)

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

    except TimeoutError:
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
        pool_size=2,
        host             =cfg.get('mysql', 'host',     fallback='localhost'),
        port             =cfg.getint('mysql', 'port',  fallback=3306),
        user             =cfg.get('mysql', 'user',     fallback='root'),
        password         =cfg.get('mysql', 'password', fallback=''),
        database         =cfg.get('mysql', 'database', fallback='reddit_images'),
        connection_timeout=10,
    )


def _db_update(cursor, old_rel: str, dst: Path) -> int:
    """UPDATE the images row. Returns rowcount (0 = no match, 1 = updated)."""
    new_rel  = _rel(dst)
    new_hash = _md5(dst)
    new_size = dst.stat().st_size
    cursor.execute(
        """
        UPDATE images
           SET file_path = %s,
               filename  = %s,
               file_hash = %s,
               file_size = %s
         WHERE file_path = %s
        """,
        (new_rel, dst.name, new_hash, new_size, old_rel),
    )
    return cursor.rowcount


# ── Repair mode (fix stale DB rows from a previous broken run) ─────────────

def repair(folder: Path, batch_size: int) -> None:
    """
    Find .jpg files on disk whose DB row still records a .png path.
    Happens when a previous run converted files but couldn't update the DB.
    """
    print(f'Repair mode: scanning {folder} for orphaned .jpg files …', flush=True)

    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    pool   = _build_pool(cfg)
    conn   = pool.get_connection()
    cursor = conn.cursor()

    # Fetch all DB rows that still have .png paths
    cursor.execute("SELECT id, file_path, filename FROM images WHERE filename LIKE '%.png'")
    png_rows = cursor.fetchall()
    print(f'DB has {len(png_rows):,} rows with .png filenames.', flush=True)

    fixed = missed = 0
    pending = 0

    for db_id, db_path, db_filename in png_rows:
        if _stop:
            break
        # Compute what the jpg path would be
        jpg_rel  = db_path[:-4] + '.jpg'          # replace trailing .png with .jpg
        jpg_abs  = _CWD / jpg_rel
        if not jpg_abs.exists():
            continue  # not converted yet — leave for main run

        # jpg exists on disk but DB still says .png → fix it
        try:
            rows = _db_update(cursor, db_path, jpg_abs)
            if rows:
                fixed += 1
                pending += 1
                print(f'  fixed  {db_filename} → {jpg_abs.name}', flush=True)
            else:
                missed += 1
        except mysql.connector.errors.IntegrityError:
            # hash collision with another row
            print(f'  warn  hash collision for {jpg_abs.name} — skipping', file=sys.stderr, flush=True)
        except Exception as e:
            print(f'  warn  DB error for {jpg_abs.name}: {e}', file=sys.stderr, flush=True)

        if pending >= batch_size:
            conn.commit()
            pending = 0

    if pending:
        conn.commit()
    cursor.close()
    conn.close()

    print(f'\nRepair done — fixed: {fixed:,}  not-yet-converted: {missed:,}', flush=True)


# ── Main conversion run ────────────────────────────────────────────────────

def run(folder: Path, quality: int, batch_size: int, dry_run: bool) -> None:
    global _stop

    print(f'Scanning {folder} for PNG files …', flush=True)
    candidates: list[Path] = []
    for root, dirs, files in os.walk(folder):
        dirs[:] = [d for d in dirs if d.lower() not in SKIP_DIRS]
        for fname in files:
            if fname.lower().endswith('.png'):
                candidates.append(Path(root) / fname)

    total      = len(candidates)
    total_size = sum(p.stat().st_size for p in candidates)
    print(f'Found {total:,} PNG files  ({_fmt(total_size)} total)', flush=True)

    if dry_run:
        print('[dry-run] No changes made.', flush=True)
        return
    if total == 0:
        print('Nothing to do.', flush=True)
        return

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
            old_rel  = _rel(src)
            print(f'[{i}/{total}] {src.name}  {_fmt(old_size)}', end=' … ', flush=True)

            dst = convert_png(src, quality)

            if dst is None:
                # .jpg already exists — treat as a repair case
                dst_existing = src.with_suffix('.jpg')
                if dst_existing.exists():
                    try:
                        rows = _db_update(cursor, old_rel, dst_existing)
                        pending += 1
                        if rows:
                            db_updated += 1
                            print(f'repaired DB → {dst_existing.name}', flush=True)
                        else:
                            db_missed += 1
                            print(f'jpg exists, no DB match for {old_rel}', flush=True)
                        # Remove the stale .png since .jpg is already there
                        src.unlink(missing_ok=True)
                    except Exception as e:
                        print(f'  warn  repair DB error: {e}', file=sys.stderr, flush=True)
                else:
                    skipped += 1
                    print('skipped', flush=True)
                continue

            new_size = dst.stat().st_size
            saved    = old_size - new_size
            new_rel  = _rel(dst)
            print(f'→ {dst.name}  {_fmt(new_size)}  (−{_fmt(saved)})', flush=True)

            try:
                rows = _db_update(cursor, old_rel, dst)
                if rows:
                    db_updated += 1
                else:
                    db_missed += 1
                    print(f'  warn  no DB row for {old_rel}', file=sys.stderr, flush=True)
                pending += 1
            except mysql.connector.errors.IntegrityError as e:
                print(f'  warn  duplicate hash {dst.name}: {e}', file=sys.stderr, flush=True)
                dst.unlink(missing_ok=True)
                failed += 1
                continue
            except Exception as e:
                print(f'  warn  DB error {dst.name}: {e}', file=sys.stderr, flush=True)

            try:
                src.unlink()
            except Exception as e:
                print(f'  warn  delete {src.name}: {e}', file=sys.stderr, flush=True)

            converted  += 1
            total_saved += saved

            if pending >= batch_size:
                conn.commit()
                pending = 0

    finally:
        if pending:
            conn.commit()
        cursor.close()
        conn.close()

    elapsed = time.time() - t0
    print(f'\n{"=" * 42}')
    print(f'Converted  : {converted:,}')
    print(f'Skipped    : {skipped:,}')
    print(f'Failed     : {failed:,}')
    print(f'DB updated : {db_updated:,}')
    print(f'DB missed  : {db_missed:,}  (on disk but not in DB)')
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
    ap.add_argument('--repair',     action='store_true',
                    help='Fix DB rows for already-converted files (.jpg on disk, .png in DB)')
    args = ap.parse_args()

    folder = Path(args.folder).resolve()
    if not folder.exists():
        print(f'Error: folder not found: {folder}', file=sys.stderr)
        sys.exit(1)

    if args.repair:
        repair(folder, args.batch_size)
    else:
        run(folder, args.quality, args.batch_size, args.dry_run)


if __name__ == '__main__':
    main()
