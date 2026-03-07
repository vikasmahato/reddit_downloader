#!/usr/bin/env python3
"""
compute_hashes.py — Step 1: Compute perceptual hashes for a folder of images.

Scans a folder, computes pHash (DCT) for each image file, and caches results
in duplicates.db (phash_cache table).  Run this before scan_duplicates.py.

Usage:
    python compute_hashes.py --folder reddit_downloads/pics
    python compute_hashes.py --folder reddit_downloads/pics --progress-json
"""

import argparse
import json
import multiprocessing as mp
import os
import signal
import sqlite3
import sys
from pathlib import Path
from typing import Optional

import cv2
import numpy as np

# ── Stop flag ─────────────────────────────────────────────────────────────
_stop_requested = False


def _handle_stop(signum, frame):
    global _stop_requested
    _stop_requested = True


signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)

# ── Constants ──────────────────────────────────────────────────────────────
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
DUPES_DB = Path('duplicates.db')
DEFAULT_HASH_SIZE = 8


# ── pHash (OpenCV DCT) ────────────────────────────────────────────────────
def compute_phash(path: Path, hash_size: int = DEFAULT_HASH_SIZE) -> Optional[int]:
    """Compute standard DCT-based perceptual hash. Returns None if unreadable."""
    img = cv2.imread(str(path), cv2.IMREAD_GRAYSCALE)
    if img is None:
        if path.suffix.lower() == '.gif':
            cap = cv2.VideoCapture(str(path))
            ret, frame = cap.read()
            cap.release()
            if not ret:
                return None
            img = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        else:
            return None
    dct_size = hash_size * 4
    small = cv2.resize(img, (dct_size, dct_size), interpolation=cv2.INTER_AREA)
    dct = cv2.dct(np.float32(small))
    low = dct[:hash_size, :hash_size]
    mean = (low.sum() - low[0, 0]) / (hash_size * hash_size - 1)
    bits = (low.flatten() > mean).astype(np.uint8)
    rem = len(bits) % 8
    if rem:
        bits = np.append(bits, np.zeros(8 - rem, dtype=np.uint8))
    return int.from_bytes(np.packbits(bits).tobytes(), 'big')


def _phash_worker(args: tuple) -> tuple:
    """Top-level worker for multiprocessing Pool."""
    path_str, mtime, size, hash_size = args
    h = compute_phash(Path(path_str), hash_size)
    return (path_str, mtime, size, h)


# ── DB init ──────────────────────────────────────────────────────────────
def _init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS phash_cache (
            path  TEXT    PRIMARY KEY,
            mtime REAL    NOT NULL,
            size  INTEGER NOT NULL,
            phash TEXT
        );
    """)
    conn.commit()
    # Migrate: allow NULL phash (old schema had NOT NULL)
    try:
        conn.execute("INSERT INTO phash_cache(path,mtime,size,phash) VALUES('__chk__',0,0,NULL)")
        conn.execute("DELETE FROM phash_cache WHERE path='__chk__'")
        conn.commit()
    except sqlite3.IntegrityError:
        conn.executescript("""
            BEGIN;
            CREATE TABLE phash_cache_new (
                path TEXT PRIMARY KEY, mtime REAL NOT NULL,
                size INTEGER NOT NULL, phash TEXT
            );
            INSERT INTO phash_cache_new SELECT path,mtime,size,phash FROM phash_cache;
            DROP TABLE phash_cache;
            ALTER TABLE phash_cache_new RENAME TO phash_cache;
            COMMIT;
        """)
    return conn


# ── Core ──────────────────────────────────────────────────────────────────
def run_compute_hashes(
    folder: Path,
    db_path: Path = DUPES_DB,
    hash_size: int = DEFAULT_HASH_SIZE,
    progress_cb=None,
) -> dict:
    """
    Compute pHash for all image files in `folder` and store in phash_cache.
    Already-cached and up-to-date files are skipped.
    Handles SIGTERM/SIGINT gracefully by saving computed hashes before exit.
    """
    global _stop_requested
    _stop_requested = False

    def progress(msg: str, cur: int = 0, tot: int = 0):
        if progress_cb:
            progress_cb(msg, cur, tot)
        else:
            print(msg)

    folder = folder.resolve()
    conn = _init_db(db_path)

    # Load existing cache
    cache_rows = conn.execute('SELECT path, mtime, size, phash FROM phash_cache').fetchall()
    phash_cache: dict = {}
    for row in cache_rows:
        try:
            ph = int(row['phash'], 16) if row['phash'] else None
            phash_cache[row['path']] = (row['mtime'], row['size'], ph)
        except Exception:
            pass
    progress(f'Loaded {len(phash_cache):,} cached hashes.', 0, 0)

    # List image files in folder
    all_paths = [
        p for p in folder.rglob('*')
        if p.is_file() and p.suffix.lower() in IMAGE_EXTENSIONS
    ]
    total = len(all_paths)
    progress(f'Found {total:,} images in {folder.name}.', 0, total)

    # Separate cached from uncached
    to_hash = []
    cache_hits = 0
    skipped = 0
    for fp in all_paths:
        sp = str(fp)
        try:
            stat = fp.stat()
            mtime, size = stat.st_mtime, stat.st_size
        except Exception:
            skipped += 1
            continue
        cached = phash_cache.get(sp)
        if cached and abs(cached[0] - mtime) < 1.0 and cached[1] == size:
            cache_hits += 1
            if cached[2] is None:
                skipped += 1  # previously failed
        else:
            to_hash.append((sp, mtime, size, hash_size))

    progress(
        f'{cache_hits:,} already cached, {len(to_hash):,} to compute, {skipped:,} skipped.',
        cache_hits, total,
    )

    if not to_hash:
        progress('All hashes up to date.', total, total)
        conn.close()
        return {'total': total, 'cached': cache_hits, 'computed': 0, 'skipped': skipped}

    # Hash uncached files in parallel
    n_workers = max(1, min(os.cpu_count() or 1, 4))
    chunk = max(50, len(to_hash) // (n_workers * 20))
    done = 0
    new_entries: list = []

    with mp.Pool(processes=n_workers, maxtasksperchild=200) as pool:
        for result in pool.imap_unordered(_phash_worker, to_hash, chunksize=chunk):
            if _stop_requested:
                pool.terminate()
                progress(
                    f'Stopped at {done:,}/{len(to_hash):,} new files.',
                    cache_hits + done, total,
                )
                break
            sp, mtime, size, h = result
            done += 1
            if h is not None:
                new_entries.append((sp, mtime, size, hex(h)))
            else:
                skipped += 1
                new_entries.append((sp, mtime, size, None))
            if done % 200 == 0:
                progress(
                    f'Hashing {done:,}/{len(to_hash):,} ({n_workers} workers)…',
                    cache_hits + done, total,
                )

    # Save to cache (including failures so they're skipped next time)
    if new_entries:
        conn.executemany(
            'INSERT OR REPLACE INTO phash_cache (path, mtime, size, phash) VALUES (?,?,?,?)',
            new_entries,
        )
        conn.commit()

    conn.close()
    n_computed = sum(1 for e in new_entries if e[3] is not None)
    n_failed = len(new_entries) - n_computed
    progress(
        f'Done. {n_computed:,} hashes computed, {n_failed:,} failed/skipped.',
        total, total,
    )
    return {'total': total, 'cached': cache_hits, 'computed': n_computed, 'skipped': skipped}


# ── CLI ───────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description='Compute perceptual hashes for images in a folder.',
        epilog='Run this first, then use scan_duplicates.py to find duplicates.',
    )
    ap.add_argument('--folder', required=True,
                    help='Path to the folder to process (e.g. reddit_downloads/pics)')
    ap.add_argument('--hash-size', type=int, default=DEFAULT_HASH_SIZE,
                    help=f'Hash dimension NxN (default: {DEFAULT_HASH_SIZE} → {DEFAULT_HASH_SIZE**2}-bit)')
    ap.add_argument('--progress-json', action='store_true',
                    help='Emit JSON progress lines (used by web UI)')
    args = ap.parse_args()

    folder = Path(args.folder).resolve()
    if not folder.exists():
        print(f'Error: {folder} not found.', file=sys.stderr)
        sys.exit(1)

    def cb(msg: str, cur: int = 0, tot: int = 0):
        if args.progress_json:
            print(json.dumps({'message': msg, 'progress': cur, 'total': tot}), flush=True)
        else:
            print(msg)

    run_compute_hashes(folder, DUPES_DB, args.hash_size, cb)


if __name__ == '__main__':
    main()
