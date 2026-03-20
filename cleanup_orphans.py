#!/usr/bin/env python3
"""
cleanup_orphans.py — Remove DB records and thumbnails for missing image files.

Two-phase cleanup:

  Phase 1 – DB orphans
    • Load every row from the `images` table.
    • For each row whose file_path does not exist on disk:
        - Delete the image record (cascade removes post_images rows).
        - If the parent post now has zero images linked, delete the post too.
        - Delete the matching thumbnail (if any).
        - Remove the stale phash_cache entry from duplicates.db.

  Phase 2 – Thumbnail orphans
    • Walk the entire reddit_downloads_thumbs tree.
    • For each .jpg thumbnail whose source image no longer exists (tried all
      known extensions), delete the thumbnail file.

Usage:
    python cleanup_orphans.py                  # live run
    python cleanup_orphans.py --dry-run        # preview only, no changes
    python cleanup_orphans.py --progress-json  # JSON progress (used by web UI)

Cron example (weekly, Sunday 4 AM):
    0 4 * * 0  cd /path/to/reddit_downloader && .venv/bin/python cleanup_orphans.py >> logs/cleanup.log 2>&1
"""

import argparse
import configparser
import json
import os
import signal
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional

import mysql.connector
from mysql.connector import pooling

# ── Stop flag ─────────────────────────────────────────────────────────────
_stop_requested = False


def _handle_stop(signum, frame):
    global _stop_requested
    _stop_requested = True


signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)

# ── Config ────────────────────────────────────────────────────────────────
DUPES_DB = Path('duplicates.db')

# Extensions that a source image might have (for thumbnail reverse-lookup)
SOURCE_EXTS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp',
               '.mp4', '.webm', '.mov', '.avi', '.mkv']


# ── Helpers ───────────────────────────────────────────────────────────────

def _emit(msg: str, cur: int = 0, tot: int = 0,
          phase: str = '', as_json: bool = False):
    if as_json:
        print(json.dumps({'message': msg, 'progress': cur,
                          'total': tot, 'phase': phase}), flush=True)
    else:
        bar = f'[{cur}/{tot}] ' if tot else ''
        print(f'{bar}{msg}', flush=True)


def _load_config():
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    return {
        'host':     cfg.get('mysql', 'host',     fallback='localhost'),
        'port':     cfg.getint('mysql', 'port',  fallback=3306),
        'user':     cfg.get('mysql', 'user',     fallback='root'),
        'password': cfg.get('mysql', 'password', fallback=''),
        'database': cfg.get('mysql', 'database', fallback='reddit_images'),
        'download_folder': cfg.get('general', 'download_folder',
                                   fallback='reddit_downloads'),
        'thumbs_folder':   cfg.get('general', 'thumbs_folder',
                                   fallback='reddit_downloads_thumbs'),
    }


def _create_pool(cfg: dict) -> pooling.MySQLConnectionPool:
    return pooling.MySQLConnectionPool(
        pool_name='cleanup_pool',
        pool_size=2,
        host=cfg['host'],
        port=cfg['port'],
        user=cfg['user'],
        password=cfg['password'],
        database=cfg['database'],
    )


def _invalidate_phash(file_path: str) -> None:
    if not DUPES_DB.exists():
        return
    try:
        conn = sqlite3.connect(str(DUPES_DB), timeout=10)
        conn.execute('DELETE FROM phash_cache WHERE path = ?', (file_path,))
        conn.commit()
        conn.close()
    except Exception:
        pass


def _thumb_for(image_path: Path, download_folder: Path,
               thumbs_folder: Path) -> Optional[Path]:
    """Return the expected thumbnail path for an image (always .jpg)."""
    try:
        rel = image_path.relative_to(download_folder)
    except ValueError:
        rel = Path(image_path.name)
    return (thumbs_folder / rel).with_suffix('.jpg')


def _source_exists(thumb_path: Path, download_folder: Path,
                   thumbs_folder: Path) -> bool:
    """Check whether any source-extension variant of a thumbnail still exists."""
    try:
        rel = thumb_path.relative_to(thumbs_folder)
    except ValueError:
        return False
    stem_rel = rel.with_suffix('')          # e.g. subreddit/filename
    for ext in SOURCE_EXTS:
        if (download_folder / stem_rel.parent / (stem_rel.name + ext)).exists():
            return True
    return False


# ── Phase 1: DB orphans ───────────────────────────────────────────────────

def phase1_db_orphans(
    pool: pooling.MySQLConnectionPool,
    dry_run: bool,
    thumbs_folder: Path,
    download_folder: Path,
    progress_json: bool,
) -> dict:
    def emit(msg, cur=0, tot=0):
        _emit(msg, cur, tot, phase='db', as_json=progress_json)

    emit('Phase 1: loading images from database…')

    try:
        conn = pool.get_connection()
    except Exception as e:
        emit(f'DB connection failed: {e}')
        return {'checked': 0, 'missing': 0, 'images_deleted': 0,
                'posts_deleted': 0, 'thumbs_deleted': 0}

    cur = conn.cursor(dictionary=True)
    cur.execute('SELECT id, file_path, filename FROM images')
    rows = cur.fetchall()
    total = len(rows)
    emit(f'Checking {total:,} image records…', 0, total)

    missing_ids   = []   # image IDs whose files are gone
    missing_paths = []   # their file_path strings
    thumbs_to_delete = []

    for i, row in enumerate(rows, 1):
        if _stop_requested:
            emit(f'Stopped at {i-1}/{total}.', i - 1, total)
            break

        fp_str  = row['file_path'] or ''
        fp      = Path(fp_str) if fp_str else None
        missing = not fp or not fp.exists()

        if missing:
            missing_ids.append(row['id'])
            missing_paths.append(fp_str)
            if fp:
                thumb = _thumb_for(fp, download_folder, thumbs_folder)
                if thumb.exists():
                    thumbs_to_delete.append(thumb)
            if progress_json:
                emit(f'Missing: {row["filename"] or fp_str}', i, total)

        if i % 500 == 0:
            emit(f'Checked {i:,}/{total:,}…', i, total)

    emit(f'Found {len(missing_ids):,} missing file(s).', total, total)

    images_deleted = posts_deleted = thumbs_deleted = 0

    if missing_ids and not dry_run:
        placeholders = ','.join(['%s'] * len(missing_ids))

        # Reconnect in case the file-scan loop caused the connection to time out
        try:
            conn.ping(reconnect=True, attempts=3, delay=2)
        except Exception:
            conn.reconnect(attempts=3, delay=2)

        # Find posts that will lose ALL their images
        cur.execute(f'''
            SELECT post_id, COUNT(*) AS total_links,
                   SUM(image_id IN ({placeholders})) AS missing_links
            FROM post_images
            GROUP BY post_id
            HAVING total_links = missing_links
        ''', missing_ids)
        posts_to_delete = [r['post_id'] for r in cur.fetchall()]

        # Delete image records (post_images cascade-deleted automatically)
        cur.execute(f'DELETE FROM images WHERE id IN ({placeholders})',
                    missing_ids)
        images_deleted = cur.rowcount

        # Delete posts that now have no images
        if posts_to_delete:
            pp = ','.join(['%s'] * len(posts_to_delete))
            cur.execute(f'DELETE FROM posts WHERE id IN ({pp})',
                        posts_to_delete)
            posts_deleted = cur.rowcount

        conn.commit()
        emit(f'Deleted {images_deleted:,} image record(s), '
             f'{posts_deleted:,} post(s).')

        # Delete thumbnails
        for tp in thumbs_to_delete:
            try:
                tp.unlink()
                thumbs_deleted += 1
            except OSError:
                pass

        # Invalidate phash cache
        for fp_str in missing_paths:
            if fp_str:
                _invalidate_phash(fp_str)

    elif dry_run and missing_ids:
        emit(f'[dry-run] Would delete {len(missing_ids):,} image(s), '
             f'up to {len(thumbs_to_delete):,} thumbnail(s).')

    cur.close()
    conn.close()

    return {
        'checked': total,
        'missing': len(missing_ids),
        'images_deleted': images_deleted,
        'posts_deleted': posts_deleted,
        'thumbs_deleted': thumbs_deleted,
    }


# ── Phase 2: thumbnail orphans ────────────────────────────────────────────

def phase2_thumb_orphans(
    cfg: dict,
    dry_run: bool,
    thumbs_folder: Path,
    download_folder: Path,
    progress_json: bool,
) -> dict:

    def emit(msg, cur=0, tot=0):
        _emit(msg, cur, tot, phase='thumbs', as_json=progress_json)

    if not thumbs_folder.exists():
        emit('Thumbs folder not found, skipping phase 2.')
        return {'checked': 0, 'orphaned': 0, 'deleted': 0}

    emit('Phase 2: scanning thumbnail folder…')

    all_thumbs = [
        p for p in thumbs_folder.rglob('*.jpg') if p.is_file()
    ]
    total = len(all_thumbs)
    emit(f'Found {total:,} thumbnail(s).', 0, total)

    orphaned = []
    for i, tp in enumerate(all_thumbs, 1):
        if _stop_requested:
            emit(f'Stopped at {i-1}/{total}.', i - 1, total)
            break
        if not _source_exists(tp, download_folder, thumbs_folder):
            orphaned.append(tp)
            if progress_json:
                emit(f'Orphan: {tp.name}', i, total)
        if i % 1000 == 0:
            emit(f'Checked {i:,}/{total:,}…', i, total)

    emit(f'Found {len(orphaned):,} orphaned thumbnail(s).', total, total)

    deleted = 0
    if orphaned and not dry_run:
        for tp in orphaned:
            try:
                tp.unlink()
                deleted += 1
            except OSError:
                pass
        emit(f'Deleted {deleted:,} orphaned thumbnail(s).')
    elif dry_run and orphaned:
        emit(f'[dry-run] Would delete {len(orphaned):,} orphaned thumbnail(s).')

    return {'checked': total, 'orphaned': len(orphaned), 'deleted': deleted}


# ── Main ──────────────────────────────────────────────────────────────────

def run_cleanup(
    dry_run: bool = False,
    progress_json: bool = False,
) -> dict:
    global _stop_requested
    _stop_requested = False

    t0  = time.time()
    cfg = _load_config()
    download_folder = Path(cfg['download_folder']).resolve()
    thumbs_folder   = Path(cfg['thumbs_folder']).resolve()

    def emit(msg, cur=0, tot=0):
        _emit(msg, cur, tot, as_json=progress_json)

    if dry_run:
        emit('*** DRY RUN — no changes will be made ***')

    try:
        pool = _create_pool(cfg)
    except Exception as e:
        emit(f'Failed to create DB connection pool: {e}')
        return {'checked': 0, 'missing': 0, 'images_deleted': 0,
                'posts_deleted': 0, 'thumbs_deleted': 0, 'elapsed_sec': 0}

    r1 = phase1_db_orphans(pool, dry_run, thumbs_folder, download_folder, progress_json)
    r2 = {}
    if not _stop_requested:
        r2 = phase2_thumb_orphans(cfg, dry_run, thumbs_folder, download_folder, progress_json)

    elapsed = time.time() - t0
    summary = (
        f'Done in {elapsed:.1f}s — '
        f'Phase 1: {r1["missing"]:,} missing images, '
        f'{r1["images_deleted"]:,} DB records removed, '
        f'{r1["posts_deleted"]:,} posts removed  |  '
        f'Phase 2: {r2.get("orphaned", 0):,} orphan thumbs, '
        f'{r2.get("deleted", 0):,} deleted'
        + (' [DRY RUN]' if dry_run else '')
    )
    emit(summary)
    return {**r1, **{f'thumb_{k}': v for k, v in r2.items()}, 'elapsed_sec': round(elapsed, 1)}


def main():
    ap = argparse.ArgumentParser(
        description='Remove DB records and thumbnails for missing image files.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument('--dry-run', action='store_true',
                    help='Preview only, make no changes')
    ap.add_argument('--progress-json', action='store_true',
                    help='Emit JSON progress lines (used by web UI)')
    args = ap.parse_args()
    run_cleanup(dry_run=args.dry_run, progress_json=args.progress_json)


if __name__ == '__main__':
    main()
