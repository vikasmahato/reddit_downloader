#!/usr/bin/env python3
"""
scan_duplicates.py

Uses OpenCV perceptual hashing (pHash via DCT) to detect visually similar
or identical media files, even when files differ slightly in compression,
resolution, watermarks, or encoding.

Algorithm:
  1. Compute pHash (64-bit DCT hash) for every file with OpenCV
  2. Build a BK-tree indexed by pHash for O(n log n) similarity search
  3. Use Union-Find for transitive grouping (A≈B and B≈C → same group)
  4. Compute MD5 for duplicate-group files only → one batch MySQL query
  5. Write all results to duplicates.db (SQLite)

Usage:
    python scan_duplicates.py [--threshold 10] [--hash-size 8]
    python scan_duplicates.py --threshold 5   # strict (near-identical only)
    python scan_duplicates.py --threshold 20  # loose (visually similar)

Requires:  pip install opencv-python numpy mysql-connector-python
"""

import argparse
import configparser
import hashlib
import json
import multiprocessing as mp
import os
import signal
import sqlite3
import sys
import time
from pathlib import Path
from typing import Callable, Optional

import cv2
import numpy as np
import mysql.connector

# ── Stop flag (set by SIGTERM handler) ───────────────────────────────────
_stop_requested = False

def _handle_sigterm(signum, frame):
    global _stop_requested
    _stop_requested = True

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT,  _handle_sigterm)

# ── Constants ─────────────────────────────────────────────────────────────

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
VIDEO_EXTENSIONS = {'.mp4', '.webm', '.mov', '.avi', '.mkv'}
MEDIA_EXTENSIONS = IMAGE_EXTENSIONS | VIDEO_EXTENSIONS  # legacy reference

DUPES_DB      = Path('duplicates.db')
DOWNLOADS_DIR = Path('reddit_downloads')

DEFAULT_THRESHOLD = 10   # Hamming bits (out of 64). 0=exact, ≤10=near-identical, ≤20=similar
DEFAULT_HASH_SIZE = 8    # Result is hash_size² bits (8 → 64-bit hash)


# ── Perceptual hash (OpenCV DCT) ──────────────────────────────────────────

def compute_phash(path: Path, hash_size: int = DEFAULT_HASH_SIZE) -> Optional[int]:
    """
    Standard pHash: resize → grayscale → DCT → keep low-frequency corner →
    threshold against mean → pack into an integer.

    Returns None if the file cannot be decoded.
    For video/GIF, uses the first readable frame.
    """
    img = cv2.imread(str(path), cv2.IMREAD_GRAYSCALE)

    if img is None:
        # GIF fallback via VideoCapture (first frame only)
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
    small    = cv2.resize(img, (dct_size, dct_size), interpolation=cv2.INTER_AREA)
    dct      = cv2.dct(np.float32(small))
    low      = dct[:hash_size, :hash_size]          # low-frequency block

    # Mean excluding the DC coefficient (position [0,0])
    mean = (low.sum() - low[0, 0]) / (hash_size * hash_size - 1)

    bits = (low.flatten() > mean).astype(np.uint8)
    rem  = len(bits) % 8
    if rem:
        bits = np.append(bits, np.zeros(8 - rem, dtype=np.uint8))

    return int.from_bytes(np.packbits(bits).tobytes(), 'big')


def _phash_worker(args: tuple) -> tuple:
    """Top-level worker for multiprocessing Pool: (path_str, mtime, size, hash_size) → (path_str, mtime, size, phash_int_or_None)"""
    path_str, mtime, size, hash_size = args
    h = compute_phash(Path(path_str), hash_size)
    return (path_str, mtime, size, h)


def hamming(a: int, b: int) -> int:
    return bin(a ^ b).count('1')


# ── BK-Tree ───────────────────────────────────────────────────────────────

class _BKNode:
    __slots__ = ('value', 'paths', 'children')

    def __init__(self, value: int, path: str):
        self.value    = value
        self.paths    = [path]              # files with this exact hash
        self.children: dict[int, '_BKNode'] = {}


class BKTree:
    """
    Burkhard-Keller tree for Hamming-distance nearest-neighbour search.
    Multiple files sharing the exact same hash are stored at the same node.

    Build:  O(n log n) average
    Query:  O(n^ε) for small ε — much faster than linear scan in practice
    """

    def __init__(self):
        self.root: Optional[_BKNode] = None

    def add(self, value: int, path: str) -> None:
        if self.root is None:
            self.root = _BKNode(value, path)
            return
        node = self.root
        while True:
            d = hamming(value, node.value)
            if d == 0:
                node.paths.append(path)          # exact hash duplicate
                return
            child = node.children.get(d)
            if child is None:
                node.children[d] = _BKNode(value, path)
                return
            node = child

    def search(self, value: int, threshold: int) -> list[tuple[str, int]]:
        """Return [(path, hamming_distance)] for every entry within threshold."""
        if self.root is None:
            return []
        results: list[tuple[str, int]] = []
        stack = [self.root]
        while stack:
            node = stack.pop()
            d = hamming(value, node.value)
            if d <= threshold:
                for p in node.paths:
                    results.append((p, d))
            lo = max(0, d - threshold)
            hi = d + threshold
            for cd, child in node.children.items():
                if lo <= cd <= hi:
                    stack.append(child)
        return results


# ── Union-Find ────────────────────────────────────────────────────────────

class UnionFind:
    """Path-compressed, rank-union disjoint-set for transitive grouping."""

    def __init__(self):
        self._parent: dict[str, str] = {}
        self._rank:   dict[str, int] = {}

    def _ensure(self, x: str):
        if x not in self._parent:
            self._parent[x] = x
            self._rank[x]   = 0

    def find(self, x: str) -> str:
        self._ensure(x)
        while self._parent[x] != x:
            self._parent[x] = self._parent[self._parent[x]]   # halving
            x = self._parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self._rank[ra] < self._rank[rb]:
            ra, rb = rb, ra
        self._parent[rb] = ra
        if self._rank[ra] == self._rank[rb]:
            self._rank[ra] += 1

    def groups(self) -> dict[str, list[str]]:
        g: dict[str, list[str]] = {}
        for x in self._parent:
            g.setdefault(self.find(x), []).append(x)
        return g


# ── SQLite schema ─────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS scan_info (
    id                  INTEGER PRIMARY KEY,
    scanned_at          TEXT,
    scan_duration_sec   REAL,
    total_files_scanned INTEGER,
    total_groups        INTEGER,
    total_wasted_bytes  INTEGER,
    threshold           INTEGER,
    hash_size           INTEGER,
    is_partial          INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS dup_groups (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    file_hash    TEXT    NOT NULL UNIQUE,
    file_count   INTEGER NOT NULL,
    total_size   INTEGER NOT NULL,
    wasted_size  INTEGER NOT NULL,
    min_distance INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS dup_files (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id   INTEGER NOT NULL,
    file_path  TEXT    NOT NULL,
    file_size  INTEGER NOT NULL,
    phash      TEXT,
    image_id   INTEGER,
    post_id    INTEGER,
    reddit_id  TEXT,
    post_title TEXT,
    subreddit  TEXT,
    permalink  TEXT,
    score      INTEGER,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (group_id) REFERENCES dup_groups(id)
);
CREATE INDEX IF NOT EXISTS idx_df_group   ON dup_files(group_id);
CREATE INDEX IF NOT EXISTS idx_df_deleted ON dup_files(group_id, is_deleted);
CREATE INDEX IF NOT EXISTS idx_df_imgid   ON dup_files(image_id);
CREATE TABLE IF NOT EXISTS phash_cache (
    path  TEXT    PRIMARY KEY,
    mtime REAL    NOT NULL,
    size  INTEGER NOT NULL,
    phash TEXT             -- NULL means previously attempted and failed
);
"""


def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()

    # Migrate phash_cache if it still has NOT NULL on phash (old schema)
    try:
        conn.execute("INSERT INTO phash_cache (path,mtime,size,phash) VALUES ('__chk__',0,0,NULL)")
        conn.execute("DELETE FROM phash_cache WHERE path='__chk__'")
        conn.commit()
    except sqlite3.IntegrityError:
        # Old schema — recreate table keeping existing data
        conn.executescript("""
            BEGIN;
            CREATE TABLE phash_cache_new (
                path  TEXT    PRIMARY KEY,
                mtime REAL    NOT NULL,
                size  INTEGER NOT NULL,
                phash TEXT
            );
            INSERT INTO phash_cache_new SELECT path, mtime, size, phash FROM phash_cache;
            DROP TABLE phash_cache;
            ALTER TABLE phash_cache_new RENAME TO phash_cache;
            COMMIT;
        """)

    return conn


# ── MD5 for prod-DB lookup ────────────────────────────────────────────────

def compute_md5(path: Path, chunk: int = 65536) -> Optional[str]:
    h = hashlib.md5()
    try:
        with open(path, 'rb') as f:
            for block in iter(lambda: f.read(chunk), b''):
                h.update(block)
        return h.hexdigest()
    except Exception:
        return None


# ── Core scan ─────────────────────────────────────────────────────────────

def _write_results(
    sc, sdb,
    raw_groups: dict,
    pair_min_d: dict,
    path_to_phash: dict,
    path_to_md5: dict,
    md5_to_db: dict,
) -> int:
    """Write groups to SQLite. Returns total wasted bytes."""
    total_wasted = 0
    for root, members in raw_groups.items():
        sizes = []
        for sp in members:
            try:
                sizes.append(Path(sp).stat().st_size)
            except Exception:
                sizes.append(0)

        total_size  = sum(sizes)
        wasted_size = total_size - (min(sizes) if sizes else 0)
        total_wasted += wasted_size

        min_dist = min(
            (d for (a, b), d in pair_min_d.items()
             if a in members and b in members),
            default=0,
        )

        rep_hash = hex(path_to_phash.get(root, 0))
        sc.execute(
            'INSERT OR REPLACE INTO dup_groups '
            '(file_hash, file_count, total_size, wasted_size, min_distance) '
            'VALUES (?,?,?,?,?)',
            (rep_hash, len(members), total_size, wasted_size, min_dist),
        )
        gid = sc.lastrowid

        for sp, size in zip(members, sizes):
            ph_hex = hex(path_to_phash.get(sp, 0))
            md5    = path_to_md5.get(sp)
            db_row = md5_to_db.get(md5, {}) if md5 else {}
            sc.execute(
                'INSERT INTO dup_files '
                '(group_id, file_path, file_size, phash, '
                'image_id, post_id, reddit_id, post_title, subreddit, permalink, score) '
                'VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                (
                    gid, sp, size, ph_hex,
                    db_row.get('image_id'), db_row.get('post_id'),
                    db_row.get('reddit_id'), db_row.get('post_title'),
                    db_row.get('subreddit'), db_row.get('permalink'), db_row.get('score'),
                ),
            )
    sdb.commit()
    return total_wasted


def run_scan(
    downloads_dir: Path,
    db_path: Path,
    mysql_cfg: Optional[dict],
    threshold: int = DEFAULT_THRESHOLD,
    hash_size: int = DEFAULT_HASH_SIZE,
    images_only: bool = True,
    progress_cb: Callable[[str, int, int], None] = None,
) -> dict:
    """
    Full pipeline.  Returns a stats dict.
    progress_cb(message, current, total) is called periodically.
    Handles SIGTERM gracefully: saves partial results and exits cleanly.
    pHash cache: already-computed hashes are reused across scans.
    """
    global _stop_requested
    _stop_requested = False

    def progress(msg: str, cur: int = 0, tot: int = 0):
        if progress_cb:
            progress_cb(msg, cur, tot)
        else:
            print(msg)

    start  = time.time()
    dl_dir = Path(downloads_dir).resolve()

    # Open DB and load pHash cache
    sdb = init_db(db_path)
    sc  = sdb.cursor()
    cache_rows = sc.execute('SELECT path, mtime, size, phash FROM phash_cache').fetchall()
    phash_cache: dict[str, tuple[float, int, int | None]] = {}  # path → (mtime, size, phash_int or None=failed)
    for row in cache_rows:
        try:
            ph = int(row['phash'], 16) if row['phash'] else None
            phash_cache[row['path']] = (row['mtime'], row['size'], ph)
        except Exception:
            pass
    progress(f'Loaded {len(phash_cache):,} cached hashes.', 0, 0)

    # 1. List files (images only by default; videos are skipped)
    scan_exts = IMAGE_EXTENSIONS if images_only else MEDIA_EXTENSIONS
    progress(f'Listing {"image" if images_only else "media"} files…', 0, 0)
    all_paths = [
        p for p in dl_dir.rglob('*')
        if p.is_file() and p.suffix.lower() in scan_exts
    ]
    total   = len(all_paths)
    skipped = 0
    cache_hits = 0
    progress(f'Computing pHash for {total:,} files…', 0, total)

    # 2. Separate cached from uncached
    file_hashes: list[tuple[Path, int]] = []
    new_cache_entries: list[tuple[str, float, int, str | None]] = []
    to_hash: list[tuple[str, float, int, int]] = []  # (path_str, mtime, size, hash_size)

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
            if cached[2] is not None:
                file_hashes.append((fp, cached[2]))
            else:
                skipped += 1  # previously failed, skip instantly
        else:
            to_hash.append((sp, mtime, size, hash_size))

    progress(f'{cache_hits:,} cache hits, {len(to_hash):,} to hash, {skipped:,} skipped.', cache_hits, total)

    # 3. Hash uncached files in parallel
    done = 0
    n_workers = max(1, min(os.cpu_count() or 1, 4))
    chunk = max(50, len(to_hash) // (n_workers * 20))
    done = 0

    if to_hash and not _stop_requested:
        with mp.Pool(processes=n_workers, maxtasksperchild=200) as pool:
            for result in pool.imap_unordered(_phash_worker, to_hash, chunksize=chunk):
                if _stop_requested:
                    pool.terminate()
                    progress(f'Stop requested after hashing {done:,}/{len(to_hash):,} — saving partial results…',
                             cache_hits + done, total)
                    break
                sp, mtime, size, h = result
                done += 1
                if h is not None:
                    file_hashes.append((Path(sp), h))
                    new_cache_entries.append((sp, mtime, size, hex(h)))
                else:
                    skipped += 1
                    new_cache_entries.append((sp, mtime, size, None))
                if done % 500 == 0:
                    progress(f'Hashing… {done:,}/{len(to_hash):,} new ({n_workers} workers)',
                             cache_hits + done, total)

    hashed_count = len(file_hashes)
    progress(
        f'Hashed {hashed_count:,} files ({cache_hits:,} from cache, {skipped:,} skipped). '
        f'Building BK-tree…',
        cache_hits + done, total,
    )

    # Save new cache entries (including failures with phash=NULL)
    if new_cache_entries:
        sc.executemany(
            'INSERT OR REPLACE INTO phash_cache (path, mtime, size, phash) VALUES (?,?,?,?)',
            new_cache_entries,
        )
        sdb.commit()
        n_new_hashes = sum(1 for e in new_cache_entries if e[3] is not None)
        n_new_fails  = len(new_cache_entries) - n_new_hashes
        progress(f'Cached {n_new_hashes:,} new hashes + {n_new_fails:,} failures.', 0, 0)

    # 3. Build BK-tree
    tree = BKTree()
    for fp, h in file_hashes:
        tree.add(h, str(fp))

    # 4. BK-tree search + union-find (with stop checks)
    uf         = UnionFind()
    pair_min_d: dict[tuple[str, str], int] = {}
    n_searched = len(file_hashes)

    for i, (fp, h) in enumerate(file_hashes):
        if _stop_requested:
            n_searched = i
            progress(f'Stop requested after BK-tree search of {i:,}/{len(file_hashes):,} — saving partial results…', i, len(file_hashes))
            break
        if i % 2000 == 0 and i > 0:
            progress(f'Searching similarities… {i:,}/{len(file_hashes):,}', i, len(file_hashes))
        sp      = str(fp)
        similar = tree.search(h, threshold)
        for other, dist in similar:
            if other == sp:
                continue
            uf.union(sp, other)
            key = (sp, other) if sp < other else (other, sp)
            if key not in pair_min_d or dist < pair_min_d[key]:
                pair_min_d[key] = dist

    # 5. Filter to groups ≥ 2
    raw_groups: dict[str, list[str]] = {
        root: members
        for root, members in uf.groups().items()
        if len(members) > 1
    }
    progress(
        f'Found {len(raw_groups):,} duplicate groups. '
        f'Computing MD5s for {sum(len(m) for m in raw_groups.values()):,} files…',
        0, 0,
    )

    # 6. MD5 for duplicate files only → prod-DB lookup
    all_dup_paths = {p for members in raw_groups.values() for p in members}
    path_to_md5: dict[str, str] = {}
    for sp in all_dup_paths:
        md5 = compute_md5(Path(sp))
        if md5:
            path_to_md5[sp] = md5

    md5_to_db: dict[str, dict] = {}
    if mysql_cfg and path_to_md5:
        progress('Querying prod DB…', 0, 0)
        try:
            my  = mysql.connector.connect(**mysql_cfg)
            cur = my.cursor(dictionary=True)
            all_md5s = list(path_to_md5.values())
            for i in range(0, len(all_md5s), 500):
                batch = all_md5s[i : i + 500]
                ph    = ','.join(['%s'] * len(batch))
                cur.execute(f"""
                    SELECT i.id AS image_id, i.file_hash,
                           pi.post_id, p.reddit_id,
                           p.title AS post_title, p.subreddit,
                           p.permalink, p.score
                    FROM   images i
                    LEFT JOIN post_images pi ON i.id = pi.image_id
                    LEFT JOIN posts p        ON pi.post_id = p.id
                    WHERE  i.file_hash IN ({ph})
                """, batch)
                for row in cur.fetchall():
                    md5_to_db[row['file_hash']] = dict(row)
            cur.close()
            my.close()
        except Exception as e:
            progress(f'Warning: DB query failed ({e})')

    # 7. Write SQLite (clear old results first)
    is_partial = _stop_requested
    status_label = 'partial' if is_partial else 'complete'
    progress(f'Writing duplicates.db ({status_label})…', 0, 0)

    sc.execute('DELETE FROM dup_files')
    sc.execute('DELETE FROM dup_groups')
    sc.execute('DELETE FROM scan_info')
    sdb.commit()

    path_to_phash = {str(fp): h for fp, h in file_hashes}
    total_wasted  = _write_results(sc, sdb, raw_groups, pair_min_d,
                                   path_to_phash, path_to_md5, md5_to_db)

    elapsed = time.time() - start
    sc.execute(
        "INSERT INTO scan_info "
        "(id, scanned_at, scan_duration_sec, total_files_scanned, "
        "total_groups, total_wasted_bytes, threshold, hash_size, is_partial) "
        "VALUES (1, datetime('now','localtime'), ?, ?, ?, ?, ?, ?, ?)",
        (elapsed, hashed_count, len(raw_groups), total_wasted, threshold, hash_size,
         1 if is_partial else 0),
    )
    sdb.commit()
    sdb.close()

    stats = {
        'total_files':        hashed_count,
        'total_groups':       len(raw_groups),
        'total_wasted_bytes': total_wasted,
        'duration_sec':       round(elapsed, 1),
        'threshold':          threshold,
        'partial':            is_partial,
    }
    done_msg = (
        f'{"Partial results saved" if is_partial else "Done"} in {elapsed:.1f}s — '
        f'{len(raw_groups):,} groups, {total_wasted // 1024 // 1024:,} MB wasted'
        + (f' (scanned {hashed_count:,}/{total:,} files)' if is_partial else '')
    )
    progress(done_msg, hashed_count, total)
    return stats


# ── Config & CLI ──────────────────────────────────────────────────────────

def _get_mysql_config() -> dict:
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    return {
        'host':     cfg.get('mysql', 'host',     fallback='localhost'),
        'port':     cfg.getint('mysql', 'port',  fallback=3306),
        'user':     cfg.get('mysql', 'user',     fallback='root'),
        'password': cfg.get('mysql', 'password', fallback=''),
        'database': cfg.get('mysql', 'database', fallback='reddit_images'),
    }


def main():
    ap = argparse.ArgumentParser(
        description='Detect visually similar images with OpenCV pHash + BK-tree.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Hamming distance guide (out of 64 bits):
   0          Bit-perfect identical hash
   1–5        Near-identical (different JPEG quality, slight crop)
   6–10       Very similar  (watermark added, minor colour shift) [default]
  11–20       Similar content
  >20         Probably different images
        """,
    )
    ap.add_argument('--threshold', type=int, default=DEFAULT_THRESHOLD,
                    metavar='N', help=f'Hamming bits threshold (default: {DEFAULT_THRESHOLD})')
    ap.add_argument('--hash-size', type=int, default=DEFAULT_HASH_SIZE,
                    metavar='N', help=f'Hash dimension NxN (default: {DEFAULT_HASH_SIZE} → 64-bit)')
    ap.add_argument('--no-db', action='store_true',
                    help='Skip MySQL lookup (no post metadata, faster)')
    ap.add_argument('--include-videos', action='store_true',
                    help='Also scan video files (mp4, webm, etc.) — slow, not recommended')
    ap.add_argument('--downloads-dir', default=str(DOWNLOADS_DIR),
                    help='Path to reddit_downloads folder')
    ap.add_argument('--progress-json', action='store_true',
                    help='Emit JSON progress lines (used by web UI)')
    args = ap.parse_args()

    dl_dir = Path(args.downloads_dir).resolve()
    if not dl_dir.exists():
        print(f'Error: {dl_dir} not found.', file=sys.stderr)
        sys.exit(1)

    mysql_cfg = None if args.no_db else _get_mysql_config()

    def cb(msg: str, cur: int, tot: int):
        if args.progress_json:
            print(json.dumps({'message': msg, 'progress': cur, 'total': tot}), flush=True)
        else:
            print(msg)

    run_scan(
        downloads_dir = dl_dir,
        db_path       = DUPES_DB,
        mysql_cfg     = mysql_cfg,
        threshold     = args.threshold,
        hash_size     = args.hash_size,
        images_only   = not args.include_videos,
        progress_cb   = cb,
    )


if __name__ == '__main__':
    main()
