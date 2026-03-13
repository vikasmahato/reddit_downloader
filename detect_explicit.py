#!/usr/bin/env python3
"""
detect_explicit.py — Scan images for explicit/NSFW content using NudeNet.

Walks the download folder, runs NudeNet detection on each image, and writes
a JSON file listing flagged images with their detection details.

Usage:
    python detect_explicit.py                       # scan all, save results
    python detect_explicit.py --folder r/pics       # single subreddit
    python detect_explicit.py --threshold 0.6       # confidence threshold (default 0.5)
    python detect_explicit.py --progress-json       # JSON progress lines (web UI)

Output:
    explicit_flagged.json  — list of flagged images, overwritten each run

Requirements:
    pip install nudenet
"""

import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path

# ── Stop flag ──────────────────────────────────────────────────────────────
_stop_requested = False


def _handle_stop(signum, frame):
    global _stop_requested
    _stop_requested = True


signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)

# ── Explicit content classes from NudeNet ──────────────────────────────────
# These are the classes we consider "explicit" for flagging purposes.
EXPLICIT_CLASSES = {
    'EXPOSED_GENITALIA_F',
    'EXPOSED_GENITALIA_M',
    'EXPOSED_BREAST_F',
    'EXPOSED_BUTTOCKS',
    'EXPOSED_ANUS_F',
    'EXPOSED_ANUS_M',
}

IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.webp', '.bmp', '.gif'}

FLAGGED_FILE = Path('explicit_flagged.json')


def _emit(msg: str, cur: int = 0, tot: int = 0, as_json: bool = False, extra: dict = None):
    if as_json:
        payload = {'message': msg, 'progress': cur, 'total': tot}
        if extra:
            payload.update(extra)
        print(json.dumps(payload), flush=True)
    else:
        bar = f'[{cur}/{tot}] ' if tot else ''
        print(f'{bar}{msg}', flush=True)


def _load_detector():
    try:
        from nudenet import NudeDetector
        return NudeDetector()
    except ImportError:
        print('ERROR: nudenet is not installed. Run: pip install nudenet', file=sys.stderr)
        sys.exit(1)


def scan_folder(
    folder: Path,
    threshold: float,
    progress_json: bool,
) -> list:
    """
    Scan all images in folder, return list of flagged dicts:
      { file_path, detections: [{class, score, box}], max_score, classes }
    """
    def emit(msg, cur=0, tot=0, extra=None):
        _emit(msg, cur, tot, as_json=progress_json, extra=extra)

    emit('Loading NudeNet detector…')
    detector = _load_detector()
    emit('Detector ready. Collecting images…')

    all_images = [
        p for p in folder.rglob('*')
        if p.is_file() and p.suffix.lower() in IMAGE_EXTS
        and 'deleted' not in p.parts
    ]
    total = len(all_images)
    emit(f'Found {total:,} image(s) to scan.', 0, total)

    flagged = []
    scanned = 0

    for i, img_path in enumerate(all_images, 1):
        if _stop_requested:
            emit(f'Stopped at {i - 1}/{total}.', i - 1, total)
            break

        try:
            detections = detector.detect(str(img_path))
        except Exception as e:
            if progress_json:
                emit(f'Error scanning {img_path.name}: {e}', i, total)
            if i % 200 == 0:
                emit(f'Checked {i:,}/{total:,}…', i, total)
            scanned += 1
            continue

        # Filter to explicit classes above threshold
        hits = [
            d for d in (detections or [])
            if d.get('class') in EXPLICIT_CLASSES and d.get('score', 0) >= threshold
        ]

        if hits:
            max_score = max(d['score'] for d in hits)
            classes_found = list({d['class'] for d in hits})
            flagged.append({
                'file_path': str(img_path),
                'filename': img_path.name,
                'detections': [
                    {'class': d['class'], 'score': round(d['score'], 3),
                     'box': d.get('box', [])}
                    for d in hits
                ],
                'max_score': round(max_score, 3),
                'classes': classes_found,
            })
            if progress_json:
                emit(
                    f'FLAGGED: {img_path.name} ({", ".join(classes_found)}, '
                    f'score={max_score:.2f})',
                    i, total,
                    extra={'flagged_count': len(flagged)},
                )

        scanned += 1
        if i % 100 == 0:
            emit(f'Checked {i:,}/{total:,} — {len(flagged):,} flagged…', i, total,
                 extra={'flagged_count': len(flagged)})

    return flagged


def main():
    ap = argparse.ArgumentParser(
        description='Detect explicit images using NudeNet.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument('--folder', default='',
                    help='Subfolder to scan (default: entire reddit_downloads)')
    ap.add_argument('--threshold', type=float, default=0.5,
                    help='Minimum confidence score to flag (0.0–1.0)')
    ap.add_argument('--progress-json', action='store_true',
                    help='Emit JSON progress lines (used by web UI)')
    args = ap.parse_args()

    import configparser
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    download_folder = Path(
        cfg.get('general', 'download_folder', fallback='reddit_downloads')
    ).resolve()

    scan_root = (download_folder / args.folder).resolve() if args.folder else download_folder

    def emit(msg, cur=0, tot=0, extra=None):
        _emit(msg, cur, tot, as_json=args.progress_json, extra=extra)

    t0 = time.time()
    flagged = scan_folder(scan_root, args.threshold, args.progress_json)

    # Save results
    FLAGGED_FILE.write_text(json.dumps(flagged, indent=2))

    elapsed = time.time() - t0
    emit(
        f'Done in {elapsed:.1f}s — scanned images, found {len(flagged):,} explicit file(s). '
        f'Results saved to {FLAGGED_FILE}',
        extra={'flagged_count': len(flagged), 'done': True},
    )


if __name__ == '__main__':
    main()
