"""
song_ids_manager.py
===================

This module contains all of the logic related to maintaining and
reconciling ``.song_ids`` files.  In the original implementation
(``playlist_extractor.py``) these helpers were defined inline inside
``if __name__ == "__main__":``.  Pulling them into a separate module
reduces the cognitive load of the main script and allows reuse across
other parts of the project.  The implementations below are taken
directly from the original code with minimal changes; they therefore
preserve the original behaviour exactly.

Usage
-----

Import the functions you need into your script.  For example,

.. code-block:: python

   from song_ids_manager import (
       reconcile_song_ids_flip_ids,
       cleanup_song_ids_orphans,
       build_existing_keys_set,
       diff_song_ids_vs_playlist_existing_only,
       apply_song_ids_from_playlist_existing_only,
       maybe_update_song_ids_from_playlist_existing_only,
       rename_single_variant_to_canonical,
   )

All functions operate on simple data structures such as lists of
playlist entries (dictionaries with ``artist``, ``title`` and ``id``
keys) and do not perform any destructive operations on the music
library by themselves.  The only file-writing side effects are
confined to ``.song_ids`` files.
"""

from __future__ import annotations

import os
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Tuple, Set, Optional

from utils import CustomPrint, sanitize_filename

__all__ = [
    "reconcile_song_ids_flip_ids",
    "cleanup_song_ids_orphans",
    "build_existing_keys_set",
    "diff_song_ids_vs_playlist_existing_only",
    "apply_song_ids_from_playlist_existing_only",
    "maybe_update_song_ids_from_playlist_existing_only",
    "rename_single_variant_to_canonical",
]


# Regular expressions reused across multiple helpers
BASE_RE = re.compile(
    r"^(?P<artist>.+?)\s*-\s*(?P<title>.+?)(?:_(?P<suffix>\d+))?\.mp3$",
    re.IGNORECASE,
)
SUFFIX_RE = re.compile(r"^(?P<base>.+?)_(?P<n>\d+)\.mp3$", re.IGNORECASE)
_SUFFIX_RE = re.compile(r"^(?P<base>.+?)(?P<suffix>_\d+)?\.mp3$", re.IGNORECASE)


def _nfc(s: Optional[str]) -> str:
    """Normalize a string to NFC form.  Returns an empty string when ``s`` is falsy."""
    return unicodedata.normalize("NFC", s or "")


def _norm_key(artist: str, title: str) -> Tuple[str, str]:
    """Return a casefolded key used for dictionary lookups based on artist and title."""
    return (_nfc((artist or "").strip()).casefold(), _nfc((title or "").strip()).casefold())


def _expected_filename(artist: str, title: str) -> str:
    """Return the canonical filename for a given artist and title."""
    return f"{artist} - {title}.mp3"


def _split_mp3_name(name: str) -> Tuple[str, str]:
    """Split a file name into base and suffix using ``_SUFFIX_RE``.

    Returns a tuple ``(base, suffix)`` where ``suffix`` includes the leading
    underscore or is ``""`` when no suffix is present.  If the name does not
    match the pattern at all, returns ``(None, None)``.
    """
    m = _SUFFIX_RE.match(name)
    if not m:
        return None, None
    return m.group("base") or "", m.group("suffix") or ""


def _filename_needs_fix(filename: str, artist: str, title: str) -> bool:
    """Determine whether a filename does not match the expected canonical name.

    A filename is considered in need of fixing if it does not match the
    expected base (artist-title) or does not adhere to the ``_SUFFIX_RE``
    pattern.
    """
    if not filename:
        return True
    m = _SUFFIX_RE.match(filename)
    if not m:
        return True
    base = m.group("base") or ""
    return _nfc(base).casefold() != _nfc(f"{artist} - {title}").casefold()


def _read_song_ids_rows(hidden_file_path: str) -> List[Dict[str, object]]:
    """Read a ``.song_ids`` file and return a list of row dictionaries.

    Each dictionary contains the keys ``id``, ``ts`` (datetime), ``ts_s`` (string),
    ``artist``, ``song`` and ``filename``.  Non-conforming lines are silently
    ignored.  Missing timestamps default to the minimal datetime.
    """
    rows: List[Dict[str, object]] = []
    if not os.path.isfile(hidden_file_path):
        return rows
    with open(hidden_file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) != 5:
                continue
            track_id, timestamp, artist, song_name, filename = parts
            try:
                ts = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                ts = datetime.min
            rows.append({
                "id": track_id,
                "ts": ts,
                "ts_s": timestamp,
                "artist": artist,
                "song": song_name,
                "filename": filename,
            })
    return rows


def cleanup_song_ids_orphans(playlist_dir: str) -> Dict[str, int]:
    """Remove rows in ``.song_ids`` where the referenced file no longer exists on disk.

    This helper rewrites the ``.song_ids`` file keeping only entries whose
    ``filename`` exists in ``playlist_dir``.  The rows are written back in a
    deterministic order (sorted by artist and title).  The return value is a
    dictionary with two keys:

    - ``kept``: the number of rows retained
    - ``dropped_orphans``: the number of rows discarded
    """
    hidden_file_path = os.path.join(playlist_dir, ".song_ids")
    rows = _read_song_ids_rows(hidden_file_path)

    # Index disk files (exact names)
    try:
        disk_files = {f for f in os.listdir(playlist_dir) if f.lower().endswith(".mp3")}
    except FileNotFoundError:
        disk_files = set()

    kept_rows = []
    dropped_orphans = 0

    for r in rows:
        filename = r.get("filename") or f"{r.get('artist', '')} - {r.get('song', '')}.mp3"
        if filename in disk_files:
            kept_rows.append(r)
        else:
            dropped_orphans += 1

    # Deterministic write
    new_lines: List[str] = []
    for r in kept_rows:
        tid = r["id"]
        ts_s = r["ts_s"]
        artist = r["artist"]
        title = r["song"]
        filename = r.get("filename") or f"{artist} - {title}.mp3"
        new_lines.append(f"{tid}\t{ts_s}\t{artist}\t{title}\t{filename}\n")

    new_lines.sort(key=lambda line: (line.split("\t")[2].casefold(), line.split("\t")[3].casefold()))

    with open(hidden_file_path, "w", encoding="utf-8") as f:
        f.writelines(new_lines)

    return {
        "kept": len(kept_rows),
        "dropped_orphans": dropped_orphans,
    }


def reconcile_song_ids_flip_ids(
    playlist_dir: str,
    playlist_tracks: List[Dict[str, str]],
) -> Tuple[int, int, int]:
    """Synchronize ``.song_ids`` rows with the current playlist and files on disk.

    This high-signal reconciliation performs three operations:

    - Add missing rows for files that exist on disk but are missing from ``.song_ids``.
    - If a row exists for a given filename but the stored track ID differs from the
      playlist's ID (and the artist/title match), update the row's ``id`` and
      timestamp.
    - Deduplicate by filename: when multiple rows exist for the same filename, keep
      the newest timestamp and, when possible, the row matching the current
      playlist ID.

    Parameters
    ----------
    playlist_dir: str
        Path to the directory containing the playlist files.
    playlist_tracks: list of dict
        Each dict must contain at least ``id``, ``artist`` and ``title``.

    Returns
    -------
    Tuple[int, int, int]
        A tuple ``(added_rows, flipped_ids, deduped_rows)`` summarizing the changes.
    """
    sid_path = Path(playlist_dir) / ".song_ids"
    sid_path.touch()

    # Read existing rows
    rows: List[Dict[str, object]] = []
    with sid_path.open("r", encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 5:
                continue
            track_id, ts, artist, title, fname = parts
            rows.append({
                "id": track_id,
                "ts": ts,
                "artist": artist,
                "title": title,
                "fname": fname,
                "key": _norm_key(artist, title),
            })

    # Index current state
    by_fname: Dict[str, Dict[str, object]] = {r["fname"]: r for r in rows}
    by_key: Dict[Tuple[str, str], Dict[str, object]] = {}
    for r in rows:
        k = r["key"]
        if k not in by_key or by_key[k]["ts"] < r["ts"]:
            by_key[k] = r

    # Build a simple view of the current playlist keyed by (artist,title)
    pl: Dict[Tuple[str, str], Dict[str, str]] = {}
    for t in playlist_tracks:
        key = _norm_key(t["artist"], t["title"])
        pl[key] = {
            "id": t["id"],
            "artist": t["artist"],
            "title": t["title"],
            "fname": f"{t['artist']} - {t['title']}.mp3",
        }

    added = flipped = deduped = 0

    # For each playlist entry, if the file exists on disk, ensure there is a row
    for key, cur in pl.items():
        fname = cur["fname"]
        file_exists = (Path(playlist_dir) / fname).is_file()
        if not file_exists:
            continue
        row = by_fname.get(fname) or by_key.get(key)
        if row is None:
            # Missing row → add
            rows.append({
                "id": cur["id"],
                "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                "artist": cur["artist"],
                "title": cur["title"],
                "fname": fname,
                "key": key,
            })
            by_fname[fname] = rows[-1]
            by_key[key] = rows[-1]
            added += 1
        else:
            # Row exists but ID differs → flip ID and update timestamp
            if row["id"] != cur["id"] and row["artist"] == cur["artist"] and row["title"] == cur["title"]:
                row["id"] = cur["id"]
                row["ts"] = time.strftime("%Y-%m-%d %H:%M:%S")
                flipped += 1
                by_fname[fname] = row
                by_key[key] = row

    # Deduplicate by filename: keep row with the latest timestamp; prefer matching playlist ID
    final_by_fname: Dict[str, Dict[str, object]] = {}
    for r in rows:
        k = r["fname"]
        keep = r
        if k in final_by_fname:
            prev = final_by_fname[k]
            want = pl.get(r["key"], {}).get("id")
            if want and r["id"] == want and prev["id"] != want:
                keep = r
            else:
                # In absence of a desired ID, keep the newest timestamp
                keep = max([prev, r], key=lambda x: x["ts"])
            if keep is not prev:
                deduped += 1
        final_by_fname[k] = keep

    out_lines: List[str] = []
    for r in sorted(final_by_fname.values(), key=lambda x: (x["artist"].casefold(), x["title"].casefold())):
        out_lines.append("\t".join([r["id"], r["ts"], r["artist"], r["title"], r["fname"]]) + "\n")

    with sid_path.open("w", encoding="utf-8") as f:
        f.writelines(out_lines)

    return added, flipped, deduped


def build_existing_keys_set(playlist_dir: str) -> Set[Tuple[str, str]]:
    """Return a set of normalized (artist,title) pairs for all MP3 files in a directory."""
    keys: Set[Tuple[str, str]] = set()
    try:
        for f in os.listdir(playlist_dir):
            if not f.lower().endswith(".mp3"):
                continue
            m = BASE_RE.match(f)
            if not m:
                continue
            artist = _nfc(m.group("artist"))
            title = _nfc(m.group("title"))
            keys.add(_norm_key(artist, title))
    except FileNotFoundError:
        pass
    return keys


def _find_existing_filename_for_base(playlist_dir: str, artist: str, title: str) -> Optional[str]:
    """Return the filename on disk for a given artist and title, if any.

    The returned name may be the canonical name (``"Artist - Title.mp3"``) or a
    variant such as ``"Artist - Title_1.mp3"``.  Returns ``None`` if no file
    matches.
    """
    want_base = f"{artist} - {title}"
    try:
        for f in os.listdir(playlist_dir):
            if not f.lower().endswith(".mp3"):
                continue
            m = BASE_RE.match(f)
            if not m:
                continue
            base = f"{_nfc(m.group('artist'))} - {_nfc(m.group('title'))}"
            if _nfc(base).casefold() == _nfc(want_base).casefold():
                return f
    except FileNotFoundError:
        pass
    return None


def diff_song_ids_vs_playlist_existing_only(
    playlist_dir: str,
    playlist_tracks: List[Dict[str, object]],
    restrict_keys: Set[Tuple[str, str]],
) -> List[Dict[str, object]]:
    """Compute differences between ``.song_ids`` and the playlist for existing files only.

    ``restrict_keys`` limits updates to files already present on disk.  This function
    proposes new rows when a file exists and is part of the playlist but no
    corresponding row exists in ``.song_ids``.  It does **not** propose ID flips
    for existing rows (no churn mode).

    Returns a list of dictionaries with the following keys:

    - ``artist`` / ``song``: track metadata
    - ``old_id`` / ``new_id``: the former and proposed IDs (``old_id`` is ``None`` when a row does not exist)
    - ``earliest_ts_s``: timestamp string for new rows
    - ``canonical_filename``: the canonical filename on disk
    - ``reason``: always ``"new_existing_file"``
    """
    hidden_file_path = os.path.join(playlist_dir, ".song_ids")
    rows = _read_song_ids_rows(hidden_file_path)

    # sanitize rows & drop incomplete ones
    cleaned_rows: List[Dict[str, object]] = []
    for r in rows:
        artist = _nfc(r.get("artist") or "")
        title = _nfc(r.get("song") or "")
        tid = r.get("id") or ""
        if not artist or not title or not tid:
            continue
        filename = r.get("filename") or _expected_filename(artist, title)
        if _filename_needs_fix(filename, artist, title):
            filename = _expected_filename(artist, title)
        cleaned_rows.append({**r, "artist": artist, "song": title, "filename": filename})

    groups: Dict[Tuple[str, str], List[Dict[str, object]]] = defaultdict(list)
    for r in cleaned_rows:
        groups[_norm_key(r["artist"], r["song"])] .append(r)

    have_row_for_key: Set[Tuple[str, str]] = {k for k in groups.keys()}

    # Build quick map of playlist entries (artist/title/id)
    pl_list: List[Dict[str, str]] = []
    for t in playlist_tracks:
        a = _nfc(t.get("artist") or "")
        s = _nfc(t.get("title") or "")
        tid = t.get("id")
        if a and s and tid:
            pl_list.append({"artist": a, "title": s, "id": tid})

    diffs: List[Dict[str, object]] = []
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for t in pl_list:
        artist, title, tid = t["artist"], t["title"], t["id"]
        k = _norm_key(artist, title)
        if k not in restrict_keys:
            continue
        # If there is already any row for this (artist,title), do not churn IDs.
        if k in have_row_for_key:
            continue
        diffs.append({
            "artist": artist,
            "song": title,
            "old_id": None,
            "new_id": tid,
            "earliest_ts_s": now_s,
            "canonical_filename": _expected_filename(artist, title),
            "reason": "new_existing_file",
        })
    return diffs


def apply_song_ids_from_playlist_existing_only(
    playlist_dir: str,
    playlist_tracks: List[Dict[str, object]],
    restrict_keys: Set[Tuple[str, str]],
) -> None:
    """Apply updates to ``.song_ids`` for entries in ``restrict_keys``.

    When a row exists for a given (artist,title), this function keeps the earliest
    timestamp, sets the ID to match the playlist ID, and updates the filename to
    reflect the actual file on disk (canonical or variant).  When no row exists
    but a file exists on disk, a new row is appended with ``now`` as the
    timestamp.  All other rows remain unchanged.  The file is rewritten
    deterministically.
    """
    hidden_file_path = os.path.join(playlist_dir, ".song_ids")
    rows = _read_song_ids_rows(hidden_file_path)

    groups: Dict[Tuple[str, str], List[Dict[str, object]]] = defaultdict(list)
    for r in rows:
        groups[_norm_key(r["artist"], r["song"])] .append(r)

    # Build a map from playlist keys to track info
    pl_map: Dict[Tuple[str, str], Dict[str, object]] = {}
    for t in playlist_tracks:
        key = _norm_key(t["artist"], t["title"])
        pl_map[key] = t

    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_lines: List[str] = []
    seen_keys: Set[Tuple[str, str]] = set()

    # Rewrite existing groups first
    for k, grp in groups.items():
        newest = max(grp, key=lambda r: r["ts"])
        earliest = min(grp, key=lambda r: r["ts"])
        artist_disp = newest["artist"]
        song_disp = newest["song"]
        if k in restrict_keys and k in pl_map:
            t = pl_map[k]
            tid = t["id"]
            ts_s = earliest["ts_s"]  # preserve earliest timestamp
            existing_name = _find_existing_filename_for_base(playlist_dir, artist_disp, song_disp)
            filename = existing_name or f"{artist_disp} - {song_disp}.mp3"
            new_lines.append(f"{tid}\t{ts_s}\t{artist_disp}\t{song_disp}\t{filename}\n")
            seen_keys.add(k)
        else:
            # Unchanged group
            new_lines.append(
                f"{newest['id']}\t{earliest['ts_s']}\t{artist_disp}\t{song_disp}\t{newest['filename']}\n"
            )
            seen_keys.add(k)

    # Add rows for existing-on-disk tracks that had no rows before
    for k in restrict_keys:
        if k not in seen_keys and k in pl_map:
            artist = pl_map[k]["artist"]
            title = pl_map[k]["title"]
            tid = pl_map[k]["id"]
            existing_name = _find_existing_filename_for_base(playlist_dir, artist, title)
            filename = existing_name or f"{artist} - {title}.mp3"
            new_lines.append(f"{tid}\t{now_s}\t{artist}\t{title}\t{filename}\n")

    new_lines.sort(key=lambda line: (line.split("\t")[2].casefold(), line.split("\t")[3].casefold()))
    with open(hidden_file_path, "w", encoding="utf-8") as f:
        f.writelines(new_lines)


def maybe_update_song_ids_from_playlist_existing_only(
    playlist_dir: str,
    playlist_tracks: List[Dict[str, object]],
    restrict_keys: Set[Tuple[str, str]],
) -> None:
    """Update ``.song_ids`` for tracks that already exist on disk.

    This helper calls ``diff_song_ids_vs_playlist_existing_only`` to compute
    differences and then applies them via ``apply_song_ids_from_playlist_existing_only``.
    It prints a short summary of the actions taken using ``CustomPrint``.  No user
    confirmation is required in this mode; it is intended to be called when
    ensuring that Zotify will skip already-downloaded tracks.
    """
    diffs = diff_song_ids_vs_playlist_existing_only(playlist_dir, playlist_tracks, restrict_keys)
    if not diffs:
        CustomPrint.bold_print("No ID updates needed for existing files.", "GREEN")
        return
    CustomPrint.bold_print(
        ">>> Auto-updating .song_ids for existing files:", "YELLOW"
    )
    for d in diffs:
        if d["old_id"] is None:
            CustomPrint.print(f"- {d['artist']} — {d['song']}: (new row) -> {d['new_id']}")
        else:
            CustomPrint.print(f"- {d['artist']} — {d['song']}: {d['old_id']} -> {d['new_id']}")
    apply_song_ids_from_playlist_existing_only(playlist_dir, playlist_tracks, restrict_keys)
    CustomPrint.bold_print("Updated .song_ids for existing files.", "GREEN")


def rename_single_variant_to_canonical(
    playlist_dir: str,
    playlist_tracks: List[Dict[str, object]],
) -> None:
    """Promote a single suffix variant to the canonical filename when safe.

    If exactly one variant like ``"Artist - Title_1.mp3"`` exists and the canonical
    ``"Artist - Title.mp3"`` does not, rename the variant to the canonical
    filename.  This matches the behaviour in the original ``playlist_extractor``
    script.
    """
    # Build expected canonical names
    expected: Set[str] = set()
    for t in playlist_tracks:
        a = _nfc(t["artist"])
        s = _nfc(t["title"])
        expected.add(f"{a} - {s}.mp3")
    # Index disk files by base
    by_base: Dict[str, List[str]] = {}
    try:
        for f in os.listdir(playlist_dir):
            if not f.lower().endswith(".mp3"):
                continue
            m = BASE_RE.match(f)
            if not m:
                continue
            base = f"{_nfc(m.group('artist'))} - {_nfc(m.group('title'))}"
            by_base.setdefault(base.casefold(), []).append(f)
    except FileNotFoundError:
        return
    # Rename where safe
    for canon in expected:
        m = BASE_RE.match(canon)
        if not m:
            continue
        base = f"{_nfc(m.group('artist'))} - {_nfc(m.group('title'))}"
        variants = by_base.get(base.casefold(), [])
        if not variants:
            continue
        # If canonical already present, skip
        if canon in variants:
            continue
        if len(variants) == 1:
            src = os.path.join(playlist_dir, variants[0])
            dst = os.path.join(playlist_dir, canon)
            try:
                os.rename(src, dst)
                print(f">>> Renamed '{variants[0]}' -> '{canon}'")
            except Exception as e:
                print(f">>> WARN: Could not rename '{variants[0]}' -> '{canon}': {e}")
        # If multiple variants exist, do nothing