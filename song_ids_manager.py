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

from utils import CustomPrint, find_matching_filename, filename_match_key, sanitize_filename

__all__ = [
    "reconcile_song_ids_flip_ids",
    "cleanup_song_ids_orphans",
    "build_existing_keys_set",
    "diff_song_ids_vs_playlist_existing_only",
    "apply_song_ids_from_playlist_existing_only",
    "maybe_update_song_ids_from_playlist_existing_only",
    "rename_single_variant_to_canonical",
    # NEW
    "detect_legacy_archive",
    "upgrade_legacy_archive",
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


def _index_mp3s_by_embedded_track_id(playlist_dir: str, filenames: Set[str]) -> Dict[str, Set[str]]:
    """Index exact Zotify TRACKID TXXX tags without inferring from filenames."""
    try:
        from mutagen.id3 import ID3
    except ImportError:
        return {}

    files_by_track_id: Dict[str, Set[str]] = defaultdict(set)
    for filename in sorted(filenames):
        try:
            tags = ID3(os.path.join(playlist_dir, filename))
        except Exception:
            continue
        for frame in tags.getall("TXXX"):
            if str(getattr(frame, "desc", "")).casefold() != "trackid":
                continue
            for value in getattr(frame, "text", ()) or ():
                track_id = str(value).strip()
                if track_id:
                    files_by_track_id[track_id].add(filename)

    return files_by_track_id


def cleanup_song_ids_orphans(playlist_dir: str) -> Dict[str, int]:
    """Remove rows in ``.song_ids`` where the referenced file no longer exists on disk.

    This helper rewrites the ``.song_ids`` file, retaining rows whose saved
    filename resolves to a file in ``playlist_dir`` or whose embedded Zotify
    ``TRACKID`` identifies a file there. Recovered rows are updated to an actual
    on-disk filename. Rows are written back in deterministic artist/title order.

    - ``kept``: the number of rows retained
    - ``dropped_orphans``: the number of rows discarded
    """
    hidden_file_path = os.path.join(playlist_dir, ".song_ids")
    rows = _read_song_ids_rows(hidden_file_path)

    # Index disk files and resolve saved names conservatively across accents/case.
    try:
        disk_files = {f for f in os.listdir(playlist_dir) if f.lower().endswith(".mp3")}
    except FileNotFoundError:
        disk_files = set()
    disk_file_keys = {filename_match_key(name) for name in disk_files}

    track_id_index = None

    def filename_for_track_id(track_id: str, expected_filename: str) -> Optional[str]:
        nonlocal track_id_index
        if track_id_index is None:
            track_id_index = _index_mp3s_by_embedded_track_id(playlist_dir, disk_files)
        matches = track_id_index.get(str(track_id or ""), set())
        if not matches:
            return None
        expected_key = filename_match_key(expected_filename)
        return min(matches, key=lambda name: (
            filename_match_key(name) != expected_key,
            name.casefold(),
            name,
        ))

    kept_rows = []
    dropped_orphans = 0

    for r in rows:
        filename = str(r.get("filename") or f"{r.get('artist', '')} - {r.get('song', '')}.mp3")
        existing_name = find_matching_filename(filename, disk_files)
        if existing_name is not None:
            r["filename"] = existing_name
            kept_rows.append(r)
        elif filename_match_key(filename) in disk_file_keys:
            # Keep ambiguous spelling variants. When Zotify's embedded ID is
            # available, also update the row to a real path (including suffix copies).
            matched_filename = filename_for_track_id(str(r.get("id") or ""), filename)
            if matched_filename is not None:
                r["filename"] = matched_filename
            kept_rows.append(r)
        else:
            # Zotify records TRACKID in the MP3's TXXX tag. Recover the real path
            # from that exact ID before declaring a stale archive row orphaned.
            matched_filename = filename_for_track_id(str(r.get("id") or ""), filename)
            if matched_filename is not None:
                r["filename"] = matched_filename
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
    try:
        disk_files = {name for name in os.listdir(playlist_dir) if name.lower().endswith(".mp3")}
    except FileNotFoundError:
        disk_files = set()

    # For each playlist entry, find its exact or uniquely normalized disk name.
    for key, cur in pl.items():
        fname = cur["fname"]
        existing_fname = find_matching_filename(fname, disk_files)
        if existing_fname is None:
            # Match the same assembled-name sanitizer used by the playlist writer.
            existing_fname = find_matching_filename(sanitize_filename(fname), disk_files)
        if existing_fname is None:
            continue
        row = by_fname.get(existing_fname) or by_fname.get(fname) or by_key.get(key)
        if row is None:
            # Missing row → add; preserve the spelling actually present on disk.
            rows.append({
                "id": cur["id"],
                "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                "artist": cur["artist"],
                "title": cur["title"],
                "fname": existing_fname,
                "key": key,
            })
            by_fname[existing_fname] = rows[-1]
            by_key[key] = rows[-1]
            added += 1
        else:
            # The file path matched uniquely, even when its display spelling differs.
            row_key = filename_match_key(f"{row['artist']} - {row['title']}.mp3")
            track_key = filename_match_key(f"{cur['artist']} - {cur['title']}.mp3")
            if row["id"] != cur["id"] and row_key == track_key:
                row["id"] = cur["id"]
                row["ts"] = time.strftime("%Y-%m-%d %H:%M:%S")
                row["artist"] = cur["artist"]
                row["title"] = cur["title"]
                row["fname"] = existing_fname
                flipped += 1
            by_fname[existing_fname] = row
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

# =====================================================================
# LEGACY .song_ids MIGRATION (STEP 1)
# =====================================================================

def _is_absolute_path(filepath: str) -> bool:
    """Return True if filepath is absolute, False otherwise."""
    return os.path.isabs(filepath)


def _find_file_by_name_in_directory(playlist_dir: str, filename: str) -> Optional[str]:
    """Find a file in playlist_dir by name and return its absolute path."""
    try:
        candidate = os.path.join(playlist_dir, filename)
        if os.path.isfile(candidate):
            return os.path.abspath(candidate)
    except Exception:
        pass
    return None


def _validate_audio_tags(filepath: str, expected_artist: str, expected_title: str) -> bool:
    """
    Best-effort validation that the audio file at filepath matches expected artist/title.

    If mutagen is unavailable or tags cannot be read, returns True so migration
    still proceeds as long as the file exists.
    """
    if not os.path.isfile(filepath):
        return False

    try:
        try:
            from mutagen.id3 import ID3

            tags = ID3(filepath)

            file_artist = str(tags.get("TPE1", "")).strip() if "TPE1" in tags else ""
            file_title = str(tags.get("TIT2", "")).strip() if "TIT2" in tags else ""

            expected_artist_norm = _nfc(expected_artist.strip()).casefold()
            expected_title_norm = _nfc(expected_title.strip()).casefold()
            file_artist_norm = _nfc(file_artist.strip()).casefold()
            file_title_norm = _nfc(file_title.strip()).casefold()

            return (file_artist_norm == expected_artist_norm and
                    file_title_norm == expected_title_norm)
        except Exception:
            # mutagen not installed or tags unreadable – accept if file exists
            return True
    except Exception:
        return True


def detect_legacy_archive(hidden_file_path: str) -> bool:
    """
    Return True if .song_ids appears to be legacy (all paths relative), False otherwise.

    Heuristic: inspect up to the first 10 non-empty, well-formed lines; if any
    5th field is an absolute path, treat file as new format.
    """
    if not os.path.isfile(hidden_file_path):
        return False

    try:
        with open(hidden_file_path, "r", encoding="utf-8") as f:
            checked = 0
            for line in f:
                if checked >= 10:
                    break
                line = line.rstrip("\n")
                if not line.strip():
                    continue
                parts = line.split("\t")
                if len(parts) != 5:
                    continue
                filepath = parts[4]
                if _is_absolute_path(filepath):
                    # already migrated / new format
                    return False
                checked += 1
        return checked > 0
    except Exception:
        return False


def upgrade_legacy_archive(playlist_dir: str, hidden_file_path: str) -> Tuple[int, int]:
    """
    Upgrade a legacy .song_ids file in playlist_dir to use absolute paths.

    Returns (upgraded_count, validation_failures).
    """
    if not os.path.isfile(hidden_file_path):
        return 0, 0

    rows = _read_song_ids_rows(hidden_file_path)
    upgraded_rows: List[Dict[str, object]] = []
    validation_failures = 0

    for row in rows:
        track_id = row.get("id") or ""
        artist = row.get("artist") or ""
        song = row.get("song") or ""
        filename = row.get("filename") or ""
        ts_s = row.get("ts_s") or ""

        if not (track_id and artist and song and filename):
            validation_failures += 1
            continue

        file_path = _find_file_by_name_in_directory(playlist_dir, filename)
        if not file_path:
            CustomPrint.bold_print(
                f"  ⚠️  Could not find file: {filename} (artist: {artist}, song: {song})",
                "YELLOW",
            )
            validation_failures += 1
            continue

        if not _validate_audio_tags(file_path, artist, song):
            CustomPrint.bold_print(
                f"  ⚠️  Tags mismatch: {filename} (expected: {artist} - {song})",
                "YELLOW",
            )
            validation_failures += 1
            continue

        row["filename"] = file_path  # now absolute
        # keep ts_s / ts / id / artist / song unchanged
        upgraded_rows.append(row)

    # write back deterministically: id\t ts_s\t artist\t song\t filepath
    new_lines: List[str] = []
    for r in upgraded_rows:
        new_lines.append(
            f"{r['id']}\t{r['ts_s']}\t{r['artist']}\t{r['song']}\t{r['filename']}\n"
        )

    new_lines.sort(
        key=lambda line: (
            line.split("\t")[2].casefold(),  # artist
            line.split("\t")[3].casefold(),  # song
        )
    )

    with open(hidden_file_path, "w", encoding="utf-8") as f:
        f.writelines(new_lines)

    return len(upgraded_rows), validation_failures
