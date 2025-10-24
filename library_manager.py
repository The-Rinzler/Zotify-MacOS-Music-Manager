"""
library_manager.py
==================

This module encapsulates the high-level operations required to manage
and consolidate music files for the Zotify MacOS Music Manager.  It
provides helpers for synchronizing duplicate files across multiple
directories, building consolidated libraries, counting and validating
playlist contents, discovering missing or orphaned songs, and
manipulating album artwork.  The majority of this code originated in
``consolidate_library.py``; extracting it here leaves the main script
focused on orchestrating user interactions and delegating work to
well-named functions.

Most functions in this module are side-effectful: they perform file
I/O, copy or delete files, and interact with the filesystem.  They do
not prompt the user for input unless explicitly documented.  Scripts
importing these functions should handle user interaction and error
reporting according to their needs.
"""

from __future__ import annotations

import atexit
import hashlib
import io
import json
import os
import re
import shutil
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union

from PIL import Image
from mutagen.id3 import ID3, APIC
from mutagen.mp3 import MP3
from requests import ReadTimeout
import spotipy
from spotipy import SpotifyException

from utils import CustomPrint, sanitize_filename, write_playlist_to_m3u8
from spotify_api import fetch_playlist_tracks


__all__ = [
    "update_playlists",
    "find_newest_versions",
    "replace_outdated_files",
    "manage_zotify_library",
    "copy_file_if_newer",
    "process_m3u_file",
    "consolidate_library",
    "get_valid_songs_from_m3u",
    "count_songs_in_m3u",
    "count_unique_songs_in_m3u",
    "check_missing_songs_in_playlists",
    "get_playlist_links",
    "get_playlist_meta",
    "check_orphan_and_leftover_songs",
    "remove_leftover_songs",
    "get_album_cover_dimensions",
    "resize_album_cover",
    "update_album_cover",
    "resize_album_covers",
]


# -----------------------------------------------------------------------------
# Cached SHA256 handling

_HASH_DB: Dict[str, Dict[str, str]] = {}
_HASH_DB_PATH: Path = Path.home() / ".cache" / "zotify_hashes.json"
_HASH_LOCK = threading.Lock()


def _load_hash_db() -> None:
    """Load the cached SHA256 database from disk, if present."""
    global _HASH_DB
    try:
        _HASH_DB = json.loads(_HASH_DB_PATH.read_text(encoding="utf-8"))
    except Exception:
        _HASH_DB = {}


def _save_hash_db() -> None:
    """Persist the cached SHA256 database to disk."""
    try:
        _HASH_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _HASH_DB_PATH.write_text(json.dumps(_HASH_DB, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


# Ensure the cache is saved at exit
atexit.register(_save_hash_db)
_load_hash_db()


def _cached_sha256(path: Path) -> str:
    """Return a SHA256 digest for ``path``, using an in-memory cache.

    The cache avoids recomputing hashes for files whose modification time
    and size have not changed.  This is identical to the logic used in
    the original script.
    """
    try:
        st = path.stat()
    except FileNotFoundError:
        return ""
    key = str(path)
    meta = f"{st.st_mtime_ns}:{st.st_size}"
    with _HASH_LOCK:
        entry = _HASH_DB.get(key)
        if entry and entry.get("meta") == meta:
            return entry["sha256"]
    h = hashlib.sha256()
    with path.open("rb", buffering=1024 * 1024) as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    digest = h.hexdigest()
    with _HASH_LOCK:
        _HASH_DB[key] = {"meta": meta, "sha256": digest}
    return digest


# -----------------------------------------------------------------------------
# Playlist update helpers

def update_playlists(zotify_dir: str, sp: spotipy.Spotify) -> List[str]:
    """Update all playlists under ``zotify_dir`` from Spotify.

    For each playlist directory, this function reads the hidden file
    ``.<playlist_name>`` to obtain the playlist URL, fetches the latest
    tracks from Spotify, and writes a fresh ``.m3u8`` file using
    ``write_playlist_to_m3u8``.  Any errors encountered while fetching the
    playlist (invalid URL, API errors, timeouts) are logged and the
    playlist is skipped.

    Parameters
    ----------
    zotify_dir: str
        Path to the directory containing per-playlist subdirectories.
    sp: spotipy.Spotify
        An authenticated Spotify client.

    Returns
    -------
    List[str]
        A list of paths to the generated ``.m3u8`` files.
    """
    playlists: List[str] = []
    CustomPrint.bold_print("\n>>> Updating playlists...")
    for root, dirs, _ in os.walk(zotify_dir):
        for dir_name in dirs:
            playlist_dir = os.path.join(root, dir_name)
            hidden_file_path = os.path.join(playlist_dir, f".{dir_name}")
            sys_stdout = sys.stdout
            # Display progress similar to the original script
            sys_stdout.write(
                f"\r{CustomPrint.bold('>>> Fetching tracks from')} {dir_name[:22]}...{' ' * 30}"
            )
            sys_stdout.flush()
            if not os.path.exists(hidden_file_path):
                continue
            try:
                with open(hidden_file_path, 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    if first_line.startswith("#"):
                        # new format: skip the comment line
                        playlist_url = f.readline().strip()
                    else:
                        playlist_url = first_line
                if not playlist_url.startswith("https://open.spotify.com/playlist/"):
                    CustomPrint.bold_print(
                        f"!!! ERROR: Invalid playlist URL in {hidden_file_path}", "RED"
                    )
                    continue
            except Exception as e:
                CustomPrint.bold_print(
                    f"!!! ERROR: Problem reading {hidden_file_path}: {e}", "RED"
                )
                continue
            try:
                playlist_id = playlist_url.split("/")[-1].split("?")[0]
                playlist_data = sp.playlist(playlist_id)
                original_playlist_name = playlist_data["name"].strip()
                m3u_file = os.path.join(playlist_dir, f"{original_playlist_name}.m3u8")
                tracks = fetch_playlist_tracks(sp, playlist_url)
                write_playlist_to_m3u8(tracks, m3u_file)
                playlists.append(m3u_file)
            except (SpotifyException, ReadTimeout) as e:
                CustomPrint.bold_print(
                    f"!!! ERROR: Unable to fetch playlist: {e}", "RED"
                )
                CustomPrint.print(f"Skipping playlist: {playlist_dir}")
            except Exception as e:
                CustomPrint.bold_print(
                    f"!!! ERROR during processing playlist: {e}", "RED"
                )
                CustomPrint.print(f"Skipping playlist: {playlist_dir}")
    return playlists


# -----------------------------------------------------------------------------
# Duplicate file synchronization

def find_newest_versions(zotify_music_dir: str) -> Dict[str, Path]:
    """Return a mapping from filename to the newest file on disk.

    This helper scans all MP3 files under ``zotify_music_dir`` and returns a
    dictionary mapping each filename to the path of the file with the most
    recent modification time.  It retains the original logic from
    ``consolidate_library.py``.
    """
    newest: Dict[str, Tuple[float, Path]] = {}
    for root, dirs, files in os.walk(zotify_music_dir):
        for fn in files:
            if not fn.lower().endswith(".mp3"):
                continue
            p = Path(root) / fn
            try:
                mt = p.stat().st_mtime
            except FileNotFoundError:
                continue
            cur = newest.get(fn)
            if not cur or mt > cur[0]:
                newest[fn] = (mt, p)
    return {name: path for name, (_, path) in newest.items()}


def replace_outdated_files(
    zotify_music_dir: str,
    newest_by_name: Dict[str, Path],
) -> Tuple[int, int]:
    """Synchronize duplicates across playlists by copying newer files over older ones.

    For each filename present in multiple places under ``zotify_music_dir``, this
    function copies the newest version to all other locations where that name
    appears.  It returns a tuple ``(replaced, skipped)`` indicating how many
    files were overwritten and how many were skipped because they were already
    identical.
    """
    replaced = 0
    skipped = 0
    groups: Dict[str, List[Path]] = {}
    for root, dirs, files in os.walk(zotify_music_dir):
        for fn in files:
            if not fn.lower().endswith(".mp3"):
                continue
            p = Path(root) / fn
            groups.setdefault(fn, []).append(p)
    for name, paths in groups.items():
        if len(paths) <= 1:
            continue
        src = newest_by_name.get(name)
        if not src or not src.exists():
            continue
        try:
            src_size = src.stat().st_size
        except FileNotFoundError:
            continue
        src_hash: Optional[str] = None
        for dst in paths:
            if dst == src:
                continue
            try:
                if src.samefile(dst):
                    continue
            except Exception:
                pass
            try:
                dst_size = dst.stat().st_size
            except FileNotFoundError:
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)
                replaced += 1
                continue
            if dst_size == src_size:
                if src_hash is None:
                    src_hash = _cached_sha256(src)
                if _cached_sha256(dst) == src_hash:
                    skipped += 1
                    continue
            shutil.copy2(src, dst)
            try:
                st = src.stat()
                os.utime(dst, (st.st_atime, st.st_mtime))
            except Exception:
                pass
            replaced += 1
    CustomPrint.bold_print(
        f"\n>>> Synchronized {replaced} files across duplicate groups.", "GREEN"
    )
    return replaced, skipped


def manage_zotify_library(zotify_music_dir: str) -> Tuple[int, int]:
    """Update outdated songs in the Zotify music library.

    Returns a tuple ``(replaced, skipped)`` representing the number of files
    replaced and the number of duplicates skipped.
    """
    CustomPrint.bold_print(">>> Updating outdated songs in Zotify Music library...")
    time.sleep(1)
    newest_versions = find_newest_versions(zotify_music_dir)
    replaced_count, skipped_count = replace_outdated_files(zotify_music_dir, newest_versions)
    if replaced_count > 0:
        CustomPrint.bold_print(
            f">>> Updating {replaced_count} songs with outdated metadata...", "GREEN"
        )
        time.sleep(2)
    time.sleep(2)
    return replaced_count, skipped_count


# -----------------------------------------------------------------------------
# Consolidation helpers

def copy_file_if_newer(src_file: Path, dest_file: Path) -> None:
    """Copy ``src_file`` to ``dest_file`` if ``src_file`` is newer.

    If ``dest_file`` does not exist or is older than ``src_file``, the contents
    are copied using ``shutil.copy2`` (which preserves metadata).  Otherwise
    nothing is done.
    """
    if not dest_file.exists() or src_file.stat().st_mtime > dest_file.stat().st_mtime:
        shutil.copy2(src_file, dest_file)


def process_m3u_file(src_file: Path, dest_file: Path) -> None:
    """Sanitize and copy an M3U8 file.

    The source file is read line by line.  Lines beginning with ``#`` are
    written verbatim.  Other lines (which are assumed to contain file
    references) are stripped, sanitized via ``sanitize_filename`` and
    written to ``dest_file``.
    """
    with open(src_file, 'r') as f_in, open(dest_file, 'w') as f_out:
        for line in f_in:
            if not line.startswith("#"):
                sanitized_line = sanitize_filename(line.strip())
                f_out.write(f"{sanitized_line}\n")
            else:
                f_out.write(line)


def consolidate_library(src_dirs: List[str], dest_dir: str) -> None:
    """Consolidate music from ``src_dirs`` into ``dest_dir``.

    For each source directory, all ``.mp3`` files are copied (with
    name sanitization) to ``dest_dir`` and any ``.m3u8`` playlists are
    sanitized and copied verbatim.  The destination directory and any
    necessary parent directories are created on the fly.
    """
    dest_path = Path(dest_dir)
    dest_path.mkdir(parents=True, exist_ok=True)
    for src_dir in src_dirs:
        for root, _, files in os.walk(src_dir):
            for file in sorted(files):
                src_file = Path(root) / file
                if file.endswith(".mp3"):
                    dest_file = dest_path / sanitize_filename(file)
                    copy_file_if_newer(src_file, dest_file)
                elif file.endswith(".m3u8"):
                    dest_file = dest_path / file
                    process_m3u_file(src_file, dest_file)


# -----------------------------------------------------------------------------
# Playlist and library analysis helpers

def get_valid_songs_from_m3u(filepath: Union[str, Path]) -> List[str]:
    """Return a list of unique, valid song references from an M3U8 file.

    Empty lines and comments are ignored.  Extended M3U format (where
    metadata lines begin with ``#EXTINF``) is handled by skipping
    metadata lines and only capturing the file references.
    """
    path = Path(filepath)
    with open(path, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]
    if not lines:
        return []
    if lines[0] == "#EXTM3U":
        song_paths = [lines[i] for i in range(1, len(lines), 2)]
    else:
        song_paths = [line for line in lines if not line.startswith('#')]
    seen: Set[str] = set()
    unique_songs: List[str] = []
    for song in song_paths:
        if song not in seen:
            seen.add(song)
            unique_songs.append(song)
    return unique_songs


def count_songs_in_m3u(filepath: Union[str, Path]) -> int:
    """Return the number of unique, valid song paths in an M3U8 file."""
    return len(get_valid_songs_from_m3u(filepath))


def count_unique_songs_in_m3u(m3u_filepath: Union[str, Path]) -> int:
    """Return the number of unique song references in an M3U8 file."""
    with open(m3u_filepath, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return len(set(lines))


def check_missing_songs_in_playlists(m3u_dir: str, songs_dir: str) -> Dict[str, List[str]]:
    """Return a mapping of playlist filename to songs missing from the library.

    For each ``.m3u8`` file in ``m3u_dir``, this function compares the song
    references listed in the playlist to the actual files present in
    ``songs_dir``.  If any referenced songs are missing, they are collected
    into a list keyed by the playlist filename.
    """
    missing_songs: Dict[str, List[str]] = {}
    for file in os.listdir(m3u_dir):
        if file.endswith('.m3u8'):
            m3u_path = os.path.join(m3u_dir, file)
            valid_songs = get_valid_songs_from_m3u(m3u_path)
            missing: List[str] = []
            for song in valid_songs:
                song_path = song if os.path.isabs(song) else os.path.join(songs_dir, song)
                if not os.path.exists(song_path):
                    missing.append(song)
            if missing:
                missing_songs[file] = missing
    return missing_songs


def get_playlist_links(zotify_music_dir: str) -> Dict[str, str]:
    """Return a mapping from sanitized playlist name to its Spotify URL.

    This helper scans each playlist directory in ``zotify_music_dir`` for
    hidden files named ``.<playlist_name>`` or ``.playlist_name``.  The first
    non-empty line starting with ``http`` is considered the playlist URL.
    The base filename of the ``.m3u8`` playlist is used as the key after
    sanitization.  Errors encountered while reading hidden files are logged
    using ``CustomPrint``.
    """
    playlist_links: Dict[str, str] = {}
    base_dir = os.path.expanduser(zotify_music_dir)
    for playlist_name in os.listdir(base_dir):
        playlist_dir = os.path.join(base_dir, playlist_name)
        if os.path.isdir(playlist_dir):
            hidden_file_path = os.path.join(playlist_dir, f".{playlist_name}")
            if os.path.isfile(hidden_file_path):
                try:
                    with open(hidden_file_path, 'rb') as f:
                        content = f.read().strip()
                        try:
                            playlist_url = content.decode('utf-8').strip()
                        except UnicodeDecodeError:
                            playlist_url = content.decode('latin-1').strip()
                    m3u_file = None
                    for file in os.listdir(playlist_dir):
                        if file.endswith(".m3u8"):
                            m3u_file = file
                            break
                    if m3u_file:
                        base_name = os.path.splitext(m3u_file)[0]
                        sanitized_base = sanitize_filename(base_name)
                        playlist_links[sanitized_base] = playlist_url
                except Exception as e:
                    CustomPrint.bold_print(
                        f"!!! ERROR: Problem reading {hidden_file_path}: {e}", "RED"
                    )
    return playlist_links


def _same_name(a: str, b: str) -> bool:
    """Casefolded string equality with NFC normalization and trimming."""
    norm = lambda s: unicodedata.normalize("NFC", s).strip().casefold()
    return norm(a) == norm(b)


def get_playlist_meta(zotify_music_dir: str) -> Dict[str, Dict[str, Optional[str]]]:
    """Return playlist metadata keyed by sanitized folder name.

    The return value is a dictionary mapping each playlist directory name
    (after sanitization) to a dictionary with ``title`` (the comment from
    the hidden file) and ``url`` (the Spotify URL).  Missing values are
    represented by ``None``.
    """
    import unicodedata as _ud
    meta: Dict[str, Dict[str, Optional[str]]] = {}
    for entry in os.scandir(zotify_music_dir):
        if not entry.is_dir():
            continue
        folder = entry.name
        hidden1 = os.path.join(entry.path, f".{folder}")
        hidden2 = os.path.join(entry.path, ".playlist_name")
        hidden = hidden1 if os.path.isfile(hidden1) else hidden2 if os.path.isfile(hidden2) else None
        title: Optional[str] = None
        url: Optional[str] = None
        if hidden:
            try:
                with open(hidden, "r", encoding="utf-8") as f:
                    for line in f:
                        s = line.strip()
                        if not s:
                            continue
                        if s.startswith("#"):
                            title = s.lstrip("#").strip()
                        elif s.startswith("http"):
                            url = s
            except OSError:
                pass
        meta[sanitize_filename(folder)] = {"title": title, "url": url}
    return meta


def check_orphan_and_leftover_songs(
    playlist_dir: Union[str, Path],
    library_dir: Union[str, Path],
    local_files_dir: Union[str, Path],
) -> Tuple[Set[str], Set[str]]:
    """Return sets of orphan and leftover songs.

    ``orphan_songs`` are tracks present in ``library_dir`` but not referenced
    in any playlists in ``playlist_dir``.  ``leftover_songs`` are orphan
    songs that also do not appear in ``local_files_dir``.  Paths are
    sanitized to ensure consistent comparisons.
    """
    playlist_dir = Path(playlist_dir).expanduser()
    library_dir = Path(library_dir).expanduser()
    local_files_dir = Path(local_files_dir).expanduser()
    all_songs: Set[str] = set()
    songs_in_playlists: Set[str] = set()
    local_files_songs: Set[str] = set()
    for playlist_file in playlist_dir.glob("*.m3u8"):
        with playlist_file.open('r') as f:
            songs_in_playlist = {
                sanitize_filename(line.strip())
                for line in f
                if line.strip() and not line.startswith("#")
            }
            songs_in_playlists.update(songs_in_playlist)
    for song_path in library_dir.rglob("*.mp3"):
        if not song_path.name.startswith("._"):
            all_songs.add(sanitize_filename(song_path.name))
    for song_path in local_files_dir.rglob("*.mp3"):
        local_files_songs.add(sanitize_filename(song_path.name))
    orphan_songs = all_songs - songs_in_playlists
    leftover_songs = orphan_songs - local_files_songs
    return orphan_songs, leftover_songs


def remove_leftover_songs(
    leftover_songs: Set[str],
    library_dir: Union[str, Path],
    *,
    auto_confirm: bool = False,
) -> None:
    """Delete leftover songs from the consolidated library.

    If ``auto_confirm`` is ``False``, the user is prompted before any
    deletion occurs.  If confirmed (or ``auto_confirm`` is ``True``), each
    song in ``leftover_songs`` is removed from ``library_dir``.  Errors
    encountered while deleting are logged using ``CustomPrint``.
    """
    if leftover_songs:
        CustomPrint.bold_print(f"\n>>> Leftover songs [{len(leftover_songs)}]:")
        for song in sorted(leftover_songs):
            print(f"\t{song}")
        confirm = 'y' if auto_confirm else input(
            CustomPrint.bold("\n### Do you want to remove these leftover songs? (y/n): ", "PURPLE")
        ).strip().lower()
        if confirm == 'y':
            for song in leftover_songs:
                song_path = Path(library_dir) / song
                if song_path.exists():
                    try:
                        song_path.unlink()
                        print(f"Removed {song} from {song_path}")
                    except Exception as e:
                        CustomPrint.bold_print(f"!!! Error removing {song}: {e}", "RED")
            CustomPrint.bold_print("\n>>> Leftover songs removed.")
        else:
            CustomPrint.bold_print("\n>>> Leftover songs not removed.")


# -----------------------------------------------------------------------------
# Album artwork helpers

def get_album_cover_dimensions(file_path: Path) -> Optional[Tuple[int, int]]:
    """Return the dimensions of the album cover embedded in an MP3 file.

    If no cover art is found, returns ``None``.  Any exceptions raised by
    Mutagen or PIL are caught and logged with ``CustomPrint``.  The return
    value mirrors the original implementation.
    """
    try:
        audio = MP3(file_path, ID3=ID3)
        for tag in audio.tags.values():
            if isinstance(tag, APIC):
                with Image.open(io.BytesIO(tag.data)) as img:
                    return img.size
    except Exception as e:
        CustomPrint.bold_print(
            f"!!! Error reading album cover dimensions for {file_path}: {e}", "RED"
        )
    return None


def resize_album_cover(
    img: Image.Image, *, size: Tuple[int, int] = (1000, 1000)
) -> bytes:
    """Resize an image to the specified ``size`` and return the JPEG bytes."""
    img_resized = img.convert("RGB").resize(size, Image.LANCZOS)
    with io.BytesIO() as output:
        img_resized.save(output, format='JPEG', quality=100)
        return output.getvalue()


def update_album_cover(file_path: Path, new_cover_data: bytes) -> None:
    """Replace the album cover of an MP3 file with ``new_cover_data``."""
    audio = MP3(file_path, ID3=ID3)
    audio.tags.delall("APIC")
    audio.tags.add(
        APIC(
            encoding=3,
            mime='image/jpeg',
            type=3,
            desc='Cover',
            data=new_cover_data,
        )
    )
    audio.save()


def resize_album_covers(
    directory: Path,
    *,
    target_size: Tuple[int, int] = (1000, 1000),
) -> None:
    """Resize album covers in all MP3 files under ``directory``.

    Any file that already contains an album cover with matching ``target_size``
    is skipped.  Errors encountered while reading or writing tags are
    logged using ``CustomPrint``.
    """
    for file_path in directory.rglob('*.mp3'):
        try:
            cover_dimensions = get_album_cover_dimensions(file_path)
            if cover_dimensions == target_size:
                continue
            audio = MP3(file_path, ID3=ID3)
            for tag in audio.tags.values():
                if isinstance(tag, APIC):
                    with Image.open(io.BytesIO(tag.data)) as img:
                        new_cover_data = resize_album_cover(img, size=target_size)
                        update_album_cover(file_path, new_cover_data)
                    break
        except Exception as e:
            CustomPrint.bold_print(
                f"!!! ERROR: Problem processing album cover for {file_path}: {e}", "RED"
            )