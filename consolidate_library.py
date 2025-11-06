"""
consolidate_library.py
======================

This script orchestrates the high‑level workflow for maintaining a Zotify
music library on macOS. It relies on helpers from ``library_manager`` to
update playlists from Spotify, synchronize duplicate files, merge multiple
music folders into one consolidated library, and produce a summary of
playlist status. The goal of this entry point is to match the behaviour
of the original ``consolidate_library.py`` while delegating the heavy
lifting to well‑named functions in separate modules.

Running this script will prompt you to optionally refresh your local
playlist files from Spotify, update your Zotify Music library for any
outdated songs, consolidate multiple source directories into a single
``Consolidated Library`` folder, and then display a summary of
incomplete and complete playlists. It also detects orphaned tracks,
offers to remove leftover songs, and ensures album artwork is resized
to a standard dimension.

Usage:

    python consolidate_library.py

Ensure your Spotify API credentials (SPOTIPY_CLIENT_ID,
SPOTIPY_CLIENT_SECRET, SPOTIPY_REDIRECT_URI) are set in your
environment. The default redirect URI used here is
``http://127.0.0.1:8888/callback`` which must match the settings in
your Spotify Developer Dashboard.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Set, Tuple

from utils import CustomPrint, sanitize_filename
from spotify_api import get_spotify_client
import library_manager

SCOPE = "playlist-read-private"
CACHE_PATH = str(Path.home() / ".cache-spotipy")  # same as extractor
REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")  # same as extractor

def print_playlists_summary(
    zotify_music_dir: str,
    consolidated_library_dir: str,
    local_files_dir: str,
) -> None:
    """Print a summary of playlist completeness and orphan tracks.

    After playlists have been updated and libraries consolidated, this
    function examines all ``.m3u8`` files in ``consolidated_library_dir``
    to determine which playlists have missing songs. It then prints
    incomplete playlists (including optional titles and links), lists
    complete playlists with their song counts, identifies orphan and
    leftover songs, and optionally removes leftover tracks.

    Parameters
    ----------
    zotify_music_dir: str
        Path to the original Zotify Music directory containing per‑playlist
        subdirectories.
    consolidated_library_dir: str
        Path to the directory containing consolidated ``.m3u8`` playlists
        and the merged music library.
    local_files_dir: str
        Path to the directory containing your user "Local Files" library.
    """

    # Compute playlists missing songs
    missing_map = library_manager.check_missing_songs_in_playlists(
        consolidated_library_dir, consolidated_library_dir
    )
    missing_map = dict(sorted(missing_map.items()))

    # Retrieve metadata (titles and URLs) from hidden playlist files
    playlist_meta = library_manager.get_playlist_meta(zotify_music_dir)

    # Print header
    CustomPrint.separator("Playlists Summary")

    # Incomplete playlists
    if missing_map:
        CustomPrint.bold_print(">>> Incomplete playlists:")
        for m3u_file, missing in missing_map.items():
            name = Path(m3u_file).stem
            key = sanitize_filename(name)
            info: Dict[str, str | None] = playlist_meta.get(key, {})
            title = info.get("title")
            url = info.get("url")

            print(f"\n\t{name.ljust(40)} [{len(missing)} Missing Songs]")
            # Print an alternate title if provided and not the same as the file name
            try:
                if title and not library_manager._same_name(name, title):
                    print(f"\t# {title}")
            except Exception:
                # Fallback: ignore title mismatch errors
                if title:
                    print(f"\t# {title}")
            # Print the playlist URL if available
            if url:
                print(f"\n{url}\n")
            else:
                print("\t\tNo link found\n")
    else:
        CustomPrint.bold_print("*** All playlists are complete.", "GREEN")

    # Complete playlists
    all_playlists = [
        file
        for file in os.listdir(os.path.expanduser(consolidated_library_dir))
        if file.endswith(".m3u8")
    ]
    missing_set: Set[str] = set(missing_map.keys())
    all_playlists.sort()
    CustomPrint.bold_print("\n>>> Complete playlists:")
    for playlist_file in all_playlists:
        if playlist_file not in missing_set:
            playlist_path = os.path.join(consolidated_library_dir, playlist_file)
            song_count: int = library_manager.count_unique_songs_in_m3u(playlist_path)
            original_name: str = os.path.splitext(playlist_file)[0]
            display_name = (
                original_name
                if len(original_name) <= 38
                else original_name[:35] + "..."
            )
            spacing = " " * (3 if song_count < 10 else 2 if song_count < 100 else 1)
            print(
                f"\t{display_name.ljust(40)} [{spacing}{song_count} Songs]"
            )

    # Orphan and leftover songs
    orphan_songs, leftover_songs = library_manager.check_orphan_and_leftover_songs(
        consolidated_library_dir,
        consolidated_library_dir,
        local_files_dir,
    )
    if orphan_songs:
        CustomPrint.bold_print(
            f"\n>>> Orphan songs [{len(orphan_songs)}]:"
        )
        for song in sorted(orphan_songs):
            print(f"\t{song}")

    # Prompt to remove leftover songs and perform deletion if requested
    library_manager.remove_leftover_songs(leftover_songs, consolidated_library_dir)

    # Resize album covers in the consolidated library
    CustomPrint.bold_print("\n>>> Album cover resizing...")
    try:
        library_manager.resize_album_covers(Path(consolidated_library_dir))
    except Exception as e:
        CustomPrint.bold_print(
            f"!!! ERROR: Problem while resizing album covers: {e}", "RED"
        )
        return
    CustomPrint.bold_print("*** Album cover resizing complete.\n", "GREEN")


def main() -> None:
    """Entry point for the consolidate library workflow."""
    CustomPrint.print_banner("Consolidate Library")

    # Define the source directories and the consolidated library directory
    zotify_music_dir = os.path.expanduser("~/Music/Zotify Music")
    local_files_dir = os.path.expanduser("~/Music/Local Files")
    consolidated_library_dir = os.path.expanduser("~/Music/Consolidated Library")

    # Authenticate with Spotify using a loopback redirect URI
    sp = get_spotify_client(
        scope=SCOPE,
        cache_path=CACHE_PATH,
        redirect_uri=REDIRECT_URI,
    )

    # Optionally refresh playlists from Spotify
    ans = input(
        CustomPrint.bold(
            "### Update playlists from Spotify? (y/n): ", "PURPLE"
        )
    ).strip().lower()
    if ans == "y":
        library_manager.update_playlists(zotify_music_dir, sp)
    else:
        CustomPrint.bold_print(">>> Skipping playlist update.")

    # Update Zotify Music library for outdated songs
    library_manager.manage_zotify_library(zotify_music_dir)

    # Consolidate libraries
    ans = input(
        CustomPrint.bold(
            "### Consolidate into Consolidated Library? (y/n): ", "PURPLE"
        )
    ).strip().lower()
    if ans == "y":
        library_manager.consolidate_library(
            [zotify_music_dir, local_files_dir], consolidated_library_dir
        )
        CustomPrint.bold_print(">>> Consolidation complete.", "GREEN")
    else:
        CustomPrint.bold_print(">>> Skipping consolidation.")

    # Print summary of playlists and orphaned tracks
    print_playlists_summary(zotify_music_dir, consolidated_library_dir, local_files_dir)


if __name__ == "__main__":
    main()
