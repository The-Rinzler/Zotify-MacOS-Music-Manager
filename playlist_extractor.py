import os
import sys
import re
import argparse
import spotipy
import unicodedata
from spotipy.oauth2 import SpotifyOAuth
from mutagen.easyid3 import EasyID3
from mutagen.id3 import ID3NoHeaderError
from pathlib import Path
import shutil
import time
from collections import defaultdict
from utils import CustomPrint
from utils import sanitize_filename
from utils import write_playlist_to_m3u8
from utils import get_possible_filenames
from utils import create_hidden_playlist_link_file
from datetime import datetime

# Import centralized Spotify API helpers and song ID management functions.
from spotify_api import (
    get_spotify_client,
    extract_playlist_id as sp_extract_playlist_id,
    fetch_playlist_tracks,
)
from song_ids_manager import (
    reconcile_song_ids_flip_ids as sid_reconcile_song_ids_flip_ids,
    cleanup_song_ids_orphans as sid_cleanup_song_ids_orphans,
    build_existing_keys_set as sid_build_existing_keys_set,
    diff_song_ids_vs_playlist_existing_only as sid_diff_song_ids_vs_playlist_existing_only,
    apply_song_ids_from_playlist_existing_only as sid_apply_song_ids_from_playlist_existing_only,
    maybe_update_song_ids_from_playlist_existing_only as sid_maybe_update_song_ids_from_playlist_existing_only,
    rename_single_variant_to_canonical as sid_rename_single_variant_to_canonical,
)


def get_playlist_url():
    """Get the playlist URL from command-line argument or prompt user."""
    parser = argparse.ArgumentParser(description="Extract and manage Spotify playlists.")
    parser.add_argument('playlist_url', nargs='?', help="Spotify playlist URL")
    args = parser.parse_args()

    if args.playlist_url:
        playlist_url = args.playlist_url.strip()
    else:
        playlist_url = input(CustomPrint.bold("### Enter your Spotify playlist URL: ", "PURPLE")).strip()

    return playlist_url


if __name__ == "__main__":
    try:
        # Get the playlist URL
        playlist_url = get_playlist_url()


        def clean_playlist_url(url):
            return url.split('?')[0]


        playlist_url = clean_playlist_url(playlist_url)

        if not playlist_url.startswith("https://open.spotify.com/playlist/"):
            CustomPrint.bold_print("!!! ERROR: Invalid playlist URL.\n", "RED")
            exit()

        # Check if the playlist URL is empty or invalid
        if not playlist_url:
            CustomPrint.bold_print("!!! ERROR: No playlist URL provided.", "RED")
            exit()

        # Get the user's home directory
        home_dir = os.path.expanduser("~")

        CustomPrint.print_banner("Playlist Extractor")



        # Define the scope for the Spotify API
        scope = "playlist-read-private"

        # Specify a cache path
        cache_path = os.path.join(str(Path.home()), ".cache-spotipy")

        # Set up Spotify authentication using centralized helper.  This will
        # reuse cached credentials and environment variables in the same way
        # as the original code.
        sp = get_spotify_client(
            scope=scope,
            cache_path=cache_path,
            redirect_uri=os.getenv('SPOTIPY_REDIRECT_URI'),
        )

        # Function to extract the playlist ID from the URL


        # Extract the playlist ID from the URL
        def extract_playlist_id(playlist_url: str) -> str:
            """Extract playlist ID from the given URL."""
            return playlist_url.split("/")[-1].split("?")[0]


        # Use the shared helper to extract the playlist ID from the URL
        playlist_id = sp_extract_playlist_id(playlist_url)

        # Fetch the playlist details
        playlist = sp.playlist(playlist_id)
        playlist_name = playlist['name']
        CustomPrint.separator(f"Playlist Name: {playlist_name}")
        playlist_name = playlist_name.strip()
        sanitized_playlist_name = sanitize_filename(playlist_name.strip())

        # Fetch all playlist items using the centralized API helper.  This
        # transparently handles pagination and mirrors the behaviour of the
        # original while-loop.
        tracks = fetch_playlist_tracks(sp, playlist_url)

        # Define the base music path relative to the home directory
        base_music_path = os.path.join(home_dir, "Music", "Zotify Music")


        def _nfc(s: str) -> str:
            return unicodedata.normalize("NFC", s or "")


        def _base_name(artist: str, title: str) -> str:
            return _nfc(f"{artist} - {title}").casefold()


        def reconcile_song_ids_flip_ids(
                playlist_dir: str,
                playlist_tracks: list[dict],  # each: {"id","artist","title"}
        ) -> tuple[int, int, int]:
            """
            High-signal reconciliation:
            - Add missing rows for existing files.
            - Flip IDs in-place when filename matches current playlist entry but ID differs.
            - Deduplicate by filename key, keep the newest timestamp and the correct ID.
            Returns: (added_rows, flipped_ids, deduped_rows)
            """
            sid_path = Path(playlist_dir) / ".song_ids"
            sid_path.touch()

            # Read
            rows = []
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
                        "key": _base_name(artist, title),
                    })

            # Index current state
            by_fname = {r["fname"]: r for r in rows}  # filename → row
            by_key = {}  # artist-title key → row (prefer newest)
            for r in rows:
                if (k := r["key"]) not in by_key or by_key[k]["ts"] < r["ts"]:
                    by_key[k] = r

            # Build current playlist view
            pl = {}
            for t in playlist_tracks:
                key = _base_name(t["artist"], t["title"])
                pl[key] = {"id": t["id"], "artist": t["artist"], "title": t["title"],
                           "fname": f'{t["artist"]} - {t["title"]}.mp3'}

            added = flipped = deduped = 0

            # For each playlist entry, if local file exists:
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
                    # Row exists but ID differs → flip in place
                    if row["id"] != cur["id"] and row["artist"] == cur["artist"] and row["title"] == cur["title"]:
                        row["id"] = cur["id"]
                        # refresh timestamp to now to reflect correction
                        row["ts"] = time.strftime("%Y-%m-%d %H:%M:%S")
                        flipped += 1
                        by_fname[fname] = row
                        by_key[key] = row

            # Deduplicate by filename: keep last timestamp, prefer matching playlist ID when conflict
            final_by_fname = {}
            for r in rows:
                k = r["fname"]
                keep = r
                if k in final_by_fname:
                    prev = final_by_fname[k]
                    # prefer row whose id equals current playlist id for that key
                    want = pl.get(r["key"], {}).get("id")
                    if want and r["id"] == want and prev["id"] != want:
                        keep = r
                    elif not want:
                        keep = max([prev, r], key=lambda x: x["ts"])
                    else:
                        keep = max([prev, r], key=lambda x: x["ts"])
                    if keep is not prev:
                        deduped += 1
                final_by_fname[k] = keep

            # Write back
            out_lines = []
            for r in sorted(final_by_fname.values(), key=lambda x: (x["artist"].casefold(), x["title"].casefold())):
                out_lines.append("\t".join([r["id"], r["ts"], r["artist"], r["title"], r["fname"]]) + "\n")

            with sid_path.open("w", encoding="utf-8") as f:
                f.writelines(out_lines)

            return added, flipped, deduped

        def find_matching_playlist_dir(playlist_url, base_music_path):
            for dir_name in os.listdir(base_music_path):
                sanitized_dir_name = sanitize_filename(dir_name)
                # Rename the directory if needed
                if dir_name != sanitized_dir_name:
                    os.rename(
                        os.path.join(base_music_path, dir_name),
                        os.path.join(base_music_path, sanitized_dir_name)
                    )
                    dir_name = sanitized_dir_name

                dir_path = os.path.join(base_music_path, dir_name)
                if os.path.isdir(dir_path):
                    hidden_file_path = os.path.join(dir_path, f".{dir_name}")
                    if os.path.isfile(hidden_file_path):
                        with open(hidden_file_path, 'r') as file:
                            url = file.read().strip()
                            if url == playlist_url:
                                return dir_path
            return None


        # Check for a matching playlist directory
        matching_dir = find_matching_playlist_dir(playlist_url, base_music_path)

        # Ensure the base directory exists
        if not os.path.exists(base_music_path):
            os.makedirs(base_music_path)

        # Check for existing playlist directories and handle accordingly
        existing_playlist_dirs = [
            sanitize_filename(dir_name) for dir_name in os.listdir(base_music_path)
            if os.path.isdir(os.path.join(base_music_path, dir_name))
        ]

        if matching_dir:
            # Playlist directory matching the URL found
            playlist_dir = matching_dir
            if playlist_name in existing_playlist_dirs:
                if playlist_name != os.path.basename(matching_dir):
                    new_playlist_dir = os.path.join(base_music_path, playlist_name)
                    # Ensure the new directory name doesn't already exist or isn't the same as the old one
                    if not os.path.exists(new_playlist_dir):
                        os.rename(matching_dir, new_playlist_dir)
                        time.sleep(1)  # Add a short delay after renaming
                        CustomPrint.bold_print(
                            f">>> Playlist directory renamed from {os.path.basename(matching_dir)} to {playlist_name}")
                        playlist_dir = new_playlist_dir
        else:
            # No matching directory found, create a new one
            playlist_dir = os.path.join(base_music_path, sanitize_filename(playlist_name))
            if not os.path.exists(playlist_dir):
                os.makedirs(playlist_dir)
                CustomPrint.bold_print(f"*** New playlist directory created: {playlist_name}", "GREEN")

        # Create the hidden playlist file containing the trimmed URL
        create_hidden_playlist_link_file(playlist_dir, playlist_name, playlist_url)


        # Remove extra .m3u8 files in the playlist directory
        def remove_extra_m3u8_files(playlist_dir, playlist_name):
            m3u8_file = f"{playlist_name}.m3u8"
            m3u8_file_path = os.path.join(playlist_dir, m3u8_file)

            for file in os.listdir(playlist_dir):
                if file.endswith(".m3u8") and file != m3u8_file:
                    file_path = os.path.join(playlist_dir, file)
                    os.remove(file_path)
                    CustomPrint.bold_print(f">>> Removed extra .m3u8 file: {file}")

            return m3u8_file_path
        # Remove any extra .m3u8 files
        output_file = remove_extra_m3u8_files(playlist_dir, playlist_name)


        def remove_extra_hidden_files(playlist_dir, playlist_name):
            allowed_hidden_files = [".song_ids", f".{playlist_name}"]

            for file in os.listdir(playlist_dir):
                if file.startswith(".") and file not in allowed_hidden_files:
                    file_path = os.path.join(playlist_dir, file)
                    os.remove(file_path)
                    CustomPrint.bold_print(f">>> Removed extra hidden file: {file}")


        # When calling the function, pass the sanitized playlist name:
        remove_extra_hidden_files(playlist_dir, sanitized_playlist_name)


        # Function to find and copy the relevant metadata line from the source .song_ids file
        def copy_metadata_line(src_dir, author_name, song_name, dest_song_ids_file):
            src_song_ids_file = os.path.join(src_dir, ".song_ids")
            if os.path.exists(src_song_ids_file):
                with open(src_song_ids_file, "r", encoding="utf-8") as file:
                    for line in file:
                        parts = line.strip().split("\t")
                        if len(parts) == 5 and parts[2] == author_name and parts[3] == song_name:
                            with open(dest_song_ids_file, "a", encoding="utf-8") as dest_file:
                                dest_file.write(line)
                            return


        def _norm_key(artist: str, title: str) -> tuple[str, str]:
            return (_nfc((artist or "").strip()).casefold(),
                    _nfc((title or "").strip()).casefold())


        def _read_song_ids_rows(hidden_file_path: str) -> list[dict[str, any]]:
            rows = []
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
                    track_id, timestamp, artist, song, filename = parts
                    try:
                        ts = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        ts = datetime.min
                    rows.append({
                        "id": track_id, "ts": ts, "ts_s": timestamp,
                        "artist": artist, "song": song, "filename": filename
                    })
            return rows


        def cleanup_song_ids_orphans(playlist_dir: str) -> dict:
            """
            Remove stale rows from .song_ids where the file is no longer present on disk.
            Keeps rows whose 'filename' exists in 'playlist_dir'.
            Writes back deterministically (artist, title sort).
            Returns simple stats dict.
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

            # Deterministic write like you do elsewhere
            new_lines = []
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


        def rename_single_variant_to_canonical(playlist_dir: str, playlist_tracks: list[dict[str, any]]):
            """
            If exactly one variant like 'Artist - Title_1.mp3' exists and the canonical
            'Artist - Title.mp3' does not, rename the variant to the canonical filename.
            """
            # Build expected canonical names
            expected = set()
            for t in playlist_tracks:
                a = _nfc(t["artist"]);
                s = _nfc(t["title"])
                expected.add(f"{a} - {s}.mp3")

            # Index disk for bases
            by_base = {}
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
                # Exactly one variant? rename it to canonical
                if len(variants) == 1:
                    src = os.path.join(playlist_dir, variants[0])
                    dst = os.path.join(playlist_dir, canon)
                    try:
                        os.rename(src, dst)
                        print(f">>> Renamed '{variants[0]}' -> '{canon}'")
                    except Exception as e:
                        print(f">>> WARN: Could not rename '{variants[0]}' -> '{canon}': {e}")
                # If multiple variants exist, do nothing (we won't guess)


        def extract_playlist_tracks(items: list[dict]) -> list[dict]:
            """
            Flatten Spotipy playlist 'items' into [{'artist','title','id','isrc'}]
            **matching Zotify's behavior**: use track['id'] directly.
            """
            out = []
            for it in items:
                tr = it.get("track") or {}
                if not tr:
                    continue
                tid = tr.get("id")  # <-- Zotify uses plain ID
                title = _nfc(tr.get("name") or "")
                artists = tr.get("artists") or []
                artist = _nfc(artists[0].get("name", "")) if artists else ""
                isrc = ((tr.get("external_ids") or {}).get("isrc") or "").upper()
                if artist and title and tid:
                    out.append({"artist": artist, "title": title, "id": tid, "isrc": isrc})
            return out


        BASE_RE = re.compile(
            r"^(?P<artist>.+?)\s*-\s*(?P<title>.+?)(?:_(?P<suffix>\d+))?\.mp3$",
            re.IGNORECASE
        )
        SUFFIX_RE = re.compile(r'^(?P<base>.+?)_(?P<n>\d+)\.mp3$', re.IGNORECASE)


        def _norm_key(artist: str, title: str) -> tuple[str, str]:
            return (_nfc((artist or "").strip()).casefold(),
                    _nfc((title or "").strip()).casefold())


        def build_existing_keys_set(playlist_dir: str) -> set[tuple[str, str]]:
            keys = set()
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


        # Minimal, generic suffix matcher: "Artist - Title[_n].mp3"
        _SUFFIX_RE = re.compile(r"^(?P<base>.+?)(?P<suffix>_\d+)?\.mp3$", re.IGNORECASE)

        def _expected_filename(artist: str, title: str) -> str:
            return f"{artist} - {title}.mp3"


        def rename_variants_to_canonical(music_dir: str, playlist_entries: list[dict]):
            """
            For each expected (artist,title), if a single suffix-variant exists on disk like
            'Artist - Title_1.mp3' and the canonical 'Artist - Title.mp3' does not,
            rename the variant to the canonical filename.
            """
            # Build expected canonical set
            expected = {}
            for t in playlist_entries:
                artist = _nfc(t["artist"]);
                title = _nfc(t["title"])
                canon = _expected_filename(artist, title)
                expected[(artist.casefold(), title.casefold())] = canon

            # Index disk files by normalized base
            on_disk = [f for f in os.listdir(music_dir) if f.lower().endswith(".mp3")]
            by_base = {}
            for fname in on_disk:
                base, suffix = _split_mp3_name(fname)
                if base is None:
                    continue
                by_base.setdefault(base.casefold(), []).append((fname, suffix))

            # Decide renames
            for (a_key, t_key), canon in expected.items():
                canon_base, _ = _split_mp3_name(canon)
                variants = by_base.get(canon_base.casefold(), [])
                if not variants:
                    continue

                # If the canonical file already exists, nothing to do.
                if any(vf == canon for vf, _ in variants):
                    continue

                # Otherwise, if we have exactly one variant and it only differs by suffix, rename it.
                # (If there are multiple variants, we leave them for manual review.)
                non_canon_variants = [vf for vf, sfx in variants if vf != canon]
                if len(non_canon_variants) == 1:
                    src = os.path.join(music_dir, non_canon_variants[0])
                    dst = os.path.join(music_dir, canon)
                    try:
                        os.rename(src, dst)
                        print(f">>> Renamed '{non_canon_variants[0]}' -> '{canon}'")
                        # keep indexes in sync
                        variants.append((canon, ""))
                        variants.remove((non_canon_variants[0], _split_mp3_name(non_canon_variants[0])[1]))
                    except Exception as e:
                        print(f">>> WARN: Could not rename '{non_canon_variants[0]}' -> '{canon}': {e}")
                else:
                    # Multiple variants exist; do not guess—protect them from cleanup this run.
                    pass


        def _split_mp3_name(name: str):
            m = _SUFFIX_RE.match(name)
            if not m:
                return None, None
            return m.group("base") or "", m.group("suffix") or ""

        def _filename_needs_fix(filename: str, artist: str, title: str) -> bool:
            if not filename: return True
            m = _SUFFIX_RE.match(filename)
            if not m: return True
            base = m.group("base") or ""
            return _nfc(base).casefold() != _nfc(f"{artist} - {title}").casefold()


        def diff_song_ids_vs_playlist_existing_only(
                playlist_dir: str,
                playlist_tracks: list[dict[str, any]],
                restrict_keys: set
        ) -> list[dict[str, any]]:
            """
            NO-CHURN MODE:
            Compare .song_ids vs playlist IDs ONLY for keys in restrict_keys,
            but *do not* propose ID flips for existing rows.
            Emit diffs only when a file exists but .song_ids lacks a row
            (reason: "new_existing_file"). We also repair malformed filenames.
            """
            hidden_file_path = os.path.join(playlist_dir, ".song_ids")
            rows = _read_song_ids_rows(hidden_file_path)

            # sanitize rows & drop incomplete
            cleaned_rows = []
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

            groups = defaultdict(list)
            for r in cleaned_rows:
                groups[_norm_key(r["artist"], r["song"])].append(r)

            have_row_for_key = {k for k in groups.keys()}

            # Build quick map (we only need artist/title/id to craft the "new row")
            pl_list = []
            for t in playlist_tracks:
                a = _nfc(t.get("artist") or "")
                s = _nfc(t.get("title") or "")
                tid = t.get("id")
                if a and s and tid:
                    pl_list.append({"artist": a, "title": s, "id": tid})

            diffs = []
            now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            for t in pl_list:
                artist, title, tid = t["artist"], t["title"], t["id"]
                k = _norm_key(artist, title)
                if k not in restrict_keys:
                    continue

                # If there is already *any* row for this (artist,title), do not churn IDs.
                if k in have_row_for_key:
                    continue

                # No row exists but a file exists (by construction of restrict_keys) -> add it
                diffs.append({
                    "artist": artist, "song": title,
                    "old_id": None, "new_id": tid,
                    "earliest_ts_s": now_s,
                    "canonical_filename": _expected_filename(artist, title),
                    "reason": "new_existing_file"
                })

            return diffs


        def _find_existing_filename_for_base(playlist_dir: str, artist: str, title: str) -> str | None:
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
                        return f  # returns canonical or a variant like "..._1.mp3"
            except FileNotFoundError:
                pass
            return None


        def apply_song_ids_from_playlist_existing_only(
                playlist_dir: str,
                playlist_tracks: list[dict[str, any]],
                restrict_keys: set
        ) -> None:
            """
            Update .song_ids ONLY for (artist,title) in restrict_keys.
            - If a row exists: keep earliest timestamp, set ID to the playlist ID,
              update filename to the file that actually exists on disk (canonical if present, else variant).
            - If no row exists but file exists (by construction of restrict_keys): add a row with timestamp=now,
              and set filename to the actual on-disk filename (canonical or variant).
            Other songs/rows remain unchanged.
            """
            hidden_file_path = os.path.join(playlist_dir, ".song_ids")
            rows = _read_song_ids_rows(hidden_file_path)

            from collections import defaultdict
            groups = defaultdict(list)
            for r in rows:
                groups[_norm_key(r["artist"], r["song"])].append(r)

            # Build quick map from playlist
            pl_map = {_norm_key(t["artist"], t["title"]): t for t in playlist_tracks}

            now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            new_lines = []

            # First, rewrite all existing groups (update only those in restrict_keys)
            seen_keys = set()
            for k, grp in groups.items():
                newest = max(grp, key=lambda r: r["ts"])
                earliest = min(grp, key=lambda r: r["ts"])
                artist_disp = newest["artist"]
                song_disp = newest["song"]

                if k in restrict_keys and k in pl_map:
                    tid = pl_map[k]["id"]
                    ts_s = earliest["ts_s"]  # preserve earliest
                    # NEW: prefer the real on-disk name (canonical or variant)
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

            # Next, add rows for existing-on-disk tracks that had no rows before
            for k in restrict_keys:
                if k not in seen_keys and k in pl_map:
                    artist, title = pl_map[k]["artist"], pl_map[k]["title"]
                    tid = pl_map[k]["id"]
                    # NEW: prefer the real on-disk name (canonical or variant)
                    existing_name = _find_existing_filename_for_base(playlist_dir, artist, title)
                    filename = existing_name or f"{artist} - {title}.mp3"
                    new_lines.append(f"{tid}\t{now_s}\t{artist}\t{title}\t{filename}\n")

            # Deterministic write
            new_lines.sort(key=lambda line: (line.split("\t")[2].casefold(), line.split("\t")[3].casefold()))
            with open(hidden_file_path, "w", encoding="utf-8") as f:
                f.writelines(new_lines)


        def _prompt_yes_no(question: str, default_no: bool = True) -> bool:
            hint = "[y/N]" if default_no else "[Y/n]"
            try:
                CustomPrint.print(f"{question} {hint}")
            except Exception:
                print(f"{question} {hint}")
            ans = input("> ").strip().lower()
            if ans == "":  return (not default_no)
            if ans in ("y", "yes"): return True
            if ans in ("n", "no"):  return False
            return False


        def maybe_update_song_ids_from_playlist_existing_only(
                playlist_dir: str,
                playlist_tracks: list[dict[str, any]],
                restrict_keys: set
        ) -> None:
            """Auto-update .song_ids for tracks that already exist on disk.
            No user confirmation required.
            """

            diffs = diff_song_ids_vs_playlist_existing_only(playlist_dir, playlist_tracks, restrict_keys)
            if not diffs:
                CustomPrint.bold_print("No ID updates needed for existing files.", "GREEN")
                return

            CustomPrint.bold_print(">>> Auto-updating .song_ids for existing files:", "YELLOW")
            for d in diffs:
                if d["old_id"] is None:
                    CustomPrint.print(f"- {d['artist']} — {d['song']}: (new row) -> {d['new_id']}")
                else:
                    CustomPrint.print(f"- {d['artist']} — {d['song']}: {d['old_id']} -> {d['new_id']}")

            apply_song_ids_from_playlist_existing_only(playlist_dir, playlist_tracks, restrict_keys)
            CustomPrint.bold_print("Updated .song_ids for existing files.", "GREEN")


        # Write the playlist to an M3U8 file and gather metadata
        output_file = os.path.join(playlist_dir, f"{playlist_name}.m3u8")  # Ensure this path is defined
        track_metadata = write_playlist_to_m3u8(tracks, output_file)

        # Build exact playlist tracks (artist, title, id)
        playlist_tracks = extract_playlist_tracks(tracks)


        # Reconcile .song_ids with what's actually on disk (remove orphans)
        CustomPrint.bold_print(">>> Reconciling .song_ids with files on disk...", "YELLOW")
        stats = sid_cleanup_song_ids_orphans(playlist_dir)
        CustomPrint.bold_print(
            f">>> .song_ids cleaned: kept={stats['kept']}, dropped_orphans={stats['dropped_orphans']}",
            "GREEN"
        )

        # Auto-reconcile .song_ids so Zotify will skip already-downloaded tracks ---
        added, flipped, deduped = sid_reconcile_song_ids_flip_ids(playlist_dir, playlist_tracks)
        CustomPrint.bold_print(
            f">>> .song_ids auto-reconciled: added={added}, flipped={flipped}, deduped={deduped}", "GREEN"
        )

        # Restrict updates to tracks that ALREADY exist in the playlist folder
        restrict_keys = sid_build_existing_keys_set(playlist_dir)

        # Opt-in: update .song_ids to match playlist IDs
        sid_maybe_update_song_ids_from_playlist_existing_only(playlist_dir, playlist_tracks, restrict_keys)

        sid_rename_single_variant_to_canonical(playlist_dir, playlist_tracks)


        def prompt_rename_mismatched_files(playlist_dir: str, m3u8_file: str) -> None:
            """
            For each expected filename (from the .m3u8 file), check if there is a file on disk
            whose name is a variant (as per get_possible_filenames) but does not exactly match.
            Prompt the user to rename the mismatched file to the exact expected name.
            """
            # Read the expected filenames from the .m3u8 file
            try:
                with open(m3u8_file, 'r', encoding='utf-8') as f:
                    expected_names = {line.strip() for line in f if line.strip()}
            except FileNotFoundError:
                print(f"Debug: {m3u8_file} not found!")
                return

            # list all .mp3 files in the playlist directory
            actual_files = [f for f in os.listdir(playlist_dir) if f.lower().endswith('.mp3')]

            # Build a mapping from acceptable variants to the expected filename
            expected_variants = {}
            for expected in expected_names:
                variants = get_possible_filenames(expected)
                for variant in variants:
                    expected_variants[variant] = expected

            # Iterate over actual files on disk
            for actual in actual_files:
                # Skip if the file is already exactly as expected
                if actual in expected_names:
                    continue

                # If the actual filename is one of the acceptable variants, prompt for renaming
                if actual in expected_variants:
                    expected_name = expected_variants[actual]
                    response = input(f"Rename '{actual}' to '{expected_name}'? (y/n): ").strip().lower()
                    if response == 'y':
                        old_path = os.path.join(playlist_dir, actual)
                        new_path = os.path.join(playlist_dir, expected_name)
                        try:
                            os.rename(old_path, new_path)
                            print(f"Renamed '{actual}' to '{expected_name}'")
                        except Exception as e:
                            print(f"Error renaming '{actual}': {e}")


        def _playlist_bases_set(playlist_tracks):
            bases = set()
            for t in (playlist_tracks or []):
                a = (t.get("artist") or "").strip()
                s = (t.get("title") or "").strip()
                if a and s:
                    # sanitize so "/"→"_" etc matches disk
                    bases.add(sanitize_filename(f"{a} - {s}").casefold())
            return bases


        def _is_suffix_variant_of_playlist(file_name: str, playlist_bases: set) -> bool:
            m = BASE_RE.match(file_name)
            if not m:
                return False
            base = sanitize_filename(f"{m.group('artist').strip()} - {m.group('title').strip()}").casefold()
            return base in playlist_bases


        def cleanup_non_matching_files(playlist_dir: str, m3u8_file: str, playlist_tracks=None) -> None:
            try:
                with open(m3u8_file, 'r', encoding='utf-8') as f:
                    expected_names = {line.strip() for line in f if line.strip()}
            except FileNotFoundError:
                print(f"Debug: {m3u8_file} not found!")
                return

            # Build full set of acceptable variants for each expected filename
            acceptable = set()
            for expected in expected_names:
                acceptable.update(get_possible_filenames(expected))

            playlist_bases = _playlist_bases_set(playlist_tracks)
            actual_files = [f for f in os.listdir(playlist_dir) if f.lower().endswith('.mp3')]

            for actual in actual_files:
                if actual in acceptable:
                    continue
                if _is_suffix_variant_of_playlist(actual, playlist_bases):
                    print(f"Skipped deletion (suffix variant of playlist): {actual}")
                    continue
                file_path = os.path.join(playlist_dir, actual)
                try:
                    os.remove(file_path)
                    print(f"Deleted non-matching file: {actual}")
                except Exception as e:
                    print(f"Error deleting '{actual}': {e}")


        # Locate the .m3u8 file in the playlist directory.
        m3u8_files = [f for f in os.listdir(playlist_dir) if f.lower().endswith('.m3u8')]
        if m3u8_files:
            m3u8_file = os.path.join(playlist_dir, m3u8_files[0])
        else:
            print("No .m3u8 file found in the playlist directory!")
            m3u8_file = None  # Or handle the error as needed

        # If an .m3u8 file exists, first prompt for renaming mismatched files...
        if m3u8_file:
            prompt_rename_mismatched_files(playlist_dir, m3u8_file)
            # Then, in a second cleanup step, delete any .mp3 files that still don't exactly match.
            cleanup_non_matching_files(playlist_dir, m3u8_file, playlist_tracks)

        # Now, proceed to process track metadata and copy files as needed.
        for file_name, original_file_name, release_year in track_metadata:
            src_file = None
            for root, dirs, files in os.walk(base_music_path):
                if file_name in files:
                    src_file = os.path.join(root, file_name)
                    src_dir = root  # Store the source directory
                    break

            if src_file:
                dest_file = os.path.join(playlist_dir, file_name)
                if not os.path.exists(dest_file):
                    shutil.copy2(src_file, dest_file)

                    # Extract author_name and song_name from the original_file_name
                    parts = original_file_name.rsplit(".", 1)[0].split(" - ")
                    if len(parts) < 2:
                        CustomPrint.bold_print(f">>> WARNING: Unexpected file name format: {original_file_name}")
                        author_name = "Unknown"
                        song_name = original_file_name.rsplit(".", 1)[0]
                    else:
                        author_name = parts[0]
                        song_name = " - ".join(parts[1:])

                    # Create the destination .song_ids file if it doesn't exist
                    dest_song_ids_file = os.path.join(playlist_dir, ".song_ids")
                    if not os.path.exists(dest_song_ids_file):
                        open(dest_song_ids_file, "w", encoding="utf-8").close()

                    # Copy the relevant metadata line to the destination .song_ids file
                    copy_metadata_line(src_dir, author_name, song_name, dest_song_ids_file)

        # Check for songs in Local Files
        local_files_dir = os.path.join(home_dir, "Music", "Local Files")
        local_files_songs = set()
        for root, dirs, files in os.walk(local_files_dir):
            for file in files:
                if file.endswith(".mp3"):
                    local_files_songs.add(file)

        TIMEOUT_SECONDS = 30  # Timeout duration in seconds
        MAX_RETRIES = 3


        def read_output(process, last_output_time):
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    print(output.strip())
                    last_output_time[0] = time.time()  # Update the last output time


        # Get a list of current files in the playlist directory with .mp3 extension
        current_files = set(f for f in os.listdir(playlist_dir) if f.endswith('.mp3'))

        # Get a list of files from the .m3u8 file
        with open(output_file, 'r') as f:
            m3u8_files = set(line.strip() for line in f if not line.startswith("#") and line.strip().endswith('.mp3'))

        # Normalize the m3u8_files
        normalized_m3u8_files = set()
        for file in m3u8_files:
            normalized_m3u8_files.update(get_possible_filenames(file))
            normalized_m3u8_files.add(sanitize_filename(file))

        # Identify files to be removed, considering possible filenames
        files_to_remove = set()
        for file in current_files:
            # Generate possible valid names
            possible_names = get_possible_filenames(file)

            # If no possible name matches the playlist, mark it for removal
            if not any(name in normalized_m3u8_files for name in possible_names):
                files_to_remove.add(file)

        # Exclude system-related hidden files
        files_to_remove.discard('._1.mp3')

        # PHASE 1: Rename files that can be "fixed" by sanitizing.
        if files_to_remove:
            CustomPrint.bold_print("\n>>> Renaming files that don't match but can be fixed by sanitization:")
            for file in files_to_remove:
                # Process only .mp3 files.
                if not file.lower().endswith(".mp3"):
                    continue
                file_path = os.path.join(playlist_dir, file)
                if os.path.exists(file_path) and os.path.isfile(file_path):
                    expected_filename = sanitize_filename(file)
                    # If the current filename is not already the expected one...
                    if file != expected_filename:
                        new_path = os.path.join(playlist_dir, expected_filename)
                        # If no file exists with the sanitized name, rename.
                        if not os.path.exists(new_path):
                            try:
                                os.rename(file_path, new_path)
                                CustomPrint.bold_print(f">>> Renamed '{file}' -> '{expected_filename}'",
                                                       "YELLOW")
                            except Exception as e:
                                CustomPrint.bold_print(f">>> Error renaming '{file}': {e}", "RED")
                        else:
                            # If a file with the sanitized name already exists, remove this duplicate.
                            try:
                                os.remove(file_path)
                                CustomPrint.bold_print(f">>> Duplicate found. Removed '{file}'", "RED")
                            except Exception as e:
                                CustomPrint.bold_print(f">>> Error removing duplicate '{file}': {e}", "RED")
                    else:
                        CustomPrint.bold_print(f">>> File '{file}' is already correct.", "YELLOW")
                else:
                    CustomPrint.bold_print(f">>> File not found: {file_path}")

            # PHASE 2: Cleanup: Remove any .mp3 file that does not exactly match an expected filename.
            CustomPrint.bold_print("\n>>> Performing final cleanup of .mp3 files not matching the .m3u8:")
            CustomPrint.bold_print(">>> Promoting suffix variants to canonical names (if needed):", "YELLOW")


            # Build the normalized expected-name set already used later
            normalized_m3u8_files = set()
            for file in m3u8_files:
                normalized_m3u8_files.update(get_possible_filenames(file))
                normalized_m3u8_files.add(sanitize_filename(file))

            # Promote e.g. 'Radiohead - Weird Fishes _ Arpeggi_1.mp3' -> 'Radiohead - Weird Fishes _ Arpeggi.mp3'
            for fname in sorted(os.listdir(playlist_dir)):
                if not fname.lower().endswith(".mp3"):
                    continue
                m = SUFFIX_RE.match(fname)
                if not m:
                    continue

                base = m.group("base")  # already sanitized-like text from disk
                canonical = f"{sanitize_filename(base)}.mp3"
                src = os.path.join(playlist_dir, fname)
                dst = os.path.join(playlist_dir, canonical)

                # Only rename if:
                # 1) canonical name is an expected playlist filename; and
                # 2) canonical file does not already exist.
                if canonical in normalized_m3u8_files and not os.path.exists(dst):
                    try:
                        os.rename(src, dst)
                        CustomPrint.bold_print(f">>> Renamed '{fname}' -> '{canonical}'", "GREEN")
                    except Exception as e:
                        CustomPrint.bold_print(f">>> Rename failed for '{fname}' -> '{canonical}': {e}", "RED")
                else:
                    # Keep as-is if not expected or canonical already exists
                    pass

            current_files = os.listdir(playlist_dir)
            playlist_bases = _playlist_bases_set(playlist_tracks)
            for file in current_files:
                # Process only .mp3 files.
                if not file.lower().endswith(".mp3"):
                    continue
                file_path = os.path.join(playlist_dir, file)
                if os.path.exists(file_path) and os.path.isfile(file_path):
                    if file not in normalized_m3u8_files:

                        if _is_suffix_variant_of_playlist(file, playlist_bases):  # <— ADD THIS BLOCK
                            CustomPrint.bold_print(f">>> Skipped deletion (suffix variant of playlist): {file}")
                            continue
                        try:
                            os.remove(file_path)
                            CustomPrint.bold_print(f">>> Removed file: {file}", "RED")
                        except Exception as e:
                            CustomPrint.bold_print(f">>> Error removing '{file}': {e}", "RED")
            CustomPrint.bold_print("*** Cleanup completed.", "GREEN")
            stats2 = cleanup_song_ids_orphans(playlist_dir)
            CustomPrint.bold_print(
                f">>> .song_ids cleaned: kept={stats2['kept']}, dropped_orphans={stats2['dropped_orphans']}",
                "GREEN"
            )

        else:
            CustomPrint.bold_print(">>> No files to remove.", "GREEN")

        # Define a fixed width for the alignment
        fixed_width = 40
        # Print the number of songs in the playlist

        CustomPrint.bold_print(f"\n\n>>> {playlist_name.ljust(fixed_width)} [{len(track_metadata)} Songs]")

        # Count for skipped songs
        skipped_songs_count = 0
        skipped_songs = []

        # Check for skipped songs (songs already in playlist directory)
        for file_name, original_file_name, _ in track_metadata:
            possible_filenames = get_possible_filenames(original_file_name)
            if any(fname in os.listdir(playlist_dir) for fname in possible_filenames):
                skipped_songs_count += 1
                skipped_songs.append(original_file_name)

        # Print the number of downloaded songs
        if skipped_songs_count > 0:
            CustomPrint.bold_print(f">>> {'Downloaded Songs'.ljust(fixed_width)} [{skipped_songs_count} Songs]")

        # Check for songs in Local Files (that are part of the current playlist)
        songs_in_local_files = set()
        for root, dirs, files in os.walk(local_files_dir):
            for file in files:
                if file.endswith(".mp3"):
                    for possible_file in get_possible_filenames(file):
                        if possible_file in [f for f, _, _ in
                                             track_metadata]:  # Only add files that are part of the current playlist
                            songs_in_local_files.add(file)

        # Print the number of songs from the playlist found in Local Files
        if len(songs_in_local_files) > 0:
            CustomPrint.bold_print(
                f">>> {'Songs found in Local Files'.ljust(fixed_width)} [{(len(songs_in_local_files))} Songs]")

        # Check for missing songs
        missing_songs = set()
        for file_name, original_file_name, _ in track_metadata:
            possible_filenames = get_possible_filenames(original_file_name)
            if not any(fname in os.listdir(playlist_dir) for fname in possible_filenames) and \
                    not any(fname in songs_in_local_files for fname in possible_filenames):
                missing_songs.add(original_file_name)

        # Print the missing songs

        if len(missing_songs) > 0:
            CustomPrint.bold_print(f">>> Missing songs: [{len(missing_songs)}]")

            for song in missing_songs:
                print(f"\t{song}")
        else:
            CustomPrint.bold_print(f"*** Playlist is up-to-date.", "GREEN")

        print()

        # Update MP3 tags with release year if the year field is empty
        tags_updated = False  # Flag to track if any MP3 tags were updated

        for file_name, original_file_name, release_year in track_metadata:
            found_file = False
            for possible_file_name in get_possible_filenames(original_file_name):
                file_path = os.path.join(playlist_dir, possible_file_name)
                if os.path.exists(file_path):
                    try:
                        audio = EasyID3(file_path)
                    except ID3NoHeaderError:
                        audio = EasyID3()
                    if not audio.get('date'):  # Check if the year field is empty
                        if release_year != 'Unknown':
                            audio['date'] = release_year
                            try:
                                audio.save()
                                CustomPrint.bold_print(f">>> MP3 tag updated for {possible_file_name}")
                                tags_updated = True  # Set flag to True if a tag was updated
                            except TypeError as e:
                                CustomPrint.bold_print(
                                    f"!!! ERROR: Problem updating MP3 tag for {possible_file_name}: {e}", "RED")
                                with open(os.path.join(playlist_dir, 'tag_update_errors.log'), 'a') as log_file:
                                    CustomPrint.bold_print(
                                        f"!!! ERROR: Problem updating MP3 tag for {possible_file_name}: {e}\n", "RED")
                    found_file = True
                    break
            if not found_file and original_file_name not in local_files_songs and original_file_name not in missing_songs:
                CustomPrint.bold_print(f">>> Unable to find or match {original_file_name} to update MP3 tag")

        # Print final message only if tags were updated
        if tags_updated:
            CustomPrint.bold_print("*** MP3 tag updating process completed. \n", "GREEN")



        def analyze_song_ids_file(playlist_dir: str) -> None:
            """Analyze and clean up the .song_ids file."""
            hidden_file_path = os.path.join(playlist_dir, '.song_ids')

            if not os.path.isfile(hidden_file_path):
                open(hidden_file_path, 'w', encoding='utf-8').close()
                return

            possible_duplicates = defaultdict(list)
            with open(hidden_file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    track_id, timestamp, artist, song_name, file_name = line.strip().split('\t')
                    key = f"{artist} {song_name}"
                    possible_duplicates[key].append(line)

            cleaned_lines = [lines[1:] if len(lines) > 1 else lines for lines in possible_duplicates.values()]
            cleaned_lines = [line for sublist in cleaned_lines for line in sublist]  # Flatten the list

            with open(hidden_file_path, 'w', encoding='utf-8') as file:
                file.writelines(cleaned_lines)

            # Define the debug function at a higher scope (e.g., before the if/else)

    except KeyboardInterrupt:
        CustomPrint.bold_print("\nOperation interrupted. Exiting...\n")
        sys.exit(1)