"""Read-only diagnostics for Spotify membership, not audio-file deduplication."""
from collections import defaultdict
import unicodedata

from utils import sanitize_filename


def analyze_playlist_duplicates(items: list[dict]) -> dict:
    """Separate exact repeated IDs from shared-ISRC review candidates.

    Positions are one-based offsets in the complete playlist, including removed
    and local entries. Shared ISRCs are evidence to review, never merge rules.
    """
    by_id = defaultdict(list)
    by_isrc = defaultdict(list)
    by_filename = defaultdict(list)
    for position, item in enumerate(items, 1):
        if not isinstance(item, dict):
            continue
        track = item.get("item") or item.get("track")
        if not isinstance(track, dict) or track.get("type", "track") != "track":
            continue
        uri = track.get("uri") or ""
        if item.get("is_local") or track.get("is_local") or str(uri).startswith(("spotify:local:", "local:")):
            continue
        track_id = track.get("id")
        if not isinstance(track_id, str) or not track_id.strip():
            continue
        album = track.get("album")
        album = album if isinstance(album, dict) else {}
        artists = track.get("artists")
        artists = artists if isinstance(artists, list) else []
        artists = [artist for artist in artists if isinstance(artist, dict)
                   and isinstance(artist.get("name"), str)]
        external_ids = track.get("external_ids")
        external_ids = external_ids if isinstance(external_ids, dict) else {}
        isrc = external_ids.get("isrc")
        isrc = isrc.strip().upper() if isinstance(isrc, str) else ""
        entry = {
            "position": position,
            "track_id": track_id,
            "artist": ", ".join(artist.get("name") or "" for artist in artists),
            "title": track.get("name") or "Unknown title",
            "album": album.get("name") or "Unknown album",
            "duration_ms": track.get("duration_ms"),
        }
        by_id[track_id].append(entry)
        if artists and artists[0].get("name") and track.get("name"):
            filename = sanitize_filename(f'{artists[0]["name"]} - {track["name"]}') + ".mp3"
            # Case-only spelling differences collide on usual macOS volumes.
            key = unicodedata.normalize("NFC", filename).casefold()
            by_filename[key].append({**entry, "filename": filename})
        if isrc:
            by_isrc[isrc].append(entry)

    exact = [{"track_id": track_id, "entries": entries}
             for track_id, entries in by_id.items() if len(entries) > 1]
    candidates = [{"isrc": isrc, "entries": entries}
                  for isrc, entries in by_isrc.items()
                  if len({entry["track_id"] for entry in entries}) > 1]
    return {
        "total_entries": len(items),
        "unique_track_ids": len(by_id),
        "repeated_entries": sum(len(group["entries"]) - 1 for group in exact),
        "exact_duplicates": exact,
        "recording_candidates": candidates,
        "filename_collisions": [
            {"filename": entries[0]["filename"], "entries": entries}
            for entries in by_filename.values()
            if len({entry["track_id"] for entry in entries}) > 1
        ],
    }


def _display(value) -> str:
    """Keep Spotify metadata on one terminal line without control sequences."""
    return "".join(" " if unicodedata.category(char).startswith("C") else char
                   for char in str(value))


def format_playlist_duplicate_report(report: dict) -> str:
    """Warn concisely without changing playlist membership or local files."""
    exact_entries = [entry for group in report["exact_duplicates"]
                     for entry in group["entries"]]
    candidates = {}
    for group in report["recording_candidates"] + report["filename_collisions"]:
        for entry in group["entries"]:
            candidates.setdefault(entry["track_id"], entry)

    lines = []
    sections = (
        (">>> Duplicate songs in playlist:", exact_entries),
        (">>> Possible duplicate songs in playlist (review only):",
         sorted(candidates.values(), key=lambda entry: entry["position"])),
    )
    for heading, entries in sections:
        if not entries:
            continue
        lines.append(heading)
        lines.extend(
            f"Song: {_display(entry['artist'])} - {_display(entry['title'])} "
            f"({_display(entry['track_id'])})"
            for entry in entries
        )
    return "\n".join(lines) if lines else ">>> No duplicate songs detected in available Spotify metadata."
