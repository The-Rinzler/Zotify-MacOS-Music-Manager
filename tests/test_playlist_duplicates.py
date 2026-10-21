"""Offline playlist diagnostics; never remove entries or inspect real audio."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import playlist_duplicates
import playlist_extractor


CLI_PROBE = r'''
import json
import runpy
import socket
import sys
from unittest.mock import patch

items = json.load(sys.stdin)
class FakeSpotify:
    def playlist(self, playlist_id):
        return {"name": "Synthetic duplicate test"}
    def playlist_items(self, playlist_id, limit=50, offset=0):
        return {"items": items[:50], "next": "fixture-next" if len(items) > 50 else None}
    def next(self, response):
        return {"items": items[50:], "next": None}

def forbidden(*args, **kwargs):
    raise AssertionError("Duplicate report must not start maintenance or network I/O")

source = sys.argv[1]
mode = sys.argv[2]
sys.argv = [source, "https://open.spotify.com/playlist/synthetic"]
if mode == "report":
    sys.argv.append("--duplicates-only")

def library_boundary(*args, **kwargs):
    if mode == "normal":
        print("MAINTENANCE_BOUNDARY")
        raise SystemExit(0)
    forbidden()

with patch("spotify_api.get_spotify_client", return_value=FakeSpotify()), \
     patch("utils.create_hidden_playlist_link_file", side_effect=library_boundary), \
     patch("utils.write_playlist_to_m3u8", side_effect=forbidden), \
     patch("song_ids_manager.cleanup_song_ids_orphans", side_effect=forbidden), \
     patch("socket.socket.connect", side_effect=forbidden), \
     patch("builtins.input", side_effect=forbidden):
    runpy.run_path(source, run_name="__main__")
'''


def entry(track_id, title='Doe Deer - 12" Version', *, shape="item", **extra):
    track = {
        "id": track_id,
        "type": "track",
        "name": title,
        "artists": [{"name": "Crystal Castles"}],
        "album": {"name": "Synthetic test album"},
        "duration_ms": 97000,
    }
    track.update(extra)
    return {shape: track}


class PlaylistDuplicateTests(unittest.TestCase):
    def analyze(self, items) -> dict:
        analyze = getattr(playlist_extractor, "analyze_playlist_duplicates", None)
        if not callable(analyze):
            self.fail("Playlist duplicate detection is missing")
        return analyze(items)

    def test_warning_lists_each_repeated_occurrence_in_requested_song_id_format(self):
        report = self.analyze([entry("one", "Doe Deer"), entry("two", "Baptism"),
                               entry("one", "Doe Deer")])
        self.assertEqual(
            playlist_duplicates.format_playlist_duplicate_report(report),
            ">>> Duplicate songs in playlist:\n"
            "Song: Crystal Castles - Doe Deer (one)\n"
            "Song: Crystal Castles - Doe Deer (one)",
        )

    def test_possible_matches_are_separate_and_not_repeated_for_overlapping_groups(self):
        report = self.analyze([entry("album-id", external_ids={"isrc": "GBAA11000001"}),
                               entry("single-id", external_ids={"isrc": "GBAA11000001"})])
        output = playlist_duplicates.format_playlist_duplicate_report(report)
        self.assertEqual(
            output,
            ">>> Possible duplicate songs in playlist (review only):\n"
            'Song: Crystal Castles - Doe Deer - 12" Version (album-id)\n'
            'Song: Crystal Castles - Doe Deer - 12" Version (single-id)',
        )

    def test_local_unavailable_and_malformed_metadata_do_not_break_detection(self):
        items = [None, {}, {"item": None}, {"track": {}},
                 entry(None, uri="spotify:local:Artist:Album:Song:251", is_local=True),
                 entry("not-a-spotify-id", uri="local:Artist:Album:Song:251"),
                 entry("episode", type="episode"),
                 entry("valid", artists=[None, {"name": "Crystal Castles"}],
                       album="invalid", external_ids=["invalid"]),
                 entry("valid", shape="track", is_playable=False)]
        report = self.analyze(items)
        self.assertEqual(report["total_entries"], 9)
        self.assertEqual(report["unique_track_ids"], 1)
        self.assertEqual(report["repeated_entries"], 1)
        self.assertEqual([e["position"] for e in report["exact_duplicates"][0]["entries"]], [8, 9])
        self.assertEqual(report["recording_candidates"], [])
        self.assertEqual(report["filename_collisions"], [])

    def test_distinct_versions_and_missing_isrc_are_not_treated_as_same_recording(self):
        report = self.analyze([entry("studio", "Doe Deer"),
                               entry("live", "Doe Deer - Live"),
                               entry("extended", 'Doe Deer - 12" Version'),
                               entry("remix", "Doe Deer - Remix")])
        self.assertEqual(report["repeated_entries"], 0)
        self.assertEqual(report["recording_candidates"], [])
        self.assertEqual(report["filename_collisions"], [])
        self.assertIn("No duplicate songs detected", playlist_duplicates.format_playlist_duplicate_report(report))

    def test_repeats_alone_are_not_also_reported_as_catalog_or_filename_conflicts(self):
        report = self.analyze([entry("one", external_ids={"isrc": "GBAA11000001"})] * 3)
        self.assertEqual(report["repeated_entries"], 2)
        self.assertEqual(report["recording_candidates"], [])
        self.assertEqual(report["filename_collisions"], [])

    def test_shared_isrc_across_catalog_ids_is_review_only(self):
        items = [entry("album-id", external_ids={"isrc": "gbaa11000001"}),
                 entry("single-id", external_ids={"isrc": "GBAA11000001"},
                       album={"name": "Synthetic single"}),
                 entry("single-id", external_ids={"isrc": "GBAA11000001"})]
        report = self.analyze(items)
        self.assertEqual(report["repeated_entries"], 1)
        self.assertEqual(report["unique_track_ids"], 2)
        self.assertEqual(len(report["recording_candidates"]), 1)
        group = report["recording_candidates"][0]
        self.assertEqual(group["isrc"], "GBAA11000001")
        self.assertEqual([item["position"] for item in group["entries"]], [1, 2, 3])
        self.assertEqual(group["entries"][1]["album"], "Synthetic single")
        self.assertEqual(group["entries"][0]["title"], 'Doe Deer - 12" Version')

    def test_same_output_filename_is_a_collision_not_proof_of_duplicate_audio(self):
        items = [entry("one"), entry("two"), entry("three", "Other?"),
                 entry("four", "Other!")]
        report = self.analyze(items)
        self.assertEqual(report["repeated_entries"], 0)
        self.assertEqual(report["recording_candidates"], [])
        groups = report["filename_collisions"]
        self.assertEqual(len(groups), 2)
        self.assertEqual(groups[0]["filename"], 'Crystal Castles - Doe Deer - 12" Version.mp3')
        self.assertEqual([item["position"] for item in groups[0]["entries"]], [1, 2])
        self.assertEqual(groups[1]["filename"], "Crystal Castles - Other_.mp3")
        self.assertEqual([item["track_id"] for item in groups[1]["entries"]], ["three", "four"])

    def run_probe(self, items, *, mode="report", existing_library=True):
        source = Path(playlist_extractor.__file__).resolve()
        with tempfile.TemporaryDirectory() as home:
            root = Path(home)
            library = root / "Music" / "Zotify Music" / "Synthetic duplicate test"
            if existing_library:
                library.mkdir(parents=True)
                for name in (".song_ids", ".DS_Store", "existing.mp3", "playlist.m3u8"):
                    (library / name).write_bytes(b"Do not alter this fixture")
            before = {str(path.relative_to(root)): path.read_bytes() if path.is_file() else None
                      for path in root.rglob("*")}
            env = {key: value for key, value in os.environ.items()
                   if key not in ("PYTHONHOME", "PYTHONPATH")}
            env["HOME"] = home
            result = subprocess.run(
                [sys.executable, "-B", "-c", CLI_PROBE, str(source), mode],
                input=json.dumps(items), text=True, capture_output=True,
                env=env, cwd=source.parent, timeout=20,
            )
            after = {str(path.relative_to(root)): path.read_bytes() if path.is_file() else None
                     for path in root.rglob("*")}
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(before, after)
        return result.stdout

    def test_report_only_cli_follows_pages_and_preserves_entire_library(self):
        items = [entry("repeated-id")] + [{"item": None}] * 49 + [entry("repeated-id")]
        output = self.run_probe(items)
        self.assertIn("Duplicate songs in playlist:", output)
        self.assertEqual(output.count('Song: Crystal Castles - Doe Deer - 12" Version (repeated-id)'), 2)
        self.assertIn("Library unchanged", output)
        self.assertNotIn("Reconciling .song_ids", output)

    def test_report_only_does_not_create_missing_library_directories(self):
        output = self.run_probe([], existing_library=False)
        self.assertIn("No duplicate songs detected", output)
        self.assertIn("Library unchanged", output)

    def test_normal_extraction_reports_conflicts_before_maintenance(self):
        output = self.run_probe([entry("one"), entry("two")], mode="normal")
        self.assertIn("Possible duplicate songs in playlist", output)
        self.assertLess(output.index("Possible duplicate songs in playlist"), output.index("MAINTENANCE_BOUNDARY"))
        self.assertNotIn("Library unchanged", output)

    def test_report_escapes_terminal_controls_and_keeps_song_ids_visible(self):
        report = self.analyze([entry("one", "Title\x1b[31m\nFake line"),
                               entry("one", "Title\x1b[31m\nFake line")])
        output = playlist_duplicates.format_playlist_duplicate_report(report)
        self.assertNotIn("\x1b", output)
        self.assertNotIn("\nFake line", output)
        self.assertIn("Song: Crystal Castles - Title", output)
        self.assertEqual(output.count("(one)"), 2)

    def test_case_and_unicode_filename_collisions_preserve_original_metadata(self):
        items = [entry("one", "Caf\u00e9"), entry("two", "CAFE\u0301")]
        original = copy.deepcopy(items)
        report = self.analyze(items)
        self.assertEqual(len(report["filename_collisions"]), 1)
        self.assertEqual(report["recording_candidates"], [])
        self.assertEqual(items, original)

    def test_reports_repeated_ids_with_original_positions_without_mutating_input(self):
        items = [entry("same-id"), {"item": None}, entry("other-id", "Baptism"),
                 entry("same-id", shape="track")]
        original = copy.deepcopy(items)
        report = self.analyze(items)
        self.assertEqual(report["total_entries"], 4)
        self.assertEqual(report["unique_track_ids"], 2)
        self.assertEqual(report["repeated_entries"], 1)
        self.assertEqual(len(report["exact_duplicates"]), 1)
        group = report["exact_duplicates"][0]
        self.assertEqual(group["track_id"], "same-id")
        self.assertEqual([item["position"] for item in group["entries"]], [1, 4])
        self.assertEqual(items, original)


if __name__ == "__main__":
    unittest.main()
