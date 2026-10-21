import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from mutagen.id3 import ID3, TIT2, USLT

from playlist_extractor import (
    clean_embedded_lyrics,
    clean_playlist_lyrics,
    format_lyrics_cleanup_summary,
    strip_lyric_timestamps,
)


class LyricsCleanupTests(unittest.TestCase):
    def test_summary_reports_total_tracks_with_lyrics(self):
        summary = format_lyrics_cleanup_summary(
            {
                "cleaned": 1,
                "already_plain": 75,
                "missing": 11,
                "unmatched": 1,
                "unsupported_version": 0,
                "errors": 0,
            }
        )

        self.assertEqual(
            summary,
            "*** Lyrics check: lyrics found on 76 tracks; 1 timestamp removed; "
            "lyrics missing on 11 tracks; 1 track unmatched; 0 unsupported ID3 versions; 0 errors.",
        )

    def test_summary_counts_lyrics_on_unsupported_id3_versions(self):
        summary = format_lyrics_cleanup_summary(
            {
                "cleaned": 1,
                "already_plain": 75,
                "missing": 11,
                "unmatched": 1,
                "unsupported_version": 2,
                "errors": 0,
            }
        )

        self.assertIn("lyrics found on 78 tracks", summary)
        self.assertIn("2 unsupported ID3 versions", summary)

    def test_scan_does_not_print_per_track_missing_lyrics(self):
        with TemporaryDirectory() as temp_dir:
            filename = "Artist - No Lyrics.mp3"
            (Path(temp_dir) / filename).touch()
            output = StringIO()

            with redirect_stdout(output):
                stats = clean_playlist_lyrics(
                    temp_dir, [("No Lyrics", filename, "2000")]
                )

        self.assertEqual(output.getvalue(), "")
        self.assertEqual(stats["missing"], 1)

    def test_playlist_pass_cleans_embedded_lyrics_and_reports_missing_tags(self):
        with TemporaryDirectory() as temp_dir:
            synced_file = Path(temp_dir) / "Artist - Synced.mp3"
            synced_tags = ID3()
            synced_tags.add(
                USLT(
                    encoding=3,
                    lang="eng",
                    desc="",
                    text="[00:01.00]Synced line",
                )
            )
            synced_tags.save(synced_file)

            no_lyrics_file = Path(temp_dir) / "Artist - No Lyrics.mp3"
            no_lyrics_tags = ID3()
            no_lyrics_tags.add(TIT2(encoding=3, text="No Lyrics"))
            no_lyrics_tags.save(no_lyrics_file)

            sidecar = Path(temp_dir) / "Artist - Synced.lrc"
            sidecar.write_text("[00:01.00]Synced line\n", encoding="utf-8")

            with patch("playlist_extractor.CustomPrint.bold_print") as output:
                stats = clean_playlist_lyrics(
                    str(temp_dir),
                    [
                        ("Synced", "Artist - Synced.mp3", "2024"),
                        ("No Lyrics", "Artist - No Lyrics.mp3", "2024"),
                        ("Absent", "Artist - Absent.mp3", "2024"),
                    ],
                )
                output.assert_not_called()

            self.assertEqual(
                stats,
                {"cleaned": 1, "already_plain": 0, "missing": 1, "unmatched": 1, "unsupported_version": 0, "errors": 0},
            )
            self.assertEqual(ID3(synced_file).getall("USLT")[0].text, "Synced line")
            self.assertEqual(sidecar.read_text(encoding="utf-8"), "[00:01.00]Synced line\n")

    def test_playlist_cleanup_matches_accented_filename_variants(self):
        with TemporaryDirectory() as temp_dir:
            actual_name = "Beyoncé - Upgrade U (feat. JAŸ-Z).mp3"
            tags = ID3()
            tags.add(USLT(encoding=3, lang="eng", desc="", text="Plain lyrics"))
            tags.save(Path(temp_dir) / actual_name)

            stats = clean_playlist_lyrics(
                temp_dir,
                [("Upgrade U", "Beyoncé - Upgrade U (feat. JAY-Z).mp3", "2006")],
            )

        self.assertEqual(stats["already_plain"], 1)
        self.assertEqual(stats["unmatched"], 0)

    def test_playlist_cleanup_does_not_follow_paths_outside_playlist_directory(self):
        with TemporaryDirectory() as temp_dir:
            playlist_dir = Path(temp_dir) / "playlist"
            playlist_dir.mkdir()
            outside_file = Path(temp_dir) / "outside.mp3"
            outside_tags = ID3()
            outside_tags.add(
                USLT(
                    encoding=3,
                    lang="eng",
                    desc="",
                    text="[00:01.00]Outside lyrics",
                )
            )
            outside_tags.save(outside_file)
            (playlist_dir / "alias.mp3").symlink_to(outside_file)

            stats = clean_playlist_lyrics(
                str(playlist_dir),
                [
                    ("Outside", "../outside.mp3", "2024"),
                    ("Alias", "alias.mp3", "2024"),
                ],
            )

            self.assertEqual(stats["unmatched"], 2)
            self.assertEqual(
                ID3(outside_file).getall("USLT")[0].text,
                "[00:01.00]Outside lyrics",
            )

    def test_cleanup_removes_timestamps_from_embedded_lyrics(self):
        with TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "track.mp3"
            tags = ID3()
            tags.add(TIT2(encoding=3, text="Test Track"))
            tags.add(
                USLT(
                    encoding=3,
                    lang="eng",
                    desc="",
                    text="[00:00.00]First line\n[00:04.25]Second line",
                )
            )
            tags.save(file_path, v2_version=3)

            status = clean_embedded_lyrics(str(file_path))

            self.assertEqual(status, "cleaned")
            saved_tags = ID3(file_path)
            self.assertEqual(saved_tags.version, (2, 3, 0))
            self.assertEqual(saved_tags.get("TIT2").text, ["Test Track"])
            self.assertEqual(
                saved_tags.getall("USLT")[0].text,
                "First line\nSecond line",
            )

    def test_cleanup_preserves_existing_id3v1_tag(self):
        with TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "track.mp3"
            tags = ID3()
            tags.add(
                USLT(
                    encoding=3,
                    lang="eng",
                    desc="",
                    text="[00:00.00]Lyrics",
                )
            )
            tags.save(file_path, v2_version=3)
            id3v1_tag = b"TAG" + b"Legacy title".ljust(30, b"\x00") + bytes(95)
            with file_path.open("ab") as audio_file:
                audio_file.write(id3v1_tag)

            status = clean_embedded_lyrics(str(file_path))

            self.assertEqual(status, "cleaned")
            self.assertTrue(file_path.read_bytes().endswith(id3v1_tag))

    def test_cleanup_reports_id3v22_without_rewriting_it(self):
        with TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "track.mp3"
            payload = bytes([0]) + b"eng" + bytes([0]) + b"[00:01.00]Lyrics"
            frame = b"ULT" + len(payload).to_bytes(3, "big") + payload
            tag_size = len(frame)
            syncsafe_size = bytes(
                (
                    (tag_size >> 21) & 0x7F,
                    (tag_size >> 14) & 0x7F,
                    (tag_size >> 7) & 0x7F,
                    tag_size & 0x7F,
                )
            )
            original_bytes = b"ID3" + bytes([2, 0, 0]) + syncsafe_size + frame + bytes([255, 251])
            file_path.write_bytes(original_bytes)

            status = clean_embedded_lyrics(str(file_path))

            self.assertEqual(status, "unsupported_version")
            stats = clean_playlist_lyrics(
                str(temp_dir), [("Track", "track.mp3", "2024")]
            )
            self.assertEqual(
                stats,
                {
                    "cleaned": 0,
                    "already_plain": 0,
                    "missing": 0,
                    "unmatched": 0,
                    "unsupported_version": 1,
                    "errors": 0,
                },
            )
            self.assertEqual(file_path.read_bytes(), original_bytes)

    def test_strips_lrc_timestamps_at_each_line_start(self):
        lyrics = (
            "[00:00.00]Intro\n"
            "[1:02.3]First lyric\n"
            "[02:03.45][02:05.00]Repeated timestamp [keep this]"
        )

        self.assertEqual(
            strip_lyric_timestamps(lyrics),
            "Intro\nFirst lyric\nRepeated timestamp [keep this]",
        )


if __name__ == "__main__":
    unittest.main()
