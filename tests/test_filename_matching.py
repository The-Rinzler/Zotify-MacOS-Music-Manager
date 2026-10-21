import tempfile
import unittest
import unicodedata
from pathlib import Path

from mutagen.id3 import ID3, TXXX

from song_ids_manager import cleanup_song_ids_orphans, reconcile_song_ids_flip_ids
from utils import find_matching_filename


class FilenameMatchingTests(unittest.TestCase):
    def test_matches_the_accent_and_case_variants(self):
        expected = "Beyoncé - Upgrade U (feat. JAY-Z).mp3"
        actual_variants = (
            "Beyoncé - Upgrade U (feat. JAŸ-Z).mp3",
            "Beyonce - Upgrade U (feat. JAŸ-Z).mp3",
            "Beyonce - Upgrade U (feat. JAY-Z).mp3",
            "beyoncé - upgrade u (feat. jaÿ-z).mp3",
        )

        for actual in actual_variants:
            with self.subTest(actual=actual):
                self.assertEqual(find_matching_filename(expected, {actual}), actual)

    def test_matches_composed_and_decomposed_unicode(self):
        expected = "Beyoncé - Upgrade U (feat. JAY-Z).mp3"
        actual = unicodedata.normalize("NFD", expected)

        self.assertEqual(find_matching_filename(expected, {actual}), actual)

    def test_does_not_guess_when_multiple_normalized_matches_exist(self):
        expected = "BEYONCE - Upgrade U (feat. JAY-Z).mp3"
        actual = {
            "Beyoncé - Upgrade U (feat. JAY-Z).mp3",
            "Beyonce - Upgrade U (feat. JAY-Z).mp3",
        }

        self.assertIsNone(find_matching_filename(expected, actual))

    def test_song_id_orphan_cleanup_preserves_accent_variant_file(self):
        actual = "Beyoncé - Upgrade U (feat. JAŸ-Z).mp3"
        archive_row = "\t".join([
            "spotify-track-id",
            "2025-01-01 00:00:00",
            "Beyoncé",
            "Upgrade U (feat. JAY-Z)",
            "Beyoncé - Upgrade U (feat. JAY-Z).mp3",
        ]) + "\n"

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, actual).touch()
            Path(temp_dir, ".song_ids").write_text(archive_row, encoding="utf-8")

            stats = cleanup_song_ids_orphans(temp_dir)
            rows = Path(temp_dir, ".song_ids").read_text(encoding="utf-8").splitlines()

        self.assertEqual(stats, {"kept": 1, "dropped_orphans": 0})
        self.assertEqual(rows[0].split("\t")[4], actual)

    def test_song_id_orphan_cleanup_recovers_suffix_path_by_embedded_track_id(self):
        track_id = "spotify-track-id"
        variants = ["Artist - Song_1.mp3", "Artist - Song_2.mp3"]
        archive_row = "\t".join([
            track_id,
            "2025-01-01 00:00:00",
            "Artist",
            "Song",
            "Artist - Song.mp3",
        ]) + "\n"

        with tempfile.TemporaryDirectory() as temp_dir:
            for variant in variants:
                audio_path = Path(temp_dir, variant)
                audio_path.write_bytes(b"test audio")
                tags = ID3()
                tags.add(TXXX(encoding=3, desc="TRACKID", text=[track_id]))
                tags.save(audio_path)
            Path(temp_dir, ".song_ids").write_text(archive_row, encoding="utf-8")

            stats = cleanup_song_ids_orphans(temp_dir)
            rows = Path(temp_dir, ".song_ids").read_text(encoding="utf-8").splitlines()

        self.assertEqual(stats, {"kept": 1, "dropped_orphans": 0})
        self.assertEqual(rows[0].split("\t")[0], track_id)
        self.assertEqual(rows[0].split("\t")[4], sorted(variants)[0])

    def test_song_id_reconciliation_records_unique_existing_variant(self):
        track = {
            "id": "spotify-track-id",
            "artist": "Beyoncé",
            "title": "Upgrade U (feat. JAY-Z)",
        }
        actual = "Beyoncé - Upgrade U (feat. JAŸ-Z).mp3"

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, actual).touch()

            stats = reconcile_song_ids_flip_ids(temp_dir, [track])
            rows = Path(temp_dir, ".song_ids").read_text(encoding="utf-8").splitlines()

        self.assertEqual(stats, (1, 0, 0))
        self.assertEqual(len(rows), 1)
        fields = rows[0].split("\t")
        self.assertEqual(fields[0], "spotify-track-id")
        self.assertEqual(fields[2:4], ["Beyoncé", "Upgrade U (feat. JAY-Z)"])
        self.assertEqual(fields[4], actual)


if __name__ == "__main__":
    unittest.main()
