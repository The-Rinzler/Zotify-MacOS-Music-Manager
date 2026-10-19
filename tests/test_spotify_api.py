import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from spotify_api import fetch_playlist_tracks
from utils import write_playlist_to_m3u8
import playlist_extractor


class FakeSpotify:
    def __init__(self, pages):
        self.pages = pages
        self.playlist_calls = []
        self.next_calls = []

    def playlist_items(self, playlist_id, offset=0, limit=50):
        self.playlist_calls.append((playlist_id, offset, limit))
        return self.pages[0]

    def next(self, page):
        self.next_calls.append(page["next"])
        return self.pages[1] if page["next"] else None


class SpotifyApiCompatibilityTests(unittest.TestCase):
    def test_missing_zotify_music_root_is_treated_as_no_matching_playlist(self):
        with TemporaryDirectory() as temp_dir:
            missing_root = Path(temp_dir) / "Music" / "Zotify Music"
            self.assertIsNone(
                playlist_extractor.find_matching_playlist_dir(
                    "https://open.spotify.com/playlist/example", str(missing_root)
                )
            )

    def test_fetches_beyond_spotipy_226_default_page_size(self):
        first_page = [{"item": {"id": f"track-{i}"}} for i in range(50)]
        second_page = [{"item": {"id": f"track-{i}"}} for i in range(50, 55)]
        sp = FakeSpotify(
            [
                {"items": first_page, "next": "https://api.example/page/2"},
                {"items": second_page, "next": None},
            ]
        )

        tracks = fetch_playlist_tracks(sp, "https://open.spotify.com/playlist/test-id")

        self.assertEqual(len(tracks), 55)
        self.assertEqual(tracks, first_page + second_page)
        self.assertEqual(sp.playlist_calls, [("test-id", 0, 50)])
        self.assertEqual(sp.next_calls, ["https://api.example/page/2"])

    def test_m3u_writer_accepts_current_and_legacy_playlist_item_shapes(self):
        entries = [
            {
                "item": {
                    "name": "Current Track",
                    "artists": [{"name": "Current Artist"}],
                    "album": {"release_date": "2024-05-01"},
                }
            },
            {
                "track": {
                    "name": "Legacy Track",
                    "artists": [{"name": "Legacy Artist"}],
                    "album": {"release_date": "1999"},
                }
            },
        ]

        with TemporaryDirectory() as temp_dir:
            output_file = Path(temp_dir) / "playlist.m3u8"
            metadata = write_playlist_to_m3u8(entries, str(output_file))

            self.assertEqual(
                metadata,
                [
                    ("Current Artist - Current Track.mp3", "Current Artist - Current Track.mp3", "2024"),
                    ("Legacy Artist - Legacy Track.mp3", "Legacy Artist - Legacy Track.mp3", "1999"),
                ],
            )
            self.assertEqual(
                output_file.read_text(encoding="utf-8").splitlines(),
                ["Current Artist - Current Track.mp3", "Legacy Artist - Legacy Track.mp3"],
            )

    def test_playlist_extractor_supports_current_and_legacy_item_shapes(self):
        extract = getattr(playlist_extractor, "extract_playlist_tracks", None)
        if not callable(extract):
            self.fail("extract_playlist_tracks should be importable")
        entries = [
            {
                "item": {
                    "id": "current-id",
                    "name": "Café Track",
                    "artists": [{"name": "Current Artist"}],
                    "external_ids": {"isrc": "usabc2400001"},
                }
            },
            {
                "track": {
                    "id": "legacy-id",
                    "name": "Legacy Track",
                    "artists": [{"name": "Legacy Artist"}],
                    "external_ids": {"isrc": "usabc9900001"},
                }
            },
        ]

        self.assertEqual(
            extract(entries),
            [
                {"artist": "Current Artist", "title": "Café Track", "id": "current-id", "isrc": "USABC2400001"},
                {"artist": "Legacy Artist", "title": "Legacy Track", "id": "legacy-id", "isrc": "USABC9900001"},
            ],
        )


if __name__ == "__main__":
    unittest.main()
