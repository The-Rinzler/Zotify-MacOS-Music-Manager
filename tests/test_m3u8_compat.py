import tempfile
import unittest
from pathlib import Path

import library_manager


class M3U8CompatibilityTests(unittest.TestCase):
    def test_parses_zotify_extended_m3u_paths_not_metadata(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            playlist = Path(temp_dir) / "mix.m3u8"
            playlist.write_text(
                "\ufeff#EXTM3U\n\n"
                "#EXTINF:123,Artist - First Track\n"
                "../audio/First Track.mp3\n\n"
                "#EXTVLCOPT:network-caching=1000\n"
                "../audio/Second Track.mp3\n\n"
                "# Missing track 3\n# None\n\n"
                "#EXTINF:234,Artist - First Track\n"
                "../audio/First Track.mp3\n\n",
                encoding="utf-8",
            )

            self.assertEqual(
                library_manager.get_valid_songs_from_m3u(playlist),
                ["../audio/First Track.mp3", "../audio/Second Track.mp3"],
            )
            self.assertEqual(library_manager.count_unique_songs_in_m3u(playlist), 2)

    def test_missing_song_check_resolves_relative_paths_from_playlist_location(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            playlist_dir = root / "zotify-output" / "Playlist"
            audio_dir = playlist_dir / "audio"
            library_dir = root / "consolidated"
            playlist_dir.mkdir(parents=True)
            audio_dir.mkdir(parents=True)
            library_dir.mkdir()
            (audio_dir / "Track.mp3").write_bytes(b"audio")
            (playlist_dir / "mix.m3u8").write_text(
                "#EXTM3U\n#EXTINF:123,Artist - Track\n"
                "audio/Track.mp3\n",
                encoding="utf-8",
            )

            self.assertEqual(
                library_manager.check_missing_songs_in_playlists(
                    str(playlist_dir), str(library_dir)
                ),
                {},
            )

    def test_missing_song_check_keeps_library_root_fallback(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            playlist_dir = root / "playlists"
            library_dir = root / "library"
            playlist_dir.mkdir()
            library_dir.mkdir()
            (library_dir / "Track.mp3").write_bytes(b"audio")
            (playlist_dir / "mix.m3u8").write_text(
                "#EXTM3U\nTrack.mp3\n", encoding="utf-8"
            )

            self.assertEqual(
                library_manager.check_missing_songs_in_playlists(
                    str(playlist_dir), str(library_dir)
                ),
                {},
            )

    def test_nested_relative_path_is_not_rebased_to_library_root(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            playlist_dir = root / "zotify-output" / "Playlist"
            library_dir = root / "consolidated"
            playlist_dir.mkdir(parents=True)
            (library_dir / "audio").mkdir(parents=True)
            (library_dir / "audio" / "Track.mp3").write_bytes(b"audio")
            (playlist_dir / "mix.m3u8").write_text(
                "#EXTM3U\n#EXTINF:123,Artist - Track\naudio/Track.mp3\n",
                encoding="utf-8",
            )

            self.assertEqual(
                library_manager.check_missing_songs_in_playlists(
                    str(playlist_dir), str(library_dir)
                ),
                {"mix.m3u8": ["audio/Track.mp3"]},
            )

    def test_consolidation_flattens_zotify_relative_track_paths(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_dir = root / "zotify-output"
            playlist_dir = source_dir / "Playlist Name"
            nested_audio_dir = playlist_dir / "audio"
            destination_dir = root / "consolidated"
            nested_audio_dir.mkdir(parents=True)
            (nested_audio_dir / "Track.mp3").write_bytes(b"audio")
            (playlist_dir / "Playlist Name.m3u8").write_text(
                "#EXTM3U\n#EXTINF:123,Artist - Track\naudio/Track.mp3\n",
                encoding="utf-8",
            )

            library_manager.consolidate_library(
                [str(source_dir)], str(destination_dir)
            )

            consolidated_playlist = destination_dir / "Playlist Name.m3u8"
            self.assertTrue((destination_dir / "Track.mp3").is_file())
            self.assertEqual(
                consolidated_playlist.read_text(encoding="utf-8").splitlines(),
                ["#EXTM3U", "#EXTINF:123,Artist - Track", "Track.mp3"],
            )
            self.assertEqual(
                library_manager.get_valid_songs_from_m3u(consolidated_playlist),
                ["Track.mp3"],
            )
            self.assertEqual(
                library_manager.check_missing_songs_in_playlists(
                    str(destination_dir), str(destination_dir)
                ),
                {},
            )


if __name__ == "__main__":
    unittest.main()
