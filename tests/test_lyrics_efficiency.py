"""Offline request-count regressions using real temporary ID3 files."""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from mutagen.id3 import ID3, TIT2, USLT
from unittest.mock import Mock, patch

from playlist_extractor import backfill_playlist_lyrics
from spotify_api import LYRICS_OK, LYRICS_UNAVAILABLE, LYRICS_RATE_LIMITED


class LyricsEfficiencyTests(unittest.TestCase):
    def make_tracks(self, directory, names):
        for name in names:
            tags = ID3()
            tags.add(TIT2(encoding=3, text=name))
            tags.save(Path(directory) / name)
        return [(name, name, "2026") for name in names]

    def test_unavailable_result_survives_a_new_cache_instance(self):
        from lyrics_cache import LyricsCache
        with TemporaryDirectory() as directory:
            root = Path(directory)
            playlist = root / "playlist"
            playlist.mkdir()
            metadata = self.make_tracks(playlist, ["Instrumental.mp3"])
            ids = {"Instrumental.mp3": "instrumental-id"}
            cache_path = root / "cache" / "lyrics.sqlite3"
            calls = []

            def fetch(track_id, token):
                calls.append(track_id)
                return LYRICS_UNAVAILABLE, ""

            for _ in range(2):
                stats = backfill_playlist_lyrics(
                    str(playlist), metadata, ids, "token", fetch=fetch,
                    cache=LyricsCache(cache_path), sleep=lambda _: None,
                )
            self.assertEqual(calls, ["instrumental-id"])
            self.assertEqual(stats["unavailable"], 1)

    def test_rate_limit_pauses_next_run_without_hiding_local_counts(self):
        from lyrics_cache import LyricsCache
        with TemporaryDirectory() as directory:
            root = Path(directory)
            playlist = root / "playlist"
            playlist.mkdir()
            names = ["A.mp3", "B.mp3", "Present.mp3"]
            metadata = self.make_tracks(playlist, names)
            metadata.append(("Ghost.mp3", "Ghost.mp3", "2026"))
            tags = ID3(playlist / "Present.mp3")
            tags.add(USLT(encoding=3, text="Keep these words"))
            tags.save(playlist / "Present.mp3")
            ids = dict(zip(names, ["a", "b", "present"]))
            cache_path = root / "cache.sqlite3"
            clock = Mock(return_value=1000)
            fetch = Mock(return_value=(LYRICS_RATE_LIMITED, "120"))
            stats = backfill_playlist_lyrics(
                str(playlist), metadata, ids, "token", fetch=fetch,
                cache=LyricsCache(cache_path, clock=clock), sleep=lambda _: None,
            )
            self.assertEqual(fetch.call_count, 1)
            self.assertEqual(stats["already_present"], 1)
            self.assertEqual(stats["unmatched"], 1)
            self.assertEqual(stats["deferred"], 1)
            fetch.reset_mock()
            stats = backfill_playlist_lyrics(
                str(playlist), metadata, ids, "token", fetch=fetch,
                cache=LyricsCache(cache_path, clock=clock), sleep=lambda _: None,
            )
            fetch.assert_not_called()
            self.assertEqual(stats["deferred"], 2)
            clock.return_value = 1121
            stats = backfill_playlist_lyrics(
                str(playlist), metadata, ids, "token", fetch=fetch,
                cache=LyricsCache(cache_path, clock=clock), sleep=lambda _: None,
            )
            self.assertEqual(fetch.call_count, 1)

    def test_cached_results_do_not_open_session_or_sleep(self):
        from lyrics_cache import LyricsCache
        with TemporaryDirectory() as directory:
            root = Path(directory)
            playlist = root / "playlist"
            playlist.mkdir()
            metadata = self.make_tracks(playlist, ["A.mp3", "B.mp3"])
            cache = LyricsCache(root / "cache.sqlite3")
            cache.put("a", LYRICS_UNAVAILABLE, "")
            cache.put("b", LYRICS_OK, "Cached words")
            provider = Mock(side_effect=AssertionError("must stay lazy"))
            fetch = Mock(side_effect=AssertionError("must not fetch"))
            slept = []
            stats = backfill_playlist_lyrics(
                str(playlist), metadata, {"A.mp3": "a", "B.mp3": "b"}, provider,
                fetch=fetch, cache=cache, sleep=slept.append,
            )
            fetch.assert_not_called()
            provider.assert_not_called()
            self.assertEqual(slept, [])
            self.assertEqual(stats["cache_hits"], 2)
            self.assertEqual(stats["lookups"], 0)

    def test_same_track_id_is_fetched_once_for_distinct_files(self):
        for status, text in ((LYRICS_OK, "Shared words"), (LYRICS_UNAVAILABLE, "")):
            with self.subTest(status=status), TemporaryDirectory() as directory:
                names = ["Artist - Song.mp3", "Artist - Song_1.mp3"]
                metadata = self.make_tracks(directory, names)
                calls = []

                def fetch(track_id, token):
                    calls.append(track_id)
                    return status, text

                stats = backfill_playlist_lyrics(
                    directory, metadata, dict.fromkeys(names, "same-id"), "token",
                    fetch=fetch, sleep=lambda _: None,
                )

                self.assertEqual(calls, ["same-id"])
                self.assertEqual(stats["embedded" if status == LYRICS_OK else "unavailable"], 2)
                if status == LYRICS_OK:
                    for name in names:
                        self.assertEqual(ID3(Path(directory) / name).getall("USLT")[0].text, text)
