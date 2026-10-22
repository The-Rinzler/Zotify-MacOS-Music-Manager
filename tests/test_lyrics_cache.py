"""Disposable cache semantics, expiry, concurrency and failure isolation."""
import sqlite3
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from lyrics_cache import LyricsCache
from spotify_api import LYRICS_OK, LYRICS_UNAVAILABLE, LYRICS_ERROR, LYRICS_AUTH_ERROR, LYRICS_RATE_LIMITED


class LyricsCacheTests(unittest.TestCase):
    def test_results_expire_and_new_instances_share_committed_data(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "lyrics.sqlite3"
            clock = Mock(return_value=1000)
            cache = LyricsCache(path, clock=clock)
            cache.put("yes", LYRICS_OK, "Words")
            cache.put("no", LYRICS_UNAVAILABLE, "")
            other = LyricsCache(path, clock=clock)
            self.assertEqual(other.get("yes"), (LYRICS_OK, "Words"))
            self.assertEqual(other.get("no"), (LYRICS_UNAVAILABLE, ""))
            clock.return_value = 1000 + cache.UNAVAILABLE_TTL
            self.assertIsNone(other.get("no"))
            self.assertEqual(other.get("yes"), (LYRICS_OK, "Words"))
            clock.return_value = 1000 + cache.FOUND_TTL
            self.assertIsNone(other.get("yes"))

    def test_errors_and_empty_successes_are_not_negative_cached(self):
        with TemporaryDirectory() as directory:
            cache = LyricsCache(Path(directory) / "cache.sqlite3")
            for status in (LYRICS_ERROR, LYRICS_AUTH_ERROR, LYRICS_RATE_LIMITED, LYRICS_OK):
                cache.put("id", status, "")
                self.assertIsNone(cache.get("id"))

    def test_cooldown_survives_restarts_and_cannot_be_shortened(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "lyrics.sqlite3"
            clock = Mock(return_value=1000)
            cache = LyricsCache(path, clock=clock)
            cache.defer(120)
            other = LyricsCache(path, clock=clock)
            clock.return_value = 1010
            other.defer(60)
            self.assertEqual(cache.cooldown_remaining(), 110)
            clock.return_value = 1120
            self.assertEqual(other.cooldown_remaining(), 0)

    def test_corrupt_cache_is_disabled_without_replacing_it(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "bad.sqlite3"
            path.write_bytes(b"not a database")
            cache = LyricsCache(path)
            self.assertIsNone(cache.get("id"))
            cache.put("id", LYRICS_OK, "Words")
            cache.defer(60)
            self.assertEqual(cache.cooldown_remaining(), 0)
            self.assertTrue(cache.failed)
            self.assertEqual(path.read_bytes(), b"not a database")

    def test_unwritable_path_is_nonfatal(self):
        with TemporaryDirectory() as directory:
            blocker = Path(directory) / "file"
            blocker.write_text("Keep me")
            cache = LyricsCache(blocker / "cache.sqlite3")
            self.assertIsNone(cache.get("id"))
            self.assertTrue(cache.failed)
            self.assertEqual(blocker.read_text(), "Keep me")

    def test_parameterized_track_ids_cannot_modify_schema(self):
        with TemporaryDirectory() as directory:
            cache = LyricsCache(Path(directory) / "cache.sqlite3")
            key = "'; DROP TABLE lyrics; --"
            cache.put(key, LYRICS_OK, "Words")
            self.assertEqual(cache.get(key), (LYRICS_OK, "Words"))
            cache.put("good", LYRICS_UNAVAILABLE, "")
            self.assertEqual(cache.get("good"), (LYRICS_UNAVAILABLE, ""))

    def test_concurrent_cache_instances_do_not_lose_results(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "cache.sqlite3"
            def put(index):
                cache = LyricsCache(path)
                cache.put(str(index), LYRICS_OK, f"Words {index}")
                return cache.failed
            with ThreadPoolExecutor(max_workers=4) as executor:
                self.assertFalse(any(executor.map(put, range(20))))
            cache = LyricsCache(path)
            for index in range(20):
                self.assertEqual(cache.get(str(index)), (LYRICS_OK, f"Words {index}"))

    def test_platform_default_is_lazy_and_outside_music(self):
        with TemporaryDirectory() as directory, patch("lyrics_cache.Path.home", return_value=Path(directory)):
            with patch("lyrics_cache.sys.platform", "darwin"):
                cache = LyricsCache()
                self.assertEqual(cache.path, Path(directory) / "Library/Caches/zotify-music-manager/lyrics.sqlite3")
            with patch("lyrics_cache.sys.platform", "linux"), patch.dict("os.environ", {"XDG_CACHE_HOME": directory}):
                self.assertEqual(LyricsCache().path, Path(directory) / "zotify-music-manager/lyrics.sqlite3")
            self.assertFalse(cache.path.exists())
