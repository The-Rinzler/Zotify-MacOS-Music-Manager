import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from mutagen.id3 import ID3, TIT2, USLT

from playlist_extractor import (
    backfill_playlist_lyrics,
    build_playlist_track_ids,
    embed_lyrics_in_file,
    file_lyrics_state,
)
from spotify_api import (
    LYRICS_AUTH_ERROR,
    LYRICS_ERROR,
    LYRICS_OK,
    LYRICS_RATE_LIMITED,
    LYRICS_UNAVAILABLE,
    fetch_track_lyrics,
    get_spotify_bearer_token,
    parse_spotify_lyrics,
)


class FakeResponse:
    def __init__(self, status_code, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = ""

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


def synced_payload():
    return {
        "lyrics": {
            "syncType": "LINE_SYNCED",
            "lines": [
                {"startTimeMs": "1000", "words": "First line"},
                {"startTimeMs": "4250", "words": "Second line"},
            ],
        }
    }


class LyricsParseTests(unittest.TestCase):
    def test_synced_payload_returns_plain_text_without_timestamps(self):
        found, text = parse_spotify_lyrics(synced_payload())
        self.assertTrue(found)
        self.assertEqual(text, "First line\nSecond line")

    def test_unsynced_payload_returns_text(self):
        payload = {
            "lyrics": {
                "syncType": "UNSYNCED",
                "lines": [{"words": "Only line"}],
            }
        }
        found, text = parse_spotify_lyrics(payload)
        self.assertTrue(found)
        self.assertEqual(text, "Only line")

    def test_missing_or_empty_lyrics_is_not_found(self):
        self.assertEqual(parse_spotify_lyrics({}), (False, ""))
        self.assertEqual(parse_spotify_lyrics(None), (False, ""))
        self.assertEqual(
            parse_spotify_lyrics({"lyrics": None}), (False, "")
        )
        self.assertEqual(
            parse_spotify_lyrics({"lyrics": {"lines": [{"words": "  "}]}}),
            (False, ""),
        )


class FetchTrackLyricsTests(unittest.TestCase):
    def make_getter(self, responses):
        calls = []

        def http_get(url, headers=None, params=None, timeout=None):
            calls.append((url, headers, params))
            response = responses[min(len(calls) - 1, len(responses) - 1)]
            if isinstance(response, Exception):
                raise response
            return response

        return http_get, calls

    def test_ok_status_returns_text(self):
        http_get, calls = self.make_getter([FakeResponse(200, synced_payload())])
        slept = []
        status, text = fetch_track_lyrics(
            "abc123", "tok", http_get=http_get, sleep=slept.append
        )
        self.assertEqual(status, LYRICS_OK)
        self.assertEqual(text, "First line\nSecond line")
        self.assertTrue(calls[0][0].endswith("/color-lyrics/v2/track/abc123"))
        self.assertEqual(calls[0][1]["Authorization"], "Bearer tok")
        self.assertIsNone(calls[0][2])  # Zotify invokes this endpoint without query params.

    def test_404_and_empty_payload_are_unavailable(self):
        http_get, _ = self.make_getter([FakeResponse(404)])
        status, _ = fetch_track_lyrics("x", "t", http_get=http_get, sleep=lambda s: None)
        self.assertEqual(status, LYRICS_UNAVAILABLE)

        http_get, _ = self.make_getter([FakeResponse(200, {"lyrics": None})])
        status, _ = fetch_track_lyrics("x", "t", http_get=http_get, sleep=lambda s: None)
        self.assertEqual(status, LYRICS_UNAVAILABLE)

    def test_invalid_json_response_is_reported_as_api_error(self):
        http_get, _ = self.make_getter([FakeResponse(200)])
        status, _ = fetch_track_lyrics("x", "t", http_get=http_get, sleep=lambda s: None)
        self.assertEqual(status, LYRICS_ERROR)

    def test_401_is_auth_error_without_retries(self):
        http_get, calls = self.make_getter([FakeResponse(401)])
        status, _ = fetch_track_lyrics(
            "x", "t", http_get=http_get, sleep=lambda s: None, retry_attempts=3
        )
        self.assertEqual(status, LYRICS_AUTH_ERROR)
        self.assertEqual(len(calls), 1)

    def test_rate_limit_is_respected_then_recovers(self):
        http_get, calls = self.make_getter(
            [FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200, synced_payload())]
        )
        slept = []
        status, text = fetch_track_lyrics(
            "x", "t", http_get=http_get, sleep=slept.append, retry_attempts=2
        )
        self.assertEqual(status, LYRICS_OK)
        self.assertEqual(text, "First line\nSecond line")
        self.assertIn(7.0, slept)

    def test_persistent_rate_limit_gives_up(self):
        http_get, _ = self.make_getter([FakeResponse(429)])
        status, _ = fetch_track_lyrics(
            "x", "t", http_get=http_get, sleep=lambda s: None, retry_attempts=1
        )
        self.assertEqual(status, LYRICS_RATE_LIMITED)

    def test_network_errors_are_retried_then_reported(self):
        http_get, calls = self.make_getter([ConnectionError("boom")])
        status, _ = fetch_track_lyrics(
            "x", "t", http_get=http_get, sleep=lambda s: None, retry_attempts=2
        )
        self.assertEqual(status, LYRICS_ERROR)
        self.assertEqual(len(calls), 3)


class SpotifyBearerTokenTests(unittest.TestCase):
    def test_requests_access_token_as_string_without_deprecated_dict_mode(self):
        class AuthManager:
            def __init__(self):
                self.as_dict = None

            def get_access_token(self, as_dict=True):
                self.as_dict = as_dict
                return "spotify-access-token" if not as_dict else {"access_token": "spotify-access-token"}

        class Client:
            def __init__(self):
                self.auth_manager = AuthManager()

        client = Client()

        self.assertEqual(get_spotify_bearer_token(client), "spotify-access-token")
        self.assertFalse(client.auth_manager.as_dict)

    def test_returns_none_without_spotify_auth_manager(self):
        self.assertIsNone(get_spotify_bearer_token(object()))


class EmbedLyricsTests(unittest.TestCase):
    def make_mp3(self, dir_path, name, tags):
        file_path = Path(dir_path) / name
        tags.save(file_path)
        return file_path

    def test_embeds_into_file_without_lyrics_and_keeps_other_tags(self):
        with TemporaryDirectory() as temp_dir:
            tags = ID3()
            tags.add(TIT2(encoding=3, text="Song"))
            file_path = self.make_mp3(temp_dir, "track.mp3", tags)

            result = embed_lyrics_in_file(str(file_path), "Line one\nLine two")

            self.assertEqual(result, "embedded")
            saved = ID3(file_path)
            self.assertEqual(saved.get("TIT2").text, ["Song"])
            self.assertEqual(saved.getall("USLT")[0].text, "Line one\nLine two")
            self.assertEqual(file_lyrics_state(str(file_path)), "present")

    def test_does_not_overwrite_existing_lyrics(self):
        with TemporaryDirectory() as temp_dir:
            tags = ID3()
            tags.add(USLT(encoding=3, lang="eng", desc="", text="Real lyrics"))
            file_path = self.make_mp3(temp_dir, "track.mp3", tags)

            result = embed_lyrics_in_file(str(file_path), "Override")

            self.assertEqual(result, "already_present")
            self.assertEqual(ID3(file_path).getall("USLT")[0].text, "Real lyrics")

    def test_preserves_id3v1_trailer_and_v23_header(self):
        with TemporaryDirectory() as temp_dir:
            tags = ID3()
            tags.add(TIT2(encoding=3, text="Song"))
            file_path = Path(temp_dir) / "track.mp3"
            tags.save(file_path, v2_version=3)
            trailer = b"TAG" + b"Legacy title".ljust(30, b"\x00") + bytes(95)
            with file_path.open("ab") as audio_file:
                audio_file.write(trailer)

            result = embed_lyrics_in_file(str(file_path), "Héllo — 世界")

            self.assertEqual(result, "embedded")
            self.assertTrue(file_path.read_bytes().endswith(trailer))
            saved_tags = ID3(file_path)
            self.assertEqual(saved_tags.version, (2, 3, 0))
            self.assertEqual(saved_tags.getall("USLT")[0].text, "Héllo — 世界")
            self.assertEqual(int(saved_tags.getall("USLT")[0].encoding), 1)

    def test_creates_tag_on_file_without_id3_header(self):
        with TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "bare.mp3"
            file_path.write_bytes(b"\xff\xfb\x90\x00" + b"\x00" * 200)

            self.assertEqual(file_lyrics_state(str(file_path)), "missing")
            result = embed_lyrics_in_file(str(file_path), "Fresh lyrics")

            self.assertEqual(result, "embedded")
            self.assertEqual(ID3(file_path).getall("USLT")[0].text, "Fresh lyrics")


class BuildTrackIdsTests(unittest.TestCase):
    def test_maps_current_and_legacy_shapes_and_skips_locals(self):
        items = [
            {"track": {"id": "id1", "name": "Song One", "artists": [{"name": "Artist"}]}},
            {"item": {"id": "id2", "name": "Song: Two", "artists": [{"name": "A/B"}]}},
            {"track": {"name": "Local file", "artists": [{"name": "Me"}]}},
            None,
        ]
        mapping = build_playlist_track_ids(items)
        self.assertEqual(mapping["Artist - Song One.mp3"], "id1")
        self.assertEqual(mapping["A_B - Song_ Two.mp3"], "id2")
        self.assertNotIn("Me - Local file.mp3", mapping)
        self.assertEqual(len(mapping), 2)


class BackfillFlowTests(unittest.TestCase):
    def _mp3(self, temp_dir, name, with_lyrics=None):
        tags = ID3()
        tags.add(TIT2(encoding=3, text=name))
        if with_lyrics is not None:
            tags.add(USLT(encoding=3, lang="eng", desc="", text=with_lyrics))
        path = Path(temp_dir) / name
        tags.save(path)
        return path

    def test_embeds_only_missing_lyrics_and_reports_stats(self):
        with TemporaryDirectory() as temp_dir:
            missing = self._mp3(temp_dir, "Missing.mp3")
            present = self._mp3(temp_dir, "Present.mp3", with_lyrics="Keep me")
            unavailable = self._mp3(temp_dir, "Unavailable.mp3")
            self._mp3(temp_dir, "NoId.mp3")

            metadata = [
                ("Missing.mp3", "Missing.mp3", "2024"),
                ("Present.mp3", "Present.mp3", "2024"),
                ("Unavailable.mp3", "Unavailable.mp3", "2024"),
                ("Ghost.mp3", "Ghost.mp3", "2024"),
                ("NoId.mp3", "NoId.mp3", "2024"),
            ]
            ids = {"Missing.mp3": "id1", "Present.mp3": "id2", "Unavailable.mp3": "id3"}

            fetches = []

            def fake_fetch(track_id, token):
                fetches.append((track_id, token))
                if track_id == "id1":
                    return LYRICS_OK, "Backfilled line"
                return "unavailable", ""

            stats = backfill_playlist_lyrics(
                temp_dir, metadata, ids, "tok", fetch=fake_fetch, sleep=lambda s: None
            )

            self.assertEqual(
                stats,
                {
                    "embedded": 1,
                    "already_present": 1,
                    "unavailable": 1,
                    "no_track_id": 1,
                    "unmatched": 1,
                    "unsupported_version": 0,
                    "errors": 0,
                    "aborted": 0,
                },
            )
            self.assertEqual(ID3(missing).getall("USLT")[0].text, "Backfilled line")
            self.assertEqual(ID3(present).getall("USLT")[0].text, "Keep me")
            self.assertEqual([tid for tid, _ in fetches], ["id1", "id3"])

    def test_auth_error_aborts_entire_run(self):
        with TemporaryDirectory() as temp_dir:
            self._mp3(temp_dir, "A.mp3")
            self._mp3(temp_dir, "B.mp3")
            metadata = [("A.mp3", "A.mp3", "2024"), ("B.mp3", "B.mp3", "2024")]
            ids = {"A.mp3": "ida", "B.mp3": "idb"}
            calls = []

            def fake_fetch(track_id, token):
                calls.append(track_id)
                return LYRICS_AUTH_ERROR, ""

            stats = backfill_playlist_lyrics(
                temp_dir, metadata, ids, "tok", fetch=fake_fetch, sleep=lambda s: None
            )
            self.assertEqual(stats["aborted"], 1)
            self.assertEqual(calls, ["ida"])

    def test_bad_tag_file_is_counted_and_later_tracks_continue(self):
        with TemporaryDirectory() as temp_dir:
            self._mp3(temp_dir, "Bad.mp3")
            self._mp3(temp_dir, "Good.mp3")
            metadata = [("Bad.mp3", "Bad.mp3", "2024"), ("Good.mp3", "Good.mp3", "2024")]
            ids = {"Bad.mp3": "bad-id", "Good.mp3": "good-id"}

            with patch(
                "playlist_extractor.file_lyrics_state",
                side_effect=[OSError("invalid tag"), "missing", "missing"],
            ):
                stats = backfill_playlist_lyrics(
                    temp_dir,
                    metadata,
                    ids,
                    "tok",
                    fetch=lambda track_id, token: (LYRICS_OK, "Lyrics"),
                    sleep=lambda seconds: None,
                )

            self.assertEqual(stats["errors"], 1)
            self.assertEqual(stats["embedded"], 1)

    def test_repeated_errors_abort_run(self):
        with TemporaryDirectory() as temp_dir:
            for name in ("A.mp3", "B.mp3", "C.mp3", "D.mp3"):
                self._mp3(temp_dir, name)
            metadata = [(f"{n}.mp3", f"{n}.mp3", "2024") for n in "ABCD"]
            ids = {f"{n}.mp3": n for n in "ABCD"}

            def fake_fetch(track_id, token):
                return LYRICS_ERROR, ""

            stats = backfill_playlist_lyrics(
                temp_dir,
                metadata,
                ids,
                "tok",
                fetch=fake_fetch,
                sleep=lambda s: None,
                max_consecutive_errors=3,
            )
            self.assertEqual(stats["aborted"], 1)
            self.assertEqual(stats["errors"], 3)

    def test_empty_token_skips_without_touching_files(self):
        with TemporaryDirectory() as temp_dir:
            path = self._mp3(temp_dir, "A.mp3")
            def fail_fetch(track_id, token):
                self.fail("fetch must not be called without a bearer token")

            stats = backfill_playlist_lyrics(
                temp_dir,
                [("A.mp3", "A.mp3", "2024")],
                {"A.mp3": "ida"},
                "",
                fetch=fail_fetch,
                sleep=lambda s: None,
            )
            self.assertEqual(stats["aborted"], 1)
            self.assertEqual(stats["embedded"], 0)
            self.assertEqual(file_lyrics_state(str(path)), "missing")


if __name__ == "__main__":
    unittest.main()
