"""Offline regression tests: lyrics must use Login5, never the playlist bearer."""
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import spotify_api


class ZotifyTokenProviderTests(unittest.TestCase):
    def test_uses_zotify_interpreter_and_keeps_token_only_in_memory(self):
        from zotify_lyrics import ZotifyTokenProvider

        with TemporaryDirectory() as directory:
            python = Path(directory) / "zotify/.venv/bin/python"
            python.parent.mkdir(parents=True)
            python.touch(mode=0o755)
            result = subprocess.CompletedProcess([], 0, json.dumps({
                "access_token": "session-only-token", "expires_in": 3600,
            }), "")
            with patch.dict(os.environ, {"PYTHONPATH": "unsafe", "PYTHONHOME": "unsafe"}), patch(
                "zotify_lyrics.subprocess.run", return_value=result
            ) as run:
                provider = ZotifyTokenProvider(python=python)
                self.assertEqual(provider(), "session-only-token")
                self.assertEqual(provider(), "session-only-token")
            self.assertEqual(run.call_count, 1)
            command = run.call_args.args[0]
            self.assertEqual(command[:2], [str(python), "-I"])
            self.assertNotIn("session-only-token", " ".join(command))
            self.assertNotIn("PYTHONPATH", run.call_args.kwargs["env"])
            self.assertNotIn("PYTHONHOME", run.call_args.kwargs["env"])
            self.assertEqual(run.call_args.kwargs["stdin"], subprocess.DEVNULL)
            self.assertGreater(run.call_args.kwargs["timeout"], 0)
            self.assertNotIn("session-only-token", repr(provider))

    def test_cache_refreshes_and_process_failures_are_sanitized(self):
        from zotify_lyrics import ZotifyAuthError, ZotifyTokenProvider
        success = subprocess.CompletedProcess([], 0, json.dumps({
            "access_token": "fixture-token", "expires_in": 3600,
        }), "")
        with patch("zotify_lyrics.resolve_zotify_python", return_value=Path(sys.executable)), patch(
            "zotify_lyrics.subprocess.run", return_value=success
        ) as run, patch("zotify_lyrics.time.monotonic", return_value=100):
            provider = ZotifyTokenProvider()
            self.assertEqual(provider(), "fixture-token")
            self.assertEqual(provider._deadline, 400)
            provider._deadline = 0
            self.assertEqual(provider(), "fixture-token")
            self.assertEqual(run.call_count, 2)
            for result in (
                subprocess.CompletedProcess([], 0, "not json SECRET", "SECRET"),
                subprocess.CompletedProcess([], 0, "[]", "SECRET"),
                subprocess.CompletedProcess([], 0, '{"access_token":"SECRET","expires_in":1}', ""),
                subprocess.CompletedProcess([], 1, '{"error":"SECRET"}', ""),
            ):
                provider._deadline = 0
                run.return_value = result
                with self.assertRaises(ZotifyAuthError) as error:
                    provider()
                self.assertNotIn("SECRET", str(error.exception))
            for failure in (subprocess.TimeoutExpired(["PRIVATE"], 60), OSError("SECRET")):
                run.side_effect = failure
                with self.assertRaises(ZotifyAuthError) as error:
                    provider()
                self.assertNotIn("SECRET", str(error.exception))
                self.assertNotIn("PRIVATE", str(error.exception))

    def test_explicit_missing_interpreter_never_silently_falls_back(self):
        from zotify_lyrics import ZotifyAuthError, resolve_zotify_python
        with TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ZotifyAuthError, "--zotify-python"):
                resolve_zotify_python(Path(directory) / "missing-python")

    def test_provider_failure_is_actionable_without_raw_stderr_or_tokens(self):
        from zotify_lyrics import ZotifyAuthError, ZotifyTokenProvider

        result = subprocess.CompletedProcess([], 1, '{"error":"credentials_missing"}', "SECRET")
        with patch("zotify_lyrics.resolve_zotify_python", return_value=Path(sys.executable)), patch(
            "zotify_lyrics.subprocess.run", return_value=result
        ):
            with self.assertRaises(ZotifyAuthError) as error:
                ZotifyTokenProvider()()
        self.assertIn("Zotify", str(error.exception))
        self.assertIn("sign in", str(error.exception))
        self.assertNotIn("SECRET", str(error.exception))


class SessionFetchTests(unittest.TestCase):
    def test_lyrics_fetch_resolves_session_provider_not_spotipy_token(self):
        class Response:
            status_code = 200

            def json(self):
                return {"lyrics": {"lines": [{"words": "A line"}]}}

        calls = []
        def get(url, **kwargs):
            calls.append(kwargs["headers"]["Authorization"])
            return Response()

        result = spotify_api.fetch_track_lyrics(
            "3V7choSVaP2zUqtIYpfynG", lambda: "login5-token", http_get=get
        )
        self.assertEqual(result, (spotify_api.LYRICS_OK, "A line"))
        self.assertEqual(calls, ["Bearer login5-token"])

    def test_auth_rejection_preserves_exact_http_status_without_body(self):
        for code in (401, 403):
            with self.subTest(code=code):
                class Response:
                    status_code = code
                    text = "SECRET_SERVER_BODY"
                result = spotify_api.fetch_track_lyrics(
                    "3V7choSVaP2zUqtIYpfynG", "secret-token",
                    http_get=lambda *args, **kwargs: Response(),
                )
                self.assertEqual(result, (spotify_api.LYRICS_AUTH_ERROR, f"HTTP {code}"))


class SavedSessionBridgeTests(unittest.TestCase):
    def test_default_and_custom_credential_locations_are_read_only(self):
        from zotify_lyrics import saved_credentials_path
        with TemporaryDirectory() as directory:
            root = Path(directory)
            default = root / "default"
            self.assertEqual(saved_credentials_path(default), default / "credentials.json")
            self.assertFalse(default.exists())
            config = root / "config.json"
            config.write_text(json.dumps({
                "ROOT_PATH": str(root / "music"),
                "CREDENTIALS_LOCATION": "./private/session.json",
            }))
            self.assertEqual(
                saved_credentials_path(default, config), root / "music/private/session.json"
            )
            explicit = root / "other.json"
            self.assertEqual(saved_credentials_path(default, config, explicit), explicit)
            self.assertFalse((root / "music").exists())
            self.assertFalse(explicit.exists())

    def test_existing_config_and_credentials_directories(self):
        from zotify_lyrics import saved_credentials_path
        with TemporaryDirectory() as directory:
            root = Path(directory)
            config_dir = root / "config"
            config_dir.mkdir()
            credentials_dir = root / "session"
            credentials_dir.mkdir()
            (config_dir / "config.json").write_text(json.dumps({
                "CREDENTIALS_LOCATION": str(credentials_dir),
            }))
            self.assertEqual(
                saved_credentials_path(root / "default", config_dir), credentials_dir / "credentials.json"
            )

    def test_invalid_or_missing_explicit_config_is_not_silently_ignored(self):
        from zotify_lyrics import ZotifyAuthError, saved_credentials_path
        with TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config.json"
            for value in (None, "not json", "[]"):
                if value is not None:
                    config.write_text(value)
                with self.assertRaisesRegex(ZotifyAuthError, "config_invalid"):
                    saved_credentials_path(root / "default", config)

    def test_reuses_zotify_login5_and_scopes_without_saving_credentials(self):
        from types import ModuleType, SimpleNamespace
        from unittest.mock import Mock
        from zotify_lyrics import session_token
        with TemporaryDirectory() as directory:
            root = Path(directory)
            credentials = {"type": "STORED", "username": "offline-fixture", "credentials": "fixture"}
            (root / "credentials.json").write_text(json.dumps(credentials))
            before = (root / "credentials.json").read_bytes()
            session = Mock()
            session.tokens.return_value.get_token.return_value = SimpleNamespace(
                access_token="fixture-session-token", expires_in=3600
            )
            builder = SimpleNamespace(conf=SimpleNamespace(store_credentials=True))
            handler = SimpleNamespace(
                SESSION=None, SESSION_BUILDER=None, CRED_TYPES={"STORED", "PKCE"},
            )
            def login(creds):
                self.assertEqual(creds, credentials)
                self.assertIs(handler.SESSION_BUILDER, builder)
                self.assertFalse(builder.conf.store_credentials)
                handler.SESSION = session
            handler.login5_cred_login = Mock(side_effect=login)
            config = ModuleType("zotify.config")
            config.Config = SimpleNamespace(_default_path=lambda: root)
            config.LoginHandler = handler
            const = ModuleType("zotify.const")
            const.SCOPES = ["zotify-scope-a", "zotify-scope-b"]
            core = ModuleType("librespot.core")
            core.OAuth = SimpleNamespace(OAUTH_PKCE_TOKEN="PKCE")
            core.Session = SimpleNamespace(Builder=lambda: builder)
            modules = {"zotify.config": config, "zotify.const": const, "librespot.core": core}
            with patch.dict(sys.modules, modules):
                self.assertEqual(session_token(), {"access_token": "fixture-session-token", "expires_in": 3600})
                session.tokens.return_value.get_token.assert_called_once_with(*const.SCOPES)
                session.close.assert_called_once()
                self.assertEqual((root / "credentials.json").read_bytes(), before)
                session.tokens.return_value.get_token.side_effect = RuntimeError("SECRET")
                from zotify_lyrics import ZotifyAuthError
                with self.assertRaisesRegex(ZotifyAuthError, "^session_failed$"):
                    session_token()
                self.assertEqual(session.close.call_count, 2)
                (root / "credentials.json").write_text(json.dumps({"type": "PKCE"}))
                with self.assertRaisesRegex(ZotifyAuthError, "credentials_invalid"):
                    session_token()
                self.assertEqual(handler.login5_cred_login.call_count, 2)


class SessionBackfillTests(unittest.TestCase):
    def test_no_session_is_opened_when_lyrics_are_already_present(self):
        from mutagen.id3 import ID3, USLT
        from playlist_extractor import backfill_playlist_lyrics
        from unittest.mock import Mock
        with TemporaryDirectory() as directory:
            path = Path(directory) / "Artist - Present.mp3"
            tags = ID3()
            tags.add(USLT(encoding=3, text="existing words"))
            tags.save(path)
            original = path.read_bytes()
            provider = Mock(side_effect=AssertionError("must stay lazy"))
            stats = backfill_playlist_lyrics(
                directory, [(path.name, path.name, "2026")], {path.name: "track"},
                provider, sleep=lambda seconds: None,
            )
            self.assertEqual(stats["already_present"], 1)
            provider.assert_not_called()
            self.assertEqual(path.read_bytes(), original)

    def test_cli_accepts_zotify_overrides_without_starting_auth(self):
        from playlist_extractor import get_playlist_url
        with patch.object(sys, "argv", [
            "playlist_extractor.py", "https://open.spotify.com/playlist/offline",
            "--zotify-python", "/zotify/.venv/bin/python",
            "--zotify-config-location", "/session/config.json",
            "--zotify-credentials-location", "/session/credentials.json",
        ]):
            url, args = get_playlist_url()
        self.assertEqual(url, "https://open.spotify.com/playlist/offline")
        self.assertEqual(args.zotify_python, "/zotify/.venv/bin/python")
        self.assertEqual(args.zotify_config_location, "/session/config.json")
        self.assertEqual(args.zotify_credentials_location, "/session/credentials.json")

    def test_missing_zotify_session_aborts_without_touching_tags(self):
        from mutagen.id3 import ID3, TIT2
        from playlist_extractor import backfill_playlist_lyrics
        from zotify_lyrics import ZotifyAuthError

        with TemporaryDirectory() as directory:
            path = Path(directory) / "INNA - Deja Vu.mp3"
            tags = ID3()
            tags.add(TIT2(encoding=3, text="Deja Vu"))
            tags.save(path)
            original = path.read_bytes()
            def provider():
                raise ZotifyAuthError("Zotify session missing; sign in with Zotify first.")
            with patch("playlist_extractor.CustomPrint.bold_print") as output:
                stats = backfill_playlist_lyrics(
                    directory, [(path.name, path.name, "2010")],
                    {path.name: "3V7choSVaP2zUqtIYpfynG"}, provider,
                    sleep=lambda seconds: None,
                )
            self.assertEqual(stats["aborted"], 1)
            self.assertEqual(stats["errors"], 1)
            self.assertEqual(path.read_bytes(), original)
            self.assertIn("sign in with Zotify", str(output.call_args_list))

    def test_http_status_track_and_provider_are_visible_in_auth_diagnostic(self):
        from mutagen.id3 import ID3
        from playlist_extractor import backfill_playlist_lyrics
        with TemporaryDirectory() as directory:
            path = Path(directory) / "INNA - Deja Vu.mp3"
            ID3().save(path)
            with patch("playlist_extractor.CustomPrint.bold_print") as output:
                stats = backfill_playlist_lyrics(
                    directory, [(path.name, path.name, "2010")],
                    {path.name: "3V7choSVaP2zUqtIYpfynG"}, lambda: "session-token",
                    fetch=lambda *args: (spotify_api.LYRICS_AUTH_ERROR, "HTTP 403"),
                    sleep=lambda seconds: None,
                )
            self.assertEqual(stats["aborted"], 1)
            message = str(output.call_args_list)
            for value in ("HTTP 403", "3V7choSVaP2zUqtIYpfynG", "Zotify"):
                self.assertIn(value, message)
            self.assertNotIn("session-token", message)


if __name__ == "__main__":
    unittest.main()
