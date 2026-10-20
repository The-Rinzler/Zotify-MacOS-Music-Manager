"""Read-only bridge to Zotify's Login5 token provider in its own environment.

The helper returns a short-lived access token over a captured pipe only. No
credentials are copied, no browser is opened, and no config/cache is written.
Run by the manager with Zotify's Python, not with its command-line downloader.
"""
from __future__ import annotations

import json
import logging
import math
import os
from pathlib import Path
import subprocess
import sys
import time
from contextlib import redirect_stderr, redirect_stdout


class ZotifyAuthError(RuntimeError):
    """Safe, actionable failure without raw library logs or credential data."""


ERROR_MESSAGES = {
    "credentials_missing": "Zotify saved session credentials are missing; sign in with Zotify first.",
    "credentials_invalid": "Zotify saved Login5 credentials are invalid; sign in with Zotify again.",
    "config_invalid": "Unable to read Zotify configuration; check --zotify-config-location.",
    "dependency_missing": "Zotify imports failed; select its installed v0.21.0 environment with --zotify-python.",
    "session_failed": "Zotify session/token acquisition failed; verify sign in with Zotify itself.",
}


def resolve_zotify_python(python=None) -> Path:
    """Use an explicit interpreter or a sibling/home Zotify project's venv."""
    explicit = python or os.getenv("ZOTIFY_PYTHON")
    if explicit:
        candidates = [Path(explicit).expanduser()]
    else:
        parent = Path(__file__).resolve().parent.parent
        candidates = [
            root / ".venv/bin/python"
            for root in (parent / "zotify", parent / "Zotify", Path.home() / "zotify", Path.home() / "Zotify")
        ]
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            # Do not resolve the venv symlink: doing so selects the base Python.
            return candidate.absolute()
    raise ZotifyAuthError(
        "Zotify Python not found; set --zotify-python to your Zotify .venv/bin/python."
    )


class ZotifyTokenProvider:
    """Lazily obtain and briefly cache a Login5 token (in memory only)."""

    def __init__(self, python=None, config_location=None, credentials_location=None):
        self.python = python
        self.config_location = config_location or os.getenv("ZOTIFY_CONFIG_LOCATION")
        self.credentials_location = credentials_location or os.getenv("ZOTIFY_CREDENTIALS_LOCATION")
        self._token = None
        self._deadline = 0.0

    def __call__(self) -> str:
        if self._token and time.monotonic() < self._deadline:
            return self._token
        command = [str(resolve_zotify_python(self.python)), "-I", str(Path(__file__).resolve())]
        for flag, value in (("--config-location", self.config_location),
                            ("--credentials-location", self.credentials_location)):
            if value:
                command.extend([flag, str(Path(value).expanduser().absolute())])
        environment = os.environ.copy()
        environment.pop("PYTHONPATH", None)
        environment.pop("PYTHONHOME", None)
        started = time.monotonic()
        try:
            result = subprocess.run(
                command, stdin=subprocess.DEVNULL, capture_output=True, text=True,
                timeout=60, env=environment,
            )
        except subprocess.TimeoutExpired:
            raise ZotifyAuthError("Zotify session/token acquisition timed out after 60 seconds.") from None
        except OSError:
            raise ZotifyAuthError("Unable to launch Zotify Python; check --zotify-python.") from None
        try:
            payload = json.loads(result.stdout)
            if result.returncode or payload.get("error"):
                raise ZotifyAuthError(ERROR_MESSAGES.get(payload.get("error"), ERROR_MESSAGES["session_failed"]))
            token = payload["access_token"]
            expires_in = float(payload["expires_in"])
            if not isinstance(token, str) or not token.strip() or not math.isfinite(expires_in) or expires_in <= 30:
                raise ValueError("invalid token response")
        except (ValueError, TypeError, KeyError, AttributeError):
            raise ZotifyAuthError(ERROR_MESSAGES["session_failed"]) from None
        self._token = token
        # Conservative cache; re-open the saved session when needed on long runs.
        self._deadline = started + min(expires_in - 30, 300)
        return token


def _as_file(path: Path, filename: str) -> Path:
    """Zotify ensure_is_file resolution, without its mkdir/touch side effects."""
    if path.is_file():
        return path
    return path / filename if path.is_dir() or not path.suffix else path


def saved_credentials_path(default_path: Path, config_location=None, credentials_location=None) -> Path:
    """Resolve v0.21.0's session config/credential locations without writing."""
    config = {}
    config_path = _as_file(Path(config_location).expanduser() if config_location else default_path, "config.json")
    if config_path.is_file():
        try:
            config = json.loads(config_path.read_text(encoding="utf-8"))
            if not isinstance(config, dict):
                raise ValueError("not an object")
        except (OSError, ValueError):
            raise ZotifyAuthError("config_invalid") from None
    elif config_location:
        raise ZotifyAuthError("config_invalid")
    location = credentials_location or config.get("CREDENTIALS_LOCATION") or ""
    try:
        if not location:
            path = default_path
        elif not credentials_location and str(location).startswith("."):
            root = Path(config.get("ROOT_PATH") or "~/Music/Zotify Music").expanduser()
            path = root / Path(location).relative_to(".")
        else:
            path = Path(location).expanduser()
        return _as_file(path, "credentials.json")
    except (TypeError, ValueError):
        raise ZotifyAuthError("config_invalid") from None


def session_token(config_location=None, credentials_location=None) -> dict:
    """Use the same saved-session login and token provider as Zotify v0.21.0.

    Deliberately bypass login()/boot(): those may prompt, fall back to OAuth,
    save credentials, create config/log/music files or start downloading.
    """
    try:
        from zotify.config import Config, LoginHandler
        from zotify.const import SCOPES
        from librespot.core import OAuth, Session
    except (ImportError, AttributeError):
        raise ZotifyAuthError("dependency_missing") from None
    path = saved_credentials_path(Config._default_path(), config_location, credentials_location)
    if not path.is_file():
        raise ZotifyAuthError("credentials_missing")
    try:
        credentials = json.loads(path.read_text(encoding="utf-8"))
        if (not isinstance(credentials, dict) or credentials.get("type") not in LoginHandler.CRED_TYPES
                or credentials.get("type") == OAuth.OAUTH_PKCE_TOKEN):
            raise ValueError("Login5 credentials required")
    except (OSError, ValueError):
        raise ZotifyAuthError("credentials_invalid") from None
    session = None
    try:
        LoginHandler.SESSION_BUILDER = Session.Builder()
        LoginHandler.SESSION_BUILDER.conf.store_credentials = False
        LoginHandler.login5_cred_login(credentials)
        session = LoginHandler.SESSION
        # Same provider as LoginHandler.choose_token(force_login5=True), retaining
        # expiry so the caller never caches a bearer beyond its lifetime.
        token = session.tokens().get_token(*SCOPES)
        return {"access_token": token.access_token, "expires_in": token.expires_in}
    except Exception:
        raise ZotifyAuthError("session_failed") from None
    finally:
        if session is not None:
            session.close()


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-location")
    parser.add_argument("--credentials-location")
    args = parser.parse_args()
    # Dependency diagnostics can contain credentials. Suppress rather than echo
    # their stdout/stderr; only our allowlisted error codes leave this process.
    logging.disable(logging.CRITICAL)
    try:
        with open(os.devnull, "w") as sink, redirect_stdout(sink), redirect_stderr(sink):
            payload = session_token(args.config_location, args.credentials_location)
        code = 0
    except ZotifyAuthError as error:
        payload = {"error": str(error) if str(error) in ERROR_MESSAGES else "session_failed"}
        code = 1
    except Exception:
        payload = {"error": "session_failed"}
        code = 1
    print(json.dumps(payload))
    return code


if __name__ == "__main__":
    sys.exit(main())
