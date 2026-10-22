"""Disposable Spotify lyrics results shared across playlists; never credentials.

Open lazily, commit each result immediately, and tolerate an unusable cache.
SQLite transactions preserve concurrent playlist runs without lost JSON writes.
"""
from __future__ import annotations

from contextlib import closing
import os
from pathlib import Path
import sqlite3
import sys
import time

from spotify_api import LYRICS_OK, LYRICS_UNAVAILABLE


class LyricsCache:
    FOUND_TTL = 30 * 24 * 60 * 60
    UNAVAILABLE_TTL = 7 * 24 * 60 * 60

    def __init__(self, path=None, *, clock=time.time):
        if path is None:
            if sys.platform == "darwin":
                root = Path.home() / "Library" / "Caches"
            else:
                root = Path(os.getenv("XDG_CACHE_HOME") or Path.home() / ".cache")
            path = root / "zotify-music-manager" / "lyrics.sqlite3"
        self.path = Path(path)
        self.clock = clock
        self.failed = False

    def _connect(self):
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.path.touch(mode=0o600, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=1)
        try:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS lyrics "
                "(track_id TEXT PRIMARY KEY, status TEXT NOT NULL, "
                "text TEXT NOT NULL, expires_at REAL NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS cooldown (id INTEGER PRIMARY KEY, until REAL NOT NULL)"
            )
        except sqlite3.Error:
            connection.close()
            raise
        return connection

    def get(self, track_id):
        if self.failed:
            return None
        try:
            with closing(self._connect()) as connection:
                row = connection.execute(
                    "SELECT status, text FROM lyrics WHERE track_id = ? AND expires_at > ?",
                    (track_id, self.clock()),
                ).fetchone()
            if row and (row[0] == LYRICS_UNAVAILABLE or (row[0] == LYRICS_OK and row[1].strip())):
                return row
        except (OSError, sqlite3.Error):
            self.failed = True
        return None

    def put(self, track_id, status, text):
        if self.failed or status not in (LYRICS_OK, LYRICS_UNAVAILABLE):
            return
        if status == LYRICS_OK and not text.strip():
            return
        ttl = self.FOUND_TTL if status == LYRICS_OK else self.UNAVAILABLE_TTL
        now = self.clock()
        try:
            with closing(self._connect()) as connection, connection:
                connection.execute("DELETE FROM lyrics WHERE expires_at <= ?", (now,))
                connection.execute(
                    "INSERT INTO lyrics VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(track_id) DO UPDATE SET "
                    "status=excluded.status, text=excluded.text, expires_at=excluded.expires_at",
                    (track_id, status, text if status == LYRICS_OK else "", now + ttl),
                )
        except (OSError, sqlite3.Error):
            self.failed = True

    def cooldown_remaining(self):
        if self.failed:
            return 0.0
        try:
            with closing(self._connect()) as connection:
                row = connection.execute("SELECT until FROM cooldown WHERE id = 1").fetchone()
            return max(0.0, row[0] - self.clock()) if row else 0.0
        except (OSError, sqlite3.Error):
            self.failed = True
            return 0.0

    def defer(self, seconds):
        if self.failed:
            return
        try:
            with closing(self._connect()) as connection, connection:
                connection.execute(
                    "INSERT INTO cooldown VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET "
                    "until=MAX(cooldown.until, excluded.until)", (self.clock() + seconds,),
                )
        except (OSError, sqlite3.Error):
            self.failed = True
