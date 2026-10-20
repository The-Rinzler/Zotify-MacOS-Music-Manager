"""
spotify_api.py
================

This module centralizes all Spotify-related API interactions for the
Zotify MacOS Music Manager.  Previously the code to authenticate
against Spotify and fetch playlist details was duplicated in both
``playlist_extractor.py`` and ``consolidate_library.py``.  To adhere
to the DRY principle and improve maintainability, all Spotify
operations are exposed here as simple functions.  Each function
strives to preserve the behaviour of the original scripts without
altering any side effects or user interactions.

Functions
---------

``get_spotify_client``
    Authenticate and return a Spotipy client using environment
    variables for credentials.  A custom cache path or redirect URI
    may be provided to match the behaviour of the callers.

``extract_playlist_id``
    Parse a Spotify playlist URL and return the playlist ID.  This
    helper mirrors the inline helper that previously lived in
    ``playlist_extractor.py``.

``fetch_playlist_tracks``
    Retrieve all items from a Spotify playlist.  It follows Spotipy's
    pagination links, which is required because the current Spotify API
    limits playlist pages to 50 items.

Note
----

This module does not print or prompt the user.  Any user-facing
interactions remain in the scripts that import these functions.
"""

from __future__ import annotations

import os
import time
from typing import Callable, List, Optional, Tuple, Union
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Spotify's internal lyrics endpoint - the exact mechanism Zotify uses to fetch
# lyrics (see zotify/const.py: LYRICS_URL and zotify/api.py: fetch_lyrics).
# No external lyrics services (Genius/Musixmatch) are involved.
LYRICS_URL = "https://spclient.wg.spotify.com/color-lyrics/v2/track/"

# Result statuses returned by fetch_track_lyrics().
LYRICS_OK = "ok"
LYRICS_UNAVAILABLE = "unavailable"
LYRICS_AUTH_ERROR = "auth_error"
LYRICS_RATE_LIMITED = "rate_limited"
LYRICS_ERROR = "error"


def get_spotify_bearer_token(client: spotipy.Spotify) -> Optional[str]:
    """Return the current OAuth access token of a Spotipy client.

    Spotipy refreshes the cached user token transparently, so this reuses the
    same developer-app credentials the playlist fetch already authenticated
    with - mirroring Zotify's ``cls.OAUTH.token()`` branch in invoke_url().
    Returns ``None`` when no usable token is available (e.g. headless run
    without a token cache).
    """
    auth_manager = getattr(client, "auth_manager", None)
    if auth_manager is None:
        return None
    try:
        token_data = auth_manager.get_access_token(as_dict=False)
    except TypeError:  # older Spotipy signatures do not accept as_dict
        try:
            token_data = auth_manager.get_access_token()
        except Exception:
            return None
    except Exception:
        return None
    if isinstance(token_data, str):
        return token_data or None
    return (token_data or {}).get("access_token") or None


def parse_spotify_lyrics(payload: Optional[dict]) -> Tuple[bool, str]:
    """Extract plain-text lyrics from a color-lyrics/v2 JSON payload.

    Returns (found, text). Both UNSYNCED and LINE_SYNCED payloads are
    accepted; only the text of every line is kept, timestamps are dropped on
    purpose so the embedded USLT frame stays clean for Music.app/iPod (the
    playlist extractor additionally strips LRC timestamps from legacy tags).
    """
    if not isinstance(payload, dict):
        return False, ""
    lyrics = payload.get("lyrics")
    if not isinstance(lyrics, dict):
        return False, ""
    lines = lyrics.get("lines")
    if not isinstance(lines, list) or not lines:
        return False, ""
    text = "\n".join(
        str(line.get("words", "")) for line in lines if isinstance(line, dict)
    )
    if not text.strip():
        return False, ""
    return True, text


def fetch_track_lyrics(
    track_id: str,
    bearer_token: Union[str, Callable[[], str]],
    *,
    http_get: Optional[Callable] = None,
    sleep: Callable[[float], None] = time.sleep,
    retry_attempts: int = 3,
    timeout: float = 15.0,
) -> Tuple[str, str]:
    """Fetch Spotify lyrics for a track ID the way Zotify does.

    Calls ``color-lyrics/v2/track/{id}`` with the same headers Zotify's
    ``invoke_url()`` sends. Returns ``(status, text)`` where status is one of
    ``LYRICS_OK`` (text holds plain lyrics), ``LYRICS_UNAVAILABLE`` (track has
    no lyrics - normal, not every song does), ``LYRICS_AUTH_ERROR`` (token was
    rejected; retrying other tracks will not help), ``LYRICS_RATE_LIMITED`` or
    ``LYRICS_ERROR`` (transient; safe to retry later).
    """
    if http_get is None:
        import requests

        http_get = requests.get

    token = bearer_token() if callable(bearer_token) else bearer_token
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Accept-Language": "en",
        "app-platform": "WebPlayer",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) "
            "Gecko/20100101 Firefox/136.0"
        ),
    }

    backoffs = [2.0, 5.0, 10.0]
    for attempt in range(retry_attempts + 1):
        try:
            response = http_get(LYRICS_URL + track_id, headers=headers, timeout=timeout)
        except Exception:
            if attempt >= retry_attempts:
                return LYRICS_ERROR, ""
            sleep(backoffs[min(attempt, len(backoffs) - 1)])
            continue

        status_code = getattr(response, "status_code", 0)
        if status_code == 200:
            try:
                payload = response.json()
            except Exception:
                return LYRICS_ERROR, ""
            found, text = parse_spotify_lyrics(payload)
            if found:
                return LYRICS_OK, text
            # 200 with no usable payload, or empty/None lyrics object:
            # the track simply has no lyrics in Spotify's catalogue.
            return LYRICS_UNAVAILABLE, ""
        if status_code in (401, 403):
            return LYRICS_AUTH_ERROR, f"HTTP {status_code}"
        if status_code == 404:
            return LYRICS_UNAVAILABLE, ""
        if status_code == 429:
            if attempt >= retry_attempts:
                return LYRICS_RATE_LIMITED, ""
            retry_after = 0.0
            try:
                retry_after = float(dict(getattr(response, "headers", {}) or {}).get("Retry-After", 0))
            except (TypeError, ValueError):
                retry_after = 0.0
            sleep(max(retry_after, backoffs[min(attempt, len(backoffs) - 1)]))
            continue
        if 500 <= status_code < 600 and attempt < retry_attempts:
            sleep(backoffs[min(attempt, len(backoffs) - 1)])
            continue
        return LYRICS_ERROR, ""

    return LYRICS_ERROR, ""


def get_spotify_client(
    scope: str = "playlist-read-private",
    *,
    cache_path: Optional[str] = None,
    redirect_uri: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
) -> spotipy.Spotify:
    """Return an authenticated Spotipy client.

    Parameters
    ----------
    scope: str, optional
        A space-separated list of scopes for which the token should be valid.  Defaults to
        ``playlist-read-private`` which is sufficient for reading playlist metadata and
        items.
    cache_path: str or None, optional
        File system location where the OAuth token cache should be stored.  If not
        provided, the default Spotipy cache location (~/.cache-spotipy) will be used.
    redirect_uri: str or None, optional
        Redirect URI registered with your Spotify application.  If ``None``, the
        environment variable ``SPOTIPY_REDIRECT_URI`` is used when set.  This mirrors
        the original scripts which relied on environment variables.
    client_id: str or None, optional
        Spotify application client ID.  If ``None``, the environment variable
        ``SPOTIPY_CLIENT_ID`` is used.
    client_secret: str or None, optional
        Spotify application client secret.  If ``None``, the environment variable
        ``SPOTIPY_CLIENT_SECRET`` is used.

    Returns
    -------
    spotipy.Spotify
        An authenticated Spotify client.

    Notes
    -----
    The original scripts created a separate ``SpotifyOAuth`` instance for each script
    with subtly different cache paths and redirect URIs.  This function accepts
    optional overrides for those values to ensure that callers can replicate the
    original behaviour exactly.  If you need to supply a custom ``redirect_uri`` or
    ``cache_path``, pass them here.
    """
    # Fall back to environment variables when explicit parameters are not given.
    client_id = client_id or os.getenv("SPOTIPY_CLIENT_ID")
    client_secret = client_secret or os.getenv("SPOTIPY_CLIENT_SECRET")
    if redirect_uri is None:
        redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI")

    auth_manager = SpotifyOAuth(
        scope=scope,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        cache_path=cache_path,
    )
    return spotipy.Spotify(auth_manager=auth_manager)


def extract_playlist_id(playlist_url: str) -> str:
    """Extract the playlist ID from a full Spotify playlist URL.

    Parameters
    ----------
    playlist_url: str
        A Spotify playlist URL.  The portion after the final slash and before the
        optional query string is returned.

    Returns
    -------
    str
        The playlist ID.
    """
    return playlist_url.split("/")[-1].split("?")[0]


def fetch_playlist_tracks(
    sp: spotipy.Spotify, playlist_id_or_url: str
) -> List[dict]:
    """Return all playlist items, following Spotify's pagination links.

    The current API caps playlist pages at 50 items. Spotipy's ``next`` helper
    follows the server-provided next-page URL, avoiding assumptions about page
    length or offsets. The function accepts either a playlist ID or full URL.

    Parameters
    ----------
    sp: spotipy.Spotify
        An authenticated Spotify client.
    playlist_id_or_url: str
        Either a plain playlist ID or a Spotify playlist URL.

    Returns
    -------
    List[dict]
        A list of track dictionaries as returned by Spotipy.
    """
    # Accept both IDs and full URLs for convenience.
    playlist_id = extract_playlist_id(playlist_id_or_url)

    tracks: List[dict] = []
    response = sp.playlist_items(playlist_id, limit=50, offset=0)
    while response:
        tracks.extend(response.get("items") or [])
        if not response.get("next"):
            break
        response = sp.next(response)
    return tracks