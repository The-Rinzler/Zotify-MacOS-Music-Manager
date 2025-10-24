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
    Retrieve all items from a Spotify playlist.  Spotipy limits
    playlist queries to 100 items per request; this helper handles
    pagination transparently and matches the logic that existed in
    multiple places in the original code base.

Note
----

This module does not print or prompt the user.  Any user-facing
interactions remain in the scripts that import these functions.
"""

from __future__ import annotations

import os
from typing import List, Optional, Union
import spotipy
from spotipy.oauth2 import SpotifyOAuth


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
    """Return all track items from a playlist.

    Spotify's API returns playlists in pages of up to 100 items.  This function
    retrieves each page sequentially until all items have been fetched.  It
    deliberately mirrors the logic present in the original scripts to avoid any
    behavioural differences.  The function accepts either a playlist ID or a full
    playlist URL; in the latter case the ID is extracted internally.

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
    offset = 0
    while True:
        response = sp.playlist_items(playlist_id, offset=offset)
        items = response.get("items") or []
        tracks.extend(items)
        # Stop when fewer than 100 items are returned; this matches Spotipy behaviour.
        if len(items) < 100:
            break
        offset += len(items)
    return tracks