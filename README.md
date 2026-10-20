# Zotify macOS Music Manager

Playlist extraction and library maintenance for **macOS, Music.app and iPod**.
Companion to [The-Rinzler's Zotify fork](https://github.com/The-Rinzler/zotify/tree/compat/macos-ipod-v0.21.0).
This project manages files and Spotify playlist metadata; **Zotify is the separate
music downloader**. It is not an Apple or Spotify product. Use only content you
are authorized to access and follow the services' terms.

[Installation and aliases](INSTALLATION.md) ·
[Spotify app / authentication guide](docs/AUTHENTICATION.md) ·
[Changelog](CHANGELOG.md)

## What it does

- `playlist_extractor.py` reads a Spotify playlist, writes M3U8/hidden playlist
  links, reconciles audio with five-column `.song_ids` records, strips LRC
  timestamps from embedded MP3 lyrics, and reports tracks without lyric tags.
  It does not fetch lyrics; `.lrc` sidecars are left unchanged. ID3v2.2 files
  are reported and left untouched to preserve their tag version.
- `consolidate_library.py` refreshes playlist information, reconciles duplicates,
  copies tracks into a flat Consolidated Library, reports missing/orphaned tracks,
  offers cleanup, and resizes embedded artwork.
- Supports Spotipy 2.26's current playlist `item` shape and the legacy `track`
  shape, follows pagination beyond 50 items, and understands Zotify's extended
  M3U8 metadata and playlist-relative paths.
- Preserves the shared flat `First Artist - Title.mp3` naming contract. The
  downloader keeps physical copies for overlapping playlist membership; ordinary
  consolidation copies into its separate library rather than moving originals.

**These are maintenance tools, not read-only viewers.** They can rename files and
folders, rewrite playlists/archives and audio metadata, and offer deletion of
leftovers. Back up your library and test on disposable copies before production
use. Declining one prompt is not a general dry-run mode. This release does not
add a universal `--dry-run` or a library-root CLI option.

## Quick setup (macOS only)

Install [Homebrew](https://brew.sh/) if needed, then:

```zsh
brew install python@3.11 git
git clone --branch release/macos-compat-setup https://github.com/The-Rinzler/Zotify-MacOS-Music-Manager.git "$HOME/Zotify-MacOS-Music-Manager"
cd "$HOME/Zotify-MacOS-Music-Manager"
PYTHON="$(brew --prefix python@3.11)/bin/python3.11" bash scripts/setup_macos.sh --test
```

Use a new clone path if the directory exists. This command selects the release
review branch; the default branch may not contain this setup until its PR merges.
Zotify's own venv and FFmpeg setup are separate—do not install both projects into
one production venv.

The script creates this checkout's `.venv`, installs `requirements.txt`, checks
dependencies and imports, and optionally runs offline tests. It clears inherited
`PYTHONPATH`/`PYTHONHOME`. It **does not** open Spotify, request keys, run the
interactive tools, edit `.zshrc`, or migrate existing venvs or music. Python 3.11
is recommended; code requires 3.10+. Dependencies are bounded, not fully locked.

Existing `.venv`? Setup stops without changing it. `--update` reuses only a venv
created by this setup script in this exact checkout; it does not pull Git updates.
See [INSTALLATION.md](INSTALLATION.md) for the older `.venv-test` bootstrap and
rollback.

## Spotify authentication

Create your own app in the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard).
The manager needs **Client ID and Client Secret** and the exact registered
redirect `http://127.0.0.1:9090`. Set `SPOTIPY_CLIENT_ID`,
`SPOTIPY_CLIENT_SECRET` and `SPOTIPY_REDIRECT_URI` in the invoking shell.
The scripts request `playlist-read-private` and normally share the local token
cache `~/.cache-spotipy`.

Follow [the full key/authentication guide](docs/AUTHENTICATION.md) for safe secret
entry, optional private config storage, browser consent and isolated cache tests.
A developer app does not replace Zotify's Login5 authentication. Don't share the
two projects' credential files. No keys or live account are needed for setup or
unit tests.

## Library locations and first run

The interactive scripts currently use these **fixed home-relative paths**:

| Path | Purpose |
| --- | --- |
| `~/Music/Zotify Music` | Zotify's per-playlist library |
| `~/Music/Local Files` | Additional local audio for consolidation |
| `~/Music/Consolidated Library` | Flat output library and playlists |
| `~/.cache-spotipy` | Spotify OAuth cache, outside the repository |

Match the downloader's production `ROOT_PATH` to `~/Music/Zotify Music` when
cutting over. An arbitrary Zotify `--root-path` does not automatically reconfigure
this manager. Complete the [isolated-home acceptance procedure](INSTALLATION.md#isolated-home-acceptance)
before running the following against your real home:

```zsh
cd "$HOME/Zotify-MacOS-Music-Manager"
env -u PYTHONPATH -u PYTHONHOME .venv/bin/python playlist_extractor.py 'https://open.spotify.com/playlist/PLAYLIST_ID'
# Run Zotify separately in its own venv to fetch authorized missing audio.
env -u PYTHONPATH -u PYTHONHOME .venv/bin/python consolidate_library.py
```

Replace `PLAYLIST_ID`. Extraction can reuse already-downloaded tracks and reconcile
archives; inspect prompts carefully. Consolidation can perform maintenance even
if you decline Spotify refresh or copying. Do not run it on your only backup.
Importing playlists into Music.app or syncing an iPod is a separate manual step;
this project does not promise automatic device sync.

## Optional `.zshrc` aliases

Back up `.zshrc`, inspect `type -a playlist_extractor consolidate_library`, then add
these manually (adjust paths if you cloned elsewhere):

```zsh
alias playlist_extractor='(cd "$HOME/Zotify-MacOS-Music-Manager" && env -u PYTHONPATH -u PYTHONHOME .venv/bin/python playlist_extractor.py)'
alias consolidate_library='(cd "$HOME/Zotify-MacOS-Music-Manager" && env -u PYTHONPATH -u PYTHONHOME .venv/bin/python consolidate_library.py)'
```

`source ~/.zshrc`, then `type -a playlist_extractor consolidate_library`.
These aliases use the exact project venv without activating it or leaking global
Python packages. Use `playlist_extractor` with its interactive URL prompt; the
[installation guide](INSTALLATION.md) includes a function for forwarding arguments.
Do not invoke the maintenance alias merely to test that an alias exists.

## Offline verification

```zsh
cd "$HOME/Zotify-MacOS-Music-Manager"
env -u PYTHONPATH -u PYTHONHOME .venv/bin/python -m unittest discover -s tests -v
```

Tests use synthetic data and disposable directories. Setup safety tests simulate
macOS tool orchestration; they do not prove native macOS installation or live
Spotify authorization. A macOS GitHub Actions workflow runs the real setup and
offline suite after publication, without account secrets. Live authorization,
Spotify endpoint access and real-library behavior remain separate Mac checks.

## Troubleshooting

- **Wrong Python/imports:** use the full `.venv/bin/python` path and unset
  `PYTHONPATH`/`PYTHONHOME`; don't use global `pip` or `PYTHONPATH=.`.
- **Missing music directory:** create the intended test folders or check the
  fixed paths before production. A missing source root is not an API failure.
- **Invalid client / redirect mismatch / 401 / 403:** consult the auth guide;
  check app credentials, exact redirect, authorized account and Dashboard limits.
- **HTTP 429:** rate limited, not an empty playlist. Stop and honor retry/backoff;
  do not repeatedly request a large library.
- **Missing tracks:** an M3U8 entry is not proof the audio exists. Run the
  downloader deliberately, then reconcile on a backed-up library.

## Credits and licensing

Maintained in [The-Rinzler/Zotify-MacOS-Music-Manager](https://github.com/The-Rinzler/Zotify-MacOS-Music-Manager).
Uses Spotipy, Mutagen, Requests, Pillow and Tabulate. See the repository's
[GNU General Public License v3.0](LICENSE). This separate project's license is
not Zotify's Unlicense.
