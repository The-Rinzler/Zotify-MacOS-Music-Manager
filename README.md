# Zotify-macOS Music Manager

Works **with** my Zotify fork:  
<https://github.com/The-Rinzler/zotify/tree/compat/macos-ipod>  


**Only tested for managing and downloading playlists.**


Workflow: you run these tools to build/maintain playlists and `.song_ids`, then run Zotify to download, then re-run `consolidate_library.py` to reconcile.


## 1) What this project does

- `playlist_extractor.py`: reads a Spotify playlist and updates your local playlist folder, `.m3u8`, and `.song_ids`.
- `consolidate_library.py`: cleans old entries, fixes names, reconciles `.song_ids`, and reports drift.


## 2) Prerequisites (fresh macOS)

- Xcode Command Line Tools
- Homebrew
- Python 3.10–3.12 installed and on `PATH`  
_Ensure those exist._


## 3) Clone

```bash
git clone https://github.com/The-Rinzler/Zotify-MacOS-Music-Manager.git ~/Zotify-MacOS-Music-Manager
cd ~/Zotify-MacOS-Music-Manager
```

## 4) Virtual environment

```bash
python3 -m venv ~/venvs/playlist-tools
~/venvs/playlist-tools/bin/python -m pip install -U pip wheel
# minimal deps
~/venvs/playlist-tools/bin/python -m pip install spotipy mutagen requests tabulate Pillow
```


## 5) Install requirements from requirements.txt

```bash
python -m pip install -r requirements.txt
```


## 6) Spotify API credentials

**6.1** Create an app in the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard).

**6.2** Copy **Client ID** and **Client Secret**.  

**6.3** Redirect URI must be exactly: `http://127.0.0.1:9090` _(the code expects this)_.  

**6.4** Add environment variables to your shell profile:
- **zsh** (default on modern macOS): add to `~/.zshrc`
- **bash**: add to `~/.bash_profile` or `~/.bashrc`
```bash
export SPOTIPY_CLIENT_ID="your_client_id"
export SPOTIPY_CLIENT_SECRET="your_client_secret"
export SPOTIPY_REDIRECT_URI="http://127.0.0.1:9090"
```
Apply changes:
```bash
# zsh
source ~/.zshrc
# bash
source ~/.bash_profile  # or ~/.bashrc
```

## 7) Run

From the repo root:
```bash
cd ~/Zotify-MacOS-Music-Manager
PYTHONPATH=. ~/venvs/playlist-tools/bin/python playlist_extractor.py
PYTHONPATH=. ~/venvs/playlist-tools/bin/python consolidate_library.py
```

## 8) Using with Zotify (with fork)

Run Zotify (separately, in its own venv if you prefer) **after** `playlist_extractor.py` finishes, then re-run `consolidate_library.py` to reconcile any new downloads.

## 9) Optional aliases

Place these **in the same shell profile** you edited above (`~/.zshrc` for zsh, or your bash profile). They:
- `cd` into the repo,
- run the scripts with the venv’s Python (no need to “activate”),
- keep local imports working via `PYTHONPATH=.`,
- effectively “execute the venv and run the command” in one step.
```bash
alias playlist_extractor='(cd "$HOME/Zotify-MacOS-Music-Manager" && PYTHONPATH=. "$HOME/venvs/playlist-tools/bin/python" playlist_extractor.py)'
alias consolidate_library='(cd "$HOME/Zotify-MacOS-Music-Manager" && PYTHONPATH=. "$HOME/venvs/playlist-tools/bin/python" consolidate_library.py)'
```
Reload your shell, then run:
```bash
playlist_extractor
consolidate_library
```

## 10) Common pitfalls on a fresh install
- Missing env vars or wrong redirect URI → Spotipy `invalid_client`.
- Running outside the repo without `PYTHONPATH=.` → `ModuleNotFoundError` for local modules.
- Mixing system Python and venv Python → missing packages. Use the full venv path shown above.
