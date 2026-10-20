# macOS installation, shell setup and safe acceptance

See the [README](README.md) for behavior, fixed library paths and limitations.
Keep Zotify and this manager in **different venvs**. Nothing here requires changing
your existing `~/venvs/playlist-tools`, Zotify venv, production cache or library.

## 1. Prerequisites and a separate checkout

Install Apple's Command Line Tools (`xcode-select --install`) if Git/Homebrew
requires them. Install [Homebrew](https://brew.sh/) from its official instructions,
then:

```zsh
brew install python@3.11 git && \
  git clone --branch main https://github.com/The-Rinzler/Zotify-MacOS-Music-Manager.git "$HOME/Zotify-MacOS-Music-Manager" && \
  cd "$HOME/Zotify-MacOS-Music-Manager" && \
  git branch --show-current && \
  git rev-parse HEAD && \
  PYTHON="$(brew --prefix python@3.11)/bin/python3.11" bash scripts/setup_macos.sh --test
```

The active branch is `main`; `release/macos-compat-setup` is no longer available.
If cloning fails, stop instead of continuing with the later `cd` or setup
commands. Use a new empty path if the destination already exists. Homebrew's
prefix is discovered instead of assuming Intel `/usr/local` or Apple Silicon
`/opt/homebrew`.

The macOS-only script resolves its own checkout root, creates `.venv`, upgrades
its packaging tools, installs the declared requirements, runs `pip check` and
import/version checks, and optionally runs offline `unittest` tests. It supports
macOS's Bash 3.2. Neither activation nor `sudo pip` is needed. It never runs
interactive library maintenance, installs Homebrew, edits shell files or handles
your Spotify credentials. No Redis server is required for this project's normal
file-backed OAuth cache; `redis` is a Python dependency.

If `.venv` exists, setup stops. After reviewing source/dependency changes,
`bash scripts/setup_macos.sh --update --test` permits updating **only** an
environment created by this script in this same checkout. Symlinks, unknown venvs
and moved checkouts are refused. Preserve an old environment rather than deleting
it to work around the guard; use a new checkout instead. `--update` is not
`git pull`, and dependency ranges are not a reproducible full lockfile.

## Updating an existing checkout

First confirm the checkout is clean and `origin` is the manager repository. Stop
if there are local changes or if the remote URL is not the expected one:

```zsh
cd "$HOME/Zotify-MacOS-Music-Manager" && \
  git status --short --branch && \
  git remote get-url origin
```

After confirming a clean tree and remote
`https://github.com/The-Rinzler/Zotify-MacOS-Music-Manager.git`, update the
checkout and then its setup-managed venv:

```zsh
git fetch origin && \
  git switch main && \
  git pull --ff-only origin main && \
  PYTHON="$(brew --prefix python@3.11)/bin/python3.11" bash scripts/setup_macos.sh --update --test
```

The Git commands update source; the setup command reinstalls requirements and
runs the offline tests. Neither operation migrates OAuth tokens or changes music
files. If setup refuses the venv, use a separate empty checkout rather than
removing the setup guard.

## 2. Verify without touching accounts or music

```zsh
cd "$HOME/Zotify-MacOS-Music-Manager"
env -u PYTHONPATH -u PYTHONHOME .venv/bin/python -c 'import sys; from importlib.metadata import version; print(sys.executable); print(version("spotipy"))'
env -u PYTHONPATH -u PYTHONHOME .venv/bin/python -m pip check
env -u PYTHONPATH -u PYTHONHOME .venv/bin/python -m unittest discover -s tests -v
```

Do not run `consolidate_library.py --help`: it has no argparse help/dry-run
interface and invoking it starts the real maintenance flow. The offline tests
are the safe noninteractive check. `playlist_extractor.py --help` is available.

An older downloaded test ZIP may include `bootstrap_test.sh`: it creates/reuses
`.venv-test` and runs the offline suite. That legacy script is **not** the new
guarded production setup and does not share its environment-preservation guard.
It is optional; use a disposable checkout if exercising it. Do not point a
production alias at `.venv-test`.

## 3. Authentication

Follow [docs/AUTHENTICATION.md](docs/AUTHENTICATION.md). Credentials must be in the
shell environment; a `.env` file is **not automatically loaded** by this project.
Avoid putting a Client Secret in `.zshrc` or shell history. Both interactive
scripts normally use `~/.cache-spotipy`; a Zotify `credentials.json` is not valid
here.

## 4. Add `.zshrc` commands manually

Inspect existing commands first:

```zsh
type -a playlist_extractor consolidate_library
cp -p "$HOME/.zshrc" "$HOME/.zshrc.before-zotify-manager"
```

The backup command assumes `.zshrc` exists and the backup path is unused; if not,
create the profile or choose a new backup name deliberately. Add the aliases from
the README, **or** use these functions if you want to forward arguments:

```zsh
playlist_extractor() {
  (cd "$HOME/Zotify-MacOS-Music-Manager" &&
    env -u PYTHONPATH -u PYTHONHOME .venv/bin/python playlist_extractor.py "$@")
}
consolidate_library() {
  (cd "$HOME/Zotify-MacOS-Music-Manager" &&
    env -u PYTHONPATH -u PYTHONHOME .venv/bin/python consolidate_library.py)
}
```

Choose aliases or functions, not both under the same names. Remove/rename an old
alias before defining a same-named function (`unalias playlist_extractor` only if
that alias exists). Reload with `source ~/.zshrc`, inspect `type -a` again, then
`playlist_extractor --help` if using the function. Do **not** run consolidation to
verify shell wiring. Keep secrets out of these definitions.

## Isolated-home acceptance

The scripts have fixed home-relative library and cache paths, not a
`--root-path` flag. Use an isolated `HOME` to test the actual entry points without
pointing them at the real library. Resolve the checkout and interpreter **before**
overriding HOME, and do not use the production aliases for this test.

```zsh
MANAGER="$HOME/Zotify-MacOS-Music-Manager"
TEST_HOME="$(mktemp -d "$HOME/manager-smoke.XXXXXX")"
chmod 700 "$TEST_HOME"
mkdir -p "$TEST_HOME/Music/Zotify Music" \
  "$TEST_HOME/Music/Local Files" "$TEST_HOME/Music/Consolidated Library"
# After following the auth guide in this shell, choose one small authorized playlist:
env -u PYTHONPATH -u PYTHONHOME HOME="$TEST_HOME" \
  "$MANAGER/.venv/bin/python" "$MANAGER/playlist_extractor.py" \
  'https://open.spotify.com/playlist/PLAYLIST_ID'
```

The browser and CLI should be on the same Mac. HOME controls both `Path.home()`
and `expanduser()` in the scripts, so test caches and library files stay under
`$TEST_HOME`. Environment credentials remain inherited; do not include them in
logs. Never copy a production token merely to skip fresh test consent.

1. Inspect the extracted M3U8/hidden link and five-column `.song_ids` behavior.
   Metadata-only playlist extraction is still a filesystem-mutating command.
2. If testing with Zotify, use its own venv and **all explicit test config,
   Login5/API credential, archive and library paths**, setting its test music
   root to `$TEST_HOME/Music/Zotify Music`. Do not invoke an old wrapper that
   drops these flags. Authenticate separately; download only one small authorized
   resource. See the downloader's isolated smoke guide.
3. Copy expendable audio into the test `Local Files` directory if desired. Never
   use symlinks to the production library: metadata/renaming operations can affect
   real data through them.
4. Inspect before/after files, then deliberately exercise consolidation:

   ```zsh
   env -u PYTHONPATH -u PYTHONHOME HOME="$TEST_HOME" \
     "$MANAGER/.venv/bin/python" "$MANAGER/consolidate_library.py"
   ```

5. Review each maintenance/deletion prompt; confirm the consolidated tracks and
   M3U8 paths work, playlist originals remain in their folders, and duplicate
   playlist membership still has physical copies. Declining copying does not
   disable other automatic maintenance. Inspect embedded metadata/artwork too.
6. Record the Git SHA, Python/Spotipy version and redacted outcome before
   considering production cutover. Offline success is not live API proof.

Do not delete the test home automatically: it may contain private OAuth data.
Keep it private while diagnosing failures and remove it deliberately when done.

## 5. Production cutover and rollback

Make a separate library/config/archive backup and retain the old venv and shell
commands. Ensure the downloader and manager agree on `~/Music/Zotify Music` and
five-column directory archives before using the new commands. Run one small
playlist before any whole-library maintenance; verify results in Music.app before
device sync. There is no promise that code rollback can undo file mutations.

To roll back shell commands, restore the backed-up `.zshrc` or manually restore
its previous definitions, reload it and inspect `type -a`. Use the old venv path
explicitly. Preserve new test caches separately from old production ones. Restore
library files from your backup if necessary; reinstalling Python does not restore
renamed/deleted files or rewritten tags.
