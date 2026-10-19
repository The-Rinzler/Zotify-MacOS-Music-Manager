#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/requirements.txt" ] && [ -f "$SCRIPT_DIR/spotify_api.py" ] && [ -d "$SCRIPT_DIR/tests" ]; then
    ROOT_DIR="$SCRIPT_DIR"
elif [ -f "$PWD/requirements.txt" ] && [ -f "$PWD/spotify_api.py" ] && [ -d "$PWD/tests" ]; then
    # Also allow invoking a separately downloaded copy while already in the repo.
    ROOT_DIR="$PWD"
else
    printf 'ERROR: Run this script from the project folder, or keep it at the project root beside requirements.txt and tests/.\n' >&2
    exit 1
fi
cd "$ROOT_DIR"

# Prevent Homebrew/system site-packages leaking into this isolated environment.
unset PYTHONPATH PYTHONHOME
export PYTHONNOUSERSITE=1

PYTHON_BIN="${PYTHON:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    printf 'ERROR: %s was not found. Install Python 3.10–3.13 or set PYTHON=/path/to/python3.11.\n' "$PYTHON_BIN" >&2
    exit 1
fi

PYTHON_VERSION="$("$PYTHON_BIN" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
case "$PYTHON_VERSION" in
    3.10|3.11|3.12|3.13) ;;
    *)
        printf 'ERROR: Python 3.10–3.13 is required; found %s. Set PYTHON to a supported interpreter.\n' "$PYTHON_VERSION" >&2
        exit 1
        ;;
esac

VENV_DIR="${VENV_DIR:-.venv-test}"
case "$VENV_DIR" in
    /*) ;;
    *) VENV_DIR="$ROOT_DIR/$VENV_DIR" ;;
esac
VENV_PYTHON="$VENV_DIR/bin/python"

if [ ! -x "$VENV_PYTHON" ]; then
    if [ -e "$VENV_DIR" ]; then
        printf 'ERROR: %s exists but is not a usable virtual environment. Remove it or set VENV_DIR to another path.\n' "$VENV_DIR" >&2
        exit 1
    fi
    printf 'Creating isolated virtual environment: %s\n' "$VENV_DIR"
    if ! "$PYTHON_BIN" -m venv "$VENV_DIR"; then
        # Remove only the incomplete environment created by this invocation.
        rm -rf "$VENV_DIR"
        printf 'ERROR: Could not create the virtual environment. On Debian/Ubuntu, install the matching python3-venv package; on macOS, use a full Python.org or Homebrew installation.\n' >&2
        exit 1
    fi
fi

VENV_VERSION="$("$VENV_PYTHON" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
case "$VENV_VERSION" in
    3.10|3.11|3.12|3.13) ;;
    *)
        printf 'ERROR: the virtual environment uses unsupported Python %s. Remove it or set VENV_DIR to a new path.\n' "$VENV_VERSION" >&2
        exit 1
        ;;
esac

printf 'Using Python %s (%s)\n' "$VENV_VERSION" "$VENV_PYTHON"
printf 'Installing project dependencies from requirements.txt...\n'
"$VENV_PYTHON" -m pip install --upgrade pip wheel
"$VENV_PYTHON" -m pip install -r "$ROOT_DIR/requirements.txt"

printf '\nInstalled Spotipy: '
"$VENV_PYTHON" -c 'from importlib.metadata import version; print(version("spotipy"))'
printf '\nRunning offline unit tests...\n'
"$VENV_PYTHON" -m unittest discover -s "$ROOT_DIR/tests" -v

printf '\nAll unit tests passed. No Spotify credentials or network API calls were needed.\n'
