# Changelog

Changes for the personal macOS Music Manager. This project does not currently
publish an independent Python package version; identify source by Git commit.

## Unreleased — macOS release preparation

### Added
- Guarded project-local `.venv` setup for macOS (`scripts/setup_macos.sh`), with
  interpreter selection, optional offline tests and explicit `--update` reuse.
- Setup orchestration safety tests and a macOS GitHub Actions offline-test job.
- Installation/rollback guide, manual `.zshrc` aliases/functions, Developer app
  key/browser-auth instructions, and an isolated-home acceptance procedure.
- Ignore rules for private environment files, OAuth caches, Python environments
  and generated library files.

### Changed
- README documents the real fixed home-relative library paths, mutating
  maintenance behavior, separate downloader venv and authorization boundaries.
- Setup clears inherited `PYTHONPATH`/`PYTHONHOME`; operational commands no longer
  rely on global packages or a `PYTHONPATH=.` workaround.

### Included local compatibility changes
These existing shared-workspace changes are included in the review branch rather
than silently discarded or attributed to documentation alone:
- Spotipy 2.26-compatible 50-item playlist pagination through server next links.
- Accept both current `item` and legacy `track` playlist response shapes in
  extraction and M3U8 output.
- Parse extended M3U8 data without treating metadata comments as song paths;
  resolve playlist-relative paths and flatten consolidated output correctly.
- Missing Zotify music roots are handled as no matching playlist during lookup.
- Focused offline regression tests and the legacy `bootstrap_test.sh` ZIP test
  helper; bounded runtime requirements including Spotipy 2.26+.

### Verification boundary
- Offline tests and fresh installs can be exercised without credentials.
- Native macOS installation, browser OAuth, live Spotify endpoint access and
  disposable real-audio maintenance remain separate Mac acceptance checks.
- No universal dry-run/library-root option, automatic Music.app/iPod sync,
  streaming authentication fix, or full dependency lockfile is claimed.
