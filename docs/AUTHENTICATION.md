# Spotify Developer app and browser authentication (macOS)

The manager uses Spotipy's Authorization Code flow for **playlist metadata**. It
needs your own **Client ID + Client Secret**, an exact redirect registration, and
consent from the Spotify account whose playlists you use. It requests
`playlist-read-private` and does not download audio.

Lyrics backfill is authenticated separately: a read-only helper uses **Zotify
v0.21.0's saved Login5 session in Zotify's own Python environment**, matching
Zotify's forced-Login5 lyrics path. It does not send the playlist-only Spotipy
bearer, copy credential files, or write Zotify config/cache/logs. Sign in using
Zotify itself first. An OAuth-only Zotify credential file is not a Login5 session
and is reported rather than used as a fallback. See
[lyrics authentication](../README.md#lyrics-authentication-separate-from-playlist-oauth)
for interpreter discovery and custom config/credential path overrides.

The only lyrics endpoint used is Spotify's undocumented
`spclient.wg.spotify.com/color-lyrics/v2/track/{id}`; no external lyrics service is
used. This is not part of the supported Web API, so playlist access does not
guarantee lyrics access. A 401/403 reports the exact status and track and aborts
backfill for that run; `--no-lyrics-backfill` disables it. Live authorization must
be verified separately from offline tests.

## 1. Create or configure your app

1. Open [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
   and sign in yourself.
2. Create a personal app, fill the required app/website/description fields, and
   select Web API where offered. Dashboard labels and access policies can change.
3. In settings, register this exact redirect and save:
   **`http://127.0.0.1:9090`**.
4. Copy the app's **Client ID**. Reveal/copy its **Client Secret** privately.
   Do not paste either into issues/screenshots; the Client Secret is sensitive.
5. For development-mode apps, add the intended Spotify account to the permitted
   users where the Dashboard requires it. Current account/app/endpoint restrictions
   still apply; creating an app does not guarantee unrestricted playlist access.

If sharing one app with Zotify's optional Developer API, register **both** this
redirect and Zotify's separate `http://127.0.0.1:4381/login`. Don't replace one
with the other. Zotify PKCE does not use the manager's Client Secret and Login5
remains a separate session.

## 2. Supply credentials without shell-history leaks

Run this in **macOS zsh** (not bash). `read` collects input rather than putting
real values into command history; `-s` hides the Client Secret:

```zsh
read -r 'SPOTIPY_CLIENT_ID?Spotify app Client ID: '
read -rs 'SPOTIPY_CLIENT_SECRET?Spotify app Client Secret (hidden): '
printf '\n'
export SPOTIPY_CLIENT_ID SPOTIPY_CLIENT_SECRET
export SPOTIPY_REDIRECT_URI='http://127.0.0.1:9090'
```

Keep the session local and trusted. Environment variables are not an encrypted
secret store and may be accessible to processes with sufficient privileges. Do
not print `env`, use shell tracing (`set -x`), or include secret-bearing logs.
To clear them from that shell afterward:

```zsh
unset SPOTIPY_CLIENT_ID SPOTIPY_CLIENT_SECRET SPOTIPY_REDIRECT_URI
```

### Optional persistent private environment file

Prefer your password manager/Keychain-backed shell tooling if available. If you
choose a plaintext local file, create a private directory **outside the checkout**
(e.g. `~/.config/zotify-manager`), restrict the directory to mode `700`, and edit a
new `spotify.env` privately with mode `600`. Its template is:

```zsh
export SPOTIPY_CLIENT_ID='YOUR_APP_CLIENT_ID'
export SPOTIPY_CLIENT_SECRET='YOUR_APP_CLIENT_SECRET'
export SPOTIPY_REDIRECT_URI='http://127.0.0.1:9090'
```

Replace placeholders in your editor, not in chat. Source only your trusted file
in the shell that runs the manager:

```zsh
source "$HOME/.config/zotify-manager/spotify.env"
```

The project does **not** auto-read `.env` files. Do not commit the file, put the
secret directly in `.zshrc`, or save it in a shared/synced repository folder.
Ignore rules are a backup guard, not secret storage.

## 3. Authorize and verify in isolation

Use the [isolated-home instructions](../INSTALLATION.md#isolated-home-acceptance)
for a small playlist. They keep `.cache-spotipy` and the fixed music paths in a
disposable home without changing production caches. The first live API request
causes Spotipy to open or print an authorization link. Use the same Mac for the
browser and CLI, inspect the account/scopes and approve yourself. Keep the
terminal running for the loopback callback.

Both normal entry points explicitly select `~/.cache-spotipy`; they share this
cache in production. It contains OAuth tokens in a local JSON file, **not an
encrypted project vault**. Use a private home/test directory and restrict cache
permissions. A saved token or browser success page is not proof a playlist query
worked—require a real returned result, not just a new file.

For a fresh **test-only** reauthorization, stop the manager, move only that test
home's `.cache-spotipy` aside privately, and repeat the test. Do not delete the
production cache as a generic fix. Spotify account app-access revocation can
invalidate that app's tokens; rotating a leaked Client Secret is a separate
Dashboard action. Never copy or rename Zotify's Login5/API caches into this file.

## 4. Troubleshooting

| Failure | Check / next action |
| --- | --- |
| Missing credentials / invalid client | Variables are exported in the actual invoking shell; app ID and Secret belong to the same app; no placeholder values. Don't print the Secret to debug. |
| Redirect mismatch | Exact scheme/IP/port/path/trailing slash registered in the Dashboard and `SPOTIPY_REDIRECT_URI`. `localhost` is not a substitute for explicit loopback IP under current Spotify rules. |
| Browser callback fails | CLI and browser on the same Mac; port 9090 free; terminal still running. No router forwarding or public callback server is needed. |
| HTTP 401 / 403 | Authorized account, app user allowlist, scopes and current development-mode/endpoint policies. Reauthentication does not bypass Spotify restrictions. |
| HTTP 429 | Rate limited, not an empty playlist. Stop and respect Retry-After/backoff rather than repeatedly rerunning maintenance. |
| Wrong account / reused old token | Identify which HOME/cache the command actually used. Isolate a new test cache; leave production credentials alone. |
| Empty/missing playlist | Check account visibility and endpoint access separately from local missing audio. A metadata query does not download the track. |

Support reports should contain only commit, Python/Spotipy version, which step
failed, HTTP/error code and redacted messages. Remove credentials, tokens,
authorization codes and callback query strings. If a secret leaks, rotate/revoke
it and remove it from Git history before publishing.

## Official references

- [Developer Dashboard](https://developer.spotify.com/dashboard)
- [Redirect URI requirements](https://developer.spotify.com/documentation/web-api/concepts/redirect_uri)
- [Authorization Code flow](https://developer.spotify.com/documentation/web-api/tutorials/code-flow)
- [Web API quota modes](https://developer.spotify.com/documentation/web-api/concepts/quota-modes)

Live authentication and endpoint access are acceptance checks on the Mac. Offline
tests and a clean dependency install do not prove either.
