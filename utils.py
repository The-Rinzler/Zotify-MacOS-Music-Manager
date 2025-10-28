import shutil
import subprocess
import os
import re
import unicodedata
from pathlib import Path

class CustomPrint:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    GREEN = "\033[32m"
    RED = "\033[31m"
    PURPLE = "\033[35m"

    @staticmethod
    def format(text: str, *styles: str) -> str:
        return f"{''.join(styles)}{text}{CustomPrint.RESET}"

    @classmethod
    def bold(cls, text: str, color: str = None) -> str:
        if color:
            color_style = getattr(cls, color.upper(), "")
            return cls.format(text, cls.BOLD, color_style)
        else:
            return cls.format(text, cls.BOLD)

    @classmethod
    def bold_print(cls, text: str, color: str = None) -> None:
        if color:
            color_style = getattr(cls, color.upper(), "")
            formatted_text = cls.format(text, cls.BOLD, color_style)
        else:
            formatted_text = cls.format(text, cls.BOLD)
        print(formatted_text)

    @staticmethod
    def print(text: str, color: str = None) -> None:
        if color:
            color_style = getattr(CustomPrint, color.upper(), "")
            formatted_text = CustomPrint.format(text, color_style)
        else:
            formatted_text = text
        print(formatted_text)

    @staticmethod
    def separator(title: str = None, char: str = '─') -> None:
        columns = shutil.get_terminal_size().columns
        if title:
            title = f" {title} "
            line = f"{char * ((columns - len(title)) // 2)}{title}{char * ((columns - len(title)) // 2)}"
            line = line.ljust(columns, char)
        else:
            line = char * columns

        print()
        CustomPrint.bold_print(line)
        print()

    @staticmethod
    def print_banner(text: str) -> None:
        columns = shutil.get_terminal_size().columns
        double_line = "=" * columns
        title_line = text.center(columns)

        # Clear the console twice
        def clear_console():
            if os.name == 'nt':
                subprocess.run('cls', shell=True)
            else:
                subprocess.run('clear', shell=True)


        # Call the function to clear the console
        clear_console()


        CustomPrint.bold_print(double_line)
        print()
        CustomPrint.bold_print(title_line)
        print()
        CustomPrint.bold_print(double_line)
        print()


def create_hidden_playlist_link_file(playlist_dir, playlist_name, playlist_url):
    """
    Creates (or updates) a hidden file (named ".{sanitized_playlist_name}") inside playlist_dir that contains
    the trimmed playlist URL along with a comment containing the playlist name.

    Args:
        playlist_dir (str): The directory where the hidden file should be created.
        playlist_name (str): The original playlist name (will be sanitized for the filename).
        playlist_url (str): The playlist URL to write into the file (will be trimmed).
    """

    def clean_playlist_url(url):
        return url.split('?')[0]  # Remove tracking parameters

    trimmed_url = clean_playlist_url(playlist_url)
    sanitized_playlist_name = sanitize_filename(playlist_name)
    hidden_file_path = os.path.join(playlist_dir, f".{sanitized_playlist_name}")

    # Always update (or create) the file:
    with open(hidden_file_path, 'w', encoding='utf-8') as f:
        f.write("# " + playlist_name + "\n")
        f.write(trimmed_url)

    CustomPrint.bold_print(
        f">>> Hidden file updated with the playlist link: {hidden_file_path}", "GREEN"
    )


def get_playlist_links(playlist_dir):
    """
    Searches for a hidden file in playlist_dir (excluding .song_ids)
    and returns its content as the playlist link.

    Args:
        playlist_dir (str): The full path to the playlist directory.

    Returns:
        str or None: The playlist link if found, otherwise None.
    """
    import os

    # List all hidden files (files starting with a dot) excluding .song_ids.
    hidden_files = [f for f in os.listdir(playlist_dir)
                    if f.startswith('.') and f != '.song_ids']
    for hidden_file in hidden_files:
        hidden_file_path = os.path.join(playlist_dir, hidden_file)
        if os.path.isfile(hidden_file_path):
            with open(hidden_file_path, 'r', encoding='utf-8') as f:
                link = f.read().strip()
                if link:
                    return link
    return None


def sanitize_filename(filename: str) -> str:
    """
    Replaces invalid characters on Linux/Windows/MacOS with underscores.

    This function uses a comprehensive regex to replace:
      - Characters: /, :, #, ', |, <, >, ", comma,
        and control characters (0-31)
      - Reserved device names (AUX, COM1..COM9, CON, LPT1..LPT9, NUL, PRN)
      - Leading/trailing whitespace and periods

    Then, it additionally replaces every occurrence of '?' and '!' with an underscore.

    Args:
        filename (str): The original filename.

    Returns:
        str: The sanitized filename.
    """
    # First pass: Replace invalid characters and reserved device names.
    sanitized = re.sub(
    r'[\/#:|_<>\0-\x1f?!]|^(AUX|COM[1-9]|CON|LPT[1-9]|NUL|PRN)(?![^.])',
    "_",
    str(filename),
    flags=re.IGNORECASE
)

    return sanitized


def write_playlist_to_m3u8(tracks, output_file: str):
    """
    Write a playlist to an M3U file in "Artist - Title.mp3" format and
    return track metadata.
    """
    track_metadata = []
    with open(output_file, 'w', encoding='utf-8') as f:
        for item in tracks:
            track = item.get('track')
            if track and track.get('artists'):
                artist = track['artists'][0]['name']
            else:
                artist = "Unknown Artist"
            if track:
                title = track.get('name', 'Unknown Title')
                album = track.get('album')
                release_date = album.get('release_date') if album else None
                release_year = release_date.split('-')[0] if release_date else 'Unknown'
                # First, sanitize the "Artist - Title" part
                #print(f"Raw: {artist} - {title}")
                base_filename = sanitize_filename(f"{artist} - {title}")
                #print(f"Sanitized: {base_filename}")
                # Then append the extension
                sanitized_file_name = f"{base_filename}.mp3"
                track_metadata.append((sanitized_file_name, sanitized_file_name, release_year))
                f.write(f"{sanitized_file_name}\n")
    return track_metadata


def get_possible_filenames(file_name: str) -> set[str]:
    possible_filenames = set()
    p = Path(file_name)
    stem = p.stem     # Name without extension
    ext = p.suffix    # Extension (e.g. ".mp3")

    # Variants for the stem
    stems = set()
    stems.add(stem)

    # Normalize to ASCII (strip accents)
    normalized_ascii = unicodedata.normalize('NFKD', stem).encode('ASCII', 'ignore').decode('ASCII')
    stems.add(normalized_ascii)

    # Strict alphanumeric version (remove non-alphanumeric except spaces, hyphens, underscores, and dots)
    alphanumeric_only = re.sub(r'[^a-zA-Z0-9\s\-_.]', '', normalized_ascii)
    stems.add(alphanumeric_only)

    # Lowercase variants
    stems.update({s.lower() for s in stems})

    # Trim whitespace
    stems.update({s.strip() for s in stems})

    # Replace spaces with underscores and dashes (problems caused by Zotify)
    # Common problems
    stems.update({s.replace(' ', '_') for s in stems})
    stems.update({s.replace(' ', '-') for s in stems})
    stems.update({s.replace('_', '.') for s in stems})
    stems.update({s.replace(':', '_') for s in stems})

    stems.update({s.replace('_', '?') for s in stems})
    stems.update({s.replace('_', '.') for s in stems})
    stems.update({s.replace('_', '!') for s in stems})
    stems.update({s.replace('_.', '..') for s in stems})
    # Reattach extension for every variant
    for variant in stems:
        possible_filenames.add(f"{variant}{ext}")

    # Also include the original file name
    possible_filenames.add(file_name)

    return possible_filenames


def rename_and_cleanup(playlist_dir, expected_names):
    """
    First renames files in playlist_dir so that if sanitizing them yields a name
    that exists in expected_names, they get renamed accordingly.
    Then, it removes any file whose name does not exactly match any name in expected_names.

    Args:
        playlist_dir (str): The directory containing the music files.
        expected_names (set): A set of expected filenames (as read from the .m3u file).
    """
    # Step 1: Rename files that could be fixed by sanitization
    for file in os.listdir(playlist_dir):
        file_path = os.path.join(playlist_dir, file)
        if os.path.isfile(file_path):
            if file not in expected_names:
                sanitized = sanitize_filename(file)
                # If sanitizing produces a name that is expected, then rename.
                if sanitized in expected_names:
                    new_path = os.path.join(playlist_dir, sanitized)
                    # If the file's name is already correct after sanitization, skip.
                    if file != sanitized:
                        if not os.path.exists(new_path):
                            os.rename(file_path, new_path)
                            print(f"Renamed '{file}' -> '{sanitized}'")
                        else:
                            # If a file with the sanitized name already exists, remove the duplicate.
                            os.remove(file_path)
                            print(f"Duplicate found. Removed '{file}'")

    # Step 2: Remove any file that does not exactly match an expected filename.
    for file in os.listdir(playlist_dir):
        file_path = os.path.join(playlist_dir, file)
        if os.path.isfile(file_path) and file not in expected_names:
            os.remove(file_path)
            x = print(f"Removed file: '{file}'")


# Example usage:
# Assume that 'expected_names' is a set of filenames from your .m3u file.
# For example:
# with open(m3u_file, 'r', encoding='utf-8') as f:
#     expected_names = {line.strip() for line in f if line.strip()}
#
# playlist_dir = "/path/to/playlist/directory"
# rename_and_cleanup(playlist_dir, expected_names)

def check_missing_songs_in_playlists(consolidated_library_dir, local_files_dir):
    """
    For each .m3u playlist file in consolidated_library_dir, check that every song listed is
    present either in the corresponding playlist folder (downloaded) or in local_files_dir.
    Uses get_possible_filenames to account for filename variants.

    Returns:
        A dict mapping the .m3u filename to a set of missing songs.
    """
    missing_songs_in_playlists = {}

    # Iterate over each .m3u file in the consolidated library.
    for playlist in os.listdir(consolidated_library_dir):
        if not playlist.endswith('.m3u'):
            continue

        m3u_path = os.path.join(consolidated_library_dir, playlist)

        # Read the playlist file (ignore empty lines and comments)
        with open(m3u_path, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]

        # Here we assume each valid line represents the original song filename.
        track_references = lines

        # The downloaded songs are assumed to be in a folder named after the playlist (without .m3u)
        playlist_folder = os.path.join(consolidated_library_dir, os.path.splitext(playlist)[0])
        downloaded_files = set(os.listdir(playlist_folder)) if os.path.isdir(playlist_folder) else set()

        # Gather all local files (assumed to be .mp3 files) from the local files directory.
        songs_in_local_files = set()
        for root, dirs, files in os.walk(local_files_dir):
            for file in files:
                if file.endswith('.mp3'):
                    songs_in_local_files.add(file)

        # Now, for each song in the playlist, check if any possible filename is found
        missing = set()
        for original_song in track_references:
            possible_names = get_possible_filenames(original_song)
            # If none of the variants is found in the downloaded folder or local files, mark as missing.
            if not any(fname in downloaded_files for fname in possible_names) and \
                    not any(fname in songs_in_local_files for fname in possible_names):
                missing.add(original_song)

        if missing:
            missing_songs_in_playlists[playlist] = missing

    return missing_songs_in_playlists

