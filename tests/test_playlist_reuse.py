"""Local reuse must finish before lyric cleanup and network backfill."""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

from mutagen.id3 import ID3, TIT2, TXXX, USLT

import playlist_extractor as extractor


class PlaylistReuseTests(unittest.TestCase):
    def make_file(self, folder, name="Artist - Song.mp3", lyrics="", track_id=None):
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / name
        tags = ID3()
        tags.add(TIT2(encoding=3, text="Preserve title"))
        if lyrics:
            tags.add(USLT(encoding=3, lang="eng", text=lyrics))
        if track_id:
            tags.add(TXXX(encoding=3, desc="TRACKID", text=[track_id]))
        tags.save(path, v2_version=3)
        return path

    def test_reuses_downloaded_copy_with_lyrics_before_backfill(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            current = root / "Zotify Music" / "Current"
            current.mkdir(parents=True)
            source = self.make_file(root / "Zotify Music" / "Other", lyrics="Existing words", track_id="id")
            original = source.read_bytes()
            metadata = [(source.name, source.name, "2026")]
            ids = {source.name: "id"}
            stats = extractor.reuse_playlist_files(current, metadata, ids, [root / "Zotify Music"])
            self.assertEqual(stats["copied"], 1)
            self.assertEqual((current / source.name).read_bytes(), original)
            self.assertEqual(source.read_bytes(), original)
            extractor.clean_playlist_lyrics(str(current), metadata)
            fetch = Mock(side_effect=AssertionError("Local lyrics must skip Spotify"))
            backfill = extractor.backfill_playlist_lyrics(str(current), metadata, ids, "token", fetch=fetch)
            fetch.assert_not_called()
            self.assertEqual(backfill["already_present"], 1)

    def test_conflicting_embedded_id_is_not_reused_by_filename(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            current = root / "Current"
            current.mkdir()
            source = self.make_file(root / "Other", lyrics="Wrong version", track_id="wrong")
            metadata = [(source.name, source.name, "2026")]
            stats = extractor.reuse_playlist_files(current, metadata, {source.name: "wanted"}, [root])
            self.assertEqual(stats["copied"], 0)
            self.assertFalse((current / source.name).exists())

    def test_conflicting_playlist_file_does_not_receive_another_recordings_lyrics(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            current = root / "Current"
            target = self.make_file(current, track_id="wrong")
            self.make_file(root / "Other", lyrics="Different recording", track_id="wanted")
            before = target.read_bytes()
            stats = extractor.reuse_playlist_files(
                current, [(target.name, target.name, "2026")], {target.name: "wanted"}, [root],
            )
            self.assertEqual(stats["lyrics_reused"], 0)
            self.assertEqual(target.read_bytes(), before)

    def test_conflicting_source_embedded_and_archive_ids_are_rejected(self):
        for embedded, archived in (("wanted", ("wrong",)), ("wrong", ("wanted",)),
                                   ("wanted", ("wanted", "wrong"))):
            for existing_target in (False, True):
                with self.subTest(embedded=embedded, archived=archived, target=existing_target), TemporaryDirectory() as directory:
                    root = Path(directory)
                    current = root / "Current"
                    current.mkdir()
                    source = self.make_file(root / "Other", lyrics="Wrong words", track_id=embedded)
                    archive = source.parent / ".song_ids"
                    archive.write_text("".join(
                        f"{track_id}\t2026-01-01 00:00:00\tArtist\tSong\t{source.name}\n"
                        for track_id in archived
                    ), encoding="utf-8")
                    target = current / source.name
                    if existing_target:
                        self.make_file(current, track_id="wanted")
                    before = {path: path.read_bytes() for path in root.rglob("*") if path.is_file()}
                    stats = extractor.reuse_playlist_files(
                        current, [(source.name, source.name, "2026")], {source.name: "wanted"}, [root],
                    )
                    self.assertEqual(stats, {"copied": 0, "lyrics_reused": 0, "errors": 0})
                    self.assertEqual({path: path.read_bytes() for path in root.rglob("*") if path.is_file()}, before)
                    self.assertEqual(target.exists(), existing_target)
                    self.assertEqual(ID3(source)["TIT2"].text, ["Preserve title"])

    def test_multiple_conflicting_source_trackids_are_rejected(self):
        for separate_frames in (False, True):
            for existing_target in (False, True):
                with self.subTest(frames=separate_frames, target=existing_target), TemporaryDirectory() as directory:
                    root = Path(directory)
                    current = root / "Current"
                    current.mkdir()
                    source = self.make_file(root / "Other", lyrics="Wrong words", track_id="wanted")
                    tags = ID3(source)
                    if separate_frames:
                        tags.add(TXXX(encoding=3, desc="trackid", text=["wrong"]))
                    else:
                        tags["TXXX:TRACKID"].text = ["wanted", "wrong"]
                    tags.save(source, v2_version=4)
                    if existing_target:
                        self.make_file(current, track_id="wanted")
                    before = {path: path.read_bytes() for path in root.rglob("*") if path.is_file()}
                    stats = extractor.reuse_playlist_files(
                        current, [(source.name, source.name, "2026")], {source.name: "wanted"}, [root],
                    )
                    self.assertEqual(stats, {"copied": 0, "lyrics_reused": 0, "errors": 0})
                    self.assertEqual({path: path.read_bytes() for path in root.rglob("*") if path.is_file()}, before)
                    self.assertEqual(ID3(source)["TIT2"].text, ["Preserve title"])

    def test_multiple_conflicting_target_trackids_are_rejected(self):
        for separate_frames in (False, True):
            with self.subTest(frames=separate_frames), TemporaryDirectory() as directory:
                root = Path(directory)
                current = root / "Current"
                target = self.make_file(current, track_id="wanted")
                tags = ID3(target)
                if separate_frames:
                    tags.add(TXXX(encoding=3, desc="trackid", text=["wrong"]))
                else:
                    tags["TXXX:TRACKID"].text = ["wanted", "wrong"]
                tags.save(target, v2_version=4)
                self.make_file(root / "Other", lyrics="Wrong words", track_id="wanted")
                before = {path: path.read_bytes() for path in root.rglob("*") if path.is_file()}
                stats = extractor.reuse_playlist_files(
                    current, [(target.name, target.name, "2026")], {target.name: "wanted"}, [root],
                )
                self.assertEqual(stats, {"copied": 0, "lyrics_reused": 0, "errors": 0})
                self.assertEqual({path: path.read_bytes() for path in root.rglob("*") if path.is_file()}, before)
                self.assertEqual(ID3(target)["TIT2"].text, ["Preserve title"])

    def test_idless_source_can_match_expected_id_without_contradictions(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            current = root / "Current"
            current.mkdir()
            source = self.make_file(root / "Local Files", lyrics="Local words")
            before = source.read_bytes()
            stats = extractor.reuse_playlist_files(
                current, [(source.name, source.name, "2026")], {source.name: "wanted"}, [root],
            )
            self.assertEqual(stats["copied"], 1)
            self.assertEqual((current / source.name).read_bytes(), before)
            self.assertEqual(source.read_bytes(), before)

    def test_archive_id_finds_renamed_local_file(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            current = root / "Current"
            current.mkdir()
            source = self.make_file(root / "Other", name="Renamed recording.mp3", lyrics="Right words", track_id="id")
            (source.parent / ".song_ids").write_text(
                f"id\t2026-01-01 00:00:00\tArtist\tSong\t{source.name}\n", encoding="utf-8",
            )
            name = "Artist - Song.mp3"
            stats = extractor.reuse_playlist_files(current, [(name, name, "2026")], {name: "id"}, [root])
            self.assertEqual(stats["copied"], 1)
            self.assertEqual((current / name).read_bytes(), source.read_bytes())

    def test_suffixed_unicode_local_copy_is_reused(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            current = root / "Current"
            current.mkdir()
            source = self.make_file(root / "Local Files" / "Nested", name="artist - Cafe\u0301_1.MP3", lyrics="Words")
            name = "Artist - Café.mp3"
            stats = extractor.reuse_playlist_files(current, [(name, name, "2026")], {}, [root / "Local Files"])
            self.assertEqual(stats["copied"], 1)
            self.assertEqual((current / name).read_bytes(), source.read_bytes())

    def test_cli_copies_local_files_before_reconciliation_and_lyrics(self):
        import contextlib
        import io
        import os
        import runpy
        import sys
        from unittest.mock import patch
        with TemporaryDirectory() as directory:
            home = Path(directory)
            source = self.make_file(home / "Music/Local Files", lyrics="Words", track_id="id")
            items = [{"item": {"id": "id", "name": "Song", "artists": [{"name": "Artist"}],
                               "album": {"release_date": "2026-01-01"}}}]
            client = Mock()
            client.playlist.return_value = {"name": "Current"}
            fetch = Mock(side_effect=AssertionError("copied lyrics must not be fetched"))
            output = io.StringIO()
            with patch.dict(os.environ, {"HOME": directory}), patch("pathlib.Path.home", return_value=home), \
                 patch("spotify_api.get_spotify_client", return_value=client), \
                 patch("spotify_api.fetch_playlist_tracks", return_value=items), \
                 patch("spotify_api.fetch_track_lyrics", fetch), \
                 patch("utils.CustomPrint.print_banner"), \
                 patch.object(sys, "argv", ["playlist_extractor", "https://open.spotify.com/playlist/test"]), \
                 contextlib.redirect_stdout(output):
                runpy.run_path(str(Path(extractor.__file__)), run_name="__main__")
            target = home / "Music/Zotify Music/Current" / source.name
            self.assertTrue(target.exists(), output.getvalue())
            self.assertEqual(ID3(target).getall("USLT")[0].text, "Words")
            self.assertIn("id\t", (target.parent / ".song_ids").read_text())
            fetch.assert_not_called()
            self.assertIn("already present=1", output.getvalue())

    def test_cli_remembers_no_lyrics_and_allows_explicit_refresh(self):
        import contextlib
        import io
        import os
        import runpy
        import sys
        from unittest.mock import patch
        with TemporaryDirectory() as directory:
            home = Path(directory)
            self.make_file(home / "Music/Local Files", track_id="id")
            items = [{"item": {"id": "id", "name": "Song", "artists": [{"name": "Artist"}],
                               "album": {"release_date": "2026-01-01"}}}]
            client = Mock()
            client.playlist.return_value = {"name": "Current"}
            fetch = Mock(return_value=("unavailable", ""))
            for flags, expected_calls in (([], 1), ([], 1), (["--refresh-lyrics-cache"], 2)):
                output = io.StringIO()
                with patch.dict(os.environ, {"HOME": directory, "XDG_CACHE_HOME": str(home / "Cache")}), \
                     patch("pathlib.Path.home", return_value=home), \
                     patch("spotify_api.get_spotify_client", return_value=client), \
                     patch("spotify_api.fetch_playlist_tracks", return_value=items), \
                     patch("spotify_api.fetch_track_lyrics", fetch), \
                     patch("utils.CustomPrint.print_banner"), \
                     patch.object(sys, "argv", ["playlist_extractor", "https://open.spotify.com/playlist/test", *flags]), \
                     contextlib.redirect_stdout(output):
                    runpy.run_path(str(Path(extractor.__file__)), run_name="__main__")
                self.assertEqual(fetch.call_count, expected_calls, output.getvalue())
                self.assertIn("not on Spotify=1", output.getvalue())

    def test_reuse_never_changes_sources_or_existing_lyrics_and_rejects_escapes(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            current = root / "Library/Current"
            current.mkdir(parents=True)
            target = self.make_file(current, lyrics="Keep my lyrics", track_id="id")
            self.make_file(root / "Library/Other", lyrics="Do not overwrite", track_id="id")
            outside = self.make_file(root / "Outside", name="Artist - Escape.mp3", lyrics="Outside")
            linked = root / "Library/Other" / outside.name
            linked.symlink_to(outside)
            original_target = target.read_bytes()
            original_outside = outside.read_bytes()
            metadata = [(target.name, target.name, "2026"),
                        (outside.name, outside.name, "2026"),
                        ("../Escape.mp3", "../Escape.mp3", "2026")]
            stats = extractor.reuse_playlist_files(current, metadata, {target.name: "id"}, [root / "Library"])
            self.assertEqual(stats["copied"], 0)
            self.assertEqual(stats["lyrics_reused"], 0)
            self.assertEqual(target.read_bytes(), original_target)
            self.assertEqual(outside.read_bytes(), original_outside)
            self.assertFalse((current / outside.name).exists())
            self.assertFalse((current.parent / "Escape.mp3").exists())

    def test_prefers_lyrical_copy_and_searches_each_root_once(self):
        from unittest.mock import patch
        with TemporaryDirectory() as directory:
            root = Path(directory)
            current = root / "Current"
            current.mkdir()
            blank = self.make_file(root / "A blank", track_id="id")
            lyrical = self.make_file(root / "Z lyrical", lyrics="Good words", track_id="id")
            metadata = [(blank.name, blank.name, "2026")]
            with patch.object(extractor.os, "walk", wraps=extractor.os.walk) as walk:
                stats = extractor.reuse_playlist_files(current, metadata, {blank.name: "id"}, [root, root])
            self.assertEqual(walk.call_count, 1)
            self.assertEqual(stats["copied"], 1)
            self.assertEqual((current / blank.name).read_bytes(), lyrical.read_bytes())

    def test_existing_blank_copy_gets_only_lyrics_from_best_local_source(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            current = root / "Current"
            target = self.make_file(current, track_id="id")
            self.make_file(root / "A blank", track_id="id")
            source = self.make_file(root / "Z lyrical", lyrics="[00:01.00]Local words", track_id="id")
            original = source.read_bytes()
            metadata = [(target.name, target.name, "2026")]
            stats = extractor.reuse_playlist_files(current, metadata, {target.name: "id"}, [root])
            self.assertEqual(stats["copied"], 0)
            self.assertEqual(stats["lyrics_reused"], 1)
            self.assertEqual(ID3(target).getall("USLT")[0].text, "Local words")
            self.assertEqual(ID3(target)["TIT2"].text, ["Preserve title"])
            self.assertEqual(ID3(target).version[:2], (2, 3))
            self.assertEqual(source.read_bytes(), original)
