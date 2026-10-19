"""Setup safety checks use disposable source folders, not the user's venv."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class MacSetupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="setup-macos-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "checkout with spaces"
        self.root.mkdir()
        self.script = Path(__file__).resolve().parents[1] / "scripts/setup_macos.sh"
        self.bin = Path(self.tmp.name) / "bin"
        self.bin.mkdir()
        self.env = dict(os.environ, HOME=str(Path(self.tmp.name)), PATH=f"{self.bin}:{os.environ['PATH']}")

    def run_setup(self, *args):
        if not self.script.is_file():
            self.fail("macOS setup script has not been implemented")
        (self.root / "scripts").mkdir(exist_ok=True)
        copy = self.root / "scripts/setup_macos.sh"
        shutil.copyfile(self.script, copy)
        return subprocess.run(["bash", str(copy), *args], cwd=self.tmp.name,
                              env=self.env, text=True, capture_output=True)

    def fake_command(self, name, body):
        path = self.bin / name
        path.write_text("#!/bin/bash\n" + body)
        path.chmod(0o755)

    def test_creates_isolated_environment_and_runs_requested_tests(self):
        self.fake_command("uname", "printf 'Darwin\\n'\n")
        self.fake_command("ffmpeg", "printf 'libmp3lame\\n'\n")
        self.fake_command("ffprobe", "exit 0\n")
        # A shell harness checks orchestration without contacting PyPI or Spotify.
        driver = self.bin / "python-driver"
        driver.write_text('''#!/bin/bash
set -eu
[ -z "${PYTHONPATH+x}" ] && [ -z "${PYTHONHOME+x}" ]
printf '%s\\n' "$*" >> "$HOME/setup-commands"
if [ "$1" = -c ]; then
    case "$2" in
        *sys.version_info*) exit 0 ;;
        *) printf 'verification fixture\\n'; exit 0 ;;
    esac
fi
if [ "$1" = -m ] && [ "$2" = venv ]; then
    mkdir -p "$3/bin"
    cp "$0" "$3/bin/python"
    printf '#!/bin/bash\\nexit 0\\n' > "$3/bin/zotify"
    chmod +x "$3/bin/python" "$3/bin/zotify"
fi
''')
        driver.chmod(0o755)
        self.env.update(PYTHON=str(driver), PYTHONPATH="/wrong/site-packages:", PYTHONHOME="/wrong/home")
        result = self.run_setup("--test")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue((self.root / ".venv/bin/python").is_file())
        self.assertEqual((self.root / ".venv/.zotify-setup-root").read_text().strip(), str(self.root))
        commands = (Path(self.tmp.name) / "setup-commands").read_text()
        self.assertIn("-m pip install", commands)
        self.assertIn("-m pip check", commands)
        self.assertTrue("-m pytest" in commands or "-m unittest" in commands)
        self.assertFalse((Path(self.tmp.name) / ".zshrc").exists())
        again = self.run_setup("--update")
        self.assertEqual(again.returncode, 0, again.stdout + again.stderr)

    def test_preserves_an_existing_environment(self):
        self.fake_command("uname", "printf 'Darwin\\n'\n")
        venv = self.root / ".venv"
        venv.mkdir()
        sentinel = venv / "keep-me"
        sentinel.write_text("existing installation")
        result = self.run_setup()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("already exists", result.stderr)
        self.assertEqual(sentinel.read_text(), "existing installation")
        self.assertEqual(list(venv.iterdir()), [sentinel])

    def test_rejects_non_macos_without_creating_environment(self):
        self.fake_command("uname", "printf 'Linux\\n'\n")
        result = self.run_setup()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("macOS", result.stderr)
        self.assertFalse((self.root / ".venv").exists())


if __name__ == "__main__":
    unittest.main()
