#!/usr/bin/env python3
"""Regression test for the archive Cloud Build extracts at its root."""
import os
from pathlib import Path
import stat
import subprocess
import tarfile
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/publish-zig-runtime-dev.sh"


class PublishZigRuntimeDevTest(unittest.TestCase):
    def test_archive_has_root_binary_and_share_antfly(self):
        with tempfile.TemporaryDirectory() as temp:
            temp = Path(temp)
            bindir = temp / "bin"
            bindir.mkdir()
            archive = temp / "runtime.tar.gz"
            argv = temp / "zig-argv"
            (bindir / "zig").write_text("#!/usr/bin/env bash\nset -eu\nprintf '%s\\n' \"$@\" > \"$ZIG_ARGV\"\nprefix=\nwhile (($#)); do [ \"$1\" = --prefix ] && { prefix=$2; shift 2; } || shift; done\nmkdir -p \"$prefix/bin\" \"$prefix/share/antfly\"\nprintf '#!/bin/sh\\n' > \"$prefix/bin/antfly\"\nchmod +x \"$prefix/bin/antfly\"\n")
            (bindir / "gcloud").write_text("#!/usr/bin/env bash\nset -eu\nif [ \"$1 $2\" = 'storage cp' ]; then cp \"$3\" \"$CAPTURED_ARCHIVE\"; elif [ \"$1 $2\" = 'artifacts docker' ]; then echo sha256:test; fi\n")
            for command in bindir.iterdir(): command.chmod(command.stat().st_mode | stat.S_IXUSR)
            arch = "amd64" if os.uname().machine == "x86_64" else "arm64"
            result = subprocess.run([str(SCRIPT), "--tag", "test", "--arch", arch, "--jobs", "1"], cwd=ROOT, text=True, capture_output=True, env=os.environ | {"PATH": f"{bindir}:{os.environ['PATH']}", "CAPTURED_ARCHIVE": str(archive), "ZIG_ARGV": str(argv)})
            self.assertEqual(result.returncode, 0, result.stderr)
            with tarfile.open(archive) as contents:
                names = contents.getnames()
                self.assertIn("antfly", names)
                self.assertIn("share/antfly", names)
                self.assertTrue(contents.getmember("antfly").mode & stat.S_IXUSR)
            self.assertIn("-j1", argv.read_text().splitlines())


if __name__ == "__main__":
    unittest.main()
