"""Verify grouped CLI checks analyze and link every registered main."""

from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class FinetuneCommandChecks(unittest.TestCase):
    def compile_commands(self, second_main):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            preamble = (
                ROOT / "pkg/inference/build/finetune/command_check_preamble.zig.txt"
            ).read_text()
            (root / "check.zig").write_text(
                preamble
                + '    if (std.mem.eql(u8, name, "first")) return @import("command_0").main(init);\n'
                + '    if (std.mem.eql(u8, name, "second")) return @import("command_1").main(init);\n'
                + '    return error.CompileCheckOnly;\n}\n'
            )
            (root / "first.zig").write_text(
                'const std = @import("std");\n'
                'pub fn main(_: std.process.Init) !void {}\n'
            )
            (root / "second.zig").write_text(second_main)
            return subprocess.run(
                [
                    "zig", "build-exe", "-lc", "-ODebug",
                    "--dep", "command_0", "--dep", "command_1",
                    f"-Mroot={root / 'check.zig'}",
                    f"-Mcommand_0={root / 'first.zig'}",
                    f"-Mcommand_1={root / 'second.zig'}",
                    f"-femit-bin={root / 'check'}",
                    "--cache-dir", str(root / "cache"),
                    "--global-cache-dir", str(Path(tempfile.gettempdir()) / "antfly-command-check-cache"),
                ],
                capture_output=True, text=True,
            )

    def test_valid_entrypoints_link(self):
        result = self.compile_commands(
            'const std = @import("std");\n'
            'pub fn main(_: std.process.Init) !void {}\n'
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_unexecuted_entrypoint_is_analyzed(self):
        result = self.compile_commands(
            'const std = @import("std");\n'
            'pub fn main(_: std.process.Init) !void { missing_function(); }\n'
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing_function", result.stderr)

    def test_unexecuted_entrypoint_is_linked(self):
        result = self.compile_commands(
            'const std = @import("std");\n'
            'extern fn missing_external_symbol() void;\n'
            'pub fn main(_: std.process.Init) !void { missing_external_symbol(); }\n'
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing_external_symbol", result.stderr)


if __name__ == "__main__":
    unittest.main()
