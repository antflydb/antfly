"""GPU driver discovery contract for the standard runtime image, without a GPU."""

import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


def runtime_library_paths():
    # The last ENV assignment is the effective runtime-stage value.
    assignments = [
        shlex.split(line)[1].split("=", 1)[1]
        for line in (ROOT / "zig/Dockerfile.runtime").read_text().splitlines()
        if line.startswith("ENV LD_LIBRARY_PATH=")
    ]
    return assignments[-1].split(":")


class RuntimeContainerTests(unittest.TestCase):
    def test_driver_and_bundled_library_paths(self):
        paths = runtime_library_paths()
        self.assertIn("/usr/local/nvidia/lib64", paths)
        self.assertIn("/usr/local/nvidia/lib", paths)
        self.assertIn("/usr/local/lib", paths)
        self.assertTrue(all(path.startswith("/") for path in paths))

    @unittest.skipUnless(
        sys.platform == "linux" and shutil.which("cc"), "needs Linux cc"
    )
    def test_dlopen_finds_driver_in_injected_mount(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            driver_dir = root / "usr/local/nvidia/lib64"
            driver_dir.mkdir(parents=True)
            subprocess.run(
                [
                    "cc",
                    "-shared",
                    "-fPIC",
                    "-x",
                    "c",
                    "-",
                    "-o",
                    str(driver_dir / "libcuda.so.1"),
                ],
                input="int antfly_test_driver(void) { return 42; }\n",
                text=True,
                check=True,
                capture_output=True,
            )
            # Probe the native loader directly. A toolcache Python may itself
            # need LD_LIBRARY_PATH to find libpython, which is unrelated to the
            # container's driver paths and must not be part of this contract.
            probe = root / "driver-probe"
            subprocess.run(
                ["cc", "-x", "c", "-", "-o", str(probe), "-ldl"],
                input=r"""
#include <dlfcn.h>
#include <stdio.h>

int main(void) {
    void *driver = dlopen("libcuda.so.1", RTLD_NOW | RTLD_LOCAL);
    if (!driver) {
        fprintf(stderr, "dlopen: %s\n", dlerror());
        return 1;
    }
    int (*probe)(void) = (int (*)(void))dlsym(driver, "antfly_test_driver");
    const char *error = dlerror();
    if (error) {
        fprintf(stderr, "dlsym: %s\n", error);
        dlclose(driver);
        return 2;
    }
    int result = probe();
    dlclose(driver);
    if (result != 42) {
        fprintf(stderr, "unexpected driver result: %d\n", result);
        return 3;
    }
    return 0;
}
""",
                text=True,
                check=True,
                capture_output=True,
            )
            env = os.environ.copy()
            # Map container absolute paths into an isolated fixture root. Do not
            # modify the host or require an NVIDIA driver/GPU on the CI runner.
            env["LD_LIBRARY_PATH"] = ":".join(
                str(root / path.lstrip("/")) for path in runtime_library_paths()
            )
            result = subprocess.run(
                [str(probe)],
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            # Prove the fixture was discovered through the configured paths,
            # rather than a host library or an embedded absolute filename.
            env["LD_LIBRARY_PATH"] = str(root / "missing")
            missing = subprocess.run(
                [str(probe)], env=env, capture_output=True, text=True
            )
            self.assertNotEqual(missing.returncode, 0)


if __name__ == "__main__":
    unittest.main()
