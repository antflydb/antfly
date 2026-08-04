from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from qualify_gliner2_cuda_hardware import CONTRACT as LANE_CONTRACT
from qualify_gliner2_cuda_hardware import FULL_PARITY_FILTERS
from qualify_gliner2_cuda_hardware import qualification_source_files
from qualify_gliner2_cuda_hardware import sha256_file
from qualify_gliner2_cuda_hardware import standalone_test_executable
from qualify_gliner2_cuda_hardware import source_fingerprint
from summarize_gliner2_cuda_hardware import summarize, verify_summary


class CudaHardwareQualificationTest(unittest.TestCase):
    def test_resolves_one_verbose_built_standalone_sanitizer_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            package_dir = Path(tmp) / "package"
            log_dir = Path(tmp) / "lane-logs"
            executable = package_dir / ".zig-cache" / "o" / ("a" * 32) / "test"
            executable.parent.mkdir(parents=True)
            executable.write_text("test executable\n", encoding="utf-8")
            log_dir.mkdir()
            run_line = (
                f"./.zig-cache/o/{'a' * 32}/test "
                "--cache-dir=./.zig-cache --seed=0x1 --listen=-\n"
            )
            for test_filter in FULL_PARITY_FILTERS:
                (log_dir / f"full-parity-{test_filter}.log").write_text(
                    run_line,
                    encoding="utf-8",
                )
            self.assertEqual(executable.resolve(), standalone_test_executable(package_dir, log_dir))

    def test_resolves_standalone_target_from_configured_absolute_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "package"
            log_dir = root / "lane-logs"
            cache_dir = root / "ci-zig-local-cache"
            executable = cache_dir / "o" / ("b" * 32) / "test"
            executable.parent.mkdir(parents=True)
            executable.write_text("test executable\n", encoding="utf-8")
            log_dir.mkdir()
            run_line = (
                f"{executable} --cache-dir={cache_dir} "
                "--seed=0x1 --listen=-\n"
            )
            for test_filter in FULL_PARITY_FILTERS:
                (log_dir / f"full-parity-{test_filter}.log").write_text(
                    run_line,
                    encoding="utf-8",
                )
            self.assertEqual(
                executable.resolve(),
                standalone_test_executable(
                    package_dir,
                    log_dir,
                    {"ZIG_LOCAL_CACHE_DIR": str(cache_dir)},
                ),
            )

    def test_does_not_parse_failed_command_prefix_as_a_cache_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "package"
            log_dir = root / "lane-logs"
            cache_dir = root / "ci-zig-local-cache"
            executable = cache_dir / "o" / ("c" * 32) / "test"
            executable.parent.mkdir(parents=True)
            executable.write_text("test executable\n", encoding="utf-8")
            log_dir.mkdir()
            failed_line = (
                f"failed command: {executable} --cache-dir={cache_dir} "
                "--seed=0x1 --listen=-\n"
            )
            for test_filter in FULL_PARITY_FILTERS:
                (log_dir / f"full-parity-{test_filter}.log").write_text(
                    failed_line,
                    encoding="utf-8",
                )
            with self.assertRaisesRegex(ValueError, "found 0"):
                standalone_test_executable(
                    package_dir,
                    log_dir,
                    {"ZIG_LOCAL_CACHE_DIR": str(cache_dir)},
                )

    def test_requires_current_fp32_l4_a100_h100_lanes(self) -> None:
        package_dir = Path(__file__).resolve().parent.parent
        fingerprint = source_fingerprint(package_dir)
        source_files = list(qualification_source_files(package_dir))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths: list[Path] = []
            for architecture, family, capability in (
                ("sm80", "A100", "8.0"),
                ("sm89", "L4", "8.9"),
                ("sm90", "H100", "9.0"),
            ):
                path = root / f"{architecture}.json"
                log_dir = root / f"{path.stem}-logs"
                log_dir.mkdir()

                def command(log_name: str) -> dict[str, object]:
                    log_path = log_dir / log_name
                    log_path.write_text(f"passing {architecture} {log_name}\n", encoding="utf-8")
                    return {
                        "pass": True,
                        "returncode": 0,
                        "timed_out": False,
                        "argv": ["test-command", architecture, log_name],
                        "log": f"{log_dir.name}/{log_name}",
                        "log_sha256": sha256_file(log_path),
                    }

                parity_subchecks = {
                    name: command(f"full-parity-{name}.log") for name in FULL_PARITY_FILTERS
                }
                path.write_text(json.dumps({
                    "contract": LANE_CONTRACT,
                    "pass": True,
                    "training_precision": "fp32",
                    "optimizer_state_precision": "fp32",
                    "cuda_artifacts": "fatbin",
                    "source_fingerprint_sha256": fingerprint,
                    "source_files": source_files,
                    "gpu": {
                        "architecture": architecture,
                        "family": family,
                        "compute_capability": capability,
                        "name": f"NVIDIA {family}",
                        "driver_version": "999.0",
                        "memory_mib": "40960",
                    },
                    "failures": [],
                    "checks": {
                        "embedded_artifacts": command("embedded-artifacts.log"),
                        "full_parity": {
                            "pass": True,
                            "returncode": 0,
                            "timed_out": False,
                            "argv": ["zig", "build"],
                            "failures": [],
                            "subchecks": parity_subchecks,
                        },
                        "memcheck": command("memcheck.log"),
                        "initcheck": command("initcheck.log"),
                        "racecheck": command("racecheck.log"),
                    },
                }), encoding="utf-8")
                paths.append(path)
            result = summarize(paths, package_dir)
            self.assertTrue(result["pass"], result["failures"])
            self.assertEqual([], verify_summary(result, package_dir))

            missing = summarize(paths[:-1], package_dir)
            self.assertFalse(missing["pass"])
            self.assertIn("sm90", " ".join(missing["failures"]))

            stale = json.loads(paths[0].read_text(encoding="utf-8"))
            stale["source_fingerprint_sha256"] = "sha256:" + "0" * 64
            paths[0].write_text(json.dumps(stale), encoding="utf-8")
            rejected = summarize(paths, package_dir)
            self.assertFalse(rejected["pass"])
            self.assertIn("source binding", " ".join(rejected["failures"]))

            # Restore the current source binding, then prove that the matrix
            # aggregator reads and hashes the retained command logs.
            stale["source_fingerprint_sha256"] = fingerprint
            paths[0].write_text(json.dumps(stale), encoding="utf-8")
            memcheck_log = root / "sm80-logs" / "memcheck.log"
            memcheck_log.write_text("tampered\n", encoding="utf-8")
            rejected_log = summarize(paths, package_dir)
            self.assertFalse(rejected_log["pass"])
            self.assertIn("log hash", " ".join(rejected_log["failures"]))

            tampered = result.copy()
            tampered["lanes"] = dict(result["lanes"])
            tampered["lanes"]["sm89"] = dict(result["lanes"]["sm89"])
            tampered["lanes"]["sm89"]["report"] = dict(result["lanes"]["sm89"]["report"])
            tampered["lanes"]["sm89"]["report"]["checks"] = dict(
                result["lanes"]["sm89"]["report"]["checks"]
            )
            tampered["lanes"]["sm89"]["report"]["checks"]["racecheck"] = {"pass": False}
            self.assertIn("sm89", " ".join(verify_summary(tampered, package_dir)))


if __name__ == "__main__":
    unittest.main()
