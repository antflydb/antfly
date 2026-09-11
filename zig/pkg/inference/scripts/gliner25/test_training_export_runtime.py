from __future__ import annotations

import copy
import io
from pathlib import Path
import stat
import tempfile
import types
import unittest
from unittest import mock
import zipfile

import check_training_export as checker
import training_export_runtime as runtime


def package(entries=None):
    entries = entries or [("peft/__init__.py", b'__version__ = "0.18.0"\n'),
                          ("peft-0.18.0.dist-info/METADATA", b"Name: peft\nVersion: 0.18.0\n")]
    target = io.BytesIO()
    with zipfile.ZipFile(target, "w") as archive:
        for name, data in entries:
            archive.writestr(name, data)
    value = target.getvalue()
    return value, {"wheel": runtime.pin(value), "wheel_files": {
        name.filename if isinstance(name, zipfile.ZipInfo) else name: runtime.pin(data) for name, data in entries},
        "max_uncompressed_bytes": 1024, "max_file_bytes": 512}


class ExportRuntimeTest(unittest.TestCase):
    def test_exact_closed_package_and_mutations(self):
        data, profile = package()
        for mutation in ("replace", "extra", "missing"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as raw:
                root = Path(raw)
                runtime.stage_wheel(data, root, profile)
                runtime.verify_overlay(root, profile)
                if mutation == "replace":
                    (root / "peft/__init__.py").write_bytes(b"other package")
                elif mutation == "extra":
                    (root / "peft/foreign.py").write_bytes(b"pass")
                else:
                    (root / "peft/__init__.py").unlink()
                with self.assertRaises(runtime.oracle.ContractError):
                    runtime.verify_overlay(root, profile)

    def test_regular_wheel_identity_and_symlink_rejection(self):
        data, profile = package()
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            path = root / "peft.whl"
            path.write_bytes(data)
            self.assertEqual(data, runtime.wheel_bytes(path, profile))
            link = root / "linked.whl"
            link.symlink_to(path)
            with self.assertRaises(OSError):
                runtime.wheel_bytes(link, profile)
            path.write_bytes(data[:-1] + bytes([data[-1] ^ 1]))
            with self.assertRaisesRegex(runtime.oracle.ContractError, "wheel hash differs"):
                runtime.wheel_bytes(path, profile)

    def test_zip_paths_symlinks_duplicate_inventory_and_expansion_are_bounded(self):
        symbolic = zipfile.ZipInfo("peft/link.py")
        symbolic.create_system = 3
        symbolic.external_attr = (stat.S_IFLNK | 0o777) << 16
        for entries in ([('/outside.py', b'pass')], [('peft/../outside.py', b'pass')],
                        [('other/__init__.py', b'pass')], [('peft/hook.pth', b'pass')],
                        [(symbolic, b'../outside.py')]):
            with self.subTest(entries=entries), tempfile.TemporaryDirectory() as raw:
                data, profile = package(entries)
                with self.assertRaises(runtime.oracle.ContractError):
                    runtime.stage_wheel(data, Path(raw), profile)
        data, profile = package()
        for field in ("max_uncompressed_bytes", "max_file_bytes"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as raw:
                altered = copy.deepcopy(profile)
                altered[field] = 1
                with self.assertRaises(runtime.oracle.ContractError):
                    runtime.stage_wheel(data, Path(raw), altered)
        with tempfile.TemporaryDirectory() as raw:
            altered = copy.deepcopy(profile)
            del altered["wheel_files"]["peft/__init__.py"]
            with self.assertRaises(runtime.oracle.ContractError):
                runtime.stage_wheel(data, Path(raw), altered)

    def test_imports_must_resolve_to_exact_isolated_package(self):
        data, profile = package()
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime.stage_wheel(data, root, profile)
            package_module = types.ModuleType("peft")
            package_module.__file__ = str(root / "peft/__init__.py")
            # The verifier examines only relevant module names; no package code
            # is imported or numerical stack substituted by this test.
            with mock.patch.dict(runtime.sys.modules, {"peft": package_module}):
                imports = runtime.verify_imports(root, root, profile)
                self.assertEqual(imports["peft"], "peft/__init__.py")
                package_module.__file__ = str(root.parent / "foreign/__init__.py")
                with self.assertRaisesRegex(runtime.oracle.ContractError, "outside isolated wheel"):
                    runtime.verify_imports(root, root, profile)

    def test_profile_selects_only_peft_override_and_pins_other_dependencies(self):
        profile = runtime.load_profile()
        expected = {**profile["original_oracle_runtime"]["packages"], **profile["additional_dependency_pins"], "peft": "0.18.0"}
        metadata = types.SimpleNamespace(get_all=lambda name: profile["metadata_requires_dist"])
        def version(name):
            canonical = name.lower().replace("_", "-")
            return next(value for key, value in expected.items() if key.lower().replace("_", "-") == canonical)
        with mock.patch.object(runtime.importlib.metadata, "version", side_effect=version), \
             mock.patch.object(runtime.importlib.metadata, "distribution", return_value=types.SimpleNamespace(metadata=metadata)), \
             mock.patch.object(runtime.platform, "python_version", return_value=profile["original_oracle_runtime"]["python"]), \
             mock.patch.object(runtime.unicodedata, "unidata_version", profile["original_oracle_runtime"]["unicode"]):
            self.assertEqual(runtime.verify_dependency_profile(profile)["packages"], expected)
            expected["transformers"] = "4.55.5"
            with self.assertRaisesRegex(runtime.oracle.ContractError, "dependency profile differs"):
                runtime.verify_dependency_profile(profile)

    def test_cli_rejects_implicit_or_incomplete_runtime_profiles_before_artifact_reads(self):
        base = ["--variant", "small", "--source-dir", "/absent-source", "--export-dir", "/absent-export", "--output-dir", "/absent-output"]
        invalid = [["--peft-wheel", "/absent-wheel"], ["--runtime-profile", runtime.PROFILE],
                   ["--runtime", "--runtime-profile", runtime.PROFILE],
                   ["--runtime", "--peft-wheel", "/absent-wheel"]]
        for options in invalid:
            with self.subTest(options=options), mock.patch.object(checker, "audit_export") as audit:
                with self.assertRaises(runtime.oracle.ContractError):
                    checker.main(base + options)
                audit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
