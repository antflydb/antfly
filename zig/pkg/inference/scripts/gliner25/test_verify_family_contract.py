# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import hashlib
import base64
import json
import struct
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import benchmark_family_python as benchmark
import benchmark_decide_pairs as pairs
import verify_family_contract as family


def tensor(dtype: str, shape: list[int], start: int) -> dict[str, object]:
    size = 4
    for dim in shape:
        size *= dim
    return {"dtype": dtype, "shape": shape, "data_offsets": [start, start + size]}


class FamilyContractTest(unittest.TestCase):
    def fixture(self, root: Path, profile: str) -> tuple[Path, dict[str, object]]:
        original = family.strict_json(family.CONTRACT_PATH)
        expected = original["models"][profile]
        model = root / profile
        (model / "encoder_config").mkdir(parents=True)
        config = {
            "architecture": expected["architecture"],
            "architectures": [expected["architecture_class"]],
            "config_version": 3,
            "model_name": expected["model_name"],
            "model_type": "extractor",
            **expected["top_level"],
        }
        (model / "config.json").write_text(json.dumps(config), encoding="utf-8")
        encoder = dict(expected["encoder"])
        if profile == "decide_1b":
            encoder["rope_parameters"] = {
                "full_attention": {"rope_theta": 160000.0, "rope_type": "default"},
                "sliding_attention": {"rope_theta": 160000.0, "rope_type": "default"},
            }
        (model / "encoder_config" / "config.json").write_text(
            json.dumps(encoder), encoding="utf-8"
        )
        (model / "tokenizer_config.json").write_text(
            json.dumps(expected["tokenizer"]), encoding="utf-8"
        )
        (model / "tokenizer.json").write_text(
            json.dumps({"version": "1.0", "profile": profile}), encoding="utf-8"
        )
        header = {
            name: tensor(expected["dtype"], shape, index * 4)
            for index, (name, shape) in enumerate(expected["critical_tensors"].items())
        }
        raw = json.dumps(header, separators=(",", ":")).encode()
        (model / "model.safetensors").write_bytes(struct.pack("<Q", len(raw)) + raw)
        generated = json.loads(json.dumps(original))
        item = generated["models"][profile]
        item["model_size_bytes"] = 8 + len(raw)
        item["tensor_header_length"] = len(raw)
        item["tensor_header_sha256"] = hashlib.sha256(raw).hexdigest()
        item["tensor_count"] = len(header)
        item["parameter_count"] = sum(
            __import__("math").prod(spec["shape"]) for spec in header.values()
        )
        item["sidecars"] = {}
        for relative in (
            "config.json",
            "encoder_config/config.json",
            "tokenizer.json",
            "tokenizer_config.json",
        ):
            path = model / relative
            item["sidecars"][relative] = {
                "size_bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        contract = root / "contract.json"
        contract.write_text(json.dumps(generated), encoding="utf-8")
        return model, generated

    def test_all_profiles_validate_their_distinct_architecture_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for profile in ("multi_v1", "multi_decide", "decide_1b"):
                with self.subTest(profile=profile):
                    model, generated = self.fixture(root, profile)
                    contract = root / "contract.json"
                    contract.write_text(json.dumps(generated), encoding="utf-8")
                    result = family.verify_model(profile, model, contract_path=contract)
                    self.assertEqual("verified", result["status"])
                    self.assertFalse(result["qualification"])
                    self.assertEqual(
                        profile != "decide_1b",
                        result["compatibility"]["gliner25_boundary_converter"],
                    )

    def test_wrong_profile_and_tensor_schema_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            model, generated = self.fixture(root, "multi_v1")
            contract = root / "contract.json"
            contract.write_text(json.dumps(generated), encoding="utf-8")
            with self.assertRaisesRegex(family.ContractError, "sidecar"):
                family.verify_model("decide_1b", model, contract_path=contract)

            weights = model / "model.safetensors"
            content = weights.read_bytes()
            raw = content[8:].replace(b"1536", b"1537", 1)
            self.assertNotEqual(content[8:], raw)
            weights.write_bytes(struct.pack("<Q", len(raw)) + raw)
            with self.assertRaisesRegex(family.ContractError, "tensor schema differs"):
                family.verify_model("multi_v1", model, contract_path=contract)

    def test_truncated_or_absurd_header_is_rejected_before_allocation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "model.safetensors"
            path.write_bytes(struct.pack("<Q", family.MAX_HEADER_BYTES + 1))
            with self.assertRaisesRegex(
                family.ContractError, "invalid safetensors header"
            ):
                family.safetensors_header(path)

    def test_unselected_rope_metadata_and_tokenizer_bytes_are_identity_bound(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            model, generated = self.fixture(root, "decide_1b")
            contract = root / "contract.json"
            contract.write_text(json.dumps(generated), encoding="utf-8")

            encoder_path = model / "encoder_config" / "config.json"
            encoder = json.loads(encoder_path.read_text(encoding="utf-8"))
            encoder["rope_parameters"]["full_attention"]["rope_theta"] = 160001.0
            encoder_path.write_text(json.dumps(encoder), encoding="utf-8")
            with self.assertRaisesRegex(
                family.ContractError, "sidecar SHA-256 differs"
            ):
                family.verify_model("decide_1b", model, contract_path=contract)

            model, generated = self.fixture(root, "multi_v1")
            contract.write_text(json.dumps(generated), encoding="utf-8")
            tokenizer = model / "tokenizer.json"
            tokenizer.write_bytes(tokenizer.read_bytes() + b" ")
            with self.assertRaisesRegex(family.ContractError, "sidecar size differs"):
                family.verify_model("multi_v1", model, contract_path=contract)

    def test_json_field_comparison_rejects_bool_as_integer(self) -> None:
        with self.assertRaisesRegex(family.ContractError, "encoder.hidden_size"):
            family._require_fields({"hidden_size": True}, {"hidden_size": 1}, "encoder")


class FamilyBenchmarkContractTest(unittest.TestCase):
    def wheel_tree(self, root: Path) -> tuple[Path, Path]:
        code = root / "package.py"
        code.write_bytes(b"trusted\n")
        info = root / "package-1.0.dist-info"
        info.mkdir()
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(code.read_bytes()).digest())
            .decode()
            .rstrip("=")
        )
        record = info / "RECORD"
        record.write_text(
            f"package.py,sha256={digest},{code.stat().st_size}\n"
            "package-1.0.dist-info/RECORD,,\n",
            encoding="utf-8",
        )
        return code, record

    def test_runtime_tree_rejects_code_tampering_and_unrecorded_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            code, _ = self.wheel_tree(root)
            identity = benchmark.verify_record_tree(root, None)
            self.assertEqual(2, identity["files"])
            benchmark.verify_record_tree(root, identity["tree_sha256"])
            extra = root / "unrecorded.py"
            extra.write_bytes(b"untrusted\n")
            with self.assertRaisesRegex(benchmark.BenchmarkError, "unrecorded"):
                benchmark.verify_record_tree(root, identity["tree_sha256"])
            extra.unlink()
            code.write_bytes(b"changed\n")
            with self.assertRaisesRegex(benchmark.BenchmarkError, "hash differs"):
                benchmark.verify_record_tree(root, identity["tree_sha256"])

    def test_runtime_records_reject_path_escape_and_duplicate_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, record = self.wheel_tree(root)
            original = record.read_text()
            for extra in (
                original.splitlines()[0] + "\n",
                "../escape.py,sha256=invalid,1\n",
            ):
                record.write_text(original + extra)
                with self.assertRaisesRegex(
                    benchmark.BenchmarkError, "escapes or is duplicated"
                ):
                    benchmark.verify_record_tree(root, None)

    def test_isolated_runtime_rejects_wrong_distribution_inventory(self) -> None:
        contract = benchmark.strict_json(benchmark.RUNTIME_CONTRACT_1B)
        expected = contract["oracle_runtime_decide_1b"]
        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.object(
                benchmark.platform, "python_version", return_value=expected["python"]
            ),
            mock.patch.object(
                benchmark.unicodedata, "unidata_version", expected["unicode"]
            ),
            mock.patch.object(
                benchmark.importlib.metadata, "distributions", return_value=[]
            ),
        ):
            with self.assertRaisesRegex(benchmark.BenchmarkError, "inventory differs"):
                benchmark.verify_runtime_dir(Path(tmp), contract)

    def test_peft_shim_only_supplies_inactive_types(self) -> None:
        names = ("peft", "peft.tuners", "peft.tuners.lora", "peft.tuners.lora.layer")
        saved = {name: sys.modules.get(name) for name in names}
        try:
            for name in names:
                sys.modules.pop(name, None)
            with mock.patch.object(
                benchmark.importlib.util, "find_spec", return_value=None
            ):
                identity = benchmark.install_inference_peft_shim()
            self.assertFalse(identity["executed_peft_code"])
            self.assertFalse(isinstance(object(), sys.modules["peft"].PeftModel))
            self.assertFalse(
                isinstance(object(), sys.modules["peft.tuners.lora.layer"].LoraLayer)
            )
        finally:
            for name in names:
                sys.modules.pop(name, None)
                if saved[name] is not None:
                    sys.modules[name] = saved[name]

    def test_capture_selection_rejects_unqualified_profile_task(self) -> None:
        for profile, task in benchmark.CAPTURES:
            _, capture, rows = benchmark.load_capture(profile, task)
            self.assertFalse(capture["qualification"])
            self.assertTrue(rows)
        with self.assertRaises(benchmark.UnsupportedBenchmark):
            benchmark.load_capture("multi_v1", "decide")

    def test_decide_schema_restores_label_order_and_rejects_missing_label(self) -> None:
        _, _, rows = benchmark.load_capture("multi_decide", "decide")
        row = json.loads(json.dumps(rows[0]))
        for task in row["schema"]["tasks"].values():
            task["labels"] = dict(reversed(list(task["labels"].items())))
        schema = benchmark.ordered_decide_schema(row)
        evidence = row["native_classification"]["tasks"]
        self.assertEqual([task["name"] for task in evidence], list(schema["tasks"]))
        for task in evidence:
            self.assertEqual(
                task["labels"], list(schema["tasks"][task["name"]]["labels"])
            )
        first = next(iter(row["schema"]["tasks"].values()))
        first["labels"].pop(next(iter(first["labels"])))
        with self.assertRaisesRegex(benchmark.BenchmarkError, "inventory differs"):
            benchmark.ordered_decide_schema(row)

    def test_extraction_schema_restores_entity_order(self) -> None:
        _, _, rows = benchmark.load_capture("multi_decide", "extract")
        row = json.loads(json.dumps(rows[0]))
        row["schema"]["entities"] = dict(
            reversed(list(row["schema"]["entities"].items()))
        )
        self.assertEqual(
            row["native_schema"]["entities"],
            list(benchmark.ordered_extract_schema(row)["entities"]),
        )
        row["schema"]["entities"].pop(next(iter(row["schema"]["entities"])))
        with self.assertRaisesRegex(benchmark.BenchmarkError, "inventory differs"):
            benchmark.ordered_extract_schema(row)

    def test_preflight_word_limit_is_preserved_before_collation(self) -> None:
        model = mock.Mock()
        model.processor.word_splitter.return_value = ["word"] * 129
        with self.assertRaisesRegex(benchmark.BenchmarkError, "128 words"):
            benchmark.encoded_evidence(model, "text", {}, boundary=True)
        model.processor.collate_fn_inference.assert_not_called()

    def test_canonical_output_preserves_selected_labels_and_source_offsets(
        self,
    ) -> None:
        schema = {"classifications": [{"name": "intent"}], "entities": ["person"]}
        output = {
            "intent": {
                "value": "refund",
                "probabilities": {"refund": 0.8, "sales": 0.2},
            },
            "entities": {
                "person": [{"text": "María", "confidence": 0.9, "start": 0, "end": 5}]
            },
        }
        expected = benchmark.canonical_expected({}, schema, output)
        self.assertEqual(
            [{"label": "refund", "confidence": 0.8}],
            expected["classifications"][0]["labels"],
        )
        self.assertEqual(
            {"start": 0, "end": 5}, expected["entities"][0]["values"][0]["source"]
        )

    def test_decide_dispatch_uses_decide_preprocessor(self) -> None:
        extract = mock.Mock()
        decide = mock.Mock(return_value="decision")
        prepare = benchmark.select_prepare("decide", extract, decide)
        self.assertEqual("decision", prepare({"id": "choice"}))
        extract.assert_not_called()
        decide.assert_called_once_with({"id": "choice"})


class PortableDecideRuntimeTest(unittest.TestCase):
    def fixture(self, root: Path):
        import csv
        import io
        import zipfile
        wheel_dir = root / "wheels"
        wheel_dir.mkdir()
        payload = {"portable.py": b"trusted = True\n", "portable-1.0.dist-info/METADATA": b"Metadata-Version: 2.4\nName: portable\nVersion: 1.0\n"}
        record = "portable-1.0.dist-info/RECORD"
        output = io.StringIO(newline="")
        writer = csv.writer(output, lineterminator="\n")
        for path, raw in payload.items():
            digest = base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b"=").decode()
            writer.writerow([path, "sha256=" + digest, len(raw)])
        writer.writerow([record, "", ""])
        payload[record] = output.getvalue().encode()
        path = wheel_dir / "portable-1.0-py3-none-any.whl"
        with zipfile.ZipFile(path, "w") as archive:
            for name, raw in payload.items():
                archive.writestr(name, raw)
        files = {name: {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw), "wheel": path.name, "mode": 0o600} for name, raw in payload.items()}
        scope = {"packages": {"portable": "1.0"}, "wheels": [{"name": "portable", "version": "1.0", "filename": path.name, "bytes": path.stat().st_size, "sha256": benchmark.sha256(path), "record_sha256": files[record]["sha256"], "metadata_sha256": files["portable-1.0.dist-info/METADATA"]["sha256"]}], "files": files}
        installed = root / "runtime"
        for name, raw in payload.items():
            destination = installed / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(raw)
            destination.chmod(0o600)
        return wheel_dir, path, installed, scope

    def test_portable_artifact_and_full_payload_authentication(self):
        with tempfile.TemporaryDirectory() as tmp:
            wheels, wheel, installed, scope = self.fixture(Path(tmp))
            self.assertEqual(scope["files"], benchmark._portable_wheels(wheels, scope))
            self.assertEqual({"files": 3}, benchmark._portable_tree(installed, scope["files"]))
            (installed / "portable.py").chmod(0o700)
            with self.assertRaisesRegex(benchmark.BenchmarkError, "identity differs"):
                benchmark._portable_tree(installed, scope["files"])
            (installed / "portable.py").chmod(0o600)
            (installed / "portable.py").write_bytes(b"trusted = False\n")
            with self.assertRaisesRegex(benchmark.BenchmarkError, "identity differs"):
                benchmark._portable_tree(installed, scope["files"])
            wheel.write_bytes(wheel.read_bytes() + b"tamper")
            with self.assertRaisesRegex(benchmark.BenchmarkError, "artifact identity"):
                benchmark._portable_wheels(wheels, scope)

    def test_portable_rejects_installer_record_rewrite_extra_and_symlink(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, _, installed, scope = self.fixture(Path(tmp))
            extra = installed / "sitecustomize.py"
            extra.write_bytes(b"untrusted\n")
            with self.assertRaisesRegex(benchmark.BenchmarkError, "inventory differs"):
                benchmark._portable_tree(installed, scope["files"])
            extra.unlink()
            code = installed / "portable.py"
            raw = code.read_bytes()
            code.unlink()
            outside = Path(tmp) / "outside.py"
            outside.write_bytes(raw)
            code.symlink_to(outside)
            with self.assertRaisesRegex(benchmark.BenchmarkError, "symlink"):
                benchmark._portable_tree(installed, scope["files"])
            code.unlink()
            code.write_bytes(raw)
            code.chmod(0o600)
            record = installed / "portable-1.0.dist-info/RECORD"
            record.write_bytes(record.read_bytes() + b"INSTALLER,sha256=notallowed,1\n")
            with self.assertRaisesRegex(benchmark.BenchmarkError, "identity differs"):
                benchmark._portable_tree(installed, scope["files"])

    def test_portable_rejects_recipe_metadata_forgery_and_package_collision(self):
        with tempfile.TemporaryDirectory() as tmp:
            wheels, _, _, scope = self.fixture(Path(tmp))
            saved = scope["wheels"][0]["record_sha256"]
            scope["wheels"][0]["record_sha256"] = "0" * 64
            with self.assertRaisesRegex(benchmark.BenchmarkError, "metadata identity"):
                benchmark._portable_wheels(wheels, scope)
            scope["wheels"][0]["record_sha256"] = saved
            scope["wheels"].append(scope["wheels"][0])
            with self.assertRaisesRegex(benchmark.BenchmarkError, "package identity"):
                benchmark._portable_wheels(wheels, scope)

    def test_portable_rejects_archive_paths_even_with_matching_artifact_hash(self):
        import zipfile
        with tempfile.TemporaryDirectory() as tmp:
            wheels, wheel, _, scope = self.fixture(Path(tmp))
            with zipfile.ZipFile(wheel, "a") as archive:
                archive.writestr("../escaped.py", b"untrusted")
            scope["wheels"][0].update(bytes=wheel.stat().st_size, sha256=benchmark.sha256(wheel))
            with self.assertRaisesRegex(benchmark.BenchmarkError, "path escapes"):
                benchmark._portable_wheels(wheels, scope)

    def test_paired_runtime_contract_parser_and_exact_command_forwarding(self):
        import argparse
        required = ["--binary", "binary", "--build-receipt", "build.json", "--model-dir", "model",
                    "--upstream", "source", "--runtime-dir", "runtime", "--output", "output", "--backend", "native"]
        self.assertIsNone(pairs.parse_args(required).runtime_contract)
        explicit = pairs.parse_args(required + ["--runtime-contract", "runtime-v2.json"])
        self.assertEqual(Path("runtime-v2.json").resolve(), explicit.runtime_contract)
        for selected in (None, Path("runtime-v2.json").resolve()):
            with tempfile.TemporaryDirectory() as tmp:
                args = argparse.Namespace(python=Path("python"), backend="native", model_dir=Path("model"),
                                          upstream=Path("source"), runtime_dir=Path("runtime"), runtime_contract=selected, timeout=1)
                captured = []
                def child(command, env, log, timeout):
                    captured.append(command)
                    return 1  # Retain the deliberate child failure; launch nothing.
                with mock.patch.object(pairs.perf, "source_identity", return_value={"source": "fixed"}), \
                     mock.patch.object(pairs.perf, "host_context", return_value={}), \
                     mock.patch.object(pairs.perf, "run_child", side_effect=child), \
                     mock.patch.object(pairs.perf, "parse_time", return_value={}), \
                     mock.patch.object(Path, "read_text", return_value=""):
                    receipt = pairs.run_python(args, Path(tmp) / "out")
                self.assertFalse(receipt["passed"])
                if selected is None:
                    self.assertNotIn("--runtime-contract", captured[0])
                else:
                    index = captured[0].index("--runtime-contract")
                    self.assertEqual(str(selected), captured[0][index + 1])

    def test_portable_module_resolution_rejects_same_version_foreign_source(self):
        root = Path("runtime-root").resolve()
        runtime = root / "runtime"
        upstream = root / "upstream"
        external = root / "venv/lib/python3.12/site-packages"
        origins = {"torch": external / "torch/__init__.py", "pydantic": external / "pydantic/__init__.py",
                   "transformers": runtime / "transformers/__init__.py", "gliner2": upstream / "gliner2/__init__.py"}
        origins.update({name: external / name / "__init__.py" for name in ("requests", "urllib3", "charset_normalizer")})
        def find(name):
            return mock.Mock(origin=str(origins[name]))
        with mock.patch.object(benchmark.importlib.util, "find_spec", side_effect=find), mock.patch.object(benchmark, "sys", mock.Mock(modules={})), mock.patch.object(benchmark, "verify_source", return_value={}):
            benchmark.verify_portable_import_sources(runtime, upstream)
            origins["torch"] = root / "foreign/torch/__init__.py"
            with self.assertRaisesRegex(benchmark.BenchmarkError, "import source differs"):
                benchmark.verify_portable_import_sources(runtime, upstream)

    def python_archive(self, root, escape=False):
        import tarfile
        import io
        path = root / "python.tar.gz"
        with tarfile.open(path, "w:gz") as archive:
            for name, raw in [("python/bin/python3.12", b"authenticated executable"), ("python/lib/__pycache__/ignored.pyc", b"official precompiled bytecode")]:
                member = tarfile.TarInfo(name)
                member.size = len(raw)
                member.mode = 0o755 if name.endswith("python3.12") else 0o644
                archive.addfile(member, io.BytesIO(raw))
            link = tarfile.TarInfo("python/bin/python")
            link.type = tarfile.SYMTYPE
            link.linkname = "../../outside" if escape else "python3.12"
            archive.addfile(link)
        return path, {"archive": {"filename": path.name, "bytes": path.stat().st_size, "sha256": benchmark.sha256(path)}, "omit_python_bytecode": True}

    def test_clean_python_is_derived_from_exact_archive_without_cached_bytecode(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive, recipe = self.python_archive(root)
            prefix = root / "prefix"
            facts = benchmark._portable_python(archive, recipe, prefix)
            self.assertEqual({"bin/python", "bin/python3.12"}, set(facts))
            self.assertFalse(any(prefix.rglob("*.pyc")))
            benchmark._portable_prefix(prefix, facts)
            (prefix / "bin/python3.12").chmod(0o644)
            with self.assertRaisesRegex(benchmark.BenchmarkError, "identity differs"):
                benchmark._portable_prefix(prefix, facts)

    def test_python_archive_rejects_unmatched_artifact_and_escaping_link(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive, recipe = self.python_archive(root, escape=True)
            with self.assertRaisesRegex(benchmark.BenchmarkError, "symlink escapes"):
                benchmark._portable_python(archive, recipe)
            recipe["archive"]["sha256"] = "0" * 64
            with self.assertRaisesRegex(benchmark.BenchmarkError, "archive identity"):
                benchmark._portable_python(archive, recipe)

    def source_dependency_fixture(self, root):
        wheels, _, _, scope = self.fixture(root)
        text = b'[project]\ndependencies=["portable>=1,<2"]\n'
        project = wheels / "pyproject.toml"
        project.write_bytes(text)
        recipe = {"source_dependencies": {"revision": benchmark.strict_json(benchmark.CONTRACT)["upstream_python"]["revision"],
                  "pyproject": {"filename": project.name, "bytes": len(text), "sha256": benchmark.sha256(project)},
                  "base_requirements": ["portable>=1,<2"], "local_transformers_exception": {}},
                  "wheels": {"isolated": scope}}
        return project, recipe

    def test_source_base_requirements_and_wheel_metadata_reach_authenticated_checker(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project, recipe = self.source_dependency_fixture(root)
            with mock.patch.object(benchmark, "_portable_dependency_check", return_value={"model_imports": False}) as child, mock.patch.object(benchmark.subprocess, "run") as process:
                result = benchmark._portable_source_dependencies(root, recipe)
                process.assert_not_called()
            self.assertFalse(result["model_imports"])
            payload = child.call_args.args[0]
            self.assertEqual(["portable>=1,<2"], payload["base"])
            self.assertEqual({"portable": "1.0"}, payload["versions"])
            self.assertEqual(str(project), payload["pyproject"])
            self.assertEqual(["portable-1.0-py3-none-any.whl"], [wheel["filename"] for wheel in payload["wheels"]])
            project.write_bytes(project.read_bytes() + b"# tampered\n")
            with mock.patch.object(benchmark.subprocess, "run") as child:
                with self.assertRaisesRegex(benchmark.BenchmarkError, "dependency artifact differs"):
                    benchmark._portable_source_dependencies(root, recipe)
                child.assert_not_called()

    def test_source_dependency_refusal_propagates_without_runtime_or_golden_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, recipe = self.source_dependency_fixture(root)
            with mock.patch.object(benchmark, "_portable_dependency_check", side_effect=benchmark.BenchmarkError("unsatisfied pinned GLiNER BASE requirement")):
                with self.assertRaisesRegex(benchmark.BenchmarkError, "unsatisfied"):
                    benchmark._portable_source_dependencies(root, recipe)

    def test_torch_virtual_namespaces_require_definition_type_and_registered_singleton(self):
        import types
        root = Path("authenticated/torch").resolve()
        for name, definition_name, class_name, export, filename in [
            ("torch.ops", "torch._ops", "_Ops", "ops", "_ops.py"),
            ("torch.classes", "torch._classes", "_Classes", "classes", "_classes.py"),
        ]:
            definition = types.ModuleType(definition_name)
            definition.__file__ = str(root / filename)
            namespace_type = type(class_name, (types.ModuleType,), {"__module__": definition_name, "__file__": filename})
            setattr(definition, class_name, namespace_type)
            namespace = namespace_type(name)
            setattr(definition, export, namespace)
            package = types.ModuleType("torch")
            setattr(package, export, namespace)
            modules = {"torch": package, definition_name: definition, name: namespace}
            with mock.patch.object(benchmark, "sys", mock.Mock(modules=modules)):
                self.assertTrue(benchmark._portable_torch_virtual(name, namespace, root))
                replacement = namespace_type(name)
                modules[name] = replacement
                with self.assertRaisesRegex(benchmark.BenchmarkError, "registration differs"):
                    benchmark._portable_torch_virtual(name, replacement, root)
                foreign = types.ModuleType(name)
                foreign.__file__ = str(root / filename)
                modules[name] = foreign
                with self.assertRaisesRegex(benchmark.BenchmarkError, "registration differs"):
                    benchmark._portable_torch_virtual(name, foreign, root)
                modules[name] = namespace
                definition.__file__ = str(root.parent / "foreign" / filename)
                with self.assertRaisesRegex(benchmark.BenchmarkError, "registration differs"):
                    benchmark._portable_torch_virtual(name, namespace, root)
                self.assertFalse(benchmark._portable_torch_virtual("torch.ops.arbitrary", namespace, root))

    def test_v2_dispatch_is_explicit_and_v1_default_unchanged(self):
        with mock.patch.object(benchmark, "verify_portable_runtime_dir", return_value={"v2": True}) as portable:
            self.assertEqual({"v2": True}, benchmark.verify_runtime_dir(Path("runtime"), {"format_version": 2}))
            portable.assert_called_once()
        contract = benchmark.strict_json(benchmark.RUNTIME_CONTRACT_1B)
        with mock.patch.object(benchmark.platform, "python_version", return_value="wrong"):
            with self.assertRaisesRegex(benchmark.BenchmarkError, "Python/Unicode"):
                benchmark.verify_runtime_dir(Path("runtime"), contract)

    def test_v2_cannot_change_frozen_numerical_or_version_contract(self):
        original = benchmark.strict_json(benchmark.RUNTIME_CONTRACT_1B)
        for key in ("python", "unicode", "isolated_packages", "external_packages", "rope_contract", "transformers_distribution"):
            changed = json.loads(json.dumps(original))
            changed["format_version"] = 2
            changed["oracle_runtime_decide_1b"][key] = "changed"
            with self.subTest(key=key), self.assertRaisesRegex(benchmark.BenchmarkError, "changes frozen contract"):
                benchmark.verify_runtime_dir(Path("runtime"), changed)


if __name__ == "__main__":
    unittest.main()
