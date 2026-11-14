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

import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from qualify_cpu_media import (
    validate_read,
    validate_timeout,
    main,
    request,
    MemoryGuard,
    pinned_installation,
    installation_manifest,
    run_case,
)


class InstallationAssertions(unittest.TestCase):
    def test_execution_inventory_contains_exact_weights_and_supporting_assets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            for name in ("model.safetensors", "tokenizer.json", "config.json"):
                (source / name).write_text(name)
            args = SimpleNamespace(models_dir=root, report=root / "report.json")
            case = dict(
                model="owner/model", checkpoint_dir=source, revision="unverified-label"
            )
            with pinned_installation(args, case) as (inventory, identity):
                self.assertEqual(3, len(identity["assets"]))
                self.assertEqual("unverified-label", identity["declared_revision"])
                self.assertNotIn("checkpoint_revision", identity)
                self.assertEqual(
                    (source / "model.safetensors").stat().st_ino,
                    (inventory / "owner/model/model.safetensors").stat().st_ino,
                )
                self.assertEqual(identity["assets"], installation_manifest(inventory))
            self.assertFalse(inventory.exists())

    def test_asset_mutation_invalidates_an_otherwise_successful_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            (source / "model.safetensors").write_text("weights")
            metadata = source / "tokenizer.json"
            metadata.write_text("original")
            args = SimpleNamespace(models_dir=root, report=root / "report.json")
            case = dict(model="owner/model", checkpoint_dir=source)

            def execute(*unused):
                metadata.write_text("changed")
                return dict(success=True)

            with patch("qualify_cpu_media.run_pinned_case", side_effect=execute):
                result = run_case(args, case, 0)
            self.assertFalse(result["success"])
            self.assertFalse(result["installation_verified_after_execution"])

    def test_composition_cannot_resolve_a_different_unhashed_leaf(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "model.safetensors").write_text("weights")
            definition = root / "document.json"
            definition.write_text(json.dumps({"models": [{"model": "other/model"}]}))
            args = SimpleNamespace(models_dir=root, report=root / "report.json")
            case = dict(
                model="reader/composite",
                preload_model="owner/model",
                checkpoint_dir=root,
                model_definition=definition,
            )
            with self.assertRaisesRegex(ValueError, "pinned leaf"):
                with pinned_installation(args, case):
                    self.fail("unhashed model accepted")


class MemoryAssertions(unittest.TestCase):
    def test_abort_is_terminal_even_after_the_last_successful_response(self):
        guard = MemoryGuard(1)
        self.assertTrue(guard.observe({"pss_bytes": 2 * 1024**2}))
        result = dict(success=True)
        guard.finish(result)
        self.assertFalse(result["success"])
        self.assertIn("PSS", result["error"])
        guard.finish(result)
        self.assertFalse(result["success"])


class CompletionAssertions(unittest.TestCase):
    def test_pdf_text_layer_does_not_qualify_native_ocr_execution(self):
        response = {
            "data": [{"status": "complete", "text": "header footer"}],
            "usage": {"model_calls": 0},
        }
        with self.assertRaisesRegex(ValueError, "did not execute"):
            validate_read(response, ["header", "footer"], require_model_execution=True)
        response["usage"]["model_calls"] = 1
        validate_read(response, ["header", "footer"], require_model_execution=True)

    def test_success_requires_complete_output_and_known_answer(self):
        for response in (
            {},
            {"data": []},
            {"data": [{"text": "last phrase"}]},
            {"data": [{"status": "partial", "text": "last phrase"}]},
            {
                "data": [
                    {"status": "complete", "text": "last phrase", "error": "provider"}
                ]
            },
            {
                "data": [
                    {"status": "complete", "text": "last phrase", "truncated": True}
                ]
            },
            {
                "data": [
                    {
                        "status": "complete",
                        "text": "last phrase",
                        "finish_reason": "length",
                    }
                ]
            },
            {"data": [{"status": "complete", "text": "first window only"}]},
            {
                "document": {"status": "unresolved"},
                "data": [{"status": "complete", "text": "last phrase"}],
            },
            {
                "truncated": True,
                "data": [{"status": "complete", "text": "last phrase"}],
            },
        ):
            with self.subTest(response=response), self.assertRaises(ValueError):
                validate_read(response, ["last phrase"])
        result = {
            "data": [
                {"status": "complete", "text": "FIRST window"},
                {"status": "complete", "text": "last\nphrase"},
            ]
        }
        self.assertEqual(
            "first window last phrase",
            validate_read(result, ["first window", "last phrase"]),
        )
        with self.assertRaises(ValueError):
            validate_read(result, [])


class DeadlineAssertions(unittest.TestCase):
    def test_transport_grace_does_not_extend_execution_budget(self):
        with patch("qualify_cpu_media.http.client.HTTPConnection") as connect:
            connection = connect.return_value
            connection.getresponse.return_value.status = 504
            connection.getresponse.return_value.read.return_value = (
                b'{"error":"Timeout"}'
            )
            status, body = request(8090, "/read", {}, 60, response_grace=30)
            connect.assert_called_once_with("127.0.0.1", 8090, timeout=90)
            headers = connection.request.call_args.args[3]
            self.assertEqual("60000", headers["X-Antfly-Inference-Timeout-Ms"])
            validate_timeout(status, body)
            connection.close.assert_called_once()

    def test_only_known_timeout_contracts_pass(self):
        for code in ("INFERENCE_TIMEOUT", "Timeout"):
            validate_timeout(504, {"error": code})
        for status, code in (
            (200, "Timeout"),
            (503, "Timeout"),
            (504, "DocumentBudgetExhausted"),
            (504, "MODEL_RESOURCE_BUSY"),
            (504, "unknown"),
        ):
            with self.subTest(status=status, code=code), self.assertRaises(ValueError):
                validate_timeout(status, {"error": code})


class ReportAssertions(unittest.TestCase):
    def test_missing_checkpoint_is_an_explicit_failed_campaign_case(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, cases, report = (
                root / name for name in ("config.json", "cases.json", "report.json")
            )
            config.write_text("{}")
            cases.write_text(json.dumps([{"model": "reader", "kind": "image"}]))
            argv = [
                "probe",
                "--binary",
                __file__,
                "--config",
                str(config),
                "--models-dir",
                str(root),
                "--cases",
                str(cases),
                "--report",
                str(report),
            ]
            with (
                patch("sys.argv", argv),
                patch(
                    "qualify_cpu_media.run_case",
                    side_effect=ValueError("missing checkpoint"),
                ),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(1, main())
            recorded = json.loads(report.read_text())["cases"]
            self.assertEqual(1, len(recorded))
            self.assertFalse(recorded[0]["success"])
            self.assertEqual("missing checkpoint", recorded[0]["error"])


if __name__ == "__main__":
    unittest.main()
