# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Exact input tokens must agree, even when prompt lengths happen to match."""
import unittest
from unittest.mock import patch
import tempfile
from pathlib import Path
from argparse import Namespace

import preflight_text_tokens as preflight
import qualify_transcription as qualification


class PromptPreflightTests(unittest.TestCase):
    def setUp(self):
        self.request = qualification.generation_request("model", 256, prompt="same question")
        self.reference = {"engine": "native", "passed": True, "token_ids": [2, 7, 9],
                          "prompt_identity_sha256": preflight.prompt_identity(self.request),
                          "token_bytes_sha256": preflight.token_identity([2, 7, 9])}

    def test_equal_lengths_cannot_hide_different_token_ids(self):
        self.assertEqual(preflight.require_match([2, 7, 9], self.reference, self.request), self.reference["token_bytes_sha256"])
        with self.assertRaisesRegex(ValueError, "index 1"):
            preflight.require_match([2, 8, 9], self.reference, self.request)
        with self.assertRaisesRegex(ValueError, "index 3"):
            preflight.require_match([2, 7, 9, 10], self.reference, self.request)

    def test_changed_request_or_reference_fails_before_timing(self):
        changed = qualification.generation_request("model", 256, prompt="different question")
        with self.assertRaisesRegex(ValueError, "messages"):
            preflight.require_match([2, 7, 9], self.reference, changed)
        self.reference["token_ids"] = [2, 8, 9]
        with self.assertRaisesRegex(ValueError, "changed"):
            preflight.require_match([2, 8, 9], self.reference, self.request)

    @patch.object(preflight, "extract_tokens", return_value={"token_ids": [2, 7, 9]})
    def test_native_reuse_verifies_same_binary_model_and_checkpoint_without_http(self, extract):
        provenance = {"binary_sha256": "binary", "model_sha256": "weights", "model": "model"}
        self.reference.update(source_provenance=provenance, checkpoint="capture.kv",
                              checkpoint_sha256_to_reverify="filehash", identity={"weights": "identity"})
        fixture = preflight.reuse_native(self.reference, self.request, provenance)
        self.assertIn("untimed", fixture["capture_kind"])
        extract.assert_called_once_with("capture.kv", "filehash", {"weights": "identity"})
        for key in provenance:
            with self.assertRaisesRegex(ValueError, key):
                preflight.reuse_native(self.reference, self.request, {**provenance, key: "changed"})
        with self.assertRaisesRegex(ValueError, "binary_sha256"):
            preflight.reuse_native(self.reference, self.request, {})

    def test_cli_parser_requires_exactly_one_token_record_and_template_confirmation(self):
        output = "model loaded\nchat_template=true\nprompt:\n<bos>same question\nprompt_token_ids: 2 7 9\nanswer\n"
        tokens, rendered = preflight.parse_cli_tokens(output)
        self.assertEqual(tokens, [2, 7, 9])
        self.assertEqual(rendered, "<bos>same question")
        for bad in (output.replace("true", "false"), output + "prompt_token_ids: 2 7 9\n", output.replace("2 7 9", "2 invalid 9")):
            with self.assertRaises(ValueError):
                preflight.parse_cli_tokens(bad)

    def test_checkpoint_binding_accepts_single_decoder_and_separate_projector(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = root / "model.gguf"
            model.write_bytes(b"GGUF decoder")
            (root / "mmproj-Q8_0.gguf").write_bytes(b"projector")
            self.assertEqual(preflight.native_gguf_checkpoint(root, model), model)
            wrong = root / "mmproj-Q8_0.gguf"
            with self.assertRaisesRegex(ValueError, "differs from"):
                preflight.native_gguf_checkpoint(root, wrong)

    def test_checkpoint_binding_rejects_competing_decoders_formats_and_overrides(self):
        for extra in ("other.gguf", "model.safetensors", "model.safetensors.index.json", "antfly_inference_bundle.json", ".antfly-download-complete.json"):
            with self.subTest(extra=extra), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                model = root / "model.gguf"
                model.write_bytes(b"GGUF decoder")
                (root / extra).write_text("{}")
                with self.assertRaises(ValueError):
                    preflight.native_gguf_checkpoint(root, model)

    def test_checkpoint_binding_rejects_nested_and_escaping_decoder_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model_dir = root / "model"
            (model_dir / "nested").mkdir(parents=True)
            nested = model_dir / "nested" / "model.gguf"
            nested.write_bytes(b"GGUF decoder")
            with self.assertRaisesRegex(ValueError, "top-level"):
                preflight.native_gguf_checkpoint(model_dir, nested)
            nested.unlink()
            outside = root / "outside.gguf"
            outside.write_bytes(b"GGUF decoder")
            (model_dir / "model.gguf").symlink_to(outside)
            with self.assertRaisesRegex(ValueError, "escapes"):
                preflight.native_gguf_checkpoint(model_dir, outside)

    @patch.object(preflight, "prompt_source_contract", return_value={"shared": "sourcehash"})
    def test_cli_log_reuse_is_verified_without_model_execution(self, contract):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "capture.log"
            log.write_text("chat_template=true\nprompt:\n<bos>same question\nprompt_token_ids: 2 7 9\n")
            provenance = {"binary_sha256": "binary", "model_sha256": "weights", "model": "model"}
            self.reference.update(source_provenance=provenance, native_cli_log=str(log),
                                  native_cli_log_sha256=preflight.q.file_sha(log),
                                  native_cli_source_root="cli-source", native_server_source_root="server-source",
                                  prompt_sources={"shared": "sourcehash"})
            result = preflight.reuse_native(self.reference, self.request, provenance)
            self.assertEqual(result["token_ids"], [2, 7, 9])
            log.write_text(log.read_text().replace("2 7 9", "2 8 9"))
            with self.assertRaisesRegex(ValueError, "CLI log changed"):
                preflight.reuse_native(self.reference, self.request, provenance)

    @patch.object(preflight.q, "file_sha", return_value="weights")
    @patch.object(preflight, "prompt_source_contract", side_effect=[{"shared": "old"}, {"shared": "changed"}])
    def test_cli_source_mismatch_rejects_before_launching_model(self, contract, sha):
        args = Namespace(native_cli_source_root=Path("cli"), native_server_source_root=Path("server"))
        with patch.object(preflight.subprocess, "run") as launch:
            with self.assertRaisesRegex(ValueError, "source mismatch"):
                preflight.capture_native_cli(args, self.request, {"binary_sha256": "binary", "model_sha256": "weights"})
            launch.assert_not_called()

    @patch.object(preflight.q, "file_sha", return_value="weights")
    @patch.object(preflight, "prompt_source_contract", return_value={"shared": "sourcehash"})
    def test_cli_capture_retains_provenance_and_exact_input_without_checkpoint(self, contract, sha):
        with tempfile.TemporaryDirectory() as directory:
            args = Namespace(native_cli=Path(directory) / "antfly", native_cli_source_root=Path("cli"),
                             native_server_source_root=Path("server"), model_file=Path(directory) / "model.gguf",
                             native_cli_prefix_json='["inference"]', native_cli_args_json='["--host-budget-mb", "8192"]',
                             native_cli_backend="native", output=Path(directory) / "preflight.json")
            args.model_file.write_bytes(b"GGUF decoder")
            def run(command, **kwargs):
                kwargs["stdout"].write(b"chat_template=true\nprompt:\n<bos>same question\nprompt_token_ids: 2 7 9\n")
                return Namespace(returncode=0)
            with patch.object(preflight.subprocess, "run", side_effect=run) as launch:
                result = preflight.capture_native_cli(args, self.request, {"binary_sha256": "binary", "model_sha256": "weights"})
            command = launch.call_args.args[0]
            self.assertEqual(command[1:3], ["inference", "generate"])
            self.assertEqual(command[4], "same question")
            self.assertEqual(command[command.index("--max-tokens") + 1], "0")
            self.assertEqual(result["token_ids"], [2, 7, 9])
            self.assertEqual(result["prompt_sources"], {"shared": "sourcehash"})

    @patch.object(preflight, "post_json", return_value={"count": 3, "tokens": [2, 7, 9]})
    def test_vllm_chat_tokenization_avoids_duplicate_bos_and_private_request_fields(self, post):
        tokens, _ = preflight.reference_tokens("vllm", "http://localhost", self.request)
        self.assertEqual(tokens, [2, 7, 9])
        body = post.call_args.args[2]
        self.assertEqual(post.call_args.args[1], "/tokenize")
        self.assertFalse(body["add_special_tokens"])
        self.assertTrue(body["add_generation_prompt"])
        self.assertEqual(body["chat_template_kwargs"], {"enable_thinking": False})
        self.assertNotIn("backend", body)
        self.assertNotIn("stream", body)

    @patch.object(preflight, "post_json", side_effect=[{"prompt": "<bos>rendered"}, {"tokens": [2, 7, 9]}])
    def test_llama_checks_actual_server_template_then_completion_tokenizer(self, post):
        tokens, evidence = preflight.reference_tokens("llama", "http://localhost", self.request)
        self.assertEqual(tokens, [2, 7, 9])
        self.assertEqual(post.call_args_list[0].args[1], "/apply-template")
        self.assertEqual(post.call_args_list[1].args[2], {"content": "<bos>rendered", "add_special": True, "parse_special": True})
        self.assertIn("rendered_prompt_sha256", evidence)


if __name__ == "__main__":
    unittest.main()
