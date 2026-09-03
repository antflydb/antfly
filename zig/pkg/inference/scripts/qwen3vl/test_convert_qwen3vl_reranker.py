#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))

import convert_qwen3vl_reranker as conversion


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ConversionTests(unittest.TestCase):
    def make_source(self, root: Path) -> Path:
        source = root / "source"
        files = {
            "model.safetensors": b"weights",
            **{asset: asset.encode() for asset in conversion.ASSETS},
        }
        artifacts = []
        for relative, data in files.items():
            path = source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            artifacts.append({"path": relative, "size": len(data), "sha256": digest(data)})
        (source / conversion.RECEIPT_NAME).write_text(
            json.dumps(
                {"version": 2, "source": conversion.SOURCE_IDENTITY, "artifacts": artifacts}
            ),
            encoding="utf-8",
        )
        return source

    def test_validate_source_accepts_exact_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            source = self.make_source(Path(raw))
            result = conversion.validate_source(source)
            self.assertEqual(result["root"], source.resolve())
            self.assertEqual(result["artifacts"]["model.safetensors"]["sha256"], digest(b"weights"))

    def test_validate_source_rejects_unreceipted_file(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            source = self.make_source(Path(raw))
            (source / "surprise.bin").write_bytes(b"unexpected")
            with self.assertRaisesRegex(conversion.ConversionError, "receipt/file set mismatch"):
                conversion.validate_source(source)

    def test_validate_source_rejects_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = self.make_source(root)
            target = root / "outside"
            target.write_bytes(b"weights")
            (source / "model.safetensors").unlink()
            (source / "model.safetensors").symlink_to(target)
            with self.assertRaisesRegex(conversion.ConversionError, "not a regular file"):
                conversion.validate_source(source)

    def test_tool_hash_is_mandatory(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            tool = Path(raw) / "tool"
            tool.write_bytes(b"tool")
            with self.assertRaisesRegex(conversion.ConversionError, "SHA-256 mismatch"):
                conversion.validate_tool(tool, "0" * 64, "converter")

    def test_quantization_preserves_semantic_classifier_head_as_f16(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            (root / "logs").mkdir()
            commands: list[list[str]] = []

            def capture(command: list[str], _log: Path) -> None:
                commands.append(command)

            with mock.patch.object(conversion, "run_logged", side_effect=capture):
                conversion.convert_pass(
                    root / "source",
                    root,
                    "a",
                    root / "converter",
                    root / "quantizer",
                    "Q4_K_M",
                )
            self.assertEqual(
                commands[1][1:3],
                ["--tensor-type", "cls.output.weight=F16"],
            )
            self.assertEqual(commands[1][-1], "Q4_K_M")

    def test_publish_is_atomic_and_receipted(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = self.make_source(root)
            converter = root / "converter"
            quantizer = root / "quantizer"
            converter.write_bytes(b"converter")
            quantizer.write_bytes(b"quantizer")
            output = root / "output"

            def fake_pass(_source: Path, work: Path, label: str, *_tools: Path) -> dict[str, Path]:
                values = {
                    "bf16": work / f"decoder-{label}-bf16.gguf",
                    "decoder": work / f"decoder-{label}-q4.gguf",
                    "projector": work / f"projector-{label}-q8.gguf",
                }
                for kind, path in values.items():
                    path.write_bytes(kind.encode())
                return values

            with (
                mock.patch.object(conversion, "convert_pass", side_effect=fake_pass),
                mock.patch.object(conversion, "validate_decoder", return_value={"ok": True}),
                mock.patch.object(conversion, "validate_projector", return_value={"ok": True}),
            ):
                report = conversion.build_bundle(
                    source,
                    output,
                    converter,
                    quantizer,
                    digest(b"converter"),
                    digest(b"quantizer"),
                )
            self.assertTrue(report["reproducible"])
            receipt = json.loads((output / conversion.RECEIPT_NAME).read_text())
            self.assertEqual(receipt["version"], 2)
            self.assertEqual(receipt["source"], conversion.output_identity("Q8_0"))
            self.assertEqual(report["decoder_quantization"], "Q8_0")
            receipted = {item["path"] for item in receipt["artifacts"]}
            self.assertIn(conversion.decoder_name("Q8_0"), receipted)
            actual = {
                str(path.relative_to(output))
                for path in output.rglob("*")
                if path.is_file() and path.name != conversion.RECEIPT_NAME
            }
            self.assertEqual(receipted, actual)

    def test_mismatched_second_pass_never_publishes(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = self.make_source(root)
            converter = root / "converter"
            quantizer = root / "quantizer"
            converter.write_bytes(b"converter")
            quantizer.write_bytes(b"quantizer")
            output = root / "output"

            def fake_pass(_source: Path, work: Path, label: str, *_tools: Path) -> dict[str, Path]:
                values = {
                    "bf16": work / f"decoder-{label}-bf16.gguf",
                    "decoder": work / f"decoder-{label}-q4.gguf",
                    "projector": work / f"projector-{label}-q8.gguf",
                }
                for kind, path in values.items():
                    path.write_bytes(f"{kind}-{label}".encode())
                return values

            with (
                mock.patch.object(conversion, "convert_pass", side_effect=fake_pass),
                mock.patch.object(conversion, "validate_decoder", return_value={}),
                mock.patch.object(conversion, "validate_projector", return_value={}),
                self.assertRaisesRegex(conversion.ConversionError, "not reproducible"),
            ):
                conversion.build_bundle(
                    source,
                    output,
                    converter,
                    quantizer,
                    digest(b"converter"),
                    digest(b"quantizer"),
                )
            self.assertFalse(output.exists())
            self.assertEqual(list(root.glob(".output.staging-*")), [])

    def test_q4_identity_remains_explicitly_ranking_only(self) -> None:
        identity = conversion.output_identity("Q4_K_M")
        self.assertEqual(identity["variant"], "q4-k-m-q8-0-bundle-v1")
        self.assertEqual(
            conversion.decoder_name("Q4_K_M"),
            "Qwen3-VL-Reranker-2B-Q4_K_M.gguf",
        )

    def test_unsupported_quantization_fails_closed(self) -> None:
        with self.assertRaisesRegex(conversion.ConversionError, "unsupported decoder quantization"):
            conversion.output_identity("IQ2_XS")

    def test_published_bundle_is_revalidated_from_receipt_and_live_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = self.make_source(root)
            converter = root / "converter"
            quantizer = root / "quantizer"
            converter.write_bytes(b"converter")
            quantizer.write_bytes(b"quantizer")
            output = root / "output"

            def fake_pass(_source: Path, work: Path, label: str, *_args: object) -> dict[str, Path]:
                values = {
                    "bf16": work / f"decoder-{label}-bf16.gguf",
                    "decoder": work / f"decoder-{label}-q8.gguf",
                    "projector": work / f"projector-{label}-q8.gguf",
                }
                for kind, path in values.items():
                    path.write_bytes(kind.encode())
                return values

            def tool_info(path: Path, expected: str, _label: str) -> dict[str, object]:
                return {"path": str(path.resolve()), "sha256": expected, "size": path.stat().st_size}

            decoder_contract = {"decoder_quantization": "Q8_0"}
            projector_contract = {"artifact_type": "mmproj"}
            with (
                mock.patch.object(conversion, "convert_pass", side_effect=fake_pass),
                mock.patch.object(conversion, "validate_tool", side_effect=tool_info),
                mock.patch.object(conversion, "validate_decoder", return_value=decoder_contract),
                mock.patch.object(conversion, "validate_projector", return_value=projector_contract),
            ):
                conversion.build_bundle(
                    source,
                    output,
                    converter,
                    quantizer,
                    conversion.DEFAULT_CONVERTER_SHA256,
                    conversion.DEFAULT_QUANTIZER_SHA256,
                    "Q8_0",
                )
                validated = conversion.validate_published_bundle(output, "Q8_0")
            self.assertEqual(validated["decoder_quantization"], "Q8_0")
            self.assertEqual(validated["contracts"]["decoder"], decoder_contract)

            decoder_path = output / conversion.decoder_name("Q8_0")
            decoder_path.write_bytes(b"tampered")
            with self.assertRaisesRegex(conversion.ConversionError, "size mismatch"):
                conversion.validate_published_bundle(output, "Q8_0")


if __name__ == "__main__":
    unittest.main()
