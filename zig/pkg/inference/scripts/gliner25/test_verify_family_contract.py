from __future__ import annotations

import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path

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
            with self.assertRaisesRegex(family.ContractError, "invalid safetensors header"):
                family.safetensors_header(path)

    def test_unselected_rope_metadata_and_tokenizer_bytes_are_identity_bound(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            model, generated = self.fixture(root, "decide_1b")
            contract = root / "contract.json"
            contract.write_text(json.dumps(generated), encoding="utf-8")

            encoder_path = model / "encoder_config" / "config.json"
            encoder = json.loads(encoder_path.read_text(encoding="utf-8"))
            encoder["rope_parameters"]["full_attention"]["rope_theta"] = 160001.0
            encoder_path.write_text(json.dumps(encoder), encoding="utf-8")
            with self.assertRaisesRegex(family.ContractError, "sidecar SHA-256 differs"):
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


if __name__ == "__main__":
    unittest.main()
