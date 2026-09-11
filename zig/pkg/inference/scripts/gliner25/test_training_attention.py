"""Source/counter admission checks; these tests never import a numerical runtime."""
import copy
import hashlib
import json
import struct
import sys
import unittest

import capture_training_attention as capture


class TrainingAttentionContractTest(unittest.TestCase):
    def test_frozen_nine_case_geometry_and_source_contract(self):
        value = capture.validate_contract(capture.oracle.read_json(capture.CONTRACT))
        self.assertEqual(9, len(value["cases"]))
        self.assertEqual({0.0, capture.f32(.1), .125}, {case["probability"] for case in value["cases"]})
        self.assertEqual(3, sum(case["cotangent"] == "fully_masked_sample_only" for case in value["cases"]))
        self.assertEqual(3, sum(case["sequence"] == 512 for case in value["cases"]))
        self.assertEqual(256, value["config"]["position_buckets"])
        self.assertEqual(capture.METHODS, set(value["methods"]))
        self.assertEqual("98fa398f62e446e1f6303ff67fa7aceddac4f746a1a6013226896c3fa4e6cdd6", value["transformers_source"]["sha256"])

    def test_control_keeps_all_u64_bits_in_physical_i32_limbs(self):
        case = capture.cases()[0]
        buckets = list(range(250, 263))
        words = capture.control(case, buckets)
        self.assertEqual(6 + 2 * 7 + 13, len(words))
        self.assertTrue(any(value < 0 for value in words[:6]))
        reconstructed = struct.unpack("<QQQ", struct.pack("<6i", *words[:6]))
        self.assertEqual((case["seed"], case["micro_batch"], case["replica"]), reconstructed)
        self.assertEqual([1] * 7 + [1] * 4 + [0] * 3, words[6:20])
        self.assertEqual(buckets, words[20:])
        for invalid in (buckets[:-1], [512] * 13, [True] * 13):
            with self.subTest(invalid=invalid):
                with self.assertRaises(capture.oracle.ContractError):
                    capture.control(case, invalid)

    def test_counter_matches_prior_actual_transformers_probability_masks(self):
        directory = capture.oracle.FIXTURES / "training_encoder"
        manifest = json.loads((directory / "capture.json").read_bytes())
        raw = (directory / "tensors.safetensors").read_bytes()
        self.assertEqual(manifest["files_sha256"]["tensors.safetensors"], hashlib.sha256(raw).hexdigest())
        header_bytes = struct.unpack("<Q", raw[:8])[0]
        header = json.loads(raw[8:8 + header_bytes])
        compared = 0
        for case in manifest["cases"]:
            for site in case["dropouts"]:
                if site["kind"] != "attention_probabilities":
                    continue
                field = header[site["tensor"]]
                self.assertEqual("F32", field["dtype"])
                begin, end = field["data_offsets"]
                values = struct.unpack("<" + "f" * ((end - begin) // 4), raw[8 + header_bytes + begin:8 + header_bytes + end])
                parameters = capture.counter_parameters(site["probability"], case["seed"], case["micro_batch"],
                    case["replica"], (site["layer"] << 32) | 3)
                # Traverse uneven tiles using global indices, against the saved
                # source mask; local tile indexing must not reset the counter.
                for start in range(0, len(values), 31):
                    self.assertEqual(values[start:start + 31], tuple(capture.mask_value(index, parameters)
                        for index in range(start, min(start + 31, len(values)))))
                compared += len(values)
        self.assertEqual(3672, compared)

    def test_probability_quantization_precedes_threshold_and_inversion(self):
        _, threshold, scale = capture.counter_parameters(.1, 0, 0, 0, 3)
        self.assertEqual(429496736, threshold)
        self.assertNotEqual(int(.1 * 2**32), threshold)
        self.assertEqual(1.1111111640930176, scale)
        self.assertEqual(536870912, capture.counter_parameters(.125, 0, 0, 0, 3)[1])
        zero = capture.counter_parameters(0, 0, 0, 0, 3)
        self.assertEqual([1.0] * 128, [capture.mask_value(index, zero) for index in range(128)])

    def test_different_counter_limbs_and_streams_remain_distinct(self):
        args = (capture.SEED, capture.MICRO_BATCH, capture.REPLICA, (7 << 32) | 3)
        streams = {capture.counter_parameters(.125, *args)[0]}
        for field in range(4):
            changed = list(args)
            changed[field] ^= 1 << 32
            streams.add(capture.counter_parameters(.125, *changed)[0])
        self.assertEqual(5, len(streams))
        stream = next(iter(streams))
        self.assertNotEqual(capture.mix(stream ^ capture.mix(2**24)), capture.mix(stream ^ capture.mix(2**24 + 1)))

    def test_invalid_counter_and_changed_capture_admission_are_rejected(self):
        for probability in (-.1, 1, float("inf"), float("nan")):
            with self.assertRaises(capture.oracle.ContractError):
                capture.counter_parameters(probability, 1, 2, 3, 4)
        with self.assertRaises(capture.oracle.ContractError):
            capture.counter_parameters(.1, 1 << 64, 2, 3, 4)
        contract = capture.oracle.read_json(capture.CONTRACT)
        changed = copy.deepcopy(contract)
        changed["limits"]["artifact_bytes"] += 1
        with self.assertRaises(capture.oracle.ContractError):
            capture.validate_contract(changed)
        changed = copy.deepcopy(contract)
        changed["cases"][0]["lengths"][1] = 0
        with self.assertRaises(capture.oracle.ContractError):
            capture.validate_contract(changed)
        changed = copy.deepcopy(contract)
        changed["counter_vectors"][1]["parameters"][1] -= 1
        with self.assertRaises(capture.oracle.ContractError):
            capture.validate_contract(changed)

    def test_source_only_module_does_not_import_torch(self):
        self.assertFalse(any(name in sys.modules for name in ("torch", "gliner2", "peft")))


if __name__ == "__main__":
    unittest.main()
