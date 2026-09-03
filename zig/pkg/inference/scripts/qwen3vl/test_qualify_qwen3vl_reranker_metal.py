#!/usr/bin/env python3

from __future__ import annotations

import argparse
import contextlib
import io
from pathlib import Path
import sys
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parent))

import qualify_qwen3vl_reranker_metal as qualification


def execution() -> dict[str, object]:
    return {
        "stdout": "",
        "stderr": "",
        "resources": {
            "max_rss_mib": 100.0,
            "min_free_percent": 50,
            "swapout_growth_mib": 0.0,
        },
    }


class RerankerQualificationTests(unittest.TestCase):
    def args(self, image: Path | None) -> argparse.Namespace:
        return argparse.Namespace(
            document=["document"],
            image=image,
            decoder_quantization="Q8_0",
            profile="calibrated",
            oracle_max_rss_mib=1_000.0,
            metal_max_rss_mib=1_000.0,
            min_free_percent=10,
            max_swap_growth_mib=0.0,
        )

    def oracle(self, image: Path | None) -> dict[str, object]:
        return {
            "request": {
                "rendered_prompts": ["prompt"],
                "input_ids": [[10, 11]],
                "attention_mask": [[1, 1]],
                "active_mrope_position_ids": [[0, 1, 0, 1, 0, 1]],
                "image": {"visual_tokens": 1} if image is not None else None,
            },
            "output": {"scores": [0.5], "score_logits": [0.0]},
        }

    def multimodal_trace(self, image: Path) -> dict[str, object]:
        return {
            "schema": "antfly.qwen3vl.multimodal_reranker_qualification.v1",
            "backend": "metal",
            "images": [str(image)],
            "rendered_prompt": "prompt",
            "expanded_token_ids": [10, 11],
            "mrope_positions": [0, 1, 0, 1, 0, 1],
            "visual_token_mask": [False, True],
            "visual_tokens": 1,
            "raw_logit": 0.0,
            "score": 0.5,
        }

    def test_multimodal_exact_trace_passes(self) -> None:
        image = Path("/tmp/fixture.jpg")
        trace = self.multimodal_trace(image)
        passed, gates, _ = qualification.evaluate(
            self.args(image),
            self.oracle(image),
            [trace, dict(trace)],
            execution(),
            [execution(), execution()],
        )
        self.assertTrue(passed)
        self.assertTrue(all(gate["pass"] for gate in gates))

    def test_multimodal_mrope_mismatch_fails(self) -> None:
        image = Path("/tmp/fixture.jpg")
        trace = self.multimodal_trace(image)
        trace["mrope_positions"] = [0, 1, 0, 2, 0, 1]
        passed, gates, _ = qualification.evaluate(
            self.args(image),
            self.oracle(image),
            [trace, dict(trace)],
            execution(),
            [execution(), execution()],
        )
        self.assertFalse(passed)
        self.assertFalse(next(gate for gate in gates if gate["name"] == "metal_1_mrope_exact")["pass"])

    def test_calibrated_q4_is_rejected_by_cli(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            qualification.parse_args([
                "--model-dir", "/tmp/model",
                "--oracle-model-dir", "/tmp/oracle",
                "--antfly-bin", "/tmp/antfly",
                "--output", "/tmp/output.json",
                "--decoder-quantization", "Q4_K_M",
            ])


if __name__ == "__main__":
    unittest.main()
