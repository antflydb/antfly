#!/usr/bin/env python3
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location("training_math_regen", Path(__file__).with_name("regen-cuda-training-math.py"))
regen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(regen)


class TrainingMathArtifactsTest(unittest.TestCase):
    def test_checked_in_manifest_binds_source_and_ptx(self):
        source = regen.source_bytes()
        ptx = (regen.ARTIFACTS / "gliner25_training_math.ptx").read_bytes()
        regen.publish(regen.ARTIFACTS, regen.artifacts(source, ptx), False)
        self.assertIn(regen.COMPILER, ptx)
        for name in ("outer", "inner", "vector", "vector_sums"):
            self.assertIn(f".visible .entry termite_gliner25_scan_{name}_v1(".encode(), ptx)
        for name in ("part", "finish"):
            self.assertIn(f".visible .entry termite_gliner25_reduce_{name}_v1(".encode(), ptx)
        self.assertIn(b".visible .entry termite_gliner25_gelu_f32_cuda128(", ptx)
        self.assertIn(b".visible .entry termite_gliner25_silu_f32_cuda128(", ptx)
        self.assertIn(b".visible .entry termite_gliner25_sigmoid_f32_cuda128(", ptx)
        self.assertIn(b".visible .entry termite_gliner25_frozen_span_features_v1(", ptx)
        self.assertIn(b".visible .entry termite_gliner25_scatter_gather_v1(", ptx)
        self.assertIn(b".visible .entry termite_gliner25_scatter_embedding_v1(", ptx)
        self.assertIn(b".visible .entry termite_gliner25_adamw_pytorch_v1(", ptx)
        self.assertIn(b".visible .entry termite_gliner25_elementwise_vjp_v1(", ptx)
        for name in ("prepare", "vjp"):
            self.assertIn(f".visible .entry termite_gliner25_listwise_{name}_v1(".encode(), ptx)
        for name in ("target_exp", "logp_vjp", "mask_vjp"):
            self.assertIn(f".visible .entry termite_gliner25_record_{name}_v1(".encode(), ptx)
        for name in ("warp", "block"):
            self.assertIn(f".visible .entry termite_gliner25_log_softmax_{name}_f32(".encode(), ptx)

    def test_shared_softmax_source_is_bound_and_missing_include_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            artifacts = root / "artifacts"
            kernels = root / "kernels"
            artifacts.mkdir()
            kernels.mkdir()
            source = artifacts / "gliner25_training_math.cu"
            header = kernels / "gliner25_softmax.cuh"
            source.write_text('#include "../kernels/gliner25_softmax.cuh"\n')
            header.write_text("first")
            with patch.object(regen, "ARTIFACTS", artifacts):
                first = regen.artifacts(regen.source_bytes(), b"ptx")
                header.write_text("second")
                self.assertNotEqual(first, regen.artifacts(regen.source_bytes(), b"ptx"))
                source.write_text("missing")
                with self.assertRaisesRegex(RuntimeError, "exactly one"):
                    regen.source_bytes()

    def test_stale_check_does_not_replace_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            outputs = regen.artifacts(b"source", b"ptx")
            regen.publish(root, outputs, True)
            stale = root / "gliner25_training_math.ptx"
            stale.write_bytes(b"stale")
            with self.assertRaisesRegex(RuntimeError, "stale CUDA training math"):
                regen.publish(root, outputs, False)
            self.assertEqual(stale.read_bytes(), b"stale")
            regen.publish(root, outputs, True)
            regen.publish(root, outputs, False)
            self.assertFalse(list(root.glob(".training-math-*")))

    def test_source_change_invalidates_manifest_even_with_same_ptx(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            regen.publish(root, regen.artifacts(b"first", b"ptx"), True)
            with self.assertRaisesRegex(RuntimeError, "gliner25_training_math.json"):
                regen.publish(root, regen.artifacts(b"second", b"ptx"), False)

    def test_wrong_nvrtc_version_fails_before_creating_program(self):
        library = Mock()

        def version(major, minor):
            major._obj.value = 13
            minor._obj.value = 2
            return 0

        library.nvrtcVersion.side_effect = version
        with patch.object(regen.c, "CDLL", return_value=library):
            with self.assertRaisesRegex(RuntimeError, "requires NVRTC 12.8"):
                regen.compile_ptx("unused", b"source")
        library.nvrtcCreateProgram.assert_not_called()


if __name__ == "__main__":
    unittest.main()
