# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import sqlite3
import tempfile
import unittest
from pathlib import Path
from analyze_profile import category, merged_ns, summarize


class ProfileTests(unittest.TestCase):
    def test_overlap_is_counted_once(self):
        self.assertEqual(merged_ns([(0, 8), (4, 10), (15, 19)]), 14)
        self.assertEqual(merged_ns([]), 0)

    def test_model_name_does_not_make_pointwise_kernel_a_gemm(self):
        self.assertEqual(
            category("termite_embedding_gemma2_packed_heads_bf16"),
            "other_fusion_or_pointwise",
        )
        self.assertEqual(
            category("termite_embedding_gemma2_attention_local_flash_bf16"), "attention"
        )
        self.assertEqual(category("gemm_fusion_dot_220"), "gemm")
        self.assertEqual(
            category("termite_embedding_gemma2_rms_norm_bf16"), "norm_rope"
        )

    def test_readbacks_delimit_forwards_and_gpu_gaps(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace.sqlite"
            con = sqlite3.connect(path)
            con.executescript("""
                CREATE TABLE StringIds(id INTEGER, value TEXT);
                INSERT INTO StringIds VALUES (1,'kernel');
                CREATE TABLE CUPTI_ACTIVITY_KIND_KERNEL(
                    start INTEGER,end INTEGER,demangledName INTEGER,
                    registersPerThread INTEGER,blockX INTEGER,blockY INTEGER,blockZ INTEGER,
                    gridX INTEGER,gridY INTEGER,gridZ INTEGER,
                    staticSharedMemory INTEGER,dynamicSharedMemory INTEGER,localMemoryPerThread INTEGER);
                INSERT INTO CUPTI_ACTIVITY_KIND_KERNEL VALUES
                    (0,8000000,1,32,256,1,1,10,1,1,0,0,0),
                    (4000000,10000000,1,32,256,1,1,10,1,1,0,0,0),
                    (20000000,24000000,1,32,256,1,1,10,1,1,0,0,0);
                CREATE TABLE CUPTI_ACTIVITY_KIND_MEMCPY(start INTEGER,end INTEGER,copyKind INTEGER,bytes INTEGER);
                INSERT INTO CUPTI_ACTIVITY_KIND_MEMCPY VALUES
                    (10000000,12000000,2,3072),(30000000,32000000,2,3072);
            """)
            con.commit()
            con.close()
            result = summarize(path)
            self.assertEqual(result["forwards"], 2)
            first, second = result["calls"]
            self.assertEqual(first["gpu_span_ms"], 12)
            self.assertEqual(first["gpu_activity_ms"], 12)
            self.assertEqual(first["kernel_count"], 2)
            self.assertEqual(second["gpu_span_ms"], 12)
            self.assertEqual(second["gpu_activity_ms"], 6)
            self.assertEqual(second["gpu_gap_ms"], 6)
            self.assertEqual(second["gpu_activity_fraction"], 0.5)


if __name__ == "__main__":
    unittest.main()
