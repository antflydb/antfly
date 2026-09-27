# Copyright 2026 Antfly, Inc.
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

import tempfile
import unittest
from argparse import Namespace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from profile_vdbbench_mixed_public_workload import load_inputs, run


class MixedInputTest(unittest.TestCase):
    def test_streamed_prefix_preserves_ids_vectors_and_ground_truth(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            vectors = [[float(i), -float(i)] for i in range(2050)]
            pq.write_table(
                pa.table({"id": list(range(2050)), "emb": vectors}),
                root / "shuffle_train.parquet",
                row_group_size=400,
            )
            pq.write_table(pa.table({"emb": vectors[:3]}), root / "test.parquet")
            pq.write_table(
                pa.table({"neighbors_id": [[0, 1], [1, 2], [2, 3]]}),
                root / "neighbors.parquet",
            )
            queries, neighbors, updates = load_inputs(
                Namespace(
                    dataset=root,
                    update_vectors=1025,
                    query_vectors=2,
                    limit=1,
                    write_batch=100,
                )
            )
            self.assertEqual(queries, vectors[:2])
            self.assertEqual(neighbors, [[0], [1]])
            self.assertEqual(updates, list(enumerate(vectors[:1025])))

    def test_invalid_offered_rate_rejected_before_dataset_io(self):
        for rate in (-1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                run(Namespace(write_rows_per_second=rate))


if __name__ == "__main__":
    unittest.main()
