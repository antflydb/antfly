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

import unittest
from types import SimpleNamespace

from qualify_small_decode import E4B_WEIGHT_SHAPES, checked_weights


class CheckWeightsTest(unittest.TestCase):
    def setUp(self):
        self.reader = SimpleNamespace(
            tensors=[
                SimpleNamespace(
                    name=f"blk.0.{name}.weight",
                    shape=shape,
                    tensor_type=2,
                    data=SimpleNamespace(nbytes=shape[0] // 32 * 18 * shape[1]),
                )
                for name, shape in E4B_WEIGHT_SHAPES.items()
            ]
        )

    def test_expected_shapes(self):
        self.assertEqual(set(checked_weights(self.reader)), set(E4B_WEIGHT_SHAPES))

    def test_reject_transposed_same_size(self):
        tensor = self.reader.tensors[-1]
        tensor.shape = tensor.shape[::-1]
        with self.assertRaisesRegex(ValueError, "GGUF dimensions"):
            checked_weights(self.reader)

    def test_reject_short_payload(self):
        self.reader.tensors[-1].data.nbytes -= 18
        with self.assertRaisesRegex(ValueError, "packed bytes"):
            checked_weights(self.reader)

    def test_reject_wrong_type(self):
        self.reader.tensors[-1].tensor_type = 1
        with self.assertRaisesRegex(ValueError, "Expected Q4_0"):
            checked_weights(self.reader)
