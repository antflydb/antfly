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

import json, sys, struct
from pathlib import Path
import numpy as np, ml_dtypes

manifest = json.loads(Path(sys.argv[1]).read_text())
out = sys.stdout.buffer
with open(sys.argv[2], "rb") as f:
    size = struct.unpack("<Q", f.read(8))[0]
    header = json.loads(f.read(size))
    base = 8 + size
    for p in manifest["parameters"]:
        dtype = ml_dtypes.bfloat16 if p["dtype"] == "bfloat16" else np.float32
        if "data" in p:
            out.write(np.asarray(p["data"], dtype=dtype).tobytes())
            continue
        t = header[p["source"]]
        assert t["dtype"] == "BF16" and t["shape"] == p["shape"]
        start, stop = t["data_offsets"]
        f.seek(base + start)
        remaining = stop - start
        while remaining:
            data = f.read(min(remaining, 1 << 22))
            assert data
            out.write(data)
            remaining -= len(data)
        out.flush()
