// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Generated kernel regions of the `reduce` families.

const r = @import("../region.zig");
const gather = @import("../reduce_gather.zig");
const rope = @import("../reduce_rope.zig");
const rows = @import("../reduce_rows.zig");

fn metal(comptime family: []const u8, comptime msl: []const u8) r.Region {
    return r.metal(family, r.metalLines(r.metal_indent, msl));
}

pub const regions = [_]r.Region{
    metal("reduce.embedding_lookup", gather.metalRegion()),
    r.cuda("reduce.embedding_lookup", gather.cudaRegion()),
    r.cuda("reduce.rope", rope.cudaRegion()),
    metal("reduce.rows", rows.rowsRegion()),
    metal("reduce.scan", rows.scanRegion()),
    metal("reduce.qwen35_norm", rows.qwen35NormRegion()),
    metal("reduce.lightonocr3_norm", rows.lightonocr3NormRegion()),
};
