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

const DB = @import("../db.zig").DB;

/// Per-kind index state owned by `DB`. Each kind keeps its runtimes and
/// background work here; see `SUBSYSTEMS.md` for the kinds still to move.
pub const Indexes = struct {
    graph: @import("graph.zig").GraphIndexes = .{},
    sparse: @import("sparse.zig").SparseIndexes = .{},

    pub fn dbPtr(self: *Indexes) *DB {
        return @alignCast(@fieldParentPtr("indexes", self));
    }
};
