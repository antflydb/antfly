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

//! Fixed identities shared by the owning retirement and its schema cohort.
//! These commitments describe authority; native transaction joins grant it.
const std = @import("std");
pub const Lookup = struct {
    group: u64,
    table: u64,
    id: [16]u8,
    pub fn validate(self: @This()) !void {
        if (self.group == 0 or self.table == 0 or std.mem.allEqual(u8, &self.id, 0)) return error.InvalidCatalogRecord;
    }
};
pub const Authority = struct {
    root: Lookup,
    effects: @import("table_retirement_manifest.zig").Reference,
    physical_plan: [32]u8,
    pub fn validate(self: @This()) !void {
        try self.root.validate();
        try self.effects.validate();
        if (self.effects.root_table != self.root.table or self.effects.survivors == 0 or std.mem.allEqual(u8, &self.physical_plan, 0)) return error.InvalidGenerationPublication;
    }
};
pub const Cohort = struct {
    table: u64,
    id: [16]u8,
    digest: [32]u8,
    pub fn validate(self: @This()) !void {
        if (self.table == 0 or std.mem.allEqual(u8, &self.id, 0) or std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidGenerationPublication;
    }
};
