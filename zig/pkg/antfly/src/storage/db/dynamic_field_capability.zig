// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Observed dynamic-field capabilities shared across the API/storage boundary.

const std = @import("std");
const schema_mod = @import("../schema.zig");

pub const CoverageReadMode = enum(u8) {
    /// Return only coverage summaries that have already been validated. This
    /// is the observability path: it must never turn a status read into an
    /// O(documents) column scan.
    cached_only,
    /// Validate uncached coverage for the selected fields. Query admission
    /// uses this fail-closed path before an exact sort reaches execution.
    validate,
};

pub const ObservationQuery = struct {
    /// When set, observe only this text index. A null index intentionally
    /// preserves the existing cross-index conservative merge semantics.
    index_name: ?[]const u8 = null,
    /// Empty means all fields. Query admission supplies its de-duplicated
    /// physical sort fields so unrelated columns remain cold.
    fields: []const []const u8 = &.{},
    coverage_read_mode: CoverageReadMode = .cached_only,

    pub fn includesField(self: ObservationQuery, field: []const u8) bool {
        if (self.fields.len == 0) return true;
        for (self.fields) |selected| {
            if (std.mem.eql(u8, selected, field)) return true;
        }
        return false;
    }
};

pub const ObservedDynamicFieldCapabilitySet = struct {
    index_name: []u8,
    field_capabilities: []schema_mod.FieldCapability,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.index_name);
        schema_mod.freeOwnedFieldCapabilities(alloc, self.field_capabilities);
        self.* = undefined;
    }
};
