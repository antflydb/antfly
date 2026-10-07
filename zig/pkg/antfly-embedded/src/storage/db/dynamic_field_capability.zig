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

//! Observed dynamic-field capabilities shared across the API/storage boundary.

const std = @import("std");
const schema_mod = @import("../schema.zig");
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const platform_time = @import("antfly_platform").time;

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
    execution_deadline_ns: ?u64 = null,
    cancellation: ?CancellationToken = null,

    pub fn includesField(self: ObservationQuery, field: []const u8) bool {
        if (self.fields.len == 0) return true;
        for (self.fields) |selected| {
            if (std.mem.eql(u8, selected, field)) return true;
        }
        return false;
    }

    pub fn checkActive(self: ObservationQuery) !void {
        if (self.cancellation) |token| try token.check();
        if (self.execution_deadline_ns) |deadline_ns| {
            if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
        }
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
