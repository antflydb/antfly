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

//! Private replicated lifecycle commands. Public batch JSON cannot supply them.
const std = @import("std");
const series = @import("retained_series.zig");
const capture = @import("retained_series_capture.zig");
const A = std.mem.Allocator;
pub const Fence = @import("retained_series_revocation.zig").Fence;
pub const Command = struct {
    action: enum { install, backfill, tick, cleanup, close, revoke },
    fence: Fence,
    binding: ?capture.Binding = null,
    complete_from_ns: i128 = 0,
    expected_cursor: []const u8 = "",
    work_limit: u16 = 128,

    pub fn validate(self: Command) !void {
        if (self.fence.generation == 0 or self.fence.incarnation == 0 or self.work_limit == 0 or self.work_limit > series.max_changes or self.expected_cursor.len > 16384 or
            std.mem.allEqual(u8, &self.fence.id, 0) or std.mem.allEqual(u8, &self.fence.definition, 0) or std.mem.allEqual(u8, &self.fence.partition, 0)) return error.InvalidRetainedSeries;
        if ((self.action == .install) != (self.binding != null)) return error.InvalidRetainedSeries;
        if (self.binding) |binding| {
            try binding.validate();
            try requireFence(self.fence, binding.spec);
        }
        if (self.action != .backfill and self.expected_cursor.len != 0) return error.InvalidRetainedSeries;
    }
};
pub const Rejection = error{
    InvalidRetainedSeries,
    RetainedSeriesTransactionsUnsupported,
    RetainedSeriesTopologyUnsupported,
    RetainedSeriesCoordinatedTtlUnsupported,
    InvalidRetainedSeriesRow,
    RetainedSeriesAlreadyExists,
    RetainedSeriesBackfillFence,
    RetainedSeriesBatchTooLarge,
    RetainedSeriesCorrectionWindow,
    RetainedSeriesCoverageMissing,
    RetainedSeriesDefinitionMismatch,
    RetainedSeriesDuplicateRow,
    RetainedSeriesHistoryExpired,
    RetainedSeriesNotReady,
    RetainedSeriesPreparedHold,
    RetainedSeriesReservationMismatch,
    RetainedSeriesNumericOverflow,
    RetainedSeriesResetRequired,
    RetainedSeriesResolutionMismatch,
    RetainedSeriesSequenceMismatch,
    RetainedSeriesSourceClosed,
    RetainedSeriesSourceRevoked,
    RetainedSeriesTimeOverflow,
};
pub fn requireFence(f: Fence, spec: series.Spec) !void {
    if (!std.mem.eql(u8, &f.id, &spec.id) or f.generation != spec.generation or !std.mem.eql(u8, &f.definition, &spec.definition) or
        !std.mem.eql(u8, &f.partition, &spec.source_partition) or f.incarnation != spec.source_incarnation) return error.RetainedSeriesDefinitionMismatch;
}
/// A control cannot be mixed with source writes, transaction intent operations,
/// topology commands, or unrelated catalog changes. Compare all fields so new
/// BatchRequest capabilities default to rejected rather than becoming implicit.
pub fn validateRequest(req: anytype) !void {
    const command = req.retained_series orelse return;
    try command.validate();
    if (req.timestamp_ns == 0) return error.InvalidRetainedSeries;
    const defaults = @TypeOf(req){};
    inline for (@typeInfo(@TypeOf(req)).@"struct".field_names) |name| {
        if (comptime !std.mem.eql(u8, name, "retained_series") and !std.mem.eql(u8, name, "timestamp_ns") and
            !std.mem.eql(u8, name, "document_version_scheme") and !std.mem.eql(u8, name, "sync_level"))
        {
            const value = @field(req, name);
            if (comptime @typeInfo(@TypeOf(value)) == .pointer and @typeInfo(@TypeOf(value)).pointer.size == .slice) {
                // Parsed empty slices need not share the default's address.
                if (value.len != 0) return error.InvalidRetainedSeries;
            } else if (!std.meta.eql(value, @field(defaults, name))) return error.InvalidRetainedSeries;
        }
    }
}
pub fn stage(a: A, txn: anytype, command: Command, source_sequence: u64, admitted_ns: i128) !void {
    try command.validate();
    if (command.action == .install) {
        try capture.install(a, txn, command.binding.?, .{ .sequence = source_sequence, .admitted_ns = admitted_ns }, command.complete_from_ns);
        return;
    }
    if (command.action == .revoke) {
        try capture.revokeBinding(a, txn, command.fence, admitted_ns);
        return;
    }
    const bindings = try capture.load(a, txn);
    const spec = for (bindings) |binding| {
        if (std.mem.eql(u8, &binding.spec.id, &command.fence.id) and binding.spec.generation == command.fence.generation) break binding.spec;
    } else return error.RetainedSeriesDefinitionMismatch;
    try requireFence(command.fence, spec);
    const engine = series.Engine{ .alloc = a, .spec = spec };
    switch (command.action) {
        .install, .revoke => unreachable,
        .backfill => _ = try capture.backfillPage(a, txn, spec.id, spec.generation, command.expected_cursor, command.work_limit),
        .tick => _ = try engine.tick(txn, admitted_ns, command.work_limit),
        .cleanup => _ = try engine.cleanupBackfill(txn, command.work_limit),
        .close => try capture.closeBinding(a, txn, spec.id, spec.generation, admitted_ns),
    }
}
