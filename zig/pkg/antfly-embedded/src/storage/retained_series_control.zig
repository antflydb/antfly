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
pub const maintenance = @import("retained_series_maintenance.zig");
const capture = @import("retained_series_capture.zig");
const A = std.mem.Allocator;
pub const Fence = @import("retained_series_revocation.zig").Fence;
pub const Command = struct {
    action: enum { install, backfill, tick, cleanup, close, revoke, drain, purge, admit_retention, archive_handoff, diagnostic },
    fence: Fence,
    binding: ?capture.Binding = null,
    complete_from_ns: i128 = 0,
    expected_source_schema: ?series.Id = null,
    expected_admitted_ns: ?i128 = null,
    diagnostic_error: ?[]const u8 = null,
    expected_cursor: []const u8 = "",
    work_limit: u16 = 128,

    pub fn jsonStringify(self: Command, writer: anytype) !void {
        try @import("../retained_dataset_types.zig").writeFieldsOmittingNulls(self, .{ "expected_source_schema", "expected_admitted_ns", "diagnostic_error" }, writer);
    }
    pub fn validate(self: Command) !void {
        if (self.fence.generation == 0 or self.fence.incarnation == 0 or self.work_limit == 0 or self.work_limit > series.max_changes or self.expected_cursor.len > 16384 or
            std.mem.allEqual(u8, &self.fence.id, 0) or std.mem.allEqual(u8, &self.fence.definition, 0) or std.mem.allEqual(u8, &self.fence.partition, 0)) return error.InvalidRetainedSeries;
        if (self.diagnostic_error) |message| {
            if (self.action != .diagnostic or message.len == 0 or message.len > 128) return error.InvalidRetainedSeries;
            for (message) |c| if (!std.ascii.isAlphanumeric(c) and c != '_') return error.InvalidRetainedSeries;
        }
        if (self.expected_admitted_ns != null and self.action != .revoke) return error.InvalidRetainedSeries;
        if ((self.action == .install) != (self.binding != null)) return error.InvalidRetainedSeries;
        if (self.expected_source_schema) |digest| if (self.action != .install or std.mem.allEqual(u8, &digest, 0)) return error.InvalidRetainedSeries;
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
    RetainedSeriesSourceDraining,
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
        if (command.expected_source_schema) |expected| {
            const current = txn.get(@import("internal_keys.zig").public_schema_json_key) catch |err| switch (err) {
                error.NotFound => "",
                else => return err,
            };
            var actual: series.Id = undefined;
            std.crypto.hash.sha2.Sha256.hash(current, &actual, .{});
            if (!std.mem.eql(u8, &expected, &actual)) return error.RetainedSeriesDefinitionMismatch;
        }
        try capture.install(a, txn, command.binding.?, .{ .sequence = source_sequence, .admitted_ns = admitted_ns }, command.complete_from_ns);
        return;
    }
    if (command.action == .revoke) {
        if (command.expected_admitted_ns) |expected| if (!try @import("retained_series_revocation.zig").isRevoked(txn, command.fence)) {
            const bindings = try capture.load(a, txn);
            const spec = for (bindings) |binding| {
                if (std.mem.eql(u8, &binding.spec.id, &command.fence.id) and binding.spec.generation == command.fence.generation) break binding.spec;
            } else return error.RetainedSeriesDefinitionMismatch;
            try requireFence(command.fence, spec);
            const progress = try (series.Engine{ .alloc = a, .spec = spec }).status(a, txn);
            if (progress.admitted_ns != expected) return error.RetainedSeriesResetRequired;
        };
        try capture.revokeBinding(a, txn, command.fence, admitted_ns);
        return;
    }
    if (command.action == .purge) return capture.purgeRevokedBinding(a, txn, command.fence, command.work_limit);
    const bindings = try capture.load(a, txn);
    const spec = for (bindings) |binding| {
        if (std.mem.eql(u8, &binding.spec.id, &command.fence.id) and binding.spec.generation == command.fence.generation) break binding.spec;
    } else return error.RetainedSeriesDefinitionMismatch;
    try requireFence(command.fence, spec);
    const engine = series.Engine{ .alloc = a, .spec = spec };
    switch (command.action) {
        .install, .revoke, .purge => unreachable,
        .backfill => _ = try capture.backfillPage(a, txn, spec.id, spec.generation, command.expected_cursor, command.work_limit),
        .tick => _ = try engine.tick(txn, admitted_ns, command.work_limit),
        .admit_retention => try engine.admitRetentionClock(txn, admitted_ns),
        .archive_handoff => try engine.beginArchiveHandoff(txn, admitted_ns),
        .diagnostic => try engine.recordDiagnostic(txn, command.diagnostic_error, admitted_ns),
        .cleanup => _ = try engine.cleanupBackfill(txn, command.work_limit),
        .close => try capture.closeBinding(a, txn, spec.id, spec.generation, admitted_ns),
        .drain => try engine.beginClose(txn, admitted_ns),
    }
    switch (command.action) {
        .backfill, .tick, .cleanup, .close, .drain => try engine.recordDiagnostic(txn, null, admitted_ns),
        else => {},
    }
    // Persist the next fair slot with the command's effects. Recreating a
    // host planner after each task or leader change cannot starve cleanup.
    switch (command.action) {
        .backfill => try engine.recordMaintenance(txn, 1),
        .tick => try engine.recordMaintenance(txn, 2),
        .cleanup => try engine.recordMaintenance(txn, 0),
        else => {},
    }
}
