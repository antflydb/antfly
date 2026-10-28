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

//! Owned scalar state adopted immediately after a primary commit. Native
//! emission values are checked against their last physical writes before
//! commit; adoption needs neither decoding nor fallible preparation afterward.
const std = @import("std");
const identity = @import("doc_identity.zig");
const table = @import("table_catalog.zig");
const keys = @import("../internal_keys.zig");
const admission = @import("../../common/read_admission.zig");
const KV = @import("../docstore.zig").KVPair;
fn check(original: ?admission.Port) !void {
    if (original) |owner| try owner.checkpoint();
}
pub const State = struct {
    visibility: ?identity.VisibilitySummary = null,
    table_catalog: ?table.Catalog = null,
    visibility_index: ?usize = null,
    table_index: ?usize = null,

    pub fn capture(writes: []const KV, visibility: ?identity.VisibilitySummary, catalog: ?table.Catalog, original: ?admission.Port) !State {
        var result: State = .{ .visibility = visibility, .table_catalog = catalog };
        var index = writes.len;
        while (index > 0 and ((visibility != null and result.visibility_index == null) or (catalog != null and result.table_index == null))) {
            index -= 1;
            try check(original);
            if (visibility != null and result.visibility_index == null and std.mem.eql(u8, writes[index].key, &keys.identity_visibility_summary_key)) result.visibility_index = index;
            if (catalog != null and result.table_index == null and std.mem.eql(u8, writes[index].key, table.key)) result.table_index = index;
        }
        try result.validate(writes, original);
        return result;
    }
    pub fn validate(self: State, writes: []const KV, original: ?admission.Port) !void {
        try check(original);
        if (self.visibility_index) |index| {
            if (self.visibility == null or index >= writes.len or !std.mem.eql(u8, writes[index].key, &keys.identity_visibility_summary_key)) return error.PreparedEffectsMismatch;
            const actual = (try identity.visibilitySummaryFromWrites(writes[index..][0..1])) orelse return error.PreparedEffectsMismatch;
            if (!std.meta.eql(actual, self.visibility.?)) return error.PreparedEffectsMismatch;
        } else if (self.visibility != null) return error.PreparedEffectsMismatch;
        try check(original);
        if (self.table_index) |index| {
            if (self.table_catalog == null or index >= writes.len or !std.mem.eql(u8, writes[index].key, table.key)) return error.PreparedEffectsMismatch;
            if (!std.meta.eql(try table.Catalog.decode(writes[index].value), self.table_catalog.?)) return error.PreparedEffectsMismatch;
        } else if (self.table_catalog != null) return error.PreparedEffectsMismatch;
        try check(original);
    }
    /// The assembler's native generation provenance has already been checked
    /// by the replay recipe. Historical generations remain below its binding.
    pub fn bindReplaySequence(self: *State, previous: u64, next: u64) void {
        if (self.visibility) |*summary| {
            if (summary.max_created_generation == previous) summary.max_created_generation = next;
            if (summary.min_deleted_generation == previous) summary.min_deleted_generation = next;
            if (summary.max_deleted_generation == previous) summary.max_deleted_generation = next;
        }
    }
    /// Caller holds apply and has successfully committed these exact bytes.
    /// Adopt before any fallible observer, replication append or visibility wait.
    pub fn adopt(self: State, core: anytype) void {
        if (self.table_catalog) |catalog| core.table_catalog = catalog;
        if (self.visibility) |summary| core.identity_visibility.publish(summary);
    }
};

test "commit memory state binds only native emissions and checks the last physical values" {
    const old: identity.VisibilitySummary = .{ .live_ordinals = 1, .max_created_generation = 2 };
    const desired: identity.VisibilitySummary = .{ .live_ordinals = 2, .max_created_generation = std.math.maxInt(u64), .min_deleted_generation = 1, .max_deleted_generation = std.math.maxInt(u64) };
    const catalog: table.Catalog = .{ .mode_initialized = true, .row_count = 1, .reconciled = true };
    const old_bytes = identity.encodeVisibilitySummary(old);
    var desired_bytes = identity.encodeVisibilitySummary(desired);
    const catalog_bytes = catalog.encode();
    const writes = [_]KV{
        .{ .key = &keys.identity_visibility_summary_key, .value = &old_bytes },
        .{ .key = table.key, .value = &catalog_bytes },
        .{ .key = &keys.identity_visibility_summary_key, .value = &desired_bytes },
    };
    var state = try State.capture(&writes, desired, catalog, null);
    try std.testing.expectEqual(@as(?usize, 2), state.visibility_index);
    state.bindReplaySequence(std.math.maxInt(u64), 10001);
    try std.testing.expectEqual(@as(u64, 10001), state.visibility.?.max_created_generation);
    try std.testing.expectEqual(@as(u64, 1), state.visibility.?.min_deleted_generation);
    try std.testing.expectEqualDeep(catalog, state.table_catalog.?);
    try std.testing.expectError(error.PreparedEffectsMismatch, state.validate(&writes, null));
    desired_bytes = identity.encodeVisibilitySummary(state.visibility.?);
    try state.validate(&writes, null);
    try std.testing.expectError(error.PreparedEffectsMismatch, State.capture(writes[0..2], desired, catalog, null));
    try std.testing.expectError(error.PreparedEffectsMismatch, State.capture(writes[0..0], desired, null, null));
    const opaque_writes = [_]KV{.{ .key = table.key, .value = "opaque" }};
    _ = try State.capture(&opaque_writes, null, null, null);
}

test "commit memory state preserves every original construction and validation rejection" {
    const summary: identity.VisibilitySummary = .{ .live_ordinals = 1 };
    const raw = identity.encodeVisibilitySummary(summary);
    const catalog: table.Catalog = .{ .mode_initialized = true, .row_count = 1 };
    const catalog_bytes = catalog.encode();
    const writes = [_]KV{ .{ .key = table.key, .value = &catalog_bytes }, .{ .key = &keys.identity_visibility_summary_key, .value = &raw } };
    const Checks = struct {
        calls: usize = 0,
        stop: usize = std.math.maxInt(usize),
        cause: anyerror = error.DeadlineExceeded,
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == self.stop) return self.cause;
        }
    };
    for ([_]bool{ false, true }) |validation| {
        const state = try State.capture(&writes, summary, catalog, null);
        var baseline: Checks = .{};
        var source: admission.Payload = .{ .ptr = &baseline, .vtable = &.{ .checkpoint = Checks.checkpoint } };
        if (validation) try state.validate(&writes, .init(&source)) else _ = try State.capture(&writes, summary, catalog, .init(&source));
        for ([_]anyerror{ error.NotFound, error.OutOfMemory, error.DeadlineExceeded }) |cause| for (1..baseline.calls + 1) |stop| {
            var failing: Checks = .{ .cause = cause, .stop = stop };
            source.ptr = &failing;
            if (validation) try std.testing.expectError(cause, state.validate(&writes, .init(&source))) else try std.testing.expectError(cause, State.capture(&writes, summary, catalog, .init(&source)));
            try state.validate(&writes, null);
        };
    }
}
