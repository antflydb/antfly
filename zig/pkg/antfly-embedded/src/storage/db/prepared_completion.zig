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

//! Owned post-commit derived inputs and convergence targets for a retained
//! mutation assembly. Capture retires all invocation-local buffers.
const std = @import("std");
const derived = @import("derived/derived_types.zig");
const targets_mod = @import("managed_sync_targets.zig");
const types = @import("types.zig");
const admission = @import("../../common/read_admission.zig");

/// Data needed after primary publication. This owns no DB, route, worker,
/// admission, or recovery authority; those lifetimes must be funded separately.
pub const Owned = struct {
    arena: std.heap.ArenaAllocator,
    batch: ?derived.DerivedBatch,
    targets: targets_mod.ManagedSyncTargets,
    sync_level: types.SyncLevel,
    append_replay: bool,
    sequence: u64,

    pub fn capture(alloc: std.mem.Allocator, source: ?derived.DerivedBatch, targets: targets_mod.ManagedSyncTargets, sync_level: types.SyncLevel, append_replay: bool, sequence: u64, control: ?admission.Port) !Owned {
        if (control) |owner| try owner.checkpoint();
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        var batch = if (source) |value| try derived.cloneBatch(a, value) else null;
        if (batch) |*value| {
            if (value.sequence != sequence) return error.InvalidDerivedSequence;
            // Replay clones intentionally discard invocation-local projections.
            // Completion also feeds shadow/inline execution, so retain those
            // typed roots and their schema witnesses in this owner's arena.
            for (@constCast(value.documents), source.?.documents) |*doc, original| {
                if (control) |owner| try owner.checkpoint();
                doc.prepared_text_root = if (original.prepared_text_root) |root| try types.cloneJsonValue(a, root) else null;
                doc.prepared_text_source_bytes = original.prepared_text_source_bytes;
                doc.prepared_schema_version = original.prepared_schema_version;
                doc.prepared_write_plan_generation = original.prepared_write_plan_generation;
            }
        }
        var copied: targets_mod.ManagedSyncTargets = .{ .target_scope_known = targets.target_scope_known };
        copied.full_text_indexes = try copyNames(a, targets.full_text_indexes, control);
        copied.all_indexes = try copyNames(a, targets.all_indexes, control);
        const identities = try a.dupe(types.IndexTargetVisibility, targets.target_identities);
        for (identities) |*identity| {
            if (control) |owner| try owner.checkpoint();
            identity.index_name = try a.dupe(u8, identity.index_name);
        }
        copied.target_identities = identities;
        if (control) |owner| try owner.checkpoint();
        return .{ .arena = arena, .batch = batch, .targets = copied, .sync_level = sync_level, .append_replay = append_replay, .sequence = sequence };
    }

    fn copyNames(a: std.mem.Allocator, names: []const []const u8, control: ?admission.Port) ![]const []const u8 {
        const result = try a.alloc([]const u8, names.len);
        for (result, names) |*destination, name| {
            if (control) |owner| try owner.checkpoint();
            destination.* = try a.dupe(u8, name);
        }
        return result;
    }

    pub fn validateSequence(self: *const Owned, expected: u64) !void {
        if (self.append_replay and self.sequence != expected) return error.InvalidDerivedSequence;
        if (self.batch) |batch| if (batch.sequence != self.sequence) return error.InvalidDerivedSequence;
    }

    /// Called only after all dependency/sequence validation has succeeded.
    pub fn bindReplaySequence(self: *Owned, previous: u64, next: u64) void {
        if (self.sequence == previous) {
            self.sequence = next;
            if (self.batch) |*batch| batch.sequence = next;
        }
    }

    pub fn deinit(self: *Owned) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

const Fixture = struct {
    fn capture(alloc: std.mem.Allocator, control: ?admission.Port) !Owned {
        var scratch = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer scratch.deinit();
        const a = scratch.allocator();
        const name = try a.dupe(u8, "text");
        const key = try a.dupe(u8, "row");
        const json = try std.json.parseFromSlice(std.json.Value, a, "{\"nested\":[\"retained\",7]}", .{ .allocate = .alloc_always });
        const vector = try a.dupe(f32, &.{ 1, 2 });
        return Owned.capture(alloc, .{
            .sequence = 17,
            .documents = &.{.{ .key = key, .cleaned_value = key, .targets = &.{.{ .kind = .full_text, .index_name = name }}, .prepared_text_root = json.value, .prepared_text_source_bytes = 29, .prepared_schema_version = 3, .prepared_write_plan_generation = 8 }},
            .dense_embeddings = &.{.{ .index_name = name, .doc_key = key, .vector = vector }},
            .deleted_keys = &.{key},
        }, .{ .full_text_indexes = &.{name}, .all_indexes = &.{name}, .target_identities = &.{.{ .index_name = name, .kind = .full_text, .incarnation = 19, .config_hash = 23, .serving_set_effect = .additive_only }}, .target_scope_known = true }, .full_text, true, 17, control);
    }
    fn run(alloc: std.mem.Allocator) !void {
        var owned = try capture(alloc, null);
        defer owned.deinit();
        try std.testing.expectEqualStrings("row", owned.batch.?.documents[0].key);
        const doc = owned.batch.?.documents[0];
        try std.testing.expectEqualStrings("retained", doc.prepared_text_root.?.object.get("nested").?.array.items[0].string);
        try std.testing.expectEqual(@as(u32, 3), doc.prepared_schema_version);
        try std.testing.expectEqual(@as(u64, 8), doc.prepared_write_plan_generation);
        try std.testing.expectEqual(@as(usize, 29), doc.prepared_text_source_bytes);
        try std.testing.expectEqualSlices(f32, &.{ 1, 2 }, owned.batch.?.dense_embeddings[0].vector);
        try std.testing.expectEqualStrings("row", owned.batch.?.deleted_keys[0]);
        try std.testing.expectEqualStrings("text", owned.targets.full_text_indexes[0]);
        try std.testing.expectEqualStrings("text", owned.targets.all_indexes[0]);
        try std.testing.expectEqualStrings("text", owned.targets.target_identities[0].index_name);
        try std.testing.expectEqual(@as(u64, 19), owned.targets.target_identities[0].incarnation);
        try std.testing.expectEqual(@as(u64, 23), owned.targets.target_identities[0].config_hash);
        try std.testing.expectEqual(types.IndexTargetVisibility.ServingSetEffect.additive_only, owned.targets.target_identities[0].serving_set_effect);
        try std.testing.expect(owned.targets.target_scope_known);
        try std.testing.expectEqual(types.SyncLevel.full_text, owned.sync_level);
        try owned.validateSequence(17);
        try std.testing.expectError(error.InvalidDerivedSequence, owned.validateSequence(18));
        const backing = owned.batch.?.documents.ptr;
        owned.bindReplaySequence(17, 10001);
        try owned.validateSequence(10001);
        try std.testing.expectEqual(backing, owned.batch.?.documents.ptr);
    }
};

test "prepared completion owns projections targets and vectors after scratch retirement at every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "prepared completion preserves original checkpoint rejections and empty no-replay obligations" {
    const Check = struct {
        calls: usize = 0,
        stop: usize = std.math.maxInt(usize),
        cause: anyerror = error.DeadlineExceeded,
        fn check(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == self.stop) return self.cause;
        }
    };
    var checks: Check = .{};
    var payload: admission.Payload = .{ .ptr = &checks, .vtable = &.{ .checkpoint = Check.check } };
    var baseline = try Fixture.capture(std.testing.allocator, .init(&payload));
    defer baseline.deinit();
    const calls = checks.calls;
    for ([_]anyerror{ error.NotFound, error.OutOfMemory, error.DeadlineExceeded }) |cause| for (1..calls + 1) |stop| {
        checks = .{ .stop = stop, .cause = cause };
        try std.testing.expectError(cause, Fixture.capture(std.testing.allocator, .init(&payload)));
    };
    var empty = try Owned.capture(std.testing.allocator, null, .{}, .write, false, 7, null);
    defer empty.deinit();
    try empty.validateSequence(19);
    try std.testing.expect(empty.batch == null and !empty.append_replay and !empty.targets.target_scope_known);
    empty.bindReplaySequence(19, 23);
    try std.testing.expectEqual(@as(u64, 7), empty.sequence);
}
