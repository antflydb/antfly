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

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");

const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const DocumentArtifactChildRangeApplyBatch = @import("document_artifact_child_range.zig").ApplyBatch;

pub const DocumentArtifactChildRangeDispatch = struct {
    owner_group_id: u64,
    doc_key: []const u8,
    artifact_name: []const u8,
    child_batch: DocumentArtifactChildRangeApplyBatch,
};

pub const DocumentArtifactChildRangeOutboxDrainResult = struct {
    scanned: usize = 0,
    dispatched: usize = 0,
    deleted: usize = 0,
};

pub const DocumentArtifactChildRangeOutboxRecord = struct {
    version: u16 = 1,
    owner_group_id: u64,
    doc_key: []const u8,
    artifact_name: []const u8,
    child_batch: DocumentArtifactChildRangeApplyBatch,
};

pub const DocumentArtifactChildRangeDispatcher = struct {
    ptr: *anyopaque,
    /// Pure, bounded destination selection; called under the local apply fence.
    select_destination: *const fn (*anyopaque, types.DocumentArtifactChildRange) ?u64,
    apply: *const fn (ptr: *anyopaque, alloc: Allocator, dispatch: DocumentArtifactChildRangeDispatch) anyerror!void,

    pub fn applyDispatch(self: DocumentArtifactChildRangeDispatcher, alloc: Allocator, dispatch: DocumentArtifactChildRangeDispatch) !void {
        return try self.apply(self.ptr, alloc, dispatch);
    }
};

pub fn appendDocumentChildRangeOutboxWrites(
    alloc: Allocator,
    sequence: u64,
    groups: anytype,
    sync_level: types.SyncLevel,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    owned_values: *std.ArrayListUnmanaged([]u8),
) !void {
    for (groups, 0..) |group, i| {
        const key = try internal_keys.documentChildRangeOutboxKeyAlloc(alloc, sequence, @intCast(i));
        errdefer alloc.free(key);
        const value = try encodeDocumentChildRangeOutboxRecordAlloc(alloc, group.dispatch(sync_level));
        errdefer alloc.free(value);
        try owned_keys.ensureUnusedCapacity(alloc, 1);
        try owned_values.ensureUnusedCapacity(alloc, 1);
        try writes.ensureUnusedCapacity(alloc, 1);
        owned_keys.appendAssumeCapacity(key);
        owned_values.appendAssumeCapacity(value);
        writes.appendAssumeCapacity(.{
            .key = key,
            .value = value,
        });
    }
}

pub fn encodeDocumentChildRangeOutboxRecordAlloc(
    alloc: Allocator,
    dispatch: DocumentArtifactChildRangeDispatch,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, DocumentArtifactChildRangeOutboxRecord{
        .owner_group_id = dispatch.owner_group_id,
        .doc_key = dispatch.doc_key,
        .artifact_name = dispatch.artifact_name,
        .child_batch = dispatch.child_batch,
    }, .{});
}

pub fn drain(alloc: Allocator, scanned: []const docstore_mod.OwnedKVPair, dispatcher: DocumentArtifactChildRangeDispatcher, limit: usize, ptr: *anyopaque, remove: *const fn (*anyopaque, []const u8) anyerror!void) !DocumentArtifactChildRangeOutboxDrainResult {
    var result = DocumentArtifactChildRangeOutboxDrainResult{};
    const max_entries = if (limit == 0 or limit > scanned.len) scanned.len else limit;
    for (scanned[0..max_entries]) |entry| {
        result.scanned += 1;
        var parsed = try std.json.parseFromSlice(DocumentArtifactChildRangeOutboxRecord, alloc, entry.value, .{
            .allocate = .alloc_always,
        });
        defer parsed.deinit();
        if (parsed.value.version != 1) return error.InvalidDocumentChildRangeOutboxRecord;
        try dispatcher.applyDispatch(alloc, .{
            .owner_group_id = parsed.value.owner_group_id,
            .doc_key = parsed.value.doc_key,
            .artifact_name = parsed.value.artifact_name,
            .child_batch = parsed.value.child_batch,
        });
        result.dispatched += 1;
        try remove(ptr, entry.key);
        result.deleted += 1;
    }
    return result;
}

test "child range outbox staging transfers ownership only after reserving all lists" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        const Group = struct {
            pub fn dispatch(_: @This(), level: types.SyncLevel) DocumentArtifactChildRangeDispatch {
                return .{ .owner_group_id = 7, .doc_key = "doc", .artifact_name = "units", .child_batch = .{ .sync_level = level } };
            }
        };
        fn run(alloc: Allocator) !void {
            var writes: std.ArrayListUnmanaged(docstore_mod.KVPair) = .empty;
            defer writes.deinit(alloc);
            var keys: std.ArrayListUnmanaged([]u8) = .empty;
            defer {
                for (keys.items) |key| alloc.free(key);
                keys.deinit(alloc);
            }
            var values: std.ArrayListUnmanaged([]u8) = .empty;
            defer {
                for (values.items) |value| alloc.free(value);
                values.deinit(alloc);
            }
            try appendDocumentChildRangeOutboxWrites(alloc, 3, &[_]Group{ .{}, .{} }, .write, &writes, &keys, &values);
            try std.testing.expectEqual(@as(usize, 2), writes.items.len);
            var parsed = try std.json.parseFromSlice(DocumentArtifactChildRangeOutboxRecord, alloc, values.items[0], .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(u16, 1), parsed.value.version);
            try std.testing.expectEqual(@as(u64, 7), parsed.value.owner_group_id);
        }
    }.run, .{});
}
