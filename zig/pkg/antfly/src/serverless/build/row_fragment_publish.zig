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

//! Publication bridge for Antfly-owned row fragments: encode a RowSource batch,
//! write it through the serverless artifact store, and produce manifest plan
//! metadata that can be attached to the next published generation.

const std = @import("std");
const Allocator = std.mem.Allocator;
const artifact_store = @import("../artifacts/store.zig");
const row_fragment_manifest = @import("row_fragment_manifest.zig");
const row_fragments = @import("row_fragments.zig");
const rowsource = @import("../../storage/rowsource/types.zig");

pub const PublishOptions = struct {
    schema_fingerprint: []const u8,
    artifact_name: []const u8 = &.{},
    stats_artifact_name: []const u8 = &.{},
    projected_columns: ?[]const []const u8 = null,
    max_dictionary_samples: usize = 16,
};

pub const PublishResult = struct {
    plan: row_fragment_manifest.Plan,

    pub fn deinit(self: *PublishResult, alloc: Allocator) void {
        self.plan.deinit(alloc);
        self.* = undefined;
    }
};

pub fn publishBatchAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    batch: rowsource.ColumnBatch,
    options: PublishOptions,
) !PublishResult {
    if (options.schema_fingerprint.len == 0) return error.InvalidRowFragmentPublishOptions;
    try batch.validate();

    const encoded = try row_fragments.encodeFragmentFromBatchAlloc(alloc, batch, .{
        .schema_fingerprint = options.schema_fingerprint,
        .projected_columns = options.projected_columns,
        .max_dictionary_samples = options.max_dictionary_samples,
    });
    defer alloc.free(encoded);

    var metadata = try artifacts.put(encoded);
    defer metadata.deinit(alloc);

    const encoded_stats = try row_fragments.encodeFragmentStatsFromBatchAlloc(alloc, batch, .{
        .schema_fingerprint = options.schema_fingerprint,
        .projected_columns = options.projected_columns,
        .max_dictionary_samples = options.max_dictionary_samples,
    });
    defer alloc.free(encoded_stats);

    var stats_metadata = try artifacts.put(encoded_stats);
    defer stats_metadata.deinit(alloc);

    const published = [_]row_fragment_manifest.PublishedArtifact{
        .{
            .artifact_id = metadata.artifact_id,
            .byte_len = metadata.byte_len,
            .checksum = metadata.checksum,
            .name = options.artifact_name,
        },
    };
    const published_stats = [_]row_fragment_manifest.PublishedArtifact{
        .{
            .artifact_id = stats_metadata.artifact_id,
            .byte_len = stats_metadata.byte_len,
            .checksum = stats_metadata.checksum,
            .name = options.stats_artifact_name,
        },
    };

    return .{
        .plan = try row_fragment_manifest.planAlloc(
            alloc,
            batch.snapshot.snapshot_id,
            options.schema_fingerprint,
            &published,
            &published_stats,
        ),
    };
}

const MemoryArtifactStore = struct {
    alloc: Allocator,
    entries: std.StringHashMapUnmanaged([]u8) = .empty,
    next_id: u64 = 0,

    fn init(alloc: Allocator) MemoryArtifactStore {
        return .{ .alloc = alloc };
    }

    fn deinit(self: *MemoryArtifactStore) void {
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            self.alloc.free(entry.value_ptr.*);
        }
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }

    fn artifactStore(self: *MemoryArtifactStore) artifact_store.ArtifactStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    fn put(self: *MemoryArtifactStore, alloc: Allocator, contents: []const u8) !artifact_store.ArtifactMetadata {
        const artifact_id = try std.fmt.allocPrint(alloc, "mem:{d}", .{self.next_id});
        errdefer alloc.free(artifact_id);
        self.next_id += 1;
        const stored_key = try self.alloc.dupe(u8, artifact_id);
        errdefer self.alloc.free(stored_key);
        const stored_value = try self.alloc.dupe(u8, contents);
        errdefer self.alloc.free(stored_value);
        try self.entries.put(self.alloc, stored_key, stored_value);
        const checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{contents.len});
        errdefer alloc.free(checksum);
        return .{
            .artifact_id = artifact_id,
            .byte_len = @intCast(contents.len),
            .checksum = checksum,
        };
    }

    fn getAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        const value = self.entries.get(artifact_id) orelse return error.ArtifactNotFound;
        return try alloc.dupe(u8, value);
    }

    fn getRangeAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        const value = self.entries.get(artifact_id) orelse return error.ArtifactNotFound;
        if (offset > value.len) return error.InvalidRange;
        const start: usize = @intCast(offset);
        const end = @min(value.len, start + len);
        return try alloc.dupe(u8, value[start..end]);
    }

    fn stat(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        const value = self.entries.get(artifact_id) orelse return error.ArtifactNotFound;
        return .{
            .artifact_id = try alloc.dupe(u8, artifact_id),
            .byte_len = @intCast(value.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{value.len}),
        };
    }

    fn delete(self: *MemoryArtifactStore, artifact_id: []const u8) !void {
        const entry = self.entries.fetchRemove(artifact_id) orelse return error.ArtifactNotFound;
        self.alloc.free(entry.key);
        self.alloc.free(entry.value);
    }

    const vtable: artifact_store.ArtifactStore.VTable = .{
        .deinit = erasedDeinit,
        .put = erasedPut,
        .get_alloc = erasedGetAlloc,
        .get_range_alloc = erasedGetRangeAlloc,
        .stat = erasedStat,
        .delete = erasedDelete,
    };

    fn erasedDeinit(_: Allocator, ptr: *anyopaque) void {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedPut(ptr: *anyopaque, alloc: Allocator, contents: []const u8) !artifact_store.ArtifactMetadata {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.put(alloc, contents);
    }

    fn erasedGetAlloc(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.getAlloc(alloc, artifact_id);
    }

    fn erasedGetRangeAlloc(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.getRangeAlloc(alloc, artifact_id, offset, len);
    }

    fn erasedStat(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.stat(alloc, artifact_id);
    }

    fn erasedDelete(ptr: *anyopaque, artifact_id: []const u8) !void {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        try self.delete(artifact_id);
    }
};

test "row fragment publisher writes artifact and returns manifest plan" {
    const alloc = std.testing.allocator;
    var memory = MemoryArtifactStore.init(alloc);
    var artifacts = memory.artifactStore();
    defer artifacts.deinit();

    const row_refs = [_]rowsource.RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
    };
    const amounts = [_]i64{ 10, 20 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batch = rowsource.ColumnBatch{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "manifest-7" },
        .row_refs = &row_refs,
        .columns = &columns,
    };

    var result = try publishBatchAlloc(alloc, &artifacts, batch, .{
        .schema_fingerprint = "schema-v1",
        .artifact_name = "orders.rows",
    });
    defer result.deinit(alloc);

    const artifact = result.plan.artifacts[0];
    try std.testing.expectEqualStrings("orders.rows", artifact.name);
    try std.testing.expectEqualStrings("mem:0", artifact.artifact_id);
    try std.testing.expectEqualStrings("mem:0", result.plan.base_source.antfly_row_fragments.row_fragment_artifacts[0]);
    try std.testing.expectEqualStrings("mem:1", result.plan.base_source.antfly_row_fragments.row_fragment_stats_artifacts[0]);

    const encoded = try artifacts.getAlloc(artifact.artifact_id);
    defer alloc.free(encoded);
    try std.testing.expect(encoded.len > 0);

    const stats_artifact = result.plan.artifacts[1];
    try std.testing.expectEqualStrings("mem:1", stats_artifact.artifact_id);
    const encoded_stats = try artifacts.getAlloc(stats_artifact.artifact_id);
    defer alloc.free(encoded_stats);
    try std.testing.expect(std.mem.indexOf(u8, encoded_stats, "\"name\":\"amount\"") != null);
}
