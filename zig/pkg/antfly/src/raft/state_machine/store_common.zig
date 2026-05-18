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

const std = @import("std");
const fs_paths = @import("../../common/fs_paths.zig");
const mod = @import("mod.zig");

pub fn ApplyStore(comptime namespace: []const u8) type {
    return struct {
        const Self = @This();

        pub const Config = struct {
            root_dir: []const u8,
        };

        pub const StoredBatch = struct {
            commit_index: u64,
            entries_bytes: []const u8,
        };

        alloc: std.mem.Allocator,
        io_impl: std.Io.Threaded,
        root_dir: []u8,
        batches: std.AutoHashMapUnmanaged(u64, OwnedBatch) = .empty,

        const OwnedBatch = struct {
            commit_index: u64,
            entries_bytes: []u8,
        };

        pub fn init(alloc: std.mem.Allocator, cfg: Config) !Self {
            return .{
                .alloc = alloc,
                .io_impl = std.Io.Threaded.init(alloc, .{}),
                .root_dir = try alloc.dupe(u8, cfg.root_dir),
            };
        }

        pub fn deinit(self: *Self) void {
            var it = self.batches.valueIterator();
            while (it.next()) |batch| self.alloc.free(batch.entries_bytes);
            self.batches.deinit(self.alloc);
            self.alloc.free(self.root_dir);
            self.io_impl.deinit();
            self.* = undefined;
        }

        pub fn snapshotBuilder(self: *Self) mod.SnapshotBuilder {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_snapshot = buildSnapshot,
                    .apply_batch = applyBatch,
                },
            };
        }

        pub fn latest(self: *Self, group_id: u64) !?StoredBatch {
            if (try self.ensureLoaded(group_id)) |batch| {
                return .{
                    .commit_index = batch.commit_index,
                    .entries_bytes = batch.entries_bytes,
                };
            }
            return null;
        }

        fn buildSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const batch = try self.ensureLoaded(group_id) orelse return error.MissingAppliedBatch;
            return try alloc.dupe(u8, batch.entries_bytes);
        }

        fn applyBatch(ptr: *anyopaque, batch: mod.ApplyBatch) !void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.writeBatch(batch.group_id, batch.commit_index, batch.entries_bytes);
        }

        fn writeBatch(self: *Self, group_id: u64, commit_index: u64, entries_bytes: []const u8) !void {
            const dir_path = try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ self.root_dir, namespace });
            defer self.alloc.free(dir_path);
            try fs_paths.createDirPathPortable(self.io(), dir_path);

            const file_path = try groupFilePath(self.alloc, self.root_dir, group_id);
            defer self.alloc.free(file_path);

            const file_bytes = try self.alloc.alloc(u8, @sizeOf(u64) + entries_bytes.len);
            defer self.alloc.free(file_bytes);
            std.mem.writeInt(u64, file_bytes[0..8], commit_index, .little);
            @memcpy(file_bytes[8..], entries_bytes);
            try std.Io.Dir.cwd().writeFile(self.io(), .{
                .sub_path = file_path,
                .data = file_bytes,
            });

            const owned_entries = try self.alloc.dupe(u8, entries_bytes);
            errdefer self.alloc.free(owned_entries);
            if (self.batches.getPtr(group_id)) |existing| {
                self.alloc.free(existing.entries_bytes);
                existing.* = .{
                    .commit_index = commit_index,
                    .entries_bytes = owned_entries,
                };
                return;
            }
            try self.batches.put(self.alloc, group_id, .{
                .commit_index = commit_index,
                .entries_bytes = owned_entries,
            });
        }

        fn ensureLoaded(self: *Self, group_id: u64) !?*OwnedBatch {
            if (self.batches.getPtr(group_id)) |batch| return batch;

            const file_path = try groupFilePath(self.alloc, self.root_dir, group_id);
            defer self.alloc.free(file_path);

            const encoded = std.Io.Dir.cwd().readFileAlloc(self.io(), file_path, self.alloc, .limited(16 * 1024 * 1024)) catch |err| switch (err) {
                error.FileNotFound => return null,
                else => return err,
            };
            defer self.alloc.free(encoded);
            if (encoded.len < @sizeOf(u64)) return error.CorruptAppliedBatch;

            const commit_index = std.mem.readInt(u64, encoded[0..8], .little);
            const owned_entries = try self.alloc.dupe(u8, encoded[8..]);
            errdefer self.alloc.free(owned_entries);
            try self.batches.put(self.alloc, group_id, .{
                .commit_index = commit_index,
                .entries_bytes = owned_entries,
            });
            return self.batches.getPtr(group_id);
        }

        fn groupFilePath(alloc: std.mem.Allocator, root_dir: []const u8, group_id: u64) ![]u8 {
            return try std.fmt.allocPrint(alloc, "{s}/{s}/group-{d}.bin", .{ root_dir, namespace, group_id });
        }

        fn io(self: *Self) std.Io {
            return self.io_impl.io();
        }
    };
}

test "metadata and data apply stores persist batches across reopen" {
    const MetadataStore = ApplyStore("metadata");
    const DataStore = ApplyStore("data");

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/apply-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    {
        var metadata_store = try MetadataStore.init(std.testing.allocator, .{ .root_dir = root });
        defer metadata_store.deinit();
        var data_store = try DataStore.init(std.testing.allocator, .{ .root_dir = root });
        defer data_store.deinit();

        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 11,
            .commit_index = 7,
            .entries_bytes = "metadata-bytes",
        });
        try data_store.snapshotBuilder().applyBatch(.{
            .group_id = 12,
            .commit_index = 9,
            .entries_bytes = "data-bytes",
        });
    }

    {
        var metadata_store = try MetadataStore.init(std.testing.allocator, .{ .root_dir = root });
        defer metadata_store.deinit();
        var data_store = try DataStore.init(std.testing.allocator, .{ .root_dir = root });
        defer data_store.deinit();

        const metadata_latest = (try metadata_store.latest(11)) orelse return error.MissingMetadataBatch;
        try std.testing.expectEqual(@as(u64, 7), metadata_latest.commit_index);
        try std.testing.expectEqualStrings("metadata-bytes", metadata_latest.entries_bytes);

        const data_latest = (try data_store.latest(12)) orelse return error.MissingDataBatch;
        try std.testing.expectEqual(@as(u64, 9), data_latest.commit_index);
        try std.testing.expectEqualStrings("data-bytes", data_latest.entries_bytes);

        const metadata_snapshot = try metadata_store.snapshotBuilder().buildSnapshot(std.testing.allocator, 11);
        defer std.testing.allocator.free(metadata_snapshot);
        try std.testing.expectEqualStrings("metadata-bytes", metadata_snapshot);

        const data_snapshot = try data_store.snapshotBuilder().buildSnapshot(std.testing.allocator, 12);
        defer std.testing.allocator.free(data_snapshot);
        try std.testing.expectEqualStrings("data-bytes", data_snapshot);
    }
}
