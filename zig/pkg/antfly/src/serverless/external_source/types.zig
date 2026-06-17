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

//! Metadata artifacts for user-owned external lake files referenced by
//! serverless manifests.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Format = enum(u8) {
    parquet = 1,
    iceberg = 2,
    lance = 3,
};

pub const RowGroup = struct {
    ordinal: u32,
    row_count: u64,

    pub fn validate(self: RowGroup) !void {
        if (self.row_count == 0) return error.InvalidExternalSourceInventory;
    }
};

pub const FileEntry = struct {
    file_id: []u8,
    object_uri: []u8,
    byte_len: u64,
    row_count: u64,
    row_groups: []RowGroup,

    pub fn deinit(self: *FileEntry, alloc: Allocator) void {
        alloc.free(self.file_id);
        alloc.free(self.object_uri);
        alloc.free(self.row_groups);
        self.* = undefined;
    }

    pub fn validate(self: FileEntry) !void {
        if (self.file_id.len == 0) return error.InvalidExternalSourceInventory;
        if (self.object_uri.len == 0) return error.InvalidExternalSourceInventory;
        if (self.byte_len == 0) return error.InvalidExternalSourceInventory;
        var total_rows: u64 = 0;
        for (self.row_groups, 0..) |row_group, idx| {
            try row_group.validate();
            if (row_group.ordinal != idx) return error.InvalidExternalSourceInventory;
            total_rows += row_group.row_count;
        }
        if (self.row_groups.len != 0 and total_rows != self.row_count) return error.InvalidExternalSourceInventory;
    }
};

pub const Inventory = struct {
    format: Format,
    source_id: []u8,
    source_uri: []u8,
    snapshot_id: []u8,
    schema_fingerprint: []u8,
    files: []FileEntry,

    pub fn deinit(self: *Inventory, alloc: Allocator) void {
        alloc.free(self.source_id);
        alloc.free(self.source_uri);
        alloc.free(self.snapshot_id);
        alloc.free(self.schema_fingerprint);
        for (self.files) |*file| file.deinit(alloc);
        alloc.free(self.files);
        self.* = undefined;
    }

    pub fn validate(self: Inventory) !void {
        if (self.source_id.len == 0) return error.InvalidExternalSourceInventory;
        if (self.source_uri.len == 0) return error.InvalidExternalSourceInventory;
        if (self.snapshot_id.len == 0) return error.InvalidExternalSourceInventory;
        if (self.schema_fingerprint.len == 0) return error.InvalidExternalSourceInventory;
        for (self.files, 0..) |file, idx| {
            try file.validate();
            for (self.files[0..idx]) |previous| {
                if (std.mem.eql(u8, previous.file_id, file.file_id)) return error.InvalidExternalSourceInventory;
            }
        }
    }

    pub fn fileById(self: Inventory, file_id: []const u8) ?FileEntry {
        for (self.files) |file| {
            if (std.mem.eql(u8, file.file_id, file_id)) return file;
        }
        return null;
    }
};

pub fn freeInventory(alloc: Allocator, inventory: *Inventory) void {
    inventory.deinit(alloc);
}

test "external source inventory validates files and row groups" {
    const alloc = std.testing.allocator;
    var inventory = Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/warehouse/events"),
        .snapshot_id = try alloc.dupe(u8, "iceberg-123"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "file-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/warehouse/events/file-a.parquet"),
        .byte_len = 1024,
        .row_count = 3,
        .row_groups = try alloc.dupe(RowGroup, &[_]RowGroup{
            .{ .ordinal = 0, .row_count = 1 },
            .{ .ordinal = 1, .row_count = 2 },
        }),
    };

    try inventory.validate();
    try std.testing.expect(inventory.fileById("file-a.parquet") != null);
    try std.testing.expect(inventory.fileById("missing.parquet") == null);
}
