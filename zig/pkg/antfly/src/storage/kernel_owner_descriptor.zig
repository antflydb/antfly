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

//! Deterministic storage-owner open contract carried by replicated apply.
//! This module intentionally contains no DB, catalog, or runtime imports.

pub const Identity = struct {
    table_id: u64,
    shard_id: u64,
    range_id: u64,

    pub fn eql(left: Identity, right: Identity) bool {
        return left.table_id == right.table_id and
            left.shard_id == right.shard_id and
            left.range_id == right.range_id;
    }
};

pub const Descriptor = struct {
    lsm_root_generation: u64,
    identity: Identity,
    schema_json: []const u8 = "",
    indexes_json: []const u8 = "",
    /// Authenticated first-open hint, not an instruction to replace a durable
    /// range subsequently changed by replicated split/merge commands.
    initial_range: ?@import("byte_range.zig").ByteRange = null,
    /// Immutable, privately authenticated restore authority. Never populated
    /// from the public catalog or a client-supplied table definition.
    restore_bootstrap_json: []const u8 = "",
    restore_cancel_recovery: bool = false,
    restore_ha_replay: bool = false,
    table_storage: ?@import("../common/table_storage.zig").Settings = null,
    restore: ?@import("restore_identity.zig").Identity = null,
};

pub fn cloneInitialRange(alloc: @import("std").mem.Allocator, range: ?@import("byte_range.zig").ByteRange) !?@import("byte_range.zig").ByteRange {
    const value = range orelse return null;
    const start = try alloc.dupe(u8, value.start);
    errdefer alloc.free(start);
    return .{ .start = start, .end = try alloc.dupe(u8, value.end) };
}

pub fn freeInitialRange(alloc: @import("std").mem.Allocator, range: ?@import("byte_range.zig").ByteRange) void {
    if (range) |value| {
        alloc.free(value.start);
        alloc.free(value.end);
    }
}

pub fn initialRangesEqual(left: ?@import("byte_range.zig").ByteRange, right: ?@import("byte_range.zig").ByteRange) bool {
    if (left == null or right == null) return left == null and right == null;
    return @import("std").mem.eql(u8, left.?.start, right.?.start) and @import("std").mem.eql(u8, left.?.end, right.?.end);
}
