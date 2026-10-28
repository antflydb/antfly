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

//! Pre-WAL metadata credit. Group by the actual output partition, not by row:
//! a large ordinary batch must not reserve one manifest descriptor per row.
const std = @import("std");
const Key = struct { namespace: []const u8, namespace_present: bool, prefix: []const u8, domain: []const u8 };
const Context = struct {
    pub fn hash(_: @This(), key: Key) u64 {
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(std.mem.asBytes(&key.namespace_present));
        for ([_][]const u8{ key.namespace, key.prefix, key.domain }) |part| {
            hasher.update(std.mem.asBytes(&part.len));
            hasher.update(part);
        }
        return hasher.final();
    }
    pub fn eql(_: @This(), a: Key, b: Key) bool {
        return a.namespace_present == b.namespace_present and std.mem.eql(u8, a.namespace, b.namespace) and std.mem.eql(u8, a.prefix, b.prefix) and std.mem.eql(u8, a.domain, b.domain);
    }
};
const Group = struct { rows: u64 = 0, logical: u64 = 0, physical: u64 = 0, bound_bytes: u64 = 0 };
const Groups = std.HashMapUnmanaged(Key, Group, Context, 80);

pub fn estimate(allocator: std.mem.Allocator, options: anytype, root_len: usize, first: anytype, second: anytype) !u64 {
    var groups: Groups = .empty;
    defer groups.deinit(allocator);
    inline for (.{ first, second }) |state| {
        for (0..state.entryCount()) |i| {
            const entry = state.entryAt(i);
            const partitioned = options.run_partition_prefix_bytes != 0 or options.run_partition_key != null;
            const key = Key{
                .namespace = if (partitioned) entry.namespace_name orelse "" else "",
                .namespace_present = partitioned and entry.namespace_name != null,
                .prefix = entry.key[0..@min(options.run_partition_prefix_bytes, entry.key.len)],
                .domain = if (options.run_partition_key) |extract| extract(entry.key) else "",
            };
            const result = try groups.getOrPut(allocator, key);
            if (!result.found_existing) result.value_ptr.* = .{};
            const group = result.value_ptr;
            const names = entry.key.len + (if (entry.namespace_name) |name| name.len else 0);
            group.rows +|= 1;
            group.logical +|= 32 +| names +| entry.value.len;
            // Raw block payload, per-entry offsets, Bloom/prefix filters and
            // worst-case one-entry-block bounds. Compression falls back to
            // raw bytes when its output grows.
            group.physical +|= 256 +| 8 *| names +| 2 *| entry.value.len;
            group.bound_bytes = @max(group.bound_bytes, names);
        }
    }
    if (groups.count() == 0) return 0;
    var bytes: u64 = 128;
    var iterator = groups.valueIterator();
    while (iterator.next()) |group| {
        bytes +|= groupWire(options, root_len, group.*);
    }
    return bytes;
}

fn groupWire(options: anytype, root_len: usize, group: Group) u64 {
    const logical_limit = @max(@as(u64, 1), @min(options.max_run_file_bytes, @import("../lsm/table_file.zig").max_entry_data_len));
    const physical_limit = @max(@as(u64, 1), @min(options.max_run_file_physical_bytes, @import("repository.zig").maxRunFileReadBytes()));
    // Greedy packing has at most 1 + 2*sum/capacity outputs. Account for
    // each independent splitting limit; tiny physical caps fall back to
    // the one-row bound. No assumption about input insertion order.
    var outputs = @min(group.rows, 1 +| (2 *| group.logical) / logical_limit);
    outputs = @min(group.rows, outputs +| if (physical_limit <= 64 * 1024) group.rows else (2 *| group.physical) / (physical_limit - 64 * 1024));
    if (options.max_run_file_entries != 0) outputs = @min(group.rows, outputs +| group.rows / options.max_run_file_entries);
    return outputs *| (192 +| root_len +| 2 *| group.bound_bytes);
}

/// Allocation-free credit planning over a stable sorted generation. Partition
/// identities must describe contiguous intervals, as required by Options.
/// Borrowed labels remain pinned by the generation until this cursor finishes;
/// no caller execution or allocator is retained.
pub const SortedEstimate = struct {
    index: usize = 0,
    group: Group = .{},
    key: ?Key = null,
    pending: ?Key = null,
    compare_part: usize = 0,
    compared: usize = 0,
    same: bool = true,
    bytes: u64 = 128,
    complete: bool = false,

    pub fn resumeWithAdmission(self: *@This(), options: anytype, root_len: usize, state: anytype, original: @import("../../common/read_admission.zig").Port) !u64 {
        try original.checkpoint();
        if (self.complete) return self.bytes;
        while (self.index < state.entryCount()) {
            try original.checkpoint();
            const entry = state.entryAt(self.index);
            if (self.pending == null) {
                const partitioned = options.run_partition_prefix_bytes != 0 or options.run_partition_key != null;
                const domain = if (options.run_partition_key != null) blk: {
                    const extract = options.run_partition_key_with_admission orelse return error.ExecutionWaitUnsupported;
                    break :blk try extract(entry.key, original);
                } else "";
                self.pending = .{
                    .namespace = if (partitioned) entry.namespace_name orelse "" else "",
                    .namespace_present = partitioned and entry.namespace_name != null,
                    .prefix = entry.key[0..@min(options.run_partition_prefix_bytes, entry.key.len)],
                    .domain = domain,
                };
                self.same = if (self.key) |key| key.namespace_present == self.pending.?.namespace_present else false;
                self.compare_part = 0;
                self.compared = 0;
            }
            if (self.key) |key| {
                const before = [_][]const u8{ key.namespace, key.prefix, key.domain };
                const after = [_][]const u8{ self.pending.?.namespace, self.pending.?.prefix, self.pending.?.domain };
                while (self.same and self.compare_part < 3) {
                    try original.checkpoint();
                    const a = before[self.compare_part];
                    const b = after[self.compare_part];
                    if (a.len != b.len) {
                        self.same = false;
                        break;
                    }
                    const end = self.compared + @min(a.len - self.compared, 16 * 1024);
                    if (!std.mem.eql(u8, a[self.compared..end], b[self.compared..end])) {
                        self.same = false;
                        break;
                    }
                    self.compared = end;
                    if (end == a.len) {
                        self.compare_part += 1;
                        self.compared = 0;
                    }
                }
            }
            try original.checkpoint();
            if (!self.same) {
                if (self.key != null) self.bytes +|= groupWire(options, root_len, self.group);
                self.group = .{};
                self.key = self.pending;
            }
            const names = @as(u64, entry.key.len) +| if (entry.namespace_name) |name| name.len else @as(usize, 0);
            self.group.rows +|= 1;
            self.group.logical +|= 32 +| names +| entry.value.len;
            self.group.physical +|= 256 +| 8 *| names +| 2 *| entry.value.len;
            self.group.bound_bytes = @max(self.group.bound_bytes, names);
            self.pending = null;
            self.index += 1;
        }
        try original.checkpoint();
        self.bytes = if (self.key != null) self.bytes +| groupWire(options, root_len, self.group) else 0;
        self.key = null;
        self.complete = true;
        return self.bytes;
    }
};
