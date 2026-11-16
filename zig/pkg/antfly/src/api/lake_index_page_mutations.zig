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

//! Bounded private metadata reduction. One external sort owns staging and
//! compaction; leased records flow directly into the final copy-on-write tree.
const std = @import("std");
const local = @import("antfly_local_sources");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const A = std.mem.Allocator;
const Id = [32]u8;
const max_blob_bytes = 4 * 1024 * 1024;
const chunk_bytes = 16 * 1024;
pub const Mutations = struct {
    a: A,
    sort: local.sql_spill.Sort,
    destination: tree.Store,
    reduction: @FieldType(tree.Store, "reduce") = null,
    ordinal: u64 = 0,
    addition_first: bool = false,
    pub fn init(a: A, manager: *local.sql_spill.Manager, storage: tree.Store) Mutations {
        var sort = local.sql_spill.Sort.init(a, manager, &.{.{}}, 1024 * 1024);
        sort.shared_runs = true;
        sort.encoded_bytes = true;
        return .{ .a = a, .sort = sort, .destination = storage };
    }
    pub fn deinit(self: *Mutations) void {
        self.sort.deinit();
    }
    pub fn add(self: *Mutations, id: Id, value: ?[]const u8, previous_length: usize) !void {
        return self.addPrefix(&id, value, previous_length);
    }
    pub fn addPrefix(self: *Mutations, prefix: []const u8, value: ?[]const u8, previous_length: usize) !void {
        const length = if (value) |bytes| bytes.len else previous_length;
        if (length == 0 or length > max_blob_bytes) return error.InvalidNativeLakeTextCorpus;
        var offset: usize = 0;
        while (offset < length) : (offset += chunk_bytes) {
            var buffer: [68]u8 = undefined;
            if (prefix.len > 64) return error.InvalidNativeLakeTextCorpus;
            @memcpy(buffer[0..prefix.len], prefix);
            std.mem.writeInt(u32, buffer[prefix.len..][0..4], @intCast(offset), .big);
            try self.addRecord(buffer[0 .. prefix.len + 4], if (value) |bytes| bytes[offset..@min(length, offset + chunk_bytes)] else null);
        }
    }
    /// The external sort admits batches by bytes and memory pressure. Its
    /// bounded fan-in and reusable extents also enforce disk and file quotas.
    pub fn addRecord(self: *Mutations, key: []const u8, value: ?[]const u8) !void {
        if (key.len == 0 or key.len > tree.max_key_bytes or (if (value) |bytes| bytes.len > tree.max_record_bytes else false)) return error.InvalidGraphPage;
        try self.sort.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = key })}, .values = &.{local.sql_scalar.Datum.json(if (value) |bytes| .{ .string = bytes } else .null)}, .ordinal = self.ordinal });
        self.ordinal += 1;
    }
    /// Private spill runs are not publication checkpoints. Only the completed
    /// tree escapes; destination authentication, reductions and COW are shared
    /// with durable metadata updates. No intermediate page tree is constructed.
    pub fn finish(self: *Mutations, prior: ?tree.Ref) !?tree.Ref {
        var storage = self.destination;
        if (self.reduction) |reduction| storage.reduce = reduction;
        var source: Source = .{ .sort = &self.sort, .addition_first = self.addition_first };
        defer source.deinit();
        return tree.applySorted(self.a, storage, prior, &source);
    }

    const Lease = local.sql_spill.Sort.RowLease;
    const Source = struct {
        sort: *local.sql_spill.Sort,
        addition_first: bool,
        held: ?Lease = null,
        pending: ?Lease = null,
        fn deinit(self: *Source) void {
            if (self.held) |lease| lease.release();
            if (self.pending) |lease| lease.release();
        }
        fn value(lease: Lease) !?[]const u8 {
            const cell = try lease.cell(0);
            return if (cell.sql_null or cell.value == .null) null else cell.value.string;
        }
        pub fn next(self: *Source) !?tree.Mutation {
            if (self.held) |lease| lease.release();
            self.held = null;
            self.held = self.pending orelse (try self.sort.nextLeased()) orelse return null;
            self.pending = null;
            while (try self.sort.nextLeased()) |candidate| {
                // Own each lease immediately, including on malformed input or
                // allocation failure. At most the winner and lookahead survive.
                self.pending = candidate;
                const key = (try self.held.?.keyCell(0)).value.string;
                if (!std.mem.eql(u8, key, (try candidate.keyCell(0)).value.string)) break;
                if (!self.addition_first or try value(self.held.?) == null or try value(candidate) != null) {
                    self.held.?.release();
                    self.held = candidate;
                } else candidate.release();
                self.pending = null;
            }
            return .{ .key = (try self.held.?.keyCell(0)).value.string, .value = try value(self.held.?) };
        }
    };
};

test "external lake metadata reducer preserves latest and addition priority across spills and prior roots" {
    const a = std.testing.allocator;
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    var context: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = Check.call, .max_bytes = 8 * 1024 * 1024, .async_writes = false };
    defer manager.deinit();
    var memory = tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    var base = Mutations.init(a, &manager, memory.store());
    defer base.deinit();
    const count = 101;
    for (0..count) |i| {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, i, .big);
        try base.addRecord(&key, "prior");
    }
    const prior = (try base.finish(null)).?;
    for ([_]bool{ false, true }) |addition_first| {
        {
            var changes = Mutations.init(a, &manager, memory.store());
            defer changes.deinit();
            changes.sort.memory_bytes = 8 * 1024;
            changes.addition_first = addition_first;
            var seen: [count]bool = @splat(false);
            var expected: [count]?u64 = @splat(null);
            for (0..512) |i| {
                const index = (i * 17) % (count - 1);
                var key: [8]u8 = undefined;
                std.mem.writeInt(u64, &key, index, .big);
                var bytes: [128]u8 = @splat(42);
                std.mem.writeInt(u64, bytes[0..8], i, .big);
                const deleting = i % 7 == 0;
                try changes.addRecord(&key, if (deleting) null else &bytes);
                seen[index] = true;
                if (!addition_first or expected[index] == null or !deleting) expected[index] = if (deleting) null else i;
            }
            try std.testing.expect(manager.files > 0);
            const root = try changes.finish(prior);
            var cursor = try tree.Cursor.init(a, memory.store(), root, "", null);
            defer cursor.deinit();
            for (0..count) |i| {
                if (seen[i] and expected[i] == null) continue;
                const record = (try cursor.next()).?;
                try std.testing.expectEqual(i, std.mem.readInt(u64, record.key[0..8], .big));
                if (seen[i]) {
                    try std.testing.expectEqual(@as(usize, 128), record.value.len);
                    try std.testing.expectEqual(expected[i].?, std.mem.readInt(u64, record.value[0..8], .big));
                    for (record.value[8..]) |byte| try std.testing.expectEqual(@as(u8, 42), byte);
                } else try std.testing.expectEqualStrings("prior", record.value);
            }
            try std.testing.expect(try cursor.next() == null);
        }
        try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
        try std.testing.expectEqual(@as(usize, 0), manager.files);
    }
    var unchanged = try tree.Cursor.init(a, memory.store(), prior, "", null);
    defer unchanged.deinit();
    for (0..count) |_| try std.testing.expectEqualStrings("prior", (try unchanged.next()).?.value);
    try std.testing.expect(try unchanged.next() == null);
}

test "external lake metadata reducer releases leases on publication failure cancellation and spill quota" {
    const a = std.testing.allocator;
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    var context: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = Check.call, .max_bytes = 8 * 1024 * 1024, .async_writes = false };
    defer manager.deinit();
    var memory = tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    for (0..3) |failure| {
        {
            var changes = Mutations.init(a, &manager, memory.store());
            defer changes.deinit();
            changes.sort.memory_bytes = 8 * 1024;
            for (0..256) |i| {
                var key: [8]u8 = undefined;
                std.mem.writeInt(u64, &key, i, .big);
                const bytes: [128]u8 = @splat(42);
                try changes.addRecord(&key, &bytes);
            }
            memory.fail_put = if (failure < 2) memory.writes + failure else null;
            memory.canceled = failure == 2;
            if (failure < 2) {
                try std.testing.expectError(error.InjectedUploadFailure, changes.finish(null));
            } else try std.testing.expectError(error.Canceled, changes.finish(null));
            memory.canceled = false;
            memory.fail_put = null;
        }
        try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
        try std.testing.expectEqual(@as(usize, 0), manager.files);
    }
    {
        var changes = Mutations.init(a, &manager, memory.store());
        defer changes.deinit();
        changes.sort.memory_bytes = 1024;
        manager.max_bytes = 1;
        const bytes: [4096]u8 = @splat(42);
        try std.testing.expectError(error.SqlProgramLimitExceeded, changes.addRecord("quota", &bytes));
    }
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
    try std.testing.expectEqual(@as(usize, 0), manager.files);
}
