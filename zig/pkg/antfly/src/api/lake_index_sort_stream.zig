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

//! Publication consumers borrow immutable spill blocks until the next record
//! or bounded window. Builders/encoders finish consuming bytes before advance;
//! no transient row or payload clone sits between sort and publication.
const std = @import("std");
const local = @import("antfly_local_sources");
const spill = local.sql_spill;
const A = std.mem.Allocator;
const RowLease = spill.Sort.RowLease;

pub const Records = struct {
    sort: *spill.Sort,
    held: ?RowLease = null,
    pub fn deinit(self: *Records) void {
        if (self.held) |lease| lease.release();
        self.held = null;
    }
    pub const Record = struct { key: []const u8, value: []const u8 };
    pub fn next(self: *Records) !?Record {
        self.deinit();
        const lease = (try self.sort.nextLeased()) orelse return null;
        self.held = lease;
        if (lease.keyWidth() != 1 or lease.width() != 1) return error.InvalidSqlSpill;
        const key = try lease.keyCell(0);
        const value = try lease.cell(0);
        if (key.sql_null or value.sql_null or key.value != .string or value.value != .string) return error.InvalidSqlSpill;
        return .{ .key = key.value.string, .value = value.value.string };
    }
};

pub const Window = struct {
    sort: *spill.Sort,
    // The descriptor view borrows page.rows; the page retires every lease.
    page: ?spill.Sort.Page = null,
    rows: []const RowLease = &.{},
    count: usize = 0,
    width: usize = 0,
    key_width: usize = 0,
    pub fn clear(self: *Window) void {
        if (self.page) |page| page.release();
        self.page = null;
        self.rows = &.{};
        self.count = 0;
    }
    pub fn deinit(self: *Window) void {
        self.clear();
    }
    /// Single-pass publication may return a shorter physical batch under
    /// pressure. The sort retains unread references; callers drain through EOF.
    pub fn read(self: *Window, maximum: usize, bytes: usize) !usize {
        if (maximum == 0 or maximum > 256 or bytes == 0) return error.InvalidSqlLimit;
        self.clear();
        errdefer self.clear();
        self.page = try self.sort.nextPublicationPageLeased(maximum, bytes);
        const rows = self.page.?.rows;
        self.rows = rows;
        self.count = rows.len;
        if (rows.len != 0) {
            self.width = rows[0].width();
            self.key_width = rows[0].keyWidth();
            for (rows) |row| if (row.width() != self.width or row.keyWidth() != self.key_width) return error.InvalidSqlSpill;
        }
        return self.count;
    }
    /// Only vector/scatter descriptors are allocated. All physical/frozen
    /// payloads remain pinned by this window, including dictionaries and JSON.
    pub fn values(self: *Window, a: A) !local.sql_execution_batch.Batch {
        const view = try a.create(spill.Sort.LeaseView);
        view.* = try .init(a, self.rows, self.width);
        return view.batch();
    }
    pub fn ordinals(self: *const Window, a: A) ![]const u64 {
        const result = try a.alloc(u64, self.count);
        for (result, self.rows) |*out, lease| out.* = lease.row.ordinal;
        return result;
    }
};

test "external lake leased publication records preserve resident and spilled bytes and release allocation failures" {
    const Probe = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: A, memory: usize) !void {
            var marker: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            var sort = spill.Sort.init(a, &manager, &.{.{}}, memory);
            defer sort.deinit();
            sort.parallel_runs = false;
            for (0..8) |i| {
                var key: [32]u8 = undefined;
                const encoded = try std.fmt.bufPrint(&key, "key-{d:0>16}", .{7 - i});
                try sort.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = encoded })}, .values = &.{local.sql_scalar.Datum.json(.{ .string = "borrowed payload" })}, .ordinal = 7 - i });
            }
            var records: Records = .{ .sort = &sort };
            defer records.deinit();
            var seen: usize = 0;
            while (try records.next()) |record| : (seen += 1) {
                var key: [32]u8 = undefined;
                try std.testing.expectEqualStrings(try std.fmt.bufPrint(&key, "key-{d:0>16}", .{seen}), record.key);
                try std.testing.expectEqualStrings("borrowed payload", record.value);
            }
            try std.testing.expectEqual(@as(usize, 8), seen);
        }
    };
    try Probe.run(std.testing.allocator, 512);
    try Probe.run(std.testing.allocator, 1 << 20);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{@as(usize, 1 << 20)});
}

test "external lake leased publication windows bound quanta preserve EOF and unwind allocation faults" {
    const Probe = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: A, memory: usize) !void {
            var marker: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = check, .async_writes = false };
            defer manager.deinit();
            var sort = spill.Sort.init(a, &manager, &.{.{}}, memory);
            defer sort.deinit();
            sort.parallel_runs = false;
            for (0..4) |i| try sort.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .integer = @intCast(i) })}, .values = &.{local.sql_scalar.Datum.json(.{ .string = "payload" })}, .ordinal = i });
            var window: Window = .{ .sort = &sort };
            defer window.deinit();
            try std.testing.expectError(error.InvalidSqlLimit, window.read(0, 1));
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            var seen: usize = 0;
            while (try window.read(2, 1) != 0) {
                // A single indivisible row can exceed the raw-byte quantum.
                try std.testing.expectEqual(@as(usize, 1), window.count);
                const page = window.page.?.values;
                try std.testing.expectEqualStrings("payload", (try page.cell(scratch.allocator(), 0, 0)).value.string);
                _ = try page.frozenJson(0, 0);
                _ = try page.encodedJson(0, 0);
                _ = try page.dictionaryColumn(scratch.allocator(), 0);
                const batch = try window.values(scratch.allocator());
                try std.testing.expectEqualStrings("payload", (try batch.cell(scratch.allocator(), 0, 0)).value.string);
                try std.testing.expectEqual(@as(u64, seen), window.rows[0].row.ordinal);
                seen += 1;
                window.clear();
                _ = scratch.reset(.free_all);
            }
            try std.testing.expectEqual(@as(usize, 4), seen);
            try std.testing.expectEqual(@as(usize, 0), try window.read(2, 1));
        }
    };
    for ([_]usize{ 512, 1 << 20 }) |memory| {
        try Probe.run(std.testing.allocator, memory);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{memory});
    }
}

test "external lake leased publication benchmark sorted record drain" {
    if (std.c.getenv("ANTFLY_LAKE_LEASED_PUBLICATION_BENCH") == null) return;
    const Probe = struct {
        const Measurement = struct { ns: i96, allocated: usize, reads: u64, written: u64 };
        fn check(_: *anyopaque) !void {}
        fn now() i96 {
            var stamp: std.c.timespec = undefined;
            if (std.c.clock_gettime(.THREAD_CPUTIME_ID, &stamp) != 0) @panic("CPU clock unavailable");
            return @as(i96, stamp.sec) * std.time.ns_per_s + stamp.nsec;
        }
        fn run(leased: bool, memory: usize) !Measurement {
            var tracking = std.testing.FailingAllocator.init(std.heap.smp_allocator, .{});
            const a = tracking.allocator();
            var marker: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            var sort = spill.Sort.init(a, &manager, &.{.{}}, memory);
            defer sort.deinit();
            sort.parallel_runs = false;
            var payload: [2048]u8 = @splat('p');
            for (0..1024) |i| {
                var key: [32]u8 = undefined;
                const encoded = try std.fmt.bufPrint(&key, "key-{d:0>16}", .{1023 - i});
                std.mem.writeInt(u64, payload[0..8], @intCast(1023 - i), .little);
                try sort.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = encoded })}, .values = &.{local.sql_scalar.Datum.json(.{ .string = &payload })}, .ordinal = 1023 - i });
            }
            var records: Records = .{ .sort = &sort };
            defer records.deinit();
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const allocated = tracking.allocated_bytes;
            const start = now();
            var seen: usize = 0;
            while (true) {
                _ = scratch.reset(.free_all);
                const record: Records.Record = if (leased) (try records.next()) orelse break else blk: {
                    const row = (try sort.next(scratch.allocator())) orelse break;
                    break :blk .{ .key = row.keys[0].value.string, .value = row.values[0].value.string };
                };
                var key: [32]u8 = undefined;
                try std.testing.expectEqualStrings(try std.fmt.bufPrint(&key, "key-{d:0>16}", .{seen}), record.key);
                std.mem.writeInt(u64, payload[0..8], @intCast(seen), .little);
                try std.testing.expectEqualSlices(u8, &payload, record.value);
                seen += 1;
            }
            const ns = now() - start;
            try std.testing.expectEqual(@as(usize, 1024), seen);
            return .{ .ns = ns, .allocated = tracking.allocated_bytes - allocated, .reads = manager.read_bytes, .written = manager.written_bytes };
        }
    };
    for ([_]usize{ 8 << 20, 64 << 10 }) |memory| {
        _ = try Probe.run(false, memory);
        _ = try Probe.run(true, memory);
        for (0..6) |sample| {
            const first = try Probe.run(sample % 2 != 0, memory);
            const second = try Probe.run(sample % 2 == 0, memory);
            const before = if (sample % 2 == 0) first else second;
            const after = if (sample % 2 == 0) second else first;
            std.debug.print("LEASED_PUBLICATION sample={d} memory={d} before_cpu_ns={d} after_cpu_ns={d} before_allocated={d} after_allocated={d} before_read={d} after_read={d} before_write={d} after_write={d}\n", .{ sample, memory, before.ns, after.ns, before.allocated, after.allocated, before.reads, after.reads, before.written, after.written });
        }
    }
}

test "external lake leased publication pressure preserves every pending key and value" {
    const Probe = struct {
        fn check(_: *anyopaque) !void {}
        fn run(mode: u8) !void {
            var budget: local.sql_memory_budget = .{ .backing = std.heap.smp_allocator, .limit = 32 << 20 };
            const a = budget.allocator();
            var marker: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            var sort = spill.Sort.init(a, &manager, &.{.{}}, 4096);
            defer sort.deinit();
            sort.encoded_bytes = true;
            sort.shared_runs = true;
            sort.parallel_runs = false;
            const count = 8192;
            var bodies: [64][8][128]u8 = @splat(@splat(@splat('v')));
            var keys: [64][20]u8 = undefined;
            var cells: [64][8]local.sql_scalar.Datum = undefined;
            var key_cells: [64][1]local.sql_scalar.Datum = undefined;
            var rows: [64][]const local.sql_scalar.Datum = undefined;
            var key_rows: [64][]const local.sql_scalar.Datum = undefined;
            var ordinals: [64]u64 = undefined;
            for (0..count / 64) |batch| {
                for (0..64) |i| {
                    const ordinal = batch * 64 + i;
                    const rank = i * (count / 64) + batch;
                    for (0..8) |column| {
                        std.mem.writeInt(u64, bodies[i][column][0..8], ordinal * 8 + column, .little);
                        cells[i][column] = local.sql_scalar.Datum.json(.{ .string = &bodies[i][column] });
                    }
                    const key = try std.fmt.bufPrint(&keys[i], "key-{d:0>16}", .{rank});

                    key_cells[i][0] = local.sql_scalar.Datum.json(.{ .string = key });
                    rows[i] = &cells[i];
                    key_rows[i] = &key_cells[i];
                    ordinals[i] = ordinal;
                }
                try sort.addBatch(.{ .rows = &rows }, &key_rows, &ordinals);
            }
            try sort.finish();
            budget.limit = budget.live + (1 << 20);
            std.debug.print("PIN_ADMISSION mode={} live={} limit={}\n", .{ mode, budget.live, budget.limit });
            var seen: usize = 0;
            if (mode == 1) {
                var window: Window = .{ .sort = &sort };
                defer window.deinit();
                var shortened = false;
                while (true) {
                    const n = window.read(256, 512 * 1024) catch |err| {
                        std.debug.print("PIN_WINDOW leased={} seen={} partial={} error={} page_limit={}\n", .{ mode, seen, window.count, err, manager.page_residency_bytes });
                        return err;
                    };
                    if (n == 0) break;
                    shortened = shortened or n < @min(256, count - seen);
                    for (window.rows) |lease| {
                        var expected: [20]u8 = undefined;
                        try std.testing.expectEqualStrings(try std.fmt.bufPrint(&expected, "key-{d:0>16}", .{seen}), (try lease.keyCell(0)).value.string);
                        const ordinal = (seen % (count / 64)) * 64 + seen / (count / 64);
                        for (0..8) |column| {
                            const bytes = (try lease.cell(column)).value.string;
                            try std.testing.expectEqual(@as(u64, ordinal * 8 + column), std.mem.readInt(u64, bytes[0..8], .little));
                        }
                        seen += 1;
                    }
                }
                try std.testing.expect(shortened);
            } else if (mode == 2) {
                while (true) {
                    const page = try sort.nextPageLeased(256, 512 * 1024);
                    defer page.release();
                    if (page.rows.len == 0) break;
                    for (page.rows) |lease| {
                        var expected: [20]u8 = undefined;
                        try std.testing.expectEqualStrings(try std.fmt.bufPrint(&expected, "key-{d:0>16}", .{seen}), (try lease.keyCell(0)).value.string);
                        const ordinal = (seen % (count / 64)) * 64 + seen / (count / 64);
                        for (0..8) |column| {
                            const bytes = (try lease.cell(column)).value.string;
                            try std.testing.expectEqual(@as(u64, ordinal * 8 + column), std.mem.readInt(u64, bytes[0..8], .little));
                        }
                        seen += 1;
                    }
                }
            } else {
                var scratch = std.heap.ArenaAllocator.init(a);
                defer scratch.deinit();
                while (true) {
                    _ = scratch.reset(.free_all);
                    const row = (try sort.next(scratch.allocator())) orelse break;
                    try std.testing.expectEqual(@as(usize, 128), row.values[0].value.string.len);
                    seen += 1;
                }
            }
            std.debug.print("PIN_WINDOW leased={} seen={} page_limit={} reads={}\n", .{ mode, seen, manager.page_residency_bytes, sort.payload_reads });
            try std.testing.expectEqual(@as(usize, count), seen);
        }
    };
    // Owning control, existing grouped delivery, and publication all complete
    // under identical headroom. The publication path must not lose a hydrated
    // prefix or an unread key when physical admission shortens a batch.
    try Probe.run(0);
    try Probe.run(2);
    try Probe.run(1);
}

test "external lake leased publication wide covering ranges coalesce before decoding" {
    const a = std.testing.allocator;
    const columns = @import("lake_index_cover_columns.zig");
    const stores = @import("../serverless/artifacts/store.zig");
    var dir = try local.common_test_directory.TestDirectory.init("review-cover-fanout");
    defer dir.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, dir.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const scope: stores.UploadScope = .{ .domain = @splat(7), .attempt = @splat(3) };
    store.upload_scope = scope;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const pa = arena.allocator();
    var rows: [32]local.sql_operators.Row = undefined;
    for (&rows, 0..) |*row, i| {
        const key = try pa.alloc(local.sql_scalar.Datum, 1);
        key[0] = local.sql_scalar.Datum.json(.{ .string = try std.fmt.allocPrint(pa, "key-{d:0>16}", .{i}) });
        const cells = try pa.alloc(local.sql_scalar.Datum, 128);
        for (cells, 0..) |*cell, col| cell.* = local.sql_scalar.Datum.json(.{ .integer = @intCast(i * 128 + col) });
        row.* = .{ .values = cells, .keys = key, .ordinal = i };
    }
    var writes: u64 = 0;
    const ref = try columns.publish(pa, store, scope, &rows, .none, &writes);
    const directory = try columns.readDirectory(pa, store, ref, .none);
    const Probe = struct {
        var active: *@This() = undefined;
        original: *const stores.ArtifactStore.VTable,
        pack: []const u8,
        calls: usize = 0,
        bytes: u64 = 0,
        fn get(raw: *anyopaque, alloc: A, id: []const u8, offset: u64, length: usize, cancel: @import("antfly_cancellation").CancellationToken) ![]u8 {
            const self = active;
            if (std.mem.eql(u8, id, self.pack)) {
                self.calls += 1;
                self.bytes += length;
            }
            return if (self.original.get_range_alloc_with_cancellation) |read| read(raw, alloc, id, offset, length, cancel) else self.original.get_range_alloc(raw, alloc, id, offset, length);
        }
    };
    var probe: Probe = .{ .original = store.vtable, .pack = directory.pack.artifact_id };
    Probe.active = &probe;
    var traced = store.vtable.*;
    traced.get_range_alloc_with_cancellation = Probe.get;
    store.vtable = &traced;
    var selected: [128]usize = undefined;
    for (&selected, 0..) |*index, i| index.* = i;
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &cache, .scope = @splat(7), .context = .{ .io = std.testing.io } };
    // Match the serving covering wave's byte-derived admission, rather than
    // relying on the loader's maximum 16 MiB allowance.
    const allowance = @min(16 << 20, (directory.pack.byte_len + ref.byte_len) * 4 + (256 << 10));
    var reads: u64 = 16 << 20;
    var loaded = try columns.load(a, store, ref, true, &selected, 128, .none, cached, allowance, &reads, null);
    defer loaded.deinit();
    for (0..32) |row| for (0..128) |col| try std.testing.expectEqual(@as(i64, @intCast(row * 128 + col)), (try loaded.block.cell(row, col)).value.integer);
    std.debug.print("WIDE_COVER columns=128 rows=32 stripes={} pack_bytes={} range_gets={} fetched={}\n", .{ directory.sections.len, directory.pack.byte_len, probe.calls, probe.bytes });
    const cold_calls = probe.calls;
    var warm = try columns.load(a, store, ref, true, &selected, 128, .none, cached, allowance, &reads, null);
    defer warm.deinit();
    std.debug.print("WIDE_COVER_WARM range_gets={}\n", .{probe.calls - cold_calls});
    try std.testing.expectEqual(@as(usize, 1), cold_calls);
    try std.testing.expectEqual(@as(usize, 0), probe.calls - cold_calls);
    // A fresh scope with a sparse projection reads the identity and two
    // required stripes, without bringing omitted columns into a coalesced GET.
    var narrow_cache = cached;
    narrow_cache.scope = @splat(8);
    const before_calls = probe.calls;
    const before_bytes = probe.bytes;
    var narrow = try columns.load(a, store, ref, true, &.{ 0, 127 }, 128, .none, narrow_cache, allowance, &reads, null);
    defer narrow.deinit();
    try std.testing.expectEqual(@as(usize, 2), probe.calls - before_calls);
    const required = directory.sections[0].bytes + directory.sections[1].bytes + directory.sections[128].bytes;
    try std.testing.expectEqual(@as(u64, required), probe.bytes - before_bytes);
    try std.testing.expect((try narrow.block.cell(0, 64)).sql_null);
    try std.testing.expectEqual(@as(i64, 127), (try narrow.block.cell(0, 127)).value.integer);
    const uncached_calls = probe.calls;
    var uncached = try columns.load(a, store, ref, true, &selected, 128, .none, null, allowance, &reads, null);
    defer uncached.deinit();
    try std.testing.expectEqual(@as(usize, 1), probe.calls - uncached_calls);
    for (0..32) |row| for (0..128) |col| try std.testing.expectEqual(@as(i64, @intCast(row * 128 + col)), (try uncached.block.cell(row, col)).value.integer);
}

test "external lake leased publication adaptive spill cache reduces wide drain reads within unchanged admission" {
    const Probe = struct {
        fn check(_: *anyopaque) !void {}
        const Measurement = struct { read: u64, written: u64, drain: u64, payload_reads: usize, cpu: i96 };
        fn now() i96 {
            var stamp: std.c.timespec = undefined;
            if (std.c.clock_gettime(.THREAD_CPUTIME_ID, &stamp) != 0) @panic("CPU clock unavailable");
            return @as(i96, stamp.sec) * std.time.ns_per_s + stamp.nsec;
        }
        fn run(compressed: bool, fixed: bool, publication: bool) !Measurement {
            return runSized(compressed, fixed, publication, 8192, 16 << 20);
        }
        fn runSized(compressed: bool, fixed: bool, publication: bool, count: usize, cache_bytes: usize) !Measurement {
            var resources = local.storage_resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 << 20 } });
            defer resources.deinit(std.heap.smp_allocator);
            const memory = try @import("sql_memory_owner.zig").create(std.heap.smp_allocator, std.heap.smp_allocator, &resources);
            defer memory.release(memory.ptr);
            var budget: local.sql_memory_budget = .{ .backing = memory.allocator, .limit = 32 << 20, .pressure = memory.pressure };
            var working = try @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.init(if (publication) std.heap.smp_allocator else memory.allocator, .{ .max_working_set_bytes = 64 << 20 });
            defer working.deinit();
            var allocations: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = working.allocator() };
            const a = if (publication) allocations.allocator() else budget.allocator();
            var marker: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = check, .compression = if (compressed) .snappy else .none, .async_writes = false };
            defer manager.deinit();
            var sort = spill.Sort.init(a, &manager, &.{.{}}, 4096);
            defer sort.deinit();
            sort.encoded_bytes = true;
            sort.shared_runs = true;
            sort.parallel_runs = false;
            var bodies: [64][8][128]u8 = @splat(@splat(@splat('v')));
            var keys: [64][20]u8 = undefined;
            var cells: [64][8]local.sql_scalar.Datum = undefined;
            var key_cells: [64][1]local.sql_scalar.Datum = undefined;
            var rows: [64][]const local.sql_scalar.Datum = undefined;
            var key_rows: [64][]const local.sql_scalar.Datum = undefined;
            var ordinals: [64]u64 = undefined;
            for (0..count / 64) |batch| {
                for (0..64) |i| {
                    const ordinal = batch * 64 + i;
                    const rank = i * (count / 64) + batch;
                    for (0..8) |column| {
                        std.mem.writeInt(u64, bodies[i][column][0..8], ordinal * 8 + column, .little);
                        cells[i][column] = local.sql_scalar.Datum.json(.{ .string = &bodies[i][column] });
                    }
                    const key = try std.fmt.bufPrint(&keys[i], "key-{d:0>16}", .{rank});

                    key_cells[i][0] = local.sql_scalar.Datum.json(.{ .string = key });
                    rows[i] = &cells[i];
                    key_rows[i] = &key_cells[i];
                    ordinals[i] = ordinal;
                }
                try sort.addBatch(.{ .rows = &rows }, &key_rows, &ordinals);
            }
            try sort.finish();
            const reader = try sort.payloadReader();
            reader.cache.max_bytes = cache_bytes;
            if (fixed) {
                reader.cache.max_bytes = 64 * 1024;
                reader.cache.max_entries = 4;
            }
            const reads_before_delivery = manager.read_bytes;
            budget.limit = budget.live + (32 << 20);
            working.quota.max_live_bytes = working.quota.live_bytes + (32 << 20);
            const started = now();
            var seen: usize = 0;
            var window: Window = .{ .sort = &sort };
            defer window.deinit();
            while (true) {
                const n = try window.read(256, 512 * 1024);
                if (n == 0) break;
                for (window.rows) |lease| {
                    var expected: [20]u8 = undefined;
                    try std.testing.expectEqualStrings(try std.fmt.bufPrint(&expected, "key-{d:0>16}", .{seen}), (try lease.keyCell(0)).value.string);
                    const ordinal = (seen % (count / 64)) * 64 + seen / (count / 64);
                    for (0..8) |column| {
                        const bytes = (try lease.cell(column)).value.string;
                        try std.testing.expectEqual(@as(u64, ordinal * 8 + column), std.mem.readInt(u64, bytes[0..8], .little));
                    }
                    seen += 1;
                }
            }
            const cpu = now() - started;
            std.debug.print("ADAPTIVE_SPILL publication={} codec={} fixed={} rows={} payload_reads={} drain_read_bytes={} total_read_bytes={} written_bytes={} cpu_ns={} budget_peak={} budget_limit={} rejected={}\n", .{ publication, manager.compression, fixed, seen, sort.payload_reads, manager.read_bytes - reads_before_delivery, manager.read_bytes, manager.written_bytes, cpu, if (publication) working.quota.peak_bytes else budget.peak, if (publication) working.maxBytes() else budget.limit, reader.cache.admission_rejections });
            try std.testing.expectEqual(@as(usize, count), seen);
            try std.testing.expect(budget.peak <= budget.limit);
            try std.testing.expect(working.quota.peak_bytes <= working.maxBytes());
            return .{ .read = manager.read_bytes, .written = manager.written_bytes, .drain = manager.read_bytes - reads_before_delivery, .payload_reads = sort.payload_reads, .cpu = cpu };
        }
    };
    for ([_]bool{ false, true }) |publication| for ([_]bool{ false, true }) |compressed| {
        const before = try Probe.run(compressed, true, publication);
        const after = try Probe.run(compressed, false, publication);
        try std.testing.expectEqual(before.written, after.written);
        try std.testing.expect(after.drain < before.drain / 2);
        try std.testing.expect(after.payload_reads < before.payload_reads / 2);
    };
    // The active payload set exceeds this cache. A pure LRU misses every
    // cycle; bounded frequency admission must preserve some reusable roots.
    const scan_before = try Probe.runSized(true, true, true, 2048, 512 << 10);
    const scan_after = try Probe.runSized(true, false, true, 2048, 512 << 10);
    try std.testing.expectEqual(scan_before.written, scan_after.written);
    try std.testing.expect(scan_after.payload_reads < scan_before.payload_reads * 3 / 4);
    try std.testing.expect(scan_after.drain < scan_before.drain * 3 / 4);
    if (std.c.getenv("ANTFLY_LAKE_SCAN_CACHE_BENCH") != null) {
        for ([_]usize{ 32768, 65536 }) |count| for (0..6) |sample| {
            std.debug.print("SCAN_CACHE_SAMPLE rows={} sample={}\n", .{ count, sample });
            _ = try Probe.runSized(true, sample % 2 == 0, true, count, 16 << 20);
            _ = try Probe.runSized(true, sample % 2 != 0, true, count, 16 << 20);
        };
    }
    if (std.c.getenv("ANTFLY_LAKE_PUBLICATION_CACHE_BENCH") != null) {
        for (0..6) |sample| {
            std.debug.print("PUBLICATION_SAMPLE sample={}\n", .{sample});
            _ = try Probe.run(true, sample % 2 == 0, true);
            _ = try Probe.run(true, sample % 2 != 0, true);
        }
    }
    if (std.c.getenv("ANTFLY_LAKE_ADAPTIVE_CACHE_BENCH") != null) {
        for (0..6) |sample| {
            std.debug.print("ADAPTIVE_SAMPLE sample={}\n", .{sample});
            _ = try Probe.run(true, sample % 2 == 0, false);
            _ = try Probe.run(true, sample % 2 != 0, false);
        }
    }
}
