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

//! Query-owned delete authority. Selected views share one fresh qualification;
//! no provider clients, callbacks or version authority enter resident caches.
const std = @import("std");
const iceberg = @import("lake_iceberg_snapshot.zig");
const external = @import("../external_source/types.zig");
const storage = @import("../../storage/object_storage.zig");
const Context = @import("lake_read_context.zig").Context;
const A = std.mem.Allocator;
const Hash = std.crypto.hash.sha2.Sha256;
pub const Qualification = struct {
    a: A,
    budget: @import("../../sql/memory_budget.zig"),
    arena: std.heap.ArenaAllocator = undefined,
    mutex: std.atomic.Mutex = .unlocked,
    building: bool = false,
    disabled: bool = false,
    data: ?Data = null,
    /// Query-owned immutable selectors authenticated by the adopted publication.
    /// Its owner retains the snapshot and reader leases until all views close.
    pub const Proof = struct {
        ptr: *anyopaque,
        count: usize,
        recipe: [32]u8,
        digest: [32]u8,
        source: [32]u8,
        data: [32]u8,
        lookup: *const fn (*anyopaque, A, usize, iceberg.IcebergDeleteFile, Context) anyerror!Version,
    };
    pub const Version = struct { etag: []const u8, version: []const u8, sequence: i64, position: bool };
    pub const Data = struct {
        versions: []const Version,
        digest: [32]u8,
        recipe: [32]u8,
        cacheable: bool = true,
        proof: ?Proof = null,
        scopes: std.AutoHashMapUnmanaged([32]u8, std.ArrayList(usize)) = .empty,
        global: std.ArrayList(usize) = .empty,
        unknown: std.ArrayList(usize) = .empty,
        pub fn evidence(self: *const Data, a: A, index: usize, file: iceberg.IcebergDeleteFile, context: Context) !Version {
            try context.ensureActive();
            if (self.proof) |proof| return proof.lookup(proof.ptr, a, index, file, context);
            return self.versions[index];
        }
        fn less(self: *const Data, left: usize, right: usize) bool {
            if (self.versions[left].sequence != self.versions[right].sequence) return self.versions[left].sequence < self.versions[right].sequence;
            return left < right;
        }
        fn appendCandidates(self: *const Data, a: A, selected: *std.AutoHashMapUnmanaged(usize, void), indices: []const usize, sequence: i64) !void {
            var lo: usize = 0;
            var hi = indices.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (self.versions[indices[mid]].sequence < sequence) lo = mid + 1 else hi = mid;
            }
            for (indices[lo..]) |index| {
                const version = self.versions[index];
                if (version.position or version.sequence > sequence) try selected.put(a, index, {});
            }
        }
        /// null retains the authoritative all-file path for incomplete scopes.
        pub fn candidates(self: *const Data, a: A, files: []const external.FileEntry) !?[]usize {
            var selected: std.AutoHashMapUnmanaged(usize, void) = .empty;
            defer selected.deinit(a);
            // Every list is a suffix by sequence. Its union over a partition
            // is the suffix starting at that partition's minimum sequence.
            var scopes: std.AutoHashMapUnmanaged([32]u8, i64) = .empty;
            defer scopes.deinit(a);
            var minimum: i64 = std.math.maxInt(i64);
            for (files) |file| {
                const sequence = file.data_sequence_number orelse return null;
                const scope = scopeKey(file.partition_spec_id orelse return null, file.partition_field_count, file.partition_values) orelse return null;
                minimum = @min(minimum, sequence);
                const entry = try scopes.getOrPut(a, scope);
                entry.value_ptr.* = if (entry.found_existing) @min(entry.value_ptr.*, sequence) else sequence;
            }
            if (files.len != 0) {
                try self.appendCandidates(a, &selected, self.global.items, minimum);
                try self.appendCandidates(a, &selected, self.unknown.items, minimum);
            }
            var groups = scopes.iterator();
            while (groups.next()) |entry| if (self.scopes.get(entry.key_ptr.*)) |indices|
                try self.appendCandidates(a, &selected, indices.items, entry.value_ptr.*);
            const result = try a.alloc(usize, selected.count());
            var it = selected.keyIterator();
            for (result) |*index| index.* = it.next().?.*;
            std.mem.sort(usize, result, {}, std.sort.asc(usize));
            return result;
        }
    };
    pub fn create(a: A) !*Qualification {
        const self = try a.create(Qualification);
        self.* = .{ .a = a, .budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.page_allocator), .limit = 16 * 1024 * 1024 } };
        self.arena = .init(self.budget.allocator());
        return self;
    }
    pub fn destroy(self: *Qualification) void {
        std.debug.assert(!self.building);
        self.arena.deinit();
        self.a.destroy(self);
    }
    fn lock(self: *Qualification) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
    }
    pub fn acquire(self: *Qualification, client: storage.ObjectStorage, plan: iceberg.IcebergDeletePlan, context: Context) !?*const Data {
        return self.acquireProven(client, plan, context, null);
    }
    pub fn acquireProven(self: *Qualification, client: storage.ObjectStorage, plan: iceberg.IcebergDeletePlan, context: Context, proof: ?Proof) !?*const Data {
        while (true) {
            try context.ensureActive();
            self.lock();
            if (self.data != null or self.disabled) {
                self.mutex.unlock();
                return if (self.data) |*data| data else null;
            }
            if (self.building) {
                self.mutex.unlock();
                if (context.io) |io| {
                    try io.sleep(.fromMilliseconds(1), .awake);
                    continue;
                }
                return null;
            }
            self.building = true;
            self.mutex.unlock();
            const data = (if (proof) |verified| self.buildProven(plan, context, verified) else self.build(client, plan, context)) catch |err| {
                const exhausted = err == error.OutOfMemory and self.budget.isExhausted();
                _ = self.arena.reset(.free_all);
                self.lock();
                self.building = false;
                self.disabled = exhausted;
                self.mutex.unlock();
                try context.ensureActive();
                if (exhausted) return null;
                return err;
            };
            self.lock();
            self.data = data;
            self.building = false;
            self.mutex.unlock();
            return &self.data.?;
        }
    }
    fn buildProven(self: *Qualification, plan: iceberg.IcebergDeletePlan, context: Context, proof: Proof) !Data {
        try context.ensureActive();
        if (proof.count != plan.files.len) return error.InvalidIcebergDeleteManifest;
        const a = self.arena.allocator();
        var data: Data = .{ .versions = try a.alloc(Version, plan.files.len), .digest = proof.digest, .recipe = undefined, .proof = proof };
        var buffer: [4096]u8 = undefined;
        var writer: std.Io.Writer.Hashing(Hash) = .init(&buffer);
        try std.json.Stringify.value(plan, .{}, &writer.writer);
        try writer.writer.flush();
        writer.hasher.final(&data.recipe);
        if (!std.mem.eql(u8, &data.recipe, &proof.recipe)) return error.InvalidIcebergDeleteManifest;
        for (plan.files, @constCast(data.versions)) |file, *version| {
            try context.ensureActive();
            try file.validate();
            version.* = .{ .etag = "", .version = "", .sequence = file.data_sequence_number, .position = file.content == .position_deletes };
        }
        try self.route(&data, plan);
        return data;
    }
    fn route(self: *Qualification, data: *Data, plan: iceberg.IcebergDeletePlan) !void {
        const a = self.arena.allocator();
        for (plan.files, 0..) |file, index| {
            if (file.content == .equality_deletes and file.partition_field_count == 0) {
                try data.global.append(a, index);
            } else if (scopeKey(file.partition_spec_id, file.partition_field_count, file.partition_values)) |scope| {
                const entry = try data.scopes.getOrPut(a, scope);
                if (!entry.found_existing) entry.value_ptr.* = .empty;
                try entry.value_ptr.append(a, index);
            } else try data.unknown.append(a, index);
        }
        var scopes = data.scopes.valueIterator();
        while (scopes.next()) |indices| std.mem.sort(usize, indices.items, data, Data.less);
        std.mem.sort(usize, data.global.items, data, Data.less);
        std.mem.sort(usize, data.unknown.items, data, Data.less);
    }
    fn build(self: *Qualification, client: storage.ObjectStorage, plan: iceberg.IcebergDeletePlan, context: Context) !Data {
        const a = self.arena.allocator();
        var data: Data = .{ .versions = try a.alloc(Version, plan.files.len), .digest = undefined, .recipe = undefined };
        var buffer: [4096]u8 = undefined;
        var recipe_writer: std.Io.Writer.Hashing(Hash) = .init(&buffer);
        try std.json.Stringify.value(plan, .{}, &recipe_writer.writer);
        try recipe_writer.writer.flush();
        recipe_writer.hasher.final(&data.recipe);
        const versions = @constCast(data.versions);
        var hash = Hash.init(.{});
        const Work = struct {
            fn stat(base: storage.ObjectStorage, uri: []const u8, ctx: Context) anyerror!storage.ObjectMetadata {
                try ctx.ensureActive();
                const location = try @import("lake_range_io.zig").objectLocationForUri(uri);
                var worker = base;
                worker.allocator = std.heap.page_allocator;
                return worker.statObject(location.bucket, location.key);
            }
        };
        const scheduler = @import("../../sql/parallel_scheduler.zig");
        var first: usize = 0;
        while (first < plan.files.len) {
            const count = @min(8, plan.files.len - first);
            var tasks: [8]?scheduler.Task(anyerror!storage.ObjectMetadata) = @splat(null);
            var results: [8]?storage.ObjectMetadata = @splat(null);
            defer {
                for (&tasks, &results) |*task, *result| if (task.*) |*work| {
                    result.* = work.cancel(context.io.?) catch null;
                };
                for (&results) |*result| if (result.*) |*meta| meta.deinit(std.heap.page_allocator);
            }
            for (0..count) |slot| {
                const file = plan.files[first + slot];
                try file.validate();
                if (context.io) |io| tasks[slot] = scheduler.global().submitIoTransient(io, 64 * 1024, Work.stat, .{ client, file.file_path, context });
                if (tasks[slot] == null) results[slot] = try Work.stat(client, file.file_path, context);
            }
            for (0..count) |slot| if (tasks[slot]) |*work| {
                const result = work.await(context.io.?);
                tasks[slot] = null;
                results[slot] = try result;
            };
            for (0..count) |slot| {
                try context.ensureActive();
                const index = first + slot;
                const file = plan.files[index];
                const meta = results[slot].?;
                if (meta.content_length != file.file_size_in_bytes) return error.IcebergManifestLengthMismatch;
                versions[index] = .{ .etag = try a.dupe(u8, meta.etag orelse ""), .version = try a.dupe(u8, meta.version_id orelse ""), .sequence = file.data_sequence_number, .position = file.content == .position_deletes };
                @import("lake_prepared_deletes.zig").Prepared.hashObjectVersion(&hash, file.file_path, versions[index].etag, versions[index].version);
                if (versions[index].etag.len == 0 and (versions[index].version.len == 0 or std.mem.eql(u8, versions[index].version, "null") or std.mem.startsWith(u8, versions[index].version, "object-stat:") or std.mem.startsWith(u8, versions[index].version, "iceberg:"))) data.cacheable = false;
            }
            first += count;
        }
        try self.route(&data, plan);
        data.digest = hash.finalResult();
        return data;
    }
};
// Partition field order is not semantic. Hash complete name/value pairs with
// lengths, then combine commutatively before binding the spec and field count.
fn scopeKey(spec: i32, count: u32, values: []const external.PartitionValue) ?[32]u8 {
    if (values.len != count) return null;
    var folded: [32]u8 = @splat(0);
    for (values) |value| {
        var item = Hash.init(.{});
        @import("lake_prepared_deletes.zig").Prepared.hashObjectVersion(&item, value.column_id, value.string_value, "");
        const digest = item.finalResult();
        for (&folded, digest) |*out, byte| out.* ^= byte;
    }
    var hash = Hash.init(.{});
    var header: [8]u8 = undefined;
    std.mem.writeInt(i32, header[0..4], spec, .big);
    std.mem.writeInt(u32, header[4..8], count, .big);
    hash.update(&header);
    hash.update(&folded);
    return hash.finalResult();
}

test "external lake metadata navigation delete qualification routes partitions and revalidates each source" {
    const a = std.testing.allocator;
    var memory = storage.MemoryObjectStorage.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    for ([_][]const u8{ "d0", "d1" }) |key| {
        var put = try client.putObject("bucket", key, "first", .{});
        put.deinit(a);
    }
    var p0 = [_]external.PartitionValue{.{ .column_id = @constCast("day"), .string_value = @constCast("0") }};
    var p1 = [_]external.PartitionValue{.{ .column_id = @constCast("day"), .string_value = @constCast("1") }};
    var files = [_]iceberg.IcebergDeleteFile{
        .{ .content = .position_deletes, .file_path = @constCast("s3://bucket/d0"), .file_format = @constCast("PARQUET"), .snapshot_id = 1, .data_sequence_number = 2, .file_sequence_number = 3, .record_count = 1, .file_size_in_bytes = 5, .partition_spec_id = 1, .partition_field_count = 1, .partition_values = &p0 },
        .{ .content = .position_deletes, .file_path = @constCast("s3://bucket/d1"), .file_format = @constCast("PARQUET"), .snapshot_id = 1, .data_sequence_number = 2, .file_sequence_number = 3, .record_count = 1, .file_size_in_bytes = 5, .partition_spec_id = 1, .partition_field_count = 1, .partition_values = &p1 },
    };
    var ids = [_]i32{1};
    var names = [_][]u8{@constCast("id")};
    files[1].content = .equality_deletes;
    files[1].equality_ids = &ids;
    files[1].equality_columns = &names;
    const plan: iceberg.IcebergDeletePlan = .{ .files = &files };
    const Meter = struct {
        base: storage.ObjectStorage,
        calls: std.atomic.Value(usize) = .init(0),
        fn stat(raw: *anyopaque, alloc: A, bucket: []const u8, key: []const u8) !storage.ObjectMetadata {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .monotonic);
            var base = self.base;
            base.allocator = alloc;
            return base.statObject(bucket, key);
        }
    };
    var meter: Meter = .{ .base = client };
    var vtable = client.vtable.*;
    vtable.stat_object = Meter.stat;
    vtable.stat_object_with_options = null;
    const measured: storage.ObjectStorage = .{ .allocator = a, .ptr = &meter, .vtable = &vtable };
    const first = try Qualification.create(a);
    defer first.destroy();
    const qualified = (try first.acquire(measured, plan, .{ .io = std.testing.io })).?;
    try std.testing.expectEqual(@as(usize, 2), meter.calls.load(.acquire));
    try std.testing.expect(qualified == (try first.acquire(measured, plan, .{ .io = std.testing.io })).?);
    try std.testing.expectEqual(@as(usize, 2), meter.calls.load(.acquire));
    try std.testing.expectError(error.DeadlineExceeded, first.acquire(measured, plan, .{ .deadline_ns = 0 }));
    var data_file: external.FileEntry = .{ .file_id = @constCast("file"), .object_uri = @constCast("s3://bucket/data"), .version_id = @constCast("v1"), .byte_len = 10, .row_count = 2, .row_groups = &.{}, .data_sequence_number = 1, .partition_spec_id = 1, .partition_field_count = 1, .partition_values = &p1 };
    const selected = (try qualified.candidates(a, &.{data_file})).?;
    defer a.free(selected);
    try std.testing.expectEqualSlices(usize, &.{1}, selected);
    data_file.data_sequence_number = 2;
    const equal_sequence = (try qualified.candidates(a, &.{data_file})).?;
    defer a.free(equal_sequence);
    try std.testing.expectEqual(@as(usize, 0), equal_sequence.len);
    data_file.partition_values = &p0;
    const equal_position = (try qualified.candidates(a, &.{data_file})).?;
    defer a.free(equal_position);
    try std.testing.expectEqualSlices(usize, &.{0}, equal_position);
    data_file.partition_values = &p1;
    data_file.data_sequence_number = 3;
    const no_deletes = (try qualified.candidates(a, &.{data_file})).?;
    defer a.free(no_deletes);
    try std.testing.expectEqual(@as(usize, 0), no_deletes.len);
    data_file.data_sequence_number = 1;
    data_file.partition_values = &.{};
    try std.testing.expect((try qualified.candidates(a, &.{data_file})) == null);
    var put = try client.putObject("bucket", "d1", "other", .{});
    put.deinit(a);
    const refreshed = try Qualification.create(a);
    defer refreshed.destroy();
    const changed = (try refreshed.acquire(measured, plan, .{ .io = std.testing.io })).?;
    try std.testing.expect(!std.mem.eql(u8, &qualified.digest, &changed.digest));
    try std.testing.expectEqual(@as(usize, 4), meter.calls.load(.acquire));
    const pressure = try Qualification.create(a);
    defer pressure.destroy();
    pressure.budget.limit = 1;
    try std.testing.expect((try pressure.acquire(measured, plan, .{})) == null);
    try std.testing.expect((try pressure.acquire(measured, plan, .{})) == null);
    try std.testing.expectEqual(@as(usize, 4), meter.calls.load(.acquire));
    const Failure = struct {
        fn run(alloc: A, base: storage.ObjectStorage, deletes: iceberg.IcebergDeletePlan) !void {
            const owned = try Qualification.create(alloc);
            owned.budget.backing = alloc;
            defer owned.destroy();
            const result = (try owned.acquire(base, deletes, .{})).?;
            try std.testing.expectEqual(deletes.files.len, result.versions.len);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Failure.run, .{ client, plan });
    // Many data files in one partition contribute one suffix, including strict
    // equality and inclusive position sequence boundaries.
    data_file.partition_values = &p1;
    const duplicates = try a.alloc(external.FileEntry, 10000);
    defer a.free(duplicates);
    @memset(duplicates, data_file);
    duplicates[0].data_sequence_number = 9;
    const unioned = (try qualified.candidates(a, duplicates)).?;
    defer a.free(unioned);
    try std.testing.expectEqualSlices(usize, &.{1}, unioned);
    const Selectors = struct {
        data: *const Qualification.Data,
        calls: usize = 0,
        fn read(raw: *anyopaque, alloc: A, index: usize, _: iceberg.IcebergDeleteFile, context: Context) !Qualification.Version {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try context.ensureActive();
            self.calls += 1;
            const item = self.data.versions[index];
            const etag = try alloc.dupe(u8, item.etag);
            errdefer alloc.free(etag);
            return .{ .etag = etag, .version = try alloc.dupe(u8, item.version), .sequence = item.sequence, .position = item.position };
        }
    };
    var selectors: Selectors = .{ .data = qualified };
    const proof: Qualification.Proof = .{ .ptr = &selectors, .count = plan.files.len, .recipe = qualified.recipe, .digest = qualified.digest, .source = @splat(7), .data = @splat(8), .lookup = Selectors.read };
    const proven = try Qualification.create(a);
    defer proven.destroy();
    const before = meter.calls.load(.acquire);
    const lazy = (try proven.acquireProven(measured, plan, .{}, proof)).?;
    try std.testing.expectEqual(before, meter.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), selectors.calls);
    const chosen = (try lazy.candidates(a, &.{data_file})).?;
    defer a.free(chosen);
    try std.testing.expectEqualSlices(usize, &.{1}, chosen);
    const selector = try lazy.evidence(a, chosen[0], plan.files[chosen[0]], .{});
    defer {
        a.free(selector.etag);
        a.free(selector.version);
    }
    try std.testing.expectEqual(@as(usize, 1), selectors.calls);
    try std.testing.expectEqualStrings(qualified.versions[1].etag, selector.etag);
    const invalid = try Qualification.create(a);
    defer invalid.destroy();
    var wrong = proof;
    wrong.recipe = @splat(0);
    try std.testing.expectError(error.InvalidIcebergDeleteManifest, invalid.acquireProven(measured, plan, .{}, wrong));
    try std.testing.expectError(error.DeadlineExceeded, proven.acquireProven(measured, plan, .{ .deadline_ns = 0 }, proof));
}
