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
const Store = @import("../metadata/storage/raft_apply_store.zig").RaftApplyStore;
const checkpoints = @import("antfly_local_sources").privacy_catalog.checkpoints;

test "privacy checkpoint GC page does not copy a large table for each header" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/privacy-gc-budget", .{tmp.sub_path});
    var counter = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var store = try Store.init(counter.allocator(), .{ .root_dir = root });
    defer store.deinit();
    const description = try a.alloc(u8, 8 * 1024 * 1024);
    @memset(description, 'x');
    var refs: [32]checkpoints.Ref = undefined;
    var tokens: [32][101]u8 = undefined;
    for (&refs, &tokens, 0..) |*ref, *token, i| {
        var generation: [16]u8 = @splat(0);
        generation[0] = @intCast(i);
        ref.* = .{ .digest = @splat('a'), .generation = std.fmt.bytesToHex(generation, .lower) };
        token.* = ref.token();
    }
    var token_views: [32][]const u8 = undefined;
    for (&tokens, &token_views) |*token, *view| view.* = token[0..];
    const indexes = try std.json.Stringify.valueAlloc(a, .{ .hold = token_views }, .{});
    try store.applyStandaloneCommand(7303, .{ .upsert_table = .{ .table_id = 1, .name = "gc", .description = description, .indexes_json = indexes } });
    const header = try std.json.Stringify.valueAlloc(a, checkpoints.Header{ .length = 1, .created_at_millis = 1 }, .{});
    for (refs) |ref| {
        var buffer: [128]u8 = undefined;
        try store.applyStandaloneCommand(7303, .{ .compare_and_set_job_state = .{ .key = try ref.storageKey(&buffer, 1, null), .expected_hash = "", .value = header } });
    }
    const before = counter.allocated_bytes;
    const bytes = try store.scanJobState(counter.allocator(), 7303, "privacy/m/");
    defer counter.allocator().free(bytes);
    const allocated = counter.allocated_bytes - before;
    // One pinned table read plus bounded page framing, rather than 32 deep
    // copies of the 8 MiB description retained until the GC page completes.
    try std.testing.expect(allocated < 32 * 1024 * 1024);
    const page = try std.json.parseFromSlice(@import("../metadata/job_state.zig").Page, a, bytes, .{});
    try std.testing.expectEqual(@as(usize, 0), page.value.rows.len);
    try std.testing.expect(page.value.next != null);
}

test "privacy GC drains many small generations fairly within a shared chunk budget" {
    const jobs = @import("../metadata/job_state.zig");
    const collector = @import("privacy_checkpoints.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try Store.init(std.testing.allocator, .{ .root_dir = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/privacy-drain", .{tmp.sub_path}) });
    defer store.deinit();
    const group = 7304;
    try store.applyStandaloneCommand(group, .{ .upsert_table = .{ .table_id = 1, .name = "gc" } });
    const Backend = struct {
        store: *Store,
        a: std.mem.Allocator,
        erased: usize = 0,
        chunks: usize = 0,
        calls: usize = 0,
        fn get(raw: *anyopaque, alloc: std.mem.Allocator, key: []const u8) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.store.getJobState(alloc, group, key);
        }
        fn scan(raw: *anyopaque, alloc: std.mem.Allocator, after: ?[]const u8) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.store.scanJobState(alloc, group, after);
        }
        fn cas(raw: *anyopaque, key: []const u8, expected: ?[]const u8, value: []const u8, term: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(u64, 9), term);
            const before = (try self.store.getJobState(self.a, group, key)) orelse return false;
            if (expected == null or !std.mem.eql(u8, &jobs.digest(before), expected.?)) return false;
            try self.store.applyStandaloneCommand(group, .{ .compare_and_set_job_state = .{ .key = key, .expected_hash = expected.?, .value = value } });
            self.calls += 1;
            if (std.mem.eql(u8, value, checkpoints.erase)) {
                try std.testing.expect(try self.store.getJobState(self.a, group, key) == null);
                self.erased += 1;
            } else {
                const old = try std.json.parseFromSliceLeaky(checkpoints.Header, self.a, before, .{});
                const next = try std.json.parseFromSliceLeaky(checkpoints.Header, self.a, value, .{});
                self.chunks += next.deleted_chunks - old.deleted_chunks;
                try std.testing.expectEqualStrings(value, (try self.store.getJobState(self.a, group, key)).?);
            }
            return true;
        }
    };
    const Source = struct {
        pub fn privacyAuthorityTerm(_: @This()) !?u64 {
            return 9;
        }
    };
    var backend = Backend{ .store = &store, .a = a };
    var server = struct {
        source: Source = .{},
        privacy_checkpoint_gc: collector.GC = .{ .quantum_ns = std.math.maxInt(u64) },
        document_job_store: struct { replicated: ?jobs.Persistence },
    }{ .document_job_store = .{ .replicated = jobs.Persistence.fromLocal(&backend, .{ .get = Backend.get, .scan = Backend.scan, .cas = Backend.cas }) } };
    var manifests: [33][]const u8 = undefined;
    for (&manifests, 0..) |*manifest, i| {
        var generation: [16]u8 = @splat(0);
        generation[0] = @intCast(i);
        const ref: checkpoints.Ref = .{ .digest = @splat('a'), .generation = std.fmt.bytesToHex(generation, .lower) };
        var buffer: [128]u8 = undefined;
        manifest.* = try a.dupe(u8, try ref.storageKey(&buffer, 1, null));
        // Expired incomplete uploads are valid GC work even without chunks.
        const header = try std.json.Stringify.valueAlloc(a, checkpoints.Header{ .accounted = true, .length = if (i == 0) checkpoints.chunk_bytes * 9 else 1, .created_at_millis = 1 }, .{});
        try store.applyStandaloneCommand(group, .{ .compare_and_set_job_state = .{ .key = manifest.*, .expected_hash = "", .value = header } });
    }
    try collector.collect(&server, a);
    try std.testing.expectEqual(@as(usize, 8), backend.chunks);
    try std.testing.expectEqual(@as(usize, 0), backend.erased);
    server.privacy_checkpoint_gc.next_ns = 0;
    server.privacy_checkpoint_gc.quantum_ns = 0;
    backend.calls = 0;
    backend.chunks = 0;
    try collector.collect(&server, a);
    try std.testing.expectEqual(@as(usize, 1), backend.erased);
    try std.testing.expectEqual(@as(usize, 2), backend.calls);
    server.privacy_checkpoint_gc.next_ns = 0;
    server.privacy_checkpoint_gc.quantum_ns = std.math.maxInt(u64);
    backend.calls = 0;
    backend.chunks = 0;
    try collector.collect(&server, a);
    try std.testing.expectEqual(@as(usize, 9), backend.erased);
    try std.testing.expectEqual(@as(usize, 8), backend.chunks);
    try std.testing.expect(backend.calls <= 32);
    try std.testing.expect(try store.getJobState(a, group, manifests[0]) != null);
    for (0..16) |_| {
        server.privacy_checkpoint_gc.next_ns = 0;
        backend.calls = 0;
        backend.chunks = 0;
        try collector.collect(&server, a);
        try std.testing.expect(backend.chunks <= checkpoints.gc_chunks);
        try std.testing.expect(backend.calls <= 32);
        if (backend.erased == manifests.len) break;
    }
    try std.testing.expectEqual(manifests.len, backend.erased);
    var buffer: [64]u8 = undefined;
    try std.testing.expect(try store.getJobState(a, group, try checkpoints.quotaKey(&buffer, 1, true)) == null);
    try std.testing.expect(try store.getJobState(a, group, try checkpoints.quotaKey(&buffer, 1, false)) == null);
}
