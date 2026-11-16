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

//! Navigation adapter for the shared authenticated range planner.
const std = @import("std");
const local = @import("antfly_local_sources");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const pages = @import("../serverless/graph_segment/page_store.zig");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const ranges = @import("lake_artifact_ranges.zig");
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;
pub const Budget = ranges.Budget;
pub const Batch = struct {
    a: A,
    refs: []const tree.Ref,
    values: [tree.max_read_batch]?[]u8 = @splat(null),
    pub fn deinit(self: *Batch) void {
        for (&self.values) |*value| if (value.*) |bytes| {
            self.a.free(bytes);
            value.* = null;
        };
    }
    pub fn lookup(self: *const Batch, ref: tree.Ref) ?[]const u8 {
        for (self.refs, self.values[0..self.refs.len]) |candidate, value| if (candidate.eql(ref)) return value;
        return null;
    }
};
pub fn load(a: A, store: stores.ArtifactStore, domain: [32]u8, refs: []const tree.Ref, token: Token, cached: ?artifacts.CachedRead, budget: Budget) !Batch {
    if (refs.len > tree.max_read_batch) return error.InvalidGraphPage;
    var ids: [tree.max_read_batch][175]u8 = undefined;
    var units: [tree.max_read_batch]ranges.Unit = undefined;
    for (refs, 0..) |ref, i| {
        try ref.validate();
        ids[i] = try pages.PageStore.identity(domain, ref);
        units[i] = .{ .object = .{ .artifact_id = &ids[i], .checksum = ids[i][7..71], .byte_len = ref.artifactBytes() }, .offset = ref.offset(), .bytes = ref.bytes, .digest = ref.digest };
    }
    const batch = try ranges.load(a, store, units[0..refs.len], token, cached, budget, .{ .max_range = tree.max_page_bytes, .max_gap = 16 * 1024 });
    return .{ .a = a, .refs = refs, .values = batch.values[0..tree.max_read_batch].* };
}

test "external lake metadata navigation range waves coalesce authenticate cache and bound budgets" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-range-waves");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var backend = fs.artifactStore();
    const scope: stores.UploadScope = .{ .domain = @splat(7), .attempt = @splat(1) };
    var payload: [384]u8 = undefined;
    for (&payload, 0..) |*byte, i| byte.* = @intCast(i % 251);
    var uploaded = try backend.putScoped(scope, &payload, .none);
    defer uploaded.deinit(a);
    var pack_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&payload, &pack_digest, .{});
    var refs_: [3]tree.Ref = undefined;
    for (&refs_, 0..) |*ref, i| {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(payload[i * 128 ..][0..128], &digest, .{});
        ref.* = .{ .digest = digest, .attempt = scope.attempt, .bytes = 128, .height = 0, .records = 1, .pack = .{ .digest = pack_digest, .bytes = payload.len, .offset = @intCast(i * 128) } };
    }
    const Probe = struct {
        var active: *@This() = undefined;
        vtable: *const stores.ArtifactStore.VTable,
        reads: std.atomic.Value(usize) = .init(0),
        max_length: std.atomic.Value(usize) = .init(0),
        corrupt: bool = false,
        fn get(raw: *anyopaque, alloc: A, id: []const u8, offset: u64, length: usize, token: Token) ![]u8 {
            const self = active;
            _ = self.reads.fetchAdd(1, .monotonic);
            _ = self.max_length.fetchMax(length, .monotonic);
            const bytes = if (self.vtable.get_range_alloc_with_cancellation) |read| try read(raw, alloc, id, offset, length, token) else try self.vtable.get_range_alloc(raw, alloc, id, offset, length);
            if (self.corrupt and bytes.len > 128) bytes[128] ^= 1;
            return bytes;
        }
    };
    var probe: Probe = .{ .vtable = backend.vtable };
    Probe.active = &probe;
    var vtable = backend.vtable.*;
    vtable.get_range_alloc_with_cancellation = Probe.get;
    backend.vtable = &vtable;
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(8), .context = .{ .io = std.testing.io } };
    const Charge = struct {
        fn charge(raw: *anyopaque, bytes: u64) !void {
            try stores.chargeReadBudget(@ptrCast(@alignCast(raw)), bytes);
        }
    };
    const shuffled = [_]tree.Ref{ refs_[2], refs_[0], refs_[1], refs_[0] };
    var remaining: u64 = 4096;
    var cold = try load(a, backend, scope.domain, &shuffled, .none, cached, .{ .ptr = &remaining, .charge = Charge.charge });
    defer cold.deinit();
    try std.testing.expectEqual(@as(usize, 1), probe.reads.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().provider_reads);
    for (shuffled, cold.values[0..shuffled.len]) |ref, bytes| try std.testing.expectEqualSlices(u8, payload[ref.offset()..][0..ref.bytes], bytes.?);
    var warm = try load(a, backend, scope.domain, &shuffled, .none, cached, .{ .ptr = &remaining, .charge = Charge.charge });
    defer warm.deinit();
    try std.testing.expectEqual(@as(usize, 1), probe.reads.load(.monotonic));
    remaining = 1;
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, load(a, backend, scope.domain, &refs_, .none, cached, .{ .ptr = &remaining, .charge = Charge.charge }));
    try std.testing.expectEqual(@as(usize, 1), probe.reads.load(.monotonic));
    remaining = 4096;
    probe.corrupt = true;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, load(a, backend, scope.domain, &refs_, .none, null, .{ .ptr = &remaining, .charge = Charge.charge }));
    probe.corrupt = false;
    // Bounded coalescing never downloads a whole large pack. Independent
    // requests still use the shared scheduler and preserve per-page results.
    {
        const large = try a.alloc(u8, 1200 * 1024);
        defer a.free(large);
        @memset(large, 37);
        var object = try backend.putScoped(scope, large, .none);
        defer object.deinit(a);
        std.crypto.hash.sha2.Sha256.hash(large, &pack_digest, .{});
        var wide: [2]tree.Ref = undefined;
        for (&wide, 0..) |*ref, i| {
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(large[i * 600 * 1024 ..][0 .. 600 * 1024], &digest, .{});
            ref.* = .{ .digest = digest, .attempt = scope.attempt, .bytes = 600 * 1024, .height = 0, .records = 1, .pack = .{ .digest = pack_digest, .bytes = @intCast(large.len), .offset = @intCast(i * 600 * 1024) } };
        }
        remaining = 8 * 1024 * 1024;
        const before = probe.reads.load(.monotonic);
        var wave = try load(a, backend, scope.domain, &wide, .none, cached, .{ .ptr = &remaining, .charge = Charge.charge });
        defer wave.deinit();
        try std.testing.expectEqual(before + 2, probe.reads.load(.monotonic));
        try std.testing.expect(probe.max_length.load(.monotonic) <= tree.max_page_bytes);
    }
    // Coalesced gaps consume real allowance and are checked before provider IO.
    {
        var spaced: [272]u8 = @splat(59);
        var object = try backend.putScoped(scope, &spaced, .none);
        defer object.deinit(a);
        std.crypto.hash.sha2.Sha256.hash(&spaced, &pack_digest, .{});
        var gap: [2]tree.Ref = undefined;
        for (&gap, 0..) |*ref, i| {
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(spaced[i * 144 ..][0..128], &digest, .{});
            ref.* = .{ .digest = digest, .attempt = scope.attempt, .bytes = 128, .height = 0, .records = 1, .pack = .{ .digest = pack_digest, .bytes = spaced.len, .offset = @intCast(i * 144) } };
        }
        remaining = 256;
        const before = probe.reads.load(.monotonic);
        try std.testing.expectError(error.ArtifactReadBudgetExceeded, load(a, backend, scope.domain, &gap, .none, null, .{ .ptr = &remaining, .charge = Charge.charge }));
        try std.testing.expectEqual(before, probe.reads.load(.monotonic));
        remaining = 272;
        var wave = try load(a, backend, scope.domain, &gap, .none, null, .{ .ptr = &remaining, .charge = Charge.charge });
        defer wave.deinit();
        try std.testing.expectEqual(@as(u64, 0), remaining);
        try std.testing.expectEqual(before + 1, probe.reads.load(.monotonic));
        // Duplicate logical seeks do not provide credit for physical gap IO.
        const repeated = [_]tree.Ref{ gap[0], gap[0], gap[1] };
        remaining = 384;
        try std.testing.expectError(error.ArtifactReadBudgetExceeded, load(a, backend, scope.domain, &repeated, .none, null, .{ .ptr = &remaining, .charge = Charge.charge }));
        remaining = 400;
        var duplicates = try load(a, backend, scope.domain, &repeated, .none, null, .{ .ptr = &remaining, .charge = Charge.charge });
        defer duplicates.deinit();
        try std.testing.expectEqual(@as(u64, 0), remaining);
    }
    const Sweep = struct {
        fn run(alloc: A, store: stores.ArtifactStore, domain: [32]u8, references: []const tree.Ref) !void {
            var allowance: u64 = 4096;
            var batch = try load(alloc, store, domain, references, .none, null, .{ .ptr = &allowance, .charge = Charge.charge });
            defer batch.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Sweep.run, .{ backend, scope.domain, &shuffled });
}
