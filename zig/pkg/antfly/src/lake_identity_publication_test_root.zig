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

//! Focused publication measurement with real immutable storage and corpus checks.
pub const antfly_sources = @import("source_owner_physical.zig");
const std = @import("std");
const local = @import("antfly_local_sources");
const stores = @import("serverless/artifacts/store.zig");
const state = @import("api/lake_index_native_state.zig");
const A = std.mem.Allocator;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const File = state.File;
const Plan = state.Plan;
const Tracker = state.Tracker;
const Deletes = state.Deletes;
const bulk_identity_chunk_target_bytes = state.bulk_identity_chunk_target_bytes;

test "external lake execution bulk identity publication batching preserves the full corpus" {
    const a = std.testing.allocator;
    const Fs = @import("serverless/artifacts/fs_store.zig").FsStore;
    const Probe = struct {
        var chunks: std.atomic.Value(usize) = .init(0);
        var puts: std.atomic.Value(usize) = .init(0);
        var quota: ?*anyopaque = null;
        fn put(raw: *anyopaque, alloc: A, scope: stores.UploadScope, bytes_: []const u8, token: Cancellation) !stores.ArtifactMetadata {
            // Covers identity chunks and every paged-reference publication.
            try std.testing.expect(alloc.ptr == quota.?);
            _ = puts.fetchAdd(1, .monotonic);
            if (std.mem.startsWith(u8, bytes_, "lake2:")) _ = chunks.fetchAdd(1, .monotonic);
            const fs: *Fs = @ptrCast(@alignCast(raw));
            const original = fs.artifactStore();
            return original.vtable.put_scoped.?(raw, alloc, scope, bytes_, token);
        }
    };
    for ([_]usize{ 96, 108 }) |key_width| {
        var chunk_counts: [2]usize = undefined;
        var put_counts: [2]usize = undefined;
        for ([_]usize{ 96 * 2048, bulk_identity_chunk_target_bytes }, 0..) |target, trial| {
            Probe.chunks.store(0, .monotonic);
            Probe.puts.store(0, .monotonic);
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const out = arena.allocator();
            var directory = try local.common_test_directory.TestDirectory.init("bulk-identity-publication");
            defer directory.cleanup();
            var fs = try Fs.init(a, directory.path());
            defer fs.deinit();
            var store = fs.artifactStore();
            var vtable = store.vtable.*;
            vtable.put_scoped = Probe.put;
            store.vtable = &vtable;
            store.upload_scope = try stores.UploadScope.forPublication(@splat(7), 1, std.testing.io);
            var current = [_]File{.{ .id = "fragment", .digest = @splat(2), .key_digest = @splat(9) }};
            var plan: Plan = .{ .a = a, .files = &current, .changed = try a.dupe(bool, &.{true}) };
            defer plan.deinit();
            try plan.by_id.put(a, "fragment", 0);
            var budget: local.sql_memory_budget = .{ .backing = a, .limit = 8 * 1024 * 1024 };
            Probe.quota = budget.allocator().ptr;
            var tracker = try Tracker.init(budget.allocator(), out, &plan, &store, .none);
            defer tracker.deinit();
            tracker.chunk_target_bytes = target;
            var expected = std.crypto.hash.Blake3.init(.{});
            var key_buffer: [1024]u8 = undefined;
            var key_alloc = std.heap.FixedBufferAllocator.init(&key_buffer);
            const begin = @import("antfly_platform").time.monotonicNs();
            for (0..1_000_000) |row| {
                key_alloc.reset();
                const ref: local.storage_rowsource_types.RowRef = .{ .external = .{ .source_id = "lake", .snapshot_id = "snapshot", .file_id = "fragment", .row_group_ordinal = 0, .row_ordinal = row } };
                const parent = try plan.privateKey(key_alloc.allocator(), ref);
                const key = if (key_width == 96) parent else try @import("api/lake_enrichment_units.zig").identity(key_alloc.allocator(), parent, .{ .chunked = true, .ordinal = 1 });
                if (row == 0) {
                    tracker.chunk_target_bytes = 1024 * 1024;
                    try std.testing.expectError(error.InvalidArgument, tracker.append(ref, key));
                    try std.testing.expectEqual(@as(u64, 0), current[0].count);
                    tracker.chunk_target_bytes = target;
                }
                expected.update(key);
                try tracker.append(ref, key);
            }
            try tracker.finish();
            try std.testing.expect(store.allocator.ptr == a.ptr);
            const elapsed = @import("antfly_platform").time.monotonicNs() - begin;
            chunk_counts[trial] = Probe.chunks.load(.monotonic);
            put_counts[trial] = Probe.puts.load(.monotonic);
            try std.testing.expectEqual(@as(u64, 1_000_000), current[0].count);
            try state.validate(&current, store.upload_scope.?.domain);
            // Reopen every real stored chunk through the established paged reader.
            var deletes: Deletes = .{ .files = &current, .current = &plan, .domain = store.upload_scope.?.domain };
            defer deletes.deinit();
            var actual = std.crypto.hash.Blake3.init(.{});
            var members: usize = 0;
            while (try deletes.next(a, store, .none)) |payload| {
                defer a.free(payload);
                actual.update(payload);
                members += payload.len / current[0].key_width;
            }
            try std.testing.expectEqual(@as(usize, 1_000_000), members);
            var left: [32]u8 = undefined;
            var right: [32]u8 = undefined;
            expected.final(&left);
            actual.final(&right);
            try std.testing.expectEqualSlices(u8, &left, &right);
            std.debug.print("BULK_IDENTITY_PUBLICATION target_bytes={} key_width={} rows={} chunks={} puts={} allocator_peak={} elapsed_ns={}\n", .{ target, key_width, members, chunk_counts[trial], put_counts[trial], budget.peak, elapsed });
            try std.testing.expect(budget.peak <= 8 * 1024 * 1024);
        }
        try std.testing.expectEqual(@as(usize, if (key_width == 96) 489 else 550), chunk_counts[0]);
        try std.testing.expectEqual(@as(usize, if (key_width == 96) 367 else 413), chunk_counts[1]);
        try std.testing.expect(put_counts[1] < put_counts[0]);
    }
}
