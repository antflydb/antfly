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
const ds = @import("docstore.zig");
const lsm = @import("lsm_backend.zig");
const schema = @import("schema.zig");
const payload = @import("document_payload.zig");
const mutation = @import("document_mutation_revision.zig");
const keys = @import("internal_keys.zig");
const claims = @import("index_source_claim.zig");
const publog = claims.publication;
const A = std.mem.Allocator;
fn sha(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
const Observed = struct { revision: u64, root: [32]u8 };
fn observe(store: *ds.DocStore, physical: []const u8) !Observed {
    var read = try store.beginReadTxn();
    defer read.abort();
    const rev = try mutation.load(&read, mutation.keyForPhysical(physical).?);
    const raw = try read.getPhysical(physical);
    return .{ .revision = rev, .root = if (payload.isManifest(raw)) payload.publishedCommitment(raw) else sha(raw) };
}
const Writer = struct {
    txn: *ds.DocStore.Txn,
    blob_reads: usize = 0,
    blob_writes: usize = 0,
    reference_writes: usize = 0,
    snapshot_reads: usize = 0,
    snapshot_writes: usize = 0,
    schema_reads: usize = 0,
    raw_override: ?struct { key: []const u8, bytes: []const u8 } = null,
    pub fn poison(self: *@This()) void {
        self.txn.poison();
    }
    pub fn hasPrefix(self: *@This(), prefix: []const u8) anyerror!bool {
        return self.txn.hasPrefix(prefix);
    }
    pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
        return self.getPhysical(key);
    }
    pub fn getPhysical(self: *@This(), key: []const u8) anyerror![]const u8 {
        if (self.raw_override) |raw| if (std.mem.eql(u8, raw.key, key)) return raw.bytes;
        if (std.mem.eql(u8, key, schema.schema_key)) self.schema_reads += 1;
        if (std.mem.startsWith(u8, key, payload.ownership.value_prefix)) self.blob_reads += 1;
        if (std.mem.startsWith(u8, key, "\x00\x00__metadata__:index_source_snapshot_v1:")) self.snapshot_reads += 1;
        return self.txn.getPhysical(key);
    }
    pub fn put(self: *@This(), key: []const u8, value: []const u8) anyerror!void {
        if (std.mem.startsWith(u8, key, payload.ownership.value_prefix)) self.blob_writes += 1;
        if (std.mem.startsWith(u8, key, payload.ownership.reference_prefix)) self.reference_writes += 1;
        if (std.mem.startsWith(u8, key, "\x00\x00__metadata__:index_source_snapshot_v1:")) self.snapshot_writes += 1;
        try self.txn.put(key, value);
    }
    pub fn delete(self: *@This(), key: []const u8) anyerror!void {
        try self.txn.delete(key);
    }
};
fn cancelled(a: A, store: *ds.DocStore, binding: publog.Binding) !publog.Decision {
    var txn = try store.beginWriteTxn();
    errdefer txn.abort();
    try publog.stageCancel(a, &txn, binding);
    try txn.commit();
    var read = try store.beginReadTxn();
    defer read.abort();
    return (try publog.decision(&read, binding)).?;
}

test "index source claims share immutable sets and exact snapshots across publication and reader retirement" {
    const a = std.testing.allocator;
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    var index_backend = lsm.Backend.init(a, .{});
    defer index_backend.close();
    var index = try ds.DocStore.openRuntime(a, try index_backend.runtimeStore(a, .{}));
    defer index.close();
    store.document_payload_prototype = true;
    const encoded = try schema.serializeSchema(a, .{});
    defer a.free(encoded);
    try store.put(schema.schema_key, encoded);
    const physical = try keys.documentKeyAlloc(a, "source\x00doc");
    defer a.free(physical);
    const body = try a.alloc(u8, 128 * 1024);
    defer a.free(body);
    @memset(body, 'x');
    const first = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":1}}", .{body});
    defer a.free(first);
    const second = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":2}}", .{body});
    defer a.free(second);
    const third = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":3}}", .{body});
    defer a.free(third);
    try store.put(physical, first);
    try store.put(physical, second);
    const original = try observe(&store, physical);
    const binding: publog.Binding = .{ .owner = @splat(1), .publication = @splat(2) };
    const other: publog.Binding = .{ .owner = @splat(3), .publication = @splat(4) };
    const updated: publog.Binding = .{ .owner = @splat(1), .publication = @splat(5) };
    var ref: claims.Reference = undefined;
    var forked: claims.Reference = undefined;
    var changed: claims.Reference = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writes: Writer = .{ .txn = &txn };
        ref = try claims.prepareCurrent(a, &writes, binding, physical, original.revision, original.root, .{});
        try std.testing.expectEqual(@as(usize, 1), writes.snapshot_writes);
        try std.testing.expectEqual(@as(usize, 1), writes.reference_writes);
        forked = try claims.fork(a, &writes, ref, other, .{});
        try std.testing.expectEqual(@as(usize, 1), writes.snapshot_writes);
        try std.testing.expectEqual(@as(usize, 0), writes.snapshot_reads);
        try std.testing.expectEqual(@as(usize, 0), writes.blob_reads);
        try std.testing.expectEqual(@as(usize, 0), writes.blob_writes);
        try std.testing.expectEqualDeep(ref, try claims.prepareCurrent(a, &writes, binding, physical, original.revision, original.root, .{}));
        const budget = try claims.Budget.load(&writes);
        try std.testing.expectEqual(@as(u64, 2), budget.claims);
        try std.testing.expect(budget.value_bytes >= body.len);
        try txn.commit();
    }
    try store.put(physical, third);
    {
        const next = try observe(&store, physical);
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writes: Writer = .{ .txn = &txn };
        changed = try claims.prepareCurrent(a, &writes, updated, physical, next.revision, next.root, .{});
        // A metadata revision adds one small snapshot; its body set already
        // owns the value, so no body/refcount I/O is repeated.
        try std.testing.expectEqual(@as(usize, 1), writes.snapshot_writes);
        try std.testing.expectEqual(@as(usize, 0), writes.reference_writes);
        try std.testing.expectEqual(@as(usize, 0), writes.blob_reads);
        try std.testing.expectEqual(@as(usize, 0), writes.blob_writes);
        try std.testing.expectEqual(@as(u64, 3), (try claims.Budget.load(&writes)).claims);
        try txn.commit();
    }
    {
        const next = try observe(&store, physical);
        const fresh_binding: publog.Binding = .{ .owner = binding.owner, .publication = @splat(99) };
        {
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            _ = try claims.prepareCurrentUnique(a, &txn, fresh_binding, physical, next.revision, next.root, .{});
            try std.testing.expectError(error.InvalidIndexSourceMap, claims.prepareCurrentUnique(a, &txn, fresh_binding, physical, next.revision, next.root, .{}));
            // Catching a duplicate cannot commit its first claim, reference counts,
            // or format fence. The ordinary prepare API above remains idempotent.
            try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
        }
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 3), (try claims.Budget.load(&read)).claims);
    }
    const root: [32]u8 = @splat(9);
    var published: publog.Decision = undefined;
    {
        var txn = try index.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try publog.stagePublish(a, &txn, binding, root, &.{7}));
        try std.testing.expect(!try publog.stagePublish(a, &txn, binding, root, &.{7}));
        try std.testing.expectError(error.IndexPublicationAlreadyCommitted, publog.stageCancel(a, &txn, binding));
        try txn.commit();
        var read = try index.beginReadTxn();
        defer read.abort();
        published = (try publog.decision(&read, binding)).?;
    }
    var pins: publog.ReaderPins = .{ .binding = binding };
    var lease = try pins.acquire();
    defer lease.deinit();
    // Primary changes cannot change the source in a published root. The read
    // is legal before acknowledgment: a crash at that point retains the pin.
    @memset(body, 'y');
    const latest = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":99}}", .{body});
    defer a.free(latest);
    try store.put(physical, latest);
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const loaded = try claims.read(a, &read, ref, published, &lease);
        defer loaded.deinit(a);
        try std.testing.expectEqualStrings(second, loaded.bytes);
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try claims.acknowledge(a, &txn, ref, published);
        try claims.acknowledge(a, &txn, ref, published);
        try txn.commit();
    }
    const fence = try cancelled(a, &index, other);
    {
        var txn = try index.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.IndexPublicationCancelled, publog.stagePublish(a, &txn, other, root, &.{8}));
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try claims.cancel(a, &txn, forked, fence));
        try std.testing.expect(!try claims.cancel(a, &txn, forked, fence));
        try txn.commit();
    }
    var retired: publog.Decision = undefined;
    {
        var txn = try index.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectError(error.IndexPublicationStillActive, publog.stageRetire(a, &txn, binding, &.{7}));
        try publog.stageRetire(a, &txn, binding, &.{});
        try txn.commit();
        var read = try index.beginReadTxn();
        defer read.abort();
        retired = (try publog.decision(&read, binding)).?;
    }
    pins.retire();
    try std.testing.expect(pins.drained() == null);
    try std.testing.expectError(error.IndexSourceLeaseClosed, pins.acquire());
    var clone = try lease.clone();
    lease.deinit();
    try std.testing.expect(pins.drained() == null);
    clone.deinit();
    const drained = pins.drained().?;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try claims.retire(a, &txn, ref, retired, drained));
        try txn.commit();
    }
    const last_fence = try cancelled(a, &index, updated);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try claims.cancel(a, &txn, changed, last_fence));
        const budget = try claims.Budget.load(&txn);
        try std.testing.expectEqualDeep(claims.Budget{}, budget);
        try txn.commit();
    }
    const current = try store.get(a, physical);
    defer a.free(current);
    try std.testing.expectEqualStrings(latest, current);
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.IndexSourceLeaseClosed, claims.read(a, &read, ref, published, &lease));
    }
    // A reader-floor downgrade would let an old binary delete owned values.
    const old_format = try schema.serializeDocumentPayloadSchema(a, .{});
    defer a.free(old_format);
    try std.testing.expectError(error.UnsupportedVersion, store.put(schema.schema_key, old_format));
}

test "index source quota failures poison partial writes and preserve the reader floor" {
    const a = std.testing.allocator;
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const encoded = try schema.serializeSchema(a, .{});
    defer a.free(encoded);
    try store.put(schema.schema_key, encoded);
    const physical = try keys.documentKeyAlloc(a, "quota");
    defer a.free(physical);
    try store.put(physical, "{\"body\":\"hello\"}");
    const source = try observe(&store, physical);
    const binding: publog.Binding = .{ .owner = @splat(1), .publication = @splat(2) };
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.IndexSourceClaimsFull, claims.prepareCurrent(a, &txn, binding, physical, source.revision, source.root, .{ .claims = 0 }));
        // A caller catching the error must not commit the preceding schema,
        // snapshot, refcount and claim writes without their budget record.
        try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
    }
    {
        var batch = try store.beginWriteBatch();
        defer batch.abort();
        const txn = batch.asTxn();
        try std.testing.expectError(error.IndexSourceClaimsFull, claims.prepareCurrent(a, txn, binding, physical, source.revision, source.root, .{ .claims = 0 }));
        try std.testing.expectError(error.RetainedEffectsTransactionFailed, batch.commit());
    }
    var read = try store.beginReadTxn();
    defer read.abort();
    try std.testing.expectError(error.IndexSourceClaimsCorrupt, claims.Budget.load(&read));
    try std.testing.expectEqualStrings(encoded, try read.getPhysical(schema.schema_key));
}

test "index source allocation failures cannot commit partial ownership" {
    const Fixture = struct {
        fn run(a: A) !void {
            const backing = std.testing.allocator;
            var backend = lsm.Backend.init(backing, .{});
            defer backend.close();
            var store = try ds.DocStore.openRuntime(backing, try backend.runtimeStore(backing, .{}));
            defer store.close();
            const encoded = try schema.serializeSchema(backing, .{});
            defer backing.free(encoded);
            try store.put(schema.schema_key, encoded);
            const physical = try keys.documentKeyAlloc(backing, "allocations");
            defer backing.free(physical);
            try store.put(physical, "{\"body\":\"hello\"}");
            const source = try observe(&store, physical);
            const binding: publog.Binding = .{ .owner = @splat(1), .publication = @splat(2) };
            var txn = try store.beginWriteTxn();
            var open = true;
            defer if (open) txn.abort();
            _ = claims.prepareCurrent(a, &txn, binding, physical, source.revision, source.root, .{}) catch |err| {
                try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
                return err;
            };
            try txn.commit();
            open = false;
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "index source retained handoff survives history reclamation and primary reopen" {
    const a = std.testing.allocator;
    const retention = @import("retained_effects.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    var index_backend = lsm.Backend.init(a, .{});
    defer index_backend.close();
    var index = try ds.DocStore.openRuntime(a, try index_backend.runtimeStore(a, .{}));
    defer index.close();
    const physical = try keys.documentKeyAlloc(a, "historical");
    defer a.free(physical);
    const body = try a.alloc(u8, 128 * 1024);
    defer a.free(body);
    @memset(body, 'x');
    const first = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":1}}", .{body});
    defer a.free(first);
    const second = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":2}}", .{body});
    defer a.free(second);
    const expected = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":3}}", .{body});
    defer a.free(expected);
    const namespace: retention.Namespace = @splat(1);
    const binding: publog.Binding = .{ .owner = @splat(4), .publication = @splat(5) };
    var ref: claims.Reference = undefined;
    var blob: payload.BlobKey = undefined;
    var published: publog.Decision = undefined;
    var pins: publog.ReaderPins = .{ .binding = binding };
    var lease = try pins.acquire();
    defer lease.deinit();
    for (0..2) |pass| {
        var backend = try lsm.Backend.open(a, path, .{ .flush_threshold_bytes = 1024 });
        defer backend.close();
        var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        store.document_payload_prototype = true;
        if (pass == 0) {
            const encoded = try schema.serializeSchema(a, .{});
            defer a.free(encoded);
            try store.put(schema.schema_key, encoded);
            try store.put(&keys.identity_namespace_key, &namespace);
            try store.put(physical, first);
            try store.put(physical, second);
            {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                _ = try retention.admitWithCapabilities(&txn, namespace, 1, @splat(3), retention.default_limit, false, true);
                try txn.commit();
            }
            {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                try txn.patchMetadata(physical, &.{.{ .name = "metadata", .value_json = "3" }});
                try txn.commit();
            }
            const source = try observe(&store, physical);
            // Handoff from history remains valid after the current row moves.
            try store.put(physical, "{\"metadata\":4}");
            {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                ref = try claims.prepareRetained(a, &txn, binding, namespace, 1, physical, source.root, .{});
                try retention.acknowledge(&txn, namespace, 1, @splat(3), 0, 2);
                try std.testing.expectEqual(@as(usize, 2), try retention.reclaim(&txn, namespace, 128, 16 * 1024 * 1024));
                const state = (try retention.load(&txn)).?;
                try std.testing.expectEqual(@as(u64, 0), state.retained_bytes);
                try txn.commit();
            }
            // Native JSON string extents include their quotes.
            const json_body = try std.fmt.allocPrint(a, "\"{s}\"", .{body});
            defer a.free(json_body);
            blob = payload.blobKey(physical, sha(json_body));
            var txn = try index.beginWriteTxn();
            errdefer txn.abort();
            try std.testing.expect(try publog.stagePublish(a, &txn, binding, @splat(7), &.{1}));
            try txn.commit();
            var read = try index.beginReadTxn();
            defer read.abort();
            published = (try publog.decision(&read, binding)).?;
        } else {
            {
                var read = try store.beginReadTxn();
                defer read.abort();
                const source = try claims.read(a, &read, ref, published, &lease);
                defer source.deinit(a);
                try std.testing.expectEqualStrings(expected, source.bytes);
                var checked: Writer = .{ .txn = &read };
                const projected = (try claims.readProjected(a, &checked, ref, published, &lease, &.{&.{"metadata"}}, .{ .bytes = 1 })).?;
                defer projected.deinit(a);
                try std.testing.expectEqualStrings("3", projected.values[0].json.?);
                try std.testing.expectEqual(@as(usize, 0), checked.blob_reads);
                try std.testing.expect((try read.getPhysical(&blob)).len >= body.len);
                const state = (try retention.load(&read)).?;
                try std.testing.expectEqual(@as(u64, 0), state.retained_bytes);
            }
            // Logical cursors must hide claims, snapshots, value sets and the
            // exact-key budget record in both directions (including seeks).
            {
                var read = try store.beginReadTxn();
                defer read.abort();
                var cursor = try read.openCursor();
                defer cursor.close();
                var entry = try cursor.first();
                var visits: usize = 0;
                while (entry) |item| : (entry = try cursor.next()) {
                    try std.testing.expect(claims.privatePrefix(item.key) == null);
                    visits += 1;
                    try std.testing.expect(visits < 100);
                }
                entry = try cursor.last();
                visits = 0;
                while (entry) |item| : (entry = try cursor.prev()) {
                    try std.testing.expect(claims.privatePrefix(item.key) == null);
                    visits += 1;
                    try std.testing.expect(visits < 100);
                }
            }
            var retired: publog.Decision = undefined;
            {
                var txn = try index.beginWriteTxn();
                errdefer txn.abort();
                try publog.stageRetire(a, &txn, binding, &.{});
                try txn.commit();
                var read = try index.beginReadTxn();
                defer read.abort();
                retired = (try publog.decision(&read, binding)).?;
            }
            pins.retire();
            lease.deinit();
            {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                try std.testing.expect(try claims.retire(a, &txn, ref, retired, pins.drained().?));
                try std.testing.expectEqualDeep(claims.Budget{}, try claims.Budget.load(&txn));
                try std.testing.expectError(error.NotFound, txn.getPhysical(&blob));
                try txn.commit();
            }
        }
    }
}

test "index source maps authenticate bounded pages and allocation cleanup" {
    const maps = @import("index_source_map.zig");
    const Fixture = struct {
        fn run(a: A) !void {
            const binding: publog.Binding = .{ .owner = @splat(31), .publication = @splat(32) };
            var refs: [65]claims.Reference = undefined;
            for (&refs, 0..) |*ref, i| ref.* = .{
                .binding = binding,
                .document = @splat(@intCast(i + 1)),
                .revision = .{ .domain = .mutation, .value = i + 1 },
                .root = @splat(33),
                .snapshot = @splat(@intCast(i + 1)),
            };
            const plan = try maps.Plan.prepare(a, binding, @splat(34), &refs);
            defer plan.deinit(a);
            try std.testing.expectEqual(@as(usize, 2), plan.pageCount());
            // A single root plus one bounded page is all a random lookup needs.
            const Reader = struct {
                plan: maps.Plan,
                calls: usize = 0,
                corrupt: bool = false,
                scratch: [38 + 64 * 105]u8 = undefined,
                pub fn get(self: *@This(), key: []const u8) ![]const u8 {
                    self.calls += 1;
                    if (std.mem.startsWith(u8, key, "index_source_map_v1:")) {
                        @memcpy(self.scratch[0..self.plan.manifest.len], self.plan.manifest);
                        return self.scratch[0..self.plan.manifest.len];
                    }
                    if (!std.mem.startsWith(u8, key, "index_source_map_page_v1:")) return error.NotFound;
                    const page = std.mem.readInt(u32, key[key.len - 4 ..][0..4], .big);
                    if (page >= self.plan.pageCount()) return error.NotFound;
                    const bytes = try self.plan.pageInto(page, &self.scratch);
                    if (self.corrupt) self.scratch[10] ^= 1;
                    return self.scratch[0..bytes.len];
                }
            };
            var reader: Reader = .{ .plan = plan };
            for ([_]u32{ 0, 63, 64 }) |ordinal| {
                reader.calls = 0;
                try std.testing.expectEqualDeep(refs[ordinal], try maps.lookup(&reader, binding, plan.root(), ordinal));
                try std.testing.expectEqual(@as(usize, 2), reader.calls);
            }
            try std.testing.expectError(error.InvalidIndexSourceMap, maps.lookup(&reader, binding, plan.root(), 65));
            const view = try maps.ReadView.open(a, &reader, binding, plan.root());
            defer view.deinit(a);
            var window: [64]claims.Reference = undefined;
            reader.calls = 0;
            try view.readWindow(&reader, 0, &window);
            try std.testing.expectEqual(@as(usize, 1), reader.calls);
            try std.testing.expectEqualDeep(refs[0..64].*, window);
            reader.calls = 0;
            try view.readWindow(&reader, 63, window[0..2]);
            try std.testing.expectEqual(@as(usize, 2), reader.calls);
            try std.testing.expectEqualDeep(refs[63], window[0]);
            try std.testing.expectEqualDeep(refs[64], window[1]);
            try std.testing.expectError(error.InvalidIndexSourceMap, view.readWindow(&reader, 65, window[0..1]));
            // Changing a borrowed source coordinate after preparation must
            // fail instead of publishing pages under the original root.
            refs[0].revision.value += 1;
            try std.testing.expectError(error.IndexPublicationConflict, plan.pageInto(0, &reader.scratch));
            refs[0].revision.value -= 1;
            reader.corrupt = true;
            try std.testing.expectError(error.InvalidIndexSourceMap, view.readWindow(&reader, 0, window[0..1]));
            try std.testing.expectError(error.InvalidIndexSourceMap, maps.lookup(&reader, binding, plan.root(), 1));
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "index source map reclamation resumes its atomic durable frontier" {
    const a = std.testing.allocator;
    const maps = @import("index_source_map.zig");
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const binding: publog.Binding = .{ .owner = @splat(41), .publication = @splat(42) };
    var refs: [65]claims.Reference = undefined;
    for (&refs, 0..) |*ref, i| ref.* = .{ .binding = binding, .document = @splat(@intCast(i + 1)), .revision = .{ .domain = .mutation, .value = i + 1 }, .root = @splat(43), .snapshot = @splat(@intCast(i + 1)) };
    const plan = try maps.Plan.prepare(a, binding, @splat(44), &refs);
    defer plan.deinit(a);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try plan.stage(&txn);
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const progress = try maps.reclaimPages(&txn, binding, plan.root(), 1);
        try std.testing.expectEqualDeep(maps.ReclaimProgress{ .next_page = 1, .done = false }, progress);
        // Aborting both deletes and progress preserves the entire old map.
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqualDeep(refs[0], try maps.lookup(&read, binding, plan.root(), 0));
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqualDeep(maps.ReclaimProgress{ .next_page = 1, .done = false }, try maps.reclaimPages(&txn, binding, plan.root(), 1));
        try txn.commit();
    }
    {
        // A fresh transaction resumes after the committed page, without any
        // caller cursor that could skip unreclaimed pages or regress progress.
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqualDeep(maps.ReclaimProgress{ .next_page = 2, .done = true }, try maps.reclaimPages(&txn, binding, plan.root(), 1));
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect((try maps.reclaimPages(&txn, binding, plan.root(), 1)).done);
        try txn.commit();
    }
    var read = try store.beginReadTxn();
    defer read.abort();
    var cursor = try read.openCursor();
    defer cursor.close();
    var entry = try cursor.first();
    while (entry) |item| : (entry = try cursor.next()) try std.testing.expect(!std.mem.startsWith(u8, item.key, "index_source_map"));
}

test "index source reader wake ownership follows the reopened maintenance owner" {
    const Context = struct {
        refs: usize = 0,
        wakes: usize = 0,
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs += 1;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.refs != 0);
            self.refs -= 1;
        }
        fn notify(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.wakes += 1;
        }
        fn port(self: *@This()) publog.SharedReaders.WakePort {
            return .{ .context = self, .retain = retain, .release = release, .notify = notify };
        }
    };
    var old: Context = .{};
    var current: Context = .{};
    var handle = try publog.SharedReaders.open(std.testing.allocator, .{ .owner = @splat(71), .publication = @splat(72) });
    var handle_open = true;
    defer if (handle_open) handle.deinit();
    handle.watch(old.port());
    try std.testing.expectEqual(@as(usize, 1), old.refs);
    try std.testing.expectEqual(@as(usize, 1), old.wakes);
    handle.watch(old.port());
    try std.testing.expectEqual(@as(usize, 1), old.wakes);
    var registration = try handle.register();
    var registration_open = true;
    defer if (registration_open) registration.deinit();
    var query = (try registration.lease()).?;
    var query_open = true;
    defer if (query_open) query.deinit();
    handle.retire();
    handle.watch(current.port());
    try std.testing.expectEqual(@as(usize, 0), old.refs);
    try std.testing.expectEqual(@as(usize, 1), current.refs);
    registration.deinit();
    registration_open = false;
    try std.testing.expectEqual(@as(usize, 0), current.wakes);
    query.deinit();
    query_open = false;
    try std.testing.expectEqual(@as(usize, 1), current.wakes);
    try std.testing.expect(handle.drained() != null);
    handle.deinit();
    handle_open = false;
    try std.testing.expectEqual(@as(usize, 0), current.refs);
}

test "index source claims project exact immutable fields after metadata patches and primary deletion" {
    const a = std.testing.allocator;
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    store.document_payload_prototype = true;
    const encoded = try schema.serializeSchema(a, .{});
    defer a.free(encoded);
    try store.put(schema.schema_key, encoded);
    const physical = try keys.documentKeyAlloc(a, "tree");
    defer a.free(physical);
    const body = try a.alloc(u8, 128 * 1024);
    defer a.free(body);
    @memset(body, 'x');
    const document = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"a.b\":{{\"x\":1}},\"a\":{{\"b\":{{\"x\":2,\"list\":[1,2]}}}},\"empty\":{{}},\"meta\":0,\"rows\":[{{\"x\":1,\"y\":true}},{{\"x\":2}},{{\"y\":false}}]}}", .{body});
    defer a.free(document);
    try store.put(physical, document);
    try store.put(physical, document); // stable values qualify the prototype
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const revision_key = mutation.keyForPhysical(physical).?;
        try txn.delete(&@import("document_projection.zig").keyForRevision(revision_key));
        try txn.delete(&@import("source_value_identity.zig").keyForRevision(revision_key));
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.patchMetadata(physical, &.{ .{ .name = "meta", .value_json = "7" }, .{ .name = "added", .value_json = "true" } });
        try txn.commit();
    }
    const observed = try observe(&store, physical);
    const binding: publog.Binding = .{ .owner = @splat(31), .publication = @splat(32) };
    var ref: claims.Reference = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .txn = &txn };
        ref = try claims.prepareCurrent(a, &writer, binding, physical, observed.revision, observed.root, .{});
        try std.testing.expectEqual(@as(usize, 0), writer.blob_reads);
        try std.testing.expect(try publog.stagePublish(a, &txn, binding, @splat(9), &.{1}));
        try txn.commit();
    }
    const indexed_document = try store.get(a, physical);
    defer a.free(indexed_document);
    // Remove both the latest source and its disposable navigation metadata.
    // Only the claimed immutable manifest may answer the following requests.
    try store.delete(physical);
    var read = try store.beginReadTxn();
    defer read.abort();
    const Reader = struct {
        txn: *ds.DocStore.Txn,
        snapshot_reads: usize = 0,
        allow_values: bool = false,
        pub fn getPhysical(self: *@This(), key: []const u8) anyerror![]const u8 {
            if (std.mem.startsWith(u8, key, "\x00\x00__metadata__:index_source_snapshot_v1:")) self.snapshot_reads += 1;
            if (keys.isPrimaryDocumentKey(key) or (!self.allow_values and std.mem.startsWith(u8, key, payload.ownership.value_prefix)) or std.mem.startsWith(u8, key, "\x00\x00__metadata__:document_projection_v1:") or std.mem.startsWith(u8, key, "\x00\x00__metadata__:source_value_identity_v1:")) return error.UnexpectedSourceRead;
            return self.txn.getPhysical(key);
        }
    };
    var reader: Reader = .{ .txn = &read };
    const decision = (try publog.decision(&read, binding)).?;
    var pins: publog.ReaderPins = .{ .binding = binding };
    var lease = try pins.acquire();
    defer lease.deinit();
    const paths = [_][]const []const u8{ &.{ "a.b", "x" }, &.{ "a", "b", "x" }, &.{"meta"}, &.{"meta"}, &.{ "a", "b", "missing" }, &.{ "a", "b", "list", "0" }, &.{ "empty", "missing" }, &.{ "meta", "x" }, &.{"added"} };
    const Case = struct {
        fn run(alloc: A, source: *Reader, reference: claims.Reference, committed: publog.Decision, held: *const publog.ReaderPins.Lease, requested: []const []const []const u8) !void {
            const result = (try claims.readProjected(alloc, source, reference, committed, held, requested, .{ .bytes = 16 })).?;
            defer result.deinit(alloc);
            try std.testing.expectEqualStrings("1", result.values[0].json.?);
            try std.testing.expectEqualStrings("2", result.values[1].json.?);
            try std.testing.expectEqual(@as(usize, 3), result.values[1].covered);
            try std.testing.expectEqualStrings("7", result.values[2].json.?);
            try std.testing.expectEqual(result.values[2].json.?.ptr, result.values[3].json.?.ptr);
            try std.testing.expect(result.values[4].json == null);
            try std.testing.expectEqual(@as(usize, 2), result.values[4].covered);
            try std.testing.expectEqual(@as(usize, 1), result.values[6].covered);
            try std.testing.expectEqual(@as(usize, 0), result.values[7].covered);
            try std.testing.expectEqualStrings("[1,2]", result.values[5].json.?);
            try std.testing.expectEqual(@as(usize, 3), result.values[5].covered);
            try std.testing.expect(result.values[6].json == null);
            try std.testing.expect(result.values[7].json == null);
            try std.testing.expectEqualStrings("true", result.values[8].json.?);
            try std.testing.expectEqual(@as(usize, 12), result.data.len);
            try std.testing.expectEqualSlices(u8, &try @import("document_commitment.zig").compute(alloc, "7"), &result.values[2].canonical.?);
        }
    };
    try Case.run(a, &reader, ref, decision, &lease, &paths);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Case.run, .{ &reader, ref, decision, &lease, &paths });
    const parent_json = "{\"x\":2,\"list\":[1,2]}";
    const overlapping = (try claims.readProjected(a, &reader, ref, decision, &lease, &.{ &.{ "a", "b", "x" }, &.{ "a", "b" }, &.{ "a", "b", "list" } }, .{ .bytes = parent_json.len })).?;
    defer overlapping.deinit(a);
    try std.testing.expectEqualStrings(parent_json, overlapping.values[1].json.?);
    try std.testing.expectEqualStrings("2", overlapping.values[0].json.?);
    try std.testing.expectEqual(parent_json.len, overlapping.data.len);
    try std.testing.expect(@intFromPtr(overlapping.values[0].json.?.ptr) >= @intFromPtr(overlapping.data.ptr) and @intFromPtr(overlapping.values[0].json.?.ptr) < @intFromPtr(overlapping.data.ptr) + overlapping.data.len);
    try std.testing.expectError(error.IndexSourceProjectionFull, claims.readProjected(a, &reader, ref, decision, &lease, &.{&.{"body"}}, .{ .bytes = 16 }));
    {
        // The real native transaction exposes a disposable direct-read scope.
        // Its decoder scratch may close while the returned selected bytes live.
        const selected_body = (try claims.readProjected(a, &read, ref, decision, &lease, &.{&.{"body"}}, .{})).?;
        defer selected_body.deinit(a);
        try std.testing.expectEqual(body.len + 2, selected_body.values[0].json.?.len);
        try std.testing.expectEqualSlices(u8, body, selected_body.values[0].json.?[1 .. body.len + 1]);
    }

    {
        // Immutable scalar certificates answer reference-only delivery without
        // touching any value blob, even after current-row deletion.
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var session = try claims.openRead(scratch, &reader, ref, decision, &lease);
        defer session.deinit();
        const protocol = @import("../api/source_projection.zig");
        const prepared = try protocol.Prepared.init(scratch, .{ .fields = &.{.{ .field = "body", .mode = .identity }} });
        const conditional = (try @import("db/indexed_conditional.zig").load(a, scratch, &session, "tree", .{ .fields = &.{"body"}, .include_all_fields = false }, prepared)).?;
        defer a.free(conditional.source);
        defer a.free(conditional.identities);
        try std.testing.expectEqualStrings("{}", conditional.source);
        var identities = try std.json.parseFromSlice(std.json.Value, a, conditional.identities, .{});
        defer identities.deinit();
        const identity = identities.value.object.get("body").?.object;
        try std.testing.expectEqualStrings("reference", identity.get("state").?.string);
        const expected = try protocol.identity(.{ .string = body });
        try std.testing.expectEqualStrings(&expected, identity.get("identity").?.string);
        const matching = try protocol.Prepared.init(scratch, .{ .fields = &.{.{ .field = "body", .if_match = &.{.{ .id = "tree", .identity = &expected }} }} });
        const unchanged = (try @import("db/indexed_conditional.zig").load(a, scratch, &session, "tree", .{ .fields = &.{"body"}, .include_all_fields = false }, matching)).?;
        defer a.free(unchanged.source);
        defer a.free(unchanged.identities);
        try std.testing.expectEqualStrings("{}", unchanged.source);
        try std.testing.expect(std.mem.indexOf(u8, unchanged.identities, "unchanged") != null);
    }

    {
        reader.allow_values = true;
        defer reader.allow_values = false;
        const before = reader.snapshot_reads;
        var local = try lease.clone();
        defer local.deinit();
        var session = try claims.openRead(a, &reader, ref, decision, &local);
        defer session.deinit();
        try std.testing.expectError(error.IndexSourceProjectionFull, session.readProjected(a, &.{&.{"body"}}, .{ .bytes = 16 }));
        const predicate = try session.readLogicalPaths(a, &.{ &.{ "rows", "1", "x" }, &.{ "rows", "x" }, &.{ "a", "b", "x" }, &.{ "a", "b" } }, .{});
        defer a.free(predicate.bytes);
        try std.testing.expect(predicate.projected);
        var selected_json = try std.json.parseFromSlice(std.json.Value, a, predicate.bytes, .{});
        defer selected_json.deinit();
        const rows_value = selected_json.value.object.get("rows").?;
        try std.testing.expectEqual(@as(usize, 3), rows_value.array.items.len);
        try std.testing.expectEqual(@as(i64, 1), rows_value.array.items[0].object.get("x").?.integer);
        try std.testing.expectEqual(@as(i64, 2), rows_value.array.items[1].object.get("x").?.integer);
        try std.testing.expect(rows_value.array.items[2].object.get("x") == null);
        try std.testing.expect(!rows_value.array.items[2].object.get("y").?.bool);
        try std.testing.expect(selected_json.value.object.get("body") == null);
        const full = try session.read(a);
        defer full.deinit(a);
        try std.testing.expectEqualStrings(indexed_document, full.bytes);
        try std.testing.expectEqual(before + 1, reader.snapshot_reads);
        local.deinit();
        try std.testing.expectError(error.IndexSourceLeaseClosed, session.read(a));
        try std.testing.expectError(error.IndexSourceLeaseClosed, session.readProjected(a, &.{&.{"meta"}}, .{}));
    }

    const Captured = struct {
        reference: claims.Reference,
        committed: publog.Decision,
        held: *const publog.ReaderPins.Lease,
        pub fn openRead(self: @This(), alloc: A, primary: anytype) !claims.ReadSession(@TypeOf(primary)) {
            return claims.openRead(alloc, primary, self.reference, self.committed, self.held);
        }
        pub fn readProjected(self: @This(), alloc: A, primary: anytype, requested: []const []const []const u8, limits: claims.ProjectionLimits) !?claims.ProjectedSource {
            return claims.readProjected(alloc, primary, self.reference, self.committed, self.held, requested, limits);
        }
    };
    const captured: Captured = .{ .reference = ref, .committed = decision, .held = &lease };
    const query = @import("db/document_query.zig");
    const requests = [_][]const []const u8{
        &.{"a.b.x"},                        &.{"a.b.missing"},          &.{"empty.missing"},      &.{"meta.x"},
        &.{"a.b.list.1"},                   &.{"rows.x"},               &.{ "rows.1", "rows.x" }, &.{ "a", "a.b.x" },
        &.{ "a.b.x", "a" },                 &.{ "rows.x", "rows.1.y" }, &.{ "meta", "-meta" },    &.{"missing"},
        &.{ "a.b.x", "rows.0.y", "added" }, &.{"-meta"},                &.{"*"},
    };
    const QueryCase = struct {
        fn run(alloc: A, source: Captured, primary: *Reader, full: []const u8, fields: []const []const u8) !void {
            const before = primary.snapshot_reads;
            const previous = primary.allow_values;
            primary.allow_values = fields.len == 1 and (std.mem.eql(u8, fields[0], "-meta") or std.mem.eql(u8, fields[0], "*"));
            defer primary.allow_values = previous;
            const options: @import("db/types.zig").LookupOptions = .{ .fields = fields, .include_all_fields = false };
            var plan = (try query.SpanProjectionPlan.init(alloc, options)).?;
            defer plan.deinit();
            const expected = try query.lookupJsonWithOutput(alloc, alloc, full, options);
            defer alloc.free(expected.json);
            const actual = try query.lookupClaimedSourceWithPlan(alloc, alloc, source, primary, &plan, false);
            defer alloc.free(actual.json);
            try std.testing.expectEqualStrings(expected.json, actual.json);
            try std.testing.expectEqual(before + 1, primary.snapshot_reads);
        }
    };
    for (requests) |fields| try QueryCase.run(a, captured, &reader, indexed_document, fields);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, QueryCase.run, .{ captured, &reader, indexed_document, requests[requests.len - 1] });
}

test "index source claims owner registries isolate quiescence and retain rendezvous through reopened handles" {
    const Case = struct {
        fn run(a: A) !void {
            const binding: publog.Binding = .{ .owner = @splat(121), .publication = @splat(122) };
            var registry = try publog.SharedReaders.OwnerRegistry.open(a, binding.owner);
            defer registry.deinit();
            var other = try publog.SharedReaders.OwnerRegistry.open(a, @splat(123));
            defer other.deinit();
            try std.testing.expectError(error.IndexSourceOwnerMismatch, other.openPublication(a, binding));
            var handle = try registry.openPublication(a, binding);
            defer handle.deinit();
            var registration = try handle.register();
            defer registration.deinit();
            var query = (try registration.lease()).?;
            defer query.deinit();
            var retained = try registry.clone();
            defer retained.deinit();
            registry.deinit();
            // A new default lookup joins the retained originating owner, rather
            // than admitting an independent cell with no knowledge of queries.
            var reopened = try publog.SharedReaders.open(a, binding);
            defer reopened.deinit();
            try std.testing.expect(handle.cell == reopened.cell);
            try std.testing.expect(retained.hasPins());
            try std.testing.expect(!other.hasPins());
            handle.retire();
            var child = try query.clone();
            defer child.deinit();
            registration.deinit();
            query.deinit();
            try std.testing.expect(retained.hasPins());
            child.deinit();
            try std.testing.expect(!retained.hasPins());
            try std.testing.expect(reopened.drained() != null);
        }
    };
    try Case.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "index source map preparation retains only the directory at legacy document capacity" {
    const a = std.testing.allocator;
    const maps = @import("index_source_map.zig");
    const binding: publog.Binding = .{ .owner = @splat(161), .publication = @splat(162) };
    const refs = try a.alloc(claims.Reference, maps.legacy_maximum_documents);
    defer a.free(refs);
    for (refs, 0..) |*ref, i| {
        ref.* = .{ .binding = binding, .document = @splat(1), .revision = .{ .domain = .mutation, .value = i + 1 }, .root = @splat(163), .snapshot = @splat(164) };
        std.mem.writeInt(u64, ref.document[0..8], i + 1, .little);
    }
    // An entire map must fit in its directory allocation. Retaining encoded
    // pages would need another ~6.6 MiB and fails this allocator immediately.
    var directory_storage: [74 + maps.legacy_maximum_documents / maps.page_capacity * 32]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&directory_storage);
    const plan = try maps.Plan.prepare(fixed.allocator(), binding, @splat(165), refs);
    defer plan.deinit(fixed.allocator());
    try std.testing.expectEqual(@as(usize, 1024), plan.pageCount());
    var scratch: [maps.maximum_page_bytes]u8 = undefined;
    const last = try plan.pageInto(1023, &scratch);
    try std.testing.expectEqual(@as(usize, maps.maximum_page_bytes), last.len);
    refs[refs.len - 1].snapshot[0] ^= 1;
    try std.testing.expectError(error.IndexPublicationConflict, plan.pageInto(1023, &scratch));
}
test "index source large maps use fixed preparation memory and bounded authenticated read and reclaim windows" {
    const a = std.testing.allocator;
    const maps = @import("index_source_map.zig");
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const binding: publog.Binding = .{ .owner = @splat(171), .publication = @splat(172) };
    const refs = try a.alloc(claims.Reference, maps.legacy_maximum_documents + 1);
    defer a.free(refs);
    for (refs, 0..) |*ref, i| {
        ref.* = .{ .binding = binding, .document = @splat(1), .revision = .{ .domain = .mutation, .value = i + 1 }, .root = @splat(173), .snapshot = @splat(174) };
        std.mem.writeInt(u64, ref.document[0..8], i + 1, .little);
    }
    var storage: [104]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    const plan = try maps.Plan.prepare(fixed.allocator(), binding, @splat(175), refs);
    defer plan.deinit(fixed.allocator());
    try std.testing.expectEqual(@as(usize, 104), plan.manifest.len);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try plan.stage(&txn);
        try txn.commit();
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        const Reader = struct {
            txn: *ds.DocStore.Txn,
            calls: usize = 0,
            pub fn get(self: *@This(), address: []const u8) anyerror![]const u8 {
                self.calls += 1;
                return self.txn.get(address);
            }
        };
        var reader: Reader = .{ .txn = &txn };
        var empty: [0]u8 = .{};
        var empty_allocator = std.heap.FixedBufferAllocator.init(&empty);
        const view = try maps.ReadView.open(empty_allocator.allocator(), &reader, binding, plan.root());
        defer view.deinit(empty_allocator.allocator());
        try std.testing.expectEqual(@as(usize, 1), reader.calls);
        try std.testing.expectEqual(@as(usize, 0), view.pages.len);
        for ([_]u32{ 0, 8191, 65535, 65536 }) |ordinal| {
            reader.calls = 0;
            var result: [1]claims.Reference = undefined;
            try view.readWindow(&reader, ordinal, &result);
            try std.testing.expectEqualDeep(refs[ordinal], result[0]);
            try std.testing.expectEqual(@as(usize, 3), reader.calls);
            try std.testing.expectEqualDeep(refs[ordinal], try maps.lookup(&reader, binding, plan.root(), ordinal));
        }
        var boundary: [2]claims.Reference = undefined;
        reader.calls = 0;
        try view.readWindow(&reader, 65535, &boundary);
        try std.testing.expectEqualDeep(refs[65535], boundary[0]);
        try std.testing.expectEqualDeep(refs[65536], boundary[1]);
        try std.testing.expectEqual(@as(usize, 6), reader.calls);
        var compact: [2]claims.SnapshotEntry = undefined;
        reader.calls = 0;
        try view.readCompactWindow(&reader, 65535, &compact);
        try std.testing.expectEqualDeep(refs[65535], compact[0].reference(binding));
        try std.testing.expectEqualDeep(refs[65536], compact[1].reference(binding));
        try std.testing.expectEqual(@as(usize, 6), reader.calls);
    }
    refs[refs.len - 1].revision.value += 1;
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.IndexPublicationConflict, plan.stage(&txn));
        try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
        // None of this failed publication's rewritten pages may commit.
    }
    refs[refs.len - 1].revision.value -= 1;
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const progress = try maps.reclaimPages(&txn, binding, plan.root(), 128);
        try std.testing.expect(!progress.done);
        try std.testing.expectEqual(@as(u32, 128), progress.next_page);
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectEqualDeep(refs[0], try maps.lookup(&txn, binding, plan.root(), 0));
        try std.testing.expectEqualDeep(refs[65536], try maps.lookup(&txn, binding, plan.root(), 65536));
    }
    var previous: u32 = 0;
    while (true) {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const progress = try maps.reclaimPages(&txn, binding, plan.root(), 128);
        try std.testing.expect(progress.next_page > previous);
        try std.testing.expect(progress.next_page - previous <= 128);
        try txn.commit();
        previous = progress.next_page;
        if (progress.done) break;
    }
    var read = try store.beginReadTxn();
    defer read.abort();
    try std.testing.expect(!try maps.hasRoot(&read, binding));
    var cursor = try read.openCursor();
    defer cursor.close();
    var entry = try cursor.first();
    while (entry) |item| : (entry = try cursor.next()) try std.testing.expect(!std.mem.startsWith(u8, item.key, "index_source_"));
}

test "index source oversized current recipes are rejected before allocation" {
    const a = std.testing.allocator;
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const physical = try keys.documentKeyAlloc(a, "oversized");
    defer a.free(physical);
    const oversized = try a.alloc(u8, 16 * 1024 * 1024 + 1);
    defer a.free(oversized);
    var txn = try store.beginWriteTxn();
    defer txn.abort();
    var measured: Writer = .{ .txn = &txn, .raw_override = .{ .key = physical, .bytes = oversized } };
    try std.testing.expectError(error.IndexSourceClaimsFull, claims.prepareCurrentUnique(std.testing.failing_allocator, &measured, .{ .owner = @splat(117), .publication = @splat(118) }, physical, 1, @splat(1), .{}));
    try std.testing.expectEqual(@as(usize, 0), measured.schema_reads);
    try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
}

test "index source preparation amortizes admission without relaxing per-document checks" {
    const a = std.testing.allocator;
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const encoded = try schema.serializeSchema(a, .{});
    defer a.free(encoded);
    try store.put(schema.schema_key, encoded);
    const physical = try keys.documentKeyAlloc(a, "cohort");
    defer a.free(physical);
    try store.put(physical, "{\"body\":\"original\"}");
    const observed = try observe(&store, physical);
    const binding: publog.Binding = .{ .owner = @splat(119), .publication = @splat(120) };
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var measured: Writer = .{ .txn = &txn };
        var preparation = claims.Preparation(*Writer).init(&measured);
        _ = try claims.prepareCurrentUnique(a, &preparation, binding, physical, observed.revision, observed.root, .{});
        _ = try claims.prepareCurrentUnique(a, &preparation, .{ .owner = binding.owner, .publication = @splat(121) }, physical, observed.revision, observed.root, .{});
        try std.testing.expectEqual(@as(usize, 1), measured.schema_reads);
        try std.testing.expectEqual(@as(u64, 2), (try claims.Budget.load(&txn)).claims);
        // The cached admission never accepts a duplicate or an altered read
        // set. Any partial cohort remains uncommittable after such a failure.
        try std.testing.expectError(error.InvalidIndexSourceMap, claims.prepareCurrentUnique(a, &preparation, binding, physical, observed.revision, observed.root, .{}));
        try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.IndexSourceClaimsCorrupt, claims.Budget.load(&txn));
        var measured: Writer = .{ .txn = &txn };
        var preparation = claims.Preparation(*Writer).init(&measured);
        _ = try claims.prepareCurrentUnique(a, &preparation, binding, physical, observed.revision, observed.root, .{});
        try std.testing.expectEqual(@as(usize, 1), measured.schema_reads);
        try std.testing.expectError(error.PreparedReadSetChanged, claims.prepareCurrentUnique(a, &preparation, .{ .owner = binding.owner, .publication = @splat(122) }, physical, observed.revision + 1, observed.root, .{}));
        try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var measured: Writer = .{ .txn = &txn };
        var preparation = claims.PreparationWithMinimum(*Writer, 28).init(&measured);
        _ = try claims.prepareCurrentUnique(a, &preparation, binding, physical, observed.revision, observed.root, .{});
        try std.testing.expectEqual(@as(u32, 28), try schema.serializedReaderFormat(try txn.getPhysical(schema.schema_key)));
        try std.testing.expectEqual(@as(usize, 1), measured.schema_reads);
        // A same-transaction schema change invalidates cached admission too.
        // In particular, an older fence cannot accompany committed claims.
        try preparation.put(schema.schema_key, encoded);
        try std.testing.expectError(error.IndexSourceClaimsCorrupt, claims.prepareCurrentUnique(a, &preparation, .{ .owner = binding.owner, .publication = @splat(123) }, physical, observed.revision, observed.root, .{}));
        try std.testing.expectEqual(@as(usize, 2), measured.schema_reads);
        try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
    }
}

test "index source admission initializes independently of the shared reader fence" {
    const a = std.testing.allocator;
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    var index_backend = lsm.Backend.init(a, .{});
    defer index_backend.close();
    var index = try ds.DocStore.openRuntime(a, try index_backend.runtimeStore(a, .{}));
    defer index.close();
    // Another feature (or a logical import) may have preserved this reader
    // fence without ever creating indexed-source ownership state.
    const encoded = try schema.serializeSchemaWithMinimumFormat(a, .{}, 27);
    defer a.free(encoded);
    try store.put(schema.schema_key, encoded);
    store.document_payload_prototype = true;
    const physical = try keys.documentKeyAlloc(a, "admitted");
    defer a.free(physical);
    const body = try a.alloc(u8, payload.minimum_value_bytes);
    defer a.free(body);
    @memset(body, 'x');
    const first = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"meta\":0}}", .{body});
    defer a.free(first);
    const second = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"meta\":1}}", .{body});
    defer a.free(second);
    try store.put(physical, first);
    try store.put(physical, second);
    const observed = try observe(&store, physical);
    const binding: publog.Binding = .{ .owner = @splat(111), .publication = @splat(112) };
    var reference: claims.Reference = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectError(error.IndexSourceClaimsCorrupt, claims.Budget.load(&txn));
        reference = try claims.prepareCurrent(a, &txn, binding, physical, observed.revision, observed.root, .{});
        try std.testing.expectEqual(@as(u64, 1), (try claims.Budget.load(&txn)).claims);
        try txn.commit();
    }
    store.document_payload_prototype = false;
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(payload.isManifest(try read.getPhysical(physical)));
        try std.testing.expectEqualStrings(second, try read.get(physical));
        // Logical cursors hide ownership metadata even without a configured
        // payload reader. Keys-only traversal does not need source values.
        var cursor = try read.openCursor();
        defer cursor.close();
        cursor.setKeysOnly();
        var seen = false;
        var entry = try cursor.first();
        while (entry) |item| : (entry = try cursor.next()) {
            try std.testing.expect(claims.privatePrefix(item.key) == null);
            try std.testing.expect(payload.ownership.privatePrefix(item.key) == null);
            seen = seen or std.mem.eql(u8, item.key, physical);
        }
        try std.testing.expect(seen);
        entry = try cursor.last();
        while (entry) |item| : (entry = try cursor.prev()) {
            try std.testing.expect(claims.privatePrefix(item.key) == null);
            try std.testing.expect(payload.ownership.privatePrefix(item.key) == null);
        }
        var values = try read.openCursor();
        defer values.close();
        // Reopening the admitted native floor attaches a source reader even
        // after the prototype hint is cleared. Export the logical afterimage,
        // never the physical manifest descriptor.
        const logical = (try values.seekAtOrAfter(physical)).?;
        try std.testing.expectEqualStrings(physical, logical.key);
        try std.testing.expectEqualStrings(second, logical.value);
    }
    const cancellation = try cancelled(a, &index, binding);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try claims.cancel(a, &txn, reference, cancellation));
        try std.testing.expectEqualDeep(claims.Budget{}, try claims.Budget.load(&txn));
        try txn.commit();
    }
    // Even after all claims/recipes are gone, losing an initialized budget
    // cannot silently turn the feature into a new, empty ownership domain.
    try store.delete("\x00\x00__metadata__:index_source_budget_v1");
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.IndexSourceClaimsCorrupt, claims.prepareCurrent(a, &txn, .{ .owner = binding.owner, .publication = @splat(113) }, physical, observed.revision, observed.root, .{}));
        try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
    }
}

test "index source admission preserves legacy budgets and rejects orphaned records" {
    const a = std.testing.allocator;
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const encoded = try schema.serializeSchemaWithMinimumFormat(a, .{}, 27);
    defer a.free(encoded);
    try store.put(schema.schema_key, encoded);
    const physical = try keys.documentKeyAlloc(a, "legacy-initialization");
    defer a.free(physical);
    try store.put(physical, "{\"body\":\"original\"}");
    const observed = try observe(&store, physical);
    const binding: publog.Binding = .{ .owner = @splat(121), .publication = @splat(122) };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        _ = try claims.prepareCurrent(a, &txn, binding, physical, observed.revision, observed.root, .{});
        try txn.commit();
    }
    // Existing experimental budgets predate the initialization witness. Their
    // counters remain authoritative and must not be replaced with zeros.
    try store.delete("\x00\x00__metadata__:index_source_initialized_v1");
    {
        var batch = try store.beginWriteBatch();
        errdefer batch.abort();
        const txn = batch.asTxn();
        _ = try claims.prepareCurrent(a, txn, .{ .owner = binding.owner, .publication = @splat(123) }, physical, observed.revision, observed.root, .{});
        try std.testing.expectEqual(@as(u64, 2), (try claims.Budget.load(txn)).claims);
        try batch.commit();
    }
    try store.delete("\x00\x00__metadata__:index_source_budget_v1");
    {
        var batch = try store.beginWriteBatch();
        defer batch.abort();
        const txn = batch.asTxn();
        // Prefix probes use the physical transactional key view, including
        // pending/committed private records that logical cursors hide.
        try std.testing.expectError(error.IndexSourceClaimsCorrupt, claims.prepareCurrent(a, txn, .{ .owner = binding.owner, .publication = @splat(124) }, physical, observed.revision, observed.root, .{}));
        try std.testing.expectError(error.RetainedEffectsTransactionFailed, batch.commit());
    }
}

test "index source observations pin producer bytes across concurrent replacement and deletion" {
    const a = std.testing.allocator;
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    store.document_payload_prototype = true;
    const encoded = try schema.serializeSchema(a, .{});
    defer a.free(encoded);
    try store.put(schema.schema_key, encoded);
    const physical = try keys.documentKeyAlloc(a, "observed");
    defer a.free(physical);
    const body = try a.alloc(u8, 128 * 1024);
    defer a.free(body);
    @memset(body, 'x');
    const original = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"meta\":1}}", .{body});
    defer a.free(original);
    try store.put(physical, original);
    try store.put(physical, original);
    var cut = try store.beginReadTxn();
    defer cut.abort();
    try std.testing.expectError(error.PreparedReadSetChanged, claims.ObservedSource.capture(a, &cut, physical, "{}"));
    var observed = try claims.ObservedSource.capture(a, &cut, physical, original);
    defer observed.deinit(a);
    const owned = (try claims.ObservedSource.readSortedOwned(a, &cut, &.{physical})).?;
    defer {
        for (owned) |*source| source.deinit(a);
        a.free(owned);
    }
    try std.testing.expectEqualStrings(original, owned[0].produced);
    try std.testing.expectEqualSlices(u8, &observed.root, &owned[0].root);
    try std.testing.expectEqual(observed.mutation, owned[0].mutation);

    var probe = try store.beginProbeTxn();
    defer probe.abort();
    try std.testing.expectError(error.IndexSourceReadSnapshotRequired, claims.ObservedSource.readSortedOwned(a, &probe, &.{physical}));
    try std.testing.expectError(error.IndexSourceReadSnapshotRequired, claims.ObservedSource.capture(a, &probe, physical, original));
    try store.put(physical, "{\"body\":\"replacement\"}");
    try store.delete(physical);
    try std.testing.expectEqualStrings(original, owned[0].produced);
    try owned[0].requireProduced(owned[0].produced);
    const binding: publog.Binding = .{ .owner = @splat(75), .publication = @splat(76) };
    var reference: claims.Reference = undefined;
    {
        var writer = try store.beginWriteTxn();
        errdefer writer.abort();
        reference = try claims.prepareObserved(a, &writer, binding, &observed, .{}, true);
        _ = try publog.stagePublish(a, &writer, binding, @splat(77), &.{1});
        try writer.commit();
    }
    var read = try store.beginReadTxn();
    defer read.abort();
    const decision = (try publog.decision(&read, binding)).?;
    var pins: publog.ReaderPins = .{ .binding = binding };
    var lease = try pins.acquire();
    defer lease.deinit();
    const loaded = try claims.read(a, &read, reference, decision, &lease);
    defer loaded.deinit(a);
    try std.testing.expectEqualStrings(original, loaded.bytes);
    const guarded: claims.LeasedSourceReader(*ds.DocStore.Txn) = .{ .reader = &probe, .binding = binding, .lease = &lease };
    try std.testing.expectError(error.CurrentPrimarySourceReadForbidden, guarded.getPhysical(physical));
    {
        var session = try claims.openRead(a, guarded, reference, decision, &lease);
        defer session.deinit();
        const exact = try session.read(a);
        defer exact.deinit(a);
        try std.testing.expectEqualStrings(original, exact.bytes);
    }
    lease.deinit();
    const claim_key = reference.key();
    try std.testing.expectError(error.IndexSourceLeaseClosed, guarded.getPhysical(&claim_key));
}

test "index source observations close canonical text over a native owned snapshot" {
    const a = std.testing.allocator;
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    store.document_payload_prototype = true;
    const encoded = try schema.serializeSchemaWithMinimumFormat(a, .{}, 29);
    defer a.free(encoded);
    try store.put(schema.schema_key, encoded);
    const physical = try keys.documentKeyAlloc(a, "native-text");
    defer a.free(physical);
    const body = try a.alloc(u8, 128 * 1024);
    defer a.free(body);
    const fragment = "native code\n\t\"quoted\" café \\ source ";
    for (body, 0..) |*byte, i| byte.* = fragment[i % fragment.len];
    @memset(body[body.len - 4 ..], ' ');
    const body_json = try std.json.Stringify.valueAlloc(a, body, .{});
    defer a.free(body_json);
    const value = try std.fmt.allocPrint(a, "{{\"body\":{s},\"meta\":1}}", .{body_json});
    defer a.free(value);
    try store.put(physical, value);
    try store.put(physical, value);
    var cut = try store.beginReadTxn();
    defer cut.abort();
    const owned = (try claims.ObservedSource.readSortedOwned(a, &cut, &.{physical})).?;
    defer {
        for (owned) |*source| source.deinit(a);
        a.free(owned);
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scope = (try @import("text_value_identity.zig").Scope.capture(arena.allocator(), &owned[0], owned[0].produced)) orelse return error.NativeTextIdentityUnavailable;
    const text = scope.root().object.get("body").?.string;
    try std.testing.expectEqualStrings(body, text);
    const identity = scope.bind("body", text).?;
    try std.testing.expect(identity.digest(text) != null);
    try std.testing.expect(identity.digest(body) == null);
    try std.testing.expect(scope.bind("meta", text) == null);
    try std.testing.expectError(error.PreparedReadSetChanged, @import("text_value_identity.zig").Scope.capture(arena.allocator(), &owned[0], value));
    const AllocationCase = struct {
        fn run(alloc: A, reader: *ds.DocStore.Txn, key: []const u8, expected: []const u8) !void {
            const observations = (try claims.ObservedSource.readSortedOwned(alloc, reader, &.{key})).?;
            defer {
                for (observations) |*source| source.deinit(alloc);
                alloc.free(observations);
            }
            try std.testing.expectEqualStrings(expected, observations[0].produced);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, AllocationCase.run, .{ &cut, physical, value });
}
