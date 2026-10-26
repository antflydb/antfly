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
const recovery = @import("index_source_recovery.zig");
const claims = @import("index_source_claim.zig");
const persistent = @import("persistent.zig");
const ds = @import("docstore.zig");
const lsm = @import("lsm_backend.zig");
const schema = @import("schema.zig");
const keys = @import("internal_keys.zig");
const mutation = @import("document_mutation_revision.zig");
const payload = @import("document_payload.zig");
const intro = @import("../introducer.zig");
const A = std.mem.Allocator;
fn observe(store: *ds.DocStore, physical: []const u8) !recovery.Input {
    var read = try store.beginReadTxn();
    defer read.abort();
    const revision = try mutation.load(&read, mutation.keyForPhysical(physical).?);
    const bytes = try read.getPhysical(physical);
    var root: [32]u8 = undefined;
    if (payload.isManifest(bytes)) root = payload.publishedCommitment(bytes) else std.crypto.hash.sha2.Sha256.hash(bytes, &root, .{});
    return .{ .physical = physical, .root = root, .from = .{ .current = revision } };
}
fn initialize(a: A, store: *ds.DocStore) !void {
    const encoded = try schema.serializeSchema(a, .{});
    defer a.free(encoded);
    try store.put(schema.schema_key, encoded);
    store.document_payload_prototype = true;
}
fn segment(a: A, docs: []const intro.TextDocument) ![]u8 {
    return intro.buildSegmentFromTextWithAnalysisOptions(a, docs, &@import("../search/analysis.zig").default_analyzer, .{}, .{ .store_document_source = false });
}

const FaultPrimary = struct {
    store: *ds.DocStore,
    remaining: usize,
    snapshot_reads: usize = 0,
    blob_reads: usize = 0,
    forced_syncs: usize = 0,
    const Txn = struct {
        inner: ds.DocStore.Txn,
        fault: *FaultPrimary,
        fn change(self: *@This()) !void {
            if (self.fault.remaining == 0) return error.OutOfMemory;
            self.fault.remaining -= 1;
        }
        pub fn get(self: *@This(), name: []const u8) anyerror![]const u8 {
            return self.inner.get(name);
        }
        pub fn hasPrefix(self: *@This(), prefix: []const u8) anyerror!bool {
            return self.inner.hasPrefix(prefix);
        }
        pub fn getPhysical(self: *@This(), name: []const u8) anyerror![]const u8 {
            if (std.mem.startsWith(u8, name, "\x00\x00__metadata__:index_source_snapshot_v1:")) self.fault.snapshot_reads += 1;
            if (std.mem.startsWith(u8, name, payload.ownership.value_prefix)) self.fault.blob_reads += 1;
            return self.inner.getPhysical(name);
        }
        pub fn put(self: *@This(), name: []const u8, value: []const u8) anyerror!void {
            try self.change();
            try self.inner.put(name, value);
        }
        pub fn delete(self: *@This(), name: []const u8) anyerror!void {
            try self.change();
            try self.inner.delete(name);
        }
        pub fn poison(self: *@This()) void {
            self.inner.poison();
        }
        pub fn abort(self: *@This()) void {
            self.inner.abort();
        }
        pub fn commit(self: *@This()) !void {
            try self.inner.commit();
        }
        pub fn openPhysicalCursorAdapter(self: *@This()) !ds.DocStore.Txn.CursorAdapter {
            return self.inner.openPhysicalCursorAdapter();
        }
    };
    pub fn sync(self: *@This(), force: bool) !void {
        if (force) self.forced_syncs += 1;
        try self.store.sync(force);
    }
    pub fn beginReadTxn(self: *@This()) !ds.DocStore.Txn {
        return self.store.beginReadTxn();
    }
    pub fn beginWriteTxn(self: *@This()) !Txn {
        return .{ .inner = try self.store.beginWriteTxn(), .fault = self };
    }
};

test "persistent source publication recovery cancels abandoned durable preparations in bounded windows" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const primary_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source-primary", .{temporary.sub_path});
    defer a.free(primary_path);
    const index_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/source-index", .{temporary.sub_path}, 0);
    defer a.free(index_path);
    const binding: claims.publication.Binding = .{ .owner = @splat(31), .publication = @splat(32) };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    var docs: [9]intro.TextDocument = undefined;
    var inputs: [9]recovery.Input = undefined;
    for (&docs, 0..) |*doc, i| doc.* = .{ .id = try std.fmt.allocPrint(owned, "doc{d}", .{i}), .stored_data = "{\"body\":\"hello\"}", .text_fields = &.{.{ .field_name = "body", .text = "hello" }} };
    const bytes = try segment(a, &docs);
    defer a.free(bytes);
    for (0..2) |pass| {
        var backend = try lsm.Backend.open(a, primary_path, .{});
        defer backend.close();
        var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        store.document_payload_prototype = true;
        var index = try persistent.PersistentIndex.open(a, .{ .path = index_path });
        defer index.close();
        if (pass == 0) {
            try initialize(a, &store);
            for (&inputs, &docs) |*input, doc| {
                const physical = try keys.documentKeyAlloc(owned, doc.id);
                try store.put(physical, doc.stored_data);
                input.* = try observe(&store, physical);
            }
            var prepared = try recovery.prepare(a, &store, &index, binding, &inputs, bytes, .{});
            defer prepared.deinit(a);
            try std.testing.expectEqual(@as(u64, docs.len), (try budget(&store)).claims);
            // No index receipt exists at this crash cut. Only the primary
            // claims and recovery job have crossed their physical barrier.
            try std.testing.expect(try index.loadDurableSourcePublication(binding) == null);
            try std.testing.expect((try recovery.recoverBinding(a, &store, &index, binding, 2)).waiting_publisher);
            try std.testing.expect(try index.loadDurableSourcePublication(binding) == null);
        } else {
            const before = try budget(&store);
            var fault: FaultPrimary = .{ .store = &store, .remaining = 2 };
            try std.testing.expectError(error.OutOfMemory, recovery.recoverBinding(a, &fault, &index, binding, 2));
            try std.testing.expectEqualDeep(before, try budget(&store));
            {
                var read = try store.beginReadTxn();
                defer read.abort();
                const job = (try recovery.load(&read, binding)).?;
                try std.testing.expectEqual(@as(u32, 0), job.processed);
                try std.testing.expect(job.after == null);
                var addresses = try claims.addressWindow(a, &read, binding, null, 128);
                defer addresses.deinit(a);
                try std.testing.expectEqual(@as(usize, docs.len), addresses.items.len);
                // Logical scans must not expose the durable recovery journal.
                var logical = try read.openCursor();
                defer logical.close();
                var row = try logical.first();
                while (row) |entry| : (row = try logical.next()) try std.testing.expect(!std.mem.startsWith(u8, entry.key, recovery.prefix));
            }
            try (try index.loadDurableSourcePublication(binding)).?.require(binding, .cancelled);
            var rounds: usize = 0;
            while (rounds < 10) : (rounds += 1) {
                const progress = try recovery.recoverBinding(a, &store, &index, binding, 2);
                if (progress.finished) break;
                try std.testing.expect(progress.worked);
            }
            try std.testing.expect(rounds >= 4 and rounds < 10);
            try std.testing.expectEqualDeep(claims.Budget{}, try budget(&store));
            try (try index.loadDurableSourcePublication(binding)).?.require(binding, .cancelled);
            try std.testing.expectError(error.IndexPublicationCancelled, index.indexSegmentOwnedWithSources(try a.dupe(u8, bytes), binding));
            var sweep: recovery.Sweep = .{};
            try std.testing.expect(!(try sweep.step(a, &store, &index, binding.owner, 2)).scan_more);
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expect(try recovery.load(&read, binding) == null);
            try std.testing.expect((try recovery.recoverBinding(a, &store, &index, binding, 2)).finished);
        }
    }
}
fn budget(store: *ds.DocStore) !claims.Budget {
    var read = try store.beginReadTxn();
    defer read.abort();
    return claims.Budget.load(&read);
}

test "persistent source publication recovery resumes acknowledgment after reopen and waits for held queries" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const primary_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source-primary", .{temporary.sub_path});
    defer a.free(primary_path);
    const index_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/source-index", .{temporary.sub_path}, 0);
    defer a.free(index_path);
    const binding: claims.publication.Binding = .{ .owner = @splat(41), .publication = @splat(42) };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    const body = try owned.alloc(u8, payload.minimum_value_bytes);
    @memset(body, 'x');
    const original = try std.fmt.allocPrint(owned, "{{\"body\":\"{s}\",\"metadata\":1}}", .{body});
    const next = try std.fmt.allocPrint(owned, "{{\"body\":\"{s}\",\"metadata\":2}}", .{body});
    var docs: [5]intro.TextDocument = undefined;
    var inputs: [5]recovery.Input = undefined;
    for (&docs, 0..) |*doc, i| doc.* = .{ .id = try std.fmt.allocPrint(owned, "doc{d}", .{i}), .stored_data = next, .text_fields = &.{.{ .field_name = "body", .text = "hello" }} };
    const bytes = try segment(a, &docs);
    defer a.free(bytes);
    for (0..3) |pass| {
        var backend = try lsm.Backend.open(a, primary_path, .{});
        defer backend.close();
        var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        store.document_payload_prototype = true;
        var index = try persistent.PersistentIndex.open(a, .{ .path = index_path });
        defer index.close();
        if (pass == 0) {
            try initialize(a, &store);
            for (&inputs, &docs) |*input, doc| {
                const physical = try keys.documentKeyAlloc(owned, doc.id);
                try store.put(physical, original);
                try store.put(physical, next);
                input.* = try observe(&store, physical);
            }
            var prepared = try recovery.prepare(a, &store, &index, binding, &inputs, bytes, .{});
            defer prepared.deinit(a);
            const decision = try recovery.publishOwned(a, &store, &index, &prepared, try a.dupe(u8, bytes));
            try decision.require(binding, .published);
            // Recovery must not need the latest primary source after restart.
            try store.put(inputs[0].physical, "{\"body\":\"new body\"}");
        } else if (pass == 1) {
            var measured: FaultPrimary = .{ .store = &store, .remaining = std.math.maxInt(usize) };
            const progress = (try index.runSourceOwnershipMaintenanceStep(&measured, 2)).?.progress;
            try std.testing.expectEqual(@as(usize, 0), measured.snapshot_reads);
            try std.testing.expectEqual(@as(usize, 0), measured.blob_reads);
            try std.testing.expect(progress.worked and !progress.finished);
            var read = try store.beginReadTxn();
            defer read.abort();
            const job = (try recovery.load(&read, binding)).?;
            try std.testing.expectEqual(@as(u32, 2), job.processed);
            try std.testing.expect(job.after != null);
        } else {
            var turns: usize = 0;
            while (index.sourceOwnershipMaintenancePending() and turns < 20) : (turns += 1) _ = try index.runSourceOwnershipMaintenanceStep(&store, 2);
            try std.testing.expect(turns < 20);
            try std.testing.expect(!index.sourceOwnershipMaintenancePending());
            try std.testing.expect(try index.runSourceOwnershipMaintenanceStep(&store, 2) == null);
            {
                var read = try store.beginReadTxn();
                defer read.abort();
                try std.testing.expectEqual(recovery.Phase.dormant, (try recovery.load(&read, binding)).?.phase);
            }
            var sweep: recovery.Sweep = .{};
            try std.testing.expect((try sweep.step(a, &store, &index, binding.owner, 2)).scan_more);
            try std.testing.expect(!(try sweep.step(a, &store, &index, binding.owner, 2)).scan_more);
            var held: ?*@import("../index.zig").IndexSnapshot = index.acquireSnapshot();
            defer if (held) |snapshot| snapshot.release();
            var captured = (try index.captureSource(&held.?.segments[0], 0)).?;
            var captured_open = true;
            defer if (captured_open) captured.deinit();
            {
                var read = try store.beginReadTxn();
                defer read.abort();
                const source = try captured.read(a, &read);
                defer source.deinit(a);
                try std.testing.expectEqualStrings(next, source.bytes);
            }
            try index.removeSegments(&.{held.?.segments[0].id});
            try (try index.loadDurableSourcePublication(binding)).?.require(binding, .retired);
            try std.testing.expect(index.sourceOwnershipMaintenancePending());
            const blocked = (try index.runSourceOwnershipMaintenanceStep(&store, 2)).?.progress;
            try std.testing.expect(blocked.waiting_readers);
            for (0..3) |_| _ = try index.runSourceOwnershipMaintenanceStep(&store, 2);
            try std.testing.expect(!index.sourceOwnershipMaintenancePending());
            held.?.release();
            held = null;
            // The remaining captured query keeps ownership live. Its final
            // release signals the owner without polling the dormant journal.
            try std.testing.expect(!index.sourceOwnershipMaintenancePending());
            captured.deinit();
            captured_open = false;
            try std.testing.expect(index.sourceOwnershipMaintenancePending());
            turns = 0;
            while (index.sourceOwnershipMaintenancePending() and turns < 20) : (turns += 1) _ = try index.runSourceOwnershipMaintenanceStep(&store, 2);
            try std.testing.expect(turns < 20);
            try std.testing.expect(!index.sourceOwnershipMaintenancePending());
            try std.testing.expectEqualDeep(claims.Budget{}, try budget(&store));
            try (try index.loadDurableSourcePublication(binding)).?.require(binding, .retired);
        }
    }
}

test "index source recovery preparation rolls back every allocation failure" {
    const Fixture = struct {
        const Index = struct {
            pub fn reserveSourceOwner(_: *@This(), _: [32]u8) !void {}
            pub fn noteSourceRecoveryWork(_: *@This()) void {}
            pub fn beginSourcePublication(_: *@This(), binding: claims.publication.Binding) !claims.publication.SharedReaders.PublisherRegistration {
                var shared = try claims.publication.SharedReaders.open(std.testing.allocator, binding);
                defer shared.deinit();
                return shared.prepare(0);
            }
            pub fn sourcePublicationState(_: *@This(), _: claims.publication.Binding) !?claims.publication.State {
                return null;
            }
        };
        fn run(a: A) !void {
            const backing = std.testing.allocator;
            var backend = lsm.Backend.init(backing, .{});
            defer backend.close();
            var store = try ds.DocStore.openRuntime(backing, try backend.runtimeStore(backing, .{}));
            defer store.close();
            try initialize(backing, &store);
            const physical = try keys.documentKeyAlloc(backing, "doc");
            defer backing.free(physical);
            try store.put(physical, "{\"body\":\"hello\"}");
            const input = try observe(&store, physical);
            var index: Index = .{};
            const binding: claims.publication.Binding = .{ .owner = @splat(51), .publication = @splat(52) };
            var prepared = recovery.prepare(a, &store, &index, binding, (&input)[0..1], "bounded synthetic segment identity", .{}) catch |err| {
                var read = try store.beginReadTxn();
                defer read.abort();
                try std.testing.expect(try recovery.load(&read, binding) == null);
                // A failed metadata admission cannot strand ASCH27 or charges.
                const decoded = try schema.deserializeSchema(backing, try read.getPhysical(schema.schema_key));
                defer schema.freeSchema(backing, decoded);
                try std.testing.expectEqual(schema.storage_format_version, decoded.decoded_format_version);
                return err;
            };
            defer prepared.deinit(a);
            try std.testing.expectEqual(@as(u64, 1), (try budget(&store)).claims);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "index source recovery DB maintenance owns publication and reader wakeups" {
    const a = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source-db", .{temporary.sub_path});
    defer a.free(path);
    var db = try db_mod.DB.open(a, path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(a, "{}");
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    db.core.store.document_payload_prototype = true;
    const physical = try keys.documentKeyAlloc(a, "doc");
    defer a.free(physical);
    try db.core.store.put(physical, "{\"body\":\"hello\"}");
    const input = try observe(db.core.store, physical);
    const bytes = try segment(a, &.{.{ .id = "doc", .stored_data = "{\"body\":\"hello\"}", .text_fields = &.{.{ .field_name = "body", .text = "hello" }} }});
    defer a.free(bytes);
    const index = &db.core.index_manager.text_indexes.items[0].persistent;
    const binding: claims.publication.Binding = .{ .owner = @splat(81), .publication = @splat(82) };
    var prepared = try recovery.prepare(a, db.core.store, index, binding, (&input)[0..1], bytes, .{});
    var prepared_open = true;
    defer if (prepared_open) prepared.deinit(a);
    try db.runArtifactRepairMetadataMaintenanceUntilIdle();
    try std.testing.expect(try index.loadDurableSourcePublication(binding) == null);
    _ = try recovery.publishOwned(a, db.core.store, index, &prepared, try a.dupe(u8, bytes));
    try db.runArtifactRepairMetadataMaintenanceUntilIdle();
    {
        var reader = try db.core.store.beginReadTxn();
        defer reader.abort();
        try std.testing.expectEqual(recovery.Phase.dormant, (try recovery.load(&reader, binding)).?.phase);
    }
    var held: ?*@import("../index.zig").IndexSnapshot = index.acquireSnapshot();
    defer if (held) |snapshot| snapshot.release();
    try index.removeSegments(&.{held.?.segments[0].id});
    try db.runArtifactRepairMetadataMaintenanceUntilIdle();
    try std.testing.expectEqual(@as(u64, 1), (try budget(db.core.store)).claims);
    try std.testing.expect(!index.sourceOwnershipMaintenancePending());
    held.?.release();
    held = null;
    // The preparation itself also pins this publication until its owner
    // releases it, independently of source/query readers.
    prepared.deinit(a);
    prepared_open = false;
    try std.testing.expect(index.sourceOwnershipMaintenancePending());
    try db.runArtifactRepairMetadataMaintenanceUntilIdle();
    try std.testing.expectEqualDeep(claims.Budget{}, try budget(db.core.store));
}

test "persistent source publication batches more than one receipt capacity of atomic outputs" {
    const a = std.testing.allocator;
    const count = claims.publication.maximum_segments + 1;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/many-source-outputs", .{temporary.sub_path}, 0);
    defer a.free(path);
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    try initialize(a, &store);
    var index = try persistent.PersistentIndex.open(a, .{ .path = path });
    defer index.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    var docs: [count]intro.TextDocument = undefined;
    var inputs: [count]recovery.Input = undefined;
    var bodies: [count][]u8 = undefined;
    for (&docs, &inputs, 0..) |*doc, *input, ordinal| {
        const id = try std.fmt.allocPrint(temp, "doc-{d:0>3}", .{ordinal});
        const json = try std.fmt.allocPrint(temp, "{{\"body\":\"needle\",\"ordinal\":{d}}}", .{ordinal});
        doc.* = .{ .id = id, .stored_data = json, .text_fields = &.{.{ .field_name = "body", .text = "needle" }} };
        const physical = try keys.documentKeyAlloc(temp, id);
        try store.put(physical, json);
        input.* = try observe(&store, physical);
        bodies[ordinal] = try segment(temp, (&doc.*)[0..1]);
    }
    const original = try segment(a, &docs);
    defer a.free(original);
    const owner: claims.Digest = @splat(233);
    {
        var prepared = try recovery.prepareFresh(a, &store, &index, owner, &inputs, original, .{}, std.testing.io);
        defer prepared.deinit(a);
        _ = try recovery.publishOwned(a, &store, &index, &prepared, try a.dupe(u8, original));
    }
    var held: ?*@import("../index.zig").IndexSnapshot = index.acquireSnapshot();
    defer if (held) |snapshot| snapshot.release();
    const previous = held.?.segments[0].id;
    var captured = (try index.captureSourceReferences(a, &held.?.segments[0])).?;
    var capture_open = true;
    defer if (capture_open) captured.deinit();
    var references: [count]claims.Reference = undefined;
    var first: u32 = 0;
    while (first < references.len) {
        const end = @min(first + 64, references.len);
        try captured.readWindow(&index, first, references[first..end]);
        first = @intCast(end);
    }
    var rows: [count][1]recovery.Input = undefined;
    var cohorts: [count][]const recovery.Input = undefined;
    for (&rows, &cohorts, references) |*row, *cohort, reference| {
        row[0] = .{ .root = reference.root, .from = .{ .published = .{ .reference = reference, .decision = captured.decision, .lease = &captured.lease.inner } } };
        cohort.* = row;
    }
    const outputs = try index.prepareSourceSegmentCopies(a, &bodies);
    var outputs_owned = true;
    defer if (outputs_owned) index.discardPreparedMergeSegments(outputs);
    var primary: FaultPrimary = .{ .store = &store, .remaining = std.math.maxInt(usize) };
    try std.testing.expectError(error.IndexSourceClaimsFull, recovery.prepareFreshSegments(std.testing.failing_allocator, &primary, &index, owner, &cohorts, outputs, .{ .claims = count - 1 }, std.testing.io));
    try std.testing.expectEqual(@as(usize, 0), primary.forced_syncs);
    // Fail after the first output's claim and journal have been staged. The
    // entire primary cohort must roll back, leaving the old index intact.
    primary.remaining = 7;
    try std.testing.expectError(error.OutOfMemory, recovery.prepareFreshSegments(a, &primary, &index, owner, &cohorts, outputs, .{}, std.testing.io));
    try std.testing.expectEqual(@as(usize, 0), primary.forced_syncs);
    try std.testing.expectEqual(@as(u64, count), (try budget(&store)).claims);
    try std.testing.expectEqual(@as(usize, 1), index.snapshot().segments.len);
    try std.testing.expectEqual(previous, index.snapshot().segments[0].id);
    primary.remaining = std.math.maxInt(usize);
    const preparations = try recovery.prepareFreshSegments(a, &primary, &index, owner, &cohorts, outputs, .{}, std.testing.io);
    var preparations_open = true;
    defer if (preparations_open) {
        for (preparations) |*item| item.deinit(a);
        a.free(preparations);
    };
    try std.testing.expectEqual(@as(usize, 1), primary.forced_syncs);
    try std.testing.expectEqual(@as(u64, count * 2), (try budget(&store)).claims);
    // Current rows are irrelevant to structural handoff and later hydration.
    try store.delete(inputs[0].physical);
    try store.put(inputs[count - 1].physical, "{\"body\":\"changed\"}");
    outputs_owned = false;
    try std.testing.expect(try recovery.replacePrepared(a, &primary, &index, preparations, &.{previous}, outputs, @as(?[]const ?@import("../encoding/roaring.zig").RoaringBitmap, null)));
    try std.testing.expectEqual(@as(usize, 2), primary.forced_syncs);
    try std.testing.expectEqual(@as(usize, count), index.snapshot().segments.len);
    for (preparations) |*item| {
        try (try index.loadDurableSourcePublication(item.job.binding)).?.require(item.job.binding, .published);
        item.deinit(a);
    }
    a.free(preparations);
    preparations_open = false;
    captured.deinit();
    capture_open = false;
    held.?.release();
    held = null;
    var turns: usize = 0;
    while (index.sourceOwnershipMaintenancePending()) : (turns += 1) {
        if (turns == 1024) return error.OwnershipMaintenanceDidNotQuiesce;
        _ = try index.runSourceOwnershipMaintenanceStep(&store, 128);
    }
    try std.testing.expectEqual(@as(u64, count), (try budget(&store)).claims);
    index.close();
    index = try persistent.PersistentIndex.open(a, .{ .path = path });
    try std.testing.expectEqual(@as(usize, count), index.snapshot().segments.len);
    for ([_]usize{ 0, count - 1 }) |ordinal| {
        var source = (try index.captureSource(&index.snapshot().segments[ordinal], 0)).?;
        defer source.deinit();
        var read = try store.beginReadTxn();
        defer read.abort();
        const exact = try source.read(a, &read);
        defer exact.deinit(a);
        try std.testing.expectEqualStrings(docs[ordinal].stored_data, exact.bytes);
    }
}

fn drainOwnership(store: *ds.DocStore, index: *persistent.PersistentIndex) !void {
    var turns: usize = 0;
    while (index.sourceOwnershipMaintenancePending()) : (turns += 1) {
        if (turns == 128) return error.OwnershipMaintenanceDidNotQuiesce;
        _ = try index.runSourceOwnershipMaintenanceStep(store, 2);
    }
}

test "persistent source publication structural handoff preserves source cuts across replacement retry and reopen" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const primary_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/handoff-primary", .{temporary.sub_path});
    defer a.free(primary_path);
    const index_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/handoff-index", .{temporary.sub_path}, 0);
    defer a.free(index_path);
    var backend = try lsm.Backend.open(a, primary_path, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    try initialize(a, &store);
    var index = try persistent.PersistentIndex.open(a, .{ .path = index_path });
    defer index.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const body = try temp.alloc(u8, payload.minimum_value_bytes);
    @memset(body, 'x');
    const expected = try std.fmt.allocPrint(temp, "{{\"body\":\"{s}\",\"metadata\":1}}", .{body});
    const docs = [_]intro.TextDocument{
        .{ .id = "a", .stored_data = expected, .text_fields = &.{.{ .field_name = "body", .text = "old" }} },
        .{ .id = "b", .stored_data = expected, .text_fields = &.{.{ .field_name = "body", .text = "old" }} },
    };
    const owner: claims.Digest = @splat(91);
    var previous_bindings: [2]claims.publication.Binding = undefined;
    for (docs, &previous_bindings, 0..) |doc, *binding, i| {
        binding.* = .{ .owner = owner, .publication = @splat(@as(u8, @intCast(92 + i))) };
        const physical = try keys.documentKeyAlloc(temp, doc.id);
        try store.put(physical, expected);
        try store.put(physical, expected);
        const input = try observe(&store, physical);
        const bytes = try segment(a, (&doc)[0..1]);
        defer a.free(bytes);
        var prepared = try recovery.prepare(a, &store, &index, binding.*, (&input)[0..1], bytes, .{});
        defer prepared.deinit(a);
        _ = try recovery.publishOwned(a, &store, &index, &prepared, try a.dupe(u8, bytes));
    }
    var held: ?*@import("../index.zig").IndexSnapshot = index.acquireSnapshot();
    defer if (held) |snapshot| snapshot.release();
    var captures: [2]persistent.PersistentIndex.CapturedSource = undefined;
    var captures_open: usize = 0;
    defer for (captures[0..captures_open]) |*capture| capture.deinit();
    var previous: [2]u64 = undefined;
    var inputs: [2]recovery.Input = undefined;
    for (&captures, &previous, &inputs, held.?.segments) |*capture, *id, *input, *source| {
        capture.* = (try index.captureSource(source, 0)).?;
        captures_open += 1;
        id.* = source.id;
        const physical = try keys.documentKeyAlloc(temp, (try source.reader.storedIdAlloc(temp, 0)).?);
        input.* = .{ .physical = physical, .root = capture.reference.root, .from = .{ .published = .{ .reference = capture.reference, .decision = capture.decision, .lease = &capture.lease.inner } } };
        // The current row and its disposable directory cannot supply this cut.
        try store.delete(physical);
    }
    const bytes = try segment(a, &docs);
    defer a.free(bytes);
    const binding: claims.publication.Binding = .{ .owner = owner, .publication = @splat(94) };
    var primary: FaultPrimary = .{ .store = &store, .remaining = std.math.maxInt(usize) };
    var prepared = try recovery.prepare(a, &primary, &index, binding, &inputs, bytes, .{});
    var preparation_open = true;
    defer if (preparation_open) prepared.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), primary.blob_reads);
    try std.testing.expectEqual(@as(u64, 4), (try budget(&store)).claims);
    const before = try budget(&store);
    {
        const invalid_binding: claims.publication.Binding = .{ .owner = owner, .publication = @splat(95) };
        const reversed = [_]intro.TextDocument{ docs[1], docs[0] };
        const invalid_bytes = try segment(a, &reversed);
        defer a.free(invalid_bytes);
        var invalid = try recovery.prepare(a, &store, &index, invalid_binding, &inputs, invalid_bytes, .{});
        var invalid_open = true;
        defer if (invalid_open) invalid.deinit(a);
        try std.testing.expectError(error.InvalidIndexSourceMap, recovery.replaceOwned(a, &store, &index, &invalid, &previous, try a.dupe(u8, invalid_bytes)));
        try std.testing.expectEqual(@as(usize, 2), index.snapshot().segments.len);
        for (previous_bindings) |old| try (try index.loadDurableSourcePublication(old)).?.require(old, .published);
        try std.testing.expect(try index.loadDurableSourcePublication(invalid_binding) == null);
        invalid.deinit(a);
        invalid_open = false;
        try drainOwnership(&store, &index);
        try (try index.loadDurableSourcePublication(invalid_binding)).?.require(invalid_binding, .cancelled);
        try std.testing.expectEqual(before.claims, (try budget(&store)).claims);
    }
    // Transient preparation buffers have an independent allocator lifetime;
    // the published mapping and snapshot must retain no pointer into it.
    const published = blk: {
        var buffers = std.heap.ArenaAllocator.init(a);
        defer buffers.deinit();
        const buffer_owner = buffers.allocator();
        break :blk (try recovery.replaceOwned(buffer_owner, &store, &index, &prepared, &previous, try buffer_owner.dupe(u8, bytes))).?;
    };
    var next_id = index.snapshot().segments[0].id;
    try std.testing.expectEqual(@as(usize, 1), index.snapshot().segments.len);
    try std.testing.expectEqualDeep(published, (try recovery.replaceOwned(a, &store, &index, &prepared, &previous, try a.dupe(u8, bytes))).?);
    try std.testing.expectEqual(next_id, index.snapshot().segments[0].id);
    try std.testing.expectEqualDeep(before, try budget(&store));
    for (previous_bindings) |old| try (try index.loadDurableSourcePublication(old)).?.require(old, .retired);
    {
        const outputs = try index.prepareMergedSegmentToFileWithAllocator(a, index.snapshot(), &.{0});
        var outputs_owned = true;
        defer if (outputs_owned) index.discardPreparedMergeSegments(outputs);
        try std.testing.expectEqual(@as(usize, 1), outputs.len);
        const file_binding: claims.publication.Binding = .{ .owner = owner, .publication = @splat(97) };
        var file_preparation = try recovery.prepareSource(a, &store, &index, file_binding, &inputs, outputs[0].data.source(), .{});
        defer file_preparation.deinit(a);
        outputs_owned = false;
        try std.testing.expect(try recovery.replacePrepared(a, &store, &index, (&file_preparation)[0..1], &.{next_id}, outputs, @as(?[]const ?@import("../encoding/roaring.zig").RoaringBitmap, null)));
        try (try index.loadDurableSourcePublication(binding)).?.require(binding, .retired);
        try (try index.loadDurableSourcePublication(file_binding)).?.require(file_binding, .published);
        next_id = index.snapshot().segments[0].id;
        // Failed native retries must not unlink an already published artifact,
        // even when validation rejects the request before reading its receipt.
        for (0..2) |attempt| {
            const retry = try a.alloc(persistent.PreparedMergeSegment, 1);
            retry[0] = .{
                .id = next_id,
                .data = @import("../index.zig").SegmentData.fromOwnedHeap(try a.dupe(u8, bytes)),
                .key_range = .{ .seg_id = next_id, .min_doc_key = &.{}, .max_doc_key = &.{} },
                .staging_alloc = a,
            };
            const root: persistent.PersistentIndex.SourceRoot = .{
                .binding = file_binding,
                .references = file_preparation.references,
                .stamp = file_preparation.source_stamp.?,
            };
            try std.testing.expectError(if (attempt == 0) error.InvalidIndexSourceMap else error.IndexPublicationConflict, index.replaceSegmentsIfActiveManyPreparedWithSourceRoots(
                a,
                &.{next_id},
                retry,
                null,
                if (attempt == 0) &.{} else (&root)[0..1],
            ));
        }
    }
    prepared.deinit(a);
    preparation_open = false;
    try drainOwnership(&store, &index);
    try std.testing.expectEqual(@as(u64, 4), (try budget(&store)).claims);
    index.close();
    index = try persistent.PersistentIndex.open(a, .{ .path = index_path });
    for (&captures) |*capture| {
        var read = try store.beginReadTxn();
        defer read.abort();
        const exact = try capture.read(a, &read);
        defer exact.deinit(a);
        try std.testing.expectEqualStrings(expected, exact.bytes);
    }
    {
        var current = (try index.captureSource(&index.snapshot().segments[0], 1)).?;
        defer current.deinit();
        var read = try store.beginReadTxn();
        defer read.abort();
        const exact = try current.read(a, &read);
        defer exact.deinit(a);
        try std.testing.expectEqualStrings(expected, exact.bytes);
    }
    for (&captures) |*capture| capture.deinit();
    captures_open = 0;
    held.?.release();
    held = null;
    try drainOwnership(&store, &index);
    try std.testing.expectEqual(@as(u64, 2), (try budget(&store)).claims);
    try std.testing.expectEqual(before.value_bytes, (try budget(&store)).value_bytes);
    {
        var current = (try index.captureSource(&index.snapshot().segments[0], 0)).?;
        var current_open = true;
        defer if (current_open) current.deinit();
        const stale_binding: claims.publication.Binding = .{ .owner = owner, .publication = @splat(96) };
        const input: recovery.Input = .{ .physical = inputs[0].physical, .root = current.reference.root, .from = .{ .published = .{ .reference = current.reference, .decision = current.decision, .lease = &current.lease.inner } } };
        const stale_bytes = try segment(a, docs[0..1]);
        defer a.free(stale_bytes);
        var stale = try recovery.prepare(a, &store, &index, stale_binding, (&input)[0..1], stale_bytes, .{});
        var stale_open = true;
        defer if (stale_open) stale.deinit(a);
        try index.removeSegments(&.{next_id});
        try std.testing.expect(try recovery.replaceOwned(a, &store, &index, &stale, &.{next_id}, try a.dupe(u8, stale_bytes)) == null);
        try std.testing.expect((try recovery.recoverBinding(a, &store, &index, stale_binding, 2)).waiting_publisher);
        stale.deinit(a);
        stale_open = false;
        current.deinit();
        current_open = false;
    }
    try drainOwnership(&store, &index);
    try std.testing.expectEqualDeep(claims.Budget{}, try budget(&store));
}

test "persistent source publication managed compaction preserves exact roots and separates legacy inputs" {
    try exerciseManagedCompaction(false);
}
test "persistent source publication managed packed compaction keeps field generations" {
    try exerciseManagedCompaction(true);
}
fn exerciseManagedCompaction(field_mode: bool) !void {
    const a = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/managed-source-merge", .{temporary.sub_path});
    defer a.free(path);
    var db = try db_mod.DB.open(a, path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(a, "{}");
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    db.core.store.document_payload_prototype = true;
    const manager = db.core.index_manager;
    const entry = &manager.text_indexes.items[0];
    const index = &entry.persistent;
    const owner: claims.Digest = @splat(101);
    for (0..24) |i| {
        const id = try std.fmt.allocPrint(a, "doc-{d}", .{i});
        defer a.free(id);
        const physical = try keys.documentKeyAlloc(a, id);
        defer a.free(physical);
        const json = try std.fmt.allocPrint(a, "{{\"body\":\"old\",\"metadata\":{d}}}", .{i});
        defer a.free(json);
        try db.core.store.put(physical, json);
        const input = try observe(db.core.store, physical);
        const bytes = try segment(a, &.{.{ .id = id, .stored_data = json, .text_fields = &.{.{ .field_name = "body", .text = "old" }} }});
        defer a.free(bytes);
        const binding: claims.publication.Binding = .{ .owner = owner, .publication = @splat(@as(u8, @intCast(102 + i))) };
        if (field_mode) {
            const projected = [_]intro.TextDocument{.{ .id = id, .stored_data = json, .text_fields = &.{.{ .field_name = "body", .text = "old" }} }};
            var compiled = try @import("index_field_inputs.zig").prepare(a, &projected, .{}, @splat(107));
            defer compiled.deinit();
            var prepared = try index.prepareFieldSourceFamily(a, db.core.store, (&input)[0..1], bytes, @splat(107), &compiled, std.testing.io);
            defer prepared.deinit();
            _ = try prepared.publish(index, db.core.store);
        } else {
            var prepared = try recovery.prepare(a, db.core.store, index, binding, (&input)[0..1], bytes, .{});
            defer prepared.deinit(a);
            _ = try recovery.publishOwned(a, db.core.store, index, &prepared, try a.dupe(u8, bytes));
        }
        try db.core.store.delete(physical);
    }
    // A transitional legacy member must never cause a source-owned merge to
    // hydrate a current row or silently lose exact source lineage.
    try index.indexSegmentOwned(try segment(a, &.{.{ .id = "legacy", .stored_data = "{\"body\":\"legacy\"}", .text_fields = &.{.{ .field_name = "body", .text = "legacy" }} }}));
    entry.compaction_pending.store(true, .release);
    var turns: usize = 0;
    while (try manager.beginTextMergeTask()) |initial| : (turns += 1) {
        if (turns == 32) return error.ManagedSourceMergeDidNotConverge;
        var task = initial;
        defer task.deinit(a);
        for (task.merge_indices) |ordinal| try std.testing.expect(task.snapshot.segments[ordinal].hasSourceReferences());
        var result = try @TypeOf(manager.*).executeTextMergeTask(a, &task);
        defer result.deinit(a);
        try std.testing.expect(if (field_mode) result.field_preparations.len != 0 else result.source_preparations.len != 0);
        try std.testing.expect(result.prepared_segments.len != 0);
        if (field_mode) for (result.prepared_segments) |output| {
            try std.testing.expect(output.native_pack != null);
            try std.testing.expect(output.native_pack.?.transferred);
        };
        if (field_mode and turns == 0) {
            const ordinal = task.merge_indices[0];
            const source = &task.snapshot.segments[ordinal];
            try std.testing.expectEqual(@as(u32, 1), source.reader.doc_count);
            const layout = try index.sourceFamilyLayoutAt(task.snapshot, ordinal);
            var captured = (try index.captureSource(source, 0)).?;
            defer captured.deinit();
            const input: recovery.Input = .{ .root = captured.reference.root, .from = .{ .published = .{ .reference = captured.reference, .decision = captured.decision, .lease = &captured.lease.inner } } };
            const cohorts = [_][]const recovery.Input{&.{input}};
            const identities = [_][32]u8{try layout.identity(0)};
            var parent_lease = (try source.sourceLease()).?;
            defer parent_lease.deinit();
            const binding = try parent_lease.binding();
            const old_root = (try index.loadDurableSourcePublication(binding)).?.root;
            const leaves = try recovery.prepareFieldSourceLeaves(a, db.core.store, index, binding.owner, &cohorts, &identities, .{}, std.testing.io);
            defer {
                for (leaves) |*leaf| leaf.deinit(a);
                a.free(leaves);
            }
            _ = try index.replaceSourceCohort(db.core.store, source.id, 0, old_root, &leaves[0], std.testing.io);
            try std.testing.expect(!try manager.finishTextMergeTask(&task, &result));
            continue;
        }
        try std.testing.expect(try manager.finishTextMergeTask(&task, &result));
    }
    try std.testing.expect(turns > 0);
    try std.testing.expect(index.snapshot().segments.len >= 2 and index.snapshot().segments.len < 25);
    var legacy_segments: usize = 0;
    for (index.snapshot().segments) |source| if (!source.hasSourceReferences()) {
        legacy_segments += 1;
    };
    try std.testing.expectEqual(@as(usize, 1), legacy_segments);
    if (field_mode) for (index.snapshot().segments) |entry_source| {
        if (entry_source.hasSourceReferences()) try std.testing.expect(entry_source.data == .authenticated);
    };
    {
        const before_force = index.snapshot().segments.len;
        try std.testing.expect(entry.apply_mutex.tryLock());
        defer entry.apply_mutex.unlock();
        try std.testing.expectError(error.IndexApplyBusy, manager.forceCompactAllTextIndexes());
        try manager.bestEffortForceCompactAllTextIndexes();
        try std.testing.expectEqual(before_force, index.snapshot().segments.len);
    }
    try manager.forceCompactAllTextIndexes();
    try std.testing.expectEqual(@as(usize, 2), index.snapshot().segments.len);
    try db.runArtifactRepairMetadataMaintenanceUntilIdle();
    try std.testing.expectEqual(@as(u64, 24), (try budget(db.core.store)).claims);
    var exact_documents: usize = 0;
    for (index.snapshot().segments) |*source| {
        if (!source.hasSourceReferences()) continue;
        var captured = (try index.captureSource(source, 0)).?;
        defer captured.deinit();
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const exact = try captured.read(a, &read);
        defer exact.deinit(a);
        const parsed = try std.json.parseFromSlice(std.json.Value, a, exact.bytes, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings("old", parsed.value.object.get("body").?.string);
        exact_documents += source.reader.doc_count;
    }
    try std.testing.expectEqual(@as(usize, 24), exact_documents);
    // Keep the left half of a source-owned segment after all mutable rows
    // disappeared. Native ordinal provenance must carry the same afterimages.
    try manager.pruneTextSplitRange("doc-2");
    try db.runArtifactRepairMetadataMaintenanceUntilIdle();
    try std.testing.expectEqual(@as(usize, 1), index.snapshot().segments.len);
    try std.testing.expectEqual(@as(u32, 12), index.snapshot().segments[0].reader.doc_count);
    try std.testing.expectEqual(@as(u64, 12), (try budget(db.core.store)).claims);
    {
        var current = (try index.captureSource(&index.snapshot().segments[0], 0)).?;
        defer current.deinit();
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const exact = try current.read(a, &read);
        defer exact.deinit(a);
        try std.testing.expect(std.mem.indexOf(u8, exact.bytes, "old") != null);
    }
    const ids = try a.alloc(u64, index.snapshot().segments.len);
    defer a.free(ids);
    for (index.snapshot().segments, ids) |source, *id| id.* = source.id;
    try index.removeSegments(ids);
    try db.runArtifactRepairMetadataMaintenanceUntilIdle();
    try std.testing.expectEqualDeep(claims.Budget{}, try budget(db.core.store));
}

test "persistent source publication catalog cleanup preserves held queries across drop restart and recreation" {
    const a = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/dropped-source-owner", .{temporary.sub_path});
    defer a.free(path);
    const options: db_mod.OpenOptions = .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var db = try db_mod.DB.open(a, path, options);
    defer db.close();
    try db.setSchemaJson(a, "{}");
    const config: @import("db/types.zig").IndexConfig = .{ .name = "text", .kind = .full_text, .config_json = "{}" };
    try db.addIndex(config);
    db.core.store.document_payload_prototype = true;
    const index = &db.core.index_manager.text_indexes.items[0].persistent;
    const owner: claims.Digest = @splat(111);
    try index.reserveSourceOwner(owner);
    _ = try index.advanceSourcePublicationEpoch();
    var captured: [2]persistent.PersistentIndex.CapturedSource = undefined;
    var captured_count: usize = 0;
    defer for (captured[0..captured_count]) |*source| source.deinit();
    for (0..12) |i| {
        const id = try std.fmt.allocPrint(a, "drop-{d}", .{i});
        defer a.free(id);
        const physical = try keys.documentKeyAlloc(a, id);
        defer a.free(physical);
        try db.core.store.put(physical, "{\"body\":\"original\"}");
        const input = try observe(db.core.store, physical);
        const bytes = try segment(a, &.{.{ .id = id, .stored_data = "{\"body\":\"original\"}", .text_fields = &.{.{ .field_name = "body", .text = "original" }} }});
        defer a.free(bytes);
        var preparation = try recovery.prepareFresh(a, db.core.store, index, owner, (&input)[0..1], bytes, .{}, std.testing.io);
        defer preparation.deinit(a);
        _ = try recovery.publishOwned(a, db.core.store, index, &preparation, try a.dupe(u8, bytes));
        if (i < captured.len) {
            captured[i] = (try index.captureSource(&index.snapshot().segments[i], 0)).?;
            captured_count += 1;
        }
        try db.core.store.delete(physical);
    }
    const pending_key = try keys.documentKeyAlloc(a, "pending");
    defer a.free(pending_key);
    try db.core.store.put(pending_key, "{\"body\":\"pending\"}");
    const pending_input = try observe(db.core.store, pending_key);
    const pending_bytes = try segment(a, &.{.{ .id = "pending", .stored_data = "{\"body\":\"pending\"}", .text_fields = &.{.{ .field_name = "body", .text = "pending" }} }});
    defer a.free(pending_bytes);
    var pending = try recovery.prepareFresh(a, db.core.store, index, owner, (&pending_input)[0..1], pending_bytes, .{}, std.testing.io);
    var pending_open = true;
    defer if (pending_open) pending.deinit(a);
    try std.testing.expect(try db.deleteIndex("text"));
    try std.testing.expect(db.core.index_manager.get("text") == null);
    try std.testing.expectError(error.IndexArtifactCleanupPending, db.addIndex(config));
    // Cleanup must not need any segment bytes. Current source bodies are
    // already absent, and the immutable artifact directory can be unavailable.
    const artifacts = try std.fmt.allocPrint(a, "{s}/indexes/text/segments", .{path});
    defer a.free(artifacts);
    try std.Io.Dir.cwd().deleteTree(std.testing.io, artifacts);
    var turns: usize = 0;
    while (try db.advanceGeneratedArtifactCleanupPage(null) != .idle) : (turns += 1) {
        if (turns == 4096) return error.SourceCleanupDidNotWait;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(@as(u64, 3), (try budget(db.core.store)).claims);
    try std.testing.expect(!db.core.index_manager.sourceOwnershipMaintenancePending());
    // A reader wait must neither spin the deletion worker nor starve another
    // namespace whose cleanup sorts after this owner's tombstone.
    const other: @import("db/types.zig").IndexConfig = .{ .name = "zzzzzz", .kind = .full_text, .config_json = "{}" };
    try db.addIndex(other);
    try std.testing.expect(try db.deleteIndex(other.name));
    turns = 0;
    while (try db.advanceGeneratedArtifactCleanupPage(null) != .idle) : (turns += 1) {
        if (turns == 4096) return error.SourceCleanupStarvedAnotherOwner;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try db.addIndex(other);
    pending.deinit(a);
    pending_open = false;
    try std.testing.expect(db.core.index_manager.sourceOwnershipMaintenancePending());
    try db.runArtifactRepairMetadataMaintenanceUntilIdle();
    try std.testing.expectEqual(@as(u64, 2), (try budget(db.core.store)).claims);
    db.close();
    db = try db_mod.DB.open(a, path, options);
    try std.testing.expect(db.core.index_manager.get("text") == null);
    turns = 0;
    while (try db.advanceGeneratedArtifactCleanupPage(null) != .idle) : (turns += 1) {
        if (turns == 4096) return error.SourceCleanupDidNotWait;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    for (captured[0..captured_count]) |*source| {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const exact = try source.read(a, &read);
        defer exact.deinit(a);
        try std.testing.expectEqualStrings("{\"body\":\"original\"}", exact.bytes);
    }
    for (captured[0..captured_count]) |*source| source.deinit();
    captured_count = 0;
    try db.runArtifactRepairMetadataMaintenanceUntilIdle();
    turns = 0;
    while (try db.advanceGeneratedArtifactCleanupPage(null) != .idle) : (turns += 1) {
        if (turns == 4096) return error.SourceCleanupDidNotFinish;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqualDeep(claims.Budget{}, try budget(db.core.store));
    try db.addIndex(config);
    try std.testing.expect(try db.core.index_manager.text_indexes.items[0].persistent.sourceOwner() == null);
}

test "persistent source publication epochs reclaim receipts without aborting preparations or forgetting readers" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const primary_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/epoch-primary", .{temporary.sub_path});
    defer a.free(primary_path);
    const index_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/epoch-index", .{temporary.sub_path}, 0);
    defer a.free(index_path);
    var backend = try lsm.Backend.open(a, primary_path, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    try initialize(a, &store);
    var index = try persistent.PersistentIndex.open(a, .{ .path = index_path });
    defer index.close();
    const owner: claims.Digest = @splat(141);
    const first: claims.publication.Binding = .{ .owner = owner, .publication = @splat(142) };
    const second: claims.publication.Binding = .{ .owner = owner, .publication = @splat(143) };
    const key1 = try keys.documentKeyAlloc(a, "one");
    defer a.free(key1);
    const key2 = try keys.documentKeyAlloc(a, "two");
    defer a.free(key2);
    try store.put(key1, "{\"body\":\"original one\"}");
    try store.put(key2, "{\"body\":\"original two\"}");
    const input1 = try observe(&store, key1);
    const input2 = try observe(&store, key2);
    const bytes1 = try segment(a, &.{.{ .id = "one", .stored_data = "{\"body\":\"original one\"}", .text_fields = &.{.{ .field_name = "body", .text = "original one" }} }});
    defer a.free(bytes1);
    const bytes2 = try segment(a, &.{.{ .id = "two", .stored_data = "{\"body\":\"original two\"}", .text_fields = &.{.{ .field_name = "body", .text = "original two" }} }});
    defer a.free(bytes2);
    {
        var prepared = try recovery.prepare(a, &store, &index, first, (&input1)[0..1], bytes1, .{});
        defer prepared.deinit(a);
        _ = try recovery.publishOwned(a, &store, &index, &prepared, try a.dupe(u8, bytes1));
    }
    var held = (try index.captureSource(&index.snapshot().segments[0], 0)).?;
    var held_open = true;
    defer if (held_open) held.deinit();
    var pending = try recovery.prepare(a, &store, &index, second, (&input2)[0..1], bytes2, .{});
    var pending_open = true;
    defer if (pending_open) pending.deinit(a);
    const epoch1 = try index.advanceSourcePublicationEpoch();
    try std.testing.expectEqual(@as(u64, 1), epoch1.current);
    try std.testing.expectEqual(@as(u64, 0), epoch1.floor);
    try std.testing.expect(epoch1.waiting_publishers);
    try std.testing.expect((try index.reclaimSourceReceiptWindow(&store, null, 64)).waiting_publishers);
    // Births moved to epoch 1; a registered epoch-0 preparation still finishes.
    _ = try recovery.publishOwned(a, &store, &index, &pending, try a.dupe(u8, bytes2));
    pending.deinit(a);
    pending_open = false;
    try std.testing.expectEqual(@as(u32, 0), (try index.reclaimSourceReceiptWindow(&store, null, 64)).purged);
    const legacy_late: claims.publication.Binding = .{ .owner = owner, .publication = @splat(144) };
    try std.testing.expectError(error.IndexPublicationEpochExpired, index.beginSourcePublication(legacy_late));
    try store.delete(key1);
    const replacement = try index.newSourcePublicationBinding(std.testing.io);
    try std.testing.expectEqual(@as(u64, 1), @import("index_source_epoch.zig").tokenEpoch(replacement).?);
    {
        const input: recovery.Input = .{ .root = held.reference.root, .from = .{ .published = .{ .reference = held.reference, .decision = held.decision, .lease = &held.lease.inner } } };
        var prepared = try recovery.prepare(a, &store, &index, replacement, (&input)[0..1], bytes1, .{});
        defer prepared.deinit(a);
        _ = (try recovery.replaceOwned(a, &store, &index, &prepared, &.{index.snapshot().segments[0].id}, try a.dupe(u8, bytes1))).?;
    }
    for ([_]claims.publication.Binding{ first, second, replacement }) |binding| {
        var steps: usize = 0;
        while ((try recovery.recoverBinding(a, &store, &index, binding, 64)).more) : (steps += 1) {
            if (steps == 64) return error.EpochJobDidNotConverge;
        }
    }
    try std.testing.expectEqual(@as(u32, 0), (try index.reclaimSourceReceiptWindow(&store, null, 64)).purged);
    try (try index.loadDurableSourcePublication(first)).?.require(first, .retired);
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const source = try held.read(a, &read);
        defer source.deinit(a);
        try std.testing.expectEqualStrings("{\"body\":\"original one\"}", source.bytes);
    }
    held.deinit();
    held_open = false;
    for ([_]claims.publication.Binding{ first, second, replacement }) |binding| {
        var steps: usize = 0;
        while ((try recovery.recoverBinding(a, &store, &index, binding, 64)).more) : (steps += 1) {
            if (steps == 64) return error.EpochJobDidNotConverge;
        }
    }
    const MetadataPrimary = struct {
        store: *ds.DocStore,
        fail_sync: bool = false,
        syncs: usize = 0,
        const Reader = struct {
            inner: ds.DocStore.Txn,
            pub fn abort(self: *@This()) void {
                self.inner.abort();
            }
            pub fn getPhysical(self: *@This(), name: []const u8) ![]const u8 {
                if (keys.isPrimaryDocumentKey(name) or std.mem.startsWith(u8, name, payload.ownership.value_prefix) or std.mem.startsWith(u8, name, "\x00\x00__metadata__:index_source_snapshot_v1:")) return error.UnexpectedSourceRead;
                return self.inner.getPhysical(name);
            }
            pub fn openPhysicalKeyCursorAdapter(self: *@This()) !ds.DocStore.Txn.CursorAdapter {
                return self.inner.openPhysicalKeyCursorAdapter();
            }
        };
        pub fn beginReadTxn(self: *@This()) !Reader {
            return .{ .inner = try self.store.beginReadTxn() };
        }
        pub fn sync(self: *@This(), force: bool) !void {
            self.syncs += 1;
            if (self.fail_sync) return error.TestPrimarySyncFailure;
            try self.store.sync(force);
        }
    };
    var primary: MetadataPrimary = .{ .store = &store, .fail_sync = true };
    try std.testing.expectError(error.TestPrimarySyncFailure, index.reclaimSourceReceiptWindow(&primary, null, 64));
    try (try index.loadDurableSourcePublication(first)).?.require(first, .retired);
    primary.fail_sync = false;
    try std.testing.expectEqual(@as(u32, 1), (try index.reclaimSourceReceiptWindow(&primary, null, 64)).purged);
    try std.testing.expect(primary.syncs >= 2);
    try std.testing.expect(try index.sourcePublicationState(first) == null);
    try (try index.loadDurableSourcePublication(first)).?.require(first, .cancelled);
    try std.testing.expectError(error.IndexPublicationEpochExpired, index.indexSegmentOwnedWithSources(try a.dupe(u8, bytes1), first));
    index.close();
    index = try persistent.PersistentIndex.open(a, .{ .path = index_path });
    try (try index.loadDurableSourcePublication(replacement)).?.require(replacement, .published);
    try std.testing.expectError(error.IndexPublicationEpochExpired, index.beginSourcePublication(first));
    const ids = try a.alloc(u64, index.snapshot().segments.len);
    defer a.free(ids);
    for (index.snapshot().segments, ids) |entry, *id| id.* = entry.id;
    try index.removeSegments(ids);
    for ([_]claims.publication.Binding{ first, second, replacement }) |binding| {
        var steps: usize = 0;
        while ((try recovery.recoverBinding(a, &store, &index, binding, 64)).more) : (steps += 1) {
            if (steps == 64) return error.EpochJobDidNotConverge;
        }
    }
    try std.testing.expectEqual(@as(u32, 1), (try index.reclaimSourceReceiptWindow(&primary, null, 64)).purged);
    const epoch2 = try index.advanceSourcePublicationEpoch();
    try std.testing.expectEqual(@as(u64, 2), epoch2.floor);
    try std.testing.expectEqual(@as(u32, 1), (try index.reclaimSourceReceiptWindow(&primary, null, 64)).purged);
    try std.testing.expectEqualDeep(claims.Budget{}, try budget(&store));
    try std.testing.expectError(error.IndexPublicationEpochExpired, index.indexSegmentOwnedWithSources(try a.dupe(u8, bytes1), replacement));
}

test "persistent source publication maintenance coalesces epoch fences and bounds idle debt" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const primary_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/epoch-gc-primary", .{temporary.sub_path});
    defer a.free(primary_path);
    const index_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/epoch-gc-index", .{temporary.sub_path}, 0);
    defer a.free(index_path);
    var backend = try lsm.Backend.open(a, primary_path, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    try initialize(a, &store);
    var index = try persistent.PersistentIndex.open(a, .{ .path = index_path });
    defer index.close();
    try index.reserveSourceOwner(@splat(151));
    _ = try index.advanceSourcePublicationEpoch();
    var first: claims.publication.Binding = undefined;
    for (0..persistent.PersistentIndex.source_receipt_epoch_batch) |i| {
        var fresh = try index.beginNewSourcePublication(std.testing.io);
        defer fresh.publisher.deinit();
        if (i == 0) first = fresh.binding;
        _ = try index.cancelSourcePublication(fresh.binding);
    }
    var turns: usize = 0;
    while (index.sourceOwnershipMaintenancePending()) : (turns += 1) {
        if (turns == 1024) return error.ReceiptMaintenanceDidNotQuiesce;
        _ = try index.runSourceOwnershipMaintenanceStep(&store, 16);
    }
    try std.testing.expect(try index.sourcePublicationState(first) == null);
    try (try index.loadDurableSourcePublication(first)).?.require(first, .cancelled);
    try (try index.cancelSourcePublication(first)).require(first, .cancelled);
    try std.testing.expect(try index.sourcePublicationState(first) == null);
    try std.testing.expectError(error.IndexPublicationEpochExpired, index.beginSourcePublication(first));
    var fresh = try index.beginNewSourcePublication(std.testing.io);
    defer fresh.publisher.deinit();
    try std.testing.expectEqual(@as(u64, 2), @import("index_source_epoch.zig").tokenEpoch(fresh.binding).?);
    _ = try index.cancelSourcePublication(fresh.binding);
    turns = 0;
    while (index.sourceOwnershipMaintenancePending()) : (turns += 1) {
        if (turns == 1024) return error.ReceiptMaintenanceDidNotQuiesce;
        _ = try index.runSourceOwnershipMaintenanceStep(&store, 16);
    }
    // One terminal receipt stays in the current epoch; idle maintenance does
    // not repeatedly advance or sync an epoch for sub-threshold debt.
    try (try index.loadDurableSourcePublication(fresh.binding)).?.require(fresh.binding, .cancelled);
    try std.testing.expect(try index.runSourceOwnershipMaintenanceStep(&store, 16) == null);
}

test "persistent source publication receipt collection retains unjournaled primary claims" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/unjournaled-source-index", .{temporary.sub_path}, 0);
    defer a.free(path);
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    try initialize(a, &store);
    var index = try persistent.PersistentIndex.open(a, .{ .path = path });
    defer index.close();
    try index.reserveSourceOwner(@splat(171));
    _ = try index.advanceSourcePublicationEpoch();
    const physical = try keys.documentKeyAlloc(a, "unjournaled");
    defer a.free(physical);
    try store.put(physical, "{\"body\":\"original\"}");
    const input = try observe(&store, physical);
    var fresh = try index.beginNewSourcePublication(std.testing.io);
    defer fresh.publisher.deinit();
    const reference = claimed: {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const ref = try claims.prepareCurrent(a, &txn, fresh.binding, physical, input.from.current, input.root, .{});
        try txn.commit();
        break :claimed ref;
    };
    const decision = try index.cancelSourcePublication(fresh.binding);
    fresh.publisher.deinit();
    _ = try index.advanceSourcePublicationEpoch();
    const NoSyncPrimary = struct {
        inner: *ds.DocStore,
        pub fn beginReadTxn(self: *@This()) !ds.DocStore.Txn {
            return self.inner.beginReadTxn();
        }
        pub fn sync(_: *@This(), _: bool) !void {
            return error.UnexpectedPrimarySync;
        }
    };
    var guarded: NoSyncPrimary = .{ .inner = &store };
    try std.testing.expectEqual(@as(u32, 0), (try index.reclaimSourceReceiptWindow(&guarded, null, 64)).purged);
    try (try index.loadDurableSourcePublication(fresh.binding)).?.require(fresh.binding, .cancelled);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try claims.cancel(a, &txn, reference, decision));
        try txn.commit();
    }
    try std.testing.expectEqual(@as(u32, 1), (try index.reclaimSourceReceiptWindow(&store, null, 64)).purged);
    try std.testing.expectEqualDeep(claims.Budget{}, try budget(&store));
}

test "persistent source publication imports held afterimages across primaries without replacing current rows" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const origin_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/source-transfer-origin", .{temporary.sub_path}, 0);
    defer a.free(origin_path);
    const target_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/source-transfer-target", .{temporary.sub_path}, 0);
    defer a.free(target_path);
    var origin_backend = lsm.Backend.init(a, .{});
    defer origin_backend.close();
    var target_backend = lsm.Backend.init(a, .{});
    defer target_backend.close();
    var origin = try ds.DocStore.openRuntime(a, try origin_backend.runtimeStore(a, .{}));
    defer origin.close();
    var target = try ds.DocStore.openRuntime(a, try target_backend.runtimeStore(a, .{}));
    defer target.close();
    try initialize(a, &origin);
    try initialize(a, &target);
    var origin_index = try persistent.PersistentIndex.open(a, .{ .path = origin_path });
    defer origin_index.close();
    var target_index = try persistent.PersistentIndex.open(a, .{ .path = target_path });
    defer target_index.close();
    const physical = try keys.documentKeyAlloc(a, "transfer");
    defer a.free(physical);
    const body = try a.alloc(u8, 128 * 1024);
    defer a.free(body);
    @memset(body, 'x');
    const first = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"label\":\"original\",\"metadata\":1}}", .{body});
    defer a.free(first);
    const indexed = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"label\":\"original\",\"metadata\":2}}", .{body});
    defer a.free(indexed);
    try origin.put(physical, first);
    try origin.put(physical, indexed);
    const input = try observe(&origin, physical);
    const bytes = try segment(a, &.{.{ .id = "transfer", .stored_data = indexed, .text_fields = &.{.{ .field_name = "label", .text = "original" }} }});
    defer a.free(bytes);
    {
        var prepared = try recovery.prepareFresh(a, &origin, &origin_index, @splat(181), (&input)[0..1], bytes, .{}, std.testing.io);
        defer prepared.deinit(a);
        _ = try recovery.publishOwned(a, &origin, &origin_index, &prepared, try a.dupe(u8, bytes));
    }
    try drainOwnership(&origin, &origin_index);
    try std.testing.expect((try budget(&origin)).value_bytes >= body.len);
    try origin.delete(physical);
    try target.put(physical, "{\"body\":\"latest destination\"}");
    {
        var captured = (try origin_index.captureSource(&origin_index.snapshot().segments[0], 0)).?;
        defer captured.deinit();
        try origin_index.removeSegments(&.{origin_index.snapshot().segments[0].id});
        try drainOwnership(&origin, &origin_index);
        try std.testing.expect((try budget(&origin)).value_bytes >= body.len);
        var read = try origin.beginReadTxn();
        defer read.abort();
        const OriginReader = struct {
            inner: *ds.DocStore.Txn,
            value_reads: usize = 0,
            forbid_values: bool = false,
            corrupt_values: bool = false,
            scratch: []u8,
            pub fn getPhysical(self: *@This(), key: []const u8) ![]const u8 {
                if (std.mem.startsWith(u8, key, payload.ownership.value_prefix)) {
                    if (self.forbid_values) return error.UnexpectedValueRead;
                    self.value_reads += 1;
                }
                if (keys.isPrimaryDocumentKey(key)) return error.UnexpectedCurrentRowRead;
                const raw = try self.inner.getPhysical(key);
                if (self.corrupt_values and std.mem.startsWith(u8, key, payload.ownership.value_prefix)) {
                    if (raw.len > self.scratch.len) return error.InvalidArgument;
                    @memcpy(self.scratch[0..raw.len], raw);
                    self.scratch[0] ^= 1;
                    return self.scratch[0..raw.len];
                }
                return raw;
            }
        };
        const copy_storage = try a.alloc(u8, indexed.len);
        defer a.free(copy_storage);
        var reader: OriginReader = .{ .inner = &read, .scratch = copy_storage };
        var session = try captured.openRead(a, &reader);
        defer session.deinit();
        const sessions = [_]*const @TypeOf(session){&session};
        reader.corrupt_values = true;
        try std.testing.expectError(error.InvalidDocumentOwnership, recovery.prepareImported(a, &target, &target_index, @splat(182), &sessions, bytes, .{}, std.testing.io));
        reader.corrupt_values = false;
        const duplicate_sessions = [_]*const @TypeOf(session){ &session, &session };
        try std.testing.expectError(error.InvalidIndexSourceMap, recovery.prepareImported(a, &target, &target_index, @splat(182), &duplicate_sessions, bytes, .{}, std.testing.io));
        // Impossible cardinality must not start another source read/import.
        const before_quota_reads = reader.value_reads;
        try std.testing.expectError(error.IndexSourceClaimsFull, recovery.prepareImported(a, &target, &target_index, @splat(182), &sessions, bytes, .{ .claims = 0 }, std.testing.io));
        try std.testing.expectEqual(before_quota_reads, reader.value_reads);
        // A partially staged imported value set cannot escape the aggregate
        // byte quota either, including its reader fence and ownership metadata.
        try std.testing.expectError(error.IndexSourceClaimsFull, recovery.prepareImported(a, &target, &target_index, @splat(182), &sessions, bytes, .{ .value_bytes = 0 }, std.testing.io));
        {
            var untouched = try target.beginReadTxn();
            defer untouched.abort();
            const raw_schema = try untouched.getPhysical(schema.schema_key);
            const decoded = try schema.deserializeSchema(a, raw_schema);
            defer schema.freeSchema(a, decoded);
            try std.testing.expect(decoded.decoded_format_version < 27);
        }
        reader.value_reads = 0;
        {
            var prepared = try recovery.prepareImported(a, &target, &target_index, @splat(182), &sessions, bytes, .{}, std.testing.io);
            defer prepared.deinit(a);
            _ = try recovery.publishOwned(a, &target, &target_index, &prepared, try a.dupe(u8, bytes));
        }
        try std.testing.expect(reader.value_reads > 0);
        const charged = try budget(&target);
        try std.testing.expect(charged.value_bytes >= body.len);
        // A second publication links the exact snapshot/value set already
        // owned by this primary; it must not copy the original body again.
        reader.forbid_values = true;
        {
            var prepared = try recovery.prepareImportedSource(a, &target, &target_index, @splat(182), &sessions, .{ .contiguous = bytes }, .{}, std.testing.io);
            defer prepared.deinit(a);
            try prepared.source_stamp.?.require(.{ .contiguous = bytes });
            _ = (try recovery.replaceOwned(a, &target, &target_index, &prepared, &.{target_index.snapshot().segments[0].id}, try a.dupe(u8, bytes))).?;
        }
        try std.testing.expectEqual(charged.value_bytes, (try budget(&target)).value_bytes);
    }
    try drainOwnership(&origin, &origin_index);
    try drainOwnership(&target, &target_index);
    try std.testing.expectEqualDeep(claims.Budget{}, try budget(&origin));
    target_index.close();
    target_index = try persistent.PersistentIndex.open(a, .{ .path = target_path });
    {
        var captured = (try target_index.captureSource(&target_index.snapshot().segments[0], 0)).?;
        defer captured.deinit();
        var read = try target.beginReadTxn();
        defer read.abort();
        const exact = try captured.read(a, &read);
        defer exact.deinit(a);
        try std.testing.expectEqualStrings(indexed, exact.bytes);
        try std.testing.expectEqualStrings("{\"body\":\"latest destination\"}", try read.get(physical));
    }
    try target_index.removeSegments(&.{target_index.snapshot().segments[0].id});
    try drainOwnership(&target, &target_index);
    try std.testing.expectEqualDeep(claims.Budget{}, try budget(&target));
    var read = try target.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqualStrings("{\"body\":\"latest destination\"}", try read.get(physical));
}

test "persistent source publication epoch floor cancels an abandoned journal without recreating a receipt" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/epoch-abandoned-index", .{temporary.sub_path}, 0);
    defer a.free(path);
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    try initialize(a, &store);
    var index = try persistent.PersistentIndex.open(a, .{ .path = path });
    defer index.close();
    const physical = try keys.documentKeyAlloc(a, "abandoned-epoch");
    defer a.free(physical);
    try store.put(physical, "{\"body\":\"never published\"}");
    const input = try observe(&store, physical);
    const bytes = try segment(a, &.{.{ .id = "abandoned-epoch", .stored_data = "{}", .text_fields = &.{.{ .field_name = "body", .text = "never published" }} }});
    defer a.free(bytes);
    var prepared = try recovery.prepareFresh(a, &store, &index, @splat(191), (&input)[0..1], bytes, .{}, std.testing.io);
    const binding = prepared.job.binding;
    try std.testing.expect((try index.advanceSourcePublicationEpoch()).waiting_publishers);
    prepared.deinit(a);
    try std.testing.expect(!(try index.advanceSourcePublicationEpoch()).waiting_publishers);
    try std.testing.expect(try index.sourcePublicationState(binding) == null);
    try (try index.loadDurableSourcePublication(binding)).?.require(binding, .cancelled);
    try drainOwnership(&store, &index);
    try std.testing.expectEqualDeep(claims.Budget{}, try budget(&store));
    try std.testing.expect(try index.sourcePublicationState(binding) == null);
    try std.testing.expectError(error.IndexPublicationEpochExpired, index.indexSegmentOwnedWithSources(try a.dupe(u8, bytes), binding));
    // Reopen must derive the same authority solely from the durable floor.
    index.close();
    index = try persistent.PersistentIndex.open(a, .{ .path = path });
    try (try index.loadDurableSourcePublication(binding)).?.require(binding, .cancelled);
    try (try index.cancelSourcePublicationIfAbandoned(binding)).require(binding, .cancelled);
    try std.testing.expect(try index.sourcePublicationState(binding) == null);
}

test "persistent source publication older receipt GC proceeds while a newer epoch waits for preparation" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/epoch-independent-gc", .{temporary.sub_path}, 0);
    defer a.free(path);
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    try initialize(a, &store);
    var index = try persistent.PersistentIndex.open(a, .{ .path = path });
    defer index.close();
    try index.reserveSourceOwner(@splat(201));
    _ = try index.advanceSourcePublicationEpoch();
    var old = try index.beginNewSourcePublication(std.testing.io);
    defer old.publisher.deinit();
    _ = try index.cancelSourcePublication(old.binding);
    old.publisher.deinit();
    _ = try index.advanceSourcePublicationEpoch();
    var pending = try index.beginNewSourcePublication(std.testing.io);
    defer pending.publisher.deinit();
    const next = try index.advanceSourcePublicationEpoch();
    try std.testing.expectEqual(@as(u64, 3), next.current);
    try std.testing.expectEqual(@as(u64, 2), next.floor);
    try std.testing.expect(next.waiting_publishers);
    const progress = try index.reclaimSourceReceiptWindow(&store, null, 64);
    try std.testing.expect(progress.waiting_publishers);
    try std.testing.expectEqual(@as(u32, 1), progress.purged);
    try std.testing.expect(try index.sourcePublicationState(old.binding) == null);
    try (try index.loadDurableSourcePublication(old.binding)).?.require(old.binding, .cancelled);
    try std.testing.expect(try index.sourcePublicationState(pending.binding) == null);
}

test "persistent source publication query frame pins before primary cut and bounds map cache across retirement" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/query-source-frame", .{temporary.sub_path}, 0);
    defer a.free(path);
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    try initialize(a, &store);
    var index = try persistent.PersistentIndex.open(a, .{ .path = path });
    defer index.close();
    const Primary = struct {
        store: *ds.DocStore,
        opens: usize = 0,
        pub fn beginReadTxn(self: *@This()) !ds.DocStore.Txn {
            self.opens += 1;
            return self.store.beginReadTxn();
        }
    };
    var primary: Primary = .{ .store = &store };
    try std.testing.expect(try index.openSourceReadFrame(a, &primary, index.snapshot()) == null);
    try std.testing.expectEqual(@as(usize, 0), primary.opens);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const strings = arena.allocator();
    const original = "{\"body\":\"original\",\"revision\":1}";
    var docs: [65]intro.TextDocument = undefined;
    var inputs: [65]recovery.Input = undefined;
    for (&docs, &inputs, 0..) |*doc, *input, i| {
        const id = try std.fmt.allocPrint(strings, "frame-{d:0>3}", .{i});
        const physical = try keys.documentKeyAlloc(strings, id);
        try store.put(physical, original);
        input.* = try observe(&store, physical);
        doc.* = .{ .id = id, .stored_data = original, .text_fields = &.{.{ .field_name = "body", .text = "original" }} };
    }
    for (0..3) |i| {
        const text = if (i == 0) try segment(a, &docs) else try segment(a, docs[i .. i + 1]);
        defer a.free(text);
        const selected = if (i == 0) &inputs else inputs[i .. i + 1];
        var prepared = try recovery.prepareFresh(a, &store, &index, @splat(211), selected, text, .{}, std.testing.io);
        defer prepared.deinit(a);
        _ = try recovery.publishOwned(a, &store, &index, &prepared, try a.dupe(u8, text));
    }
    const legacy_bytes = try segment(a, &.{.{ .id = "legacy", .stored_data = "{}", .text_fields = &.{.{ .field_name = "body", .text = "legacy" }} }});
    defer a.free(legacy_bytes);
    try index.indexSegment(legacy_bytes);
    try drainOwnership(&store, &index);
    {
        var source = (try index.captureSourceReferences(a, &index.snapshot().segments[0])).?;
        defer source.deinit();
        const Counted = struct {
            source: *persistent.PersistentIndex.CapturedSourceReferences,
            view: *const @import("index_source_map.zig").ReadView,
            decision: claims.publication.Decision,
            lease: *claims.publication.SharedReaders.Lease,
            reads: usize = 0,
            pub fn readWindow(self: *@This(), owner: *persistent.PersistentIndex, first: u32, refs: []claims.Reference) !void {
                self.reads += 1;
                try self.source.readWindow(owner, first, refs);
            }
        };
        var counted: Counted = .{ .source = &source, .view = &source.view, .decision = source.decision, .lease = &source.lease };
        var expected_last = (try index.captureSource(&index.snapshot().segments[0], 64)).?;
        defer expected_last.deinit();
        var map = try @import("../segment.zig").MergeSourceMap.init(a, &.{65});
        defer map.deinit();
        try map.recordRange(0, 64, 1, 0);
        try map.finishOutput(1);
        var output = [_]recovery.Input{.{ .root = @splat(0), .from = .{ .current = 0 } }};
        try recovery.assignMappedInputs(&index, &counted, &map, 0, &.{&output});
        try std.testing.expectEqual(@as(usize, 1), counted.reads);
        try std.testing.expectEqualDeep(expected_last.reference, output[0].from.published.reference);
        try std.testing.expect(output[0].from.published.lease == &source.lease.inner);
        var shuffled = try @import("../segment.zig").MergeSourceMap.init(a, &.{65});
        defer shuffled.deinit();
        try shuffled.record(0, 64, 0);
        try shuffled.record(0, 0, 1);
        try shuffled.finishOutput(2);
        var outputs: [2]recovery.Input = @splat(.{ .root = @splat(0), .from = .{ .current = 0 } });
        counted.reads = 0;
        try recovery.assignMappedInputs(&index, &counted, &shuffled, 0, &.{&outputs});
        try std.testing.expectEqual(@as(usize, 2), counted.reads);
        try std.testing.expectEqualDeep(expected_last.reference, outputs[0].from.published.reference);
        var expected_first = (try index.captureSource(&index.snapshot().segments[0], 0)).?;
        defer expected_first.deinit();
        try std.testing.expectEqualDeep(expected_first.reference, outputs[1].from.published.reference);
    }
    var frame = (try index.openSourceReadFrame(a, &primary, index.snapshot())).?;
    var frame_open = true;
    defer if (frame_open) frame.deinit();
    try std.testing.expectEqual(@as(usize, 0), primary.opens);
    _ = try frame.publicationRootAt(0);
    _ = try frame.identityAt(0, 0);
    try std.testing.expectEqual(@as(usize, 0), primary.opens);
    const read_view = try frame.readView();
    try std.testing.expectEqual(@as(usize, 1), primary.opens);
    try store.put(inputs[0].physical, "{\"body\":\"newer\",\"revision\":2}");
    // Both claimed and legacy fallback reads retain the original primary cut.
    try std.testing.expectEqualStrings(original, try read_view.get(inputs[0].physical));
    try std.testing.expect(try frame.captureAt(3, 0) == null);
    try std.testing.expectError(error.InvalidArgument, frame.captureAt(4, 0));
    try std.testing.expectError(error.InvalidArgument, frame.captureAt(0, 65));
    var held = (try frame.captureAt(0, 64)).?;
    var held_open = true;
    defer if (held_open) held.deinit();
    for ([_]usize{ 0, 1, 2, 0, 2, 1, 0 }) |ordinal| {
        var source = (try frame.captureAt(ordinal, 0)).?;
        defer source.deinit();
        const exact = try source.read(a, try frame.readView());
        defer exact.deinit(a);
        try std.testing.expectEqualStrings(original, exact.bytes);
    }
    try std.testing.expectEqual(@as(usize, 1), primary.opens);
    var ids: [4]u64 = undefined;
    for (index.snapshot().segments, &ids) |entry, *id| id.* = entry.id;
    try index.removeSegments(&ids);
    try drainOwnership(&store, &index);
    try std.testing.expectEqual(@as(u64, 67), (try budget(&store)).claims);
    {
        var source = (try frame.captureAt(2, 0)).?;
        defer source.deinit();
        const exact = try source.read(a, try frame.readView());
        defer exact.deinit(a);
        try std.testing.expectEqualStrings(original, exact.bytes);
    }
    frame.deinit();
    frame_open = false;
    try drainOwnership(&store, &index);
    try std.testing.expectEqual(@as(u64, 65), (try budget(&store)).claims);
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const exact = try held.read(a, &read);
        defer exact.deinit(a);
        try std.testing.expectEqualStrings(original, exact.bytes);
    }
    held.deinit();
    held_open = false;
    try drainOwnership(&store, &index);
    try std.testing.expectEqualDeep(claims.Budget{}, try budget(&store));
}

test "persistent source publication ordinary value queries verify indexed revisions with literal paths and original budgets" {
    const a = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/indexed-value-queries", .{temporary.sub_path});
    defer a.free(path);
    var db = try db_mod.DB.open(a, path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(a, "{}");
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    db.core.store.document_payload_prototype = true;
    const manager = db.core.index_manager;
    try std.testing.expect(!manager.source_ownership_possible.load(.acquire));
    {
        var empty: [0]u8 = .{};
        var fixed = std.heap.FixedBufferAllocator.init(&empty);
        const before = manager.catalog_mutex.snapshot().shared_lock_calls;
        try std.testing.expect((try @import("db/query/source_context.zig").Plan.create(fixed.allocator(), manager, .{ .index_name = "text" })) == null);
        try std.testing.expectEqual(before, manager.catalog_mutex.snapshot().shared_lock_calls);
    }
    const index = &manager.text_indexes.items[0].persistent;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const strings = arena.allocator();
    const body = try strings.alloc(u8, 128 * 1024);
    @memset(body, 'x');
    var docs: [3]intro.TextDocument = undefined;
    var inputs: [3]recovery.Input = undefined;
    for (&docs, &inputs, 0..) |*doc, *input, i| {
        const id = try std.fmt.allocPrint(strings, "value-{d}", .{i});
        const physical = try keys.documentKeyAlloc(strings, id);
        const content = if (i < 2) "old needle" else "old hay";
        const first = try std.fmt.allocPrint(strings, "{{\"content\":\"{s}\",\"-literal\":\"needle\",\"*\":\"needle\",\"body\":\"{s}\",\"a\":{{\"\":{{\"b\":\"needle\"}}}},\"revision\":1}}", .{ content, body });
        const original = try std.fmt.allocPrint(strings, "{{\"content\":\"{s}\",\"-literal\":\"needle\",\"*\":\"needle\",\"body\":\"{s}\",\"a\":{{\"\":{{\"b\":\"needle\"}}}},\"revision\":2}}", .{ content, body });
        try db.core.store.put(physical, first);
        try db.core.store.put(physical, original);
        input.* = try observe(db.core.store, physical);
        const text = try strings.alloc(@typeInfo(@FieldType(intro.TextDocument, "text_fields")).pointer.child, 1);
        text[0] = .{ .field_name = "content", .text = content };
        doc.* = .{ .id = id, .stored_data = original, .text_fields = text };
    }
    const bytes = try segment(a, &docs);
    defer a.free(bytes);
    {
        const duplicates = [_]recovery.Input{ inputs[0], inputs[0], inputs[2] };
        try std.testing.expectError(error.InvalidIndexSourceMap, recovery.prepareFresh(a, db.core.store, index, @splat(221), &duplicates, bytes, .{}, std.testing.io));
        var untouched = try db.core.store.beginReadTxn();
        defer untouched.abort();
        try std.testing.expectError(error.IndexSourceClaimsCorrupt, claims.Budget.load(&untouched));
    }
    {
        var prepared = try recovery.prepareFresh(a, db.core.store, index, @splat(221), &inputs, bytes, .{}, std.testing.io);
        defer prepared.deinit(a);
        _ = try recovery.publishOwned(a, db.core.store, index, &prepared, try a.dupe(u8, bytes));
    }
    try std.testing.expect(manager.source_ownership_possible.load(.acquire));
    for (inputs) |input| try db.core.store.put(input.physical, "{\"content\":\"newer value\",\"-literal\":\"miss\",\"*\":\"miss\"}");
    const requests = [_]struct { field: []const u8, text: []const u8, expected: u64 }{
        .{ .field = "content", .text = "needle", .expected = 2 },
        .{ .field = "content", .text = "newer", .expected = 0 },
        .{ .field = "-literal", .text = "needle", .expected = 3 },
        .{ .field = "*", .text = "needle", .expected = 3 },
        .{ .field = "a..b", .text = "needle", .expected = 3 },
        .{ .field = "content.", .text = "needle", .expected = 2 },
    };
    for (requests) |request| {
        var result = try db.search(a, .{ .index_name = "text", .full_text = .{ .value = .{ .kind = .contains, .field = request.field, .text = request.text } }, .count_only = true, .include_stored = false, .value_search_execution = .{ .allow_scan = true } });
        defer result.deinit();
        try std.testing.expectEqual(request.expected, result.total_hits);
    }
    {
        // A declared lexical access path needs one read lease and no scratch
        // allocation, including when another producer might request a writer.
        var empty: [0]u8 = .{};
        var fixed = std.heap.FixedBufferAllocator.init(&empty);
        const entry = db.core.textIndexEntry("text").?;
        const before = entry.analysis_mutex.snapshot().shared_lock_calls;
        const access = (try db.core.index_manager.planFullTextLexicalAccessPathAlloc(fixed.allocator(), "text", "content", null, .slice)).?;
        try std.testing.expectEqualStrings("text", access.access_path.owner);
        try std.testing.expectEqual(before + 1, entry.analysis_mutex.snapshot().shared_lock_calls);
    }
    for ([_]bool{ true, false }) |include_stored| {
        var result = try db.search(a, .{ .index_name = "text", .full_text = .{ .match = .{ .field = "content", .text = "needle" } }, .include_stored = include_stored, .fields = &.{"revision"}, .include_all_fields = false, .highlight = .{ .fields = &.{"content"} } });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.hits.len);
        for (result.hits) |hit| {
            try std.testing.expect(hit.indexed_source == null);
            try std.testing.expectEqual(@as(usize, 1), hit.highlights.len);
            try std.testing.expectEqualStrings("content", hit.highlights[0].field);
            try std.testing.expectEqualStrings("old needle", hit.highlights[0].fragments[0].text);
            if (include_stored) {
                var body_value = try std.json.parseFromSlice(std.json.Value, a, hit.stored_data.?, .{});
                defer body_value.deinit();
                try std.testing.expectEqual(@as(i64, 2), body_value.value.object.get("revision").?.integer);
                try std.testing.expect(body_value.value.object.get("content") == null);
                try std.testing.expect(body_value.value.object.get("body") == null);
            } else try std.testing.expect(hit.stored_data == null);
        }
    }
    {
        var result = try db.search(a, .{ .index_name = "text", .full_text = .{ .match = .{ .field = "content", .text = "needle" } }, .order_by = &.{.{ .field = "_id" }}, .include_stored = true, .fields = &.{"content"}, .include_all_fields = false });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.hits.len);
        try std.testing.expectEqualStrings("value-0", result.hits[0].id);
        try std.testing.expectEqualStrings("value-1", result.hits[1].id);
        for (result.hits) |hit| {
            try std.testing.expect(hit.indexed_source == null);
            try std.testing.expect(std.mem.indexOf(u8, hit.stored_data.?, "old needle") != null);
        }
    }
    // Selected metadata is consumed inside the DB boundary. Results carry
    // owned projected indexed bytes and no DB-borrowing native handles.
    const query: @import("db/types.zig").TextQuery = .{ .value = .{ .kind = .contains, .field = "content", .text = "needle" } };
    {
        var result = try db.search(a, .{ .index_name = "text", .full_text = query, .include_stored = true, .fields = &.{ "content", "revision" }, .include_all_fields = false, .value_search_execution = .{ .allow_scan = true } });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.hits.len);
        for (result.hits) |hit| {
            try std.testing.expect(hit.indexed_source == null);
            var source = try std.json.parseFromSlice(std.json.Value, a, hit.stored_data.?, .{});
            defer source.deinit();
            try std.testing.expectEqualStrings("old needle", source.value.object.get("content").?.string);
            try std.testing.expectEqual(@as(i64, 2), source.value.object.get("revision").?.integer);
            try std.testing.expect(source.value.object.get("body") == null);
        }
    }
    {
        var result = try db.search(a, .{ .index_name = "text", .full_text = query, .filter_query_json = "{\"term\":{\"revision\":2}}", .include_stored = false, .value_search_execution = .{ .allow_scan = true } });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.hits.len);
        for (result.hits) |hit| {
            try std.testing.expect(hit.indexed_source == null);
            try std.testing.expect(hit.stored_data == null);
        }
    }
    {
        var result = try db.search(a, .{ .index_name = "text", .full_text = query, .filter_text = .{ .value = .{ .kind = .equals, .field = "-literal", .text = "needle" } }, .exclusion_text = .{ .value = .{ .kind = .equals, .field = "content", .text = "newer value" } }, .count_only = true, .include_stored = false, .value_search_execution = .{ .allow_scan = true } });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    }
    {
        var result = try db.search(a, .{ .full_text_queries = &.{ .{ .name = "first", .index_name = "text", .query = query }, .{ .name = "second", .index_name = "text", .query = query } }, .include_stored = true, .fields = &.{ "content", "revision" }, .include_all_fields = false, .value_search_execution = .{ .allow_scan = true } });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.hits.len);
        for (result.hits) |hit| {
            try std.testing.expect(hit.indexed_source == null);
            try std.testing.expect(std.mem.indexOf(u8, hit.stored_data.?, "old needle") != null);
        }
    }
    {
        var result = try db.search(a, .{ .index_name = "text", .full_text = .{ .match_all = {} }, .include_stored = true, .fields = &.{ "content", "revision" }, .include_all_fields = false, .source_projection_json = "{\"fields\":[{\"field\":\"content\",\"mode\":\"identity\"}]}", .value_search_execution = .{ .allow_scan = true } });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 3), result.hits.len);
        for (result.hits) |hit| {
            try std.testing.expect(hit.indexed_source == null);
            try std.testing.expect(hit.finalized_source != null);
            var source = try std.json.parseFromSlice(std.json.Value, a, hit.stored_data.?, .{});
            defer source.deinit();
            try std.testing.expect(source.value.object.get("content") == null);
            try std.testing.expectEqual(@as(i64, 2), source.value.object.get("revision").?.integer);
            var identities = try std.json.parseFromSlice(std.json.Value, a, hit.source_projection_json.?, .{});
            defer identities.deinit();
            try std.testing.expectEqualStrings("reference", identities.value.object.get("content").?.object.get("state").?.string);
        }
    }
    // Projection cannot make the original-value evaluation budget disappear.
    try std.testing.expectError(error.ValueSearchLimitExceeded, db.search(a, .{ .index_name = "text", .full_text = .{ .value = .{ .kind = .contains, .field = "content", .text = "old" } }, .count_only = true, .include_stored = false, .value_search_execution = .{ .allow_scan = true, .max_bytes = 1 } }));
}

test "persistent source publication logical roots release while unchanged physical postings remain shared" {
    const a = std.testing.allocator;
    const Writer = @import("../index.zig").IndexWriter;
    const publication = @import("index_publication.zig");
    const bytes = try segment(a, &.{.{ .id = "shared", .stored_data = "{}", .text_fields = &.{.{ .field_name = "content", .text = "unchanged" }} }});
    defer a.free(bytes);
    var original = try Writer.init(a);
    var original_open = true;
    defer if (original_open) original.deinit();
    try original.addSegmentWithId(1, bytes);
    var old = try publication.SharedReaders.open(a, .{ .owner = @splat(71), .publication = @splat(72) });
    defer old.deinit();
    try original.current.segments[0].attachSourceGeneration(a, try old.register());
    var next = try Writer.init(a);
    defer next.deinit();
    try next.shareImmutableSegments(&.{.{ .snapshot = original.current, .ordinal = 0, .target_id = 1 }});
    try std.testing.expect(original.current.segments[0].shared == next.current.segments[0].shared);
    var fresh = try publication.SharedReaders.open(a, .{ .owner = @splat(71), .publication = @splat(73) });
    defer fresh.deinit();
    // The private unpublished next generation reuses physical bytes while
    // replacing only its logical owner. No physical_parent chain is involved.
    const replacement = try @import("index_source_generation.zig").Generation.create(a, try fresh.register());
    next.current.segments[0].source_generation.?.release();
    next.current.segments[0].source_generation = replacement;
    old.retire();
    try std.testing.expect(old.drained() == null);
    original.deinit();
    original_open = false;
    try std.testing.expect(old.drained() != null);
    try std.testing.expect(next.current.segments[0].shared.physical_parent == null);
    try std.testing.expectEqual(@as(u32, 1), next.current.liveDocCount());
    var held = (try next.current.segments[0].sourceLease()).?;
    defer held.deinit();
    try std.testing.expectEqualSlices(u8, &@as([32]u8, @splat(73)), &(try held.binding()).publication);
}

test "persistent source publication reader broker rendezvous spans independent native archives" {
    const Broker = persistent.SourceReaderBroker;
    const Port = @import("antfly_runtime_abi").source_owner_abi.Port;
    const Provider = struct {
        extern fn antfly_test_source_registry_broker(out: *Port) callconv(.c) void;
    };
    const a = std.testing.allocator;
    var port: Port = undefined;
    Provider.antfly_test_source_registry_broker(&port);
    const broker = try Broker.receive(port);
    const binding: claims.publication.Binding = .{ .owner = @splat(95), .publication = @splat(96) };
    var origin = try broker.open(a, binding.owner);
    defer origin.deinit();
    var root = try origin.openPublication(a, binding);
    defer root.deinit();
    var held = try root.register();
    defer held.deinit();
    // A new receiver uses the supplied origin, rather than its own identically
    // named module global. Its retirement must still see the original reader.
    var receiver = try (try Broker.receive(port)).open(a, binding.owner);
    defer receiver.deinit();
    var reopened = try receiver.openPublication(a, binding);
    defer reopened.deinit();
    reopened.retire();
    try std.testing.expect(reopened.drained() == null);
    // Negative control: the consumer archive really has a separate namespace.
    var isolated = try Broker.local().open(a, binding.owner);
    defer isolated.deinit();
    var isolated_root = try isolated.openPublication(a, binding);
    defer isolated_root.deinit();
    isolated_root.retire();
    try std.testing.expect(isolated_root.drained() != null);
    held.deinit();
    try std.testing.expect(reopened.drained() != null);
}

test "persistent source publication family leaves publish exact sources query across cohorts and reopen" {
    const family = @import("index_source_family.zig");
    const benchmark = @import("builtin").link_libc and std.c.getenv("ANTFLY_BENCH_SOURCE_FAMILY") != null;
    const a = if (benchmark) @import("antfly_platform").allocator.processAllocator(std.heap.page_allocator) else std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const primary_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/family-primary", .{temporary.sub_path});
    defer a.free(primary_path);
    const index_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/family-index", .{temporary.sub_path}, 0);
    defer a.free(index_path);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    const owner: [32]u8 = @splat(113);
    const document_count: usize = if (benchmark) (if (std.c.getenv("ANTFLY_BENCH_SOURCE_DOCUMENTS")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else 131) else 131;
    const payload_bytes: usize = if (benchmark) (if (std.c.getenv("ANTFLY_BENCH_SOURCE_BYTES")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else 0) else 0;
    if (document_count < 128 or document_count > 65539 or (document_count % 64 != 3 and document_count % 64 != 0) or payload_bytes > 1024 * 1024 or document_count * payload_bytes > 64 * 1024 * 1024) return error.InvalidArgument;
    const data = try owned.alloc(u8, payload_bytes);
    @memset(data, 'x');
    const docs = try owned.alloc(intro.TextDocument, document_count);
    const inputs = try owned.alloc(recovery.Input, document_count);
    const last_first: u32 = @intCast((document_count - 1) / 64 * 64);
    const cohort_count = document_count - last_first;
    const last: u32 = @intCast(document_count - 1);
    const clock = @import("antfly_platform").time;
    var timing: [8]u64 = @splat(0);
    var verified: usize = 0;
    for (docs, 0..) |*doc, n| doc.* = .{ .id = try std.fmt.allocPrint(owned, "family-{d}", .{n}), .stored_data = try std.fmt.allocPrint(owned, "{{\"body\":\"hello\",\"n\":{d},\"data\":\"{s}\"}}", .{ n, data }), .text_fields = &.{.{ .field_name = "body", .text = "hello" }} };
    const bytes = try segment(a, docs);
    defer a.free(bytes);
    var root: [32]u8 = undefined;
    var leaf_binding: claims.publication.Binding = undefined;
    for (0..2) |pass| {
        const open_start = clock.monotonicNs();
        var backend = try lsm.Backend.open(a, primary_path, .{});
        defer backend.close();
        var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        var index = try persistent.PersistentIndex.open(a, .{ .path = index_path });
        var index_open = true;
        defer if (index_open) index.close();
        if (pass == 1) timing[6] = clock.monotonicNs() - open_start;
        if (pass == 0) {
            try initialize(a, &store);
            for (inputs, docs) |*input, doc| {
                const physical = try keys.documentKeyAlloc(owned, doc.id);
                try store.put(physical, doc.stored_data);
                input.* = try observe(&store, physical);
            }
            try index.indexSegment(bytes);
            var physical: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes, &physical, .{});
            const layout: family.Layout = .{ .segment = index.snapshot().segments[0].id, .documents = @intCast(docs.len), .physical = physical, .configuration = @splat(114) };
            const cohorts = try owned.alloc([]const recovery.Input, (document_count + 63) / 64);
            const identities = try owned.alloc([32]u8, cohorts.len);
            for (cohorts, identities, 0..) |*cohort, *identity, n| {
                const first = n * 64;
                cohort.* = inputs[first..@min(first + 64, inputs.len)];
                identity.* = try layout.identity(@intCast(first));
            }
            var measured: FaultPrimary = .{ .store = &store, .remaining = std.math.maxInt(usize) };
            var started = clock.monotonicNs();
            const prepared = try recovery.prepareSourceLeaves(a, &measured, &index, owner, cohorts, identities, .{}, std.testing.io);
            timing[0] = clock.monotonicNs() - started;
            var preparations_open = true;
            defer if (preparations_open) {
                for (prepared) |*item| item.deinit(a);
                a.free(prepared);
            };
            try std.testing.expectEqual(@as(usize, 1), measured.forced_syncs);
            {
                var read = try store.beginReadTxn();
                defer read.abort();
                try std.testing.expectEqual(@as(u32, 28), try schema.serializedReaderFormat(try read.getPhysical(schema.schema_key)));
            }
            // Repeat admission must close its transaction and retry durability.
            try index.admitSourceFamilies();
            started = clock.monotonicNs();
            const decision = try index.publishSourceFamily(&measured, layout, prepared, std.testing.io);
            timing[1] = clock.monotonicNs() - started;
            try decision.requireRole(.family);
            root = decision.root;
            leaf_binding = prepared[prepared.len - 1].job.binding;
            try std.testing.expectEqual(@as(usize, 2), measured.forced_syncs);
            if (!benchmark) {
                _ = try recovery.recoverBinding(a, &store, &index, prepared[0].job.binding, 64);
                const retried = try index.publishSourceFamily(&measured, layout, prepared, std.testing.io);
                try std.testing.expectEqualDeep(decision, retried);
            }
            try std.testing.expectEqual(@as(u64, docs.len), (try budget(&store)).claims);
            try std.testing.expectError(error.IndexPublicationStillActive, index.retireSourcePublication(leaf_binding));
            var previous_snapshot: ?*@import("../index.zig").IndexSnapshot = index.writer.acquireSnapshot();
            defer if (previous_snapshot) |held| held.release();
            const previous_shared = previous_snapshot.?.segments[0].shared;
            var old_rows: [64]claims.CapturedSource = undefined;
            var old_rows_open: usize = 0;
            defer for (old_rows[0..old_rows_open]) |*row| row.deinit();
            for (old_rows[0..cohort_count], 0..) |*row, n| {
                row.* = (try index.captureSource(&previous_snapshot.?.segments[0], last_first + @as(u32, @intCast(n)))).?;
                old_rows_open += 1;
            }
            const original_last = docs[last].stored_data;
            docs[last].stored_data = try std.fmt.allocPrint(owned, "{{\"body\":\"hello\",\"n\":{d},\"metadata\":1,\"data\":\"{s}\"}}", .{ last, data });
            try store.put(inputs[last].physical, docs[last].stored_data);
            const changed_inputs = try owned.alloc(recovery.Input, cohort_count);
            for (old_rows[0 .. cohort_count - 1], changed_inputs[0 .. cohort_count - 1]) |*row, *input| input.* = .{ .root = row.reference.root, .from = .{ .published = .{ .reference = row.reference, .decision = row.decision, .lease = &row.lease.inner } } };
            changed_inputs[cohort_count - 1] = try observe(&store, inputs[last].physical);
            const changed_cohorts = [_][]const recovery.Input{changed_inputs};
            const changed_identities = [_][32]u8{try layout.identity(last_first)};
            started = clock.monotonicNs();
            const changed = try recovery.prepareSourceLeaves(a, &measured, &index, owner, &changed_cohorts, &changed_identities, .{}, std.testing.io);
            timing[2] = clock.monotonicNs() - started;
            var changed_open = true;
            defer if (changed_open) {
                for (changed) |*item| item.deinit(a);
                a.free(changed);
            };
            started = clock.monotonicNs();
            const next_decision = try index.replaceSourceCohort(&measured, layout.segment, last_first, root, &changed[0], std.testing.io);
            timing[3] = clock.monotonicNs() - started;
            const retry = try index.replaceSourceCohort(&measured, layout.segment, last_first, root, &changed[0], std.testing.io);
            try std.testing.expectEqualDeep(next_decision, retry);
            root = next_decision.root;
            leaf_binding = changed[0].job.binding;
            try std.testing.expect(previous_shared == index.snapshot().segments[0].shared);
            try std.testing.expect(index.snapshot().segments[0].shared.physical_parent == null);
            {
                var read = try store.beginReadTxn();
                defer read.abort();
                const source = try old_rows[cohort_count - 1].read(a, &read);
                defer source.deinit(a);
                try std.testing.expectEqualStrings(original_last, source.bytes);
            }
            for (prepared) |*item| item.deinit(a);
            a.free(prepared);
            preparations_open = false;
            for (changed) |*item| item.deinit(a);
            a.free(changed);
            changed_open = false;
            _ = try index.collectSourceFamilyGarbage(&store, 8);
            try std.testing.expectEqual(@as(u64, docs.len + cohort_count), (try budget(&store)).claims);
            previous_snapshot.?.release();
            previous_snapshot = null;
            for (0..8) |_| _ = try index.collectSourceFamilyGarbage(&store, 8);
            // A selected leaf page remains a reader after its family is gone.
            try std.testing.expectEqual(@as(u64, docs.len + cohort_count), (try budget(&store)).claims);
            for (old_rows[0..old_rows_open]) |*row| row.deinit();
            old_rows_open = 0;
            started = clock.monotonicNs();
            var turns: usize = 0;
            while (turns < 32 and (try budget(&store)).claims != docs.len) : (turns += 1) _ = try index.collectSourceFamilyGarbage(&store, 8);
            timing[4] = clock.monotonicNs() - started;
            try std.testing.expect(turns < 32);
            try std.testing.expectEqual(@as(u64, docs.len), (try budget(&store)).claims);
            try store.put(inputs[0].physical, "{\"body\":\"changed\"}");
            try store.delete(inputs[last].physical);
            try store.sync(true);
        }
        const read_start = clock.monotonicNs();
        var frame = (try index.openSourceReadFrame(a, &store, index.snapshot())).?;
        var frame_open = true;
        defer if (frame_open) frame.deinit();
        try std.testing.expectEqualSlices(u8, &root, &(try frame.publicationRootAt(0)).?);
        for ([_]u32{ last, 0, 64, 63, last - 1, 1 }) |document| {
            var captured = (try frame.captureAt(0, document)).?;
            defer captured.deinit();
            try captured.decision.requireRole(.source_leaf);
            const source = try captured.read(a, try frame.readView());
            defer source.deinit(a);
            try std.testing.expectEqualStrings(docs[document].stored_data, source.bytes);
            verified += 1;
        }
        var captured = (try index.captureSourceReferencesAt(a, &index.snapshot().segments[0], last)).?;
        var captured_open = true;
        defer if (captured_open) captured.deinit();
        try std.testing.expectEqual(last_first, captured.first_document);
        try std.testing.expectEqual(@as(u32, @intCast(cohort_count)), captured.view.documents);
        try std.testing.expectEqual(leaf_binding, captured.decision.binding);
        timing[5] += clock.monotonicNs() - read_start;
        var refs: [2]claims.Reference = undefined;
        try std.testing.expectError(error.InvalidIndexSourceMap, frame.captureReferencesAt(0, &.{ 0, 64 }, &refs));
        // Structural provenance uses global ordinals, even though each held
        // map addresses its own cohort locally. Lease addresses stay stable.
        if (pass == 0 and !benchmark) {
            var cohorts = (try index.captureSourceCohorts(a, &index.snapshot().segments[0])).?;
            defer cohorts.deinit();
            try std.testing.expectEqual(@as(usize, 3), cohorts.items.len);
            var map = try @import("../segment.zig").MergeSourceMap.init(a, &.{@intCast(document_count)});
            defer map.deinit();
            try map.recordRange(0, last, 1, 0);
            try map.finishOutput(1);
            var output = [_]recovery.Input{.{ .root = @splat(0), .from = .{ .current = 0 } }};
            for (cohorts.items) |*cohort| try recovery.assignMappedInputs(&index, cohort, &map, 0, &.{&output});
            try std.testing.expect(output[0].from == .published);
            try std.testing.expectEqual(leaf_binding, output[0].from.published.reference.binding);
        }
        if (pass == 1) {
            captured.deinit();
            captured_open = false;
            frame.deinit();
            frame_open = false;
            index.close();
            index_open = false;
            const drop_start = clock.monotonicNs();
            var cleanup = try persistent.PersistentIndex.SourceOwnerCleanup.open(a, .{ .path = index_path }, owner);
            defer cleanup.deinit();
            var finished = false;
            for (0..document_count / 8 + 128) |_| {
                const step = try cleanup.step(&store, 64);
                if (step.finished) {
                    finished = true;
                    break;
                }
            }
            try std.testing.expect(finished);
            try std.testing.expectEqual(@as(u64, 0), (try budget(&store)).claims);
            timing[7] = clock.monotonicNs() - drop_start;
        }
    }
    if (benchmark) std.debug.print("SOURCE_FAMILY documents={d} payload={d} update_claims={d} prepare_ns={d} publish_ns={d} update_prepare_ns={d} update_publish_ns={d} reclaim_ns={d} read_ns={d} reopen_ns={d} drop_ns={d} optimize_mode={d} production_allocator=1 claims_after_drop=0 physical_shared=1 verified={d} claims_after_gc={d}\n", .{ document_count, payload_bytes, cohort_count, timing[0], timing[1], timing[2], timing[3], timing[4], timing[5], timing[6], timing[7], @backingInt(@import("builtin").mode), verified, document_count });
}

test "persistent source publication DB delivers and filters selected pages across family cohorts" {
    const a = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const family = @import("index_source_family.zig");
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/family-query", .{temporary.sub_path});
    defer a.free(path);
    var db = try db_mod.DB.open(a, path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(a, "{}");
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    db.core.store.document_payload_prototype = true;
    const index = &db.core.index_manager.text_indexes.items[0].persistent;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    var docs: [65]intro.TextDocument = undefined;
    var inputs: [65]recovery.Input = undefined;
    for (&docs, &inputs, 0..) |*doc, *input, n| {
        const id = try std.fmt.allocPrint(owned, "cross-{d}", .{n});
        const json = try std.fmt.allocPrint(owned, "{{\"body\":\"hello\",\"n\":{d}}}", .{n});
        doc.* = .{ .id = id, .stored_data = json, .text_fields = &.{.{ .field_name = "body", .text = "hello" }} };
        const physical = try keys.documentKeyAlloc(owned, id);
        try db.core.store.put(physical, json);
        input.* = try observe(db.core.store, physical);
    }
    const bytes = try segment(a, &docs);
    defer a.free(bytes);
    const outputs = try index.prepareSourceSegmentCopies(a, &.{bytes});
    var submitted = false;
    defer if (!submitted) index.discardPreparedMergeSegments(outputs) else a.free(outputs);
    try std.testing.expectEqual(@as(usize, 0), index.snapshot().segments.len);
    var physical: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &physical, .{});
    const layout: family.Layout = .{ .segment = outputs[0].id, .documents = docs.len, .physical = physical, .configuration = @splat(124) };
    const cohorts = [_][]const recovery.Input{ inputs[0..64], inputs[64..] };
    const identities = [_][32]u8{ try layout.identity(0), try layout.identity(64) };
    const prepared = try recovery.prepareSourceLeaves(a, db.core.store, index, @splat(123), &cohorts, &identities, .{}, std.testing.io);
    defer {
        for (prepared) |*item| item.deinit(a);
        a.free(prepared);
    }
    // Claims alone never make an unowned physical segment visible.
    try std.testing.expectEqual(@as(usize, 0), index.snapshot().segments.len);
    var parent = try index.beginNewSourcePublication(std.testing.io);
    defer parent.publisher.deinit();
    submitted = true;
    _ = try index.appendPreparedSourceFamily(db.core.store, outputs[0], layout, prepared, &parent);
    try std.testing.expectEqual(@as(usize, 1), index.snapshot().segments.len);
    for (inputs) |input| try db.core.store.put(input.physical, "{\"body\":\"changed\",\"n\":-1}");
    {
        var result = try db.search(a, .{ .index_name = "text", .full_text = .{ .match_all = {} }, .limit = 65, .include_stored = true, .fields = &.{ "body", "n" }, .include_all_fields = false });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 65), result.hits.len);
        for (result.hits) |hit| {
            try std.testing.expect(hit.indexed_source == null);
            var json = try std.json.parseFromSlice(std.json.Value, a, hit.stored_data.?, .{});
            defer json.deinit();
            try std.testing.expectEqualStrings("hello", json.value.object.get("body").?.string);
            try std.testing.expectEqual(try std.fmt.parseInt(i64, hit.id[6..], 10), json.value.object.get("n").?.integer);
        }
    }
    {
        var result = try db.search(a, .{ .index_name = "text", .full_text = .{ .match_all = {} }, .filter_query_json = "{\"term\":{\"n\":64}}", .include_stored = true });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
        try std.testing.expectEqualStrings("cross-64", result.hits[0].id);
    }
}

test "persistent source publication ordinary replay preserves the producer read cut" {
    const benchmark = @import("builtin").link_libc and std.c.getenv("ANTFLY_BENCH_SOURCE_REPLAY") != null;
    const a = if (benchmark) @import("antfly_platform").allocator.processAllocator(std.heap.page_allocator) else std.testing.allocator;
    const exact = !benchmark or std.c.getenv("ANTFLY_BENCH_SOURCE_REPLAY_LEGACY") == null;
    const document_count: usize = if (benchmark) (if (std.c.getenv("ANTFLY_BENCH_SOURCE_DOCUMENTS")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else 65) else 65;
    if (document_count == 0 or document_count > 4099) return error.InvalidArgument;
    const requested_limit: usize = if (benchmark) (if (std.c.getenv("ANTFLY_BENCH_SOURCE_LIMIT")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else document_count) else document_count;
    if (requested_limit == 0 or requested_limit > document_count) return error.InvalidArgument;
    persistent.test_force_source_snapshot_reads = benchmark and std.c.getenv("ANTFLY_BENCH_SOURCE_FORCE_SNAPSHOT") != null;
    defer persistent.test_force_source_snapshot_reads = false;
    persistent.test_source_probe_reads.store(0, .monotonic);
    persistent.test_source_point_eligible.store(false, .monotonic);
    const clock = @import("antfly_platform").time;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const collectors = @import("db/document_collectors.zig");
    const derived = @import("db/derived/derived_types.zig");
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/replay-family", .{temporary.sub_path});
    defer a.free(path);
    var db = try db_mod.DB.open(a, path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(a, "{}");
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    const store = db.core.store;
    if (exact) {
        store.document_payload_prototype = true;
        // Reader/peer qualification remains test-local until the production
        // cohort and distributed transfer consumer are qualified together.
        const raw_schema = try store.get(a, schema.schema_key);
        defer a.free(raw_schema);
        const decoded = try schema.deserializeSchema(a, raw_schema);
        defer schema.freeSchema(a, decoded);
        const admitted = try schema.serializeSchemaWithMinimumFormat(a, decoded, 28);
        defer a.free(admitted);
        try store.put(schema.schema_key, admitted);
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    const documents = try owned.alloc(derived.DerivedDocument, document_count);
    for (documents, 0..) |*doc, n| {
        const id = try std.fmt.allocPrint(owned, "replayed-{d}", .{n});
        const original = try std.fmt.allocPrint(owned, "{{\"body\":\"original source\",\"n\":{d}}}", .{n});
        const physical = try keys.documentKeyAlloc(owned, id);
        try store.put(physical, original);
        doc.* = .{ .key = id, .cleaned_value = original, .targets = &.{.{ .kind = .full_text, .index_name = "text" }} };
    }
    const collect_start = clock.monotonicNs();
    var collected = try collectors.collectTextDocumentWritesForIndex(a, store, db.core.index_manager, documents, "text", false, .{ .start = "", .end = "" }, .{});
    defer collected.deinit();
    const collect_ns = clock.monotonicNs() - collect_start;
    try std.testing.expectEqual(document_count, collected.docs.items.len);
    try std.testing.expectEqual(exact, collected.docs.items[0].source_observation != null);
    const Replace = struct {
        fn run(allocator: A, primary: *ds.DocStore, rows: []const derived.DerivedDocument) !void {
            for (rows) |doc| {
                const key = try keys.documentKeyAlloc(allocator, doc.key);
                defer allocator.free(key);
                try primary.delete(key);
                // Current existence/authorization remain authoritative. Restore
                // an allowed row with different bytes before querying old hits.
                try primary.put(key, "{\"body\":\"changed\",\"n\":-1}");
            }
        }
    };
    if (exact and !benchmark) try Replace.run(a, store, documents);
    var context = try db.core.index_manager.acquireTextPublicationContext(a, "text");
    defer context.deinit();
    const prepare_start = clock.monotonicNs();
    var prepared = try db.core.index_manager.prepareTextMapperDocsPublication(a, store, "text", &context, collected.docs.items);
    defer prepared.deinit();
    const prepare_ns = clock.monotonicNs() - prepare_start;
    try std.testing.expectEqual(exact, prepared.source_families.len != 0);
    // In the qualified path preparation already imported exact claims. Drop
    // the collector's MVCC cut before current deletion and publication: staged
    // families must no longer depend on either source or catalog borrows.
    if (exact) {
        collected.deinit();
        collected = .{ .alloc = a };
        if (benchmark) try Replace.run(a, store, documents);
    }
    const publish_start = clock.monotonicNs();
    try db.core.index_manager.applyPreparedTextMapperPublicationByNameWithOptions(store, "text", &.{}, &prepared, .{});
    const publish_ns = clock.monotonicNs() - publish_start;
    const read_start = clock.monotonicNs();
    var result = try db.search(a, .{ .index_name = "text", .full_text = .{ .match_all = {} }, .include_stored = true, .limit = @intCast(requested_limit) });
    defer result.deinit();
    const read_ns = clock.monotonicNs() - read_start;
    try std.testing.expectEqual(requested_limit, result.hits.len);
    for (result.hits) |hit| {
        var value = try std.json.parseFromSlice(std.json.Value, a, hit.stored_data.?, .{});
        defer value.deinit();
        try std.testing.expectEqualStrings("original source", value.value.object.get("body").?.string);
        try std.testing.expectEqual(try std.fmt.parseInt(i64, hit.id[9..], 10), value.value.object.get("n").?.integer);
    }
    if (benchmark) std.debug.print("SOURCE_REPLAY documents={d} family={d} collect_ns={d} prepare_ns={d} publish_ns={d} read_ns={d} verified={d} production_allocator=1 optimize_mode={d} limit={d} forced_snapshot={d} probe_reads={d} point_eligible={d}\n", .{ document_count, @intFromBool(exact), collect_ns, prepare_ns, publish_ns, read_ns, result.hits.len, @backingInt(@import("builtin").mode), requested_limit, @intFromBool(persistent.test_force_source_snapshot_reads), persistent.test_source_probe_reads.load(.monotonic), @intFromBool(persistent.test_source_point_eligible.load(.monotonic)) });
}

test "persistent source publication staged family cleanup outlives its catalog owner" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/staged-family", .{temporary.sub_path}, 0);
    defer a.free(path);
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    store.document_payload_prototype = true;
    try initialize(a, &store);
    const physical = try keys.documentKeyAlloc(a, "staged");
    defer a.free(physical);
    try store.put(physical, "{\"body\":\"hello\"}");
    const input = try observe(&store, physical);
    const bytes = try segment(a, &.{.{ .id = "staged", .stored_data = "{}", .text_fields = &.{.{ .field_name = "body", .text = "hello" }} }});
    defer a.free(bytes);
    var index = try persistent.PersistentIndex.open(a, .{ .path = path });
    var open = true;
    defer if (open) index.close();
    const first = try index.ensureSourceOwner(std.testing.io);
    try std.testing.expectEqualDeep(first, try index.ensureSourceOwner(std.testing.io));
    var prepared = try index.prepareSourceFamily(a, &store, &.{input}, bytes, @splat(88), std.testing.io);
    const id = prepared.physical.?.id;
    try std.testing.expectEqual(@as(usize, 0), index.snapshot().segments.len);
    const artifact = try std.fmt.allocPrint(a, "{s}/segments/{d}.seg", .{ path, id });
    defer a.free(artifact);
    try std.Io.Dir.cwd().access(std.testing.io, artifact, .{});
    index.close();
    open = false;
    var reopened = try persistent.PersistentIndex.open(a, .{ .path = path });
    defer reopened.close();
    try std.testing.expectEqual(@as(usize, 0), reopened.snapshot().segments.len);
    var next = try reopened.prepareSourceFamily(a, &store, &.{input}, bytes, @splat(89), std.testing.io);
    var next_open = true;
    defer if (next_open) next.deinit();
    try std.testing.expect(next.physical.?.id > id);
    const next_artifact = try std.fmt.allocPrint(a, "{s}/segments/{d}.seg", .{ path, next.physical.?.id });
    defer a.free(next_artifact);
    // No movable PersistentIndex or catalog pointer is consulted on discard;
    // it must not unlink the new owner's independently reserved artifact.
    prepared.deinit();
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, artifact, .{}));
    try std.Io.Dir.cwd().access(std.testing.io, next_artifact, .{});
    next.deinit();
    next_open = false;
    var attempts: usize = 0;
    while (reopened.sourceOwnershipMaintenancePending() and attempts < 32) : (attempts += 1)
        _ = try reopened.runSourceOwnershipMaintenanceStep(&store, 64);
    try std.testing.expect(attempts < 32);
    var read = try store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqual(@as(u64, 0), (try claims.Budget.load(&read)).claims);
}

test "persistent source publication point-read admission accounts for cached snapshots" {
    const a = std.testing.allocator;
    var backend = lsm.Backend.init(a, .{ .flush_threshold = 1024 });
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    try std.testing.expect(!store.prefersImmutablePointReads(1));
    for (0..64) |n| {
        var key: [32]u8 = undefined;
        const name = try std.fmt.bufPrint(&key, "point-admission-{d}", .{n});
        try store.put(name, "value");
    }
    try std.testing.expect(store.prefersImmutablePointReads(1));
    try std.testing.expect(!store.prefersImmutablePointReads(64));
    var snapshot = try store.beginReadTxn();
    defer snapshot.abort();
    // An already-held snapshot is reusable without cloning mutable state.
    try std.testing.expect(!store.prefersImmutablePointReads(1));
}

test "persistent source publication batches dirty source cohorts into one generation" {
    const benchmark = @import("builtin").link_libc and std.c.getenv("ANTFLY_BENCH_SOURCE_COHORT_BATCH") != null;
    const sequential = benchmark and std.c.getenv("ANTFLY_BENCH_SOURCE_COHORT_SEQUENTIAL") != null;
    const a = if (benchmark) @import("antfly_platform").allocator.processAllocator(std.heap.page_allocator) else std.testing.allocator;
    const document_count: usize = if (benchmark) (if (std.c.getenv("ANTFLY_BENCH_SOURCE_DOCUMENTS")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else 131) else 131;
    const payload_bytes: usize = if (benchmark) (if (std.c.getenv("ANTFLY_BENCH_SOURCE_BYTES")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else 0) else 0;
    if (document_count < 131 or document_count > 4099 or document_count % 64 != 3 or payload_bytes > 16384) return error.InvalidArgument;
    const last: u32 = @intCast(document_count - 1);
    const last_first = last / 64 * 64;
    const clock = @import("antfly_platform").time;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/cohort-batch", .{temporary.sub_path}, 0);
    defer a.free(path);
    const primary_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/cohort-primary", .{temporary.sub_path});
    defer a.free(primary_path);
    var backend = try lsm.Backend.open(a, primary_path, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    try initialize(a, &store);
    var index = try persistent.PersistentIndex.open(a, .{ .path = path });
    defer index.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    const padding = try owned.alloc(u8, payload_bytes);
    @memset(padding, 'x');
    const original_bytes = try std.fmt.allocPrint(owned, "{{\"body\":\"hello\",\"metadata\":0,\"padding\":\"{s}\"}}", .{padding});
    const docs = try owned.alloc(intro.TextDocument, document_count);
    const inputs = try owned.alloc(recovery.Input, document_count);
    for (docs, inputs, 0..) |*doc, *input, n| {
        doc.* = .{ .id = try std.fmt.allocPrint(owned, "batch-{d:0>6}", .{n}), .stored_data = original_bytes, .text_fields = &.{.{ .field_name = "body", .text = "hello" }} };
        const physical = try keys.documentKeyAlloc(owned, doc.id);
        try store.put(physical, doc.stored_data);
        input.* = try observe(&store, physical);
    }
    const bytes = try segment(a, docs);
    defer a.free(bytes);
    var initial = try index.prepareSourceFamily(a, &store, inputs, bytes, @splat(161), std.testing.io);
    defer initial.deinit();
    const original = try initial.publish(&index, &store);
    const segment_id = index.snapshot().segments[0].id;
    const shared = index.snapshot().segments[0].shared;
    var old = try a.alloc(persistent.PersistentIndex.CapturedSource, 67);
    defer a.free(old);
    var captured: usize = 0;
    defer for (old[0..captured]) |*row| row.deinit();
    for (old, 0..) |*row, n| {
        const ordinal: u32 = @intCast(if (n < 64) n else last_first + n - 64);
        row.* = (try index.captureSource(&index.snapshot().segments[0], ordinal)).?;
        captured += 1;
    }
    const layout = blk: {
        const family = @import("index_source_family.zig");
        var physical: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &physical, .{});
        break :blk family.Layout{ .segment = segment_id, .documents = @intCast(docs.len), .physical = physical, .configuration = @splat(161) };
    };
    var changed_inputs: [67]recovery.Input = undefined;
    for (old, &changed_inputs) |*row, *input| input.* = .{ .root = row.reference.root, .from = .{ .published = .{ .reference = row.reference, .decision = row.decision, .lease = &row.lease.inner } } };
    const changed_bytes = try std.fmt.allocPrint(owned, "{{\"body\":\"hello\",\"metadata\":1,\"padding\":\"{s}\"}}", .{padding});
    try store.put(inputs[0].physical, changed_bytes);
    try store.put(inputs[last].physical, changed_bytes);
    changed_inputs[0] = try observe(&store, inputs[0].physical);
    changed_inputs[66] = try observe(&store, inputs[last].physical);
    const cohorts = [_][]const recovery.Input{ changed_inputs[0..64], changed_inputs[64..] };
    const identities = [_][32]u8{ try layout.identity(0), try layout.identity(last_first) };
    var measured: FaultPrimary = .{ .store = &store, .remaining = std.math.maxInt(usize) };
    const prepare_start = clock.monotonicNs();
    const prepared = try recovery.prepareSourceLeaves(a, &measured, &index, original.binding.owner, &cohorts, &identities, .{}, std.testing.io);
    defer {
        for (prepared) |*item| item.deinit(a);
        a.free(prepared);
    }
    const batch = [_]persistent.PersistentIndex.SourceCohortReplacement{
        .{ .first = 0, .prepared = &prepared[0] },
        .{ .first = last_first, .prepared = &prepared[1] },
    };
    const prepare_ns = clock.monotonicNs() - prepare_start;
    if (!benchmark) {
        // Fail validation of the second cohort after the first leaf/map was
        // staged. Aborting must preserve the entire original generation.
        const saved = prepared[1].references[0].document;
        prepared[1].references[0].document = @splat(0);
        defer prepared[1].references[0].document = saved;
        try std.testing.expectError(error.InvalidIndexSourceMap, index.replaceSourceCohorts(&measured, segment_id, original.root, &batch, std.testing.io));
        var unchanged = (try index.captureSource(&index.snapshot().segments[0], 0)).?;
        defer unchanged.deinit();
        var read = try store.beginReadTxn();
        defer read.abort();
        const source = try unchanged.read(a, &read);
        defer source.deinit(a);
        try std.testing.expectEqualStrings(original_bytes, source.bytes);
    }
    const before_syncs = measured.forced_syncs;
    const publish_start = clock.monotonicNs();
    const next = if (sequential) blk: {
        const first = try index.replaceSourceCohort(&measured, segment_id, batch[0].first, original.root, batch[0].prepared, std.testing.io);
        break :blk try index.replaceSourceCohort(&measured, segment_id, batch[1].first, first.root, batch[1].prepared, std.testing.io);
    } else try index.replaceSourceCohorts(&measured, segment_id, original.root, &batch, std.testing.io);
    const publish_ns = clock.monotonicNs() - publish_start;
    const barriers = measured.forced_syncs - before_syncs;
    try std.testing.expectEqual(@as(usize, if (sequential) 2 else 1), barriers);
    try std.testing.expect(shared == index.snapshot().segments[0].shared);
    const read_start = clock.monotonicNs();
    const retry = try index.replaceSourceCohorts(&measured, segment_id, original.root, &batch, std.testing.io);
    try std.testing.expectEqualDeep(next, retry);
    const reversed = [_]persistent.PersistentIndex.SourceCohortReplacement{ batch[1], batch[0] };
    try std.testing.expectError(error.InvalidIndexSourceFamily, index.replaceSourceCohorts(&measured, segment_id, next.root, &reversed, std.testing.io));
    for ([_]u32{ 0, 64, last }) |ordinal| {
        var row = (try index.captureSource(&index.snapshot().segments[0], ordinal)).?;
        defer row.deinit();
        var read = try store.beginReadTxn();
        defer read.abort();
        const source = try row.read(a, &read);
        defer source.deinit(a);
        try std.testing.expectEqualStrings(if (ordinal == 64) docs[64].stored_data else changed_bytes, source.bytes);
    }
    // Old readers still see the exact old generation across a multi-leaf CAS.
    var read = try store.beginReadTxn();
    defer read.abort();
    for ([_]usize{ 0, 66 }) |n| {
        const source = try old[n].read(a, &read);
        defer source.deinit(a);
        try std.testing.expectEqualStrings(docs[0].stored_data, source.bytes);
    }
    if (benchmark) std.debug.print("SOURCE_COHORT_BATCH documents={d} payload={d} sequential={d} cohorts=2 prepare_ns={d} publish_ns={d} verify_ns={d} barriers={d} verified=5 physical_shared=1 production_allocator=1 optimize_mode={d}\n", .{ document_count, payload_bytes, @intFromBool(sequential), prepare_ns, publish_ns, clock.monotonicNs() - read_start, barriers, @backingInt(@import("builtin").mode) });
}

test "persistent source publication foreground JSON shares replay publication and survives reopen" {
    try exerciseForegroundFamily(false);
}
test "persistent source publication foreground and replay coordinator writes admitted packed fields" {
    try exerciseForegroundFamily(true);
}
fn exerciseForegroundFamily(field_mode: bool) !void {
    const a = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/foreground-family", .{temporary.sub_path});
    defer a.free(path);
    const original = "{\"body\":\"indexed foreground\",\"metadata\":1}";
    for (0..2) |pass| {
        var db = try db_mod.DB.open(a, path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
        defer db.close();
        if (pass == 0) {
            try db.setSchemaJson(a, "{}");
            try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
            db.core.store.document_payload_prototype = true;
            const raw_schema = try db.core.store.get(a, schema.schema_key);
            defer a.free(raw_schema);
            const decoded = try schema.deserializeSchema(a, raw_schema);
            defer schema.freeSchema(a, decoded);
            const admitted = try schema.serializeSchemaWithMinimumFormat(a, decoded, if (field_mode) 29 else 28);
            defer a.free(admitted);
            try db.core.store.put(schema.schema_key, admitted);
            const physical = try keys.documentKeyAlloc(a, "foreground");
            defer a.free(physical);
            try db.core.store.put(physical, original);
            // Exercise the synchronous consumer, not the replay helper itself.
            try db.core.index_manager.indexTextBatchByName(db.core.store, "text", &.{.{ .key = "foreground", .value = original }});
            const index = &db.core.index_manager.text_indexes.items[0].persistent;
            try std.testing.expectEqual(@as(usize, 1), index.snapshot().segments.len);
            if (field_mode) try std.testing.expect(index.snapshot().segments[0].data == .authenticated);
            var retained = (try index.captureSource(&index.snapshot().segments[0], 0)).?;
            defer retained.deinit();
            // Current source changes after indexing; result bytes must stay
            // with the retained foreground cut, including after DB reopen.
            try db.core.store.put(physical, "{\"body\":\"new authority\",\"metadata\":2}");
            try db.core.store.sync(true);
        }
        var result = try db.search(a, .{ .index_name = "text", .full_text = .{ .match_all = {} }, .include_stored = true, .include_all_fields = true });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
        var value = try std.json.parseFromSlice(std.json.Value, a, result.hits[0].stored_data.?, .{});
        defer value.deinit();
        try std.testing.expectEqualStrings("indexed foreground", value.value.object.get("body").?.string);
        try std.testing.expectEqual(@as(i64, 1), value.value.object.get("metadata").?.integer);
    }
}

test "persistent source publication imported family rebases cohorts without receiver source authority" {
    try exerciseImportedFamily(false);
    try exerciseImportedFamily(true);
}

fn exerciseImportedFamily(flat_source: bool) !void {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const donor_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/family-donor", .{temporary.sub_path}, 0);
    defer a.free(donor_path);
    const receiver_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/family-receiver", .{temporary.sub_path}, 0);
    defer a.free(receiver_path);
    var from_backend = lsm.Backend.init(a, .{});
    defer from_backend.close();
    var to_backend = lsm.Backend.init(a, .{});
    defer to_backend.close();
    var from = try ds.DocStore.openRuntime(a, try from_backend.runtimeStore(a, .{}));
    defer from.close();
    var to = try ds.DocStore.openRuntime(a, try to_backend.runtimeStore(a, .{}));
    defer to.close();
    try initialize(a, &from);
    try initialize(a, &to);
    var donor = try persistent.PersistentIndex.open(a, .{ .path = donor_path });
    var donor_open = true;
    defer if (donor_open) donor.close();
    var receiver = try persistent.PersistentIndex.open(a, .{ .path = receiver_path });
    var receiver_open = true;
    defer if (receiver_open) receiver.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    var docs: [65]intro.TextDocument = undefined;
    var inputs: [65]recovery.Input = undefined;
    for (&docs, &inputs, 0..) |*doc, *input, n| {
        const id = try std.fmt.allocPrint(owned, "import-{d:0>3}", .{n});
        const json = try std.fmt.allocPrint(owned, "{{\"body\":\"donor original\",\"n\":{d}}}", .{n});
        doc.* = .{ .id = id, .stored_data = json, .text_fields = &.{.{ .field_name = "body", .text = "donor original" }} };
        const physical = try keys.documentKeyAlloc(owned, id);
        try from.put(physical, json);
        input.* = try observe(&from, physical);
        try to.put(physical, "{\"body\":\"receiver current\"}");
    }
    const bytes = try segment(a, &docs);
    defer a.free(bytes);
    if (flat_source) {
        var original = try recovery.prepareFresh(a, &from, &donor, @splat(172), &inputs, bytes, .{}, std.testing.io);
        defer original.deinit(a);
        _ = try recovery.publishOwned(a, &from, &donor, &original, try a.dupe(u8, bytes));
        try std.testing.expectError(error.IndexSourceConfigurationUnavailable, donor.sourceFamilyConfiguration(donor.snapshot().segments[0].id));
    } else {
        var original = try donor.prepareSourceFamily(a, &from, &inputs, bytes, @splat(173), std.testing.io);
        defer original.deinit();
        _ = try original.publish(&donor, &from);
    }
    for (inputs) |input| try from.delete(input.physical);
    try std.testing.expect(try donor.deleteById(docs[1].id));
    if (!flat_source) try std.testing.expectError(error.IndexPublicationConflict, receiver.prepareImportedSourceFamily(a, &to, &donor, &from, donor.snapshot().segments[0].id, @splat(174), std.testing.io));
    var imported = try receiver.prepareImportedSourceFamily(a, &to, &donor, &from, donor.snapshot().segments[0].id, @splat(173), std.testing.io);
    defer imported.deinit();
    try std.testing.expectEqual(@as(usize, 2), imported.leaves.len);
    try std.testing.expectEqual(@as(u64, 1), imported.deletions.?.cardinality());
    try std.testing.expectEqual(@as(usize, 0), receiver.snapshot().segments.len);
    // Preparation must own every receiver resource before the donor closes.
    donor.close();
    donor_open = false;
    const decision = try imported.publish(&receiver, &to);
    try decision.requireRole(.family);
    const imported_owner = decision.binding.owner;
    receiver.close();
    receiver_open = false;
    receiver = try persistent.PersistentIndex.open(a, .{ .path = receiver_path });
    receiver_open = true;
    try std.testing.expectEqual(@as(u32, 64), receiver.snapshot().liveDocCount());
    for ([_]u32{ 0, 63, 64 }) |ordinal| {
        var captured = (try receiver.captureSource(&receiver.snapshot().segments[0], ordinal)).?;
        defer captured.deinit();
        try std.testing.expectEqualDeep(imported_owner, captured.reference.binding.owner);
        var read = try to.beginReadTxn();
        defer read.abort();
        const source = try captured.read(a, &read);
        defer source.deinit(a);
        try std.testing.expectEqualStrings(docs[ordinal].stored_data, source.bytes);
        try std.testing.expectEqualStrings("{\"body\":\"receiver current\"}", try read.getPhysical(inputs[ordinal].physical));
    }
}

test "persistent source publication split manager imports whole and pruned families from donor cuts" {
    try exerciseManagerFamilySplit(false);
}
test "persistent source publication split manager imports whole and pruned packed families" {
    try exerciseManagerFamilySplit(true);
}
fn exerciseManagerFamilySplit(field_mode: bool) !void {
    const a = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const source_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/split-source", .{temporary.sub_path});
    defer a.free(source_path);
    const target_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/split-target", .{temporary.sub_path});
    defer a.free(target_path);
    var source = try db_mod.DB.open(a, source_path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer source.close();
    var target = try db_mod.DB.open(a, target_path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer target.close();
    for ([_]*db_mod.DB{ &source, &target }) |db| {
        try db.setSchemaJson(a, "{}");
        try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
        db.core.store.document_payload_prototype = true;
        const raw = try db.core.store.get(a, schema.schema_key);
        defer a.free(raw);
        const decoded = try schema.deserializeSchema(a, raw);
        defer schema.freeSchema(a, decoded);
        const admitted = try schema.serializeSchemaWithMinimumFormat(a, decoded, if (field_mode) 29 else 28);
        defer a.free(admitted);
        try db.core.store.put(schema.schema_key, admitted);
    }
    const rows = [_]@import("db/types.zig").BatchWrite{
        .{ .key = "a", .value = "{\"body\":\"left original\",\"n\":0}" },
        .{ .key = "z", .value = "{\"body\":\"right original\",\"n\":1}" },
        .{ .key = "zz", .value = "{\"body\":\"whole original\",\"n\":2}" },
    };
    for (rows) |row| {
        const key = try keys.documentKeyAlloc(a, row.key);
        defer a.free(key);
        try source.core.store.put(key, row.value);
        if (!std.mem.eql(u8, row.key, "a")) try target.core.store.put(key, "{\"body\":\"receiver authority\",\"n\":-1}");
    }
    try source.core.index_manager.indexTextBatchByNameWithOptions(source.core.store, "text", rows[0..2], .{ .compact_text = false });
    try source.core.index_manager.indexTextBatchByNameWithOptions(source.core.store, "text", rows[2..], .{ .compact_text = false });
    if (field_mode) for (source.core.index_manager.text_indexes.items[0].persistent.snapshot().segments) |entry| try std.testing.expect(entry.data == .authenticated);
    for (rows) |row| {
        const key = try keys.documentKeyAlloc(a, row.key);
        defer a.free(key);
        try source.core.store.delete(key);
    }
    const handoffs = try target.core.index_manager.handoffRightOnlyTextSegmentsFrom(source.core.index_manager, "m", true);
    defer {
        for (handoffs) |*handoff| handoff.deinit(a);
        a.free(handoffs);
    }
    try std.testing.expectEqual(@as(usize, 1), handoffs.len);
    try std.testing.expectEqual(@as(usize, 2), handoffs[0].transferred_segments);
    try std.testing.expectEqual(@as(u32, 2), handoffs[0].skip_doc_keys.count());
    try std.testing.expect(!handoffs[0].skip_doc_keys.contains("a"));
    const published = target.core.index_manager.text_indexes.items[0].persistent.writer.acquireSnapshot();
    defer published.release();
    if (field_mode) for (published.segments) |entry| try std.testing.expect(entry.data == .authenticated);
    const claims_before = blk: {
        var read = try target.core.store.beginReadTxn();
        defer read.abort();
        break :blk try claims.Budget.load(&read);
    };
    const retried = try target.core.index_manager.handoffRightOnlyTextSegmentsFrom(source.core.index_manager, "m", true);
    defer {
        for (retried) |*handoff| handoff.deinit(a);
        a.free(retried);
    }
    try std.testing.expectEqual(@as(usize, 2), retried[0].transferred_segments);
    try std.testing.expectEqual(@as(u32, 2), retried[0].skip_doc_keys.count());
    const after_retry = target.core.index_manager.text_indexes.items[0].persistent.writer.acquireSnapshot();
    defer after_retry.release();
    try std.testing.expectEqual(published.segments.len, after_retry.segments.len);
    {
        var read = try target.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqualDeep(claims_before, try claims.Budget.load(&read));
    }
    var result = try target.search(a, .{ .index_name = "text", .full_text = .{ .match_all = {} }, .include_stored = true, .include_all_fields = true });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    for (result.hits) |hit| {
        var value = try std.json.parseFromSlice(std.json.Value, a, hit.stored_data.?, .{});
        defer value.deinit();
        try std.testing.expect(value.value.object.get("n").?.integer > 0);
        try std.testing.expect(std.mem.indexOf(u8, value.value.object.get("body").?.string, "original") != null);
    }
    // A different donor generation cannot be appended over an already staged
    // live ID. This is a conflict requiring a fresh destination, not a second
    // visible hit or a grant to substitute the receiver's current row.
    try source.core.index_manager.deleteTextBatchByNameWithOptions("text", &.{"z"}, .{ .compact_text = false });
    try std.testing.expect(source.core.index_manager.text_indexes.items[0].compaction_pending.load(.acquire));
    const changed_key = try keys.documentKeyAlloc(a, "z");
    defer a.free(changed_key);
    const changed = "{\"body\":\"changed donor generation\",\"n\":3}";
    try source.core.store.put(changed_key, changed);
    try source.core.index_manager.indexTextBatchByNameWithOptions(source.core.store, "text", &.{.{ .key = "z", .value = changed }}, .{ .compact_text = false });
    try std.testing.expectError(error.IndexPublicationConflict, target.core.index_manager.handoffRightOnlyTextSegmentsFrom(source.core.index_manager, "m", true));
    const after_conflict = target.core.index_manager.text_indexes.items[0].persistent.writer.acquireSnapshot();
    defer after_conflict.release();
    try std.testing.expectEqual(after_retry.segments.len, after_conflict.segments.len);
}

test "persistent source publication public split preserves claimed JSON and copied format admission" {
    const a = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const source_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/public-split-source", .{temporary.sub_path});
    defer a.free(source_path);
    const target_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/public-split-target", .{temporary.sub_path});
    defer a.free(target_path);
    var source = try db_mod.DB.open(a, source_path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer source.close();
    try source.setSchemaJson(a, "{}");
    try source.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    source.core.store.document_payload_prototype = true;
    const raw = try source.core.store.get(a, schema.schema_key);
    defer a.free(raw);
    const decoded = try schema.deserializeSchema(a, raw);
    defer schema.freeSchema(a, decoded);
    const admitted = try schema.serializeSchemaWithMinimumFormat(a, decoded, 28);
    defer a.free(admitted);
    try source.core.store.put(schema.schema_key, admitted);
    try source.batch(.{ .writes = &.{
        .{ .key = "a", .value = "{\"body\":\"left original\",\"n\":0}" },
        .{ .key = "z", .value = "{\"body\":\"right original\",\"n\":1}" },
    }, .sync_level = .full_index });
    try source.split(source.getRange(), "m", "", target_path, true);
    var child = try db_mod.DB.open(a, target_path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer child.close();
    try std.testing.expect(child.core.store.document_payload_prototype);
    var result = try child.search(a, .{ .index_name = "text", .full_text = .{ .match_all = {} }, .include_stored = true, .include_all_fields = true });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("z", result.hits[0].id);
    var value = try std.json.parseFromSlice(std.json.Value, a, result.hits[0].stored_data.?, .{});
    defer value.deinit();
    try std.testing.expectEqualStrings("right original", value.value.object.get("body").?.string);
}

test "persistent source publication packed fields commit reopen retire and recover abandoned preparations" {
    const compiled = @import("index_field_inputs.zig");
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/packed-family", .{temporary.sub_path}, 0);
    defer a.free(path);
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    try initialize(a, &store);
    const physical = try keys.documentKeyAlloc(a, "packed");
    defer a.free(physical);
    try store.put(physical, "{\"body\":\"hello\"}");
    const input = try observe(&store, physical);
    const docs = [_]intro.TextDocument{.{ .id = "packed", .stored_data = "{}", .text_fields = &.{.{ .field_name = "body", .text = "hello" }}, .typed_fields = &.{} }};
    const bytes = try segment(a, &docs);
    defer a.free(bytes);
    var inputs = try compiled.prepare(a, &docs, .{}, @splat(91));
    defer inputs.deinit();
    var index = try persistent.PersistentIndex.open(a, .{ .path = path });
    defer index.close();
    var prepared = try index.prepareFieldSourceFamily(a, &store, &.{input}, bytes, @splat(91), &inputs, std.testing.io);
    var prepared_live = true;
    defer if (prepared_live) prepared.deinit();
    const id = prepared.layout.segment;
    const file = prepared.plan.resource.file;
    const pack_path = try std.fmt.allocPrint(a, "{s}/segments/{d}.seg", .{ path, file });
    defer a.free(pack_path);
    const original_reference = prepared.leaves[0].references[0];
    prepared.leaves[0].references[0].document = @splat(7);
    try std.testing.expectError(error.InvalidIndexSourceMap, prepared.publish(&index, &store));
    prepared.leaves[0].references[0] = original_reference;
    prepared.leaves[0].references[0].root = @splat(8);
    try std.testing.expectError(error.IndexPublicationConflict, prepared.publish(&index, &store));
    prepared.leaves[0].references[0] = original_reference;
    _ = try prepared.publish(&index, &store);
    _ = try prepared.publish(&index, &store);
    try std.testing.expectEqual(@as(usize, 1), index.snapshot().segments.len);
    try std.testing.expect(index.snapshot().segments[0].data == .authenticated);
    try std.testing.expect((try index.sourceFamilyLayoutAt(index.snapshot(), 0)).field_inventory != null);
    {
        const hits = try index.snapshot().search(a, "body", &.{"hello"}, 10);
        defer a.free(hits.hits);
        try std.testing.expectEqual(@as(u64, 1), hits.total_count);
    }
    {
        const clone_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/packed-checkpoint", .{temporary.sub_path}, 0);
        defer a.free(clone_path);
        const main_path = try std.fmt.allocPrint(a, "{s}/index", .{clone_path});
        defer a.free(main_path);
        const wal_path = try std.fmt.allocPrint(a, "{s}/wal", .{clone_path});
        defer a.free(wal_path);
        const segments_path = try std.fmt.allocPrint(a, "{s}/segments", .{clone_path});
        defer a.free(segments_path);
        var checkpoint = try index.pinNativeCheckpoints();
        defer checkpoint.deinit();
        _ = try checkpoint.main.materialize(std.testing.io, main_path, .none);
        _ = try checkpoint.wal.materialize(std.testing.io, wal_path, .none);
        try std.testing.expectEqual(prepared.plan.resource.physical_bytes, try checkpoint.segments.?.materialize(a, std.testing.io, segments_path, .none));
        var clone = try persistent.PersistentIndex.open(a, .{ .path = clone_path });
        defer clone.close();
        const hits = try clone.snapshot().search(a, "body", &.{"hello"}, 10);
        defer a.free(hits.hits);
        try std.testing.expectEqual(@as(u64, 1), hits.total_count);
    }
    {
        const receiver_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/packed-import", .{temporary.sub_path}, 0);
        defer a.free(receiver_path);
        var target_backend = lsm.Backend.init(a, .{});
        defer target_backend.close();
        var target = try ds.DocStore.openRuntime(a, try target_backend.runtimeStore(a, .{}));
        defer target.close();
        try initialize(a, &target);
        try target.put(physical, "{\"body\":\"receiver current\"}");
        var receiver = try persistent.PersistentIndex.open(a, .{ .path = receiver_path });
        defer receiver.close();
        var imported = try receiver.prepareImportedSourceFamily(a, &target, &index, &store, id, @splat(91), std.testing.io);
        defer imported.deinit();
        try std.testing.expect(imported.field_family != null);
        _ = try imported.publish(&receiver, &target);
        try std.testing.expect(receiver.snapshot().segments[0].data == .authenticated);
        const hits = try receiver.snapshot().search(a, "body", &.{"hello"}, 10);
        defer a.free(hits.hits);
        try std.testing.expectEqual(@as(u64, 1), hits.total_count);
        var captured = (try receiver.captureSource(&receiver.snapshot().segments[0], 0)).?;
        defer captured.deinit();
        var read = try target.beginReadTxn();
        defer read.abort();
        const source = try captured.read(a, &read);
        defer source.deinit(a);
        try std.testing.expectEqualStrings("{\"body\":\"hello\"}", source.bytes);
        try std.testing.expectEqualStrings("{\"body\":\"receiver current\"}", try read.getPhysical(physical));
    }
    prepared.deinit();
    prepared_live = false;
    // Reopen reads the retained field root, never a fictitious logical-ID file.
    index.close();
    index = try persistent.PersistentIndex.open(a, .{ .path = path });
    try std.testing.expect(index.snapshot().segments[0].data == .authenticated);
    const retained = index.acquireSnapshot();
    var retained_live = true;
    defer if (retained_live) retained.release();
    try index.removeSegments(&.{id});
    for (0..16) |_| _ = try index.collectSourceFamilyGarbage(&store, 64);
    try std.Io.Dir.cwd().access(std.testing.io, pack_path, .{});
    {
        const hits = try retained.search(a, "body", &.{"hello"}, 10);
        defer a.free(hits.hits);
        try std.testing.expectEqual(@as(u64, 1), hits.total_count);
    }
    retained.release();
    retained_live = false;
    for (0..32) |_| _ = try index.collectSourceFamilyGarbage(&store, 64);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, pack_path, .{}));
    // Exercise abandonment separately from a committed family.
    var abandoned = try index.prepareFieldSourceFamily(a, &store, &.{input}, bytes, @splat(91), &inputs, std.testing.io);
    const orphan = try std.fmt.allocPrint(a, "{s}/segments/{d}.seg", .{ path, abandoned.plan.resource.file });
    defer a.free(orphan);
    abandoned.deinit();
    for (0..16) |_| _ = try index.collectSourceFamilyGarbage(&store, 64);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, orphan, .{}));
}

test "persistent source publication ordinary packed writes reuse unchanged fields across reopen" {
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/field-reuse", .{temporary.sub_path});
    defer a.free(path);
    const physical = try keys.documentKeyAlloc(a, "same");
    defer a.free(physical);
    const body = try a.alloc(u8, 65536);
    defer a.free(body);
    @memset(body, 'x');
    var prior_file: ?u64 = null;
    for (0..2) |open_pass| {
        var db = try db_mod.DB.open(a, path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
        defer db.close();
        if (open_pass == 0) {
            try db.setSchemaJson(a, "{}");
            try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
            db.core.store.document_payload_prototype = true;
            const raw = try db.core.store.get(a, schema.schema_key);
            defer a.free(raw);
            const decoded = try schema.deserializeSchema(a, raw);
            defer schema.freeSchema(a, decoded);
            const admitted = try schema.serializeSchemaWithMinimumFormat(a, decoded, 29);
            defer a.free(admitted);
            try db.core.store.put(schema.schema_key, admitted);
        }
        const index = &db.core.index_manager.text_indexes.items[0].persistent;
        const first: usize = if (open_pass == 0) 1 else 4;
        for (first..4 + open_pass) |version| {
            const json = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":{d}}}", .{ body, version });
            defer a.free(json);
            try db.core.store.put(physical, json);
            if (version != 1) _ = try index.deleteById("same");
            try db.core.index_manager.indexTextBatchByName(db.core.store, "text", &.{.{ .key = "same", .value = json }});
            const corpus = index.snapshot();
            const latest = &corpus.segments[corpus.segments.len - 1];
            try std.testing.expect(latest.data == .authenticated);
            var inventory = try latest.data.authenticated.inventory();
            defer inventory.deinit();
            const directory = try inventory.directory();
            const evidence = try index.copyFieldInputLedger(a, inventory, "body");
            defer if (evidence) |raw| a.free(raw);
            if (version == 1) try std.testing.expect(evidence == null);
            if (version >= 3) try std.testing.expect(evidence != null);
            var file: ?u64 = null;
            for (0..directory.fields) |ordinal| {
                const field = try inventory.field(ordinal);
                if (!std.mem.eql(u8, field.name(), "body")) continue;
                file = (try inventory.fieldResource(ordinal, 0)).file;
            }
            try std.testing.expect(file != null);
            // The first observation can install inferred analyzer definitions.
            // Once those definitions stabilize, subsequent metadata edits and
            // process reopen must keep the same physical body resource.
            if (version >= 3) {
                try std.testing.expectEqual(prior_file.?, file.?);
                try std.testing.expect(db.core.index_manager.text_indexes.items[0].field_compilations_avoided.load(.monotonic) != 0);
            }
            prior_file = file;
            var result = try db.search(a, .{ .index_name = "text", .full_text = .{ .match_all = {} }, .include_stored = true, .include_all_fields = true });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.hits.len);
            var parsed = try std.json.parseFromSlice(std.json.Value, a, result.hits[0].stored_data.?, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(i64, @intCast(version)), parsed.value.object.get("metadata").?.integer);
        }
        if (open_pass == 1) {
            // Only a tiny metadata field survives a body replacement. Sharing
            // it must not retain the obsolete large body pack in the new root.
            @memset(body, 'y');
            const changed = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":4}}", .{body});
            defer a.free(changed);
            try db.core.store.put(physical, changed);
            _ = try index.deleteById("same");
            try db.core.index_manager.indexTextBatchByName(db.core.store, "text", &.{.{ .key = "same", .value = changed }});
            const corpus = index.snapshot();
            var inventory = try corpus.segments[corpus.segments.len - 1].data.authenticated.inventory();
            defer inventory.deinit();
            const changed_evidence = try index.copyFieldInputLedger(a, inventory, "body");
            defer if (changed_evidence) |raw| a.free(raw);
            try std.testing.expect(changed_evidence == null);
            try std.testing.expectEqual(@as(usize, 1), try inventory.resourceCount());
            try std.testing.expect((try inventory.resourceDescriptor(0)).file != prior_file.?);
        }
    }
}

test "persistent source publication compiler reuse survives compaction and changed row layouts" {
    try exerciseCompilerReuseLayout(false);
    try exerciseCompilerReuseLayout(true);
}
fn exerciseCompilerReuseLayout(sorted: bool) !void {
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/layout-reuse", .{temporary.sub_path});
    defer a.free(path);
    const body = try a.alloc(u8, 128 * 1024);
    defer a.free(body);
    for (body, 0..) |*byte, index| byte.* = "alpha "[index % 6];
    for (0..2) |pass| {
        var db = try db_mod.DB.open(a, path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
        defer db.close();
        if (pass == 0) {
            try db.setSchemaJson(a, if (sorted)
                "{\"version\":1,\"default_type\":\"doc\",\"enforce_types\":false,\"index_sort\":[{\"field\":\"metadata\",\"order\":\"asc\"},{\"field\":\"_id\",\"order\":\"asc\"}],\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"body\":{\"type\":\"string\",\"x-antfly-field\":{\"type\":\"text\"}},\"metadata\":{\"type\":\"number\",\"x-antfly-field\":{\"type\":\"number\",\"sortable\":true}}}}}}}"
            else
                "{}");
            try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
            db.core.store.document_payload_prototype = true;
            const raw = try db.core.store.get(a, schema.schema_key);
            defer a.free(raw);
            const decoded = try schema.deserializeSchema(a, raw);
            defer schema.freeSchema(a, decoded);
            const admitted = try schema.serializeSchemaWithMinimumFormat(a, decoded, 29);
            defer a.free(admitted);
            try db.core.store.put(schema.schema_key, admitted);
            // Stabilize inferred field configuration before producing rows.
            const seed_key = try keys.documentKeyAlloc(a, "seed");
            defer a.free(seed_key);
            const seed = "{\"body\":\"alpha\",\"metadata\":0}";
            try db.core.store.put(seed_key, seed);
            try db.core.index_manager.indexTextBatchByName(db.core.store, "text", &.{.{ .key = "seed", .value = seed }});
            _ = try db.core.index_manager.text_indexes.items[0].persistent.deleteById("seed");
            for ([_][]const u8{ "zeta", "alpha", "middle" }) |id| {
                const key = try keys.documentKeyAlloc(a, id);
                defer a.free(key);
                const json = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":1}}", .{body});
                defer a.free(json);
                try db.core.store.put(key, json);
                try db.core.index_manager.indexTextBatchByName(db.core.store, "text", &.{.{ .key = id, .value = json }});
            }
        }
        const manager = db.core.index_manager;
        const entry = &manager.text_indexes.items[0];
        if (pass == 0) {
            // Demand admission records composable evidence only after reuse
            // is observed. Warm each body's proof before row consolidation.
            for ([_][]const u8{ "zeta", "alpha", "middle" }) |id| {
                const warm_key = try keys.documentKeyAlloc(a, id);
                defer a.free(warm_key);
                const warm = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":0}}", .{body});
                defer a.free(warm);
                try db.core.store.put(warm_key, warm);
                _ = try entry.persistent.deleteById(id);
                try manager.indexTextBatchByName(db.core.store, "text", &.{.{ .key = id, .value = warm }});
            }
        }
        try manager.forceCompactAllTextIndexes();
        try std.testing.expectEqual(@as(usize, 1), entry.persistent.snapshot().segments.len);
        const old_count = entry.field_compilations_avoided.load(.monotonic);
        const key = try keys.documentKeyAlloc(a, "middle");
        defer a.free(key);
        const json = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":{d}}}", .{ body, pass + 2 });
        defer a.free(json);
        try db.core.store.put(key, json);
        _ = try entry.persistent.deleteById("middle");
        try manager.indexTextBatchByName(db.core.store, "text", &.{.{ .key = "middle", .value = json }});
        try std.testing.expect(entry.field_compilations_avoided.load(.monotonic) > old_count);
        var result = try db.search(a, .{ .index_name = "text", .full_text = .{ .match_all = {} }, .include_stored = true, .include_all_fields = true });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 3), result.hits.len);
        const hits = try entry.persistent.snapshot().search(a, "body", &.{"alpha"}, 10);
        defer a.free(hits.hits);
        try std.testing.expectEqual(@as(u64, 3), hits.total_count);
    }
}

test "persistent source publication remapped field sections match fresh compilation in reverse order" {
    const a = std.testing.allocator;
    const docs = [_]intro.TextDocument{
        .{ .id = "a", .stored_data = "{}", .text_fields = &.{.{ .field_name = "body", .text = "alpha beta alpha" }}, .typed_fields = &.{} },
        .{ .id = "b", .stored_data = "{}", .text_fields = &.{.{ .field_name = "body", .text = "gamma beta" }}, .typed_fields = &.{} },
        .{ .id = "c", .stored_data = "{}", .text_fields = &.{.{ .field_name = "body", .text = "delta" }}, .typed_fields = &.{} },
    };
    const bytes = try segment(a, &docs);
    defer a.free(bytes);
    var reader = try @import("../segment.zig").SegmentReader.init(a, bytes);
    defer reader.deinit();
    var remapped = try @import("../segment.zig").remapFieldAlloc(a, &reader, "body", &.{ 1, 0 });
    defer remapped.deinit(a);
    const fresh = try segment(a, &.{ docs[1], docs[0] });
    defer a.free(fresh);
    var fresh_reader = try @import("../segment.zig").SegmentReader.init(a, fresh);
    defer fresh_reader.deinit();
    const expected = (try fresh_reader.sectionView("body", .inverted_text)).?;
    const actual = remapped.sections[0];
    const wanted = try a.alloc(u8, @intCast(expected.length));
    defer a.free(wanted);
    try expected.readInto(0, wanted);
    try std.testing.expectEqualSlices(u8, wanted, remapped.bytes[@intCast(actual.offset)..][0..@intCast(actual.length)]);
    try std.testing.expectEqualDeep(try fresh_reader.invertedFieldStats("body"), remapped.stats);
}

test "persistent source publication ledger rejects tampering and preserves unknown rows" {
    const ledger = @import("index_field_ledger.zig");
    const a = std.testing.allocator;
    const rows = [_]?[32]u8{ @splat(3), null, @splat(9) };
    const raw = try ledger.encode(a, @splat(7), &rows);
    defer a.free(raw);
    const expected = ledger.digest(@splat(7), &rows);
    const view = try ledger.open(raw, expected);
    try std.testing.expectEqualDeep(rows[0], try view.row(0));
    try std.testing.expectEqual(@as(?[32]u8, null), try view.row(1));
    try std.testing.expectError(error.InvalidIndexFieldGeneration, view.row(3));
    raw[raw.len - 1] ^= 1;
    try std.testing.expectError(error.IndexPublicationFenceMismatch, ledger.open(raw, expected));
}
