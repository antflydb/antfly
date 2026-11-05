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

//! Native transaction and cold-recovery tests of scope reservation custody.
//! These exercise the journal only, not physical child completion authority.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const contract = domain.scope_retirement;
const ledger = @import("scope_retirement_ledger.zig");
const native = @import("storage/raft_apply_store.zig");
const topology = @import("topology_protocol.zig");
const docstore = local.storage_docstore;
const group = 21;

fn seed(store: *native.RaftApplyStore, a: std.mem.Allocator) !void {
    return seedWithVersion(store, a, topology.current_version);
}
fn seedWithVersion(store: *native.RaftApplyStore, a: std.mem.Allocator, version: u16) !void {
    try store.applyStandaloneCommand(group, .{ .initialize_metadata_incarnation = @splat('1') });
    try store.applyStandaloneCommand(group, .{ .activate_topology_protocol = try std.json.Stringify.valueAlloc(a, topology.Activation{ .version = version, .incarnation = @splat('1'), .member_count = 1, .membership_fingerprint = @splat(7) }, .{}) });
    try store.applyStandaloneCommand(group, .{ .apply_system_catalog = try std.json.Stringify.valueAlloc(a, native.SystemCatalogCommand{ .expected_revision = 0, .mutation = .{ .action = .create, .kind = .database, .name = "tenant_ops" } }, .{}) });
    for (0..2) |i| {
        const id: u64 = 101 + i;
        const name = try std.fmt.allocPrint(a, "table:{d}", .{id});
        const table: @import("domain.zig").TableRecord = .{ .table_id = id, .name = name, .min_ranges = 1 };
        const ranges = [_]@import("domain.zig").RangeRecord{.{ .table_id = id, .group_id = 301 + i, .range_id = 301 + i, .start_key = "" }};
        const command: native.SystemCatalogCommand = .{ .expected_revision = 1 + i, .mutation = .{ .action = .create, .kind = .table, .database = "tenant_ops", .name = if (i == 0) "first" else "second", .table_id = id, .storage_name = name }, .topology = .{ .create = .{ .expected_transition_generation = 0, .table = table, .ranges = &ranges } } };
        try store.applyStandaloneCommand(group, .{ .apply_system_catalog = try std.json.Stringify.valueAlloc(a, command, .{}) });
    }
}
const Fault = struct {
    txn: *docstore.DocStore.Txn,
    remaining: usize,
    pub fn get(self: *Fault, key: []const u8) ![]const u8 {
        return self.txn.get(key);
    }
    pub fn openCursor(self: *Fault) !docstore.DocStore.Txn.CursorAdapter {
        return self.txn.openCursor();
    }
    pub fn put(self: *Fault, key: []const u8, value: []const u8) !void {
        if (self.remaining == 0) return error.InjectedFailure;
        self.remaining -= 1;
        try self.txn.put(key, value);
    }
    pub fn delete(self: *Fault, key: []const u8) !void {
        if (self.remaining == 0) return error.InjectedFailure;
        self.remaining -= 1;
        try self.txn.delete(key);
    }
};

fn absent(store: *native.RaftApplyStore, lookup: ledger.Lookup) !void {
    var read = try store.store.beginReadTxn();
    defer read.abort();
    try std.testing.expect(try ledger.head(&read, lookup) == null);
    try std.testing.expect(try ledger.member(&read, group, .database, lookup.reference.database) == null);
    for (0..lookup.reference.tables) |ordinal| try std.testing.expect(try ledger.child(&read, lookup, @intCast(ordinal)) == null);
}

test "SQL scope retirement journal atomically owns membership exact retries cold custody and terminal release" {
    const a = std.testing.allocator;
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(scratch, ".zig-cache/tmp/{s}/scope-retirement-journal", .{tmp.sub_path});
    var encoded: []const u8 = undefined;
    var expected: ledger.Head = undefined;
    for (0..4) |pass| {
        var store = try native.RaftApplyStore.init(a, .{ .root_dir = path });
        defer store.deinit();
        if (pass == 0) {
            try seed(&store, scratch);
            encoded = try store.prepareScopeRetirementManifestJsonWithContext(scratch, group, .{ .kind = .database, .name = "tenant_ops", .creator = .bootstrap }, .{});
        }
        var prepared = try ledger.prepare(a, group, @splat(6), encoded);
        defer prepared.deinit();
        if (pass == 0) {
            const puts = 5 + prepared.manifest.resources.len + prepared.lookup.reference.tables;
            for (0..puts) |fail_at| {
                var txn = try store.store.beginWriteTxn();
                var open = true;
                defer if (open) txn.abort();
                var fault: Fault = .{ .txn = &txn, .remaining = fail_at };
                try std.testing.expectError(error.InjectedFailure, ledger.stageBegin(&fault, &prepared));
                txn.abort();
                open = false;
                try absent(&store, prepared.lookup);
            }
            var txn = try store.store.beginWriteTxn();
            var committed = false;
            defer if (!committed) txn.abort();
            expected = try ledger.stageBegin(&txn, &prepared);
            try txn.commit();
            committed = true;
        } else {
            var txn = try store.store.beginWriteTxn();
            var committed = false;
            defer if (!committed) txn.abort();
            const observed = (try ledger.existing(&txn, &prepared)).?;
            try std.testing.expectEqualDeep(expected, observed);
            try std.testing.expectEqualDeep(expected, try ledger.stageBegin(&txn, &prepared));
            var other = try ledger.prepare(a, group, @splat(7), encoded);
            defer other.deinit();
            try std.testing.expectError(error.TableTransitionActive, ledger.stageBegin(&txn, &other));
            const next = switch (pass) {
                1 => try observed.progress.drained(),
                2 => try observed.progress.tablePublished(0),
                3 => try observed.progress.tablePublished(1),
                else => unreachable,
            };
            expected = try ledger.stageAdvance(&txn, &prepared, observed, next);
            try std.testing.expectError(error.CatalogGenerationChanged, ledger.stageAdvance(&txn, &prepared, observed, next));
            try txn.commit();
            committed = true;
        }
    }
    var store = try native.RaftApplyStore.init(a, .{ .root_dir = path });
    defer store.deinit();
    var prepared = try ledger.prepare(a, group, @splat(6), encoded);
    defer prepared.deinit();
    try std.testing.expectEqual(contract.Phase.publishing_catalog, expected.progress.phase);
    // The native apply owner separately verifies actual publication. Here the
    // journal only proves atomic terminal custody, not a fabricated owner ACK.
    const next = try expected.progress.published(prepared.manifest.catalog_revision + 1, prepared.manifest.catalog_revision);
    const writes = 4 + prepared.manifest.resources.len + prepared.lookup.reference.tables;
    for (0..writes) |fail_at| {
        var txn = try store.store.beginWriteTxn();
        var open = true;
        defer if (open) txn.abort();
        var fault: Fault = .{ .txn = &txn, .remaining = fail_at };
        try std.testing.expectError(error.InjectedFailure, ledger.stageAdvance(&fault, &prepared, expected, next));
        txn.abort();
        open = false;
        var read = try store.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqualDeep(expected, (try ledger.existing(&read, &prepared)).?);
    }
    {
        var txn = try store.store.beginWriteTxn();
        var committed = false;
        defer if (!committed) txn.abort();
        expected = try ledger.stageAdvance(&txn, &prepared, expected, next);
        try txn.commit();
        committed = true;
    }
    var read = try store.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqualDeep(expected, (try ledger.existing(&read, &prepared)).?);
    try std.testing.expect(try ledger.member(&read, group, .database, prepared.lookup.reference.database) == null);
    for (prepared.manifest.resources) |resource| try std.testing.expect(try ledger.member(&read, group, resource.kind, resource.id) == null);
}

test "SQL scope retirement journal rejects cross group replay and inconsistent queue counters" {
    const a = std.testing.allocator;
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(scratch, ".zig-cache/tmp/{s}/scope-retirement-corruption", .{tmp.sub_path});
    var store = try native.RaftApplyStore.init(a, .{ .root_dir = path });
    defer store.deinit();
    try seed(&store, scratch);
    const encoded = try store.prepareScopeRetirementManifestJsonWithContext(scratch, group, .{ .kind = .database, .name = "tenant_ops", .creator = .bootstrap }, .{});
    try std.testing.expectError(error.InvalidScopeRetirement, ledger.prepare(a, group + 1, @splat(6), encoded));
    var prepared = try ledger.prepare(a, group, @splat(6), encoded);
    defer prepared.deinit();
    var count: [8]u8 = undefined;
    std.mem.writeInt(u64, &count, 1, .little);
    const queued = try ledger.QueueRecord.init(prepared.lookup);
    var buffer: [256]u8 = undefined;
    {
        var txn = try store.store.beginWriteTxn();
        var committed = false;
        defer if (!committed) txn.abort();
        // Deliberately inconsistent recovery state: sequence 2 already owns
        // a queue row while the counter says 1. Never overwrite that work.
        try txn.put(try ledger.counterKey(&buffer, group), &count);
        try txn.put(try ledger.queueKey(&buffer, group, 2), queued.slice());
        try txn.commit();
        committed = true;
    }
    {
        var txn = try store.store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.InvalidScopeRetirement, ledger.stageBegin(&txn, &prepared));
    }
    try absent(&store, prepared.lookup);
    var read = try store.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqualSlices(u8, &count, try read.get(try ledger.counterKey(&buffer, group)));
    try std.testing.expectEqualSlices(u8, queued.slice(), try read.get(try ledger.queueKey(&buffer, group, 2)));
}

const wire = @import("scope_retirement_wire.zig");
const lifecycle = @import("table_lifecycle_lock.zig");
const dependencies = @import("schema_validation_dependencies.zig");
const AdmissionHarness = struct {
    fn request(store: *native.RaftApplyStore, a: std.mem.Allocator, nonce: u8) !wire.Request {
        const bytes = try store.prepareScopeRetirementManifestJsonWithContext(a, group, .{ .kind = .database, .name = "tenant_ops", .creator = .bootstrap }, .{});
        const manifest = try std.json.parseFromSliceLeaky(contract.Manifest, a, bytes, .{});
        return .{ .admission = .{ .nonce = @splat(nonce), .head = manifest.catalog_revision }, .operation = .{ .reserve = manifest } };
    }
    fn apply(store: *native.RaftApplyStore, a: std.mem.Allocator, request_value: wire.Request, index: u64, replicated: bool) !wire.Observation {
        const bytes = try request_value.encodeAlloc(a);
        var input = try wire.Input.parse(a, group, bytes);
        defer input.deinit();
        const encoded = try native.encodeTransitionCommand(a, .{ .apply_scope_retirement = bytes });
        var decoded = (try native.decodeTransitionCommand(a, encoded)) orelse return error.InvalidMetadataTransitionEncoding;
        defer decoded.deinit(a);
        try std.testing.expectEqualSlices(u8, bytes, decoded.apply_scope_retirement);
        if (replicated) {
            const raft = @import("../raft/state_machine/mod.zig");
            const entries = try raft.encodeCommittedEntries(a, &.{.{ .term = 1, .index = index, .entry_type = .normal, .data = encoded }});
            try store.snapshotBuilder().applyBatch(.{ .group_id = group, .commit_index = index, .entries_bytes = entries });
        } else try store.applyStandaloneCommand(group, decoded);
        const observed = try std.json.parseFromSliceLeaky(wire.Observation, a, try store.scopeRetirementObservationJson(a, group, input.query), .{});
        try observed.requireFor(input.query);
        return observed;
    }
    fn cancel(lookup: ledger.Lookup, nonce: u8) wire.Request {
        return .{ .admission = .{ .nonce = @splat(nonce), .head = 0 }, .operation = .{ .cancel = .{ .lookup = lookup, .expected_revision = 1 } } };
    }
};

test "SQL scope retirement native admission retains exact outcomes and cancellation across cold standalone Raft and snapshot owners" {
    const a = std.testing.allocator;
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |replicated| {
        const path = try std.fmt.allocPrint(scratch, ".zig-cache/tmp/{s}/admission-{any}", .{ tmp.sub_path, replicated });
        var request: wire.Request = undefined;
        var original: wire.Outcome = undefined;
        var cancel: wire.Request = undefined;
        var terminal: wire.Outcome = undefined;
        for (0..3) |pass| {
            var store = try native.RaftApplyStore.init(a, .{ .root_dir = path });
            defer store.deinit();
            if (pass == 0) {
                try seed(&store, scratch);
                request = try AdmissionHarness.request(&store, scratch, 71);
                const bytes = try request.encodeAlloc(scratch);
                const checked = try store.scopeRetirementPreflightJson(scratch, group, bytes);
                const query = try std.json.parseFromSliceLeaky(wire.Query, scratch, checked, .{});
                // An aborting preflight may not own membership or an outcome.
                const prior = try std.json.parseFromSliceLeaky(wire.Observation, scratch, try store.scopeRetirementObservationJson(scratch, group, query), .{});
                try std.testing.expect(prior.outcome == null and prior.current == null);
                const observed = try AdmissionHarness.apply(&store, scratch, request, 100, replicated);
                original = observed.outcome.?;
                try std.testing.expectEqual(contract.Phase.draining_connections, original.result.accepted.progress.phase);
                try std.testing.expectEqualDeep(original, (try AdmissionHarness.apply(&store, scratch, request, 101, replicated)).outcome.?);
                const work = (try std.json.parseFromSliceLeaky(?ledger.Work, scratch, try store.scopeRetirementWorkJson(scratch, group, std.math.maxInt(u64)), .{})).?;
                try std.testing.expectEqualDeep(original.result.accepted, work.head);
                {
                    const PointReader = struct {
                        txn: *docstore.DocStore.Txn,
                        gets: usize = 0,
                        cursors: usize = 0,
                        pub fn get(self: *@This(), name: []const u8) ![]const u8 {
                            // Hot polling must remain independent of manifest
                            // bytes and membership cardinality.
                            if (std.mem.endsWith(u8, name, ":manifest")) return error.TestUnexpectedResult;
                            self.gets += 1;
                            return self.txn.get(name);
                        }
                        pub fn openCursor(self: *@This()) !docstore.DocStore.Txn.CursorAdapter {
                            self.cursors += 1;
                            return self.txn.openCursor();
                        }
                    };
                    var read = try store.store.beginReadTxn();
                    defer read.abort();
                    var measured: PointReader = .{ .txn = &read };
                    const polled = (try ledger.nextOwnerWork(&measured, group, 0)).?;
                    try std.testing.expectEqualDeep(work, polled);
                    try std.testing.expect(measured.gets <= 3);
                    try std.testing.expectEqual(@as(usize, 1), measured.cursors);
                }
                const selected = work.child orelse return error.TestUnexpectedResult;
                try std.testing.expectEqual(@as(u16, 0), selected.ordinal);
                try std.testing.expectEqual(@as(u64, 101), selected.table);
                try std.testing.expectEqualSlices(u8, &try original.query.lookup.reference.childId(101), &selected.id);
                const retained = try std.json.parseFromSliceLeaky(contract.Manifest, scratch, try store.scopeRetirementManifestJson(scratch, group, original.query.lookup), .{});
                try std.testing.expectEqualDeep(original.query.lookup.reference, try retained.reference(scratch, original.query.lookup.reference.id));
                try std.testing.expectError(error.CatalogPublicationProofPending, store.exportSystemCatalog(scratch, group));
                const active_snapshot = try store.snapshotBuilder().buildSnapshot(scratch, group);
                const active_copy_path = try std.fmt.allocPrint(scratch, "{s}-active-copy", .{path});
                var active_copy = try native.RaftApplyStore.init(a, .{ .root_dir = active_copy_path });
                defer active_copy.deinit();
                try std.testing.expect(try active_copy.snapshotBuilder().installSnapshot(scratch, group, 101, active_snapshot));
                const active_work = try std.json.parseFromSliceLeaky(?ledger.Work, scratch, try active_copy.scopeRetirementWorkJson(scratch, group, 0), .{});
                try std.testing.expectEqualDeep(work, active_work.?);
                try std.testing.expectEqualStrings(try store.scopeRetirementManifestJson(scratch, group, original.query.lookup), try active_copy.scopeRetirementManifestJson(scratch, group, original.query.lookup));
                cancel = AdmissionHarness.cancel(original.query.lookup, 72);
            } else if (pass == 1) {
                const observed = try AdmissionHarness.apply(&store, scratch, request, 102, replicated);
                try std.testing.expectEqualDeep(original, observed.outcome.?);
                terminal = (try AdmissionHarness.apply(&store, scratch, cancel, 103, replicated)).outcome.?;
                try std.testing.expectEqual(contract.Phase.canceled, terminal.result.accepted.progress.phase);
            } else {
                try std.testing.expectEqualDeep(terminal, (try AdmissionHarness.apply(&store, scratch, cancel, 104, replicated)).outcome.?);
                const retried = try AdmissionHarness.apply(&store, scratch, request, 105, replicated);
                try std.testing.expectEqualDeep(original, retried.outcome.?);
                try std.testing.expectEqual(contract.Phase.canceled, retried.current.?.progress.phase);
                try std.testing.expectEqualStrings("null", try store.scopeRetirementWorkJson(scratch, group, 0));
                try std.testing.expectEqualStrings("null", try store.scopeRetirementManifestJson(scratch, group, original.query.lookup));
                var read = try store.store.beginReadTxn();
                try std.testing.expect(try ledger.member(&read, group, .database, original.query.lookup.reference.root) == null);
                read.abort();
                const snapshot = try store.snapshotBuilder().buildSnapshot(scratch, group);
                const copy_path = try std.fmt.allocPrint(scratch, "{s}-copy", .{path});
                var copy = try native.RaftApplyStore.init(a, .{ .root_dir = copy_path });
                defer copy.deinit();
                try std.testing.expect(try copy.snapshotBuilder().installSnapshot(scratch, group, 105, snapshot));
                const recovered = try std.json.parseFromSliceLeaky(wire.Observation, scratch, try copy.scopeRetirementObservationJson(scratch, group, original.query), .{});
                try std.testing.expectEqualDeep(retried, recovered);
            }
        }
    }
}

test "SQL scope retirement native admission fences capability stale cuts nonce substitution and intersecting catalog writes" {
    const a = std.testing.allocator;
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(scratch, ".zig-cache/tmp/{s}/admission-fences", .{tmp.sub_path});
    var store = try native.RaftApplyStore.init(a, .{ .root_dir = path });
    defer store.deinit();
    try seedWithVersion(&store, scratch, topology.sql_scope_retirement_version - 1);
    const request = try AdmissionHarness.request(&store, scratch, 81);
    // The previous decoder cannot accept the operation, even when every
    // ordinary contained table can already be decoded by that version.
    try store.applyStandaloneCommand(group, .{ .activate_topology_protocol = try std.json.Stringify.valueAlloc(scratch, topology.Activation{ .version = topology.sql_scope_retirement_version - 1, .incarnation = @splat('1'), .member_count = 1, .membership_fingerprint = @splat(7) }, .{}) });
    const bytes = try request.encodeAlloc(scratch);
    try std.testing.expectError(error.TableTopologyProtocolUpgradeRequired, store.scopeRetirementPreflightJson(scratch, group, bytes));
    try std.testing.expectError(error.TableTopologyProtocolUpgradeRequired, store.applyStandaloneCommand(group, .{ .apply_scope_retirement = bytes }));
    try store.applyStandaloneCommand(group, .{ .activate_topology_protocol = try std.json.Stringify.valueAlloc(scratch, topology.Activation{ .version = topology.current_version, .incarnation = @splat('1'), .member_count = 1, .membership_fingerprint = @splat(7) }, .{}) });
    const original = (try AdmissionHarness.apply(&store, scratch, request, 1, false)).outcome.?;
    // There is one nonce namespace per metadata group, not per mutable root.
    var substituted = request;
    substituted.operation.reserve.cascade = true;
    try std.testing.expectError(error.SequenceAdmissionConflict, AdmissionHarness.apply(&store, scratch, substituted, 2, false));
    const meta = try store.systemCatalogMeta(scratch, group);
    const blocked: native.SystemCatalogCommand = .{ .expected_revision = meta.revision, .mutation = .{ .action = .create, .kind = .namespace, .database = "tenant_ops", .name = "new_child" } };
    const blocked_bytes = try std.json.Stringify.valueAlloc(scratch, blocked, .{});
    try std.testing.expectError(error.TableTransitionActive, store.applyStandaloneCommand(group, .{ .apply_system_catalog = blocked_bytes }));
    try std.testing.expectEqual(meta.revision, (try store.systemCatalogMeta(scratch, group)).revision);
    try std.testing.expectError(error.TableTransitionActive, store.applyStandaloneCommand(group, .{ .upsert_table = .{ .table_id = 101, .name = "table:101", .min_ranges = 1, .schema_json = "{\"version\":2}" } }));
    // Disjoint namespaces in another database remain writable.
    const outside: native.SystemCatalogCommand = .{ .expected_revision = meta.revision, .mutation = .{ .action = .create, .kind = .namespace, .name = "outside_scope" } };
    try store.applyStandaloneCommand(group, .{ .apply_system_catalog = try std.json.Stringify.valueAlloc(scratch, outside, .{}) });
    try std.testing.expectError(error.TableTransitionActive, AdmissionHarness.request(&store, scratch, 82));
    var contender = request;
    contender.admission.nonce = @splat(82);
    contender.admission.head = (try store.systemCatalogMeta(scratch, group)).revision;
    contender.operation.reserve.catalog_revision = contender.admission.head;
    const rejected = (try AdmissionHarness.apply(&store, scratch, contender, 3, false)).outcome.?;
    try std.testing.expectEqual(wire.Rejection.TableTransitionActive, rejected.result.rejected);
    _ = try AdmissionHarness.apply(&store, scratch, AdmissionHarness.cancel(original.query.lookup, 83), 4, false);
    // A new attempt based on the old cut is durably rejected. Replaying the
    // accepted original still observes its original acceptance after release.
    var stale = request;
    stale.admission.nonce = @splat(84);
    const changed = (try AdmissionHarness.apply(&store, scratch, stale, 5, false)).outcome.?;
    try std.testing.expectEqual(wire.Rejection.CatalogGenerationChanged, changed.result.rejected);
    try std.testing.expectEqualDeep(original, (try AdmissionHarness.apply(&store, scratch, request, 6, false)).outcome.?);
    // Fresh nonce/cut can reserve after cancellation, but the original
    // rejected nonce must keep its earlier result through the new ownership.
    const fresh = try AdmissionHarness.request(&store, scratch, 85);
    _ = try AdmissionHarness.apply(&store, scratch, fresh, 7, false);
    try std.testing.expectEqualDeep(changed, (try AdmissionHarness.apply(&store, scratch, stale, 8, false)).outcome.?);
}

test "SQL scope retirement admission and outcome writes abort as one native transaction at every fault position" {
    const a = std.testing.allocator;
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(scratch, ".zig-cache/tmp/{s}/admission-atomic", .{tmp.sub_path});
    var store = try native.RaftApplyStore.init(a, .{ .root_dir = path });
    defer store.deinit();
    try seed(&store, scratch);
    const request = try AdmissionHarness.request(&store, scratch, 91);
    var input = try wire.Input.parse(a, group, try request.encodeAlloc(scratch));
    defer input.deinit();
    var prepared = try ledger.prepare(a, group, request.admission.nonce, try std.json.Stringify.valueAlloc(scratch, request.operation.reserve, .{}));
    defer prepared.deinit();
    for (0..6 + prepared.manifest.resources.len + prepared.lookup.reference.tables) |fail_at| {
        var txn = try store.store.beginWriteTxn();
        var open = true;
        defer if (open) txn.abort();
        var fault: Fault = .{ .txn = &txn, .remaining = fail_at };
        if (ledger.stageBegin(&fault, &prepared)) |after| {
            try std.testing.expectError(error.InjectedFailure, wire.stageOutcome(&fault, .{ .query = input.query, .result = .{ .accepted = after } }));
        } else |err| try std.testing.expectEqual(error.InjectedFailure, err);
        txn.abort();
        open = false;
        try absent(&store, prepared.lookup);
        const observed = try std.json.parseFromSliceLeaky(wire.Observation, scratch, try store.scopeRetirementObservationJson(scratch, group, input.query), .{});
        try std.testing.expect(observed.outcome == null and observed.current == null);
    }
    const admitted = (try AdmissionHarness.apply(&store, scratch, request, 1, false)).outcome.?.result.accepted;
    const cancel = AdmissionHarness.cancel(admitted.lookup, 92);
    var cancellation = try wire.Input.parse(a, group, try cancel.encodeAlloc(scratch));
    defer cancellation.deinit();
    // Cancellation releases all active custody and retains both the exact
    // result and its per-job receipt link in one abortable native transaction.
    for (0..6 + prepared.manifest.resources.len + prepared.lookup.reference.tables) |fail_at| {
        var txn = try store.store.beginWriteTxn();
        var open = true;
        defer if (open) txn.abort();
        var fault: Fault = .{ .txn = &txn, .remaining = fail_at };
        if (ledger.stageAdvance(&fault, &prepared, admitted, try admitted.progress.cancelBeforeEffects())) |after| {
            try std.testing.expectError(error.InjectedFailure, wire.stageOutcome(&fault, .{ .query = cancellation.query, .result = .{ .accepted = after } }));
        } else |err| try std.testing.expectEqual(error.InjectedFailure, err);
        txn.abort();
        open = false;
        var read = try store.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqualDeep(admitted, (try ledger.existing(&read, &prepared)).?);
        const observed = try wire.observation(&read, cancellation.query);
        try std.testing.expect(observed.outcome == null);
        try std.testing.expectEqualDeep(admitted, observed.current.?);
        try std.testing.expectError(error.NotFound, wire.terminalQuery(&read, admitted.lookup));
    }
}

test "SQL scope retirement native admission rejects changed physical cuts without relying on logical catalog revision" {
    const a = std.testing.allocator;
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const Change = enum { definition, ranges, incarnation };
    inline for (.{ Change.definition, Change.ranges, Change.incarnation }) |change| {
        const path = try std.fmt.allocPrint(scratch, ".zig-cache/tmp/{s}/physical-cut-{s}", .{ tmp.sub_path, @tagName(change) });
        var store = try native.RaftApplyStore.init(a, .{ .root_dir = path });
        defer store.deinit();
        try seed(&store, scratch);
        var request = try AdmissionHarness.request(&store, scratch, 93);
        switch (change) {
            .definition => try store.applyStandaloneCommand(group, .{ .upsert_table = .{ .table_id = 101, .name = "table:101", .min_ranges = 2 } }),
            .ranges => try store.applyStandaloneCommand(group, .{ .upsert_range = .{ .table_id = 101, .group_id = 301, .range_id = 301, .start_key = "", .end_key = "z" } }),
            .incarnation => request.operation.reserve.metadata_incarnation[0] ^= 1,
        }
        try std.testing.expectEqual(request.operation.reserve.catalog_revision, (try store.systemCatalogMeta(scratch, group)).revision);
        const result = (try AdmissionHarness.apply(&store, scratch, request, 1, false)).outcome.?;
        try std.testing.expectEqual(if (change == .incarnation) wire.Rejection.MetadataIncarnationMismatch else wire.Rejection.CatalogGenerationChanged, result.result.rejected);
        try std.testing.expectEqualStrings("null", try store.scopeRetirementWorkJson(scratch, group, 0));
        try absent(&store, result.query.lookup);
    }
}

fn replaceRow(store: *native.RaftApplyStore, key: []const u8, value: ?[]const u8) !void {
    var txn = try store.store.beginWriteTxn();
    var committed = false;
    defer if (!committed) txn.abort();
    if (value) |bytes| try txn.put(key, bytes) else try txn.delete(key);
    try txn.commit();
    committed = true;
}

fn rejectDamagedCustody(store: *native.RaftApplyStore, a: std.mem.Allocator, checkpoint: []const u8, key: []const u8, value: ?[]const u8) !void {
    const before: ?[]u8 = before: {
        var read = try store.store.beginReadTxn();
        defer read.abort();
        const bytes = read.get(key) catch |err| switch (err) {
            error.NotFound => break :before null,
            else => return err,
        };
        break :before try a.dupe(u8, bytes);
    };
    defer if (before) |bytes| a.free(bytes);
    try replaceRow(store, key, value);
    defer replaceRow(store, key, before) catch @panic("cannot restore damaged test row");
    // Both paths use the same streamed custody rules. Snapshot refusal must
    // occur before encoding/publishing a replacement image; checkpoint refusal
    // must occur before creating a portable native artifact.
    try std.testing.expectError(error.InvalidMetadataSnapshot, store.snapshotBuilder().buildSnapshot(a, group));
    try std.testing.expectError(error.InvalidMetadataHACheckpoint, store.exportHotStandbyCheckpoint(store.io_impl.io(), checkpoint));
}

test "SQL scope retirement recovery rejects every orphan missing owner overlap and fabricated progress through snapshots and native checkpoints" {
    const a = std.testing.allocator;
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(scratch, ".zig-cache/tmp/{s}/scope-checkpoint", .{tmp.sub_path});
    const checkpoint = try std.fmt.allocPrint(scratch, "{s}/damaged.bin", .{path});
    var store = try native.RaftApplyStore.init(a, .{ .root_dir = path });
    defer store.deinit();
    try seed(&store, scratch);
    const request = try AdmissionHarness.request(&store, scratch, 81);
    const accepted = (try AdmissionHarness.apply(&store, scratch, request, 1, false)).outcome.?;
    const head = accepted.result.accepted;
    var prepared = try ledger.prepare(a, group, head.lookup.reference.id, try std.json.Stringify.valueAlloc(scratch, request.operation.reserve, .{}));
    defer prepared.deinit();
    var buffer: [320]u8 = undefined;
    // Each registered child is an exact member, with a root-derived ID and
    // independently bound physical definition/ranges. Both snapshot encodings
    // must refuse missing children, substitutions and orphaned registrations.
    for (0..head.lookup.reference.tables) |ordinal| {
        const child_key = try ledger.childKey(&buffer, head.lookup, @intCast(ordinal));
        try rejectDamagedCustody(&store, a, checkpoint, child_key, null);
        var read = try store.store.beginReadTxn();
        var selected = (try ledger.child(&read, head.lookup, @intCast(ordinal))).?;
        read.abort();
        selected.definition[0] ^= 1;
        const wrong_definition = try ledger.ChildRecord.init(selected);
        try rejectDamagedCustody(&store, a, checkpoint, child_key, wrong_definition.slice());
        selected.definition[0] ^= 1;
        selected.ranges[0] ^= 1;
        const wrong_ranges = try ledger.ChildRecord.init(selected);
        try rejectDamagedCustody(&store, a, checkpoint, child_key, wrong_ranges.slice());
        selected.ranges[0] ^= 1;
        selected.id[0] ^= 1;
        const wrong_id = try ledger.ChildRecord.init(selected);
        try rejectDamagedCustody(&store, a, checkpoint, child_key, wrong_id.slice());
        selected.id[0] ^= 1;
        selected.table += 1000;
        selected.id = try selected.lookup.reference.childId(selected.table);
        const swapped = try ledger.ChildRecord.init(selected);
        try rejectDamagedCustody(&store, a, checkpoint, child_key, swapped.slice());
    }
    // Required forward images, work and immutable admission cannot disappear.
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.key(&buffer, head.lookup, .head), null);
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.key(&buffer, head.lookup, .manifest), null);
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.queueKey(&buffer, group, head.sequence), null);
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.memberKey(&buffer, group, .database, head.lookup.reference.root), null);
    try rejectDamagedCustody(&store, a, checkpoint, try wire.outcomeKey(&buffer, accepted.query), null);
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.counterKey(&buffer, group), null);
    var count: [8]u8 = undefined;
    std.mem.writeInt(u64, &count, head.sequence + 1, .little);
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.counterKey(&buffer, group), &count);
    const lease = try ledger.LeaseRecord.init(.{ .lookup = head.lookup, .metadata_incarnation = head.metadata_incarnation });
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.memberKey(&buffer, group, .table, 999), lease.slice());
    const queue = try ledger.QueueRecord.init(head.lookup);
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.queueKey(&buffer, group, head.sequence + 1), queue.slice());
    var duplicate = accepted;
    duplicate.query.nonce = @splat(84);
    var extra_receipt = try wire.OutcomeRecord.init(duplicate);
    try rejectDamagedCustody(&store, a, checkpoint, try wire.outcomeKey(&buffer, duplicate.query), extra_receipt.slice());
    const extra_link = try wire.QueryRecord.init(duplicate.query);
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.key(&buffer, head.lookup, .terminal_outcome), extra_link.slice());
    var orphan = head.lookup;
    orphan.reference.id = @splat(82);
    {
        var read = try store.store.beginReadTxn();
        var stray = (try ledger.child(&read, head.lookup, 0)).?;
        read.abort();
        stray.lookup = orphan;
        stray.id = try orphan.reference.childId(stray.table);
        const encoded_child = try ledger.ChildRecord.init(stray);
        try rejectDamagedCustody(&store, a, checkpoint, try ledger.childKey(&buffer, orphan, 0), encoded_child.slice());
    }
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.key(&buffer, orphan, .manifest), prepared.encoded);
    try rejectDamagedCustody(&store, a, checkpoint, try std.fmt.bufPrint(&buffer, "\x00\x00__metadata__:system_catalog:{d}:scope-retirement-unknown", .{group}), "unknown");
    var altered = head;
    altered.metadata_incarnation[0] ^= 1;
    var record = try ledger.HeadRecord.init(altered);
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.key(&buffer, head.lookup, .head), record.slice());
    altered = head;
    altered.progress = try head.progress.drained();
    record = try ledger.HeadRecord.init(altered);
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.key(&buffer, head.lookup, .head), record.slice());
    // A durable lifecycle key or a new FK parent lease cannot coexist with
    // reserved table custody, even if an unchecked writer copied one in.
    const lock = lifecycle.value(.index_build, @splat(9));
    try rejectDamagedCustody(&store, a, checkpoint, try lifecycle.key(&buffer, group, 101), &lock);
    const parent: dependencies.Key = .{ .group = group, .parent = 101, .child = 909, .id = @splat(9), .ordinal = 0 };
    try rejectDamagedCustody(&store, a, checkpoint, try parent.encode(&buffer), "unbacked lease");
    // Terminal ownership needs the exact cancellation receipt, not merely a
    // terminal-looking head. No terminal body, member or queue may survive.
    const terminal = (try AdmissionHarness.apply(&store, scratch, AdmissionHarness.cancel(head.lookup, 83), 2, false)).outcome.?;
    try rejectDamagedCustody(&store, a, checkpoint, try wire.outcomeKey(&buffer, terminal.query), null);
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.key(&buffer, head.lookup, .terminal_outcome), null);
    var wrong_query = terminal.query;
    wrong_query.digest[0] ^= 1;
    const wrong_link = try wire.QueryRecord.init(wrong_query);
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.key(&buffer, head.lookup, .terminal_outcome), wrong_link.slice());
    duplicate = terminal;
    duplicate.query.nonce = @splat(84);
    extra_receipt = try wire.OutcomeRecord.init(duplicate);
    try rejectDamagedCustody(&store, a, checkpoint, try wire.outcomeKey(&buffer, duplicate.query), extra_receipt.slice());
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.key(&buffer, head.lookup, .manifest), prepared.encoded);
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.memberKey(&buffer, group, .database, head.lookup.reference.root), lease.slice());
    try rejectDamagedCustody(&store, a, checkpoint, try ledger.queueKey(&buffer, group, head.sequence), queue.slice());
    const snapshot = try store.snapshotBuilder().buildSnapshot(a, group);
    defer a.free(snapshot);
    const artifact = try store.exportHotStandbyCheckpoint(store.io_impl.io(), checkpoint);
    try std.testing.expect(artifact.size_bytes > 0);
}

test "SQL scope retirement admission excludes every lifecycle kind and FK parent leases bidirectionally without blocking disjoint tables" {
    const a = std.testing.allocator;
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(scratch, ".zig-cache/tmp/{s}/scope-exclusion", .{tmp.sub_path});
    var store = try native.RaftApplyStore.init(a, .{ .root_dir = path });
    defer store.deinit();
    try seed(&store, scratch);
    const request = try AdmissionHarness.request(&store, scratch, 91);
    var prepared = try ledger.prepare(a, group, @splat(91), try std.json.Stringify.valueAlloc(scratch, request.operation.reserve, .{}));
    defer prepared.deinit();
    inline for (comptime std.meta.tags(lifecycle.Kind).*) |kind| {
        var txn = try store.store.beginWriteTxn();
        defer txn.abort();
        try lifecycle.acquire(&txn, group, 101, .{ .kind = kind, .id = @splat(9) });
        try std.testing.expectError(error.TableTransitionActive, ledger.stageBegin(&txn, &prepared));
    }
    const source: local.storage_db_relational_schema_owner_contract.ForeignKeySource = .{ .table_name = "borrower", .parents = &.{.{ .constraint_name = "fk", .declared_parent = "table:101", .parent_table = "table:101", .parent_table_id = 101, .parent_schema_version = 1, .parent_schema = @splat(1), .parent_public = @splat(2), .parent_catalog = @splat(3), .parent_unique = "pk", .parent_generation = @splat(4), .parent_definition = @splat(5) }} };
    const child = .{ .metadata_group_id = group, .metadata_incarnation = @as([32]u8, @splat('1')), .table_id = @as(u64, 909), .id = @as([16]u8, @splat(9)), .plan_digest = @as([32]u8, @splat(10)) };
    {
        var txn = try store.store.beginWriteTxn();
        defer txn.abort();
        try dependencies.acquire(scratch, &txn, child, source);
        try std.testing.expectError(error.TableTransitionActive, ledger.stageBegin(&txn, &prepared));
    }
    const admitted = (try AdmissionHarness.apply(&store, scratch, request, 1, false)).outcome.?;
    inline for (comptime std.meta.tags(lifecycle.Kind).*) |kind| {
        var txn = try store.store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.GenerationPublicationChanged, lifecycle.acquire(&txn, group, 101, .{ .kind = kind, .id = @splat(9) }));
        try std.testing.expect(try lifecycle.read(&txn, group, 101) == null);
        try lifecycle.acquire(&txn, group, 999, .{ .kind = kind, .id = @splat(9) });
        try lifecycle.release(&txn, group, 999, .{ .kind = kind, .id = @splat(9) });
    }
    {
        var txn = try store.store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.GenerationPublicationChanged, dependencies.acquire(scratch, &txn, child, source));
        try dependencies.requireUnleased(&txn, group, 101);
        var parents = [_]local.storage_db_relational_schema_owner_contract.ForeignKeyParent{ source.parents[0], source.parents[0] };
        parents[0].constraint_name = "external_first";
        parents[0].parent_table_id = 999;
        parents[0].declared_parent = "table:999";
        parents[0].parent_table = "table:999";
        const combined: local.storage_db_relational_schema_owner_contract.ForeignKeySource = .{ .table_name = source.table_name, .parents = &parents };
        try std.testing.expectError(error.GenerationPublicationChanged, dependencies.acquire(scratch, &txn, child, combined));
        try dependencies.requireUnleased(&txn, group, 999);
        try dependencies.requireUnleased(&txn, group, 101);
        const uses = @import("../system_catalog/sequence_default_dependencies.zig");
        var buffer: [320]u8 = undefined;
        const lease = try ledger.LeaseRecord.init(.{ .lookup = admitted.query.lookup, .metadata_incarnation = admitted.result.accepted.metadata_incarnation });
        try txn.put(try ledger.memberKey(&buffer, group, .sequence, 909), lease.slice());
        var references = [_]uses.Reference{.{ .column_id = 1, .sequence_id = 909, .namespace_id = 2, .incarnation = @splat(1), .generation = .serial }};
        try std.testing.expectError(error.CatalogPublicationProofPending, uses.prepare(a, &txn, group, 999, &references));
        references[0].sequence_id = 998;
        var allowed = (try uses.prepare(a, &txn, group, 999, &references)).?;
        defer allowed.deinit();
    }
    _ = try AdmissionHarness.apply(&store, scratch, AdmissionHarness.cancel(admitted.query.lookup, 92), 2, false);
    {
        var txn = try store.store.beginWriteTxn();
        defer txn.abort();
        try lifecycle.acquire(&txn, group, 101, .{ .kind = .index_build, .id = @splat(9) });
        try lifecycle.release(&txn, group, 101, .{ .kind = .index_build, .id = @splat(9) });
        try dependencies.acquire(scratch, &txn, child, source);
    }
}

test "SQL scope retirement dependency intents abort every journal write and survive cold custody until atomic cancellation" {
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(scratch, ".zig-cache/tmp/{s}/scope-dependency-intents", .{tmp.sub_path});
    var encoded: []const u8 = undefined;
    var admitted: ledger.Head = undefined;
    for (0..2) |pass| {
        var store = try native.RaftApplyStore.init(a, .{ .root_dir = path });
        defer store.deinit();
        if (pass == 0) {
            try seed(&store, scratch);
            try store.applyStandaloneCommand(group, .{ .apply_system_catalog = try std.json.Stringify.valueAlloc(scratch, native.SystemCatalogCommand{ .expected_revision = 3, .mutation = .{ .action = .create, .kind = .database, .name = "outside" } }, .{}) });
            for (0..2) |i| {
                const id: u64 = 303 + i;
                const name = try std.fmt.allocPrint(scratch, "table:{d}", .{id});
                const table: @import("domain.zig").TableRecord = .{ .table_id = id, .name = name, .min_ranges = 1, .schema_json = "{\"version\":1}" };
                const ranges = [_]@import("domain.zig").RangeRecord{.{ .table_id = id, .group_id = 503 + i, .range_id = 503 + i, .start_key = "" }};
                try store.applyStandaloneCommand(group, .{ .apply_system_catalog = try std.json.Stringify.valueAlloc(scratch, native.SystemCatalogCommand{ .expected_revision = 4 + i, .mutation = .{ .action = .create, .kind = .table, .database = "outside", .name = if (i == 0) "survivor" else "held_parent", .table_id = id, .storage_name = name }, .topology = .{ .create = .{ .expected_transition_generation = 0, .table = table, .ranges = &ranges } } }, .{}) });
            }
            var selected = try std.json.parseFromSliceLeaky(contract.Manifest, scratch, try store.prepareScopeRetirementManifestJsonWithContext(scratch, group, .{ .kind = .database, .name = "tenant_ops", .creator = .bootstrap }, .{}), .{});
            const outside = try std.json.parseFromSliceLeaky(contract.Manifest, scratch, try store.prepareScopeRetirementManifestJsonWithContext(scratch, group, .{ .kind = .database, .name = "outside", .creator = .bootstrap }, .{}), .{});
            var footprint_dependencies: [2]contract.Dependency = undefined;
            var found: usize = 0;
            for (outside.resources) |resource| if (resource.kind == .table) {
                footprint_dependencies[found] = .{ .kind = .table, .id = resource.id, .resource = resource, .table = outside.tableById(resource.id).?, .action = if (found == 0) .rewrite else .hold, .schema_after = if (found == 0) "{\"version\":2}" else null };
                found += 1;
            };
            try std.testing.expectEqual(@as(usize, 2), found);
            selected.dependencies = &footprint_dependencies;
            // Exercise journal custody independently of dependency discovery.
            // Both immutable preimages are captured from actual native owners;
            // this test does not manufacture physical publication authority.
            try selected.validate(scratch);
            encoded = try std.json.Stringify.valueAlloc(scratch, selected, .{});
        }
        var prepared = try ledger.prepare(a, group, @splat(93), encoded);
        defer prepared.deinit();
        if (pass == 0) {
            const writes = 5 + prepared.manifest.resources.len + prepared.manifest.dependencies.len + prepared.lookup.reference.tables;
            for (0..writes) |fail_at| {
                var txn = try store.store.beginWriteTxn();
                var fault: Fault = .{ .txn = &txn, .remaining = fail_at };
                const result = ledger.stageBegin(&fault, &prepared);
                txn.abort();
                try std.testing.expectError(error.InjectedFailure, result);
                try absent(&store, prepared.lookup);
                var read = try store.store.beginReadTxn();
                defer read.abort();
                for (prepared.manifest.dependencies) |dependency| try std.testing.expect(try ledger.member(&read, group, dependency.kind, dependency.id) == null);
            }
            var txn = try store.store.beginWriteTxn();
            errdefer txn.abort();
            admitted = try ledger.stageBegin(&txn, &prepared);
            try txn.commit();
        } else {
            {
                var txn = try store.store.beginWriteTxn();
                defer txn.abort();
                try ledger.requireMembers(&txn, &prepared, admitted);
                for (prepared.manifest.dependencies) |dependency| {
                    var lease = (try ledger.member(&txn, group, dependency.kind, dependency.id)).?;
                    try std.testing.expectEqual(dependency.action, lease.intent);
                    try std.testing.expectError(error.GenerationPublicationChanged, lifecycle.acquire(&txn, group, dependency.id, .{ .kind = .index_build, .id = @splat(8) }));
                    lease.intent = .retire;
                    const altered = try ledger.LeaseRecord.init(lease);
                    var buffer: [256]u8 = undefined;
                    try txn.put(try ledger.memberKey(&buffer, group, dependency.kind, dependency.id), altered.slice());
                    try std.testing.expectError(error.InvalidScopeRetirement, ledger.requireMembers(&txn, &prepared, admitted));
                    lease.intent = dependency.action;
                    const restored = try ledger.LeaseRecord.init(lease);
                    try txn.put(try ledger.memberKey(&buffer, group, dependency.kind, dependency.id), restored.slice());
                }
            }
            const canceled = try admitted.progress.cancelBeforeEffects();
            const writes = 4 + prepared.manifest.resources.len + prepared.manifest.dependencies.len + prepared.lookup.reference.tables;
            for (0..writes) |fail_at| {
                var txn = try store.store.beginWriteTxn();
                var fault: Fault = .{ .txn = &txn, .remaining = fail_at };
                const result = ledger.stageAdvance(&fault, &prepared, admitted, canceled);
                txn.abort();
                try std.testing.expectError(error.InjectedFailure, result);
                var read = try store.store.beginReadTxn();
                defer read.abort();
                try ledger.requireMembers(&read, &prepared, admitted);
            }
            var txn = try store.store.beginWriteTxn();
            var committed = false;
            defer if (!committed) txn.abort();
            const terminal = try ledger.stageAdvance(&txn, &prepared, admitted, canceled);
            try txn.commit();
            committed = true;
            var read = try store.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqualDeep(terminal, (try ledger.existing(&read, &prepared)).?);
            for (prepared.manifest.dependencies) |dependency| try std.testing.expect(try ledger.member(&read, group, dependency.kind, dependency.id) == null);
        }
    }
}

test "SQL scope retirement raced committed definition and topology commands preserve custody without blocking later entries" {
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(scratch, ".zig-cache/tmp/{s}/scope-committed-noop", .{tmp.sub_path});
    var store = try native.RaftApplyStore.init(a, .{ .root_dir = path });
    defer store.deinit();
    try seed(&store, scratch);
    const before = (try store.getTable(scratch, group, 101)).?;
    const range_before = (try store.getRange(scratch, group, 301)).?;
    const admitted = (try AdmissionHarness.apply(&store, scratch, try AdmissionHarness.request(&store, scratch, 94), 1, true)).outcome.?;
    var changed = before;
    changed.schema_json = "{\"version\":2}";
    var range_changed = range_before;
    range_changed.start_key = "late-stale-boundary";
    // Admission remains a precise refusal. A proposal that raced admission
    // is already in the log and must instead be an effect-free applied entry.
    try std.testing.expectError(error.TableTransitionActive, store.applyStandaloneCommand(group, .{ .upsert_table = changed }));
    try std.testing.expectError(error.TableTransitionActive, store.applyStandaloneCommand(group, .{ .compare_and_replace_table = .{ .expected = before, .replacement = changed } }));
    const commands = [_]native.TransitionCommand{
        .{ .upsert_table = changed },
        .{ .compare_and_replace_table = .{ .expected = before, .replacement = changed } },
        .{ .upsert_range = range_changed },
        .{ .remove_range = .{ .group_id = 301 } },
        .{ .upsert_table = .{ .table_id = 999, .name = "unscoped", .min_ranges = 1 } },
    };
    const raft = @import("../raft/state_machine/mod.zig");
    var entries: [commands.len]@import("raft_engine").core.Entry = undefined;
    for (commands, &entries, 0..) |command, *entry, i| entry.* = .{ .term = 1, .index = 2 + i, .entry_type = .normal, .data = try native.encodeTransitionCommand(scratch, command) };
    try store.snapshotBuilder().applyBatch(.{ .group_id = group, .commit_index = 1 + commands.len, .entries_bytes = try raft.encodeCommittedEntries(scratch, &entries) });
    try std.testing.expectEqualDeep(before, (try store.getTable(scratch, group, 101)).?);
    try std.testing.expectEqualDeep(range_before, (try store.getRange(scratch, group, 301)).?);
    try std.testing.expectEqualStrings("unscoped", (try store.getTable(scratch, group, 999)).?.name);
    const custody = try std.json.parseFromSliceLeaky(wire.Observation, scratch, try store.scopeRetirementObservationJson(scratch, group, admitted.query), .{});
    try std.testing.expectEqualDeep(admitted.result.accepted, custody.current.?);
    const snapshot = try store.snapshotBuilder().buildSnapshot(scratch, group);
    try std.testing.expect(snapshot.len != 0);
    _ = try AdmissionHarness.apply(&store, scratch, AdmissionHarness.cancel(admitted.query.lookup, 95), 2 + commands.len, true);
    const encoded = try native.encodeTransitionCommand(scratch, .{ .upsert_table = changed });
    try store.snapshotBuilder().applyBatch(.{ .group_id = group, .commit_index = 3 + commands.len, .entries_bytes = try raft.encodeCommittedEntries(scratch, &.{.{ .term = 1, .index = 3 + commands.len, .entry_type = .normal, .data = encoded }}) });
    try std.testing.expectEqualStrings(changed.schema_json, (try store.getTable(scratch, group, 101)).?.schema_json);
}

test "SQL scope retirement callable closure fences retired sources preserves held users and rejects escaped edges across cold owners" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const routines = @import("../system_catalog/routines.zig");
    const casts = @import("../system_catalog/casts.zig");
    const storage = @import("../system_catalog/storage.zig");
    const H = struct {
        fn request(store: *native.RaftApplyStore, alloc: std.mem.Allocator, target: domain.Target, kind: routines.records.Kind, definition: routines.records.Definition, nonce: u8) !routines.Mutation {
            const cut = try std.json.parseFromSliceLeaky(routines.Snapshot, alloc, try store.sqlRoutineSnapshot(alloc, group, .{ .targets = &.{target}, .kind = kind, .signature = try definition.signatureAlloc(alloc) }, .{}), .{});
            return .{ .admission = .{ .nonce = @splat(nonce), .head = cut.head }, .target = target, .namespace_id = cut.scopes[0].namespace_id.?, .incarnation = cut.incarnation, .operation = .{ .publish = .{ .kind = kind, .definition = definition } } };
        }
        fn apply(store: *native.RaftApplyStore, alloc: std.mem.Allocator, request_value: routines.Mutation) !routines.Outcome {
            try store.applyStandaloneCommand(group, .{ .apply_system_catalog = try std.json.Stringify.valueAlloc(alloc, routines.command(request_value), .{}) });
            return (try std.json.parseFromSliceLeaky(?routines.Outcome, alloc, try store.sqlRoutineOutcome(alloc, group, try routines.observation(alloc, request_value)), .{})).?;
        }
        fn publish(store: *native.RaftApplyStore, alloc: std.mem.Allocator, request_value: routines.Mutation) !routines.records.FunctionReference {
            const identity = (try apply(store, alloc, request_value)).success.identity.?;
            return .{ .database = request_value.target.database, .namespace = request_value.target.namespace, .name = request_value.target.table, .namespace_id = request_value.namespace_id, .id = identity.id };
        }
        fn captureProbe(alloc: std.mem.Allocator, store: *native.RaftApplyStore) !void {
            const encoded = try store.prepareScopeRetirementManifestJsonWithContext(alloc, group, .{ .kind = .namespace, .name = "retiring", .cascade = true, .creator = .bootstrap }, .{});
            defer alloc.free(encoded);
        }
        fn replaceRaw(store: *native.RaftApplyStore, alloc: std.mem.Allocator, resource: domain.Resource) !void {
            var txn = try store.store.beginWriteTxn();
            errdefer txn.abort();
            try txn.put(try storage.recordKeyAlloc(alloc, group, resource.kind, resource.id), try std.json.Stringify.valueAlloc(alloc, resource, .{}));
            // The escaped reference is otherwise a valid catalog. Rebuild the
            // derived directory so its corruption cannot mask the scope check.
            try storage.rebuildNameIndex(alloc, &txn, group);
            try storage.validateNameIndex(alloc, &txn, group);
            try txn.commit();
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(scratch, ".zig-cache/tmp/{s}/callable-scope", .{tmp.sub_path});
    var admitted: wire.Outcome = undefined;
    var blocked: routines.Mutation = undefined;
    var blocked_outcome: routines.Outcome = undefined;
    for (0..2) |pass| {
        var store = try native.RaftApplyStore.init(a, .{ .root_dir = path });
        defer store.deinit();
        if (pass == 0) {
            try store.applyStandaloneCommand(group, .{ .initialize_metadata_incarnation = @splat('1') });
            try store.applyStandaloneCommand(group, .{ .activate_topology_protocol = try std.json.Stringify.valueAlloc(scratch, topology.Activation{ .version = topology.current_version, .incarnation = @splat('1'), .member_count = 1, .membership_fingerprint = @splat(7) }, .{}) });
            try store.applyStandaloneCommand(group, .{ .apply_system_catalog = try std.json.Stringify.valueAlloc(scratch, native.SystemCatalogCommand{ .expected_revision = 0, .mutation = .{ .action = .create, .kind = .namespace, .name = "retiring" } }, .{}) });
            const step_definition: routines.records.Definition = .{ .arguments = &.{ .{ .type = .{ .scalar = .text } }, .{ .type = .{ .scalar = .text } } }, .result = .{ .scalar = .text }, .body = "SELECT coalesce($1,$2)", .owner = .auth_disabled };
            const step = try H.publish(&store, scratch, try H.request(&store, scratch, .{ .namespace = "retiring", .table = "step" }, .function, step_definition, 1));
            const outside = try H.publish(&store, scratch, try H.request(&store, scratch, .{ .table = "outside_step" }, .function, step_definition, 2));
            const finish_request = try H.request(&store, scratch, .{ .table = "finish" }, .function, .{ .arguments = &.{.{ .type = .{ .scalar = .text } }}, .result = .{ .scalar = .text }, .body = "SELECT upper($1)", .owner = .auth_disabled }, 3);
            const finish = try H.publish(&store, scratch, finish_request);
            const equal_function = try H.publish(&store, scratch, try H.request(&store, scratch, .{ .namespace = "retiring", .table = "same" }, .function, .{ .arguments = step_definition.arguments, .result = .{ .scalar = .boolean }, .body = "SELECT $1=$2", .owner = .auth_disabled }, 4));
            const convert = try H.publish(&store, scratch, try H.request(&store, scratch, .{ .namespace = "retiring", .table = "convert" }, .function, .{ .arguments = &.{.{ .type = .{ .scalar = .jsonb } }}, .result = .{ .scalar = .text }, .body = "SELECT 'native'", .owner = .auth_disabled }, 5));
            const aggregate_definition: routines.records.Definition = .{ .arguments = finish_request.operation.publish.definition.arguments, .result = .{ .scalar = .text }, .body = "", .function_reference = step, .aggregate = .{ .state = .{ .scalar = .text }, .final = finish }, .owner = .auth_disabled };
            _ = try H.publish(&store, scratch, try H.request(&store, scratch, .{ .table = "collect" }, .aggregate, aggregate_definition, 6));
            _ = try H.publish(&store, scratch, try H.request(&store, scratch, .{ .table = "===" }, .operator, .{ .arguments = step_definition.arguments, .result = .{ .scalar = .boolean }, .body = "", .function_reference = equal_function, .owner = .auth_disabled }, 7));
            const pair: casts.records.Pair = .{ .source = .{ .scalar = .jsonb }, .target = .{ .scalar = .text } };
            const cast_cut = try std.json.parseFromSliceLeaky(casts.Snapshot, scratch, try store.sqlCastSnapshot(scratch, group, .{ .database = "default", .pair = pair }, .{}), .{});
            const cast_request: casts.Mutation = .{ .admission = .{ .nonce = @splat(8), .head = cast_cut.head }, .database = "default", .database_id = cast_cut.database_id.?, .incarnation = cast_cut.incarnation, .operation = .{ .create = .{ .pair = pair, .function = convert, .context = .assignment, .owner = .auth_disabled } } };
            try store.applyStandaloneCommand(group, .{ .apply_system_catalog = try std.json.Stringify.valueAlloc(scratch, try casts.command(scratch, cast_request), .{}) });
            const cast_outcome = (try std.json.parseFromSliceLeaky(?casts.Outcome, scratch, try store.sqlCastOutcome(scratch, group, try casts.observation(scratch, cast_request)), .{})).?;
            try std.testing.expect(cast_outcome == .success);
            const manifest = try std.json.parseFromSliceLeaky(contract.Manifest, scratch, try store.prepareScopeRetirementManifestJsonWithContext(scratch, group, .{ .kind = .namespace, .name = "retiring", .cascade = true, .creator = .bootstrap }, .{}), .{});
            try std.testing.expectEqual(@as(usize, 3), manifest.resources.len);
            try std.testing.expectEqual(@as(usize, 4), manifest.dependencies.len);
            for (manifest.dependencies) |dependency| {
                try std.testing.expectEqual(if (dependency.id == finish.id) @as(@TypeOf(dependency.action), .hold) else .retire, dependency.action);
            }
            try @import("antfly_platform").allocator.checkAllAllocationFailures(a, H.captureProbe, .{&store});
            admitted = (try AdmissionHarness.apply(&store, scratch, .{ .admission = .{ .nonce = @splat(91), .head = manifest.catalog_revision }, .operation = .{ .reserve = manifest } }, 1, false)).outcome.?;
            blocked = try H.request(&store, scratch, .{ .table = "blocked" }, .aggregate, aggregate_definition, 9);
            blocked_outcome = try H.apply(&store, scratch, blocked);
            try std.testing.expectEqual(.TableTransitionActive, blocked_outcome.rejected);
            try std.testing.expectEqualDeep(blocked_outcome, try H.apply(&store, scratch, blocked));
            var changed = try H.request(&store, scratch, .{ .table = "finish" }, .function, finish_request.operation.publish.definition, 10);
            changed.operation.publish.replace = true;
            changed.operation.publish.expected = .{ .id = finish.id, .generation = 1 };
            changed.operation.publish.definition.body = "SELECT lower($1)";
            try std.testing.expectEqual(.TableTransitionActive, (try H.apply(&store, scratch, changed)).rejected);
            var independent = aggregate_definition;
            independent.function_reference = outside;
            const unrelated = try H.publish(&store, scratch, try H.request(&store, scratch, .{ .table = "unrelated" }, .aggregate, independent, 11));
            var read = try store.store.beginReadTxn();
            const original = (try storage.getById(scratch, &read, group, .routine, unrelated.id)).?.value;
            read.abort();
            var escaped = original;
            var record = (try routines.records.Record.decode(scratch, escaped.source_json)).value;
            record.definition.function_reference = step;
            escaped.source_json = try record.encode(scratch);
            try H.replaceRaw(&store, scratch, escaped);
            try std.testing.expectError(error.InvalidMetadataSnapshot, store.snapshotBuilder().buildSnapshot(scratch, group));
            const checkpoint = try std.fmt.allocPrint(scratch, "{s}/escaped.bin", .{path});
            try std.testing.expectError(error.InvalidMetadataHACheckpoint, store.exportHotStandbyCheckpoint(store.io_impl.io(), checkpoint));
            try H.replaceRaw(&store, scratch, original);
            const snapshot = try store.snapshotBuilder().buildSnapshot(scratch, group);
            var copy = try native.RaftApplyStore.init(a, .{ .root_dir = try std.fmt.allocPrint(scratch, "{s}-copy", .{path}) });
            defer copy.deinit();
            try std.testing.expect(try copy.snapshotBuilder().installSnapshot(scratch, group, 1, snapshot));
            try std.testing.expectEqualDeep(admitted, (try std.json.parseFromSliceLeaky(wire.Observation, scratch, try copy.scopeRetirementObservationJson(scratch, group, admitted.query), .{})).outcome.?);
        } else {
            try std.testing.expectEqualDeep(blocked_outcome, try H.apply(&store, scratch, blocked));
            const canceled = try AdmissionHarness.apply(&store, scratch, AdmissionHarness.cancel(admitted.query.lookup, 92), 2, false);
            try std.testing.expectEqual(contract.Phase.canceled, canceled.current.?.progress.phase);
            try std.testing.expectEqualDeep(blocked_outcome, try H.apply(&store, scratch, blocked));
            const fresh = try H.request(&store, scratch, blocked.target, .aggregate, blocked.operation.publish.definition, 12);
            try std.testing.expect((try H.apply(&store, scratch, fresh)) == .success);
            const snapshot = try store.snapshotBuilder().buildSnapshot(scratch, group);
            try std.testing.expect(snapshot.len != 0);
        }
    }
}
