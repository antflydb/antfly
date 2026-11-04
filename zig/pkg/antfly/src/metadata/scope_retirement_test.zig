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
            const puts = 5 + prepared.manifest.resources.len;
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
    const writes = 4 + prepared.manifest.resources.len;
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
                const work = (try std.json.parseFromSliceLeaky(?ledger.Head, scratch, try store.scopeRetirementWorkJson(scratch, group, std.math.maxInt(u64)), .{})).?;
                try std.testing.expectEqualDeep(original.result.accepted, work);
                const retained = try std.json.parseFromSliceLeaky(contract.Manifest, scratch, try store.scopeRetirementManifestJson(scratch, group, original.query.lookup), .{});
                try std.testing.expectEqualDeep(original.query.lookup.reference, try retained.reference(scratch, original.query.lookup.reference.id));
                try std.testing.expectError(error.CatalogPublicationProofPending, store.exportSystemCatalog(scratch, group));
                const active_snapshot = try store.snapshotBuilder().buildSnapshot(scratch, group);
                const active_copy_path = try std.fmt.allocPrint(scratch, "{s}-active-copy", .{path});
                var active_copy = try native.RaftApplyStore.init(a, .{ .root_dir = active_copy_path });
                defer active_copy.deinit();
                try std.testing.expect(try active_copy.snapshotBuilder().installSnapshot(scratch, group, 101, active_snapshot));
                const active_work = try std.json.parseFromSliceLeaky(?ledger.Head, scratch, try active_copy.scopeRetirementWorkJson(scratch, group, 0), .{});
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
    const rejected = (try AdmissionHarness.apply(&store, scratch, try AdmissionHarness.request(&store, scratch, 82), 3, false)).outcome.?;
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
    for (0..6 + prepared.manifest.resources.len) |fail_at| {
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
