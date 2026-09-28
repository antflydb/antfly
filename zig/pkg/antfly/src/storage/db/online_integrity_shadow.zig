// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Receiver-local serving ownership while its disjoint donor interval is a
//! shadow. The canonical physical claim keys do not change; normal readers and
//! participants may only observe the live base interval until atomic publish.
const std = @import("std");
const ranges = @import("range_state.zig");
const ByteRange = @import("types.zig").ByteRange;
const types = @import("types.zig");
const pages = @import("merge_page_contract.zig");
const integrity = @import("relational_integrity_contract.zig");
const catalog = @import("relational_integrity_catalog.zig");
pub const range_key = "\x00\x00__metadata__:online_integrity_shadow_range";
pub const key = "\x00\x00__metadata__:online_integrity_shadow";
pub const State = struct {
    source: pages.Source,
    context: types.MergeReplicationContext,
    pub fn jsonStringify(self: @This(), stream: anytype) !void {
        try @import("relational_integrity_json.zig").write(self, stream);
    }
};

pub fn load(alloc: std.mem.Allocator, txn: anytype) !?std.json.Parsed(State) {
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len > 16384) return error.InvalidMergeState;
    return try std.json.parseFromSlice(State, alloc, raw, .{ .allocate = .alloc_always });
}

pub fn requireBinding(alloc: std.mem.Allocator, txn: anytype, source: pages.Source) !void {
    try source.validate();
    const binding = source.integrity orelse return error.InvalidMergePage;
    const raw = txn.get(catalog.key) catch |err| switch (err) {
        error.NotFound => return error.IntegrityCatalogChanged,
        else => return err,
    };
    if (!std.mem.eql(u8, &integrity.hash(raw), &binding.catalog_digest)) return error.IntegrityCatalogChanged;
    var compiled = try catalog.decode(alloc, raw);
    defer compiled.deinit();
    if (!std.mem.eql(u8, &@import("relational_integrity_activation.zig").generationSet(compiled), &binding.generation_set)) return error.IntegrityCatalogChanged;
    try @import("relational_integrity_activation.zig").requireReady(txn, compiled);
}

/// Admission is local proof, not publication: all serving operations remain
/// inside the base interval. Only the exact copy may mutate its shadow.
pub fn admit(alloc: std.mem.Allocator, txn: anytype, req: types.BatchRequest, namespace: @import("doc_identity.zig").Namespace) !bool {
    if (req.merge_checkpoint) |checkpoint| if (checkpoint.kind == .accept and checkpoint.page_source != null and checkpoint.page_source.?.integrity != null) {
        if (checkpoint.page_receiver_namespace == null or !checkpoint.page_receiver_namespace.?.eql(namespace) or checkpoint.allow_doc_identity_reassignment) return error.MergeCopyFenced;
        try requireBinding(alloc, txn, checkpoint.page_source.?);
        if (try @import("relational_integrity_topology.zig").current(txn) != null or try @import("relational_integrity_retirement.zig").active(txn)) return error.IntegrityTopologyBusy;
        if (try load(alloc, txn)) |value| {
            var current = value;
            defer current.deinit();
            if (try rawRange(txn) != null and (!current.value.source.eql(checkpoint.page_source.?) or current.value.context.transition_id != checkpoint.transition_id or current.value.context.donor_group_id != checkpoint.donor_group_id or current.value.context.receiver_group_id != checkpoint.receiver_group_id)) return error.MergeCopyFenced;
        }
        return true;
    };
    var current = (try load(alloc, txn)) orelse return false;
    defer current.deinit();
    if (try rawRange(txn) == null) {
        if (req.merge_checkpoint) |checkpoint| if (checkpoint.transition_id != current.value.context.transition_id) return false;
        if (req.merge_replication) |replication| if (replication.transition_id != current.value.context.transition_id) return false;
    }
    if (!current.value.context.identity_namespace.eql(namespace)) return error.MergeCopyFenced;
    try requireBinding(alloc, txn, current.value.source);
    if (req.merge_checkpoint) |checkpoint| {
        const context = current.value.context;
        const first_begin = checkpoint.kind == .begin_copy and context.copy_attempt.sequence == 0 and checkpoint.copy_attempt.sequence != 0 and checkpoint.copy_attempt.donor_term != 0;
        if (checkpoint.transition_id != context.transition_id or checkpoint.donor_group_id != context.donor_group_id or checkpoint.receiver_group_id != context.receiver_group_id or (!first_begin and !std.meta.eql(checkpoint.copy_attempt, context.copy_attempt))) return error.MergeCopyFenced;
        if (checkpoint.page_source) |source| if (!source.eql(current.value.source)) return error.MergeCopyFenced;
        return true;
    }
    if (req.merge_replication) |replication| {
        if (!std.meta.eql(replication, current.value.context) or req.merge_page == null or !req.merge_page.?.source.eql(current.value.source)) return error.MergeCopyFenced;
        return true;
    }
    return false;
}

/// One prefix seek per integrity kind, bounded by the page budget. Physical
/// order is the durable cursor; no scan walks unrelated receiver-base claims.
pub fn cleanup(alloc: std.mem.Allocator, txn: anytype, donor: ByteRange, after: []const u8) !struct { effects: []const pages.IntegrityEffect, exhausted: bool } {
    var result: std.ArrayList(pages.IntegrityEffect) = .empty;
    var cursor = try txn.openCursor();
    defer cursor.close();
    const initial = if (after.len != 0) (try integrity.parseKey(after)).kind else integrity.Kind.claim;
    for ([_]integrity.Kind{ .claim, .reference, .job }) |kind| {
        if (@intFromEnum(kind) < @intFromEnum(initial)) continue;
        var prefix: [integrity.namespace.len + 1]u8 = undefined;
        @memcpy(prefix[0..integrity.namespace.len], integrity.namespace);
        prefix[integrity.namespace.len] = @intFromEnum(kind);
        const lower = try std.mem.concat(alloc, u8, &.{ &prefix, donor.start });
        defer alloc.free(lower);
        const resuming = kind == initial and after.len != 0;
        var entry = try cursor.seekAtOrAfter(if (resuming) after else lower);
        if (entry) |value| if (resuming and std.mem.eql(u8, value.key, after)) {
            entry = try cursor.next();
        };
        while (entry) |value| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, value.key, &prefix)) break;
            const address = (try integrity.parseKey(value.key)).address;
            if (donor.end.len != 0 and std.mem.order(u8, &address.routing, donor.end) != .lt) break;
            if (!donor.contains(&address.routing)) return error.KeyOutOfRange;
            if (result.items.len == pages.max_rows) return .{ .effects = result.items, .exhausted = false };
            try result.append(alloc, .{ .key = try alloc.dupe(u8, value.key), .value = null });
        }
    }
    return .{ .effects = result.items, .exhausted = true };
}

pub fn rawRange(txn: anytype) !?[]const u8 {
    return txn.get(range_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

/// Borrow only while the read/apply transaction is alive. This path is shared
/// by point lookups, bounded range scans and transaction operation admission.
pub fn servingRange(txn: anytype, fallback: ByteRange) !ByteRange {
    const raw = (try rawRange(txn)) orelse return fallback;
    if (raw.len < 8) return error.InvalidRangeState;
    const start_len: usize = std.mem.readInt(u32, raw[0..4], .little);
    if (start_len > raw.len - 8) return error.InvalidRangeState;
    const end_len: usize = std.mem.readInt(u32, raw[4 + start_len ..][0..4], .little);
    if (end_len != raw.len - 8 - start_len) return error.InvalidRangeState;
    const result: ByteRange = .{ .start = raw[4..][0..start_len], .end = raw[8 + start_len ..] };
    if (result.end.len != 0 and std.mem.order(u8, result.start, result.end) != .lt) return error.InvalidRangeState;
    return result;
}

pub fn requireServing(txn: anytype, fallback: ByteRange, routing_key: []const u8) !void {
    if (!(try servingRange(txn, fallback)).contains(routing_key)) return error.KeyOutOfRange;
}

pub fn requireCatalogMutable(txn: anytype) !void {
    if (try rawRange(txn) != null) return error.IntegrityTopologyBusy;
}

test "online integrity shadow confines borrowed serving range without changing full physical range" {
    const alloc = std.testing.allocator;
    const raw = try ranges.encodeRangeAlloc(alloc, .{ .start = "m", .end = "z" });
    defer alloc.free(raw);
    const Txn = struct {
        value: ?[]const u8,
        fn get(self: *@This(), physical_key: []const u8) ![]const u8 {
            if (!std.mem.eql(u8, physical_key, range_key)) return error.UnexpectedKey;
            return self.value orelse error.NotFound;
        }
    };
    var txn: Txn = .{ .value = raw };
    const merged: ByteRange = .{ .start = "a", .end = "z" };
    try requireServing(&txn, merged, "n");
    try std.testing.expectError(error.KeyOutOfRange, requireServing(&txn, merged, "b"));
    try std.testing.expectError(error.IntegrityTopologyBusy, requireCatalogMutable(&txn));
    txn.value = null;
    try requireServing(&txn, merged, "b");
    try requireCatalogMutable(&txn);
    txn.value = "corrupt";
    try std.testing.expectError(error.InvalidRangeState, servingRange(&txn, merged));
}

test "relational index system online integrity shadow cleanup publication and reopen preserve serving ownership" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/shadow", .{tmp.sub_path});
    defer alloc.free(path);
    const options: @import("antfly_source_root").antfly_sources.physical_db.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var db = try @import("antfly_source_root").antfly_sources.physical_db.DB.open(alloc, path, options);
    defer db.close();
    try db.updateRange(.{ .start = "\xff", .end = "" });
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    try db.setSchemaJson(alloc, schema);
    const raw_catalog = try db.core.store.get(alloc, catalog.key);
    defer alloc.free(raw_catalog);
    var compiled = try catalog.decode(alloc, raw_catalog);
    defer compiled.deinit();
    const address = try integrity.Address.init(compiled.find(.unique, "pk").?.generation, "parent");
    try std.testing.expect(std.mem.order(u8, &address.routing, "\xff") == .lt);
    const claim_key = address.claimKey();
    const claim: integrity.Claim = .{ .tuple = "parent", .parent_table = "parents", .parent_key = "parent", .schema_version = 1 };
    const claim_bytes = try claim.encode(alloc, address);
    defer alloc.free(claim_bytes);
    // Leftover from an interrupted older attempt: cleanup must prove and erase
    // this exact prefix, never trust an empty caller-authored exhaustion page.
    try db.core.store.put(&claim_key, claim_bytes);
    const donor_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/donor", .{tmp.sub_path});
    defer alloc.free(donor_path);
    var donor_options = options;
    donor_options.identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 };
    var donor = try @import("antfly_source_root").antfly_sources.physical_db.DB.open(alloc, donor_path, donor_options);
    defer donor.close();
    try donor.updateRange(.{ .start = "", .end = "\xff" });
    try donor.setSchemaJson(alloc, schema);
    const generations = @import("relational_integrity_activation.zig").generationSet(compiled);
    const create = try donor.beginTransactionWithId(@splat(71), 100);
    try donor.writeTransaction(create, .{ .relational_schema_version = 1, .relational_integrity_generation_set = generations, .writes = &.{.{ .key = "parent", .value = "{\"id\":1}" }}, .integrity_commands = &.{.{ .address = address, .operation = .{ .establish = claim } }} });
    try donor.commitTransaction(create, 123);
    // Interrupted older topology work may leave physical rows outside this
    // owner's serving range. The immutable source artifact must exclude them.
    const stale_primary = try @import("../internal_keys.zig").documentKeyAlloc(alloc, "\xffstale");
    defer alloc.free(stale_primary);
    try donor.core.store.put(stale_primary, "{\"id\":999}");
    const donor_identity = try donor.relationalTopologyIdentity();
    const scope: @import("online_source_contract.zig").Scope = .{ .fence = .{ .role = .merge_source, .transition_id = 9, .attempt = 1, .admission_epoch = donor_identity.next_epoch, .owner_group_id = 2, .peer_group_id = 3, .namespace = donor_identity.namespace, .catalog_digest = donor_identity.catalog_digest }, .receiver_namespace = options.identity_namespace.?, .consumer_epoch = 1, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } };
    var unbound = scope;
    unbound.fence.attempt = 0;
    unbound.fence.admission_epoch = 0;
    unbound.fence.catalog_digest = @splat(0);
    unbound.consumer_epoch = 0;
    unbound.copy_attempt = .{ .donor_term = 0, .sequence = 0 };
    const io_contract = @import("online_merge_io_contract.zig");
    for ([_]io_contract.Side{ .donor, .receiver }) |side| {
        const raw_facts = try @import("online_merge_io.zig").executeJson(if (side == .donor) &donor else &db, alloc, .{ .scope = unbound, .operation = .{ .admission = side } }, .none);
        defer alloc.free(raw_facts);
        var facts = try std.json.parseFromSlice(io_contract.AdmissionFacts, alloc, raw_facts, .{});
        defer facts.deinit();
        try std.testing.expect(facts.value.eligible);
        try std.testing.expectEqualDeep(generations, facts.value.integrity.?.generation_set);
        try std.testing.expectEqualDeep(donor_identity.catalog_digest, facts.value.integrity.?.catalog_digest);
    }
    try donor.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 1 });
    const certificate = try donor.prepareOnlineSourcePublication(scope, .none);
    try std.testing.expect(certificate.integrity != null);
    try std.testing.expectEqualDeep(generations, certificate.integrity.?.generation_set);
    var stale_certificate = certificate;
    stale_certificate.integrity.?.catalog_digest[0] ^= 1;
    try std.testing.expectError(error.SourceSnapshotCutMismatch, donor.batchRaftReplicatedApply(.{ .online_source = .{ .publish_certificate = .{ .scope = scope, .certificate = stale_certificate } } }, .{ .term = 1, .index = 2 }));
    try donor.batchRaftReplicatedApply(.{ .online_source = .{ .publish_certificate = .{ .scope = scope, .certificate = certificate } } }, .{ .term = 1, .index = 2 });
    const source: pages.Source = .{ .namespace = donor_identity.namespace, .pin_digest = try certificate.digest(), .applied_index = certificate.cut.applied_index, .retention = .{ .epoch = 1, .after_sequence = certificate.cut.retained_start }, .integrity = certificate.integrity };
    var checkpoint: types.MergeReplicationCheckpoint = .{ .kind = .accept, .transition_id = 9, .donor_group_id = 2, .receiver_group_id = 3, .receiver_base_start = "\xff", .receiver_base_end = "", .merged_start = "", .merged_end = "", .page_source = source, .page_receiver_namespace = options.identity_namespace };
    try db.batch(.{ .merge_checkpoint = checkpoint });
    const context: types.MergeReplicationContext = .{ .transition_id = 9, .donor_group_id = 2, .receiver_group_id = 3, .identity_namespace = options.identity_namespace.?, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } };
    checkpoint.kind = .begin_copy;
    checkpoint.copy_attempt = context.copy_attempt;
    try db.batch(.{ .merge_checkpoint = checkpoint });
    var wrong_source = source;
    wrong_source.pin_digest[0] ^= 1;
    var wrong_checkpoint = checkpoint;
    wrong_checkpoint.page_source = wrong_source;
    try std.testing.expectError(error.MergeCopyFenced, db.batch(.{ .merge_checkpoint = wrong_checkpoint }));
    wrong_checkpoint = checkpoint;
    wrong_checkpoint.copy_attempt.sequence += 1;
    try std.testing.expectError(error.MergeCopyFenced, db.batch(.{ .merge_checkpoint = wrong_checkpoint }));
    try std.testing.expectError(error.IntegrityTopologyBusy, @import("relational_integrity_activation.zig").Page.prepare(alloc, db.backend_runtime.io(), db.core, .{}));
    try std.testing.expectError(error.IntegrityTopologyBusy, db.setSchemaJson(alloc,
        \\{"version":2,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ));
    const Apply = struct {
        fn run(owner: *@import("antfly_source_root").antfly_sources.physical_db.DB, request: types.BatchRequest) !void {
            var batch = request;
            batch.merge_page.?.digest = pages.commandDigest(batch);
            try owner.batch(batch);
        }
        fn pump(from: *@import("antfly_source_root").antfly_sources.physical_db.DB, to: *@import("antfly_source_root").antfly_sources.physical_db.DB, copy_scope: @import("online_source_contract.zig").Scope, cert: @import("../source_snapshot.zig").Certificate, until: pages.Phase) !void {
            for (0..1000) |_| {
                const raw_progress = try to.core.store.get(std.testing.allocator, pages.key);
                defer std.testing.allocator.free(raw_progress);
                var progress = try pages.decode(std.testing.allocator, raw_progress);
                defer progress.deinit();
                if (progress.value.phase == until) return;
                const request: @import("online_merge_io_contract.zig").Request = .{ .scope = copy_scope, .operation = switch (progress.value.phase) {
                    .rows => .{ .snapshot = .{ .receipt = progress.value, .certificate = cert } },
                    .artifacts => .{ .integrity = .{ .receipt = progress.value, .certificate = cert } },
                    .tail => .{ .tail = progress.value },
                    else => return error.UnexpectedPhase,
                } };
                const raw = try @import("online_merge_io.zig").executeJson(from, std.testing.allocator, request, .none);
                defer std.testing.allocator.free(raw);
                var prepared = try std.json.parseFromSlice(@import("online_merge_io_contract.zig").Prepared, std.testing.allocator, raw, .{});
                defer prepared.deinit();
                if (prepared.value.request) |batch| {
                    try to.batch(batch);
                    // A lost reply repeats the identical page, never its effects.
                    try to.batch(batch);
                }
            }
            return error.TestUnexpectedResult;
        }
    };
    var page: pages.Command = .{ .source = source, .sequence = 1, .phase = .cleanup, .exhausted = true, .digest = @splat(0) };
    try Apply.run(&db, .{ .merge_replication = context, .merge_page = page });
    page.sequence = 2;
    page.phase = .cleanup_integrity;
    try std.testing.expectError(error.InvalidMergePage, Apply.run(&db, .{ .merge_replication = context, .merge_page = page }));
    page.integrity = &.{.{ .key = &claim_key, .value = null }};
    page.next = &claim_key;
    try Apply.run(&db, .{ .merge_replication = context, .merge_page = page });
    try Apply.pump(&donor, &db, scope, certificate, .tail);
    try std.testing.expectEqual(@as(u64, 123), try db.getTimestamp(alloc, "parent"));
    db.close();
    db = try @import("antfly_source_root").antfly_sources.physical_db.DB.open(alloc, path, options);
    const lookup_request = try std.json.Stringify.valueAlloc(alloc, .{ .kind = "references", .address = address }, .{});
    defer alloc.free(lookup_request);
    try std.testing.expectError(error.KeyOutOfRange, db.lookup(alloc, &address.routing, .{ .relational_integrity_jobs_json = lookup_request }));
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try @import("relational_integrity_activation.zig").requireReady(&read, compiled);
        try requireServing(&read, db.getRange(), "\xffbase");
        try std.testing.expectError(error.KeyOutOfRange, requireServing(&read, db.getRange(), &address.routing));
    }
    const reference: integrity.Reference = .{ .child_table = "children", .child_key = "child", .constraint_name = "fk", .constraint_generation = @splat(4) };
    const attach = try donor.beginTransactionWithId(@splat(72), 200);
    try donor.writeTransaction(attach, .{ .relational_schema_version = 1, .relational_integrity_generation_set = generations, .writes = &.{.{ .key = "parent", .value = "{\"id\":1}" }}, .integrity_commands = &.{.{ .address = address, .operation = .{ .attach = reference } }} });
    try donor.commitTransaction(attach, 222);
    try donor.batchRaftReplicatedApply(.{ .relational_topology = .{ .fence = scope.fence, .action = .begin } }, .{ .term = 1, .index = 3 });
    try donor.batchRaftReplicatedApply(.{ .online_source = .{ .final_fence = .{ .scope = scope, .expected_sequence = 1 } } }, .{ .term = 1, .index = 4 });
    donor.close();
    donor = try @import("antfly_source_root").antfly_sources.physical_db.DB.open(alloc, donor_path, donor_options);
    try Apply.pump(&donor, &db, scope, certificate, .complete);
    try std.testing.expectEqual(@as(u64, 222), try db.getTimestamp(alloc, "parent"));
    try std.testing.expectError(error.KeyOutOfRange, db.lookup(alloc, &address.routing, .{ .relational_integrity_jobs_json = lookup_request }));
    checkpoint.kind = .bootstrap_complete;
    checkpoint.bootstrap_applied_index = 4;
    checkpoint.page_source = null;
    checkpoint.page_receiver_namespace = null;
    try db.batch(.{ .merge_checkpoint = checkpoint });
    checkpoint.kind = .finalize;
    try db.batch(.{ .merge_checkpoint = checkpoint });
    var visible = (try db.lookup(alloc, &address.routing, .{ .relational_integrity_jobs_json = lookup_request })).?;
    defer visible.deinit(alloc);
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expect(try rawRange(&read) == null);
    try @import("relational_integrity_activation.zig").requireReady(&read, compiled);
}
