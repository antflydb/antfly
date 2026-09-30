// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! One hidden parent-owner transaction remaps sealed source FK admission proof
//! to the target incarnation. Imported proof keys are evidence, not authority.
const std = @import("std");
const contract = @import("restore_staging_contract.zig");
const admission = @import("relational_integrity_generation_admission.zig");
const portable = @import("../portable_backup.zig");

pub fn loadReceipt(txn: anytype) !?contract.GenerationAdmissionReceipt {
    const bytes = txn.get(contract.generation_admission_receipt_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try contract.GenerationAdmissionReceipt.decode(bytes);
}

/// Seal proof-only metadata before the restore can enter cutover. A missing,
/// extra, or modified scope must fail while cancellation is still safe.
pub fn verifyImportedProofSummary(
    alloc: std.mem.Allocator,
    txn: anytype,
    namespace: @import("doc_identity_namespace.zig").Namespace,
    expected: [32]u8,
) !void {
    var entries: std.ArrayListUnmanaged(portable.SourceGenerationAdmissionSummaryEntry) = .empty;
    defer {
        for (entries.items) |entry| {
            alloc.free(entry.child_table_name);
            alloc.free(entry.constraint_name);
        }
        entries.deinit(alloc);
    }
    var cursor = try txn.openCursor();
    defer cursor.close();
    var item = try cursor.seekAtOrAfter(portable.source_generation_admission_prefix);
    while (item) |record| : (item = try cursor.next()) {
        if (!std.mem.startsWith(u8, record.key, portable.source_generation_admission_prefix)) break;
        if (entries.items.len >= portable.max_source_generation_admissions or
            record.key.len != portable.source_generation_admission_prefix.len + 32)
            return error.RestoreSourceProofMissing;
        const scope = admission.Scope.decode(record.value) catch return error.RestoreSourceProofMissing;
        const key = try admission.scopeKey(scope.child_table_name, scope.constraint_name);
        if (scope.phase != .active or
            !std.mem.eql(u8, record.key[portable.source_generation_admission_prefix.len..], key[admission.prefix.len..]))
            return error.RestoreSourceProofMissing;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(record.value, &digest, .{});
        const child_name = try alloc.dupe(u8, scope.child_table_name);
        errdefer alloc.free(child_name);
        const constraint_name = try alloc.dupe(u8, scope.constraint_name);
        errdefer alloc.free(constraint_name);
        try entries.append(alloc, .{
            .child_table_id = scope.child_table_id,
            .child_table_name = child_name,
            .constraint_name = constraint_name,
            .active_generation = scope.active_generation,
            .source_scope_digest = digest,
        });
    }
    std.mem.sort(portable.SourceGenerationAdmissionSummaryEntry, entries.items, {}, struct {
        fn less(_: void, lhs: portable.SourceGenerationAdmissionSummaryEntry, rhs: portable.SourceGenerationAdmissionSummaryEntry) bool {
            const order = std.mem.order(u8, lhs.child_table_name, rhs.child_table_name);
            return order == .lt or (order == .eq and std.mem.order(u8, lhs.constraint_name, rhs.constraint_name) == .lt);
        }
    }.less);
    if (!std.mem.eql(u8, &expected, &try portable.sourceGenerationAdmissionSummaryDigest(namespace, entries.items)))
        return error.RestoreSourceProofMissing;
}

pub fn stageInstall(
    alloc: std.mem.Allocator,
    txn: anytype,
    command: contract.InstallGenerationAdmissions,
    applied_term: u64,
    applied_index: u64,
) !contract.GenerationAdmissionReceipt {
    try command.validate();
    const logical_digest = try contract.admissionReceiptDigest(command);
    const raw_progress = txn.get(contract.key) catch |err| switch (err) {
        error.NotFound => return error.RestoreStagingScopeChanged,
        else => return err,
    };
    var progress = try contract.Progress.decode(alloc, raw_progress);
    defer progress.deinit();
    if (!std.mem.eql(u8, &progress.value.scope.digest(), &command.scope) or
        progress.value.scope.empty_generation or progress.value.scope.rewrite != null)
        return error.RestoreStagingScopeChanged;
    if (try loadReceipt(txn)) |existing| {
        if (!std.mem.eql(u8, &existing.scope, &command.scope) or
            !std.mem.eql(u8, &existing.source_summary_digest, &command.source_summary_digest) or
            !std.mem.eql(u8, &existing.logical_digest, &logical_digest) or
            progress.value.phase == .canceled) return error.RestoreStagingScopeChanged;
        for (command.mappings) |mapping| {
            const accepted = (try admission.load(txn, mapping.target_child_table_name, mapping.constraint_name)) orelse
                return error.GenerationAdmissionChanged;
            if (accepted.phase != .active or accepted.child_table_id != mapping.target_child_table_id or
                !std.meta.eql(accepted.active_generation, mapping.target_generation) or
                !std.mem.eql(u8, &accepted.plan_id, &progress.value.scope.plan_id) or
                !std.mem.eql(u8, &accepted.decision_digest, &logical_digest))
                return error.GenerationAdmissionChanged;
        }
        return existing;
    }
    if (progress.value.phase != .validated or !progress.value.source_generation_proofs_complete or applied_term == 0 or applied_index == 0)
        return error.RestoreStagingInProgress;

    const source_entries = try alloc.alloc(portable.SourceGenerationAdmissionSummaryEntry, command.mappings.len);
    defer alloc.free(source_entries);
    var source_keys: std.AutoHashMapUnmanaged([32]u8, usize) = .empty;
    defer source_keys.deinit(alloc);
    var target_keys: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer target_keys.deinit(alloc);
    for (command.mappings, 0..) |mapping, index| {
        source_entries[index] = .{
            .child_table_id = mapping.source_child_table_id,
            .child_table_name = mapping.source_child_table_name,
            .constraint_name = mapping.constraint_name,
            .active_generation = mapping.source_generation,
            .source_scope_digest = mapping.source_scope_digest,
        };
        const source_key = try admission.scopeKey(mapping.source_child_table_name, mapping.constraint_name);
        const source_slot = try source_keys.getOrPut(alloc, source_key[admission.prefix.len..].*);
        if (source_slot.found_existing) return error.InvalidRestoreStagingCommand;
        source_slot.value_ptr.* = index;
        const target_key = try admission.scopeKey(mapping.target_child_table_name, mapping.constraint_name);
        if ((try target_keys.getOrPut(alloc, target_key[admission.prefix.len..].*)).found_existing)
            return error.InvalidRestoreStagingCommand;
        if (try admission.load(txn, mapping.target_child_table_name, mapping.constraint_name) != null)
            return error.GenerationAdmissionChanged;
    }
    if (!std.mem.eql(u8, &command.source_summary_digest, &try portable.sourceGenerationAdmissionSummaryDigest(progress.value.scope.source_namespace, source_entries)))
        return error.RestoreSourceProofMissing;

    var seen: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer seen.deinit(alloc);
    var cursor = try txn.openCursor();
    defer cursor.close();
    var item = try cursor.seekAtOrAfter(portable.source_generation_admission_prefix);
    while (item) |record| : (item = try cursor.next()) {
        if (!std.mem.startsWith(u8, record.key, portable.source_generation_admission_prefix)) break;
        if (record.key.len != portable.source_generation_admission_prefix.len + 32)
            return error.RestoreSourceProofMissing;
        const key_hash = record.key[portable.source_generation_admission_prefix.len..][0..32].*;
        const index = source_keys.get(key_hash) orelse return error.RestoreSourceProofMissing;
        if ((try seen.getOrPut(alloc, key_hash)).found_existing) return error.RestoreSourceProofMissing;
        const mapping = command.mappings[index];
        const source_scope = admission.Scope.decode(record.value) catch return error.RestoreSourceProofMissing;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(record.value, &digest, .{});
        if (source_scope.phase != .active or source_scope.child_table_id != mapping.source_child_table_id or
            !std.mem.eql(u8, source_scope.child_table_name, mapping.source_child_table_name) or
            !std.mem.eql(u8, source_scope.constraint_name, mapping.constraint_name) or
            !std.meta.eql(source_scope.active_generation, mapping.source_generation) or
            !std.mem.eql(u8, &digest, &mapping.source_scope_digest))
            return error.RestoreSourceProofMissing;
    }
    if (seen.count() != command.mappings.len) return error.RestoreSourceProofMissing;

    for (command.mappings) |mapping| {
        const target: admission.Scope = .{
            .child_table_id = mapping.target_child_table_id,
            .child_table_name = mapping.target_child_table_name,
            .constraint_name = mapping.constraint_name,
            .revision = 1,
            .phase = .active,
            .active_generation = mapping.target_generation,
            .plan_id = progress.value.scope.plan_id,
            .decision_digest = logical_digest,
        };
        const encoded = try target.encode(alloc);
        defer alloc.free(encoded);
        const key = try admission.scopeKey(mapping.target_child_table_name, mapping.constraint_name);
        try txn.put(&key, encoded);
        const source_key = try admission.scopeKey(mapping.source_child_table_name, mapping.constraint_name);
        var proof_key: [portable.source_generation_admission_prefix.len + 32]u8 = undefined;
        @memcpy(proof_key[0..portable.source_generation_admission_prefix.len], portable.source_generation_admission_prefix);
        @memcpy(proof_key[portable.source_generation_admission_prefix.len..], source_key[admission.prefix.len..]);
        try txn.delete(&proof_key);
    }
    const receipt: contract.GenerationAdmissionReceipt = .{
        .scope = command.scope,
        .source_summary_digest = command.source_summary_digest,
        .logical_digest = logical_digest,
        .applied_term = applied_term,
        .applied_index = applied_index,
    };
    const encoded_receipt = try receipt.encode();
    try txn.put(contract.generation_admission_receipt_key, &encoded_receipt);
    return receipt;
}

test "mapped restore admission install requires exact imported proof and persists atomically" {
    const db_mod = @import("db.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/mapped-admission", .{tmp.sub_path});
    defer alloc.free(path);
    const options: db_mod.OpenOptions = .{
        .identity_namespace = .{ .table_id = 20, .shard_id = 21, .range_id = 21 },
        .primary_backend = .{ .lsm = .{} },
        .start_index_workers = false,
        .start_optional_runtimes = false,
    };
    var db = try db_mod.DB.open(alloc, path, options);
    defer db.close();
    const scope: contract.Scope = .{
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .source_artifact_digest = @splat(3),
        .source_namespace = .{ .table_id = 10, .shard_id = 11, .range_id = 11 },
        .target_namespace = options.identity_namespace.?,
        .target_schema_digest = @splat(4),
    };
    const source: admission.Scope = .{
        .child_table_id = 10,
        .child_table_name = "source_child",
        .constraint_name = "fk_parent",
        .revision = 7,
        .phase = .active,
        .active_generation = @splat(5),
        .plan_id = @splat(6),
        .decision_digest = @splat(7),
    };
    const source_bytes = try source.encode(alloc);
    defer alloc.free(source_bytes);
    var source_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(source_bytes, &source_digest, .{});
    const entries = [_]portable.SourceGenerationAdmissionSummaryEntry{.{
        .child_table_id = source.child_table_id,
        .child_table_name = source.child_table_name,
        .constraint_name = source.constraint_name,
        .active_generation = source.active_generation,
        .source_scope_digest = source_digest,
    }};
    const mapping = [_]contract.GenerationAdmissionMapping{.{
        .source_child_table_id = source.child_table_id,
        .source_child_table_name = source.child_table_name,
        .target_child_table_id = 20,
        .target_child_table_name = "target_child",
        .constraint_name = source.constraint_name,
        .source_generation = source.active_generation,
        .target_generation = @splat(8),
        .source_scope_digest = source_digest,
    }};
    const command: contract.InstallGenerationAdmissions = .{
        .scope = scope.digest(),
        .source_summary_digest = try portable.sourceGenerationAdmissionSummaryDigest(scope.source_namespace, &entries),
        .mappings = &mapping,
    };
    const source_key = try admission.scopeKey(source.child_table_name, source.constraint_name);
    var proof_key: [portable.source_generation_admission_prefix.len + 32]u8 = undefined;
    @memcpy(proof_key[0..portable.source_generation_admission_prefix.len], portable.source_generation_admission_prefix);
    @memcpy(proof_key[portable.source_generation_admission_prefix.len..], source_key[admission.prefix.len..]);
    const progress = try (contract.Progress{ .scope = scope, .phase = .validated, .source_generation_proofs_complete = true }).encode(alloc);
    defer alloc.free(progress);
    {
        var txn = try db.core.store.beginWriteTxn();
        var open = true;
        defer if (open) txn.abort();
        try txn.put(contract.key, progress);
        try txn.put(&proof_key, source_bytes);
        try txn.commit();
        open = false;
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        var damaged = mapping;
        damaged[0].source_scope_digest[0] ^= 1;
        var bad = command;
        bad.mappings = &damaged;
        try std.testing.expectError(error.RestoreSourceProofMissing, stageInstall(alloc, &txn, bad, 2, 3));
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        var changed = source;
        changed.revision += 1;
        const changed_bytes = try changed.encode(alloc);
        defer alloc.free(changed_bytes);
        try txn.put(&proof_key, changed_bytes);
        try std.testing.expectError(error.RestoreSourceProofMissing, stageInstall(alloc, &txn, command, 2, 3));
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        var extra = source;
        extra.constraint_name = "unlisted_fk";
        const extra_key = try admission.scopeKey(extra.child_table_name, extra.constraint_name);
        var extra_proof_key: [portable.source_generation_admission_prefix.len + 32]u8 = undefined;
        @memcpy(extra_proof_key[0..portable.source_generation_admission_prefix.len], portable.source_generation_admission_prefix);
        @memcpy(extra_proof_key[portable.source_generation_admission_prefix.len..], extra_key[admission.prefix.len..]);
        const extra_bytes = try extra.encode(alloc);
        defer alloc.free(extra_bytes);
        try txn.put(&extra_proof_key, extra_bytes);
        try std.testing.expectError(error.RestoreSourceProofMissing, stageInstall(alloc, &txn, command, 2, 3));
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        var open = true;
        defer if (open) txn.abort();
        const receipt = try stageInstall(alloc, &txn, command, 2, 3);
        try std.testing.expectEqual(@as(u64, 3), receipt.applied_index);
        try txn.commit();
        open = false;
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        const receipt = try stageInstall(alloc, &txn, command, 2, 4);
        try std.testing.expectEqual(@as(u64, 3), receipt.applied_index);
        const target = (try admission.load(&txn, "target_child", "fk_parent")) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(u64, 20), target.child_table_id);
        try std.testing.expect(std.meta.eql(@as(?@TypeOf(source.active_generation.?), @splat(8)), target.active_generation));
        try std.testing.expectError(error.NotFound, txn.get(&proof_key));
    }
}
