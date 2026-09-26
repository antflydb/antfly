// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Source-owner authority for an empty-generation relational rewrite. The
//! fence closes admission before a read-indexed summary is taken. No scan of
//! historical tombstones occurs in the deterministic Raft apply path.
const std = @import("std");
const topology = @import("relational_integrity_topology_contract.zig");
const admission = @import("relational_integrity_generation_admission.zig");
const retirement = @import("relational_integrity_generation_retirement.zig");
const portable = @import("../portable_backup.zig");
const integrity = @import("relational_integrity_contract.zig");

pub const intent_key = "\x00\x00__metadata__:empty_generation_handoff_intent";
pub const seal_key = "\x00\x00__metadata__:empty_generation_handoff_seal";
pub const install_receipt_key = "\x00\x00__metadata__:empty_generation_handoff_installed";

pub const Install = topology.GenerationHandoffInstall;

pub fn installReceiptDigest(command: Install) !integrity.Digest {
    try command.validate();
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly empty generation mapped admissions v1");
    hash.update(&command.scope);
    hash.update(&command.source_summary_digest);
    hash.update(&command.retired_digest);
    var number: [8]u8 = undefined;
    std.mem.writeInt(u64, &number, command.retired_count, .little);
    hash.update(&number);
    std.mem.writeInt(u64, &number, @intCast(command.mappings.len), .little);
    hash.update(&number);
    for (command.mappings) |mapping| {
        inline for (.{ mapping.source_child_table_id, mapping.target_child_table_id }) |value| {
            std.mem.writeInt(u64, &number, value, .little);
            hash.update(&number);
        }
        for ([_][]const u8{ mapping.source_child_table_name, mapping.target_child_table_name, mapping.constraint_name }) |name| {
            std.mem.writeInt(u64, &number, @intCast(name.len), .little);
            hash.update(&number);
            hash.update(name);
        }
        const source_generation = mapping.source_generation.?;
        const target_generation = mapping.target_generation.?;
        hash.update(&source_generation);
        hash.update(&target_generation);
        hash.update(&mapping.source_scope_digest);
    }
    var digest: integrity.Digest = undefined;
    hash.final(&digest);
    return digest;
}

pub fn loadInstallReceipt(txn: anytype) !?@import("restore_staging_contract.zig").GenerationAdmissionReceipt {
    const bytes = (try optional(txn, install_receipt_key)) orelse return null;
    return try @import("restore_staging_contract.zig").GenerationAdmissionReceipt.decode(bytes);
}

fn admittedScopeCount(txn: anytype) !usize {
    var cursor = try txn.openCursor();
    defer cursor.close();
    var count: usize = 0;
    var item = try cursor.seekAtOrAfter(admission.prefix);
    while (item) |record| : (item = try cursor.next()) {
        if (!std.mem.startsWith(u8, record.key, admission.prefix)) break;
        if (record.key.len != admission.prefix.len + 32 or count >= portable.max_source_generation_admissions)
            return error.GenerationAdmissionChanged;
        count += 1;
    }
    return count;
}

pub fn stageInstall(alloc: std.mem.Allocator, txn: anytype, command: Install, plan_id: [16]u8, term: u64, index: u64) !@import("restore_staging_contract.zig").GenerationAdmissionReceipt {
    const contract = @import("restore_staging_contract.zig");
    const logical = try installReceiptDigest(command);
    if (term == 0 or index == 0) return error.InvalidGenerationHandoff;
    var target_keys: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer target_keys.deinit(alloc);
    for (command.mappings) |mapping| {
        const key = try admission.scopeKey(mapping.target_child_table_name, mapping.constraint_name);
        if ((try target_keys.getOrPut(alloc, key[admission.prefix.len..].*)).found_existing)
            return error.InvalidGenerationHandoff;
    }
    if (try loadInstallReceipt(txn)) |receipt| {
        if (!std.mem.eql(u8, &receipt.scope, &command.scope) or
            !std.mem.eql(u8, &receipt.source_summary_digest, &command.source_summary_digest) or
            !std.mem.eql(u8, &receipt.logical_digest, &logical)) return error.GenerationAdmissionChanged;
        for (command.mappings) |mapping| {
            const existing = (try admission.load(txn, mapping.target_child_table_name, mapping.constraint_name)) orelse return error.GenerationAdmissionChanged;
            if (existing.phase != .active or existing.child_table_id != mapping.target_child_table_id or
                !std.meta.eql(existing.active_generation, mapping.target_generation) or
                !std.mem.eql(u8, &existing.plan_id, &plan_id) or
                !std.mem.eql(u8, &existing.decision_digest, &logical)) return error.GenerationAdmissionChanged;
        }
        if (try admittedScopeCount(txn) != command.mappings.len) return error.GenerationAdmissionChanged;
        return receipt;
    }
    // The hidden target begins with no admitted FK authority. A leftover or
    // forged scope is never silently overwritten by a Plan-bound install.
    if (try admittedScopeCount(txn) != 0) return error.GenerationAdmissionChanged;
    for (command.mappings) |mapping| {
        if (try admission.load(txn, mapping.target_child_table_name, mapping.constraint_name) != null)
            return error.GenerationAdmissionChanged;
        const target: admission.Scope = .{
            .child_table_id = mapping.target_child_table_id,
            .child_table_name = mapping.target_child_table_name,
            .constraint_name = mapping.constraint_name,
            .revision = 1,
            .phase = .active,
            .active_generation = mapping.target_generation,
            .plan_id = plan_id,
            .decision_digest = logical,
        };
        const encoded = try target.encode(alloc);
        defer alloc.free(encoded);
        const key = try admission.scopeKey(mapping.target_child_table_name, mapping.constraint_name);
        try txn.put(&key, encoded);
    }
    const receipt: contract.GenerationAdmissionReceipt = .{
        .scope = command.scope,
        .source_summary_digest = command.source_summary_digest,
        .logical_digest = logical,
        .applied_term = term,
        .applied_index = index,
    };
    const encoded = try receipt.encode();
    try txn.put(install_receipt_key, &encoded);
    return receipt;
}

pub const IntentRecord = struct {
    fence: topology.Fence,
    intent: topology.GenerationHandoffIntent,
};

pub const SealRecord = struct {
    fence: topology.Fence,
    seal: topology.GenerationHandoffSeal,
    term: u64,
    index: u64,
};

fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}

fn validIntent(intent: topology.GenerationHandoffIntent) bool {
    return !std.mem.allEqual(u8, &intent.plan_id, 0) and !std.mem.allEqual(u8, &intent.plan_digest, 0);
}

fn encodeIntent(record: IntentRecord) ![220]u8 {
    if (record.fence.role != .rewrite_source or !validIntent(record.intent)) return error.InvalidGenerationHandoff;
    var bytes: [220]u8 = @splat(0);
    @memcpy(bytes[0..4], "EGI1");
    const fence_bytes = try record.fence.encode();
    @memcpy(bytes[4..140], &fence_bytes);
    @memcpy(bytes[140..156], &record.intent.plan_id);
    @memcpy(bytes[156..188], &record.intent.plan_digest);
    std.crypto.hash.Blake3.hash(bytes[0..188], bytes[188..220], .{});
    return bytes;
}

pub fn loadIntent(txn: anytype) !?IntentRecord {
    const bytes = (try optional(txn, intent_key)) orelse return null;
    if (bytes.len != 220 or !std.mem.eql(u8, bytes[0..4], "EGI1")) return error.InvalidGenerationHandoff;
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes[0..188], &digest, .{});
    if (!std.mem.eql(u8, &digest, bytes[188..220])) return error.InvalidGenerationHandoff;
    const record: IntentRecord = .{
        .fence = try topology.Fence.decode(bytes[4..140]),
        .intent = .{ .plan_id = bytes[140..156].*, .plan_digest = bytes[156..188].* },
    };
    if (!validIntent(record.intent) or record.fence.role != .rewrite_source) return error.InvalidGenerationHandoff;
    return record;
}

pub fn stageBegin(txn: anytype, fence: topology.Fence, intent: topology.GenerationHandoffIntent) !void {
    const encoded = try encodeIntent(.{ .fence = fence, .intent = intent });
    if (try loadIntent(txn)) |existing| {
        if (existing.fence.eql(fence) and std.meta.eql(existing.intent, intent)) return;
        if (existing.fence.admission_epoch >= fence.admission_epoch) return error.IntegrityTopologyChanged;
    }
    try txn.put(intent_key, &encoded);
    try txn.delete(seal_key);
}

fn encodeSeal(record: SealRecord) ![292]u8 {
    if (record.fence.role != .rewrite_source or record.term == 0 or record.index == 0 or
        std.mem.allEqual(u8, &record.seal.plan_digest, 0) or
        std.mem.allEqual(u8, &record.seal.admissions_digest, 0) or
        std.mem.allEqual(u8, &record.seal.retired_digest, 0)) return error.InvalidGenerationHandoff;
    var bytes: [292]u8 = @splat(0);
    @memcpy(bytes[0..4], "EGS1");
    const fence_bytes = try record.fence.encode();
    @memcpy(bytes[4..140], &fence_bytes);
    @memcpy(bytes[140..172], &record.seal.plan_digest);
    @memcpy(bytes[172..204], &record.seal.admissions_digest);
    @memcpy(bytes[204..236], &record.seal.retired_digest);
    std.mem.writeInt(u64, bytes[236..244], record.seal.retired_count, .little);
    std.mem.writeInt(u64, bytes[244..252], record.term, .little);
    std.mem.writeInt(u64, bytes[252..260], record.index, .little);
    std.crypto.hash.Blake3.hash(bytes[0..260], bytes[260..292], .{});
    return bytes;
}

pub fn loadSeal(txn: anytype) !?SealRecord {
    const bytes = (try optional(txn, seal_key)) orelse return null;
    if (bytes.len != 292 or !std.mem.eql(u8, bytes[0..4], "EGS1")) return error.InvalidGenerationHandoff;
    var checksum: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes[0..260], &checksum, .{});
    if (!std.mem.eql(u8, &checksum, bytes[260..292])) return error.InvalidGenerationHandoff;
    const record: SealRecord = .{
        .fence = try topology.Fence.decode(bytes[4..140]),
        .seal = .{
            .plan_digest = bytes[140..172].*,
            .admissions_digest = bytes[172..204].*,
            .retired_digest = bytes[204..236].*,
            .retired_count = std.mem.readInt(u64, bytes[236..244], .little),
        },
        .term = std.mem.readInt(u64, bytes[244..252], .little),
        .index = std.mem.readInt(u64, bytes[252..260], .little),
    };
    _ = try encodeSeal(record);
    return record;
}

pub fn stageSeal(txn: anytype, fence: topology.Fence, seal: topology.GenerationHandoffSeal, term: u64, index: u64) !void {
    const current = (try @import("relational_integrity_topology.zig").current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!current.eql(fence)) return error.IntegrityTopologyChanged;
    const intent = (try loadIntent(txn)) orelse return error.GenerationHandoffIntentMissing;
    if (!intent.fence.eql(fence) or !std.mem.eql(u8, &intent.intent.plan_digest, &seal.plan_digest)) return error.IntegrityTopologyChanged;
    const record: SealRecord = .{ .fence = fence, .seal = seal, .term = term, .index = index };
    if (try loadSeal(txn)) |existing| {
        if (!existing.fence.eql(fence) or !std.meta.eql(existing.seal, seal)) return error.IntegrityTopologyChanged;
        return;
    }
    const encoded = try encodeSeal(record);
    try txn.put(seal_key, &encoded);
}

pub fn stageCancel(txn: anytype, fence: topology.Fence) !void {
    const sealed = try loadSeal(txn);
    if (try loadIntent(txn)) |record| {
        if (sealed) |receipt| if (!receipt.fence.eql(record.fence)) return error.InvalidGenerationHandoff;
        if (!record.fence.eql(fence)) {
            const completed = (try @import("relational_integrity_topology.zig").completed(txn)) orelse return error.IntegrityTopologyChanged;
            if (!completed.eql(record.fence) or record.fence.admission_epoch >= fence.admission_epoch)
                return error.IntegrityTopologyChanged;
            return; // A newer non-handoff rewrite never revokes an old seal.
        }
        if (sealed != null) return error.GenerationHandoffSealed;
        try txn.delete(intent_key);
    } else if (sealed) |receipt| {
        if (receipt.fence.eql(fence)) return error.GenerationHandoffSealed;
        return error.InvalidGenerationHandoff;
    }
}

pub fn active(txn: anytype) !bool {
    const record = (try loadIntent(txn)) orelse {
        if (try loadSeal(txn) != null) return error.InvalidGenerationHandoff;
        return false;
    };
    const fence = (try @import("relational_integrity_topology.zig").current(txn)) orelse return false;
    return fence.eql(record.fence);
}

pub const Summary = struct {
    namespace: @import("doc_identity_namespace.zig").Namespace,
    admissions: []portable.SourceGenerationAdmissionSummaryEntry,
    admissions_digest: integrity.Digest,
    retired_digest: integrity.Digest,
    retired_count: u64,
    intent: ?topology.GenerationHandoffIntent,
    seal: ?SealRecord,

    pub fn deinit(self: *Summary, alloc: std.mem.Allocator) void {
        portable.freeSourceGenerationAdmissionSummary(alloc, self.admissions);
    }
};

/// Constant-work receipt read after the preflight summary has been durably
/// recorded. The source fence freezes admissions and retirements, so retrying
/// the seal must not rescan historical tombstones or rebuild live scopes.
pub const SealStatus = struct {
    namespace: @import("doc_identity_namespace.zig").Namespace,
    intent: ?topology.GenerationHandoffIntent,
    seal: ?SealRecord,
};

pub fn sealStatus(txn: anytype, namespace: @import("doc_identity_namespace.zig").Namespace) !SealStatus {
    const fence = (try @import("relational_integrity_topology.zig").current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!fence.namespace.eql(namespace)) return error.IdentityNamespaceMismatch;
    const intent = (try loadIntent(txn)) orelse return error.GenerationHandoffIntentMissing;
    if (!intent.fence.eql(fence)) return error.IntegrityTopologyChanged;
    const seal = try loadSeal(txn);
    if (seal) |record| if (!record.fence.eql(fence)) return error.IntegrityTopologyChanged;
    return .{ .namespace = namespace, .intent = intent.intent, .seal = seal };
}

/// Called on the linearizable owner read path, after the fence has closed
/// admission. The bounded live-scope projection is sorted logically; the
/// tombstone digest streams physical records in key order without allocation.
pub fn summaryAlloc(alloc: std.mem.Allocator, txn: anytype, namespace: @import("doc_identity_namespace.zig").Namespace) !Summary {
    const persisted = try loadIntent(txn);
    const fence = try @import("relational_integrity_topology.zig").current(txn);
    if (fence) |active_fence| {
        if (persisted == null or !persisted.?.fence.eql(active_fence)) return error.IntegrityTopologyBusy;
    }
    if (persisted) |record| if (fence != null and !record.fence.namespace.eql(namespace)) return error.IdentityNamespaceMismatch;
    var entries: std.ArrayListUnmanaged(portable.SourceGenerationAdmissionSummaryEntry) = .empty;
    errdefer {
        for (entries.items) |entry| {
            alloc.free(entry.child_table_name);
            alloc.free(entry.constraint_name);
        }
        entries.deinit(alloc);
    }
    var cursor = try txn.openCursor();
    defer cursor.close();
    var item = try cursor.seekAtOrAfter(admission.prefix);
    while (item) |record| : (item = try cursor.next()) {
        if (!std.mem.startsWith(u8, record.key, admission.prefix)) break;
        if (entries.items.len >= portable.max_source_generation_admissions or record.key.len != admission.prefix.len + 32)
            return error.InvalidGenerationAdmission;
        const scope = try admission.Scope.decode(record.value);
        if (scope.phase != .active) return error.IntegrityTopologyBusy;
        if (!std.mem.eql(u8, record.key, &try admission.scopeKey(scope.child_table_name, scope.constraint_name)))
            return error.InvalidGenerationAdmission;
        const canonical = try scope.encode(alloc);
        defer alloc.free(canonical);
        if (!std.mem.eql(u8, record.value, canonical)) return error.InvalidGenerationAdmission;
        var digest: integrity.Digest = undefined;
        std.crypto.hash.Blake3.hash(record.value, &digest, .{});
        const child_name = try alloc.dupe(u8, scope.child_table_name);
        const constraint_name = alloc.dupe(u8, scope.constraint_name) catch |err| {
            alloc.free(child_name);
            return err;
        };
        entries.append(alloc, .{
            .child_table_id = scope.child_table_id,
            .child_table_name = child_name,
            .constraint_name = constraint_name,
            .active_generation = scope.active_generation,
            .source_scope_digest = digest,
        }) catch |err| {
            alloc.free(child_name);
            alloc.free(constraint_name);
            return err;
        };
    }
    std.mem.sort(portable.SourceGenerationAdmissionSummaryEntry, entries.items, {}, struct {
        fn less(_: void, a: portable.SourceGenerationAdmissionSummaryEntry, b: portable.SourceGenerationAdmissionSummaryEntry) bool {
            const order = std.mem.order(u8, a.child_table_name, b.child_table_name);
            return order == .lt or (order == .eq and std.mem.order(u8, a.constraint_name, b.constraint_name) == .lt);
        }
    }.less);
    const admissions_digest = try portable.sourceGenerationAdmissionSummaryDigest(namespace, entries.items);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("empty-generation-retirements-v1");
    var number: [8]u8 = undefined;
    inline for (.{ namespace.table_id, namespace.shard_id, namespace.range_id }) |part| {
        std.mem.writeInt(u64, &number, part, .little);
        hash.update(&number);
    }
    var retired_count: u64 = 0;
    item = try cursor.seekAtOrAfter(retirement.active_prefix);
    while (item) |record| : (item = try cursor.next()) {
        if (!std.mem.startsWith(u8, record.key, retirement.active_prefix)) break;
        if (record.key.len != retirement.active_prefix.len + 16) return error.InvalidGenerationRetirement;
        _ = try retirement.Active.decode(record.value, record.key[retirement.active_prefix.len..][0..16].*);
        std.mem.writeInt(u64, &number, record.key.len, .little);
        hash.update(&number);
        hash.update(record.key);
        std.mem.writeInt(u64, &number, record.value.len, .little);
        hash.update(&number);
        hash.update(record.value);
        retired_count = std.math.add(u64, retired_count, 1) catch return error.InvalidGenerationRetirement;
    }
    std.mem.writeInt(u64, &number, retired_count, .little);
    hash.update(&number);
    var retired_digest: integrity.Digest = undefined;
    hash.final(&retired_digest);
    const seal = if (fence != null) try loadSeal(txn) else null;
    const owned_entries = try entries.toOwnedSlice(alloc);
    return .{
        .namespace = namespace,
        .admissions = owned_entries,
        .admissions_digest = admissions_digest,
        .retired_digest = retired_digest,
        .retired_count = retired_count,
        .intent = if (fence != null) persisted.?.intent else null,
        .seal = seal,
    };
}

test "empty generation handoff intent and seal are fence-bound and cancel only before seal" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const Mock = struct {
        alloc: std.mem.Allocator,
        records: std.StringHashMap([]const u8),
        cursor_opens: usize = 0,

        const Cursor = struct {
            const Entry = struct { key: []const u8, value: []const u8 };
            pub fn seekAtOrAfter(_: *@This(), _: []const u8) !?Entry {
                return null;
            }
            pub fn next(_: *@This()) !?Entry {
                return null;
            }
            pub fn close(_: *@This()) void {}
        };

        pub fn openCursor(self: *@This()) !Cursor {
            self.cursor_opens += 1;
            return .{};
        }

        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.records.get(key) orelse error.NotFound;
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            try self.records.put(key, try self.alloc.dupe(u8, value));
        }
        pub fn delete(self: *@This(), key: []const u8) !void {
            _ = self.records.remove(key);
        }
    };
    var txn: Mock = .{ .alloc = alloc, .records = std.StringHashMap([]const u8).init(alloc) };
    const fence: topology.Fence = .{
        .transition_id = 7,
        .attempt = 1,
        .peer_group_id = 8,
        .owner_group_id = 9,
        .role = .rewrite_source,
        .namespace = .{ .table_id = 10, .shard_id = 9, .range_id = 9 },
        .catalog_digest = @splat(1),
    };
    const fence_bytes = try fence.encode();
    try std.testing.expectError(error.IntegrityTopologyFenceMissing, sealStatus(&txn, fence.namespace));
    var preview = try summaryAlloc(alloc, &txn, fence.namespace);
    defer preview.deinit(alloc);
    try std.testing.expect(preview.intent == null and preview.seal == null);
    try txn.put(topology.fence_key, &fence_bytes);
    const intent: topology.GenerationHandoffIntent = .{ .plan_id = @splat(2), .plan_digest = @splat(3) };
    try stageBegin(&txn, fence, intent);
    const cursor_opens_before_status = txn.cursor_opens;
    const begin_status = try sealStatus(&txn, fence.namespace);
    try std.testing.expect(std.meta.eql(begin_status.intent.?, intent));
    try std.testing.expect(begin_status.seal == null);
    try std.testing.expectEqual(cursor_opens_before_status, txn.cursor_opens);
    var wrong_namespace = fence.namespace;
    wrong_namespace.range_id += 1;
    try std.testing.expectError(error.IdentityNamespaceMismatch, sealStatus(&txn, wrong_namespace));
    var fenced = try summaryAlloc(alloc, &txn, fence.namespace);
    defer fenced.deinit(alloc);
    try std.testing.expect(std.meta.eql(fenced.intent.?, intent));
    try std.testing.expectEqualDeep(preview.admissions_digest, fenced.admissions_digest);
    try std.testing.expectEqualDeep(preview.retired_digest, fenced.retired_digest);
    const transition: admission.Transition = .{
        .child_table_id = 11,
        .child_table_name = "children",
        .constraint_name = "parent_fk",
        .expected_generation = null,
        .next_generation = @splat(8),
        .plan_id = @splat(9),
        .decision_digest = @splat(10),
    };
    try std.testing.expectError(error.IntegrityTopologyChanged, admission.stageTransition(alloc, &txn, transition));
    try std.testing.expectError(error.IntegrityTopologyChanged, retirement.stageChildGenerationRetirements(alloc, &txn, fence, &.{transition}));
    try std.testing.expect(std.meta.eql(intent, (try loadIntent(&txn)).?.intent));
    try stageCancel(&txn, fence);
    try std.testing.expect(try loadIntent(&txn) == null);
    try stageBegin(&txn, fence, intent);
    const seal: topology.GenerationHandoffSeal = .{ .plan_digest = intent.plan_digest, .admissions_digest = @splat(4), .retired_digest = @splat(5), .retired_count = 2 };
    try stageSeal(&txn, fence, seal, 6, 7);
    const sealed_status = try sealStatus(&txn, fence.namespace);
    try std.testing.expectEqual(@as(u64, 7), sealed_status.seal.?.index);
    try std.testing.expectEqual(cursor_opens_before_status + 1, txn.cursor_opens);
    try stageSeal(&txn, fence, seal, 6, 7);
    try stageSeal(&txn, fence, seal, 6, 8); // lost reply, new Raft entry
    try std.testing.expectEqual(@as(u64, 7), (try loadSeal(&txn)).?.index);
    try std.testing.expectError(error.GenerationHandoffSealed, stageCancel(&txn, fence));
    var changed = seal;
    changed.retired_count += 1;
    try std.testing.expectError(error.IntegrityTopologyChanged, stageSeal(&txn, fence, changed, 6, 8));
}

test "empty generation install digest binds mapped active generations and empty attestations" {
    const empty: Install = .{ .scope = @splat(1), .source_summary_digest = @splat(2), .retired_digest = @splat(3), .retired_count = 0, .mappings = &.{} };
    const digest = try installReceiptDigest(empty);
    var changed = empty;
    changed.retired_count = 1;
    try std.testing.expect(!std.mem.eql(u8, &digest, &try installReceiptDigest(changed)));
    changed = empty;
    changed.source_summary_digest = @splat(4);
    try std.testing.expect(!std.mem.eql(u8, &digest, &try installReceiptDigest(changed)));
}

test "empty generation owner lookup streams a coherent summary from a read transaction" {
    const db_mod = @import("db.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/empty-generation-summary", .{tmp.sub_path});
    defer alloc.free(path);
    const namespace: @import("doc_identity_namespace.zig").Namespace = .{
        .table_id = 20,
        .shard_id = 21,
        .range_id = 21,
    };
    var db = try db_mod.DB.open(alloc, path, .{
        .identity_namespace = namespace,
        .primary_backend = .{ .lsm = .{} },
        .start_index_workers = false,
        .start_optional_runtimes = false,
    });
    defer db.close();
    var result = (try db.lookup(alloc, "", .{ .relational_topology_json = "{\"mode\":\"generation_handoff_summary\"}" })) orelse return error.TestMissingGenerationHandoffSummary;
    defer result.deinit(alloc);
    var parsed = try std.json.parseFromSlice(Summary, alloc, result.json, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    try std.testing.expect(parsed.value.namespace.eql(namespace));
    try std.testing.expectEqual(@as(usize, 0), parsed.value.admissions.len);
    try std.testing.expectEqual(@as(u64, 0), parsed.value.retired_count);
    try std.testing.expect(parsed.value.intent == null and parsed.value.seal == null);
}
