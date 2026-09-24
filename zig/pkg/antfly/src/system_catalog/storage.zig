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

//! Transactional catalog persistence. Records and name indexes are separate so
//! routing resolves a qualified name with point reads. Derived indexes are rebuilt
//! from authoritative records at projection initialization and snapshot install.
const std = @import("std");
const docstore = @import("../storage/docstore.zig");
const domain = @import("domain.zig");
const settings = @import("settings.zig");
const policies = @import("policies.zig");

pub const Meta = domain.Meta;

pub const OwnedState = domain.OwnedState;

pub fn prefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:system_catalog:{d}:", .{group_id});
}

fn keyAlloc(alloc: std.mem.Allocator, group_id: u64, suffix: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:system_catalog:{d}:{s}", .{ group_id, suffix });
}

fn recordKeyAlloc(alloc: std.mem.Allocator, group_id: u64, kind: domain.Kind, id: u64) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:system_catalog:{d}:record:{s}:{d}", .{ group_id, @tagName(kind), id });
}

fn settingKeyAlloc(alloc: std.mem.Allocator, group_id: u64, id: u64) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:system_catalog:{d}:setting:{d}", .{ group_id, id });
}

fn policyKeyAlloc(alloc: std.mem.Allocator, group_id: u64, id: u64) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:system_catalog:{d}:policy:{d}", .{ group_id, id });
}

fn policyPublicationKeyAlloc(alloc: std.mem.Allocator, group_id: u64, table_id: u64) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:system_catalog:{d}:policy-publication:{d}", .{ group_id, table_id });
}

fn policyPublicationStampKeyAlloc(alloc: std.mem.Allocator, group_id: u64, table_id: u64) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:system_catalog:{d}:policy-publication-stamp:{d}", .{ group_id, table_id });
}

fn policyPublicationWorkPrefixAlloc(alloc: std.mem.Allocator, group_id: u64) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:system_catalog:{d}:policy-publication-work:", .{group_id});
}

fn policyPublicationWorkKeyAlloc(alloc: std.mem.Allocator, group_id: u64, table_id: u64) ![]u8 {
    const prefix = try policyPublicationWorkPrefixAlloc(alloc, group_id);
    defer alloc.free(prefix);
    const key = try alloc.alloc(u8, prefix.len + 8);
    @memcpy(key[0..prefix.len], prefix);
    std.mem.writeInt(u64, key[prefix.len..][0..8], table_id, .big);
    return key;
}

pub fn policyPublicationNeedsWork(publication: policies.Publication) bool {
    return switch (publication.phase) {
        .active => false,
        .disabled => publication.disabled_acknowledged_owners.len != publication.required_owners.len,
        .pending_install, .pending_disable, .serving_install, .serving_disable => true,
    };
}

/// One indexed, cyclic work item under the caller's read-index view. The
/// durable work key is transactionally maintained with publication state, so
/// healthy inactive tables cost no scan or JSON decode on every supervisor
/// tick. A successor after `after_table_id` is preferred for fair rotation.
pub fn nextPolicyPublicationWork(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, after_table_id: u64) !?std.json.Parsed(policies.Publication) {
    const prefix = try policyPublicationWorkPrefixAlloc(alloc, group_id);
    defer alloc.free(prefix);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const seek = if (after_table_id == std.math.maxInt(u64))
        prefix
    else
        try policyPublicationWorkKeyAlloc(alloc, group_id, after_table_id + 1);
    defer if (seek.ptr != prefix.ptr) alloc.free(seek);
    var entry = try cursor.seekAtOrAfter(seek);
    if (entry == null or !std.mem.startsWith(u8, entry.?.key, prefix)) entry = try cursor.seekAtOrAfter(prefix);
    const found = entry orelse return null;
    if (!std.mem.startsWith(u8, found.key, prefix)) return null;
    if (found.key.len != prefix.len + 8 or found.value.len != 1 or found.value[0] != 1) return error.InvalidRowPolicyPublication;
    const table_id = std.mem.readInt(u64, found.key[prefix.len..][0..8], .big);
    var publication = (try loadPolicyPublication(alloc, txn, group_id, table_id)) orelse return error.InvalidRowPolicyPublication;
    errdefer publication.deinit();
    if (!policyPublicationNeedsWork(publication.value)) return error.InvalidRowPolicyPublication;
    return publication;
}

fn policyInstallSnapshotKeyAlloc(alloc: std.mem.Allocator, group_id: u64, table_id: u64, generation: u64, phase: policies.Publication.Phase) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:system_catalog:{d}:policy-install:{d}:{d}:{s}", .{ group_id, table_id, generation, @tagName(phase) });
}

pub fn loadPolicyInstallSnapshotBytes(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, request: policies.InstallRequest) ![]u8 {
    var publication = (try loadPolicyPublication(alloc, txn, group_id, request.table_id)) orelse return error.RowPolicyCatalogChanged;
    defer publication.deinit();
    if (publication.value.generation != request.expected_generation or
        publication.value.catalog_epoch != request.expected_catalog_epoch) return error.RowPolicyCatalogChanged;
    var exact_owner = false;
    for (publication.value.required_owners) |owner| {
        if (owner.group_id == request.owner_group_id and
            std.mem.eql(u8, &owner.descriptor_digest, &request.expected_descriptor_digest))
        {
            exact_owner = true;
            break;
        }
    }
    if (!exact_owner) return error.RowPolicyCatalogChanged;
    return loadPolicyInstallSnapshotBytesForPhase(alloc, txn, group_id, request.table_id, request.expected_generation, request.expected_catalog_epoch, request.expected_phase);
}

fn loadPolicyInstallSnapshotBytesForPhase(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, table_id: u64, generation: u64, catalog_epoch: u64, phase: policies.Publication.Phase) ![]u8 {
    const key = try policyInstallSnapshotKeyAlloc(alloc, group_id, table_id, generation, phase);
    defer alloc.free(key);
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return error.RowPolicyCatalogChanged,
        else => return err,
    };
    if (bytes.len > 4 * 1024 * 1024) return error.RowPolicyLimitExceeded;
    var parsed = try std.json.parseFromSlice(policies.InstallSnapshot, alloc, bytes, .{});
    defer parsed.deinit();
    try parsed.value.validateShape();
    if (parsed.value.table_id != table_id or
        parsed.value.policy_generation != generation or
        parsed.value.catalog_epoch != catalog_epoch or
        parsed.value.phase != phase) return error.RowPolicyCatalogChanged;
    return alloc.dupe(u8, bytes);
}

/// Resolve the immutable serving generation, never the mutable policy draft.
/// A pending install or disable continues to serve its previously active cut.
pub fn loadServingPolicyInstallSnapshot(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, table_id: u64) !std.json.Parsed(policies.InstallSnapshot) {
    var publication = (try loadPolicyPublication(alloc, txn, group_id, table_id)) orelse return error.RowPolicyUnsupported;
    defer publication.deinit();
    const generation = switch (publication.value.phase) {
        .active => publication.value.generation,
        .pending_install, .pending_disable => publication.value.serving_generation orelse return error.RowPolicyUnsupported,
        .serving_install, .serving_disable => return error.RowPolicyUnsupported,
        .disabled => return error.RowPolicyUnsupported,
    };
    const key = try policyInstallSnapshotKeyAlloc(alloc, group_id, table_id, generation, .active);
    defer alloc.free(key);
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return error.RowPolicyCatalogChanged,
        else => return err,
    };
    if (bytes.len > policies.max_install_snapshot_bytes) return error.RowPolicyLimitExceeded;
    var parsed = try std.json.parseFromSlice(policies.InstallSnapshot, alloc, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.validateShape();
    if (parsed.value.table_id != table_id or parsed.value.policy_generation != generation or parsed.value.phase != .active or
        (publication.value.serving_catalog_epoch != null and parsed.value.catalog_epoch != publication.value.serving_catalog_epoch.?) or
        parsed.value.schema_version != publication.value.schema_version or !std.mem.eql(u8, &parsed.value.schema_digest, &publication.value.schema_digest))
        return error.RowPolicyCatalogChanged;
    return parsed;
}

fn putPolicyInstallSnapshot(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, snapshot: policies.InstallSnapshot) !void {
    try snapshot.validateShape();
    const key = try policyInstallSnapshotKeyAlloc(alloc, group_id, snapshot.table_id, snapshot.policy_generation, snapshot.phase);
    defer alloc.free(key);
    const encoded = try std.json.Stringify.valueAlloc(alloc, snapshot, .{});
    defer alloc.free(encoded);
    if (encoded.len > 4 * 1024 * 1024) return error.RowPolicyLimitExceeded;
    const existing = txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (existing) |prior| {
        if (!std.mem.eql(u8, prior, encoded)) return error.RowPolicyCatalogChanged;
        return;
    }
    try txn.put(key, encoded);
}

pub fn loadPolicyPublication(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, table_id: u64) !?std.json.Parsed(policies.Publication) {
    const key = try policyPublicationKeyAlloc(alloc, group_id, table_id);
    defer alloc.free(key);
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    var parsed = try std.json.parseFromSlice(policies.Publication, alloc, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.validateShape();
    if (parsed.value.table_id != table_id) return error.InvalidRowPolicyPublication;
    return parsed;
}

pub fn loadPolicyPublicationStamp(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, table_id: u64) !policies.PublicationStamp {
    const key = try policyPublicationStampKeyAlloc(alloc, group_id, table_id);
    defer alloc.free(key);
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return error.RowPolicyCatalogChanged,
        else => return err,
    };
    var parsed = try std.json.parseFromSlice(policies.PublicationStamp, alloc, bytes, .{});
    defer parsed.deinit();
    try parsed.value.validateShape();
    if (parsed.value.table_id != table_id) return error.RowPolicyCatalogChanged;
    return parsed.value;
}

pub fn loadPolicyPublications(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64) ![]const policies.Publication {
    const prefix = try keyAlloc(alloc, group_id, "policy-publication:");
    defer alloc.free(prefix);
    const kvs = try docstore.DocStore.scanPrefixTxn(alloc, txn, prefix);
    defer {
        for (kvs) |kv| {
            alloc.free(kv.key);
            alloc.free(kv.value);
        }
        alloc.free(kvs);
    }
    if (kvs.len > 4096) return error.RowPolicyLimitExceeded;
    const out = try alloc.alloc(policies.Publication, kvs.len);
    for (kvs, out, 0..) |kv, *publication, index| {
        publication.* = try std.json.parseFromSliceLeaky(policies.Publication, alloc, kv.value, .{ .allocate = .alloc_always });
        try publication.validateShape();
        const expected = try policyPublicationKeyAlloc(alloc, group_id, publication.table_id);
        defer alloc.free(expected);
        if (!std.mem.eql(u8, expected, kv.key)) return error.InvalidRowPolicyPublication;
        for (out[0..index]) |prior| if (prior.table_id == publication.table_id) return error.InvalidRowPolicyPublication;
    }
    return out;
}

pub fn loadPolicies(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64) ![]const policies.Record {
    const prefix = try keyAlloc(alloc, group_id, "policy:");
    defer alloc.free(prefix);
    const kvs = try docstore.DocStore.scanPrefixTxn(alloc, txn, prefix);
    defer {
        for (kvs) |kv| {
            alloc.free(kv.key);
            alloc.free(kv.value);
        }
        alloc.free(kvs);
    }
    if (kvs.len > 1024) return error.RowPolicyLimitExceeded;
    const records = try alloc.alloc(policies.Record, kvs.len);
    for (kvs, records, 0..) |kv, *record, i| {
        record.* = try std.json.parseFromSliceLeaky(policies.Record, alloc, kv.value, .{ .allocate = .alloc_always });
        try record.validateShape();
        const expected_key = try policyKeyAlloc(alloc, group_id, record.id);
        defer alloc.free(expected_key);
        if (!std.mem.eql(u8, kv.key, expected_key)) return error.InvalidRowPolicyRecord;
        for (records[0..i]) |prior| if (prior.id == record.id or (prior.table_id == record.table_id and std.ascii.eqlIgnoreCase(prior.name, record.name))) return error.InvalidRowPolicyRecord;
    }
    return records;
}

/// Only trusted admin admission may propose this internal transition. The
/// deterministic apply path checks revision and table identity again; it does
/// not activate policy enforcement merely by storing a definition.
pub fn applyPolicyCommand(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, command: policies.Command, command_hash: [32]u8) !void {
    if (command.version != 1) return error.InvalidRowPolicyRecord;
    const meta = try readMeta(alloc, txn, group_id);
    if (meta.revision != command.expected_revision) return;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const records = try loadPolicies(a, txn, group_id);
    var next_id = meta.next_id;
    switch (command.change) {
        .put => |record| {
            try record.validateShape();
            var publication = try loadPolicyPublication(a, txn, group_id, record.table_id);
            defer if (publication) |*value| value.deinit();
            // Active readers use the immutable installed generation. New
            // definitions are a draft until a later publication succeeds.
            // Freeze the draft only while that candidate is being installed.
            if (publication != null and (publication.?.value.phase == .pending_install or publication.?.value.phase == .serving_install or publication.?.value.phase == .pending_disable or publication.?.value.phase == .serving_disable)) return error.RowPolicyCatalogChanged;
            const table = (try getById(a, txn, group_id, .table, record.table_id)) orelse return error.TableNotFound;
            defer table.deinit();
            var existing: ?policies.Record = null;
            for (records) |prior| {
                if (prior.id == record.id) existing = prior;
                if (prior.table_id == record.table_id and std.ascii.eqlIgnoreCase(prior.name, record.name) and prior.id != record.id) return error.InvalidRowPolicyRecord;
            }
            if (existing) |prior| {
                if (prior.table_id != record.table_id or !std.mem.eql(u8, prior.name, record.name) or record.generation != try std.math.add(u64, prior.generation, 1)) return error.RowPolicyCatalogChanged;
            } else {
                if (record.id != meta.next_id or record.generation != 1) return error.RowPolicyCatalogChanged;
                next_id = try std.math.add(u64, next_id, 1);
            }
            const key = try policyKeyAlloc(a, group_id, record.id);
            const encoded = try std.json.Stringify.valueAlloc(a, record, .{});
            try txn.put(key, encoded);
        },
        .drop => |identity| {
            var publication = try loadPolicyPublication(a, txn, group_id, identity.table_id);
            defer if (publication) |*value| value.deinit();
            if (publication != null and (publication.?.value.phase == .pending_install or publication.?.value.phase == .serving_install or publication.?.value.phase == .pending_disable or publication.?.value.phase == .serving_disable)) return error.RowPolicyCatalogChanged;
            const prior = for (records) |record| {
                if (record.id == identity.id) break record;
            } else return error.RowPolicyCatalogChanged;
            if (prior.generation != identity.generation or prior.table_id != identity.table_id) return error.RowPolicyCatalogChanged;
            const key = try policyKeyAlloc(a, group_id, identity.id);
            try txn.delete(key);
        },
    }
    try applyDelta(alloc, txn, group_id, .{ .upserts = @constCast(&[_]domain.Resource{}), .removes = @constCast(&[_]domain.Resource{}), .next_id = next_id }, meta, command_hash);
}

/// Replicated, deterministic publication transition. Metadata admission
/// verifies the owner topology cut before proposing `begin` and `promote`;
/// apply independently checks monotonic identity and exact committed ACKs.
pub fn applyPolicyPublicationCommand(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, command: policies.PublicationCommand, command_hash: [32]u8) !void {
    if (command.version != 1) return error.InvalidRowPolicyPublication;
    const meta = try readMeta(alloc, txn, group_id);
    if (meta.revision != command.expected_revision) return;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var next: policies.Publication = undefined;
    var retained_prior: ?std.json.Parsed(policies.Publication) = null;
    defer if (retained_prior) |*value| value.deinit();
    switch (command.change) {
        .begin => |incoming| {
            try incoming.validateShape();
            if (incoming.phase != .pending_install and incoming.phase != .pending_disable) return error.InvalidRowPolicyPublication;
            if (incoming.acknowledged_owners.len != 0 or incoming.serving_acknowledged_owners.len != 0 or incoming.disabled_acknowledged_owners.len != 0 or incoming.catalog_epoch != try std.math.add(u64, meta.revision, 1)) return error.RowPolicyCatalogChanged;
            const table = (try getById(a, txn, group_id, .table, incoming.table_id)) orelse return error.TableNotFound;
            defer table.deinit();
            var prior = try loadPolicyPublication(a, txn, group_id, incoming.table_id);
            defer if (prior) |*value| value.deinit();
            if (prior) |value| {
                if (value.value.phase != .active and value.value.phase != .disabled) return error.RowPolicyCatalogChanged;
                if (value.value.phase == .disabled and value.value.disabled_acknowledged_owners.len != value.value.required_owners.len) return error.RowPolicyCatalogChanged;
                if (incoming.generation <= value.value.generation or incoming.catalog_epoch <= value.value.catalog_epoch) return error.RowPolicyCatalogChanged;
                if (incoming.phase == .pending_disable and value.value.phase != .active) return error.RowPolicyCatalogChanged;
                if (incoming.serving_generation != (if (value.value.phase == .active) value.value.generation else null)) return error.RowPolicyCatalogChanged;
            } else if (incoming.phase == .pending_disable) return error.RowPolicyCatalogChanged;
            if (prior == null and incoming.serving_generation != null) return error.RowPolicyCatalogChanged;
            const records = try loadPolicies(a, txn, group_id);
            var selected: std.ArrayList(policies.Record) = .empty;
            defer selected.deinit(a);
            for (records) |record| if (record.table_id == incoming.table_id) {
                if (record.schema_version != incoming.schema_version or !std.mem.eql(u8, &record.schema_digest, &incoming.schema_digest)) return error.RowPolicyCatalogChanged;
                try selected.append(a, record);
            };
            if (incoming.phase == .pending_install and selected.items.len == 0) return error.RowPolicyCatalogChanged;
            const setting_records = try loadSettings(a, txn, group_id);
            try putPolicyInstallSnapshot(a, txn, group_id, .{
                .table_id = incoming.table_id,
                .schema_version = incoming.schema_version,
                .schema_digest = incoming.schema_digest,
                .policy_generation = incoming.generation,
                .catalog_epoch = incoming.catalog_epoch,
                .phase = incoming.phase,
                .records = selected.items,
                .settings = setting_records,
            });
            next = incoming;
        },
        .acknowledge => |input| {
            retained_prior = (try loadPolicyPublication(a, txn, group_id, input.table_id)) orelse return error.RowPolicyCatalogChanged;
            next = retained_prior.?.value;
            if (next.generation != input.generation or
                (next.phase != .pending_install and next.phase != .pending_disable and
                    next.phase != .serving_install and next.phase != .serving_disable and next.phase != .disabled) or
                input.receipt.catalog_epoch != next.catalog_epoch or input.receipt.phase != next.phase or
                input.receipt.applied_term == 0 or input.receipt.applied_index == 0) return error.RowPolicyCatalogChanged;
            var matching_owner = false;
            for (next.required_owners) |owner| if (owner.group_id == input.receipt.owner.group_id and std.mem.eql(u8, &owner.descriptor_digest, &input.receipt.owner.descriptor_digest)) {
                matching_owner = true;
                break;
            };
            if (!matching_owner) return error.RowPolicyCatalogChanged;
            const exact_bundle = try loadPolicyInstallSnapshotBytesForPhase(a, txn, group_id, next.table_id, next.generation, next.catalog_epoch, next.phase);
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(exact_bundle, &digest, .{});
            if (!std.mem.eql(u8, &input.receipt.bundle_digest, &digest)) return error.RowPolicyCatalogChanged;
            const prior_acks = switch (next.phase) {
                .pending_install, .pending_disable => next.acknowledged_owners,
                .serving_install, .serving_disable => next.serving_acknowledged_owners,
                .disabled => next.disabled_acknowledged_owners,
                .active => unreachable,
            };
            if (prior_acks.len != 0 and !std.mem.eql(u8, &prior_acks[0].bundle_digest, &input.receipt.bundle_digest)) return error.RowPolicyCatalogChanged;
            var insert_index: usize = 0;
            while (insert_index < prior_acks.len and prior_acks[insert_index].owner.group_id < input.receipt.owner.group_id) insert_index += 1;
            if (insert_index < prior_acks.len and prior_acks[insert_index].owner.group_id == input.receipt.owner.group_id) return error.RowPolicyCatalogChanged;
            const acknowledgements = try a.alloc(policies.Publication.OwnerAck, prior_acks.len + 1);
            @memcpy(acknowledgements[0..insert_index], prior_acks[0..insert_index]);
            acknowledgements[insert_index] = input.receipt;
            @memcpy(acknowledgements[insert_index + 1 ..], prior_acks[insert_index..]);
            switch (next.phase) {
                .pending_install, .pending_disable => next.acknowledged_owners = acknowledgements,
                .serving_install, .serving_disable => next.serving_acknowledged_owners = acknowledgements,
                .disabled => next.disabled_acknowledged_owners = acknowledgements,
                .active => unreachable,
            }
        },
        .promote => |input| {
            retained_prior = (try loadPolicyPublication(a, txn, group_id, input.table_id)) orelse return error.RowPolicyCatalogChanged;
            next = retained_prior.?.value;
            if (next.generation != input.generation or next.acknowledged_owners.len != next.required_owners.len) return error.RowPolicyCatalogChanged;
            const from = next.phase;
            const complete_serving = switch (from) {
                .pending_install => input.phase == .serving_install,
                .pending_disable => input.phase == .serving_disable,
                .serving_install => input.phase == .active and next.serving_acknowledged_owners.len == next.required_owners.len,
                .serving_disable => input.phase == .disabled and next.serving_acknowledged_owners.len == next.required_owners.len,
                .active, .disabled => false,
            };
            if (!complete_serving) return error.RowPolicyCatalogChanged;
            const prior_bytes = try loadPolicyInstallSnapshotBytesForPhase(a, txn, group_id, next.table_id, next.generation, next.catalog_epoch, from);
            var prior = try std.json.parseFromSlice(policies.InstallSnapshot, a, prior_bytes, .{});
            defer prior.deinit();
            prior.value.phase = input.phase;
            try putPolicyInstallSnapshot(a, txn, group_id, prior.value);
            next.phase = input.phase;
            if (input.phase == .active or input.phase == .disabled) {
                next.serving_generation = null;
                next.serving_catalog_epoch = null;
            }
        },
    }
    try next.validateShape();
    const key = try policyPublicationKeyAlloc(a, group_id, next.table_id);
    const encoded = try std.json.Stringify.valueAlloc(a, next, .{});
    try txn.put(key, encoded);
    const stamp_key = try policyPublicationStampKeyAlloc(a, group_id, next.table_id);
    const stamp_bytes = try std.json.Stringify.valueAlloc(a, policies.PublicationStamp.fromPublication(next), .{});
    try txn.put(stamp_key, stamp_bytes);
    const work_key = try policyPublicationWorkKeyAlloc(a, group_id, next.table_id);
    if (policyPublicationNeedsWork(next))
        try txn.put(work_key, &.{1})
    else
        try txn.delete(work_key);
    try applyDelta(alloc, txn, group_id, .{ .upserts = @constCast(&[_]domain.Resource{}), .removes = @constCast(&[_]domain.Resource{}), .next_id = meta.next_id }, meta, command_hash);
}

pub fn loadSettings(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64) ![]const settings.Record {
    const prefix = try keyAlloc(alloc, group_id, "setting:");
    defer alloc.free(prefix);
    const kvs = try docstore.DocStore.scanPrefixTxn(alloc, txn, prefix);
    defer {
        for (kvs) |kv| {
            alloc.free(kv.key);
            alloc.free(kv.value);
        }
        alloc.free(kvs);
    }
    if (kvs.len > 1024) return error.SettingLimitExceeded;
    const records = try alloc.alloc(settings.Record, kvs.len);
    for (kvs, records, 0..) |kv, *record, i| {
        record.* = try std.json.parseFromSliceLeaky(settings.Record, alloc, kv.value, .{ .allocate = .alloc_always });
        try record.validate();
        const expected_key = try settingKeyAlloc(alloc, group_id, record.identity.id);
        defer alloc.free(expected_key);
        if (!std.mem.eql(u8, kv.key, expected_key)) return error.InvalidSettingRecord;
        for (records[0..i]) |prior| if (prior.identity.id == record.identity.id or std.ascii.eqlIgnoreCase(prior.name, record.name)) return error.InvalidSettingRecord;
    }
    return records;
}

pub fn applySettingCommand(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, command: settings.Command, command_hash: [32]u8) !void {
    if (command.version != 1) return error.InvalidSettingRecord;
    const meta = try readMeta(alloc, txn, group_id);
    if (meta.revision != command.expected_revision) return;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const records = try loadSettings(a, txn, group_id);
    const publication_sensitive = switch (command.change) {
        .put => |record| blk: {
            if (record.policy_sensitive) break :blk true;
            for (records) |prior| if (prior.identity.id == record.identity.id and prior.policy_sensitive) break :blk true;
            break :blk false;
        },
        .drop => |identity| blk: {
            for (records) |prior| if (prior.identity.id == identity.id) break :blk prior.policy_sensitive;
            break :blk false;
        },
    };
    if (publication_sensitive) {
        const publications = try loadPolicyPublications(a, txn, group_id);
        for (publications) |publication| if (publication.phase != .disabled) return error.RowPolicyCatalogChanged;
    }
    var next_id = meta.next_id;
    switch (command.change) {
        .put => |record| {
            try record.validate();
            var existing: ?settings.Record = null;
            for (records) |prior| {
                if (prior.identity.id == record.identity.id) existing = prior;
                if (std.ascii.eqlIgnoreCase(prior.name, record.name) and prior.identity.id != record.identity.id) return error.InvalidSettingRecord;
            }
            if (existing) |prior| {
                if (!std.mem.eql(u8, prior.name, record.name) or record.identity.generation != try std.math.add(u64, prior.identity.generation, 1)) return error.SettingCatalogChanged;
            } else {
                if (record.identity.id != meta.next_id or record.identity.generation != 1) return error.SettingCatalogChanged;
                next_id = try std.math.add(u64, meta.next_id, 1);
            }
            const key = try settingKeyAlloc(a, group_id, record.identity.id);
            const encoded = try std.json.Stringify.valueAlloc(a, record, .{});
            try txn.put(key, encoded);
        },
        .drop => |identity| {
            const prior = for (records) |record| {
                if (record.identity.id == identity.id) break record;
            } else return error.SettingCatalogChanged;
            if (prior.identity.generation != identity.generation) return error.SettingCatalogChanged;
            const key = try settingKeyAlloc(a, group_id, identity.id);
            try txn.delete(key);
        },
    }
    try applyDelta(alloc, txn, group_id, .{ .upserts = @constCast(&[_]domain.Resource{}), .removes = @constCast(&[_]domain.Resource{}), .next_id = next_id }, meta, command_hash);
}

pub fn nameKeyAlloc(alloc: std.mem.Allocator, group_id: u64, kind: domain.Kind, parent: u64, name: []const u8) ![]u8 {
    try domain.validateResourceName(kind, name);
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata_derived__:system_catalog_name:{d}:{s}:{d}:{s}", .{ group_id, @tagName(kind), parent, name });
}

pub fn readMeta(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64) !Meta {
    const key = try keyAlloc(alloc, group_id, "meta");
    defer alloc.free(key);
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return .{},
        else => return err,
    };
    var parsed = try std.json.parseFromSlice(Meta, alloc, bytes, .{});
    defer parsed.deinit();
    if (parsed.value.version != 1 or parsed.value.next_id < 3) return error.InvalidCatalogRecord;
    return parsed.value;
}

pub fn loadState(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64) !OwnedState {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    const meta = try readMeta(a, txn, group_id);
    const prefix = try keyAlloc(a, group_id, "record:");
    const kvs = try docstore.DocStore.scanPrefixTxn(a, txn, prefix);
    const resources = try a.alloc(domain.Resource, kvs.len);
    for (kvs, resources) |kv, *resource| {
        resource.* = try std.json.parseFromSliceLeaky(domain.Resource, a, kv.value, .{ .allocate = .alloc_always });
        try domain.validateResourceName(resource.kind, resource.name);
        if (resource.id == 0) return error.InvalidCatalogRecord;
        const expected_key = try recordKeyAlloc(a, group_id, resource.kind, resource.id);
        if (!std.mem.eql(u8, expected_key, kv.key)) return error.InvalidCatalogRecord;
    }
    const loaded_settings = try loadSettings(a, txn, group_id);
    const loaded_policies = try loadPolicies(a, txn, group_id);
    const loaded_publications = try loadPolicyPublications(a, txn, group_id);
    return .{ .arena = arena, .meta = meta, .value = .{ .revision = meta.revision, .next_id = meta.next_id, .resources = resources, .settings = loaded_settings, .policies = loaded_policies, .policy_publications = loaded_publications } };
}

pub fn getById(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, kind: domain.Kind, id: u64) !?std.json.Parsed(domain.Resource) {
    const key = try recordKeyAlloc(alloc, group_id, kind, id);
    defer alloc.free(key);
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    var parsed = try std.json.parseFromSlice(domain.Resource, alloc, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    if (parsed.value.kind != kind or parsed.value.id != id) return error.InvalidCatalogRecord;
    return parsed;
}

pub fn find(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, kind: domain.Kind, parent: u64, name: []const u8) !?std.json.Parsed(domain.Resource) {
    const key = try nameKeyAlloc(alloc, group_id, kind, parent, name);
    defer alloc.free(key);
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (bytes.len != 8) return error.InvalidCatalogRecord;
    const id = std.mem.readInt(u64, bytes[0..8], .little);
    var resource = (try getById(alloc, txn, group_id, kind, id)) orelse return error.InvalidCatalogRecord;
    errdefer resource.deinit();
    if (resource.value.parent_id != parent or !std.mem.eql(u8, resource.value.name, name)) return error.InvalidCatalogRecord;
    return resource;
}

/// All returned records borrow this view's arena. Its transaction pins one
/// catalog revision for both planning and response projection.
pub const View = struct {
    alloc: std.mem.Allocator,
    txn: *docstore.DocStore.Txn,
    group_id: u64,
    meta: Meta,

    pub fn byId(self: View, kind: domain.Kind, id: u64) !?domain.Resource {
        if (try getById(self.alloc, self.txn, self.group_id, kind, id)) |record| return record.value;
        return (domain.State{}).byId(kind, id);
    }
    pub fn lookup(self: View, kind: domain.Kind, parent: u64, name: []const u8) !?domain.Resource {
        if (try find(self.alloc, self.txn, self.group_id, kind, parent, name)) |record| return record.value;
        return (domain.State{}).find(kind, parent, name);
    }
    pub fn children(self: View, kind: domain.Kind, parent: u64, limit: usize) ![]const domain.Resource {
        return self.childrenPage(kind, parent, limit, "", null);
    }
    pub fn childrenPage(self: View, kind: domain.Kind, parent: u64, limit: usize, name_prefix: []const u8, after: ?[]const u8) ![]const domain.Resource {
        const base = try std.fmt.allocPrint(self.alloc, "\x00\x00__metadata_derived__:system_catalog_name:{d}:children:{s}:{d}:", .{ self.group_id, @tagName(kind), parent });
        const prefix = try std.mem.concat(self.alloc, u8, &.{ base, name_prefix });
        const seek = if (after) |name| try std.mem.concat(self.alloc, u8, &.{ base, name }) else prefix;
        var cursor = try self.txn.openCursor();
        defer cursor.close();
        var out: std.ArrayListUnmanaged(domain.Resource) = .empty;
        var entry = try cursor.seekAtOrAfter(if (std.mem.lessThan(u8, seek, prefix)) prefix else seek);
        while (entry) |kv| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, kv.key, prefix)) break;
            const record = try std.json.parseFromSliceLeaky(domain.Resource, self.alloc, kv.value, .{ .allocate = .alloc_always });
            if (record.kind != kind or record.parent_id != parent or record.id == 0 or !std.mem.eql(u8, kv.key, try childKey(self.alloc, self.group_id, record))) return error.InvalidCatalogRecord;
            try domain.validateResourceName(record.kind, record.name);
            if (after) |name| if (!std.mem.lessThan(u8, name, record.name)) continue;
            try out.append(self.alloc, record);
            if (limit != 0 and out.items.len >= limit) break;
        }
        if (out.items.len == 0 and self.meta.revision == 0) {
            if (kind == .database and parent == 0) try out.append(self.alloc, domain.default_database);
            if (kind == .namespace and parent == domain.default_database_id) try out.append(self.alloc, domain.default_namespace);
        }
        return out.items;
    }
    pub fn bindingForStorage(self: View, name: []const u8) !?domain.Resource {
        const key = try storageBindingKey(self.alloc, self.group_id, name);
        const bytes = self.txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        if (bytes.len != 8) return error.InvalidCatalogRecord;
        const record = (try self.byId(.table, std.mem.readInt(u64, bytes[0..8], .little))) orelse return error.InvalidCatalogRecord;
        if (!std.mem.eql(u8, record.storage_name, name)) return error.InvalidCatalogRecord;
        return record;
    }
    pub fn tablespaceInUse(self: View, id: u64) !bool {
        const prefix = try std.fmt.allocPrint(self.alloc, "\x00\x00__metadata_derived__:system_catalog_name:{d}:uses:{d}:", .{ self.group_id, id });
        var cursor = try self.txn.openCursor();
        defer cursor.close();
        const kv = (try cursor.seekAtOrAfter(prefix)) orelse return false;
        if (!std.mem.startsWith(u8, kv.key, prefix)) return false;
        const suffix = kv.key[prefix.len..];
        const colon = std.mem.indexOfScalar(u8, suffix, ':') orelse return error.InvalidCatalogRecord;
        const kind = std.meta.stringToEnum(domain.Kind, suffix[0..colon]) orelse return error.InvalidCatalogRecord;
        const resource_id = std.fmt.parseInt(u64, suffix[colon + 1 ..], 10) catch return error.InvalidCatalogRecord;
        const record = (try self.byId(kind, resource_id)) orelse return error.InvalidCatalogRecord;
        if (record.tablespace_id != id) return error.InvalidCatalogRecord;
        return true;
    }
    pub fn namespaceFor(self: View, database: []const u8, namespace: []const u8) !domain.Resource {
        const db = (try self.lookup(.database, 0, database)) orelse return error.DatabaseNotFound;
        return (try self.lookup(.namespace, db.id, namespace)) orelse error.NamespaceNotFound;
    }
    pub fn effectiveTablespace(self: View, namespace: domain.Resource, explicit: u64) !?domain.Resource {
        const db = (try self.byId(.database, namespace.parent_id)) orelse return error.DatabaseNotFound;
        const id = if (explicit != 0) explicit else if (namespace.tablespace_id != 0) namespace.tablespace_id else db.tablespace_id;
        return if (id == 0) null else (try self.byId(.tablespace, id)) orelse return error.TablespaceNotFound;
    }
    pub fn read(self: View, request: domain.Read) !domain.State {
        if (request.kind == .table) return error.InvalidCatalogMutation;
        const parent = if (request.kind == .namespace) ((try self.lookup(.database, 0, request.database)) orelse return error.DatabaseNotFound).id else 0;
        var out: std.ArrayListUnmanaged(domain.Resource) = .empty;
        if (request.name) |name| {
            try out.append(self.alloc, (try self.lookup(request.kind, parent, name)) orelse return error.CatalogNotFound);
        } else try out.appendSlice(self.alloc, try self.children(request.kind, parent, 0));
        var related: std.AutoHashMapUnmanaged(u64, void) = .empty;
        const count = out.items.len;
        for (0..count) |i| {
            const id = out.items[i].tablespace_id;
            if (id == 0 or related.contains(id)) continue;
            try related.put(self.alloc, id, {});
            try out.append(self.alloc, (try self.byId(.tablespace, id)) orelse return error.InvalidCatalogRecord);
        }
        if (request.kind == .namespace) try out.append(self.alloc, (try self.byId(.database, parent)) orelse return error.InvalidCatalogRecord);
        return .{ .revision = self.meta.revision, .next_id = self.meta.next_id, .resources = out.items };
    }
};

pub fn namePrefixAlloc(alloc: std.mem.Allocator, group_id: u64) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata_derived__:system_catalog_name:{d}:", .{group_id});
}

/// Rebuild only derived rows. Duplicate authoritative names/IDs fail closed.
pub fn rebuildNameIndex(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64) !void {
    var state = try loadState(alloc, txn, group_id);
    defer state.deinit();
    var index = try domain.StateIndex.init(alloc, state.value);
    defer index.deinit(alloc);
    const prefix = try namePrefixAlloc(alloc, group_id);
    defer alloc.free(prefix);
    const rows = try docstore.DocStore.scanPrefixTxn(alloc, txn, prefix);
    defer {
        for (rows) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        alloc.free(rows);
    }
    for (rows) |row| try txn.delete(row.key);
    for (state.value.resources) |resource| {
        const key = try nameKeyAlloc(alloc, group_id, resource.kind, resource.parent_id, resource.name);
        defer alloc.free(key);
        var id: [8]u8 = undefined;
        std.mem.writeInt(u64, &id, resource.id, .little);
        try txn.put(key, &id);
        try writeReferences(alloc, txn, group_id, resource);
    }
}

pub fn validateNameIndex(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64) !void {
    var state = try loadState(alloc, txn, group_id);
    defer state.deinit();
    var index = try domain.StateIndex.init(alloc, state.value);
    defer index.deinit(alloc);
    for (state.value.resources) |resource| {
        var found = (try find(alloc, txn, group_id, resource.kind, resource.parent_id, resource.name)) orelse return error.InvalidCatalogRecord;
        defer found.deinit();
        if (found.value.id != resource.id) return error.InvalidCatalogRecord;
        const child_key = try childKey(alloc, group_id, resource);
        defer alloc.free(child_key);
        const child_json = try std.json.Stringify.valueAlloc(alloc, resource, .{});
        defer alloc.free(child_json);
        if (!std.mem.eql(u8, txn.get(child_key) catch return error.InvalidCatalogRecord, child_json)) return error.InvalidCatalogRecord;
        if (resource.kind == .table) {
            const key = try storageBindingKey(alloc, group_id, resource.storage_name);
            defer alloc.free(key);
            const bytes = txn.get(key) catch return error.InvalidCatalogRecord;
            if (bytes.len != 8 or std.mem.readInt(u64, bytes[0..8], .little) != resource.id) return error.InvalidCatalogRecord;
        }
        if (resource.tablespace_id != 0) {
            const key = try tablespaceUseKey(alloc, group_id, resource);
            defer alloc.free(key);
            _ = txn.get(key) catch return error.InvalidCatalogRecord;
        }
    }
}

fn storageBindingKey(alloc: std.mem.Allocator, group_id: u64, name: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata_derived__:system_catalog_name:{d}:storage:{s}", .{ group_id, name });
}
fn tablespaceUseKey(alloc: std.mem.Allocator, group_id: u64, r: domain.Resource) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata_derived__:system_catalog_name:{d}:uses:{d}:{s}:{d}", .{ group_id, r.tablespace_id, @tagName(r.kind), r.id });
}
// Covering parent rows keep list reads sequential. These values are derived,
// written atomically with the primary record, and rebuilt on index migration.
fn childKey(alloc: std.mem.Allocator, group_id: u64, r: domain.Resource) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata_derived__:system_catalog_name:{d}:children:{s}:{d}:{s}", .{ group_id, @tagName(r.kind), r.parent_id, r.name });
}
fn writeReferences(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, r: domain.Resource) !void {
    const child_key = try childKey(alloc, group_id, r);
    defer alloc.free(child_key);
    const child_json = try std.json.Stringify.valueAlloc(alloc, r, .{});
    defer alloc.free(child_json);
    try txn.put(child_key, child_json);
    if (r.kind == .table) {
        const key = try storageBindingKey(alloc, group_id, r.storage_name);
        defer alloc.free(key);
        var id: [8]u8 = undefined;
        std.mem.writeInt(u64, &id, r.id, .little);
        const previous = txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (previous) |value| if (!std.mem.eql(u8, value, &id)) return error.InvalidCatalogRecord;
        try txn.put(key, &id);
    }
    if (r.tablespace_id != 0) {
        const key = try tablespaceUseKey(alloc, group_id, r);
        defer alloc.free(key);
        try txn.put(key, "");
    }
}
fn removeReferences(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, r: domain.Resource) !void {
    const child_key = try childKey(alloc, group_id, r);
    defer alloc.free(child_key);
    txn.delete(child_key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
    if (r.kind == .table) {
        const key = try storageBindingKey(alloc, group_id, r.storage_name);
        defer alloc.free(key);
        txn.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }
    if (r.tablespace_id != 0) {
        const key = try tablespaceUseKey(alloc, group_id, r);
        defer alloc.free(key);
        txn.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }
}

pub fn writeResource(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, resource: domain.Resource) !void {
    if (try getById(alloc, txn, group_id, resource.kind, resource.id)) |old_value| {
        var old = old_value;
        defer old.deinit();
        try removeReferences(alloc, txn, group_id, old.value);
        const old_name_key = try nameKeyAlloc(alloc, group_id, old.value.kind, old.value.parent_id, old.value.name);
        defer alloc.free(old_name_key);
        txn.delete(old_name_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }
    const record_key = try recordKeyAlloc(alloc, group_id, resource.kind, resource.id);
    defer alloc.free(record_key);
    const name_key = try nameKeyAlloc(alloc, group_id, resource.kind, resource.parent_id, resource.name);
    defer alloc.free(name_key);
    const json = try std.json.Stringify.valueAlloc(alloc, resource, .{});
    defer alloc.free(json);
    var encoded_id: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded_id, resource.id, .little);
    try txn.put(record_key, json);
    try txn.put(name_key, &encoded_id);
    try writeReferences(alloc, txn, group_id, resource);
}

pub fn removeResource(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, resource: domain.Resource) !void {
    try removeReferences(alloc, txn, group_id, resource);
    const record_key = try recordKeyAlloc(alloc, group_id, resource.kind, resource.id);
    defer alloc.free(record_key);
    const name_key = try nameKeyAlloc(alloc, group_id, resource.kind, resource.parent_id, resource.name);
    defer alloc.free(name_key);
    txn.delete(record_key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
    txn.delete(name_key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}

pub fn applyDelta(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, delta: domain.Delta, previous: Meta, command_hash: [32]u8) !void {
    if (previous.revision == 0) {
        try writeResource(alloc, txn, group_id, domain.default_database);
        try writeResource(alloc, txn, group_id, domain.default_namespace);
    }
    var removed_tables: bool = false;
    for (delta.removes) |resource| if (resource.kind == .table) {
        removed_tables = true;
        break;
    };
    if (removed_tables) {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const policy_records = try loadPolicies(a, txn, group_id);
        for (delta.removes) |resource| if (resource.kind == .table) {
            for (policy_records) |record| if (record.table_id == resource.id) {
                const policy_key = try policyKeyAlloc(a, group_id, record.id);
                try txn.delete(policy_key);
            };
            const publication_key = try policyPublicationKeyAlloc(a, group_id, resource.id);
            try txn.delete(publication_key);
            const stamp_key = try policyPublicationStampKeyAlloc(a, group_id, resource.id);
            try txn.delete(stamp_key);
            const work_key = try policyPublicationWorkKeyAlloc(a, group_id, resource.id);
            try txn.delete(work_key);
        };
    }
    for (delta.removes) |resource| try removeResource(alloc, txn, group_id, resource);
    for (delta.upserts) |resource| try writeResource(alloc, txn, group_id, resource);
    const meta: Meta = .{ .revision = try std.math.add(u64, previous.revision, 1), .next_id = delta.next_id, .last_command = command_hash };
    const key = try keyAlloc(alloc, group_id, "meta");
    defer alloc.free(key);
    const bytes = try std.json.Stringify.valueAlloc(alloc, meta, .{});
    defer alloc.free(bytes);
    try txn.put(key, bytes);
}

/// Bootstrap a previously published standalone row journal without renumbering
/// its logical catalog generation. Never overwrite an initialized authority.
pub fn importState(alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, group_id: u64, state: domain.State) !void {
    if ((try readMeta(alloc, txn, group_id)).revision != 0) return error.CatalogGenerationChanged;
    if (state.next_id < 3) return error.InvalidCatalogRecord;
    try writeResource(alloc, txn, group_id, domain.default_database);
    try writeResource(alloc, txn, group_id, domain.default_namespace);
    for (state.resources) |resource| try writeResource(alloc, txn, group_id, resource);
    for (state.settings, 0..) |record, i| {
        try record.validate();
        if (record.identity.id >= state.next_id) return error.InvalidSettingRecord;
        for (state.settings[0..i]) |prior| if (prior.identity.id == record.identity.id or std.ascii.eqlIgnoreCase(prior.name, record.name)) return error.InvalidSettingRecord;
        const setting_key = try settingKeyAlloc(alloc, group_id, record.identity.id);
        defer alloc.free(setting_key);
        const encoded = try std.json.Stringify.valueAlloc(alloc, record, .{});
        defer alloc.free(encoded);
        try txn.put(setting_key, encoded);
    }
    for (state.policies, 0..) |record, i| {
        try record.validateShape();
        if (record.id >= state.next_id) return error.InvalidRowPolicyRecord;
        var bound = false;
        for (state.resources) |resource| if (resource.kind == .table and resource.id == record.table_id) {
            bound = true;
            break;
        };
        if (!bound) return error.InvalidRowPolicyRecord;
        for (state.policies[0..i]) |prior| if (prior.id == record.id or (prior.table_id == record.table_id and std.ascii.eqlIgnoreCase(prior.name, record.name))) return error.InvalidRowPolicyRecord;
        const policy_key = try policyKeyAlloc(alloc, group_id, record.id);
        defer alloc.free(policy_key);
        const encoded = try std.json.Stringify.valueAlloc(alloc, record, .{});
        defer alloc.free(encoded);
        try txn.put(policy_key, encoded);
    }
    for (state.policy_publications, 0..) |publication, index| {
        try publication.validateShape();
        // A catalog-only import cannot make restored owners policy-ready.
        // Coordinated restore must replay/verify owner installations before
        // importing an active or pending publication.
        if (publication.phase != .disabled or policyPublicationNeedsWork(publication)) return error.RowPolicyUnsupported;
        var bound = false;
        for (state.resources) |resource| if (resource.kind == .table and resource.id == publication.table_id) {
            bound = true;
            break;
        };
        if (!bound) return error.InvalidRowPolicyPublication;
        for (state.policy_publications[0..index]) |prior| if (prior.table_id == publication.table_id) return error.InvalidRowPolicyPublication;
        const publication_key = try policyPublicationKeyAlloc(alloc, group_id, publication.table_id);
        defer alloc.free(publication_key);
        const encoded = try std.json.Stringify.valueAlloc(alloc, publication, .{});
        defer alloc.free(encoded);
        try txn.put(publication_key, encoded);
        const stamp_key = try policyPublicationStampKeyAlloc(alloc, group_id, publication.table_id);
        defer alloc.free(stamp_key);
        const stamp_bytes = try std.json.Stringify.valueAlloc(alloc, policies.PublicationStamp.fromPublication(publication), .{});
        defer alloc.free(stamp_bytes);
        try txn.put(stamp_key, stamp_bytes);
    }
    const key = try keyAlloc(alloc, group_id, "meta");
    defer alloc.free(key);
    const bytes = try std.json.Stringify.valueAlloc(alloc, Meta{ .revision = state.revision, .next_id = state.next_id }, .{});
    defer alloc.free(bytes);
    try txn.put(key, bytes);
}
