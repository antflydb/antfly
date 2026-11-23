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

//! Physical PREPARE companion. All reads and writes below use the accepting
//! intent batch while the DB apply lock is held. No row is polled after its
//! decision, and no metadata decision or owner grant is minted here.
const std = @import("std");
const contract = @import("logical_replication_outbox_contract.zig");
const capture = @import("../../system_catalog/logical_replication_capture.zig");
const publisher = @import("../../system_catalog/logical_replication_publisher.zig");
const source = @import("../source_authority.zig");
const schema = @import("../schema.zig");
const rows = @import("algebraic/relational_row_codec.zig");
const internal = @import("../internal_keys.zig");
const identity = @import("doc_identity_namespace.zig");
const positions = @import("receipt_position.zig");
const A = std.mem.Allocator;
pub const guard_key = "\x00\x00__metadata__:logical_capture:v1";
pub const max_capture_bytes: usize = @import("table_catalog.zig").default_transaction_admission_bytes;
const max_guard_bytes: usize = contract.max_metadata_record_bytes;

/// Constructed only by locked DB preparation from its actual checked request,
/// current DB identity and ordered-apply context. It is not a public/wire flag.
pub const Preparation = struct {
    demand: capture.Demand,
    request_digest: [32]u8,
    pregrant_body_digest: [32]u8,
    namespace: identity.Namespace,
    physical_root_incarnation: u128,
    /// Present only for the actual replicated entry being applied. Native
    /// owners allocate their source position in the same accepting batch.
    ordered: ?@FieldType(positions.Position, "raft") = null,
};

pub fn loadGuard(a: A, txn: anytype) !?capture.State {
    const bytes = txn.get(guard_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const value = try decode(capture.State, a, bytes, max_guard_bytes);
    try value.validate();
    return value;
}

/// The native catalog controller installs this state only after its independent
/// publication/Root admission. This helper does not authorize installation.
/// Its ATBL6 fence is durable in the SAME batch, including pending states.
pub fn storeGuard(a: A, txn: anytype, value: capture.State) !void {
    try value.validate();
    const catalog = @import("table_catalog.zig");
    _ = try catalog.stageLogicalCaptureFormat(txn, 1);
    try txn.put(guard_key, try encode(a, value, max_guard_bytes));
}

/// Run before any normalization/provider demand and again in the accepting
/// batch. A pending policy blocks even an otherwise ordinary direct writer.
pub fn requirePrepare(backing: A, txn: anytype, preparation: ?Preparation) !void {
    _ = try checkedBinding(backing, txn, preparation);
}

fn checkedBinding(backing: A, txn: anytype, preparation: ?Preparation) !?capture.Binding {
    // Guard decoding owns a JSON tree. This validation returns no borrowed
    // data, so release it here even when the caller uses a budgeted allocator.
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const guard = try loadGuard(a, txn);
    if (guard == null) {
        if (preparation != null) return error.LogicalCaptureControlChanged;
        return null;
    }
    const effective = try guard.?.effectiveBinding();
    if (effective == null) {
        if (preparation != null) return error.LogicalCaptureControlChanged;
        return null;
    }
    const input = preparation orelse return error.LogicalPublicationTransactionRequired;
    try input.demand.validate(a);
    const binding = effective.?;
    // The immutable guard is not permission to use a changed live catalog.
    // Parent FK controls can change AIC without changing public ASCH; both
    // exact accepting-cut definitions must still match before any row vote.
    const current_schema = try txn.get(schema.schema_key);
    const current_catalog = try txn.get(@import("relational/integrity/catalog.zig").key);
    var current_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(current_schema, &current_digest, .{});
    if (!std.mem.eql(u8, &current_digest, &binding.schema_digest)) return error.LogicalCaptureControlChanged;
    std.crypto.hash.Blake3.hash(current_catalog, &current_digest, .{});
    if (!std.mem.eql(u8, &current_digest, &binding.catalog_digest)) return error.LogicalCaptureControlChanged;
    if (!input.namespace.eql(binding.namespace) or input.physical_root_incarnation == 0 or
        guard.?.physical_root_incarnation != input.physical_root_incarnation or
        !binding.scope.realm.eql(input.demand.reference.realm) or input.demand.owner_group_id != binding.owner_group_id or
        !std.mem.eql(u8, &input.pregrant_body_digest, &input.demand.reference.pregrant_body_digest) or
        !std.mem.eql(u8, &input.request_digest, &input.demand.reference.request_digest) or
        !std.mem.eql(u8, &try binding.digest(a), &input.demand.reference.guard_digest)) return error.LogicalCaptureControlChanged;
    const relation = input.demand.roster.relation;
    if (relation.physical_table_id != binding.physical_table_id or relation.logical_relation_id != binding.logical_relation_id or
        relation.schema_version != binding.schema_version or !std.mem.eql(u8, &relation.schema_digest, &binding.schema_digest)) return error.LogicalCaptureControlChanged;
    return binding;
}

/// The shape is derived inside the physical owner from actual mutations. It is
/// not a public request marker; pure read/predicate participants need no fake
/// row capture. An admitted capture still takes the full guarded path.
pub fn requireUncapturedIntents(a: A, read: anytype, intents: anytype) !void {
    for (intents) |intent| if (!std.mem.startsWith(u8, intent.key, "\x00\x00")) return requirePrepare(a, read, null);
}
pub fn requireOrdinaryRequest(a: A, read: anytype, request: anytype) !void {
    if (request.writes.len != 0 or request.deletes.len != 0 or request.transforms.len != 0 or request.graph_writes.len != 0 or request.graph_deletes.len != 0) try requirePrepare(a, read, null);
}

pub fn readHeader(a: A, txn: anytype, demand: capture.Demand) !?contract.Header {
    const key = try contract.headerKey(a, demand.owner, demand.member);
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const header = try decode(contract.Header, a, bytes, max_guard_bytes);
    try header.validate();
    if (!header.reference.owner.eql(demand.owner) or header.reference.participant != demand.member or
        !std.mem.eql(u8, &header.reference.transaction_id, &demand.transaction_id) or
        !std.mem.eql(u8, &header.demand_digest, &try demand.digest(a))) return error.ExternalTransactionDecisionChanged;
    return header;
}

/// Same-PREPARE retry observes immutable evidence. It cannot replace an old
/// capture, advance its native clock or evaluate a new set of row effects.
pub fn requireRetry(a: A, txn: anytype, input: Preparation, intent_revision: u64) !contract.Header {
    try requirePrepare(a, txn, input);
    const header = (try readHeader(a, txn, input.demand)) orelse return error.LogicalCaptureReceiptMissing;
    if (header.intent_revision != intent_revision or !header.reference.namespace.eql(input.namespace) or
        (header.reference.position == .native and header.reference.physical_root_incarnation != input.physical_root_incarnation)) return error.ExternalTransactionDecisionChanged;
    return header;
}

/// Same-cut fixed header discovery for canonical ABORT reconciliation. The
/// native capacity permission and actual terminal fence are checked separately.
pub fn capacityHeader(a: A, read: anytype, owner: @import("../transaction_decision_owner.zig").External, member: u16) !?contract.Header {
    // Header is an owned POD. Keep point-key and JSON scratch scoped here even
    // when a recovery caller supplies its long-lived general allocator.
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const raw = read.get(try contract.headerKey(scratch, owner, member)) catch |err| return if (err == error.NotFound) null else err;
    const result = try decode(contract.Header, scratch, raw, max_guard_bytes);
    try result.validate();
    if (!result.reference.owner.eql(owner) or result.reference.participant != member) return error.ExternalTransactionDecisionChanged;
    return result;
}

/// Called BEFORE intent rows are installed into this batch. The primary AROW
/// lookup is the actual locked before-image, independent of caller predicates
/// or sanitized JSON. Canonical final intents supply only the after-image.
/// All returned/header slices live in the caller's bounded preparation arena.
fn hashLength(hash: *std.crypto.hash.Blake3, size: usize) void {
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, size, .little);
    hash.update(&encoded);
}
/// Both observation and final accepting transaction call this on the affected
/// key set. Unrelated rows/source clocks never invalidate this OCC witness.
pub fn beforeDigest(a: A, read: anytype, keys: []const []const u8) ![32]u8 {
    const sorted = try a.dupe([]const u8, keys);
    defer a.free(sorted);
    std.mem.sort([]const u8, sorted, {}, struct {
        fn less(_: void, left: []const u8, right: []const u8) bool {
            return std.mem.order(u8, left, right) == .lt;
        }
    }.less);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/logical-capture-before/v1\x00");
    for (sorted, 0..) |key, index| {
        if (std.mem.startsWith(u8, key, "\x00\x00")) return error.InvalidLogicalCaptureControl;
        if (index != 0 and std.mem.eql(u8, sorted[index - 1], key)) continue;
        const physical = try internal.relationalRowKeyAlloc(a, key);
        defer a.free(physical);
        const raw = read.get(physical) catch |err| if (err == error.NotFound) null else return err;
        hashBeforeImage(&hash, key, raw);
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

fn hashBeforeImage(hash: *std.crypto.hash.Blake3, key: []const u8, raw: ?[]const u8) void {
    hashLength(hash, key.len);
    hash.update(key);
    hash.update(&.{@as(u8, @intFromBool(raw != null))});
    if (raw) |value| {
        hashLength(hash, value.len);
        hash.update(value);
    }
}

pub fn stage(a: A, txn: anytype, input: Preparation, transaction_id: [16]u8, intent_revision: u64, intents: anytype) !contract.Header {
    const actual_binding = (try checkedBinding(a, txn, input)) orelse return error.LogicalCaptureControlChanged;
    const capacity = input.demand.capacity orelse return error.LogicalCaptureReceiptPending;
    var affected: std.ArrayList([]const u8) = .empty;
    defer affected.deinit(a);
    for (intents) |intent| if (!std.mem.startsWith(u8, intent.key, "\x00\x00")) try affected.append(a, intent.key);
    const before_digest = try beforeDigest(a, txn, affected.items);
    if (!std.mem.eql(u8, &before_digest, &capacity.before_digest)) return error.PreparedReadSetChanged;
    if (!std.mem.eql(u8, &input.demand.transaction_id, &transaction_id) or intent_revision == 0) return error.ExternalTransactionDecisionChanged;
    if (try readHeader(a, txn, input.demand) != null) return error.ExternalTransactionDecisionChanged;
    var byte_count: usize = 0;
    var count: u64 = 0;
    const roster = try encode(a, input.demand.roster, publisher.max_roster_bytes + 32);
    try charge(&byte_count, roster.len);
    try txn.put(try contract.rosterKey(a, input.demand.owner, input.demand.member), roster);
    var changes_hash = std.crypto.hash.Blake3.init(.{});
    changes_hash.update("antfly/logical-outbox-changes/v1\x00");
    var retained_schemas: std.AutoArrayHashMapUnmanaged(u32, RetainedSchema) = .empty;
    var last: std.StringHashMapUnmanaged(usize) = .empty;
    for (intents, 0..) |intent, index| if (!std.mem.startsWith(u8, intent.key, "\x00\x00")) try last.put(a, intent.key, index);
    // Match the transaction manager's actual last-intent-wins coalescing;
    // iteration remains final-list order, never hash-map order.
    for (intents, 0..) |intent, index| {
        if (std.mem.startsWith(u8, intent.key, "\x00\x00")) continue;
        if (last.get(intent.key).? != index) continue;
        const row_key = try internal.relationalRowKeyAlloc(a, intent.key);
        const before = txn.get(row_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const after = if (intent.value != null) intent.prepared_row orelse return error.RelationalTableRequired else null;
        if (before == null and after == null) continue;
        try requireReplicaIdentity(input.demand.roster, before != null, after != null);
        const before_image = if (before) |raw| try image(a, txn, raw, &retained_schemas) else null;
        const after_image = if (after) |raw| selected: {
            if (try rows.rowSchemaVersion(raw) != input.demand.roster.relation.schema_version) return error.RelationalRowSchemaMismatch;
            const selected = try image(a, txn, raw, &retained_schemas);
            const actual = retained_schemas.get(input.demand.roster.relation.schema_version).?;
            if (!std.mem.eql(u8, &try rows.PhysicalLayout.schemaDigest(a, actual.table), &input.demand.roster.relation.schema_digest)) return error.LogicalCaptureControlChanged;
            break :selected selected;
        } else null;
        count = std.math.add(u64, count, 1) catch return error.TransactionTooLarge;
        const change: contract.Change = .{ .ordinal = count, .key = intent.key, .before = before_image, .after = after_image };
        const encoded = try change.encode(a, max_capture_bytes - byte_count);
        try charge(&byte_count, encoded.len);
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, encoded.len, .little);
        changes_hash.update(&length);
        changes_hash.update(encoded);
        try txn.put(try contract.rowKey(a, input.demand.owner, input.demand.member, count), encoded);
    }
    // Stable order independent of insertion order in the schema cache.
    const versions = try a.dupe(u32, retained_schemas.keys());
    std.mem.sort(u32, versions, {}, std.sort.asc(u32));
    var schemas_hash = std.crypto.hash.Blake3.init(.{});
    schemas_hash.update("antfly/logical-outbox-schemas/v1\x00");
    for (versions) |version| {
        const value = retained_schemas.get(version).?.encoded;
        try charge(&byte_count, value.encoded.len);
        schemas_hash.update(&value.digest);
        try txn.put(try contract.schemaKey(a, input.demand.owner, input.demand.member, value.digest), value.encoded);
    }
    // A genuine no-op still binds its exact schema and owner in a durable
    // header. Its zero-row manifest is not a fabricated mutation/ACK.
    if (versions.len == 0) {
        const value = try retainSchema(a, txn, input.demand.roster.relation.schema_version);
        try charge(&byte_count, value.encoded.len);
        schemas_hash.update(&value.digest);
        try txn.put(try contract.schemaKey(a, input.demand.owner, input.demand.member, value.digest), value.encoded);
    }
    var changes_digest: [32]u8 = undefined;
    changes_hash.final(&changes_digest);
    var schemas_digest: [32]u8 = undefined;
    schemas_hash.final(&schemas_digest);
    if (byte_count > capacity.max_payload_bytes) return error.LogicalPublisherCapacityExceeded;
    const source_namespace = @import("online_source_contract.zig").namespaceBytes(input.namespace);
    const authority = (try source.load(txn)) orelse return error.OnlineSourceScopeChanged;
    if (!std.mem.eql(u8, &authority.namespace, &source_namespace)) return error.OnlineSourceScopeChanged;
    const position: positions.Position = switch (authority.kind) {
        .native => blk: {
            if (input.ordered != null) return error.OnlineSourceScopeChanged;
            break :blk .{ .native = .{ .namespace = input.namespace, .sequence = try source.advance(txn, source_namespace, null) } };
        },
        .raft => .{ .raft = input.ordered orelse return error.OrderedApplyRequired },
    };
    try position.validate();
    const header: contract.Header = .{
        .reference = .{ .owner = input.demand.owner, .transaction_id = transaction_id, .participant = input.demand.member, .group_id = actual_binding.owner_group_id, .namespace = input.namespace, .physical_root_incarnation = input.physical_root_incarnation, .position = position, .roster_digest = input.demand.reference.roster_digest, .guard_digest = input.demand.reference.guard_digest, .pregrant_body_digest = input.demand.reference.pregrant_body_digest, .changes_digest = changes_digest, .capacity_digest = capacity.bound_digest, .before_digest = before_digest, .change_count = count, .encoded_bytes = byte_count },
        .intent_revision = intent_revision,
        .demand_digest = try input.demand.digest(a),
        .rows_digest = changes_digest,
        .roster_bytes = @intCast(roster.len),
        .schema_count = @intCast(@max(versions.len, 1)),
        .schemas_digest = schemas_digest,
    };
    try header.validate();
    try txn.put(try contract.headerKey(a, input.demand.owner, input.demand.member), try encode(a, header, max_guard_bytes));
    const locator: contract.Transaction = .{ .reference = header.reference, .intent_revision = intent_revision };
    try locator.validate();
    const locator_key = try contract.transactionKey(a, transaction_id);
    if (txn.get(locator_key)) |old| {
        const prior = try decode(contract.Transaction, a, old, max_guard_bytes);
        if (!prior.reference.sameLogical(locator.reference) or prior.intent_revision != locator.intent_revision) return error.ExternalTransactionDecisionChanged;
    } else |err| if (err != error.NotFound) return err;
    try txn.put(locator_key, try encode(a, locator, max_guard_bytes));
    return header;
}

/// Exact point join used by phase2/activation after all API-local state died.
/// No current roster refresh, name lookup or participant scan is authority.
pub fn forTransaction(a: A, read: anytype, transaction_id: [16]u8, owner: @import("../transaction_decision_owner.zig").External, namespace: @import("doc_identity_namespace.zig").Namespace) !?contract.Header {
    // Both the locator and returned Header contain only fixed owned values.
    // No key or parser arena escapes this synchronous same-image join.
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const raw = read.get(try contract.transactionKey(scratch, transaction_id)) catch |err| return if (err == error.NotFound) null else err;
    const locator = try decode(contract.Transaction, scratch, raw, max_guard_bytes);
    try locator.validate();
    if (!std.mem.eql(u8, &locator.reference.transaction_id, &transaction_id) or !locator.reference.owner.eql(owner) or !locator.reference.namespace.eql(namespace)) return error.ExternalTransactionDecisionChanged;
    const header = (try capacityHeader(scratch, read, owner, locator.reference.participant)) orelse return error.InvalidLogicalOutbox;
    if (!header.reference.sameLogical(locator.reference) or header.intent_revision != locator.intent_revision) return error.InvalidLogicalOutbox;
    return header;
}

/// Size the frozen postimages and affected before-images on one pinned owner cut.
pub fn observeCapacity(a: A, read: anytype, request: @import("../../system_catalog/logical_replication_capacity.zig").Request, namespace: identity.Namespace, table: schema.TableSchema, root: u128, context: @import("../../api/operation.zig").RequestContext) !@import("../../system_catalog/logical_replication_capacity.zig").Observation {
    try context.ensureActive();
    const demand = request.demand;
    const intent = request.intent;
    const owner = intent.external_decision_owner orelse return error.ExternalTransactionDecisionRequired;
    if (!owner.eql(demand.owner) or demand.capacity != null or intent.publication_capture != null or
        intent.transforms.len != 0 or intent.external_activation or intent.commit_requires != null or
        intent.prepared_enrichments_json != null) return error.InvalidLogicalCaptureControl;
    const hashes = try @import("logical_replication_request.zig").digests(a, .{
        .namespace = namespace,
        .owner_group_id = demand.owner_group_id,
        .transaction_id = demand.transaction_id,
        .member = demand.member,
        .topology_epoch = demand.topology_epoch,
    }, intent);
    try requirePrepare(a, read, .{
        .demand = demand,
        .request_digest = hashes.request,
        .pregrant_body_digest = hashes.body,
        .namespace = namespace,
        .physical_root_incarnation = root,
    });
    var layout = try rows.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var selected: std.StringArrayHashMapUnmanaged(?usize) = .empty;
    defer selected.deinit(a);
    for (intent.writes, 0..) |write, index| try selected.put(a, write.key, index);
    for (intent.deletes) |key| try selected.put(a, key, null);
    var used: usize = 0;
    const roster = try encode(a, demand.roster, publisher.max_roster_bytes + 32);
    defer a.free(roster);
    try charge(&used, roster.len);
    var versions: std.AutoHashMapUnmanaged(u32, void) = .empty;
    defer versions.deinit(a);
    const sorted = try a.dupe([]const u8, selected.keys());
    defer a.free(sorted);
    std.mem.sort([]const u8, sorted, {}, struct {
        fn less(_: void, left: []const u8, right: []const u8) bool {
            return std.mem.order(u8, left, right) == .lt;
        }
    }.less);
    var before_hash = std.crypto.hash.Blake3.init(.{});
    before_hash.update("antfly/logical-capture-before/v1\x00");
    for (sorted) |key| {
        try context.ensureActive();
        const after = selected.get(key).?;
        if (std.mem.startsWith(u8, key, "\x00\x00")) return error.InvalidLogicalCaptureControl;
        const physical = try internal.relationalRowKeyAlloc(a, key);
        defer a.free(physical);
        const before = read.get(physical) catch |err| if (err == error.NotFound) null else return err;
        hashBeforeImage(&before_hash, key, before);
        if (before == null and after == null) continue;
        try charge(&used, 56 + key.len);
        if (before) |raw| {
            try charge(&used, 32 + raw.len);
            try versions.put(a, try rows.rowSchemaVersion(raw), {});
        }
        if (after) |index| {
            const write = intent.writes[index];
            var parsed = try std.json.parseFromSlice(std.json.Value, a, write.value, .{ .parse_numbers = false });
            defer parsed.deinit();
            try charge(&used, 32 + try @import("document_mapper.zig").frozenRelationalRowSize(a, parsed.value, table, &layout, write.json_null_fields));
            try versions.put(a, table.version, {});
        }
    }
    if (versions.count() == 0) try versions.put(a, table.version, {});
    var schema_versions = versions.keyIterator();
    while (schema_versions.next()) |version| {
        const key = try schema.schemaVersionKeyAlloc(a, version.*);
        defer a.free(key);
        try charge(&used, (try read.get(key)).len);
    }
    const authority = (try source.load(read)) orelse return error.OnlineSourceScopeChanged;
    if (!std.mem.eql(u8, &authority.namespace, &@import("online_source_contract.zig").namespaceBytes(namespace))) return error.OnlineSourceScopeChanged;
    const position: positions.Position = switch (authority.kind) {
        .native => if (authority.sequence == 0) return error.ReadUnavailable else .{ .native = .{ .namespace = namespace, .sequence = authority.sequence } },
        .raft => if (try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(read)) |cut| .{ .raft = .{ .term = cut.term, .index = cut.index } } else return error.ReadUnavailable,
    };
    var before_digest: [32]u8 = undefined;
    before_hash.final(&before_digest);
    const result: @import("../../system_catalog/logical_replication_capacity.zig").Observation = .{
        .owner = owner,
        .transaction_id = demand.transaction_id,
        .member = demand.member,
        .owner_group_id = demand.owner_group_id,
        .topology_epoch = demand.topology_epoch,
        .namespace = namespace,
        .physical_root_incarnation = root,
        .position = position,
        .reference = demand.reference,
        .demand_digest = try demand.preCapacityDigest(a),
        .before_digest = before_digest,
        .max_payload_bytes = used,
    };
    try result.require(a, demand);
    try context.ensureActive();
    return result;
}

pub const OrderedTerminal = publisher.OrderedTerminal;

pub fn terminalKey(a: A, transaction_id: [16]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}terminal:{x}", .{ contract.family, transaction_id });
}

pub fn readOrderedTerminal(a: A, read: anytype, transaction_id: [16]u8) !?OrderedTerminal {
    const key = try terminalKey(a, transaction_id);
    defer a.free(key);
    const raw = read.get(key) catch |err| return if (err == error.NotFound) null else err;
    if (raw.len > max_guard_bytes) return error.InvalidLogicalOutbox;
    var parsed = try std.json.parseFromSlice(OrderedTerminal, a, raw, .{});
    defer parsed.deinit();
    try parsed.value.validate();
    if (!std.mem.eql(u8, &parsed.value.capture_ref.transaction_id, &transaction_id)) return error.InvalidLogicalOutbox;
    return parsed.value;
}

pub const OrderedResolution = struct {
    alloc: A,
    transaction_id: [16]u8,
    owner: @import("../transaction_decision_owner.zig").External,
    intent_revision: u64,
    position: @FieldType(positions.Position, "raft"),
    status: @FieldType(OrderedTerminal, "status"),
    commit_version: u64,

    pub fn sink(self: *@This()) @import("../publication_prepare_sink.zig").TerminalSink {
        return .{ .ptr = self, .stage = stageTerminal };
    }

    fn stageTerminal(raw: *anyopaque, view: @import("../commit_participant.zig").View) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const a = self.alloc;
        const locator_key = try contract.transactionKey(a, self.transaction_id);
        const raw_locator = view.get(locator_key) catch |err| return if (err == error.NotFound) error.LogicalCaptureReceiptMissing else err;
        const locator = try decode(contract.Transaction, a, raw_locator, max_guard_bytes);
        try locator.validate();
        const header = (try forTransaction(a, view, self.transaction_id, self.owner, locator.reference.namespace)) orelse return error.LogicalCaptureReceiptMissing;
        if (header.intent_revision != self.intent_revision) return error.LogicalCaptureReceiptMissing;
        const record: OrderedTerminal = .{
            .capture_ref = header.reference,
            .position = self.position,
            .status = self.status,
            .commit_version = self.commit_version,
        };
        try record.validate();
        if (try readOrderedTerminal(a, view, self.transaction_id) != null) return error.ExternalTransactionDecisionChanged;
        try view.put(try terminalKey(a, self.transaction_id), try std.json.Stringify.valueAlloc(a, record, .{}));
    }
};

/// Read immutable capture pages under ONE actual image lease. The caller first
/// verifies native P/journal/slot authority; a presented reference alone grants
/// no feed access. All returned bytes are copied into a's lifetime so no store
/// cursor/borrow survives the physical owner call.
pub fn readPage(a: A, read: anytype, request: contract.PageRequest) !contract.Page {
    try request.reference.validate();
    if (request.limit == 0 or request.limit > publisher.max_feed_page) return error.InvalidLogicalOutbox;
    try contract.validatePageWireBudget(request.max_bytes);
    const reference = request.reference;
    const raw_header = try read.get(try contract.headerKey(a, reference.owner, reference.participant));
    // Persisted header/roster include their checksums. 512 additionally
    // covers LCP1 header/checksum and JSON object wrapper; payload framing is
    // charged per selected item below. The actual encoder checks exact size.
    var used: usize = 0;
    try chargePage(&used, 512, request.max_bytes);
    try chargePage(&used, raw_header.len, request.max_bytes);
    const header = try decode(contract.Header, a, raw_header, max_guard_bytes);
    try header.validate();
    if (!header.reference.sameLogical(reference) or request.after > reference.change_count) return error.ExternalTransactionDecisionChanged;
    const raw_roster = try read.get(try contract.rosterKey(a, reference.owner, reference.participant));
    if (raw_roster.len != header.roster_bytes) return error.InvalidLogicalOutbox;
    try chargePage(&used, raw_roster.len, request.max_bytes);
    const roster = try decode(publisher.PublishedRelation, a, raw_roster, publisher.max_roster_bytes + 32);
    var roster_budget: publisher.RosterBudget = .{};
    try roster_budget.add(roster);
    const actual_roster = try capture.canonicalDigest(a, "antfly/logical-capture-roster/v1\x00", roster);
    if (!std.mem.eql(u8, &actual_roster, &reference.roster_digest)) return error.InvalidLogicalOutbox;
    var changes: std.ArrayList(contract.Change) = .empty;
    var schemas: std.ArrayList(contract.Schema) = .empty;
    var known_schemas: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    var ordinal = request.after;
    while (ordinal < reference.change_count and changes.items.len < request.limit) {
        const next = try std.math.add(u64, ordinal, 1);
        const raw = try read.get(try contract.rowKey(a, reference.owner, reference.participant, next));
        const row_wire_bytes = try std.math.add(usize, raw.len, 4);
        if (row_wire_bytes > request.max_bytes - used) {
            if (changes.items.len == 0) return error.ResourceRequestTooLarge;
            break;
        }
        const change = try contract.Change.decode(raw, max_capture_bytes);
        if (change.ordinal != next) return error.InvalidLogicalOutbox;
        // At most two immutable schema point reads per row, deduplicated within
        // the page. No catalog scan or recompilation from current type aliases.
        var extra_schemas: [2]?contract.Schema = @splat(null);
        var extra_bytes: usize = row_wire_bytes;
        for ([_]?contract.Image{ change.before, change.after }, 0..) |maybe, index| if (maybe) |value| {
            if (known_schemas.contains(value.schema_digest)) continue;
            if (index == 1 and extra_schemas[0] != null and std.mem.eql(u8, &extra_schemas[0].?.digest, &value.schema_digest)) continue;
            const encoded = try read.get(try contract.schemaKey(a, reference.owner, reference.participant, value.schema_digest));
            const schema_wire_bytes = try std.math.add(usize, encoded.len, 36);
            if (schema_wire_bytes > request.max_bytes - used - extra_bytes) {
                if (changes.items.len == 0) return error.ResourceRequestTooLarge;
                return finishPage(a, header, roster, changes, schemas, ordinal, true);
            }
            const retained: contract.Schema = .{ .digest = value.schema_digest, .encoded = encoded };
            try retained.validate();
            extra_bytes += schema_wire_bytes;
            extra_schemas[index] = retained;
        };
        try chargePage(&used, extra_bytes, request.max_bytes);
        for (extra_schemas) |maybe| if (maybe) |value| {
            try known_schemas.put(a, value.digest, {});
            try schemas.append(a, .{ .digest = value.digest, .encoded = try a.dupe(u8, value.encoded) });
        };
        try changes.append(a, try contract.Change.decode(try a.dupe(u8, raw), max_capture_bytes));
        ordinal = next;
    }
    const page = try finishPage(a, header, roster, changes, schemas, ordinal, ordinal < reference.change_count);
    try request.require(page);
    return page;
}
fn finishPage(a: A, header: contract.Header, roster: publisher.PublishedRelation, changes: std.ArrayList(contract.Change), schemas: std.ArrayList(contract.Schema), ordinal: u64, more: bool) !contract.Page {
    var owned_changes = changes;
    var owned_schemas = schemas;
    return .{ .header = header, .roster = roster, .changes = try owned_changes.toOwnedSlice(a), .schemas = try owned_schemas.toOwnedSlice(a), .next_after = if (more) ordinal else null };
}
fn chargePage(used: *usize, bytes: usize, maximum: usize) !void {
    if (used.* > maximum or bytes > maximum - used.*) return error.ResourceRequestTooLarge;
    used.* += bytes;
}

const RetainedSchema = struct { encoded: contract.Schema, table: schema.TableSchema, layout: rows.PhysicalLayout };
fn image(a: A, txn: anytype, raw: []const u8, cache: *std.AutoArrayHashMapUnmanaged(u32, RetainedSchema)) !contract.Image {
    const version = try rows.rowSchemaVersion(raw);
    const entry = try cache.getOrPut(a, version);
    if (!entry.found_existing) {
        const encoded = try retainSchema(a, txn, version);
        const table = try schema.deserializeSchema(a, encoded.encoded);
        entry.value_ptr.* = .{ .encoded = encoded, .table = table, .layout = try rows.PhysicalLayout.init(a, table) };
    }
    const value = entry.value_ptr;
    try rows.validateOrdinalWithLayout(raw, value.table, &value.layout);
    return .{ .schema_digest = value.encoded.digest, .row = raw };
}
fn retainSchema(a: A, txn: anytype, version: u32) !contract.Schema {
    const bytes = try a.dupe(u8, try txn.get(try schema.schemaVersionKeyAlloc(a, version)));
    const table = try schema.deserializeSchema(a, bytes);
    if (table.version != version or table.storage_mode != .relational) return error.RelationalRowSchemaMismatch;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/logical-outbox-schema/v1\x00");
    hash.update(bytes);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return .{ .digest = digest, .encoded = bytes };
}
fn charge(used: *usize, bytes: usize) !void {
    if (bytes > max_capture_bytes - used.*) return error.TransactionTooLarge;
    used.* += bytes;
}
fn encode(a: A, value: anytype, maximum: usize) ![]u8 {
    const json = try std.json.Stringify.valueAlloc(a, value, .{});
    if (json.len > maximum - 32) return error.TransactionTooLarge;
    const bytes = try a.alloc(u8, json.len + 32);
    @memcpy(bytes[0..json.len], json);
    std.crypto.hash.Blake3.hash(json, bytes[json.len..][0..32], .{});
    return bytes;
}
fn decode(comptime T: type, a: A, bytes: []const u8, maximum: usize) !T {
    if (bytes.len < 32 or bytes.len > maximum) return error.InvalidLogicalOutbox;
    const body = bytes[0 .. bytes.len - 32];
    var actual: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(body, &actual, .{});
    if (!std.mem.eql(u8, &actual, bytes[body.len..])) return error.InvalidLogicalOutbox;
    return (try std.json.parseFromSlice(T, a, body, .{ .allocate = .alloc_always })).value;
}

/// Match publisher-side SQL semantics before accepting PREPARED, rather than
/// discovering an unrepresentable UPDATE/DELETE after irreversible COMMIT.
/// INSERT-only publications do not require an old-row identity.
fn requireReplicaIdentity(roster: publisher.PublishedRelation, before: bool, after: bool) !void {
    if (!before) return;
    var published = false;
    for (roster.memberships) |member| {
        const action = if (after) member.parameters.publish.update else member.parameters.publish.delete;
        published = published or action;
        if (action and roster.relation.replica_identity != .full and member.parameters.publish_generated_columns != .stored)
            for (roster.relation.columns) |column| if (column.key and column.generated) return error.SqlReplicationReplicaIdentityRequired;
    }
    if (!published or roster.relation.replica_identity == .full) return;
    if (roster.relation.replica_identity == .nothing) return error.SqlReplicationReplicaIdentityRequired;
    for (roster.relation.columns) |column| if (column.key) return;
    return error.SqlReplicationReplicaIdentityRequired;
}

test "logical outbox replica identity admission refuses published updates and deletes without restricting inserts or disabled actions" {
    var membership = [_]publisher.Membership{.{ .publication = .{ .id = 1, .generation = 1, .digest = @splat(1) }, .parameters = .{} }};
    var columns = [_]publisher.Column{.{ .attnum = 1, .name = "id", .type_oid = 23 }};
    var roster: publisher.PublishedRelation = .{ .relation = .{ .logical_relation_id = 40, .physical_table_id = 50, .database_id = 2, .namespace_id = 3, .relation_oid = 40, .namespace = "public", .name = "published", .schema_version = 1, .schema_digest = @splat(5), .replica_identity = .default_key, .columns = &columns }, .memberships = &membership };
    try requireReplicaIdentity(roster, false, true);
    try std.testing.expectError(error.SqlReplicationReplicaIdentityRequired, requireReplicaIdentity(roster, true, true));
    try std.testing.expectError(error.SqlReplicationReplicaIdentityRequired, requireReplicaIdentity(roster, true, false));
    columns[0].key = true;
    try requireReplicaIdentity(roster, true, true);
    try requireReplicaIdentity(roster, true, false);
    columns[0].generated = true;
    try std.testing.expectError(error.SqlReplicationReplicaIdentityRequired, requireReplicaIdentity(roster, true, true));
    membership[0].parameters.publish_generated_columns = .stored;
    try requireReplicaIdentity(roster, true, true);
    columns[0].generated = false;
    roster.relation.replica_identity = .nothing;
    try std.testing.expectError(error.SqlReplicationReplicaIdentityRequired, requireReplicaIdentity(roster, true, true));
    membership[0].parameters.publish.update = false;
    membership[0].parameters.publish.delete = false;
    try requireReplicaIdentity(roster, true, true);
    try requireReplicaIdentity(roster, true, false);
    membership[0].parameters.publish.delete = true;
    roster.relation.replica_identity = .full;
    try requireReplicaIdentity(roster, true, false);
}

test "logical outbox canceled ALTER accepts original retained guard and refuses canceled new demand" {
    const a0 = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a0);
    defer arena.deinit();
    const a = arena.allocator();
    var backend = @import("../mem_backend.zig").Backend.init(a0, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a0, .{ .name = "cancel-restores-publication" });
    defer store.deinit();
    var txn = try store.beginWrite();
    var committed = false;
    defer if (!committed) txn.abort();
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{.{ .name = "id", .path = "id", .column_type = .integer }} };
    const encoded = try schema.serializeSchema(a, table);
    var schema_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(encoded, &schema_digest, .{});
    try txn.put(schema.schema_key, encoded);
    const table_catalog = @import("table_catalog.zig");
    const table_bytes = try (table_catalog.Catalog{ .mode_initialized = true, .storage_mode = .relational, .active_schema_version = 1 }).encodeDurable();
    try txn.put(table_catalog.key, table_bytes.slice());
    const catalogs = @import("relational/integrity/catalog.zig");
    var catalog = try catalogs.prepare(a0, null, try catalogs.incarnationFromTableId(50), 1, schema_digest, &.{});
    defer catalog.deinit();
    try catalog.stage(&txn);
    var catalog_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(catalog.value, &catalog_digest, .{});
    const original: capture.Binding = .{ .scope = .{ .realm = .{ .group_id = 1, .incarnation = @splat('1'), .database_id = 2 }, .subject = .{ .publication = .{ .id = 10, .generation = 1 } }, .operation_id = @splat(1), .change_digest = @splat(2) }, .logical_relation_id = 40, .physical_table_id = 50, .owner_group_id = 9, .namespace = .{ .table_id = 50, .shard_id = 1, .range_id = 2 }, .schema_version = 1, .schema_digest = schema_digest, .catalog_digest = catalog_digest, .catalog_revision = 5, .previous = .{}, .next = .{ .table = .{ .count = 1, .digest = @splat(3) } } };
    var changed = original;
    changed.scope.operation_id = @splat(4);
    changed.scope.subject.publication.generation += 1;
    changed.catalog_revision += 1;
    changed.previous = original.next;
    changed.next.table = .{ .count = 2, .digest = @splat(5) };
    try storeGuard(a, &txn, .{ .binding = changed, .previous_binding = original, .phase = .canceled, .position = .{ .native = .{ .namespace = original.namespace, .sequence = 3 } }, .physical_root_incarnation = 9 });
    const roster: publisher.PublishedRelation = .{ .relation = .{ .logical_relation_id = 40, .physical_table_id = 50, .database_id = 2, .namespace_id = 3, .relation_oid = 40, .namespace = "public", .name = "published", .schema_version = 1, .schema_digest = schema_digest, .replica_identity = .default_key, .columns = &.{.{ .attnum = 1, .name = "id", .type_oid = 20, .key = true }} }, .memberships = &.{.{ .publication = .{ .id = 10, .generation = 1, .digest = @splat(6) }, .parameters = .{} }} };
    const input: Preparation = .{ .demand = .{ .owner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(7), .admission_digest = @splat(8), .expires_at_unix_ns = 1000 }, .transaction_id = @splat(9), .member = 0, .owner_group_id = 9, .topology_epoch = 1, .reference = .{ .realm = original.scope.realm, .guard_digest = try original.digest(a), .roster_digest = try capture.canonicalDigest(a, "antfly/logical-capture-roster/v1\x00", roster), .pregrant_body_digest = @splat(10), .request_digest = @splat(11) }, .roster = roster }, .request_digest = @splat(11), .pregrant_body_digest = @splat(10), .namespace = original.namespace, .physical_root_incarnation = 9 };
    try requirePrepare(a, &txn, input);
    try std.testing.expectError(error.LogicalPublicationTransactionRequired, requirePrepare(a, &txn, null));
    var wrong = input;
    wrong.demand.reference.guard_digest = try changed.digest(a);
    try std.testing.expectError(error.LogicalCaptureControlChanged, requirePrepare(a, &txn, wrong));
    var pending = (try loadGuard(a, &txn)).?;
    pending.phase = .preparing;
    try storeGuard(a, &txn, pending);
    try std.testing.expectError(error.LogicalCapturePublicationPending, requirePrepare(a, &txn, input));

    pending.phase = .canceled;
    try storeGuard(a, &txn, pending);
    try txn.put(try schema.schemaVersionKeyAlloc(a, 1), encoded);
    const namespace = @import("online_source_contract.zig").namespaceBytes(original.namespace);
    try source.bind(&txn, .native, namespace);
    const row = try rows.serializeOrdinal(a, 1, table.relational_columns, &.{.{
        .ordinal = 0,
        .path = "id",
        .value_type = .i64_val,
        .value = .{ .i64_val = 42 },
    }}, @splat(1));
    var granted = input;
    granted.demand.capacity = .{
        .bound_digest = @splat(12),
        .before_digest = try beforeDigest(a, &txn, &.{"row-42"}),
        .max_payload_bytes = 64 * 1024,
    };
    const intents: []const @import("../transactions.zig").WriteIntent = &.{.{
        .key = "row-42",
        .value = "{\"id\":42}",
        .prepared_row = row,
    }};
    const header = try stage(a, &txn, granted, granted.demand.transaction_id, 1, intents);
    try std.testing.expectEqual(@as(u64, 1), header.reference.change_count);
    try std.testing.expectEqual(@as(u64, 1), header.reference.position.native.sequence);
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, stage(a, &txn, granted, granted.demand.transaction_id, 1, intents));
    try txn.commit();
    committed = true;

    var reopened = try store.beginRead();
    var reading = true;
    defer if (reading) reopened.abort();
    const retained = (try forTransaction(a, &reopened, granted.demand.transaction_id, granted.demand.owner, original.namespace)).?;
    try std.testing.expect(retained.reference.sameLogical(header.reference));
    const page = try readPage(a, &reopened, .{ .reference = retained.reference, .max_bytes = 64 * 1024 });
    try std.testing.expectEqual(@as(usize, 1), page.changes.len);
    try std.testing.expect(page.changes[0].before == null and page.next_after == null);
    try std.testing.expectEqualSlices(u8, row, page.changes[0].after.?.row);
    try std.testing.expectEqual(@as(u8, 1), (try table_catalog.Catalog.decode(try reopened.get(table_catalog.key))).logical_capture_format);
    reopened.abort();
    reading = false;

    const owner_control = @import("logical_replication_owner_control.zig");
    var replacement = changed;
    replacement.scope.operation_id = @splat(13);
    replacement.scope.subject.publication.generation += 1;
    replacement.catalog_revision += 1;
    var control_txn = try store.beginWrite();
    var control_committed = false;
    defer if (!control_committed) control_txn.abort();
    const prepare: capture.Control = .{ .binding = replacement, .action = .prepare };
    const activate: capture.Control = .{ .binding = replacement, .action = .activate };
    try std.testing.expectError(error.LogicalCapturePublicationPending, owner_control.stage(a, original.namespace, &control_txn, activate, 9, .{ .native = null }, .{}));
    const prepared = try owner_control.stage(a, original.namespace, &control_txn, prepare, 9, .{ .native = null }, .{});
    try std.testing.expectEqual(capture.Action.prepare, prepared.receipt.?.action);
    try std.testing.expectEqual(@as(u64, 2), prepared.position.native.sequence);
    const duplicate = try owner_control.stage(a, original.namespace, &control_txn, prepare, 9, .{ .native = null }, .{});
    try std.testing.expect(duplicate.duplicate);
    try std.testing.expectEqualDeep(prepared.position, duplicate.position);
    try std.testing.expectError(error.LogicalCapturePublicationPending, requirePrepare(a, &control_txn, null));
    const active = try owner_control.stage(a, original.namespace, &control_txn, activate, 9, .{ .native = null }, .{});
    try std.testing.expectEqual(capture.Action.activate, active.receipt.?.action);
    try std.testing.expectEqual(@as(u64, 3), active.position.native.sequence);
    try std.testing.expectError(error.LogicalCaptureControlChanged, owner_control.stage(a, original.namespace, &control_txn, prepare, 9, .{ .native = null }, .{}));
    try control_txn.commit();
    control_committed = true;
}

/// Collection uses the actual retained header, never a current row/schema
/// reconstruction. Its caller independently verifies native GC permission.
pub fn collectionHeader(a: A, read: anytype, reference: publisher.CaptureRef) !contract.Header {
    const raw = try read.get(try contract.headerKey(a, reference.owner, reference.participant));
    const value = try decode(contract.Header, a, raw, max_guard_bytes);
    try value.validate();
    if (!value.reference.sameLogical(reference) or !std.mem.eql(u8, &value.rows_digest, &reference.changes_digest)) return error.InvalidLogicalOutbox;
    return value;
}
pub fn collectionRoster(a: A, raw: []const u8, reference: publisher.CaptureRef) !publisher.PublishedRelation {
    const value = try decode(publisher.PublishedRelation, a, raw, publisher.max_roster_bytes + 32);
    try value.validate();
    if (!std.mem.eql(u8, &try capture.canonicalDigest(a, "antfly/logical-capture-roster/v1\x00", value), &reference.roster_digest)) return error.InvalidLogicalOutbox;
    return value;
}
