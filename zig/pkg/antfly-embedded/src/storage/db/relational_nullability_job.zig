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

//! Durable, fenced read-side validation jobs. These transaction helpers never
//! commit, authenticate metadata, sign receipts or publish a schema. Callers
//! must abort on any error and join effects with their apply/outbox transaction.
//! Public/generic topology admission remains guarded until the complete owner,
//! metadata, standby and installation protocol is integrated.
const std = @import("std");
const A = std.mem.Allocator;
const schema = @import("../schema.zig");
const api = @import("../../schema/mod.zig");
const compiler = @import("../../schema/relational_declarations.zig");
const logical = @import("../../schema/column_nullability.zig");
const projection = @import("relational_nullability.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const topology = @import("relational_integrity_topology.zig");
const positions = @import("receipt_position.zig");
const authority = @import("../source_authority.zig");
const integrity = @import("relational_integrity_catalog.zig");
const activation = @import("relational_integrity_activation.zig");
const physical = @import("relational_index_catalog.zig");
const rows = @import("relational_rows.zig");
const transactions = @import("../transactions.zig");
const receipts = @import("apply_receipts.zig");
const Digest = [32]u8;
pub const key = "\x00\x00__metadata__:schema_validation_job:v1";
pub const inputs_key = key ++ ":inputs";
pub const cursor_key = key ++ ":cursor";
const public_key = "\x00\x00__metadata__:schema_json";
const max_schema = 2 * 1024 * 1024;
const max_names = 1024 * 1024;
const max_cursor = @import("relational_index_limits.zig").max_cursor_key_bytes;
const max_header = 8192;

pub fn hash(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}
fn eq(a: Digest, b: Digest) bool {
    return std.mem.eql(u8, &a, &b);
}
fn optional(txn: anytype, name: []const u8) !?[]const u8 {
    return txn.get(name) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}
fn digestAt(txn: anytype, name: []const u8) !Digest {
    return hash((try optional(txn, name)) orelse "");
}

/// Owned immutable preparation. Compilation is outside serialized apply;
/// admission checks exact bytes/hashes, not a caller's asserted column types.
pub const Candidate = struct {
    alloc: A,
    inputs: []u8,
    native_before: []u8,
    native_after: []u8,
    catalog_after: []u8,
    plan: projection.Plan,
    before_version: u32,
    after_version: u32,

    pub fn deinit(self: *Candidate) void {
        self.alloc.free(self.inputs);
        self.alloc.free(self.native_before);
        self.alloc.free(self.native_after);
        self.alloc.free(self.catalog_after);
        self.plan.deinit();
        self.* = undefined;
    }
    pub fn init(a: A, table_id: u64, before_json: []const u8, after_json: []const u8, predecessor: []const u8, names: []const []const u8, control: projection.Control) !Candidate {
        try control.ensureActive();
        if (table_id == 0 or before_json.len == 0 or before_json.len > max_schema or after_json.len == 0 or after_json.len > max_schema or predecessor.len == 0 or predecessor.len > integrity.max_catalog_bytes)
            return error.InvalidSchemaValidationJob;
        var before = try api.PreparedSchemaDefinition.init(a, before_json);
        defer before.deinit(a);
        var after = try api.PreparedSchemaDefinition.init(a, after_json);
        defer after.deinit(a);
        if (before.runtime_schema.storage_mode != .relational or after.runtime_schema.storage_mode != .relational or
            after.runtime_schema.version != (std.math.add(u32, before.runtime_schema.version, 1) catch return error.InvalidSchemaValidationJob))
            return error.InvalidSchemaValidationJob;
        var layout = try codec.PhysicalLayout.init(a, before.runtime_schema);
        defer layout.deinit();
        var plan = try projection.Plan.init(a, before.runtime_schema, &layout, names, control);
        errdefer plan.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const changes = try scratch.alloc(logical.Change, names.len);
        for (names, changes) |name, *change| change.* = .{ .column = name, .nullable = false };
        const old = try std.json.parseFromSliceLeaky(std.json.Value, scratch, before_json, .{ .parse_numbers = false });
        const next = try std.json.parseFromSliceLeaky(std.json.Value, scratch, after_json, .{ .parse_numbers = false });
        const summary = try logical.verifyTransition(scratch, old, next, changes, .{ .checkpoint = control.checkpoint, .checkpoint_context = control.context });
        if (summary.changed != names.len or summary.tightened != names.len) return error.InvalidSchemaValidationJob;
        var old_catalog = try compiler.compileCatalog(a, before.public_schema, table_id, predecessor);
        defer old_catalog.deinit();
        if (!std.mem.eql(u8, old_catalog.value, predecessor)) return error.IntegrityCatalogChanged;
        var next_catalog = try compiler.compileCatalog(a, after.public_schema, table_id, predecessor);
        defer next_catalog.deinit();
        // NOT NULL does not authorize replacing existing claims/references or
        // silently promoting independently incomplete constraint coverage.
        if (!eq(activation.generationSet(old_catalog.catalog), activation.generationSet(next_catalog.catalog)))
            return error.InvalidSchemaValidationJob;
        const native_before = try schema.serializeSchema(a, before.runtime_schema);
        errdefer a.free(native_before);
        const native_after = try schema.serializeSchema(a, after.runtime_schema);
        errdefer a.free(native_after);
        const catalog_after = try a.dupe(u8, next_catalog.value);
        errdefer a.free(catalog_after);
        const name_json = try std.json.Stringify.valueAlloc(a, plan.names, .{});
        defer a.free(name_json);
        if (name_json.len > max_names) return error.InvalidSchemaValidationJob;
        const components = [_][]const u8{ before_json, after_json, predecessor, name_json };
        var length: usize = 24 + 32;
        for (components) |part| length += part.len;
        const inputs = try a.alloc(u8, length);
        errdefer a.free(inputs);
        @memcpy(inputs[0..8], "ASVI\x01\x00\x00\x00");
        var offset: usize = 24;
        for (components, 0..) |part, i| {
            std.mem.writeInt(u32, inputs[8 + 4 * i ..][0..4], @intCast(part.len), .little);
            @memcpy(inputs[offset..][0..part.len], part);
            offset += part.len;
        }
        @memcpy(inputs[offset..][0..32], &hash(inputs[0..offset]));
        try control.ensureActive();
        return .{ .alloc = a, .inputs = inputs, .native_before = native_before, .native_after = native_after, .catalog_after = catalog_after, .plan = plan, .before_version = before.runtime_schema.version, .after_version = after.runtime_schema.version };
    }
    pub fn parts(bytes: []const u8) ![4][]const u8 {
        if (bytes.len < 56 or bytes.len > 56 + 2 * max_schema + integrity.max_catalog_bytes + max_names or !std.mem.eql(u8, bytes[0..8], "ASVI\x01\x00\x00\x00") or !eq(hash(bytes[0 .. bytes.len - 32]), bytes[bytes.len - 32 ..][0..32].*))
            return error.InvalidSchemaValidationJob;
        var result: [4][]const u8 = undefined;
        var offset: usize = 24;
        const limits = [_]usize{ max_schema, max_schema, integrity.max_catalog_bytes, max_names };
        for (&result, limits, 0..) |*part, limit, i| {
            const length: usize = std.mem.readInt(u32, bytes[8 + 4 * i ..][0..4], .little);
            if (length == 0 or length > limit or length > bytes.len - 32 - offset) return error.InvalidSchemaValidationJob;
            part.* = bytes[offset..][0..length];
            offset += length;
        }
        if (offset != bytes.len - 32) return error.InvalidSchemaValidationJob;
        return result;
    }
    pub fn load(a: A, txn: anytype, binding: Binding, control: projection.Control) !Candidate {
        const raw = (try optional(txn, inputs_key)) orelse return error.SchemaValidationJobRequired;
        if (!eq(hash(raw), binding.inputs)) return error.SchemaValidationJobChanged;
        const view = try parts(raw);
        var names = try std.json.parseFromSlice([]const []const u8, a, view[3], .{});
        defer names.deinit();
        var result = try init(a, binding.fence.namespace.table_id, view[0], view[1], view[2], names.value, control);
        errdefer result.deinit();
        try binding.requireCandidate(&result);
        return result;
    }
};

pub const Binding = struct {
    id: [16]u8,
    plan_digest: Digest,
    owner: Digest,
    fence: topology.Fence,
    authority: authority.Kind,
    before_version: u32,
    after_version: u32,
    columns: u16,
    before_schema: Digest,
    after_schema: Digest,
    before_public: Digest,
    after_public: Digest,
    after_catalog: Digest,
    physical_catalog: Digest,
    inputs: Digest,
    projection_identity: Digest,

    pub fn validate(self: Binding) !void {
        _ = try self.fence.encode();
        if (self.fence.role != .schema_validation or self.fence.owner_group_id != self.fence.peer_group_id or
            self.fence.namespace.shard_id == 0 or self.fence.namespace.range_id == 0 or
            self.columns == 0 or self.columns > projection.max_columns or std.mem.allEqual(u8, &self.id, 0) or
            self.after_version != (std.math.add(u32, self.before_version, 1) catch return error.InvalidSchemaValidationJob))
            return error.InvalidSchemaValidationJob;
        inline for (.{ "plan_digest", "owner", "before_schema", "after_schema", "before_public", "after_public", "after_catalog", "physical_catalog", "inputs", "projection_identity" }) |field|
            if (std.mem.allEqual(u8, &@field(self, field), 0)) return error.InvalidSchemaValidationJob;
    }
    pub fn prepare(txn: anytype, candidate: *const Candidate, id: [16]u8, plan_digest: Digest, fence: topology.Fence, kind: authority.Kind) !Binding {
        const parts = try Candidate.parts(candidate.inputs);
        const result: Binding = .{
            .id = id,
            .plan_digest = plan_digest,
            .fence = fence,
            .authority = kind,
            .owner = try activation.ownership(txn),
            .before_version = candidate.before_version,
            .after_version = candidate.after_version,
            .columns = @intCast(candidate.plan.names.len),
            .before_schema = hash(candidate.native_before),
            .after_schema = hash(candidate.native_after),
            .before_public = hash(parts[0]),
            .after_public = hash(parts[1]),
            .after_catalog = hash(candidate.catalog_after),
            .physical_catalog = try digestAt(txn, physical.head_key),
            .inputs = hash(candidate.inputs),
            .projection_identity = candidate.plan.identity,
        };
        try result.validate();
        try result.requireCurrent(txn);
        return result;
    }
    pub fn requireCandidate(self: Binding, candidate: *const Candidate) !void {
        try self.validate();
        const parts = try Candidate.parts(candidate.inputs);
        if (self.before_version != candidate.before_version or self.after_version != candidate.after_version or
            self.columns != candidate.plan.names.len or !eq(self.projection_identity, candidate.plan.identity) or
            !eq(self.inputs, hash(candidate.inputs)) or !eq(self.before_schema, hash(candidate.native_before)) or
            !eq(self.after_schema, hash(candidate.native_after)) or !eq(self.before_public, hash(parts[0])) or
            !eq(self.after_public, hash(parts[1])) or !eq(self.after_catalog, hash(candidate.catalog_after)) or
            !eq(self.fence.catalog_digest, hash(parts[2]))) return error.SchemaValidationJobChanged;
    }
    fn requireNamespace(self: Binding, txn: anytype) !void {
        const actual = (try @import("doc_identity.zig").loadNamespaceTxn(txn)) orelse return error.IdentityNamespaceMismatch;
        if (!actual.eql(self.fence.namespace)) return error.IdentityNamespaceMismatch;
        _ = try authority.require(txn, self.authority, @import("online_source_contract.zig").namespaceBytes(actual));
    }
    fn requireCurrent(self: Binding, txn: anytype) !void {
        try self.requireNamespace(txn);
        if (!eq(self.owner, try activation.ownership(txn)) or !eq(self.before_schema, try digestAt(txn, schema.schema_key)) or
            !eq(self.before_public, try digestAt(txn, public_key)) or !eq(self.fence.catalog_digest, try digestAt(txn, integrity.key)) or
            !eq(self.physical_catalog, try digestAt(txn, physical.head_key))) return error.SchemaValidationJobChanged;
    }
};

pub const Phase = enum { fencing, scanning, ready, failed, canceling, canceled };
pub const Header = struct {
    binding: Binding,
    phase: Phase,
    admitted: ?positions.Position,
    frozen: ?positions.Position = null,
    updated: positions.Position,
    revision: u64 = 1,
    records: u64 = 0,
    rows_scanned: u64 = 0,
    cursor_digest: Digest,
    chain: Digest,
    last_effect: Digest,
    failure_column: ?u16 = null,

    fn validate(self: Header) !void {
        try self.binding.validate();
        inline for (.{ "cursor_digest", "chain", "last_effect" }) |field|
            if (std.mem.allEqual(u8, &@field(self, field), 0)) return error.InvalidSchemaValidationJob;
        if (self.revision == 0 or self.rows_scanned > self.records or
            (self.phase == .failed) != (self.failure_column != null) or
            (self.failure_column != null and self.failure_column.? >= self.binding.columns) or
            (self.phase != .canceling and self.phase != .canceled and self.admitted == null) or
            (self.frozen != null and self.admitted == null)) return error.InvalidSchemaValidationJob;
        if (self.phase == .fencing and (self.frozen != null or self.records != 0 or self.rows_scanned != 0 or !eq(self.cursor_digest, hash(""))))
            return error.InvalidSchemaValidationJob;
        if ((self.phase == .scanning or self.phase == .ready or self.phase == .failed) and self.frozen == null) return error.InvalidSchemaValidationJob;
        for ([_]?positions.Position{ self.admitted, self.frozen, self.updated }) |maybe| if (maybe) |position| {
            try position.requireNamespace(self.binding.fence.namespace);
            if ((position == .native) != (self.binding.authority == .native)) return error.InvalidControlReceiptPosition;
            try precedesOrEqual(position, self.updated);
        };
        if (self.frozen) |cut| try precedes(self.admitted.?, cut);
    }
    pub fn encode(self: Header, a: A) ![]u8 {
        try self.validate();
        var body: [max_header]u8 = undefined;
        var writer = std.Io.Writer.fixed(&body);
        try std.json.Stringify.value(self, .{}, &writer);
        const json = writer.buffered();
        const bytes = try a.alloc(u8, 12 + json.len + 32);
        @memcpy(bytes[0..8], "ASVJ\x01\x00\x00\x00");
        std.mem.writeInt(u32, bytes[8..12], @intCast(json.len), .little);
        @memcpy(bytes[12..][0..json.len], json);
        @memcpy(bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]));
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Header {
        if (bytes.len < 44 or bytes.len > 44 + max_header or !std.mem.eql(u8, bytes[0..8], "ASVJ\x01\x00\x00\x00") or
            std.mem.readInt(u32, bytes[8..12], .little) != bytes.len - 44 or !eq(hash(bytes[0 .. bytes.len - 32]), bytes[bytes.len - 32 ..][0..32].*))
            return error.InvalidSchemaValidationJob;
        var scratch: [64 * 1024]u8 = undefined;
        var fixed = std.heap.FixedBufferAllocator.init(&scratch);
        const result = std.json.parseFromSliceLeaky(Header, fixed.allocator(), bytes[12 .. bytes.len - 32], .{}) catch return error.InvalidSchemaValidationJob;
        try result.validate();
        var canonical: [max_header]u8 = undefined;
        var writer = std.Io.Writer.fixed(&canonical);
        try std.json.Stringify.value(result, .{}, &writer);
        if (!std.mem.eql(u8, writer.buffered(), bytes[12 .. bytes.len - 32])) return error.InvalidSchemaValidationJob;
        return result;
    }
    pub fn digest(self: Header) !Digest {
        var buffer: [max_header]u8 = undefined;
        var writer = std.Io.Writer.fixed(&buffer);
        try std.json.Stringify.value(self, .{}, &writer);
        return hash(writer.buffered());
    }
};

fn precedesOrEqual(a: positions.Position, b: positions.Position) !void {
    if (a == .native and b == .native) {
        if (!a.native.namespace.eql(b.native.namespace) or a.native.sequence > b.native.sequence) return error.InvalidControlReceiptPosition;
    } else if (a == .raft and b == .raft) {
        if (a.raft.index > b.raft.index or a.raft.term > b.raft.term or (a.raft.index == b.raft.index and a.raft.term != b.raft.term))
            return error.InvalidControlReceiptPosition;
    } else return error.InvalidControlReceiptPosition;
}
fn precedes(a: positions.Position, b: positions.Position) !void {
    try precedesOrEqual(a, b);
    if (std.meta.eql(a, b)) return error.InvalidControlReceiptPosition;
}
pub fn load(txn: anytype) !?Header {
    return if (try optional(txn, key)) |bytes| try Header.decode(bytes) else null;
}
fn save(a: A, txn: anytype, header: Header) !void {
    const bytes = try header.encode(a);
    defer a.free(bytes);
    try txn.put(key, bytes);
}
fn requireCommitted(txn: anytype, header: Header) !void {
    try header.binding.requireNamespace(txn);
    const actual: positions.Position = if (header.binding.authority == .native)
        .{ .native = .{ .namespace = header.binding.fence.namespace, .sequence = (try authority.require(txn, .native, @import("online_source_contract.zig").namespaceBytes(header.binding.fence.namespace))).sequence } }
    else blk: {
        const entry = (try receipts.readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidControlReceiptPosition;
        break :blk .{ .raft = .{ .term = entry.term, .index = entry.index } };
    };
    try actual.validate();
    try precedesOrEqual(header.updated, actual);
}
fn nextPosition(txn: anytype, binding: Binding, entry: ?receipts.OrderedApplyReceipt) !positions.Position {
    try binding.requireNamespace(txn);
    if (binding.authority == .native) {
        if (entry != null) return error.InvalidControlReceiptPosition;
        return .{ .native = .{ .namespace = binding.fence.namespace, .sequence = try authority.advance(txn, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace), null) } };
    }
    const committed = entry orelse return error.InvalidControlReceiptPosition;
    if (try receipts.orderedApplyDisposition(try receipts.readOrderedApplyReceiptTxn(txn), committed) != .apply) return error.InvalidControlReceiptPosition;
    return .{ .raft = .{ .term = committed.term, .index = committed.index } };
}

/// Hot fencing checks are fixed-size and allocation-free. They deny writes;
/// structural validity is not an authenticated validation receipt.
pub fn requireActive(txn: anytype, fence: topology.Fence) !void {
    const header = (try load(txn)) orelse return error.SchemaValidationJobRequired;
    if (!header.binding.fence.eql(fence) or header.phase == .canceled) return error.SchemaValidationJobChanged;
    try header.binding.requireNamespace(txn);
}
pub fn requireFencing(txn: anytype, fence: topology.Fence) !void {
    try requireActive(txn, fence);
    if ((try load(txn)).?.phase != .fencing) return error.SchemaValidationJobChanged;
}
pub fn requireCanceling(txn: anytype, fence: topology.Fence) !void {
    try requireActive(txn, fence);
    if ((try load(txn)).?.phase != .canceling) return error.SchemaValidationCancellationRequired;
}
fn workHeader(txn: anytype, binding: Binding) !Header {
    const header = (try load(txn)) orelse return error.SchemaValidationJobRequired;
    if (!std.meta.eql(header.binding, binding) or header.phase == .canceling or header.phase == .canceled) return error.SchemaValidationJobChanged;
    try requireCommitted(txn, header);
    const actual = (try topology.current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!actual.eql(binding.fence)) return error.IntegrityTopologyChanged;
    // The exclusive owner fence blocks schema/catalog/ownership changes in
    // core.commitSchemaMetadata and the normal topology paths. Recheck small
    // durable facts here; do not rehash multi-MiB schemas on each page apply.
    const facts = try @import("table_catalog.zig").Catalog.decode((try optional(txn, @import("table_catalog.zig").key)) orelse return error.SchemaValidationJobChanged);
    if (facts.storage_mode != .relational or facts.active_schema_version != binding.before_version or
        !eq(binding.physical_catalog, try digestAt(txn, physical.head_key))) return error.SchemaValidationJobChanged;
    return header;
}

/// Preparation guard, not permission to install or release a schema. The
/// publication participant must recheck this exact ready cut under apply.
pub fn requireReady(txn: anytype, binding: Binding) !Header {
    const header = try workHeader(txn, binding);
    if (header.phase != .ready) return error.SchemaValidationNotReady;
    try binding.requireCurrent(txn);
    return header;
}

pub fn stageBegin(a: A, txn: anytype, binding: Binding, candidate: *const Candidate, entry: ?receipts.OrderedApplyReceipt) !Header {
    try binding.validate();
    try binding.requireCandidate(candidate);
    try binding.requireCurrent(txn);
    if (try load(txn)) |prior| {
        try requireCommitted(txn, prior);
        if (std.meta.eql(prior.binding, binding)) {
            if (prior.phase == .canceled) return error.SchemaValidationCanceled;
            return workHeader(txn, binding);
        }
        if (prior.phase != .canceled or prior.binding.fence.admission_epoch >= binding.fence.admission_epoch) return error.SchemaValidationJobChanged;
    }
    if (try topology.current(txn) != null) return error.IntegrityTopologyBusy;
    const position = try nextPosition(txn, binding, entry);
    const header: Header = .{ .binding = binding, .phase = .fencing, .admitted = position, .updated = position, .cursor_digest = hash(""), .chain = hash(candidate.inputs), .last_effect = hash("schema-validation-begin/v1") };
    try txn.put(inputs_key, candidate.inputs);
    try txn.put(cursor_key, "");
    try save(a, txn, header);
    try topology.stageSchemaValidationBegin(txn, binding.fence);
    return header;
}
pub fn stageFrozen(a: A, txn: anytype, manager: *transactions.TxnManager, binding: Binding, entry: ?receipts.OrderedApplyReceipt) !Header {
    var header = try workHeader(txn, binding);
    try binding.requireCurrent(txn);
    try topology.requireDrained(txn, manager, binding.fence);
    if (header.frozen != null) return header;
    if (header.phase != .fencing) return error.SchemaValidationJobChanged;
    const position = try nextPosition(txn, binding, entry);
    try precedes(header.updated, position);
    header.frozen = position;
    header.updated = position;
    header.phase = .scanning;
    header.revision = try std.math.add(u64, header.revision, 1);
    try save(a, txn, header);
    return header;
}

/// Trusted local read preparation, not a serializable success assertion.
/// A Raft adapter must independently prepare/verify bounded work from its
/// committed command; accepting caller-supplied page fields is not supported.
pub const PreparedPage = struct {
    page: rows.NullabilityPage,
    expected: Header,
    pub fn deinit(self: *PreparedPage) void {
        self.page.deinit();
        self.* = undefined;
    }
    pub fn prepare(a: A, core: anytype, binding: Binding, budget: rows.Budget, control: projection.Control) !PreparedPage {
        try control.ensureActive();
        var reader = blk: {
            core.lockApplyShared();
            defer core.unlockApplyShared();
            var view = core.acquireSchemaView() orelse return error.RelationalTableRequired;
            defer view.release();
            break :blk try rows.Reader.open(a, core.store, view, null, .{}, 0);
        };
        defer reader.deinit();
        const header = try workHeader(&reader.read, binding);
        try binding.requireCurrent(&reader.read);
        if (header.phase != .scanning) return error.SchemaValidationJobChanged;
        const raw_cursor = (try optional(&reader.read, cursor_key)) orelse return error.InvalidSchemaValidationJob;
        if (raw_cursor.len > max_cursor or !eq(hash(raw_cursor), header.cursor_digest)) return error.InvalidSchemaValidationJob;
        const cursor = try a.dupe(u8, raw_cursor);
        defer a.free(cursor);
        var candidate = try Candidate.load(a, &reader.read, binding, control);
        defer candidate.deinit();
        try reader.resumeNullabilityScan(&candidate.plan, .{ .projection_identity = binding.projection_identity, .after = cursor }, control);
        const page = try reader.nextNullabilityPage(a, null, budget, &candidate.plan, control);
        return .{ .page = page, .expected = header };
    }
    fn effect(self: *const PreparedPage) !Digest {
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("schema-validation-page/v1");
        state.update(&try self.expected.digest());
        state.update(&hash(self.page.after));
        var numbers: [25]u8 = undefined;
        std.mem.writeInt(u64, numbers[0..8], self.page.rows_examined, .little);
        std.mem.writeInt(u64, numbers[8..16], self.page.page.records_examined, .little);
        std.mem.writeInt(u64, numbers[16..24], if (self.page.violation_column) |column| column else std.math.maxInt(u64), .little);
        numbers[24] = @intFromBool(self.page.page.more);
        state.update(&numbers);
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }
};

pub fn stagePage(a: A, txn: anytype, manager: *transactions.TxnManager, prepared: *const PreparedPage, entry: ?receipts.OrderedApplyReceipt) !Header {
    const binding = prepared.expected.binding;
    var header = try workHeader(txn, binding);
    try topology.requireDrained(txn, manager, binding.fence);
    const effect = try prepared.effect();
    if (eq(header.last_effect, effect)) return header;
    if (header.phase != .scanning or !eq(try header.digest(), try prepared.expected.digest()) or
        prepared.page.rows_examined > prepared.page.page.records_examined or
        (prepared.page.page.more and prepared.page.page.records_examined == 0) or prepared.page.after.len > max_cursor)
        return error.SchemaValidationJobChanged;
    header.records = try std.math.add(u64, header.records, prepared.page.page.records_examined);
    header.rows_scanned = try std.math.add(u64, header.rows_scanned, prepared.page.rows_examined);
    header.revision = try std.math.add(u64, header.revision, 1);
    header.cursor_digest = hash(prepared.page.after);
    header.failure_column = if (prepared.page.violation_column) |column| std.math.cast(u16, column) orelse return error.InvalidSchemaValidationJob else null;
    header.phase = if (header.failure_column != null) .failed else if (prepared.page.page.more) .scanning else .ready;
    // The terminal validation cut rechecks the complete immutable metadata
    // once. Intermediate pages use the fixed fence/epoch guards above.
    if (header.phase == .ready) try binding.requireCurrent(txn);
    var chain = std.crypto.hash.Blake3.init(.{});
    chain.update(&header.chain);
    chain.update(&effect);
    chain.final(&header.chain);
    header.last_effect = effect;
    const position = try nextPosition(txn, binding, entry);
    try precedes(header.updated, position);
    header.updated = position;
    try txn.put(cursor_key, prepared.page.after);
    try save(a, txn, header);
    return header;
}

/// Consumes the admission epoch even when begin never arrived. Old schema,
/// rows, indexes and claims are unchanged. A dedicated terminal intent gates
/// release; generic topology cancellation stays unavailable.
pub fn stageCancel(a: A, txn: anytype, manager: *transactions.TxnManager, binding: Binding, entry: ?receipts.OrderedApplyReceipt) !Header {
    try binding.validate();
    try binding.requireNamespace(txn);
    const previous = try load(txn);
    if (previous) |prior| {
        // Cancellation is terminal progress, not permission to repair an
        // unproven restored/future cut by overwriting it with a local clock.
        try requireCommitted(txn, prior);
        if (std.meta.eql(prior.binding, binding) and prior.phase == .canceled) {
            return prior;
        }
        if (!std.meta.eql(prior.binding, binding) and (prior.phase != .canceled or prior.binding.fence.admission_epoch >= binding.fence.admission_epoch))
            return error.SchemaValidationJobChanged;
    }
    if (try topology.current(txn)) |actual| if (!actual.eql(binding.fence)) return error.IntegrityTopologyBusy;
    if (try topology.completed(txn)) |done| if (done.admission_epoch >= binding.fence.admission_epoch) return error.SchemaValidationJobChanged;
    const position = try nextPosition(txn, binding, entry);
    if (previous) |prior| try precedes(prior.updated, position);
    var header: Header = if (previous != null and std.meta.eql(previous.?.binding, binding)) previous.? else .{ .binding = binding, .phase = .canceling, .admitted = null, .updated = position, .cursor_digest = hash(""), .chain = hash("schema-validation-unvisited/v1"), .last_effect = hash("schema-validation-cancel/v1") };
    header.phase = .canceling;
    header.failure_column = null;
    header.updated = position;
    header.revision = try std.math.add(u64, header.revision, 1);
    try save(a, txn, header);
    try topology.stageSchemaValidationCancellation(txn, manager, binding.fence);
    header.phase = .canceled;
    header.last_effect = hash("schema-validation-cancel/v1");
    try save(a, txn, header);
    return header;
}
