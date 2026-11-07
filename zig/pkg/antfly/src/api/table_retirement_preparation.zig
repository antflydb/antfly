// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Read-only PRE-SEAL capture for an already admitted table-retirement job.
//! The receiving owner fetches its own immutable manifest and reserved head;
//! callers supply neither a physical root nor arbitrary lookup options.
const std = @import("std");
const local = @import("antfly_local_sources");
const ledger = @import("../metadata/table_retirement_ledger.zig");
const manifests = @import("../metadata/table_retirement_manifest.zig");
const scopes = @import("../metadata/scope_retirement_ledger.zig");
const tables = @import("../metadata/table_manager.zig");
const persistence = local.common_topology_records.RelationPersistence;
const operation = local.api_operation;
pub const max_request_bytes = 16384;
pub const max_response_bytes = std.base64.standard.Encoder.calcSize(local.storage_db_relational_integrity_catalog.max_catalog_bytes) + 65536;

pub const Request = struct {
    lookup: ledger.Lookup,
    expected_revision: u64,
    effects: manifests.Reference,
    scope_parent: ?scopes.Child = null,
    metadata_incarnation: [32]u8,
    table_id: u64,
    owner_group_id: u64,
    namespace: local.storage_db_doc_identity_namespace.Namespace,

    pub fn validate(self: @This(), group: u64) !void {
        try self.lookup.validate();
        try self.effects.validate();
        if (group == 0 or self.owner_group_id != group or self.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or self.table_id != self.namespace.table_id or self.effects.root_table != self.lookup.table or
            self.expected_revision == 0 or self.expected_revision == std.math.maxInt(u64) or !local.metadata_incarnation.isValid(self.metadata_incarnation)) return error.InvalidCatalogRecord;
        if (self.scope_parent) |parent| {
            try parent.validate();
            if (parent.table != self.lookup.table or parent.lookup.group != self.lookup.group or !std.mem.eql(u8, &parent.id, &self.lookup.id)) return error.InvalidCatalogRecord;
        }
    }
    pub fn requireReservedHead(self: @This(), head: ledger.Head) !void {
        try self.validate(self.owner_group_id);
        try head.validate();
        if (head.phase != .reserved or head.revision != self.expected_revision or !std.meta.eql(head.lookup, self.lookup) or
            !std.meta.eql(head.effects, self.effects) or !std.meta.eql(head.scope.parent, self.scope_parent) or
            !std.mem.eql(u8, &head.incarnation, &self.metadata_incarnation)) return error.GenerationRetirementChanged;
    }
};

/// Borrowed from the receiver-owned parsed immutable manifest region.
pub const Selection = struct { table: local.common_topology_records.TableRecord, range: local.common_topology_records.RangeRecord };

pub fn selectReserved(a: std.mem.Allocator, request: Request, group: u64, table_name: []const u8, head: ledger.Head, manifest: manifests.Manifest) !Selection {
    try request.validate(group);
    try request.requireReservedHead(head);
    try manifest.validate(a);
    if (!std.meta.eql(try manifest.reference(), request.effects) or !std.meta.eql(manifest.scope_child, request.scope_parent) or
        !std.mem.eql(u8, &manifest.metadata_incarnation, &request.metadata_incarnation) or
        manifest.metadata_group_id != request.lookup.group or manifest.root.table_id != request.lookup.table) return error.GenerationRetirementChanged;
    const Find = struct {
        fn member(selected: Request, name: []const u8, table: local.common_topology_records.TableRecord, ranges: []const local.common_topology_records.RangeRecord) !?Selection {
            if (table.table_id != selected.table_id) return null;
            if (!std.mem.eql(u8, table.name, name) or table.sql_persistence == null) return error.InvalidSqlRelationPersistence;
            try table.sql_persistence.?.validate();
            for (ranges) |range| if (range.table_id == table.table_id and range.group_id == selected.owner_group_id and
                tables.rangeDocIdentityShardId(range) == selected.namespace.shard_id and tables.rangeDocIdentityRangeId(range) == selected.namespace.range_id)
            {
                return .{ .table = table, .range = range };
            };
            return error.IdentityNamespaceMismatch;
        }
    };
    if (try Find.member(request, table_name, manifest.root, manifest.root_ranges)) |selected| return selected;
    for (manifest.external_parents) |parent| if (try Find.member(request, table_name, parent.table, parent.ranges)) |selected| return selected;
    for (manifest.survivors) |pair| if (try Find.member(request, table_name, pair.before, manifest.survivor_ranges)) |selected| return selected;
    return error.InvalidCatalogRecord;
}

pub const Response = struct {
    request: Request,
    identity: local.storage_db_relational_integrity_topology_contract.Identity,
    native_binding: persistence.NativeBinding,
    boot_generation: u64,
    metadata_incarnation: [32]u8,
    /// Existing bounded integrity-catalog JSON envelope, not caller row data.
    catalog_json: []const u8,

    pub fn requireRequest(self: @This(), expected: Request) !void {
        try expected.validate(expected.owner_group_id);
        try self.request.validate(expected.owner_group_id);
        try self.native_binding.validate();
        if (!std.meta.eql(self.request, expected) or self.boot_generation == 0 or !std.mem.eql(u8, &self.metadata_incarnation, &expected.metadata_incarnation) or
            !self.identity.namespace.eql(expected.namespace) or self.identity.next_epoch == 0 or !self.identity.catalog_checks_identity_supported or
            self.native_binding.table_id != expected.namespace.table_id or self.native_binding.shard_id != expected.namespace.shard_id or self.native_binding.range_id != expected.namespace.range_id or
            self.catalog_json.len == 0 or self.catalog_json.len > max_response_bytes - 32768) return error.InvalidCatalogRecord;
    }
    pub fn requireFor(self: @This(), expected: Request, descriptor: persistence.Descriptor) !void {
        try self.requireRequest(expected);
        if (!self.native_binding.descriptor.eql(descriptor)) return error.SqlRelationOwnerChanged;
        try self.requireCatalog();
    }
    fn requireCatalog(self: @This()) !void {
        // Canonical native envelopes borrow their Base64 string; malformed
        // escaped/extra JSON cannot consume an unbounded parsing arena here.
        var json_buffer: [16384]u8 = undefined;
        var json_allocator = std.heap.FixedBufferAllocator.init(&json_buffer);
        const Envelope = struct { catalog: []const u8, schema_version: u32, table_id: []const u8 };
        const envelope = std.json.parseFromSliceLeaky(Envelope, json_allocator.allocator(), self.catalog_json, .{ .ignore_unknown_fields = true, .allocate = .alloc_if_needed }) catch return error.InvalidCatalogRecord;
        if ((std.fmt.parseInt(u64, envelope.table_id, 10) catch return error.InvalidCatalogRecord) != self.request.table_id) return error.InvalidCatalogRecord;
        const size = std.base64.standard.Decoder.calcSizeForSlice(envelope.catalog) catch return error.InvalidCatalogRecord;
        if (size == 0 or size > local.storage_db_relational_integrity_catalog.max_catalog_bytes) return error.InvalidCatalogRecord;
        var decoded: [3072]u8 = undefined;
        var hash = std.crypto.hash.Blake3.init(.{});
        var remaining = envelope.catalog;
        while (remaining.len != 0) {
            const count = @min(@as(usize, 4096), remaining.len);
            const encoded = remaining[0..count];
            if (count < remaining.len and std.mem.indexOfScalar(u8, encoded, '=') != null) return error.InvalidCatalogRecord;
            const length = std.base64.standard.Decoder.calcSizeForSlice(encoded) catch return error.InvalidCatalogRecord;
            if (length > decoded.len) return error.InvalidCatalogRecord;
            std.base64.standard.Decoder.decode(decoded[0..length], encoded) catch return error.InvalidCatalogRecord;
            hash.update(decoded[0..length]);
            remaining = remaining[count..];
        }
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        if (!std.mem.eql(u8, &digest, &self.identity.catalog_digest)) return error.InvalidCatalogRecord;
    }
    pub fn encodeAlloc(self: @This(), a: std.mem.Allocator) ![]u8 {
        try self.requireFor(self.request, self.native_binding.descriptor);
        const bytes = try std.json.Stringify.valueAlloc(a, self, .{});
        errdefer a.free(bytes);
        if (bytes.len > max_response_bytes) return error.InvalidCatalogRecord;
        return bytes;
    }
};

pub const Port = struct {
    ptr: *anyopaque,
    prepare_fn: *const fn (*anyopaque, std.mem.Allocator, u64, []const u8, Request, operation.RequestContext) anyerror![]u8,
    pub fn prepare(self: @This(), a: std.mem.Allocator, group: u64, table: []const u8, request: Request, context: operation.RequestContext) ![]u8 {
        if (!context.setting_admin or !context.schema_validation_authority or !context.fk_generation_publication_authority) return error.Forbidden;
        try context.ensureActive();
        try request.validate(group);
        return self.prepare_fn(self.ptr, a, group, table, request, context);
    }
};

test "SQL relation lifetime retirement capture rejects stale reserved heads before manifest selection" {
    const request: Request = .{
        .lookup = .{ .group = 2188, .table = 7, .id = @splat(1) },
        .expected_revision = 1,
        .effects = .{ .digest = @splat(2), .root_table = 7, .survivors = 0, .counters = 0, .views = 0 },
        .metadata_incarnation = "11111111111111111111111111111111".*,
        .table_id = 7,
        .owner_group_id = 72,
        .namespace = .{ .table_id = 7, .shard_id = 172, .range_id = 173 },
    };
    const head: ledger.Head = .{ .lookup = request.lookup, .incarnation = request.metadata_incarnation, .effects = request.effects, .scope = .{ .database = 1, .namespace = 2, .creator = .bootstrap }, .participant_count = 1, .range_count = 1, .participants_digest = @splat(3) };
    try request.validate(72);
    try std.testing.expectError(error.InvalidCatalogRecord, request.validate(172));
    try request.requireReservedHead(head);
    var canceled = head;
    canceled.phase = .canceled;
    canceled.revision = 2;
    try canceled.validate();
    try std.testing.expectError(error.GenerationRetirementChanged, request.requireReservedHead(canceled));
    var changed = head;
    changed.effects.digest[0] ^= 1;
    try changed.validate();
    try std.testing.expectError(error.GenerationRetirementChanged, request.requireReservedHead(changed));
    changed = head;
    changed.incarnation[0] = '2';
    try changed.validate();
    try std.testing.expectError(error.GenerationRetirementChanged, request.requireReservedHead(changed));
    var advanced = request;
    advanced.expected_revision += 1;
    try std.testing.expectError(error.GenerationRetirementChanged, advanced.requireReservedHead(head));
}

test "SQL relation lifetime retirement capture binds bounded catalog bytes descriptor namespace and exact request" {
    const a = std.testing.allocator;
    const request: Request = .{ .lookup = .{ .group = 2188, .table = 7, .id = @splat(1) }, .expected_revision = 1, .effects = .{ .digest = @splat(2), .root_table = 7, .survivors = 0, .counters = 0, .views = 0 }, .metadata_incarnation = "11111111111111111111111111111111".*, .table_id = 7, .owner_group_id = 72, .namespace = .{ .table_id = 7, .shard_id = 172, .range_id = 173 } };
    const descriptor: persistence.Descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(3), .data_generation = 1 };
    var raw: [6144]u8 = undefined;
    @memset(&raw, 0x93);
    const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(raw.len));
    defer a.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, &raw);
    const catalog = try std.json.Stringify.valueAlloc(a, .{ .catalog = encoded, .schema_version = @as(u32, 0), .table_id = "7" }, .{});
    defer a.free(catalog);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(&raw, &digest, .{});
    const response: Response = .{ .request = request, .identity = .{ .namespace = request.namespace, .catalog_digest = digest, .next_epoch = 1, .catalog_checks_identity_supported = true }, .native_binding = .{ .descriptor = descriptor, .native_instance = @splat(4), .table_id = 7, .shard_id = 172, .range_id = 173, .physical_root_incarnation = 1, .schema_version = 0, .schema_digest = @splat(5), .catalog_generation = 1, .catalog_fingerprint = @splat(6) }, .boot_generation = 1, .metadata_incarnation = request.metadata_incarnation, .catalog_json = catalog };
    try response.requireFor(request, descriptor);
    const wire = try response.encodeAlloc(a);
    defer a.free(wire);
    var parsed = try std.json.parseFromSlice(Response, a, wire, .{});
    defer parsed.deinit();
    try parsed.value.requireFor(request, descriptor);
    var wrong = response;
    wrong.identity.catalog_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidCatalogRecord, wrong.requireFor(request, descriptor));
    wrong = response;
    wrong.identity.namespace.shard_id = 72;
    try std.testing.expectError(error.InvalidCatalogRecord, wrong.requireFor(request, descriptor));
    wrong = response;
    wrong.request.expected_revision += 1;
    try std.testing.expectError(error.InvalidCatalogRecord, wrong.requireFor(request, descriptor));
    var other = descriptor;
    other.data_generation += 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, response.requireFor(request, other));
    wrong = response;
    wrong.catalog_json = @as([*]const u8, @ptrFromInt(1))[0..max_response_bytes];
    try std.testing.expectError(error.InvalidCatalogRecord, wrong.requireRequest(request));
}
