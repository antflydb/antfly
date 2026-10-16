// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Owned online-index preparation from a table-scoped metadata cut and actual
//! read-index owner observations. Preparation does not reserve or publish.
const std = @import("std");
const local = @import("antfly_local_sources");
const publication = @import("../metadata/relational_index_publication.zig");
const manager = @import("../metadata/table_manager.zig");
const tables = @import("tables.zig");
const operation = local.api_operation;
const types = local.storage_db_types;

pub const Owned = struct {
    arena: std.heap.ArenaAllocator,
    request: publication.BeginRequest,
    prepared: publication.Prepared,
    observation: publication.AdmissionRequest,
    recovery_token: [@import("index_publication_recovery.zig").Token.encoded_len]u8,

    pub fn deinit(self: *Owned) void {
        self.prepared.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
};

const catalog_wire_limit = std.base64.standard.Encoder.calcSize(local.storage_db_relational_integrity_catalog.max_catalog_bytes) + 2048;

/// Entry point shared by trusted SQL/REST lowering after table authorization.
/// Never manufacture administrative authority from a public request here.
pub fn prepare(source: anytype, reader: anytype, alloc: std.mem.Allocator, io: std.Io, context: operation.RequestContext, request: publication.PrepareRequest, schema_json: []const u8) !Owned {
    if (!context.setting_admin or !context.index_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try request.validate();
    const bytes = try source.systemCatalog(alloc, context, .{ .relational_index_prepare = request });
    defer alloc.free(bytes);
    if (bytes.len == 0 or bytes.len > publication.PrepareCut.max_wire_bytes) return error.InvalidCatalogRecord;
    var cut = try std.json.parseFromSlice(publication.PrepareCut, alloc, bytes, .{ .allocate = .alloc_always });
    defer cut.deinit();
    return build(reader, alloc, io, context, cut.value, request, schema_json);
}

/// Owns every table/range/name after returning; neither a response buffer nor
/// the metadata snapshot supplying the cut needs to remain alive.
pub fn build(reader: anytype, alloc: std.mem.Allocator, io: std.Io, context: operation.RequestContext, cut: publication.PrepareCut, request: publication.PrepareRequest, schema_json: []const u8) !Owned {
    try context.ensureActive();
    try cut.requireRequest(alloc, request);
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    const s = scratch.allocator();
    var opts: types.LookupOptions = .{ .relational_integrity_catalog = true, .execution_deadline_ns = context.deadline_ns, .execution_io = context.deadline_io, .cancellation = context.cancellation };
    const predecessor = blk: {
        var response = (try reader.lookupGroupLocal(s, cut.ranges[0].group_id, cut.before.name, cut.ranges[0].start_key, opts, .read_index)) orelse return error.IntegrityCatalogUnavailable;
        defer response.deinit(s);
        if (response.json.len == 0 or response.json.len > catalog_wire_limit) return error.InvalidIntegrityCatalog;
        const envelope = try std.json.parseFromSliceLeaky(struct { catalog: []const u8, schema_version: u32, table_id: []const u8 }, s, response.json, .{ .ignore_unknown_fields = true });
        if ((std.fmt.parseInt(u64, envelope.table_id, 10) catch return error.InvalidIntegrityCatalog) != cut.before.table_id or
            envelope.schema_version != request.guard.owner.schema_version) return error.CatalogGenerationChanged;
        const size = std.base64.standard.Decoder.calcSizeForSlice(envelope.catalog) catch return error.InvalidIntegrityCatalog;
        if (size == 0 or size > local.storage_db_relational_integrity_catalog.max_catalog_bytes) return error.InvalidIntegrityCatalog;
        const raw = try s.alloc(u8, size);
        std.base64.standard.Decoder.decode(raw, envelope.catalog) catch return error.InvalidIntegrityCatalog;
        var observed_digest: publication.Digest = undefined;
        std.crypto.hash.Blake3.hash(raw, &observed_digest, .{});
        break :blk .{ .b64 = try a.dupe(u8, envelope.catalog), .digest = observed_digest };
    };
    const catalog_b64 = predecessor.b64;
    const catalog_digest = predecessor.digest;
    const owners = try a.alloc(publication.Owner, cut.ranges.len);
    opts.relational_integrity_catalog = false;
    opts.relational_topology_json = "{\"mode\":\"identity\"}";
    for (cut.ranges, owners) |range, *owner| {
        try context.ensureActive();
        _ = scratch.reset(.retain_capacity);
        // Address the captured group directly. A key-based reroute could select
        // another owner after topology churn and adds needless routing work.
        var observed = (try reader.lookupGroupLocal(s, range.group_id, cut.before.name, range.start_key, opts, .read_index)) orelse return error.TableNotFound;
        defer observed.deinit(s);
        if (observed.json.len == 0 or observed.json.len > 2048) return error.InvalidIntegrityCatalog;
        const identity = try std.json.parseFromSliceLeaky(struct {
            namespace: local.storage_db_doc_identity.Namespace,
            catalog_digest: publication.Digest,
            next_epoch: u64,
            catalog_checks_identity_supported: bool = false,
        }, s, observed.json, .{ .ignore_unknown_fields = true });
        if (!identity.catalog_checks_identity_supported) return error.TableTopologyProtocolUpgradeRequired;
        if (identity.next_epoch == 0 or identity.namespace.table_id != cut.before.table_id or
            identity.namespace.shard_id != manager.rangeDocIdentityShardId(range) or
            identity.namespace.range_id != manager.rangeDocIdentityRangeId(range) or
            !std.mem.eql(u8, &identity.catalog_digest, &catalog_digest)) return error.GenerationPublicationChanged;
        owner.* = .{ .range = try manager.cloneRange(a, range), .namespace = identity.namespace, .admission_epoch = identity.next_epoch, .catalog_digest = catalog_digest, .receipt_authority = cut.authority };
    }
    var guard = request.guard;
    guard.target.database = try a.dupe(u8, guard.target.database);
    guard.target.namespace = try a.dupe(u8, guard.target.namespace);
    guard.target.name = try a.dupe(u8, guard.target.name);
    guard.logical_table = try a.dupe(u8, guard.logical_table);
    const before = try manager.cloneTable(a, cut.before);
    const after = try tables.prepareForeignKeyPublicationRecord(a, &before, schema_json);
    var id: publication.Id = undefined;
    while (true) {
        try context.ensureActive();
        try io.randomSecure(&id);
        if (std.mem.readInt(u64, id[0..8], .little) != 0 and std.mem.readInt(u64, id[8..16], .little) != 0) break;
    }
    const begin: publication.BeginRequest = .{ .metadata_incarnation = cut.metadata_incarnation, .begin = .{
        .expected_head = cut.expected_head,
        .plan = .{ .id = id, .namespace_id = cut.namespace_id, .index_name = try a.dupe(u8, request.index_name), .table_guard = guard, .before = before, .after = after, .catalog_before_b64 = catalog_b64, .owners = owners },
    } };
    var prepared = try begin.begin.plan.prepare(alloc);
    errdefer prepared.deinit();
    try context.ensureActive();
    const observation = try begin.observation(&prepared);
    return .{ .arena = arena, .request = begin, .prepared = prepared, .observation = observation, .recovery_token = try @import("index_publication_recovery.zig").Token.encode(observation) };
}

test "relational index publication plan builder owns actual burned catalog allocations and rejects owner drift" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var plan = try publication.testing.plan(arena.allocator());
    var original = try plan.prepare(a);
    defer original.deinit();
    // A canceled attempt burns an allocation without adding a public schema
    // declaration. Reconstructing the catalog from schema loses this state.
    const burned = original.candidate.reservation.value;
    const b64 = try arena.allocator().alloc(u8, std.base64.standard.Encoder.calcSize(burned.len));
    _ = std.base64.standard.Encoder.encode(b64, burned);
    plan.catalog_before_b64 = b64;
    var digest: publication.Digest = undefined;
    std.crypto.hash.Blake3.hash(burned, &digest, .{});
    const Reader = struct {
        catalog: []const u8,
        digest: publication.Digest,
        namespace: local.storage_db_doc_identity.Namespace,
        reads: usize = 0,
        changed: bool = false,
        wrong_owner: bool = false,
        old_decoder: bool = false,
        pub fn lookupGroupLocal(self: *@This(), alloc: std.mem.Allocator, group_id: u64, table: []const u8, key: []const u8, opts: types.LookupOptions, consistency: anytype) !?local.api_table_read_source.LookupResponse {
            try std.testing.expectEqual(@as(u64, 301), group_id);
            try std.testing.expectEqualStrings("physical:101", table);
            try std.testing.expectEqualStrings("", key);
            try std.testing.expect(consistency == .read_index);
            self.reads += 1;
            const bytes = if (opts.relational_integrity_catalog)
                try std.json.Stringify.valueAlloc(alloc, .{ .catalog = self.catalog, .schema_version = @as(u32, 1), .table_id = "101" }, .{})
            else blk: {
                try std.testing.expectEqualStrings("{\"mode\":\"identity\"}", opts.relational_topology_json);
                var hash = self.digest;
                if (self.changed) hash[0] ^= 1;
                var namespace = self.namespace;
                if (self.wrong_owner) namespace.range_id += 1;
                break :blk try std.json.Stringify.valueAlloc(alloc, .{ .namespace = namespace, .catalog_digest = hash, .next_epoch = @as(u64, 7), .catalog_checks_identity_supported = !self.old_decoder }, .{});
            };
            return .{ .json = bytes, .version = 0 };
        }
    };
    var reader: Reader = .{ .catalog = b64, .digest = digest, .namespace = plan.owners[0].namespace };
    const request: publication.PrepareRequest = .{ .guard = plan.table_guard, .index_name = plan.index_name };
    const cut: publication.PrepareCut = .{ .metadata_incarnation = "01010101010101010101010101010101".*, .namespace_id = plan.namespace_id, .table_guard = plan.table_guard, .before = plan.before, .ranges = &.{plan.owners[0].range}, .authority = .native };
    const Source = struct {
        cut: publication.PrepareCut,
        calls: usize = 0,
        pub fn systemCatalog(self: *@This(), alloc: std.mem.Allocator, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            try std.testing.expect(context.setting_admin and context.index_publication_authority);
            try std.testing.expect(call == .relational_index_prepare);
            try self.cut.requireRequest(alloc, call.relational_index_prepare);
            self.calls += 1;
            return self.cut.encode(alloc);
        }
    };
    var source: Source = .{ .cut = cut };
    try std.testing.expectError(error.Forbidden, prepare(&source, &reader, a, std.testing.io, .{ .setting_admin = true }, request, plan.after.schema_json));
    try std.testing.expectEqual(@as(usize, 0), source.calls);
    var owned = try prepare(&source, &reader, a, std.testing.io, .{ .setting_admin = true, .index_publication_authority = true }, request, plan.after.schema_json);
    defer owned.deinit();
    try std.testing.expectEqual(@as(usize, 1), source.calls);
    try std.testing.expectEqual(@as(usize, 2), reader.reads);
    try std.testing.expectEqualStrings(b64, owned.request.begin.plan.catalog_before_b64);
    try std.testing.expectEqual(digest, owned.request.begin.plan.owners[0].catalog_digest);
    try std.testing.expect(owned.prepared.candidate.reservation.catalog.next_generation > original.candidate.reservation.catalog.next_generation);
    try std.testing.expectEqual(owned.prepared.head.plan_digest, owned.observation.plan_digest);
    try std.testing.expect(owned.request.begin.plan.before.schema_json.ptr != cut.before.schema_json.ptr);
    reader.changed = true;
    try std.testing.expectError(error.GenerationPublicationChanged, build(&reader, a, std.testing.io, .{}, cut, request, plan.after.schema_json));
    reader.changed = false;
    reader.wrong_owner = true;
    try std.testing.expectError(error.GenerationPublicationChanged, build(&reader, a, std.testing.io, .{}, cut, request, plan.after.schema_json));
    reader.wrong_owner = false;
    reader.old_decoder = true;
    try std.testing.expectError(error.TableTopologyProtocolUpgradeRequired, build(&reader, a, std.testing.io, .{}, cut, request, plan.after.schema_json));
    reader.old_decoder = false;
    var foreign_cut = cut;
    foreign_cut.table_guard.target.namespace = "foreign";
    try std.testing.expectError(error.CatalogGenerationChanged, build(&reader, a, std.testing.io, .{}, foreign_cut, request, plan.after.schema_json));
    foreign_cut = cut;
    foreign_cut.authority = .raft;
    try std.testing.expectError(error.UnsupportedNativeIndexOwner, build(&reader, a, std.testing.io, .{}, foreign_cut, request, plan.after.schema_json));
}
