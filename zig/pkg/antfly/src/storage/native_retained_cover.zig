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

//! A current, catalog-fenced carrier executes an immutable original range
//! cover. Logical group IDs remain distinct even when every generation moves
//! to one owner. Existing distributed rank/aggregation/graph merge is reused.
const std = @import("std");
const root = @import("antfly_source_root").antfly_sources;
const db_mod = root.selected_db;
const reads = @import("../api/table_reads.zig");
const catalog = @import("../api/table_catalog.zig");
const metadata = @import("../metadata/api.zig");
const records = @import("../metadata/table_manager.zig");
const query = @import("antfly_local_sources").api_query;
const Cut = @typeInfo(@FieldType(db_mod.types.SearchRequest, "native_query_cut")).optional.child;
const A = std.mem.Allocator;

pub const Owner = struct {
    alloc: A,
    carrier: *db_mod.DB,
    cut: Cut,
    table: [1]records.TableRecord,
    ranges: []records.RangeRecord,
    dbs: []?db_mod.DB,
    cancellation: db_mod.types.CancellationToken,

    pub fn init(a: A, carrier: *db_mod.DB, table_name: []const u8, cut: Cut, cancellation: db_mod.types.CancellationToken) !Owner {
        try cut.validate(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
        if (cut.create or cut.cover.len == 0 or carrier.core.identity_namespace.table_id != cut.table_id) return error.CatalogGenerationChanged;
        const recipe = cut.recipe orelse return error.CatalogGenerationChanged;
        const ranges = try a.alloc(records.RangeRecord, cut.cover.len);
        errdefer a.free(ranges);
        const dbs = try a.alloc(?db_mod.DB, cut.cover.len);
        @memset(dbs, null);
        for (ranges, cut.cover) |*range, origin| range.* = .{ .table_id = cut.table_id, .group_id = origin.group_id, .range_id = origin.namespace.range_id, .doc_identity_shard_id = origin.namespace.shard_id, .doc_identity_range_id = origin.namespace.range_id, .start_key = origin.start_key, .end_key = origin.end_key };
        return .{ .alloc = a, .carrier = carrier, .cut = cut, .table = .{.{ .table_id = cut.table_id, .name = table_name, .schema_json = recipe.schema_json, .read_schema_json = recipe.read_schema_json, .indexes_json = recipe.indexes_json }}, .ranges = ranges, .dbs = dbs, .cancellation = cancellation };
    }
    pub fn deinit(self: *Owner) void {
        for (self.dbs) |*db| if (db.*) |*opened| opened.close();
        self.alloc.free(self.dbs);
        self.alloc.free(self.ranges);
        self.* = undefined;
    }
    fn routing(ptr: *anyopaque, _: ?u64) !metadata.CatalogRoutingSnapshot {
        const self: *Owner = @ptrCast(@alignCast(ptr));
        try self.cancellation.check();
        return .{ .tables = &self.table, .ranges = self.ranges };
    }
    fn freeRouting(_: *anyopaque, _: *metadata.CatalogRoutingSnapshot) void {}
    fn admin(ptr: *anyopaque) !metadata.AdminSnapshot {
        const self: *Owner = @ptrCast(@alignCast(ptr));
        return .{ .status = .{ .metadata_group_id = 0, .metrics = .{} }, .tables = &self.table, .ranges = self.ranges, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
    }
    fn freeAdmin(_: *anyopaque, _: *metadata.AdminSnapshot) void {}
    fn release(_: *anyopaque, _: A) void {}
    fn lease(ptr: *anyopaque, _: A, table_name: []const u8, group: u64, _: u64, _: reads.ResidentDbSource.LeaseOptions) !?reads.ResidentDbLease {
        const self: *Owner = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table[0].name, table_name)) return null;
        try self.cancellation.check();
        for (self.cut.cover, 0..) |origin, i| if (origin.group_id == group) {
            if (self.dbs[i] == null) {
                const cut = try self.cut.forGroup(origin.group_id);
                self.dbs[i] = try self.carrier.openQueryCut(cut, self.cancellation);
            }
            return .{ .ptr = self, .db = &self.dbs[i].?, .release_fn = release };
        };
        return null;
    }
    pub fn source(self: *Owner) reads.ProvisionedTableReadSource {
        var result = reads.ProvisionedTableReadSource.init("", .{ .ptr = self, .vtable = &.{ .admin_snapshot = admin, .free_admin_snapshot = freeAdmin, .routing_snapshot = routing, .linearizable_routing_snapshot = routing, .free_routing_snapshot = freeRouting } }, @import("../raft/read_gate.zig").alreadyReadSafeBarrier());
        result.resident_db = .{ .ptr = self, .lease_group = lease };
        return result;
    }
    pub fn preflight(self: *Owner, a: A, req: db_mod.types.SearchRequest, max_work: u32) !db_mod.RuntimePreflightSummary {
        var retained = req;
        retained.native_query_cut = null;
        // The leased DBs already are the retained generation. Forwarding the
        // public capability after removing its cut descriptor would ask those
        // DBs to open an unrelated index snapshot, especially on remote storage.
        retained.remote_snapshot = null;
        retained.identity_read_generation = null;
        var reader = self.source();
        return (try reader.source().preflightQuery(a, self.table[0].name, retained, .stale, max_work)) orelse error.CatalogGenerationChanged;
    }
    pub fn execute(self: *Owner, a: A, req: db_mod.types.SearchRequest) !query.QueryResponse {
        var retained = req;
        retained.native_query_cut = null;
        // The leased DBs already are the retained generation. Forwarding the
        // public capability after removing its cut descriptor would ask those
        // DBs to open an unrelated index snapshot, especially on remote storage.
        retained.remote_snapshot = null;
        // The outer routing slot is a carrier, not a logical retained range.
        // Its generation token cannot be compared with original range tokens.
        retained.identity_read_generation = null;
        var reader = self.source();
        var response = (try reader.source().query(a, self.table[0].name, retained, .stale)) orelse return error.CatalogGenerationChanged;
        errdefer response.deinit(a);
        if (req.remote_snapshot) |token| {
            var parsed = try std.json.parseFromSlice(std.json.Value, a, response.json, .{});
            defer parsed.deinit();
            const pa = parsed.arena.allocator();
            // QueryResponse has one result for this table. Preserve the public
            // capability even though virtual fanout does not reopen each cut.
            if (parsed.value == .object) if (parsed.value.object.getPtr("responses")) |items| if (items.* == .array) for (items.array.items) |*item| {
                if (item.* == .object) try item.object.put(pa, "remote_snapshot", .{ .string = token });
            };
            const bytes = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
            a.free(response.json);
            response.json = bytes;
        }
        return response;
    }
};
