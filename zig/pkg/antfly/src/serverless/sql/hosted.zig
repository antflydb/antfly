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

//! Bound-client execution for hosted WASM. Only the trusted host supplies lanes,
//! source bindings and owner identity; public callers never select credentials.
const std = @import("std");
const local = @import("antfly_local_sources");
const storage = @import("objectstore");
const runtime = @import("runtime.zig");
const plan = @import("plan.zig");
const retained = @import("../../api/lake_sql_retained_cut.zig");
const lake = local.serverless_external_source_mod.lake_catalog;
const source = @import("lake_source.zig");
const A = std.mem.Allocator;
const Context = local.serverless_query_lake_read_context.Context;
const Binding = local.serverless_external_source_catalog_binding.Binding;
const Opened = local.serverless_object_store_support.OpenedObjectStore;
pub const Lane = struct { bucket: []const u8, prefix: []const u8 };
pub const Config = struct {
    state: Lane,
    exchange: Lane,
    artifacts: local.metadata_lake_index_catalog.StoreLocator,
    source_buckets: []const []const u8,
    task_memory: u64 = 8 * 1024 * 1024,
};
const Backend = struct {
    client: storage.Client,
    config: Config,
    fn openBound(self: *const Backend, a: A, binding: Binding) !Opened {
        var uri = try local.serverless_remote_uri.parseAlloc(a, binding.source_uri);
        defer switch (uri) {
            .file => |value| a.free(value),
            inline .s3, .gcs => |*value| value.deinit(a),
        };
        const location = switch (uri) {
            .file => return error.SqlExecutionUnauthorized,
            inline .s3, .gcs => |value| value,
        };
        for (self.config.source_buckets) |allowed| {
            if (std.mem.eql(u8, allowed, location.bucket)) return Opened.initWithExistingClient(a, self.client, location.bucket, location.prefix);
        }
        return error.SqlExecutionUnauthorized;
    }
    fn pin(raw: *anyopaque, a: A, table: local.sql_catalog.Table, cut: retained.Descriptor, context: Context) !void {
        const self: *Backend = @ptrCast(@alignCast(raw));
        const binding = table.external_base_source.?.binding;
        if (binding.catalog == null or binding.catalog.?.type != .managed or binding.write_policy != .iceberg_writer) return error.SqlDistributedJoinUnsupported;
        const now = @import("antfly_platform").time.realtimeNs();
        if (cut.expires_ms <= now / std.time.ns_per_ms) return error.SqlQueryExpired;
        const pins = @import("../lake_snapshot_pins.zig");
        const store: pins.Store = .{ .client = self.client, .bucket = self.config.artifacts.bucket, .prefix = try pins.namespace(a, self.config.artifacts.prefix, binding, cut.table_uuid), .context = context };
        _ = try store.acquireFor(a, cut.snapshot_id, now, @intCast((cut.expires_ms - now / std.time.ns_per_ms) * std.time.ns_per_ms));
        var opened = try self.openBound(a, binding);
        defer opened.deinit();
        var managed: lake.managed.Managed = .{ .client = opened.client, .bucket = opened.bucket, .prefix = opened.prefix, .source_uri = binding.source_uri, .context = context };
        var current = try managed.load(a);
        defer current.deinit(a);
        const root = try lake.metadata.parse(a, current.metadata_json);
        if (!std.mem.eql(u8, try lake.metadata.str(try lake.metadata.get(root, "table-uuid")), cut.table_uuid)) return error.CatalogGenerationChanged;
    }
    const Resolver = struct {
        backend: *Backend,
        cut: retained.Descriptor,
        fn open(raw: *const anyopaque, a: A, binding: Binding) anyerror!Opened {
            const self: *const Resolver = @ptrCast(@alignCast(raw));
            return self.backend.openBound(a, binding);
        }
        fn load(raw: *const anyopaque, a: A, _: Binding, _: Context) anyerror!lake.types.Table {
            const self: *const Resolver = @ptrCast(@alignCast(raw));
            return .{ .metadata_location = try a.dupe(u8, self.cut.metadata_location), .metadata_json = try a.dupe(u8, self.cut.metadata_json) };
        }
    };
    fn open(raw: *anyopaque, a: A, table: local.sql_catalog.Table, cut: retained.Descriptor, context: Context) !local.serverless_query_lake_serving.ServingSource {
        const self: *Backend = @ptrCast(@alignCast(raw));
        const resolver: Resolver = .{ .backend = self, .cut = cut };
        return local.serverless_query_lake_serving.ServingSource.openLazyCached(a, .{ .storage_mode = .relational, .external_base_source = table.external_base_source }, .{ .resolver = .{ .ptr = &resolver, .open_fn = Resolver.open }, .catalog_resolver = .{ .ptr = &resolver, .load_fn = Resolver.load } }, context, null);
    }
};
pub const Request = struct {
    op: enum { sql_submit, sql_turn, sql_execute, sql_result, sql_cancel, sql_collect },
    config: Config,
    tenant: plan.Digest,
    owner: []const u8,
    raw_plan: ?[]const u8 = null,
    query: ?@import("state.zig").Query = null,
    ticket: ?plan.Ticket = null,
    working_set_bytes: usize = 20 * 1024 * 1024,
};
const Tickets = struct {
    a: A,
    items: []const plan.Ticket = &.{},
    fn dispatch(raw: *anyopaque, items: []const plan.Ticket, _: Context) !void {
        const self: *Tickets = @ptrCast(@alignCast(raw));
        self.items = try self.a.dupe(plan.Ticket, items);
    }
};
pub fn execute(a: A, client: storage.Client, io: std.Io, context: Context, raw: []const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const result = try executeIn(arena.allocator(), client, io, context, raw);
    return a.dupe(u8, result);
}
fn executeIn(a: A, client: storage.Client, io: std.Io, context: Context, raw: []const u8) ![]u8 {
    const request = try std.json.parseFromSliceLeaky(Request, a, raw, .{ .allocate = .alloc_always });
    try request.config.artifacts.validate();
    if (request.config.artifacts.protocol != .s3 or request.config.task_memory < 1024 * 1024 or request.config.task_memory > 8 * 1024 * 1024 or request.owner.len == 0 or request.owner.len > 256) return error.InvalidSqlDeployment;
    var backend: Backend = .{ .client = client, .config = request.config };
    var object_artifacts = try @import("../artifacts/object_store.zig").ObjectStore.initWithClientOptions(a, client, request.config.artifacts.bucket, request.config.artifacts.prefix, .{ .read_only = true, .skip_bucket_probe = true });
    var artifacts = object_artifacts.artifactStore();
    defer artifacts.deinit();
    var host: source.Host = .{ .artifacts = &artifacts, .store_identity = try @import("../../api/native_retained_cut.zig").storeIdentity(a, request.config.artifacts), .io = io, .backend = .{ .ptr = &backend, .pin = Backend.pin, .open = Backend.open } };
    const service: runtime.Service = .{ .a = a, .state_store = .{ .client = client, .bucket = request.config.state.bucket, .prefix = request.config.state.prefix, .now_ms = 0 }, .exchange_store = .{ .client = client, .bucket = request.config.exchange.bucket, .prefix = request.config.exchange.prefix }, .tenant = request.tenant, .role = .query_only, .owner = request.owner, .context = context, .retention = host.retention(), .source = host.source(), .task_memory_ceiling = request.config.task_memory };
    return switch (request.op) {
        .sql_submit => blk: {
            const spec = try std.json.parseFromSliceLeaky(plan.Plan, a, request.raw_plan orelse return error.InvalidSqlDistributedPlan, .{ .allocate = .alloc_always });
            break :blk std.json.Stringify.valueAlloc(a, .{ .query = try service.submit(spec) }, .{});
        },
        .sql_turn => blk: {
            var tickets: Tickets = .{ .a = a };
            const complete = try service.turn(request.query orelse return error.InvalidSqlDistributedPlan, .{ .ptr = &tickets, .dispatch = Tickets.dispatch });
            break :blk std.json.Stringify.valueAlloc(a, .{ .complete = complete, .tickets = tickets.items }, .{});
        },
        .sql_execute => blk: {
            const ticket = request.ticket orelse return error.InvalidSqlDistributedPlan;
            try service.execute(ticket);
            const query_key = try service.state_store.key(a, ticket.query, null, 0);
            var query = (try service.state_store.load(a, query_key)) orelse return error.SqlExecutionNotFound;
            defer query.deinit(a);
            if (query.value.terminal()) break :blk std.json.Stringify.valueAlloc(a, .{ .status = query.value.status, .until_ms = @as(u64, 0) }, .{});
            const key = try service.state_store.key(a, ticket.query, ticket.stage, ticket.task);
            var value = (try service.state_store.load(a, key)) orelse return error.SqlExecutionNotFound;
            defer value.deinit(a);
            break :blk std.json.Stringify.valueAlloc(a, .{ .status = value.value.status, .until_ms = value.value.until_ms }, .{});
        },
        .sql_result => std.json.Stringify.valueAlloc(a, try service.pollResult(a, request.query orelse return error.InvalidSqlDistributedPlan), .{}),
        .sql_cancel => blk: {
            try service.requestCancel(request.query orelse return error.InvalidSqlDistributedPlan);
            break :blk a.dupe(u8, "{}");
        },
        .sql_collect => blk: {
            const collector: @import("collector.zig").Collector = .{ .state_store = service.state_store, .exchange_store = service.exchange_store };
            break :blk std.json.Stringify.valueAlloc(a, .{ .deleted = try collector.run(a, context) }, .{});
        },
    };
}
