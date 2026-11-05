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

//! Native lake adapter: retained accepted cuts and deterministic disjoint scans.
//! Authorization is re-established by the host before opening any credential.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const lake = local.serverless_external_source_mod.lake_catalog;
const serving = local.serverless_query_lake_serving;
const retained = @import("../../api/lake_sql_retained_cut.zig");
const overlay = @import("../../api/lake_sql_overlay.zig");
const ingestion = @import("../lake_ingestion.zig");
const pins = @import("../lake_snapshot_pins.zig");
const configured = @import("../configured_object_store_support.zig");
const plan = @import("plan.zig");
const runner = @import("runner.zig");
const runtime = @import("runtime.zig");
const A = std.mem.Allocator;
const Context = local.serverless_query_lake_read_context.Context;
const Datum = local.sql_scalar.Datum;
pub fn overlayBytes(pending: ingestion.Pending) !u64 {
    var bytes: u64 = 0;
    for (pending.changes) |change| bytes +|= try local.sql_operators.datumBytes(Datum.json(change.row));
    for (pending.change_blocks) |block| for (block) |change| {
        bytes +|= try local.sql_operators.datumBytes(Datum.json(change.row));
    };
    return bytes;
}
pub const Host = struct {
    artifacts: *@import("../artifacts/store.zig").ArtifactStore,
    store_identity: [32]u8,
    options: if (@import("builtin").os.tag == .freestanding) void else configured.BindingObjectStoreOpenOptions = if (@import("builtin").os.tag == .freestanding) {} else .{},
    /// Hosted implementations reopen the same retained grants using a bound
    /// object-store client, without loading native credential SDKs.
    backend: ?struct {
        ptr: *anyopaque,
        pin: *const fn (*anyopaque, A, catalog.Table, retained.Descriptor, Context) anyerror!void,
        open: *const fn (*anyopaque, A, catalog.Table, retained.Descriptor, Context) anyerror!serving.ServingSource,
    } = null,
    cache: ?*local.serverless_query_lake_serving_cache.Cache = null,
    io: std.Io,
    authorization: ?struct {
        ptr: *anyopaque,
        /// Scope is an authenticated principal/policy capability, not a table
        /// name supplied by an RPC caller. Returned metadata belongs to a.
        resolve: *const fn (*anyopaque, A, plan.Digest, u64, Context) anyerror!catalog.Table,
    } = null,
    pub fn source(self: *Host) runner.Source {
        return .{ .ptr = self, .open = open };
    }
    pub fn retention(self: *Host) runtime.Retention {
        return .{ .ptr = self, .ensure = ensure };
    }
    fn request(context: Context) local.api_operation.RequestContext {
        return .{ .deadline_ns = context.deadline_ns, .cancellation = if (context.cancellation) |token| .{ .ptr = token.ptr, .is_cancelled_fn = token.is_cancelled_fn } else .none };
    }
    pub fn fingerprint(a: A, table: catalog.Table) !plan.Digest {
        const bytes = try std.json.Stringify.valueAlloc(a, .{ table.id, table.schema_version, if (table.external_indexes) |indexes| indexes.object_generation else 0, if (table.external_indexes) |indexes| indexes.schema_json else "", table.external_base_source.?.binding }, .{});
        defer a.free(bytes);
        return lake.types.digestHex(bytes);
    }
    fn admitted(self: *Host, a: A, tenant: plan.Digest, input: plan.Input, context: Context) !struct { table: catalog.Table, cut: retained.Descriptor } {
        const now: u64 = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
        var scoped_artifacts = self.artifacts.*;
        scoped_artifacts.allocator = a;
        const cut = try retained.loadDescriptor(a, &scoped_artifacts, self.store_identity, input.cut, input.table_id, now, request(context).cancellation);
        const table = if (self.authorization) |authority|
            try authority.resolve(authority.ptr, a, tenant, input.table_id, context)
        else grant: {
            const grant = cut.authorization orelse return error.SqlExecutionUnauthorized;
            if (!std.mem.eql(u8, &grant.scope, &tenant)) return error.SqlExecutionUnauthorized;
            break :grant grant.table.view();
        };
        if (table.id != input.table_id or table.external_base_source == null or !std.mem.eql(u8, &try fingerprint(a, table), &input.schema) or cut.schema_version != table.schema_version or cut.object_generation != (if (table.external_indexes) |indexes| indexes.object_generation else 0) or !std.mem.eql(u8, cut.snapshot_id, input.snapshot) or cut.pending.lsn != input.wal_cut) return error.SqlExecutionUnauthorized;
        for (input.columns) |column| {
            const actual = try table.column(column.name);
            if (actual.type != column.type or actual.element_type != column.element_type) return error.SqlExecutionUnauthorized;
        }
        return .{ .table = table, .cut = cut };
    }
    fn pin(self: *Host, a: A, table: catalog.Table, cut: retained.Descriptor, context: Context) !void {
        if (self.backend) |backend| return backend.pin(backend.ptr, a, table, cut, context);
        if (comptime @import("builtin").os.tag == .freestanding) return error.SqlDistributedJoinUnsupported;
        var options = self.options;
        options.catalog_table_id = table.id;
        options.catalog_generation = cut.object_generation;
        const binding = table.external_base_source.?.binding;
        // Native snapshot pins cannot fence an independent external catalog.
        if (binding.catalog == null or binding.catalog.?.type != .managed or binding.write_policy != .iceberg_writer) return error.SqlDistributedJoinUnsupported;
        var queue = try ingestion.openQueue(a, binding, options);
        defer queue.deinit();
        const pin_store: pins.Store = .{ .client = queue.client, .bucket = queue.bucket, .prefix = try pins.namespace(a, queue.prefix, binding, cut.table_uuid), .context = context };
        const now = @import("antfly_platform").time.realtimeNs();
        if (cut.expires_ms <= now / std.time.ns_per_ms) return error.SqlQueryExpired;
        _ = try pin_store.acquireFor(a, cut.snapshot_id, now, @intCast((cut.expires_ms - now / std.time.ns_per_ms) * std.time.ns_per_ms));
        var current = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
        defer current.deinit(a);
        const root = try lake.metadata.parse(a, current.table.metadata_json);
        if (!std.mem.eql(u8, try lake.metadata.str(try lake.metadata.get(root, "table-uuid")), cut.table_uuid)) return error.CatalogGenerationChanged;
    }
    fn ensure(raw: *anyopaque, a: A, spec: plan.Plan, context: Context) !void {
        const self: *Host = @ptrCast(@alignCast(raw));
        inline for (.{ "left", "right" }) |side| {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const scratch = arena.allocator();
            const input = @field(spec, side);
            const admitted_ = try self.admitted(scratch, spec.tenant, input, context);
            const table = admitted_.table;
            const cut = admitted_.cut;
            if (cut.paged) |index| {
                if (spec.strategy == .bucket or (@as(u64, index.count) + input.splits - 1) / input.splits > retained.distributed_rows) return error.SqlJoinBudgetExceeded;
            }
            if (cut.expires_ms < spec.expires_ms) return error.SqlQueryExpired;
            try self.pin(scratch, table, cut, context);
        }
    }
    /// Capture once before dispatch. The capability copies all accepted WAL
    /// images, so WAL truncation cannot change a retry's overlay.
    pub fn capture(self: *Host, a: A, tenant: plan.Digest, table_id: u64, columns: []const plan.Column, splits: u32, expires_ms: u64, context: Context) !plan.Input {
        const authority = self.authorization orelse return error.SqlExecutionUnauthorized;
        const table = try authority.resolve(authority.ptr, a, tenant, table_id, context);
        if (table.external_base_source == null or splits == 0 or splits > 65536) return error.SqlDistributedJoinUnsupported;
        var options = self.options;
        options.catalog_table_id = table.id;
        options.catalog_generation = if (table.external_indexes) |indexes| indexes.object_generation else 0;
        const schema: local.storage_schema.TableSchema = .{ .storage_mode = .relational, .external_base_source = table.external_base_source };
        var pinned_source = try serving.ServingSource.openLazyCached(a, schema, options.lakeOptions(), context, self.cache);
        defer pinned_source.deinit();
        const pinned = pinned_source.catalog_metadata orelse return error.InvalidLakeMetadata;
        const root = try lake.metadata.parse(a, pinned.metadata_json);
        const coverage = try @import("../../api/lake_search_overlay.zig").snapshotCoverage(root, pinned_source.inventory.snapshot_id);
        const pending = try ingestion.pending(a, table.external_base_source.?.binding, options, context, coverage);
        const cut: retained.Descriptor = .{ .authorization = .{ .scope = tenant, .table = retained.ReadTable.from(table) }, .table_id = table.id, .schema_version = table.schema_version, .object_generation = options.catalog_generation, .expires_ms = expires_ms, .metadata_location = pinned.metadata_location, .metadata_json = pinned.metadata_json, .table_uuid = try lake.metadata.str(try lake.metadata.get(root, "table-uuid")), .snapshot_id = pinned_source.inventory.snapshot_id, .pending = pending };
        try self.pin(a, table, cut, context);
        const token = try retained.saveDistributed(a, self.artifacts, self.store_identity, self.io, cut, request(context).cancellation);
        const estimates = pinned_source.estimates() orelse blk: {
            try pinned_source.ensureInventory(context);
            var bytes: u64 = 0;
            var rows: u64 = 0;
            for (pinned_source.inventory.files) |file| {
                bytes +|= file.byte_len;
                rows +|= file.row_count;
            }
            break :blk serving.ServingSource.Estimates{ .bytes = bytes, .rows = rows };
        };
        const selected_splits = @max(splits, token.minimumSplits());
        if (selected_splits > 65536) return error.SqlJoinBudgetExceeded;
        return .{ .table_id = table.id, .schema = try fingerprint(a, table), .snapshot = try a.dupe(u8, cut.snapshot_id), .wal_cut = pending.lsn, .cut = token.token, .columns = columns, .estimated_bytes = estimates.bytes +| token.bytes, .estimated_rows = estimates.rows +| token.rows, .splits = @intCast(selected_splits), .bucket = if (token.paged) null else if (try bucketLayout(a, &pinned_source, context)) |layout| blk: {
            for (columns, 0..) |column, index| if (std.mem.eql(u8, column.name, layout.column)) break :blk plan.Bucket{ .column = @intCast(index), .count = layout.count };
            break :blk null;
        } else null };
    }
    pub fn bucketLayout(a: A, pinned: *serving.ServingSource, context: Context) !?catalog.BucketLayout {
        const metadata = pinned.catalog_metadata orelse return null;
        const root = try lake.metadata.parse(a, metadata.metadata_json);
        const schema = try lake.row_commit.schema(a, root);
        // Unbucketed tables keep the lazy scan path without manifest discovery.
        if (try lake.bucket.defaultSpec(root, schema) == null) return null;
        try pinned.ensureInventory(context);
        const spec = (try lake.bucket.prove(root, schema, pinned.inventory.files)) orelse return null;
        return .{ .column = try a.dupe(u8, spec.source), .count = spec.count };
    }
    fn openNative(self: *Host, a: A, table: catalog.Table, cut: retained.Descriptor, context: Context) !serving.ServingSource {
        var options = self.options;
        options.catalog_table_id = table.id;
        options.catalog_generation = cut.object_generation;
        options.retained_catalog_metadata = .{ .metadata_location = @constCast(cut.metadata_location), .metadata_json = @constCast(cut.metadata_json) };
        const schema: local.storage_schema.TableSchema = .{ .storage_mode = .relational, .external_base_source = table.external_base_source };
        return serving.ServingSource.openLazyCached(a, schema, options.lakeOptions(), context, self.cache);
    }
    fn open(raw: *anyopaque, a: A, tenant: plan.Digest, input: plan.Input, split: u32, filter: ?*const local.sql_dynamic_filter.Filter, context: Context) !runner.Stream {
        const self: *Host = @ptrCast(@alignCast(raw));
        const owner = try a.create(Owner);
        errdefer a.destroy(owner);
        owner.* = .{ .a = a, .arena = .init(a), .input = input, .filter = filter, .context = context };
        errdefer owner.arena.deinit();
        const scratch = owner.arena.allocator();
        const admitted_ = try self.admitted(scratch, tenant, input, context);
        const table = admitted_.table;
        const cut = admitted_.cut;
        try self.pin(scratch, table, cut, context);
        owner.source = if (self.backend) |backend|
            try backend.open(backend.ptr, a, table, cut, context)
        else if (comptime @import("builtin").os.tag == .freestanding)
            return error.SqlDistributedJoinUnsupported
        else
            try self.openNative(a, table, cut, context);
        errdefer owner.source.deinit();
        if (!std.mem.eql(u8, owner.source.inventory.snapshot_id, input.snapshot)) return error.CatalogGenerationChanged;
        const names = try scratch.alloc([]const u8, input.columns.len);
        for (input.columns, names) |column, *name| name.* = column.name;
        const scan: catalog.Scan = .{ .fields = names, .limit = 256 };
        const delta = try scratch.create(overlay.Cut);
        delta.* = try overlay.Cut.initForTable(scratch, cut.pending, table);
        if (cut.paged != null) owner.paged = try PagedReader.init(a, self.artifacts, self.store_identity, cut, delta, input, split, context);
        errdefer if (owner.paged) |*paged| paged.deinit();
        if (owner.paged) |paged| owner.recent_at = paged.begin;
        var initial_scan = scan;
        if (cut.paged != null) initial_scan.fields = cut.pending.key_fields;
        const physical = try overlay.physicalRequest(scratch, initial_scan, delta.*);
        const child = if (input.bucket_group) |group| blk: {
            const declared = input.bucket orelse return error.InvalidSqlDistributedPlan;
            if (declared.column >= input.columns.len) return error.InvalidSqlDistributedPlan;
            const layout = (try bucketLayout(scratch, &owner.source, context)) orelse return error.SqlExecutionUnauthorized;
            if (layout.count != declared.count or !std.mem.eql(u8, layout.column, input.columns[declared.column].name)) return error.SqlExecutionUnauthorized;
            break :blk try local.sql_lake_cursor.openPinnedBucket(a, table, physical, request(context), &owner.source, .{ .source = layout.column, .count = layout.count, .index = group.index, .groups = group.count });
        } else try local.sql_lake_cursor.openPinnedPartition(a, table, physical, request(context), &owner.source, split, input.splits);
        errdefer child.close(child.ptr);
        // Install a safe filter on the base and apply it again to post-overlay
        // rows. Changed-key suppression sees the entire cut on every split.
        if (cut.paged == null) {
            if (filter) |value| {
                if (child.set_dynamic_filter) |install| _ = try install(child.ptr, value);
            }
        }
        if (child.next_columns == null) return error.SqlDistributedJoinUnsupported;
        owner.cursor = child;
        owner.table = table;
        owner.cut = delta;
        owner.split = split;
        return .{ .ptr = owner, .next_fn = Owner.next, .next_batch_fn = Owner.nextBatch, .close_fn = Owner.close };
    }
};
const Owner = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    input: plan.Input,
    source: serving.ServingSource = undefined,
    cursor: catalog.Cursor = undefined,
    filter: ?*const local.sql_dynamic_filter.Filter,
    context: Context,
    table: catalog.Table = undefined,
    cut: *const overlay.Cut = undefined,
    split: u32 = 0,
    base_done: bool = false,
    recent_at: usize = 0,
    paged: ?PagedReader = null,
    cursor_closed: bool = false,
    base_suppressed: []bool = &.{},
    base_at: usize = 0,
    base_prepared: bool = false,
    fn recentEnd(self: *const Owner) usize {
        return if (self.paged) |paged| paged.end else self.cut.pending.changes.len;
    }
    fn nextBatch(raw: *anyopaque, a: A, maximum: usize) !?local.sql_execution_batch.Batch {
        const self: *Owner = @ptrCast(@alignCast(raw));
        // WAL reconciliation and bucket/filter evaluation retain their scalar
        // fallback. Clean retained base scans borrow the physical column page.
        if (self.paged != null or self.cut.changed.count() != 0 or self.cut.pending.changes.len != 0 or self.input.bucket_group != null or self.filter != null)
            return .{ .rows = (try next(raw, a, maximum)) orelse return null };
        if (maximum == 0) return error.InvalidSqlBackendResponse;
        while (!self.base_done) {
            try self.context.ensureActive();
            const page = try self.cursor.next_columns.?(self.cursor.ptr, a, @intCast(maximum));
            try page.validate();
            if (page.selection.len > maximum) return error.InvalidSqlBackendResponse;
            self.base_done = page.after == null;
            if (page.selection.len == 0) continue;
            const definitions = try a.alloc(local.sql_scalar.Column, self.input.columns.len);
            for (self.input.columns, definitions) |column, *definition| {
                definition.* = .{ .name = (try self.table.column(column.name)).path, .type = column.type, .element_type = column.element_type };
            }
            return .{ .columns = .{ .page = page, .definitions = definitions } };
        }
        return null;
    }
    fn next(raw: *anyopaque, a: A, maximum: usize) !?[]const []const Datum {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (maximum == 0) return error.InvalidSqlBackendResponse;
        if (self.paged != null) return self.nextPaged(a, maximum);
        while (!self.base_done or self.recent_at < self.recentEnd()) {
            try self.context.ensureActive();
            // Scratch is reclaimed on every pull, including entirely filtered
            // pages. Only selected typed cells survive in the caller's page.
            var scratch_arena = std.heap.ArenaAllocator.init(self.a);
            defer scratch_arena.deinit();
            const scratch = scratch_arena.allocator();
            var result: std.ArrayList([]const Datum) = .empty;
            var page: ?catalog.ColumnPage = null;
            if (!self.base_done) {
                page = try self.cursor.next_columns.?(self.cursor.ptr, scratch, @intCast(maximum));
                try page.?.validate();
                if (page.?.selection.len > maximum) return error.InvalidSqlBackendResponse;
                self.base_done = page.?.after == null;
            }
            var row_at: usize = 0;
            while (result.items.len < maximum) {
                try self.context.ensureActive();
                const cells = try scratch.alloc(Datum, self.input.columns.len);
                if (page != null and row_at < page.?.selection.len) {
                    const index = row_at;
                    row_at += 1;
                    if (self.cut.changed.count() != 0 or self.paged != null) {
                        var key_row: std.json.Value = .{ .object = .empty };
                        for (self.cut.pending.key_fields) |key| try key_row.object.put(scratch, key, (try page.?.cell(scratch, index, (try self.table.column(key)).path)).value);
                        if (try overlay.suppressesBase(scratch, self.cut, key_row)) continue;
                    }
                    for (self.input.columns, cells) |column, *cell| cell.* = try column.datum(scratch, try page.?.typedCell(scratch, index, (try self.table.column(column.name)).path, column.element_type == .numeric));
                } else {
                    if (!self.base_done or self.recent_at == self.recentEnd()) break;
                    const position = self.recent_at;
                    self.recent_at += 1;
                    if (self.paged == null and self.input.bucket_group == null and position % self.input.splits != self.split) continue;
                    const row = (if (self.paged) |*paged| try paged.recent(scratch, position) else try overlay.recentRow(scratch, self.cut, position)) orelse continue;
                    for (self.input.columns, cells) |column, *cell| cell.* = try column.datum(scratch, try row.cell(column.name));
                }
                if (self.input.bucket_group) |group| {
                    const layout = self.input.bucket.?;
                    const cell = cells[layout.column];
                    const hash: u32 = switch (cell.value) {
                        .null => 0,
                        .integer => |value| lake.bucket.hashInteger(value) & 0x7fffffff,
                        .string => |value| lake.bucket.hashBytes(value) & 0x7fffffff,
                        else => return error.InvalidSqlBackendResponse,
                    };
                    if (hash % group.count != group.index) continue;
                }
                if (self.filter) |filter| {
                    var keys: [32]Datum = undefined;
                    for (filter.columns, 0..) |column, index| {
                        const ordinal = for (self.input.columns, 0..) |candidate, i| {
                            if (std.mem.eql(u8, candidate.name, column.name)) break i;
                        } else return error.InvalidSqlDistributedPlan;
                        keys[index] = cells[ordinal];
                    }
                    if (!try filter.contains(keys[0..filter.columns.len])) continue;
                }
                const owned = try a.alloc(Datum, cells.len);
                for (cells, owned) |cell, *out| out.* = try local.sql_operators.cloneDatum(a, cell);
                try result.append(a, owned);
            }
            if (result.items.len != 0) return result.items;
        }
        return null;
    }
    fn prepareBase(self: *Owner) !void {
        var work = std.heap.ArenaAllocator.init(self.a);
        defer work.deinit();
        var lookups: std.ArrayList(PagedReader.Lookup) = .empty;
        var page_arena = std.heap.ArenaAllocator.init(self.a);
        defer page_arena.deinit();
        // This cursor projects only stable keys. Do not fetch, coerce, or retain
        // payloads until the accepted WAL has resolved visibility.
        while (!self.base_done) {
            try self.context.ensureActive();
            _ = page_arena.reset(.free_all);
            const scratch = page_arena.allocator();
            const page = try self.cursor.next_columns.?(self.cursor.ptr, scratch, 256);
            try page.validate();
            if (page.selection.len > 256) return error.InvalidSqlBackendResponse;
            self.base_done = page.after == null;
            for (0..page.selection.len) |index| {
                var key_row: std.json.Value = .{ .object = .empty };
                for (self.cut.pending.key_fields) |key| try key_row.object.put(scratch, key, (try page.cell(scratch, index, (try self.table.column(key)).path)).value);
                const key = lake.types.digestHex(try overlay.baseIdentity(scratch, self.cut.pending.key_fields, key_row, self.cut.kinds));
                try lookups.append(work.allocator(), .{ .key = key, .row = lookups.items.len });
            }
        }
        self.cursor.close(self.cursor.ptr);
        self.cursor_closed = true;
        self.base_suppressed = try self.a.alloc(bool, lookups.items.len);
        @memset(self.base_suppressed, false);
        try self.paged.?.suppressBatch(lookups.items, self.base_suppressed);
        const all_suppressed = for (self.base_suppressed) |hidden| {
            if (!hidden) break false;
        } else true;
        if (all_suppressed) {
            self.a.free(self.base_suppressed);
            self.base_suppressed = &.{};
        } else {
            var arena = std.heap.ArenaAllocator.init(self.a);
            defer arena.deinit();
            const scratch = arena.allocator();
            const names = try scratch.alloc([]const u8, self.input.columns.len);
            for (self.input.columns, names) |column, *name| name.* = column.name;
            const scan = try overlay.physicalRequest(scratch, .{ .fields = names, .limit = 256 }, self.cut.*);
            // Both passes use the same immutable, unfiltered split. A dynamic
            // filter must not change ordinal alignment with the bitmap.
            self.cursor = try local.sql_lake_cursor.openPinnedPartition(self.a, self.table, scan, Host.request(self.context), &self.source, self.split, self.input.splits);
            self.cursor_closed = false;
            self.base_done = false;
        }
        self.base_prepared = true;
    }
    fn appendSelected(self: *Owner, a: A, rows: *std.ArrayList([]const Datum), cells: []const Datum) !void {
        if (self.filter) |filter| {
            var keys: [32]Datum = undefined;
            for (filter.columns, 0..) |column, index| {
                const ordinal = for (self.input.columns, 0..) |candidate, i| {
                    if (std.mem.eql(u8, candidate.name, column.name)) break i;
                } else return error.InvalidSqlDistributedPlan;
                keys[index] = cells[ordinal];
            }
            if (!try filter.contains(keys[0..filter.columns.len])) return;
        }
        const values = try a.alloc(Datum, cells.len);
        for (cells, values) |cell, *value| value.* = try local.sql_operators.cloneDatum(a, cell);
        try rows.append(a, values);
    }
    fn nextPaged(self: *Owner, a: A, maximum: usize) !?[]const []const Datum {
        if (!self.base_prepared) try self.prepareBase();
        var scratch_arena = std.heap.ArenaAllocator.init(self.a);
        defer scratch_arena.deinit();
        var result: std.ArrayList([]const Datum) = .empty;
        while (result.items.len < maximum) {
            try self.context.ensureActive();
            _ = scratch_arena.reset(.free_all);
            const scratch = scratch_arena.allocator();
            if (!self.base_done) {
                const page = try self.cursor.next_columns.?(self.cursor.ptr, scratch, @intCast(maximum - result.items.len));
                try page.validate();
                if (page.selection.len > maximum - result.items.len) return error.InvalidSqlBackendResponse;
                self.base_done = page.after == null;
                for (0..page.selection.len) |index| {
                    if (self.base_at >= self.base_suppressed.len) return error.InvalidSqlBackendResponse;
                    const hidden = self.base_suppressed[self.base_at];
                    self.base_at += 1;
                    if (hidden) continue;
                    const cells = try scratch.alloc(Datum, self.input.columns.len);
                    for (self.input.columns, cells) |column, *cell| cell.* = try column.datum(scratch, try page.typedCell(scratch, index, (try self.table.column(column.name)).path, column.element_type == .numeric));
                    try self.appendSelected(a, &result, cells);
                }
                if (self.base_done) {
                    if (self.base_at != self.base_suppressed.len) return error.InvalidSqlBackendResponse;
                    self.a.free(self.base_suppressed);
                    self.base_suppressed = &.{};
                    self.cursor.close(self.cursor.ptr);
                    self.cursor_closed = true;
                }
                continue;
            }
            if (self.recent_at == self.recentEnd()) break;
            const position = self.recent_at;
            self.recent_at += 1;
            const row = (try self.paged.?.recent(scratch, position)) orelse continue;
            const values = try scratch.alloc(Datum, self.input.columns.len);
            for (self.input.columns, values) |column, *cell| cell.* = try column.datum(scratch, try row.cell(column.name));
            try self.appendSelected(a, &result, values);
        }
        return if (result.items.len == 0) null else result.items;
    }
    fn close(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.a.free(self.base_suppressed);
        if (self.paged) |*paged| paged.deinit();
        if (!self.cursor_closed) self.cursor.close(self.cursor.ptr);
        self.source.deinit();
        self.arena.deinit();
        self.a.destroy(self);
    }
};

const PagedReader = struct {
    const Lookup = struct { key: [64]u8, row: usize };
    a: A,
    artifacts: *@import("../artifacts/store.zig").ArtifactStore,
    identity: [32]u8,
    descriptor: retained.Descriptor,
    cut: *const overlay.Cut,
    context: Context,

    change_arena: std.heap.ArenaAllocator,
    bloom: []const u8,
    changes: []const ingestion.Batch.Change = &.{},
    change_page: ?usize = null,
    begin: usize,
    end: usize,
    fn init(a: A, artifacts: *@import("../artifacts/store.zig").ArtifactStore, identity_: [32]u8, descriptor: retained.Descriptor, cut: *const overlay.Cut, input: plan.Input, split: u32, context: Context) !PagedReader {
        const index = descriptor.paged.?;
        if (input.bucket_group != null) return error.SqlDistributedJoinUnsupported;
        if ((@as(u64, index.count) + input.splits - 1) / input.splits > retained.distributed_rows) return error.SqlJoinBudgetExceeded;
        const bloom = try retained.readPage(a, artifacts, identity_, descriptor, index.bloom, requestToken(&context));
        errdefer a.free(bloom);
        if (bloom.len == 0) return error.InvalidSqlExchange;
        return .{ .a = a, .artifacts = artifacts, .identity = identity_, .descriptor = descriptor, .cut = cut, .context = context, .change_arena = .init(a), .bloom = bloom, .begin = @intCast(@as(u64, index.count) * split / input.splits), .end = @intCast(@as(u64, index.count) * (split + 1) / input.splits) };
    }
    fn requestToken(context: *const Context) @import("antfly_cancellation").CancellationToken {
        return .{ .ptr = context, .is_cancelled_fn = struct {
            fn canceled(raw: *const anyopaque) bool {
                const value: *const Context = @ptrCast(@alignCast(raw));
                value.ensureActive() catch return true;
                return false;
            }
        }.canceled };
    }
    /// Sort one split's candidate keys so every relevant immutable page is
    /// fetched once, independently of physical row order or key skew.
    fn suppressBatch(self: *PagedReader, lookups: []Lookup, suppressed: []bool) !void {
        std.mem.sort(Lookup, lookups, {}, struct {
            fn less(_: void, l: Lookup, r: Lookup) bool {
                return std.mem.order(u8, &l.key, &r.key) == .lt;
            }
        }.less);
        const index = self.descriptor.paged.?;
        const width: usize = index.key_bytes;
        if (width != 32 and width != 64) return error.InvalidSqlExchange;
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        var cached: ?usize = null;
        var bytes: []const u8 = &.{};
        for (lookups) |lookup| {
            try self.context.ensureActive();
            if (lookup.row >= suppressed.len) return error.InvalidSqlExchange;
            var possible = true;
            for (0..4) |seed| {
                const bit = retained.bloomBit(lookup.key, self.bloom.len * 8, seed);
                if (self.bloom[bit / 8] & (@as(u8, 1) << @as(u3, @intCast(bit % 8))) == 0) {
                    possible = false;
                    break;
                }
            }
            if (!possible) continue;
            var low: usize = 0;
            var high = index.keys.len;
            while (low < high) {
                const middle = low + (high - low) / 2;
                if (std.mem.order(u8, &index.keys[middle].last, &lookup.key) == .lt) low = middle + 1 else high = middle;
            }
            if (low == index.keys.len or std.mem.order(u8, &lookup.key, &index.keys[low].first) == .lt) continue;
            if (cached != low) {
                _ = arena.reset(.free_all);
                bytes = try retained.readPage(arena.allocator(), self.artifacts, self.identity, self.descriptor, index.keys[low].token, requestToken(&self.context));
                if (bytes.len == 0 or bytes.len % width != 0) return error.InvalidSqlExchange;
                cached = low;
            }
            var binary: [32]u8 = undefined;
            const key: []const u8 = if (width == 32) try std.fmt.hexToBytes(&binary, &lookup.key) else &lookup.key;
            var begin: usize = 0;
            var end = bytes.len / width;
            while (begin < end) {
                const middle = begin + (end - begin) / 2;
                switch (std.mem.order(u8, bytes[middle * width ..][0..width], key)) {
                    .eq => {
                        suppressed[lookup.row] = true;
                        break;
                    },
                    .lt => begin = middle + 1,
                    .gt => end = middle,
                }
            }
        }
    }
    fn recent(self: *PagedReader, a: A, position: usize) !?catalog.Row {
        const pages = self.descriptor.paged.?.changes;
        var low: usize = 0;
        var high = pages.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            if (position >= @as(u64, pages[middle].begin) + pages[middle].count) low = middle + 1 else high = middle;
        }
        const index = low;
        if (index == pages.len or position < pages[index].begin) return error.InvalidSqlExchange;
        if (self.change_page == null or self.change_page.? != index) {
            _ = self.change_arena.reset(.free_all);
            const alloc = self.change_arena.allocator();
            const bytes = try retained.readPage(alloc, self.artifacts, self.identity, self.descriptor, pages[index].token, requestToken(&self.context));
            self.changes = try std.json.parseFromSliceLeaky([]const ingestion.Batch.Change, alloc, bytes, .{ .allocate = .alloc_always });
            if (self.changes.len != pages[index].count) return error.InvalidSqlExchange;
            self.change_page = index;
        }
        var cut = self.cut.*;
        cut.pending.changes = self.changes;
        return overlay.recentRow(a, &cut, position - pages[index].begin);
    }
    fn deinit(self: *PagedReader) void {
        self.a.free(self.bloom);
        self.change_arena.deinit();
    }
};

test "distributed SQL clustered WAL keys read each exact page once" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    var objects = try @import("../artifacts/object_store.zig").ObjectStore.initWithClientOptions(a, memory.client(), "source", "artifacts", .{ .skip_bucket_probe = true });
    var artifacts = objects.artifactStore();
    defer artifacts.deinit();
    const binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(sa, "{\"base_source\":{\"kind\":\"external\",\"table_id\":\"t\",\"format\":\"iceberg\",\"uri\":\"s3://source/table\",\"schema_fingerprint\":\"auto\",\"write_policy\":\"iceberg_writer\",\"catalog\":{\"type\":\"managed\"}}}")).?;
    const table: catalog.Table = .{ .id = 7, .physical_name = "t", .schema_version = 1, .external_base_source = binding, .columns = &.{.{ .name = "id", .path = "id", .type = .integer }} };
    const changes = try sa.alloc(ingestion.Batch.Change, 100000);
    for (changes, 1..) |*change, id| {
        var row: std.json.Value = .{ .object = .empty };
        try row.object.put(sa, "id", .{ .integer = @intCast(id) });
        change.* = .{ .op = .upsert, .row = row };
    }
    const descriptor: retained.Descriptor = .{ .table_id = 7, .schema_version = 1, .object_generation = 0, .expires_ms = 1000000, .metadata_location = "s3://source/table/meta.json", .metadata_json = "{}", .table_uuid = "u", .snapshot_id = "42", .pending = .{ .lsn = 1, .key_fields = &.{"id"}, .changes = changes }, .authorization = .{ .scope = @splat('a'), .table = retained.ReadTable.from(table) } };
    const identity_: [32]u8 = @splat(1);
    const saved = try retained.saveDistributed(sa, &artifacts, identity_, std.testing.io, descriptor, .none);
    const loaded = try retained.loadDescriptor(sa, &artifacts, identity_, saved.token, 7, 1, .none);
    const cut = try overlay.Cut.initForTable(sa, loaded.pending, table);
    const input: plan.Input = .{ .table_id = 7, .schema = @splat('a'), .snapshot = "42", .wal_cut = 1, .cut = saved.token, .columns = &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = 20 * 1024 * 1024, .estimated_rows = 1200000, .splits = saved.minimumSplits() };
    var reader = try PagedReader.init(a, &artifacts, identity_, loaded, &cut, input, 0, .{});
    defer reader.deinit();
    const count = 1601;
    const lookups = try sa.alloc(PagedReader.Lookup, count);
    const suppressed = try sa.alloc(bool, count);
    for (lookups, 0..) |*lookup, index| {
        var row: std.json.Value = .{ .object = .empty };
        try row.object.put(sa, "id", .{ .integer = if (index < 1500) @intCast(index + 1) else if (index == 1600) 1 else @intCast(index + 100000) });
        lookup.* = .{ .key = lake.types.digestHex(try overlay.baseIdentity(sa, cut.pending.key_fields, row, cut.kinds)), .row = index };
    }
    @memset(suppressed, false);
    const metrics = @import("objectstore").operation_metrics;
    const before = metrics.runtime.snapshot();
    try reader.suppressBatch(lookups, suppressed);
    const after = metrics.runtime.snapshot();
    const gets = after.calls[@backingInt(metrics.Operation.get)] - before.calls[@backingInt(metrics.Operation.get)];
    try std.testing.expectEqual(@as(usize, 25), loaded.paged.?.keys.len);
    try std.testing.expect(gets <= loaded.paged.?.keys.len);
    for (suppressed, 0..) |value, index| try std.testing.expectEqual(index < 1500 or index == 1600, value);
    // Hidden wide payloads must never be copied during key preparation.
    const PageFixture = struct {
        batch: local.sql_execution_batch.Batch,
        selection: [128]usize,
        calls: usize = 0,
        fn columns(raw: *anyopaque, _: A, _: u32) !catalog.ColumnPage {
            const fixture: *@This() = @ptrCast(@alignCast(raw));
            fixture.calls += 1;
            return .{ .native = .{ .values = &fixture.batch, .names = &.{ "id", "payload" } }, .selection = &fixture.selection };
        }
        fn next(_: *anyopaque, _: A, _: u32) !catalog.Page {
            return error.UnexpectedRowPath;
        }
        fn close(_: *anyopaque) void {}
    };
    const payload = try sa.alloc(u8, 64 * 1024);
    @memset(payload, 'x');
    const base = try sa.alloc([]const Datum, 128);
    for (base, 1..) |*row, id| row.* = try sa.dupe(Datum, &.{ Datum.json(.{ .integer = @intCast(id) }), Datum.json(.{ .string = payload }) });
    var page_fixture: PageFixture = .{ .batch = .{ .rows = base }, .selection = undefined };
    for (&page_fixture.selection, 0..) |*index, i| index.* = i;
    var budget: local.sql_memory_budget = .{ .backing = a, .limit = 8 * 1024 * 1024 };
    defer std.debug.assert(budget.live == 0);
    var wide_input = input;
    wide_input.columns = &.{ .{ .name = "id", .type = .integer }, .{ .name = "payload", .type = .string } };
    var wide_table = table;
    wide_table.columns = &.{ .{ .name = "id", .path = "id", .type = .integer }, .{ .name = "payload", .path = "payload", .type = .string } };
    var owner: Owner = .{ .a = budget.allocator(), .arena = .init(budget.allocator()), .input = wide_input, .table = wide_table, .cut = &cut, .filter = null, .context = .{}, .cursor = .{ .ptr = &page_fixture, .next = PageFixture.next, .next_columns = PageFixture.columns, .close = PageFixture.close }, .paged = try PagedReader.init(budget.allocator(), &artifacts, identity_, loaded, &cut, input, 0, .{}) };
    defer owner.arena.deinit();
    defer owner.paged.?.deinit();
    defer owner.a.free(owner.base_suppressed);
    try owner.prepareBase();
    try std.testing.expect(owner.base_done and owner.cursor_closed);
    try std.testing.expectEqual(@as(usize, 0), owner.base_suppressed.len);
    try std.testing.expectEqual(@as(usize, 1), page_fixture.calls);
    // Retained capabilities from older writers contain ASCII digest pages.
    var legacy = loaded;
    const legacy_pages = try sa.dupe(retained.KeyPage, loaded.paged.?.keys);
    for (legacy_pages) |*page| {
        const binary = try retained.readPage(sa, &artifacts, identity_, loaded, page.token, .none);
        const hex = try sa.alloc(u8, binary.len * 2);
        for (0..binary.len / 32) |i| @memcpy(hex[i * 64 ..][0..64], &std.fmt.bytesToHex(binary[i * 32 ..][0..32].*, .lower));
        const envelope = try @import("../../api/lake_generation_envelope.zig").decode(sa, "sql-page1:", page.token, retained.max_bytes);
        const scope = (try @import("../artifacts/store.zig").uploadScopeFromArtifactId(envelope.id)).?;
        var object = try artifacts.putScoped(scope, hex, .none);
        defer object.deinit(artifacts.allocator);
        page.token = try @import("../../api/lake_generation_envelope.zig").encode(sa, "sql-page1:", object);
    }
    legacy.paged.?.keys = legacy_pages;
    legacy.paged.?.key_bytes = 64;
    var legacy_reader = try PagedReader.init(a, &artifacts, identity_, legacy, &cut, input, 0, .{});
    defer legacy_reader.deinit();
    @memset(suppressed, false);
    try legacy_reader.suppressBatch(lookups, suppressed);
    for (suppressed, 0..) |value, index| try std.testing.expectEqual(index < 1500 or index == 1600, value);
}
