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
        const physical = try overlay.physicalRequest(scratch, scan, delta.*);
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
        if (filter) |value| {
            if (child.set_dynamic_filter) |install| _ = try install(child.ptr, value);
        }
        if (child.next_columns == null) return error.SqlDistributedJoinUnsupported;
        owner.cursor = child;
        owner.table = table;
        owner.cut = delta;
        owner.split = split;
        return .{ .ptr = owner, .next_fn = Owner.next, .close_fn = Owner.close };
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
    fn recentEnd(self: *const Owner) usize {
        return if (self.paged) |paged| paged.end else self.cut.pending.changes.len;
    }
    fn next(raw: *anyopaque, a: A, maximum: usize) !?[]const []const Datum {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (maximum == 0) return error.InvalidSqlBackendResponse;
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
                        if (if (self.paged) |*paged| try paged.suppresses(scratch, key_row) else try overlay.suppressesBase(scratch, self.cut, key_row)) continue;
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
    fn close(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (self.paged) |*paged| paged.deinit();
        self.cursor.close(self.cursor.ptr);
        self.source.deinit();
        self.arena.deinit();
        self.a.destroy(self);
    }
};

const PagedReader = struct {
    const KeyCache = struct { arena: std.heap.ArenaAllocator, page: ?usize = null, bytes: []const u8 = &.{}, used: u64 = 0 };
    a: A,
    artifacts: *@import("../artifacts/store.zig").ArtifactStore,
    identity: [32]u8,
    descriptor: retained.Descriptor,
    cut: *const overlay.Cut,
    context: Context,
    key_cache: [8]KeyCache,
    key_clock: u64 = 0,
    change_arena: std.heap.ArenaAllocator,
    bloom: []const u8,
    keys: []const u8 = &.{},
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
        return .{ .a = a, .artifacts = artifacts, .identity = identity_, .descriptor = descriptor, .cut = cut, .context = context, .key_cache = blk: {
            var slots: [8]KeyCache = undefined;
            for (&slots) |*slot| slot.* = .{ .arena = .init(a) };
            break :blk slots;
        }, .change_arena = .init(a), .bloom = bloom, .begin = @intCast(@as(u64, index.count) * split / input.splits), .end = @intCast(@as(u64, index.count) * (split + 1) / input.splits) };
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
    fn suppresses(self: *PagedReader, a: A, row: std.json.Value) !bool {
        const key = lake.types.digestHex(try overlay.baseIdentity(a, self.cut.pending.key_fields, row, self.cut.kinds));
        for (0..4) |seed| {
            const bit = retained.bloomBit(key, self.bloom.len * 8, seed);
            if (self.bloom[bit / 8] & (@as(u8, 1) << @as(u3, @intCast(bit % 8))) == 0) return false;
        }
        const pages = self.descriptor.paged.?.keys;
        var low: usize = 0;
        var high = pages.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            if (std.mem.order(u8, &pages[middle].last, &key) == .lt) low = middle + 1 else high = middle;
        }
        if (low == pages.len or std.mem.order(u8, &key, &pages[low].first) == .lt) return false;
        const cached = for (&self.key_cache) |*slot| {
            if (slot.page == low) break slot;
        } else null;
        const slot = cached orelse blk: {
            var victim = &self.key_cache[0];
            for (&self.key_cache) |*candidate| {
                if (candidate.page == null) {
                    victim = candidate;
                    break;
                }
                if (candidate.used < victim.used) victim = candidate;
            }
            _ = victim.arena.reset(.free_all);
            victim.page = null;
            victim.bytes = try retained.readPage(victim.arena.allocator(), self.artifacts, self.identity, self.descriptor, pages[low].token, requestToken(&self.context));
            if (victim.bytes.len % 64 != 0) return error.InvalidSqlExchange;
            victim.page = low;
            break :blk victim;
        };
        self.key_clock += 1;
        slot.used = self.key_clock;
        self.keys = slot.bytes;
        var begin: usize = 0;
        var end = self.keys.len / 64;
        while (begin < end) {
            const middle = begin + (end - begin) / 2;
            switch (std.mem.order(u8, self.keys[middle * 64 ..][0..64], &key)) {
                .eq => return true,
                .lt => begin = middle + 1,
                .gt => end = middle,
            }
        }
        return false;
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
        for (&self.key_cache) |*slot| slot.arena.deinit();
        self.change_arena.deinit();
    }
};
