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

//! Durable logical fragments reuse native per-file mutation/contribution paths.
//! Radix boundaries split under pressure and never depend on source file layout.
const std = @import("std");
const local = @import("antfly_local_sources");
const identity = @import("lake_row_identity.zig");
const navigation = @import("lake_source_navigation.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const Pages = @import("../serverless/graph_segment/page_store.zig").PageStore;
const stores = @import("../serverless/artifacts/store.zig");
const Provider = @import("lake_index_row_source.zig").Provider;
const rows = local.storage_rowsource_types;
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;
const max_rows = 8192;
const max_fragments = 16384;
const Fragment = struct { prefix: [32]u8, bits: u16, name: []const u8, count: u64, digest: [32]u8 };

pub const Build = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    physical: *local.serverless_query_lake_serving.ServingSource,
    source: local.serverless_query_lake_serving.ServingSource = undefined,
    provider: Provider = undefined,
    root: identity.Root,
    space: identity.Space = undefined,
    fragments: []const Fragment = &.{},
    store: stores.ArtifactStore,
    schemas: @import("sql_schema_cache.zig").Cache,
    table: local.sql_catalog.Table = undefined,
    context: local.serverless_query_lake_read_context.Context,
    token: Token,
    initialized: bool = false,
    pub fn deinit(self: *Build) void {
        if (self.initialized) self.source.closeParallelView(self.physical);
        self.schemas.deinit();
        self.arena.deinit();
        self.a.destroy(self);
    }
    fn check(raw: *anyopaque) !void {
        const self: *Build = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
        try self.token.check();
    }
    fn pageStore(self: *Build, reads: *u64, writes: *u64) Pages {
        return .{ .domain = self.root.domain, .attempt = self.store.upload_scope.?.attempt, .artifacts = &self.store, .remaining_read_bytes = reads, .remaining_write_bytes = writes, .streaming = true, .cancellation = self.token };
    }
    fn open(raw: *anyopaque, a: A, binding: local.serverless_segment_source_binding.Binding, file: ?usize, token: Token, phase: ?*const stores.IoPhase) !rows.Source {
        const self: *Build = @ptrCast(@alignCast(raw));
        const columns = try a.dupe([]const u8, binding.column_bindings);
        errdefer a.free(columns);
        var physical = try self.physical.parallelView(a);
        errdefer physical.closeParallelView(self.physical);
        const cursor = try a.create(Gather);
        cursor.* = .{ .a = a, .owner = self, .arena = .init(a), .page_arena = .init(a), .binding = binding, .file = file, .token = token, .store = self.store, .physical = physical };
        cursor.binding.column_bindings = columns;
        cursor.store.io_phase = phase;
        return .{ .kind = if (self.source.inventory.format == .iceberg) .external_iceberg else .external_parquet, .ctx = cursor, .next_batch = Gather.next, .deinit_fn = Gather.close, .retain_columns = Gather.retain };
    }
};

pub fn prepare(a: A, store: *stores.ArtifactStore, table: local.common_topology_records.TableRecord, physical: *local.serverless_query_lake_serving.ServingSource, provider: *Provider, root: identity.Root, previous: ?identity.Root) !?*Build {
    if (provider.identity_spill_bytes == 0 or root.current.rows == 0) return null;
    const self = try a.create(Build);
    self.* = .{ .a = a, .arena = .init(a), .physical = physical, .root = root, .store = store.*, .schemas = .init(a), .context = provider.context, .token = .{ .ptr = provider, .is_cancelled_fn = canceled } };
    var owned = true;
    defer if (owned) self.deinit();
    const scheduler = local.sql_parallel_scheduler.global();
    const retained = scheduler.reservePullUpTo(1, 1024 * 1024, 64 * 1024 * 1024) orelse return null;
    defer scheduler.releasePull(1, retained);
    const cpu = scheduler.cpuWorkspaceBudget(@min(retained, 8 * 1024 * 1024));
    if (cpu < 1024 * 1024) return null;
    const io = provider.context.io orelse return error.InvalidLogicalRowIdentity;
    var execution: local.sql_parallel_scheduler.Execution = .{ .scheduler = scheduler, .bytes = cpu, .checkpoint = .{ .ptr = self, .check = Build.check } };
    defer execution.pause();
    try execution.enter(io);
    var phase: stores.ExecutionIo = .{ .execution = &execution, .io = io };
    self.store.io_phase = phase.capability();
    defer self.store.io_phase = store.io_phase;
    var workspace: local.sql_memory_budget = .{ .backing = a, .limit = retained };
    prepareInner(self, workspace.allocator(), table, provider, previous) catch |err| {
        if (err == error.LogicalRowIdentityAdmissionDeclined or err == error.SqlProgramLimitExceeded or (err == error.OutOfMemory and workspace.isLocallyExhausted())) return null;
        return err;
    };
    owned = false;
    return self;
}
fn canceled(raw: *const anyopaque) bool {
    const provider: *const Provider = @ptrCast(@alignCast(raw));
    provider.context.ensureActive() catch return true;
    return false;
}
fn upperBound(prefix: [32]u8, bits: u16) ?[32]u8 {
    if (bits == 0) return null;
    var upper = prefix;
    var bit = bits;
    while (bit != 0) {
        bit -= 1;
        const mask = @as(u8, 128) >> @as(u3, @intCast(bit % 8));
        const byte = bit / 8;
        if (upper[byte] & mask == 0) {
            upper[byte] |= mask;
            return upper;
        }
        upper[byte] &= ~mask;
    }
    return null;
}
fn parseName(name: []const u8) !struct { prefix: [32]u8, bits: u16 } {
    var pieces = std.mem.splitScalar(u8, name, '-');
    if (!std.mem.eql(u8, pieces.next() orelse return error.InvalidLogicalRowIdentity, "lf1")) return error.InvalidLogicalRowIdentity;
    const bits = std.fmt.parseInt(u16, pieces.next() orelse return error.InvalidLogicalRowIdentity, 10) catch return error.InvalidLogicalRowIdentity;
    const hex = pieces.next() orelse return error.InvalidLogicalRowIdentity;
    if (bits < 4 or bits > 255 or hex.len != 64 or pieces.next() != null) return error.InvalidLogicalRowIdentity;
    var prefix: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&prefix, hex) catch return error.InvalidLogicalRowIdentity;
    for (bits..256) |bit| if (prefix[bit / 8] & (@as(u8, 128) >> @as(u3, @intCast(bit % 8))) != 0) return error.InvalidLogicalRowIdentity;
    return .{ .prefix = prefix, .bits = bits };
}
fn planRange(self: *Build, a: A, pages: *Pages, prefix: [32]u8, bits: u16, fragments: *std.ArrayList(Fragment)) anyerror!void {
    try Build.check(self);
    const upper = upperBound(prefix, bits);
    var cursor = try tree.Cursor.init(a, pages.store(), self.root.current.by_id, &prefix, if (upper) |*value| value else null);
    defer cursor.deinit();
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("native-lake-logical-fragment-v1");
    hash.update(&self.root.namespace);
    hash.update(&self.root.values_recipe);
    var count: u64 = 0;
    while (try cursor.next()) |record| {
        try Build.check(self);
        if (record.key.len != 32 or record.value.len != 56) return error.InvalidLogicalRowIdentity;
        hash.update(record.key);
        hash.update(record.value[16..48]);
        count += 1;
        if (count > max_rows) break;
    }
    if (count > max_rows) {
        if (bits == 255) return error.LogicalRowIdentityAdmissionDeclined;
        try planRange(self, a, pages, prefix, bits + 1, fragments);
        var right = prefix;
        right[bits / 8] |= @as(u8, 128) >> @as(u3, @intCast(bits % 8));
        try planRange(self, a, pages, right, bits + 1, fragments);
        return;
    }
    if (fragments.items.len == max_fragments) return error.LogicalRowIdentityAdmissionDeclined;
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    const name = try std.fmt.allocPrint(self.arena.allocator(), "lf1-{d}-{s}", .{ bits, std.fmt.bytesToHex(&prefix, .lower) });
    try fragments.append(self.arena.allocator(), .{ .prefix = prefix, .bits = bits, .name = name, .count = count, .digest = digest });
}
fn prepareInner(self: *Build, a: A, table: local.common_topology_records.TableRecord, provider: *Provider, previous: ?identity.Root) !void {
    const out = self.arena.allocator();
    var reads: u64 = 0;
    var writes: u64 = 0;
    var pages = self.pageStore(&reads, &writes);
    pages.admitPublication();
    const prior = if (previous) |old| if (std.mem.eql(u8, &old.namespace, &self.root.namespace) and std.mem.eql(u8, &old.values_recipe, &self.root.values_recipe)) old.native_space else null else null;
    var fragments: std.ArrayList(Fragment) = .empty;
    if (prior) |space| {
        var cursor = try tree.Cursor.init(a, pages.store(), space.files, "", null);
        defer cursor.deinit();
        while (try cursor.next()) |record| {
            const prefix = try parseName(record.value);
            try planRange(self, a, &pages, prefix.prefix, prefix.bits, &fragments);
        }
    } else for (0..16) |i| {
        var prefix: [32]u8 = @splat(0);
        prefix[0] = @as(u8, @intCast(i)) << 4;
        try planRange(self, a, &pages, prefix, 4, &fragments);
    }
    // Prior file slots are sparse and need not be in radix order.
    std.mem.sort(Fragment, fragments.items, {}, struct {
        fn less(_: void, left: Fragment, right: Fragment) bool {
            return std.mem.order(u8, &left.prefix, &right.prefix) == .lt;
        }
    }.less);
    var boundary: ?[32]u8 = @splat(0);
    for (fragments.items) |fragment| {
        if (boundary == null or !std.mem.eql(u8, &boundary.?, &fragment.prefix)) return error.InvalidLogicalRowIdentity;
        boundary = upperBound(fragment.prefix, fragment.bits);
    }
    if (boundary != null) return error.InvalidLogicalRowIdentity;
    self.fragments = fragments.items;
    const files = try out.alloc(local.serverless_external_source_types.FileEntry, fragments.items.len);
    const names = try out.alloc([]const u8, files.len);
    for (self.fragments, files, names) |fragment, *file, *name| {
        name.* = fragment.name;
        file.* = .{ .file_id = @constCast(fragment.name), .object_uri = try std.fmt.allocPrint(out, "file:///antfly-native-fragments/{s}", .{fragment.name}), .version_id = try out.dupe(u8, &std.fmt.bytesToHex(&fragment.digest, .lower)), .byte_len = @max(1, fragment.count), .row_count = fragment.count, .row_groups = &.{} };
    }
    var inventory = self.physical.inventory;
    inventory.files = files;
    inventory.deleted_row_groups = &.{};
    self.source = .{ .alloc = self.a, .store = self.physical.store, .scanner = self.physical.scanner, .inventory = inventory, .inventory_owned = false };
    self.source.scanner.iceberg_delete_plan = null;
    self.source.scanner.shared_reader = null;
    self.initialized = true;
    self.provider = provider.*;
    self.provider.source = &self.source;
    self.provider.previous_inventory = null;
    self.provider.native_delta = null;
    self.provider.replay = null;
    self.provider.statistics = null;
    self.provider.skip_indexes = &.{};
    self.provider.expected_delete_objects = null;
    self.provider.fragment_rows = .{ .ptr = self, .open = Build.open };
    const prior_navigation = if (prior) |space| try navigation.load(out, self.store, space.navigation orelse return error.InvalidLogicalRowIdentity, self.token, null) else null;
    const nav = try navigation.build(a, out, &self.store, &self.provider, prior_navigation, self.token);
    self.provider.native_delta = nav.delta;
    self.provider.replay_recommended = true;
    var old_root = self.root;
    old_root.current = prior orelse self.root.current;
    old_root.native_space = null;
    old_root.indexes = null;
    old_root.reused = &.{};
    old_root.certificate = null;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = self.context.io orelse return error.InvalidLogicalRowIdentity, .context = self, .checkpoint = Build.check, .max_bytes = provider.identity_spill_bytes };
    defer manager.deinit();
    const builder = try @import("lake_row_identity_delta.zig").Builder.create(a, &manager, &pages, names, if (prior != null) old_root else null, if (prior != null) nav.delta else null, &.{}, inventory.source_id, inventory.snapshot_id);
    defer builder.deinit();
    for (self.fragments, builder.slots, builder.scan) |fragment, slot, changed| if (changed) {
        const upper = upperBound(fragment.prefix, fragment.bits);
        var cursor = try tree.Cursor.init(a, pages.store(), self.root.current.by_id, &fragment.prefix, if (upper) |*value| value else null);
        defer cursor.deinit();
        var row: u64 = 0;
        while (try cursor.next()) |record| {
            try Build.check(self);
            if (record.key.len != 32 or record.value.len != 56) return error.InvalidLogicalRowIdentity;
            try builder.add(.{ .id = record.key[0..32].*, .values = record.value[16..48].* }, .{ .file = slot, .group = 0, .row = row });
            row += 1;
        }
        if (row != fragment.count) return error.InvalidLogicalRowIdentity;
    };
    self.space = (try builder.finish(self.root.namespace, self.root.values_recipe, self.root.cut)).current;
    self.space.navigation = nav.ref;
    if (self.space.rows != self.root.current.rows) return error.InvalidLogicalRowIdentity;
    self.table = try self.schemas.resolve(self.context.io.?, out, table.schema_json, table.table_id, table.name);
}

/// A per-consumer gather cursor. Shared publication replay spools a projected
/// fragment once across native families; no builder rescans the archive.
const Gather = struct {
    a: A,
    owner: *Build,
    arena: std.heap.ArenaAllocator,
    page_arena: std.heap.ArenaAllocator,
    physical: local.serverless_query_lake_serving.ServingSource,
    binding: local.serverless_segment_source_binding.Binding,
    file: ?usize,
    token: Token,
    store: stores.ArtifactStore,
    position: usize = 0,
    row: u64 = 0,
    tile_rows: usize = 256,
    active: ?local.sql_catalog.Cursor = null,
    page: ?*local.storage_db_types.ColumnSourcePage = null,
    refs: []const rows.RowRef = &.{},
    current: []const rows.RowRef = &.{},
    fn selectedBytes(batch: rows.ColumnBatch, selection: []const usize) !usize {
        try batch.validate();
        for (selection) |row| if (row >= batch.rowCount()) return error.InvalidSqlBackendResponse;
        var bytes: usize = 0;
        for (batch.columns) |column| {
            bytes +|= column.name.len +| @sizeOf(rows.ColumnVector);
            for (selection) |row| {
                bytes +|= 1 + @sizeOf(rows.RowRef);
                if (column.nulls.isNull(row)) continue;
                bytes +|= switch (column.values) {
                    .bytes, .json => |v| v[row].len +| @sizeOf([]const u8),
                    .dictionary_bytes => |v| v.at(row).len +| @sizeOf([]const u8),
                    .decimal128 => @sizeOf(i128),
                    .i64, .f64, .dictionary_i64, .dictionary_f64 => 8,
                    .bool => 1,
                    .vector_f32 => |v| v[row].len *| @sizeOf(f32) +| @sizeOf([]const f32),
                    .encoded => return error.UnsupportedSqlExecution,
                };
            }
        }
        return bytes;
    }
    fn canceled(raw: *const anyopaque) bool {
        const self: *const Gather = @ptrCast(@alignCast(raw));
        self.token.check() catch return true;
        Build.check(self.owner) catch return true;
        return false;
    }
    fn clear(self: *Gather) void {
        if (self.active) |cursor| cursor.close(cursor.ptr);
        self.active = null;
        _ = self.arena.reset(.retain_capacity);
    }
    fn same(left: rows.RowRef, right: rows.RowRef) bool {
        return left == .external and right == .external and left.external.row_group_ordinal == right.external.row_group_ordinal and left.external.row_ordinal == right.external.row_ordinal and std.mem.eql(u8, left.external.file_id, right.external.file_id);
    }
    fn next(raw: *anyopaque, a: A) !?rows.ColumnBatch {
        const self: *Gather = @ptrCast(@alignCast(raw));
        if (self.page) |page| page.release();
        self.page = null;
        _ = self.page_arena.reset(.retain_capacity);
        tiles: while (true) {
            try self.token.check();
            try Build.check(self.owner);
            if (self.active) |cursor| {
                const Page = local.storage_db_types.ColumnSourcePage;
                var positions: [256]Page.Position = undefined;
                var seen: [256]bool = @splat(false);
                var payload_bytes: usize = 0;
                var chunks: std.ArrayList(*Page) = .empty;
                defer {
                    for (chunks.items) |chunk| chunk.release();
                    chunks.deinit(a);
                }
                while (true) {
                    try self.token.check();
                    var phase = try self.store.beginIo(1);
                    defer phase.abort();
                    const page = cursor.next_columns.?(cursor.ptr, self.page_arena.allocator(), 256) catch |err| return phase.failed(err);
                    try phase.finish();
                    if (page.selection.len == 0) break;
                    payload_bytes +|= try selectedBytes(page.batch, page.selection);
                    // Source pages already obey decoder admission. Bound the
                    // extra scatter independently, and retry smaller selections
                    // before copying payloads from multiple physical pages.
                    if (payload_bytes > 8 * 1024 * 1024 and self.current.len > 1) {
                        self.tile_rows = @max(1, self.current.len / 2);
                        self.row -= self.current.len;
                        self.clear();
                        _ = self.page_arena.reset(.retain_capacity);
                        continue :tiles;
                    }
                    const copied = try Page.copy(a, page.batch, page.selection);
                    chunks.append(a, copied) catch |err| {
                        copied.release();
                        return err;
                    };
                    for (page.selection, 0..) |selected, row| {
                        const current = page.batch.rowRef(selected);
                        const found = for (self.current, 0..) |candidate, i| {
                            if (same(candidate, current)) break i;
                        } else return error.InvalidLogicalRowIdentity;
                        if (seen[found]) return error.InvalidLogicalRowIdentity;
                        seen[found] = true;
                        positions[found] = .{ .page = copied, .row = row };
                    }
                    _ = self.page_arena.reset(.retain_capacity);
                }
                for (seen[0..self.refs.len]) |present| if (!present) return error.InvalidLogicalRowIdentity;
                const copied = try Page.gatherPositions(a, positions[0..self.refs.len]);
                errdefer copied.release();
                for (@constCast(copied.columns)) |*column| for (self.owner.table.columns) |definition| {
                    if (std.mem.eql(u8, column.name, definition.name)) {
                        column.name = try copied.arena.allocator().dupe(u8, definition.path);
                        break;
                    }
                };
                const refs = try copied.arena.allocator().dupe(rows.RowRef, self.refs);
                // clear releases the tile's lookup arena, so retain identities
                // in the output page before opening the next physical gather.
                for (refs) |*ref| {
                    ref.external.file_id = try copied.arena.allocator().dupe(u8, ref.external.file_id);
                }
                self.clear();
                self.page = copied;
                return .{ .snapshot = .{ .table_id = self.binding.source_id, .snapshot_id = self.binding.snapshot_id }, .row_refs = refs, .columns = copied.columns };
            }
            const index = self.file orelse self.position;
            if (index >= self.owner.fragments.len) return null;
            const fragment = self.owner.fragments[index];
            if (self.row == fragment.count) {
                if (self.file != null) return null;
                self.position += 1;
                self.row = 0;
                continue;
            }
            const count: usize = @intCast(@min(self.tile_rows, fragment.count - self.row));
            const sa = self.arena.allocator();
            const refs = try sa.alloc(rows.RowRef, count);
            for (refs, self.row..) |*ref, row| ref.* = .{ .external = .{ .identity_version = self.owner.source.inventory.row_identity_version, .source_id = self.binding.source_id, .snapshot_id = self.binding.snapshot_id, .file_id = fragment.name, .row_group_ordinal = 0, .row_ordinal = row } };
            var reads: u64 = 0;
            var writes: u64 = 0;
            var pages = self.owner.pageStore(&reads, &writes);
            pages.cancellation = .{ .ptr = self, .is_cancelled_fn = Gather.canceled };
            var root = self.owner.root;
            root.indexes = self.owner.space;
            root.reused = &.{};
            var cache: tree.Cache = .{ .alloc = sa, .underlying = pages.store(), .max_bytes = 1024 * 1024 };
            defer cache.deinit();
            var reader: identity.Reader = .{ .root = root, .pages = &pages, .cache = &cache };
            var phase = try self.store.beginIo(1);
            defer phase.abort();
            const resolved = reader.translateMany(sa, refs, true) catch |err| return phase.failed(err);
            const current = try sa.alloc(rows.RowRef, count);
            for (resolved, current) |value, *ref| {
                ref.* = reader.reference(sa, root.current, value) catch |err| return phase.failed(err);
                ref.external.identity_version = self.owner.physical.inventory.row_identity_version;
            }
            try phase.finish();
            self.refs = refs;
            self.current = current;
            self.active = try local.sql_lake_cursor.openPinned(self.a, self.owner.table, .{ .fields = self.binding.column_bindings, .row_refs = current, .limit = @intCast(count) }, .{ .deadline_ns = self.owner.context.deadline_ns, .cancellation = .{ .ptr = self, .is_cancelled_fn = Gather.canceled } }, &self.physical);
            if (self.active.?.next_columns == null) return error.UnsupportedSqlExecution;
            self.row += count;
        }
    }
    fn release(raw: *anyopaque) void {
        const page: *local.storage_db_types.ColumnSourcePage = @ptrCast(@alignCast(raw));
        page.release();
    }
    fn retain(raw: *anyopaque, _: A) !?rows.ColumnOwner {
        const self: *Gather = @ptrCast(@alignCast(raw));
        const page = self.page orelse return null;
        page.retain();
        return .{ .ptr = page, .release_fn = release, .retained_bytes = page.arena.queryCapacity() };
    }
    fn close(raw: *anyopaque, a: A) void {
        const self: *Gather = @ptrCast(@alignCast(raw));
        if (self.page) |page| page.release();
        self.clear();
        self.arena.deinit();
        self.page_arena.deinit();
        self.physical.closeParallelView(self.owner.physical);
        a.free(self.binding.column_bindings);
        a.destroy(self);
    }
};

test "external lake logical fragments retain boundaries and isolate append removal mutations" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("logical-fragment-census");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(77), 1, std.testing.io);
    var physical: local.serverless_query_lake_serving.ServingSource = .{ .alloc = a, .store = undefined, .scanner = undefined, .inventory = .{ .format = .parquet, .source_id = @constCast("source"), .source_uri = @constCast("file:///source"), .snapshot_id = @constCast("one"), .schema_fingerprint = @constCast("schema"), .files = &.{} } };
    physical.scanner.iceberg_delete_plan = null;
    physical.scanner.shared_reader = null;
    var provider: Provider = .{ .source = &physical, .context = .{ .io = std.testing.io } };
    const table: local.common_topology_records.TableRecord = .{ .table_id = 7, .name = "source", .indexes_json = "{}", .schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","base_source":{"kind":"external","table_id":"source","format":"parquet","uri":"file:///source","schema_fingerprint":"schema","row_identity":{"key_columns":["id"]}},"document_schemas":{"row":{"schema":{"type":"object","additionalProperties":false,"properties":{"id":{"type":"integer"}}}}}}
    };
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.call };
    defer manager.deinit();
    var reads: u64 = 0;
    var writes: u64 = 0;
    var pages: Pages = .{ .domain = store.upload_scope.?.domain, .attempt = store.upload_scope.?.attempt, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    var previous: ?identity.Root = null;
    var translation: ?identity.Translation = null;
    var last: ?*Build = null;
    defer if (last) |value| value.deinit();
    for (0..3) |step| {
        const builder = try @import("lake_row_identity_delta.zig").Builder.create(a, &manager, &pages, &.{"physical"}, null, null, &.{}, "source", "one");
        defer builder.deinit();
        for (0..(if (step == 0) @as(usize, 128) else 129)) |row| {
            if (step == 2 and row == 17) continue;
            var id: [32]u8 = @splat(3);
            id[0] = @intCast((row % 16) << 4);
            std.mem.writeInt(u64, id[24..32], row, .big);
            try builder.add(.{ .id = id, .values = @splat(4) }, .{ .file = 0, .group = 0, .row = row });
        }
        var root = try builder.finish(@splat(1), @splat(2), @splat(3));
        root.values_format = 2;
        const next = (try prepare(a, &store, table, &physical, &provider, root, previous)).?;
        try std.testing.expectEqual(@as(u64, if (step == 1) 129 else 128), next.space.rows);
        try std.testing.expectEqual(@as(u32, 16), next.space.file_count);
        var changed: usize = 0;
        const delta = next.provider.native_delta.?;
        for (0..delta.count()) |i| if (delta.isChanged(i)) {
            changed += 1;
        };
        try std.testing.expectEqual(@as(usize, if (step == 0) 16 else 1), changed);
        translation = (try @import("lake_row_translation_delta.zig").buildWithOriginal(a, &pages, next.space, root.current, if (previous) |old| old.current else null, if (previous) |old| old.native_space else null, translation, .{ .io = std.testing.io }, 1024 * 1024)).?;
        try std.testing.expectEqual(next.space.rows, translation.?.forward.records);
        var reader: identity.Reader = .{ .root = root, .pages = &pages };
        for ([_]u64{ 0, 16, 17, 128 }) |row| {
            if ((step == 0 and row == 128) or (step == 2 and row == 17)) continue;
            var id: [32]u8 = @splat(3);
            id[0] = @intCast((row % 16) << 4);
            std.mem.writeInt(u64, id[24..32], row, .big);
            const original = (try reader.byId(a, next.space, id)).?;
            // byId returns value-owned coordinates; its temporary lookup bytes
            // are freed by Reader, so no caller arena is required here.
            const physical_row = try reader.translateAt(a, translation.?, original.coordinate, true);
            try std.testing.expectEqual(row, physical_row.coordinate.row);
            const reverse = try reader.translateAt(a, translation.?, physical_row.coordinate, false);
            try std.testing.expectEqual(original.coordinate, reverse.coordinate);
        }
        root.native_space = next.space;
        root.artifacts = @splat(9);
        try root.reseal(a);
        try root.validate(a);
        // The same iterators feed reader leases, GC, warming and cloud closure.
        var dependencies = identity.dependencies(root);
        var found_native_ids = false;
        while (dependencies.next()) |ref| if (ref.eql(next.space.by_id.?)) {
            found_native_ids = true;
        };
        try std.testing.expect(found_native_ids);
        var navigations = identity.navigationDependencies(root);
        var found_native_navigation = false;
        while (navigations.next()) |ref| if (std.meta.eql(ref, next.space.navigation.?)) {
            found_native_navigation = true;
        };
        try std.testing.expect(found_native_navigation);
        // Keep prior metadata alive while the next COW census is prepared.
        if (last) |value| value.deinit();
        last = next;
        previous = root;
    }
}

test "external lake logical fragments split oversized radix leaves without physical reads" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("logical-fragment-split");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(78), 1, std.testing.io);
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.call };
    defer manager.deinit();
    var reads: u64 = 0;
    var writes: u64 = 0;
    var pages: Pages = .{ .domain = store.upload_scope.?.domain, .attempt = store.upload_scope.?.attempt, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    var builder = try identity.Builder.init(a, &manager, &.{"physical"}, 64 * 1024, .{ .source = "source", .snapshot = "one" });
    defer builder.deinit();
    for (0..max_rows + 1) |row| {
        var id: [32]u8 = @splat(3);
        id[0] = @intCast((row % 2) << 3);
        std.mem.writeInt(u64, id[24..32], row, .big);
        try builder.add(.{ .id = id, .values = @splat(4) }, .{ .file = 0, .group = 0, .row = row });
    }
    const root = try builder.finish(&pages, "source", "one", @splat(1), @splat(2), @splat(3));
    var self: Build = .{ .a = a, .arena = .init(a), .physical = undefined, .store = store, .root = root, .schemas = .init(a), .context = .{ .io = std.testing.io }, .token = .none };
    defer self.arena.deinit();
    defer self.schemas.deinit();
    var fragments: std.ArrayList(Fragment) = .empty;
    try planRange(&self, a, &pages, @splat(0), 4, &fragments);
    try std.testing.expectEqual(@as(usize, 2), fragments.items.len);
    try std.testing.expectEqual(@as(u64, max_rows + 1), fragments.items[0].count + fragments.items[1].count);
    for (fragments.items) |fragment| {
        try std.testing.expectEqual(@as(u16, 5), fragment.bits);
        try std.testing.expect(fragment.count <= max_rows);
        const parsed = try parseName(fragment.name);
        try std.testing.expectEqual(fragment.prefix, parsed.prefix);
        try std.testing.expectEqual(fragment.bits, parsed.bits);
    }
}

test "external lake logical fragments gather typed physical pages into logical order" {
    const a = std.testing.allocator;
    const Page = local.storage_db_types.ColumnSourcePage;
    const refs: []const rows.RowRef = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" } };
    const batch: rows.ColumnBatch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = refs, .columns = &.{
        .{ .name = "name", .values = .{ .dictionary_bytes = .{ .values = &.{"alpha"}, .indices = &.{ 0, 123 } } }, .nulls = .{ .bytes = &.{ 0, 1 } } },
        .{ .name = "id", .values = .{ .i64 = &.{ 9007199254740993, 9007199254740995 } } },
        .{ .name = "price", .values = .{ .decimal128 = .{ .values = &.{ 3, 9 }, .precision = 4, .scale = -1 } } },
    } };
    const first = try Page.copy(a, batch, &.{ 0, 1 });
    defer first.release();
    var other = batch;
    other.columns = &.{
        .{ .name = "name", .values = .{ .bytes = &.{ "charlie", "delta" } } },
        .{ .name = "id", .values = .{ .dictionary_i64 = .{ .values = &.{9007199254740997}, .indices = &.{ 0, 0 } } } },
        .{ .name = "price", .values = .{ .decimal128 = .{ .values = &.{ 7, 8 }, .precision = 4, .scale = -1 } } },
    };
    const second = try Page.copy(a, other, &.{0});
    defer second.release();
    const positions: []const Page.Position = &.{ .{ .page = second, .row = 0 }, .{ .page = first, .row = 1 }, .{ .page = first, .row = 0 } };
    const gathered = try Page.gatherPositions(a, positions);
    defer gathered.release();
    try std.testing.expectEqualStrings("charlie", try gathered.columns[0].bytesAt(0));
    try std.testing.expect(gathered.columns[0].nulls.isNull(1));
    try std.testing.expectEqualStrings("alpha", try gathered.columns[0].bytesAt(2));
    try std.testing.expectEqual(@as(i64, 9007199254740997), try gathered.columns[1].integerAt(0));
    try std.testing.expectEqualSlices(i128, &.{ 7, 9, 3 }, gathered.columns[2].values.decimal128.values);
    try std.testing.expectEqual(@as(i32, -1), gathered.columns[2].values.decimal128.scale);
    const Failure = struct {
        fn run(alloc: A, selected: []const Page.Position) !void {
            const page = try Page.gatherPositions(alloc, selected);
            defer page.release();
            try std.testing.expectEqual(@as(usize, 3), page.len);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Failure.run, .{positions});
}

test "external lake logical fragments shrink wide Parquet gathers before copying tiles" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("logical-fragment-wide");
    defer directory.cleanup();
    var storage = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer storage.deinit();
    var client = storage.client();
    try client.makeBucket("antfly");
    const payload = try a.alloc(u8, 128 * 1024);
    defer a.free(payload);
    @memset(payload, 'x');
    var ids: [65]i64 = undefined;
    var bodies: [65][]const u8 = undefined;
    for (&ids, &bodies, 0..) |*id, *body, i| {
        id.* = @intCast(i);
        body.* = payload;
    }
    const bytes = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{.{ .column_id = "id", .field_id = 1, .values = &ids }}, &.{.{ .column_id = "body", .field_id = 2, .converted_type = 0, .values = &bodies }});
    defer a.free(bytes);
    var uploaded = try client.putObject("antfly", "wide.parquet", bytes, .{});
    uploaded.deinit(a);
    const schema = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"source","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema","row_identity":{{"key_columns":["id"]}}}},"document_schemas":{{"row":{{"schema":{{"type":"object","additionalProperties":false,"properties":{{"id":{{"type":"integer"}},"body":{{"type":"string"}}}}}}}}}}}}
    , .{directory.path()});
    defer a.free(schema);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema)).?;
    defer binding.deinit(a);
    var physical = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer physical.deinit();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(79), 1, std.testing.io);
    var provider: Provider = .{ .source = &physical, .context = .{ .io = std.testing.io } };
    const table: local.common_topology_records.TableRecord = .{ .table_id = 7, .name = "source", .indexes_json = "{}", .schema_json = schema };
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.call };
    defer manager.deinit();
    var reads: u64 = 0;
    var writes: u64 = 0;
    var pages: Pages = .{ .domain = store.upload_scope.?.domain, .attempt = store.upload_scope.?.attempt, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    const builder = try @import("lake_row_identity_delta.zig").Builder.create(a, &manager, &pages, &.{physical.inventory.files[0].file_id}, null, null, &.{}, physical.inventory.source_id, physical.inventory.snapshot_id);
    defer builder.deinit();
    for (0..65) |row| {
        var id: [32]u8 = @splat(3);
        id[0] = 0;
        std.mem.writeInt(u64, id[24..32], row, .big);
        try builder.add(.{ .id = id, .values = @splat(4) }, .{ .file = 0, .group = 0, .row = row });
    }
    const root = try builder.finish(@splat(1), @splat(2), @splat(3));
    const native = (try prepare(a, &store, table, &physical, &provider, root, null)).?;
    defer native.deinit();
    const input = try Build.open(native, a, .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = physical.inventory.source_id, .snapshot_id = physical.inventory.snapshot_id, .schema_fingerprint = "schema", .column_bindings = &.{ "id", "body" }, .index_config_hash = "test" }, 0, .none, null);
    defer input.deinit(a);
    var count: usize = 0;
    while (try input.next(a)) |batch| {
        try std.testing.expect(batch.rowCount() <= 32);
        for (0..batch.rowCount()) |row| {
            try std.testing.expectEqual(@as(i64, @intCast(count)), try batch.findColumn("id").?.integerAt(row));
            try std.testing.expectEqual(payload.len, (try batch.findColumn("body").?.bytesAt(row)).len);
            try std.testing.expectEqual(@as(u64, @intCast(count)), batch.rowRef(row).external.row_ordinal);
            count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 65), count);
}
