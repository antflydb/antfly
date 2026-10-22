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
const Fragment = struct { prefix: [32]u8, bits: u16, name: []const u8, count: u64, digest: [32]u8, dirty: bool = false };

pub const Build = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    physical: *local.serverless_query_lake_serving.ServingSource,
    source: local.serverless_query_lake_serving.ServingSource = undefined,
    provider: Provider = undefined,
    root: identity.Root,
    space: identity.Space = undefined,
    prior_navigation: ?navigation.Root = null,
    fragments: []const Fragment = &.{},
    slots: []const u32 = &.{},
    spool: ?*Spool = null,
    census_rows_hashed: u64 = 0,
    input_rows_hashed: u64 = 0,
    store: stores.ArtifactStore,
    schemas: @import("sql_schema_cache.zig").Cache,
    table: local.sql_catalog.Table = undefined,
    context: local.serverless_query_lake_read_context.Context,
    token: Token,
    initialized: bool = false,
    pub fn deinit(self: *Build) void {
        if (self.spool) |spool| spool.deinit();
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
        if (self.spool) |spool| return spool.open(a, binding, file, token, phase);
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
        self.census_rows_hashed += 1;
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
    const prior_navigation = if (prior) |space| try navigation.load(out, self.store, space.navigation orelse return error.InvalidLogicalRowIdentity, self.token, null) else null;
    self.prior_navigation = prior_navigation;
    if (prior_navigation) |nav| {
        const old_fragments = try readFragments(a, out, &pages, nav);
        const Changes = struct {
            owner: *Build,
            fragments: []Fragment,
            pub fn change(self_: *@This(), left: ?tree.Cursor.Record, right: ?tree.Cursor.Record) !void {
                try Build.check(self_.owner);
                if (left) |row| if (row.key.len != 32 or row.value.len != 56) return error.InvalidLogicalRowIdentity;
                if (right) |row| if (row.key.len != 32 or row.value.len != 56) return error.InvalidLogicalRowIdentity;
                if (left != null and right != null and std.mem.eql(u8, left.?.value[16..48], right.?.value[16..48])) return;
                const id = (right orelse left.?).key;
                self_.fragments[try fragmentFor(self_.fragments, id)].dirty = true;
            }
        };
        var changed: Changes = .{ .owner = self, .fragments = old_fragments };
        try tree.visitDifferences(a, pages.store(), previous.?.current.by_id, self.root.current.by_id, &changed);
        for (old_fragments) |fragment| {
            if (fragment.dirty) try planRange(self, a, &pages, fragment.prefix, fragment.bits, &fragments) else try fragments.append(out, fragment);
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
    self.slots = try out.dupe(u32, builder.slots);
    self.space = (try builder.finish(self.root.namespace, self.root.values_recipe, self.root.cut)).current;
    self.space.navigation = nav.ref;
    if (self.space.rows != self.root.current.rows) return error.InvalidLogicalRowIdentity;
    try prepareInputs(self, a, &pages, previous);
    // Admit the complete fresh binding set before reading source payloads or
    // building native artifacts. A later mixed-space overflow still takes the
    // explicit physical fallback at the publication owner.
    var prospective = self.root;
    prospective.version = identity.metadata_version;
    prospective.native_space = self.space;
    const bindings = try a.alloc(identity.ReusedIndex, self.root.current.input_recipes.len);
    defer a.free(bindings);
    for (bindings, self.root.current.input_recipes) |*binding, recipe| binding.* = .{ .name = recipe.name, .space = indexSpace(self, recipe.name), .translation = if (self.space.by_id) |ref| .{ .forward = ref, .reverse = ref, .ranked = false } else null };
    prospective.reused = bindings;
    prospective.certificate = @splat(255);
    const encoded = try std.json.Stringify.valueAlloc(a, prospective, .{});
    defer a.free(encoded);
    if (encoded.len +| 4096 > identity.max_root_bytes) return error.LogicalRowIdentityAdmissionDeclined;
    self.table = try self.schemas.resolve(self.context.io.?, out, table.schema_json, table.table_id, table.name);
}

fn fragmentFor(fragments: []const Fragment, id: []const u8) !usize {
    if (id.len != 32 or fragments.len == 0) return error.InvalidLogicalRowIdentity;
    var lo: usize = 0;
    var hi = fragments.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (std.mem.order(u8, &fragments[mid].prefix, id) != .gt) lo = mid + 1 else hi = mid;
    }
    if (lo == 0) return error.InvalidLogicalRowIdentity;
    return lo - 1;
}
fn readFragments(a: A, out: A, pages: *Pages, nav: navigation.Root) ![]Fragment {
    var fragments: std.ArrayList(Fragment) = .empty;
    var cursor = try tree.Cursor.init(a, pages.store(), nav.files, "", null);
    defer cursor.deinit();
    while (try cursor.next()) |record| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const file = (try std.json.parseFromSliceLeaky(navigation.Record, arena.allocator(), record.value, .{})).file;
        const prefix = try parseName(file.file_id);
        const version = if (std.mem.startsWith(u8, file.version_id, "iv1-")) file.version_id[4..] else file.version_id;
        if (version.len != 64) return error.InvalidLogicalRowIdentity;
        var digest: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&digest, version) catch return error.InvalidLogicalRowIdentity;
        try fragments.append(out, .{ .name = try out.dupe(u8, file.file_id), .prefix = prefix.prefix, .bits = prefix.bits, .count = file.row_count, .digest = digest });
    }
    std.mem.sort(Fragment, fragments.items, {}, struct {
        fn less(_: void, left: Fragment, right: Fragment) bool {
            return std.mem.lessThan(u8, &left.prefix, &right.prefix);
        }
    }.less);
    return fragments.items;
}
fn sameColumns(left: []const []const u8, right: []const []const u8) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| if (!std.mem.eql(u8, l, r)) return false;
    return true;
}
/// Input-version trees have no physical coordinates. COW differences identify
/// exactly the input cohorts and radix leaves whose payload fingerprints changed.
fn prepareInputs(self: *Build, a: A, pages: *Pages, previous: ?identity.Root) !void {
    const out = self.arena.allocator();
    const recipes = self.root.current.input_recipes;
    const views = try out.alloc(Provider.FragmentView, recipes.len);
    const owners = try a.alloc(usize, recipes.len);
    defer a.free(owners);
    const dirty = try a.alloc([]bool, recipes.len);
    defer a.free(dirty);
    var initialized: usize = 0;
    defer for (dirty[0..initialized]) |mask| a.free(mask);
    for (recipes, 0..) |recipe, i| {
        owners[i] = i;
        for (recipes[0..i], 0..) |candidate, j| if (sameColumns(recipe.columns, candidate.columns)) {
            owners[i] = owners[j];
            break;
        };
        dirty[i] = try a.alloc(bool, self.fragments.len);
        initialized += 1;
        @memset(dirty[i], previous == null or self.prior_navigation == null);
    }
    if (previous != null and self.prior_navigation != null) {
        const Changes = struct {
            owner: *Build,
            recipes: []const @import("lake_row_input_recipes.zig").Recipe,
            owners: []const usize,
            dirty: [][]bool,
            pub fn change(self_: *@This(), left: ?tree.Cursor.Record, right: ?tree.Cursor.Record) !void {
                try Build.check(self_.owner);
                if (left != null and right != null and std.mem.eql(u8, left.?.value, right.?.value)) return;
                const row = right orelse left.?;
                if (row.key.len != 64 or row.value.len != 32) return error.InvalidLogicalRowIdentity;
                for (self_.recipes, 0..) |recipe, i| if (std.mem.eql(u8, &recipe.digest, row.key[0..32])) {
                    self_.dirty[self_.owners[i]][try fragmentFor(self_.owner.fragments, row.key[32..64])] = true;
                    break;
                };
            }
        };
        var changed: Changes = .{ .owner = self, .recipes = recipes, .owners = owners, .dirty = dirty };
        try tree.visitDifferences(a, pages.store(), previous.?.current.input_versions, self.root.current.input_versions, &changed);
    }
    for (recipes, views, 0..) |recipe, *view, i| {
        if (owners[i] != i) {
            view.* = views[owners[i]];
            view.name = recipe.name;
            continue;
        }
        var old: ?navigation.Root = null;
        var versions: std.StringHashMapUnmanaged(Fragment) = .empty;
        defer versions.deinit(a);
        if (previous) |prior| if (self.prior_navigation != null) {
            const space = prior.spaceForIndex(recipe.name);
            if (space.navigation) |ref| {
                old = try navigation.load(out, self.store, ref, self.token, null);
                // Legacy global versions lack the input-specific domain marker.
                var records = try tree.Cursor.init(a, pages.store(), old.?.files, "", null);
                defer records.deinit();
                while (try records.next()) |record| {
                    var arena = std.heap.ArenaAllocator.init(a);
                    defer arena.deinit();
                    const file = (try std.json.parseFromSliceLeaky(navigation.Record, arena.allocator(), record.value, .{})).file;
                    if (!std.mem.startsWith(u8, file.version_id, "iv1-")) continue;
                    const prefix = try parseName(file.file_id);
                    var digest: [32]u8 = undefined;
                    _ = std.fmt.hexToBytes(&digest, file.version_id[4..]) catch return error.InvalidLogicalRowIdentity;
                    try versions.put(a, try out.dupe(u8, file.file_id), .{ .prefix = prefix.prefix, .bits = prefix.bits, .name = "", .count = file.row_count, .digest = digest });
                }
            }
        };
        const files = try out.dupe(local.serverless_external_source_types.FileEntry, self.source.inventory.files);
        for (self.fragments, files, dirty[i]) |fragment, *file, changed| {
            const cached = versions.get(fragment.name);
            const digest = if (!changed and cached != null and cached.?.count == fragment.count) cached.?.digest else hash: {
                var hash = std.crypto.hash.Blake3.init(.{});
                hash.update("native-lake-logical-input-fragment-v1");
                hash.update(&self.root.namespace);
                hash.update(&recipe.digest);
                var lower: [64]u8 = undefined;
                lower[0..32].* = recipe.digest;
                lower[32..64].* = fragment.prefix;
                var upper: [64]u8 = undefined;
                const bound = upperBound(fragment.prefix, fragment.bits);
                if (bound) |value| {
                    upper[0..32].* = recipe.digest;
                    upper[32..64].* = value;
                }
                var cursor = try tree.Cursor.init(a, pages.store(), self.root.current.input_versions, &lower, if (bound != null) &upper else null);
                defer cursor.deinit();
                var count: u64 = 0;
                while (try cursor.next()) |record| {
                    if (record.key.len != 64 or record.value.len != 32) return error.InvalidLogicalRowIdentity;
                    if (!std.mem.eql(u8, record.key[0..32], &recipe.digest)) break;
                    hash.update(record.key[32..64]);
                    hash.update(record.value);
                    count += 1;
                    self.input_rows_hashed += 1;
                }
                if (count != fragment.count) return error.InvalidLogicalRowIdentity;
                var value: [32]u8 = undefined;
                hash.final(&value);
                break :hash value;
            };
            file.version_id = try std.fmt.allocPrint(out, "iv1-{s}", .{std.fmt.bytesToHex(&digest, .lower)});
        }
        var source = self.source;
        source.inventory.files = files;
        var provider = self.provider;
        provider.source = &source;
        // The parent census describes the union of inputs. Recompute native
        // identities from this cohort's version descriptors.
        provider.native_delta = null;
        const nav = try navigation.build(a, out, &self.store, &provider, old, self.token);
        view.* = .{ .name = recipe.name, .files = files, .delta = nav.delta, .navigation = nav.ref };
    }
    self.provider.fragment_views = views;
}
pub fn indexSpace(self: *const Build, name: []const u8) identity.Space {
    var space = self.space;
    for (self.provider.fragment_views) |view| if (std.mem.eql(u8, view.name, name)) {
        space.navigation = view.navigation;
        break;
    };
    return space;
}

/// Broad rebuilds partition references over one typed projected source pass.
/// Payload pages are written once; only compact row references are externally
/// sorted. Selective rebuilds keep the physical gather path.
const Spool = struct {
    const spill = local.sql_spill;
    const Page = local.storage_db_types.ColumnSourcePage;
    const workspace_bytes = 64 * 1024 * 1024;
    const Spec = @import("lake_row_payloads.zig").Spec;
    a: A,
    owner: *Build,
    budget: local.sql_memory_budget,
    manager: spill.Manager = undefined,
    payloads: spill.JoinPayloads = undefined,
    routes: @import("../serverless/graph_segment/page_store.zig").PrivatePages = undefined,
    route_store: tree.Store = undefined,
    route_root: ?tree.Ref = null,
    arena: std.heap.ArenaAllocator = undefined,
    columns: []const []const u8 = &.{},
    specs: []Spec = &.{},
    needed: []const bool = &.{},
    positions: std.AutoHashMapUnmanaged(u32, usize) = .empty,
    mutex: std.Io.Mutex = .init,
    initialized: bool = false,
    source_passes: u64 = 0,
    captured: ?*@import("lake_row_payloads.zig").Capture = null,
    fn deinit(self: *Spool) void {
        if (self.initialized) {
            self.routes.deinit();
            self.payloads.close();
        }
        self.positions.deinit(self.budget.allocator());
        self.arena.deinit();
        self.manager.deinit();
        std.debug.assert(self.budget.live == 0);
        local.sql_parallel_scheduler.global().releasePull(0, workspace_bytes);
        self.a.destroy(self);
    }
    fn storage(self: *Spool) tree.Store {
        return .{ .domain = self.owner.root.domain, .ptr = self, .get = getPage, .put = noWrite, .check = check };
    }
    fn check(raw: *anyopaque) !void {
        try Build.check(@as(*Spool, @ptrCast(@alignCast(raw))).owner);
    }
    fn noWrite(_: *anyopaque, _: tree.Ref, _: []const u8) !void {
        return error.InvalidSqlSpill;
    }
    fn getPage(raw: *anyopaque, a: A, ref: tree.Ref) ![]u8 {
        const self: *Spool = @ptrCast(@alignCast(raw));
        try self.mutex.lock(self.manager.io);
        defer self.mutex.unlock(self.manager.io);
        const store = self.routes.store();
        return store.get(store.ptr, a, ref);
    }
    fn scanCapture(self: *Spool, provider: *Provider, store: *stores.ArtifactStore) !void {
        const a = self.budget.allocator();
        const out = self.arena.allocator();
        var names: std.StringHashMapUnmanaged(void) = .empty;
        defer names.deinit(a);
        for (self.owner.root.current.input_recipes) |recipe| for (recipe.columns) |name| try names.put(a, name, {});
        const columns = try out.alloc([]const u8, names.count());
        var it = names.keyIterator();
        for (columns) |*name| name.* = it.next().?.*;
        if (columns.len == 0) return error.UnsupportedExternalLakeIndex;
        self.columns = columns;
        self.specs = try out.alloc(Spec, columns.len);
        for (self.owner.slots, 0..) |slot, ordinal| try self.positions.put(a, slot, ordinal);
        var input = provider.*;
        input.fragment_rows = null;
        input.fragment_views = &.{};
        input.replay = null;
        input.replay_consumer = null;
        input.only_file = null;
        input.only_files = null;
        input.only_plan = null;
        input.io_phase = store.io_phase;
        var phase = try store.beginIo(1);
        defer phase.abort();
        const source = input.provider().open_with_cancellation_fn.?(input.provider().ptr, a, .{ .source_kind = if (self.owner.physical.inventory.format == .iceberg) .external_iceberg else .external_parquet, .row_ref_kind = .external, .source_id = self.owner.physical.inventory.source_id, .snapshot_id = self.owner.physical.inventory.snapshot_id, .schema_fingerprint = self.owner.physical.inventory.schema_fingerprint, .sidecar_kind = .text, .index_config_hash = "logical-fragment-spool-v1", .column_bindings = columns }, self.owner.token) catch |err| return phase.failed(err);
        defer {
            if (store.io_phase) |io| if (io.park) |park| park(io.ptr);
            source.deinit(a);
        }
        try phase.finish();
        self.source_passes += 1;
        self.route_store = try @import("../serverless/graph_segment/page_store.zig").PrivatePages.beginRun(&self.routes);
        var edits = @import("lake_index_page_mutations.zig").Mutations.init(a, &self.manager, self.route_store);
        defer edits.deinit();
        edits.private_pages.phase = store.io_phase;
        var read_bytes: u64 = 0;
        var write_bytes: u64 = 0;
        var pages = self.owner.pageStore(&read_bytes, &write_bytes);
        pages.artifacts = store;
        var cache: tree.Cache = .{ .alloc = a, .underlying = pages.store(), .max_bytes = 2 * 1024 * 1024 };
        defer cache.deinit();
        var root = self.owner.root;
        root.indexes = self.owner.space;
        root.reused = &.{};
        var resolver: identity.Reader = .{ .root = root, .pages = &pages, .cache = &cache };
        var bound = false;
        while (true) {
            var reading = try store.beginIo(1);
            defer reading.abort();
            const next = source.next(a) catch |err| return reading.failed(err);
            try reading.finish();
            const batch = next orelse break;
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const pa = arena.allocator();
            const vectors = try @import("lake_row_payloads.zig").vectors(pa, batch, columns, self.specs, bound);
            bound = true;
            var begin: usize = 0;
            while (begin < batch.rowCount()) {
                const end = @min(batch.rowCount(), begin + 256);
                var tile = std.heap.ArenaAllocator.init(a);
                defer tile.deinit();
                const ta = tile.allocator();
                var translating = try store.beginIo(1);
                defer translating.abort();
                const references = try ta.alloc(rows.RowRef, end - begin);
                for (references, begin..) |*ref, row| ref.* = batch.rowRef(row);
                const logical = resolver.translateMany(ta, references, false) catch |err| return translating.failed(err);
                try translating.finish();
                var selected: [256]usize = undefined;
                var coordinates: [256]identity.logical.Coordinate = undefined;
                var count: usize = 0;
                for (logical, begin..) |row, physical| {
                    const file = self.positions.get(row.coordinate.file) orelse return error.InvalidLogicalRowIdentity;
                    if (!self.needed[file]) continue;
                    selected[count] = physical;
                    coordinates[count] = row.coordinate;
                    count += 1;
                }
                var captured: usize = 0;
                while (captured < count) {
                    var take = @min(spill.JoinPayloads.page_rows, count - captured);
                    while (take > 1 and try Gather.selectedBytes(batch, selected[captured..][0..take]) > 8 * 1024 * 1024) take = @max(1, take / 2);
                    var adapter: @import("lake_index_build_replay.zig").Replay.CaptureBatch = .{ .vectors = vectors };
                    const Reader = @import("lake_index_build_replay.zig").Replay.CaptureBatch;
                    const borrowed: local.sql_execution_batch.Batch = .{ .reader = .{ .ptr = &adapter, .read = Reader.cell, .read_physical = Reader.physical, .read_identity = Reader.identity, .count = batch.rowCount(), .width = columns.len } };
                    var writing = try store.beginIo(1);
                    defer writing.abort();
                    const offset = self.payloads.appendBatch(try borrowed.select(ta, selected[captured..][0..take])) catch |err| return writing.failed(err);
                    try writing.finish();
                    for (selected[captured..][0..take], coordinates[captured..][0..take], 0..) |physical, coordinate, row| {
                        var value: [24]u8 = @splat(0);
                        std.mem.writeInt(u64, value[0..8], offset, .big);
                        std.mem.writeInt(u64, value[8..16], row, .big);
                        std.mem.writeInt(u64, value[16..24], try Gather.selectedBytes(batch, &.{physical}), .big);
                        const key = coordinate.encode();
                        try edits.addRecord(&key, &value);
                    }
                    captured += take;
                }
                begin = end;
            }
        }
        self.route_root = try edits.finish(null);
        self.routes.phase = null;
    }
    /// Only compact coordinate/offset records are sorted after verification.
    /// Typed Parquet payloads stay in the original private capture pages.
    fn adopt(self: *Spool, capture: *@import("lake_row_payloads.zig").Capture, store: *stores.ArtifactStore) !void {
        if (!capture.complete or capture.rows != self.owner.root.current.rows) return error.InvalidLogicalRowIdentity;
        const a = self.budget.allocator();
        capture.routes.phase = store.io_phase;
        defer capture.routes.phase = null;
        self.columns = capture.columns;
        self.specs = capture.specs;
        self.captured = capture;
        for (self.owner.slots, 0..) |slot, ordinal| try self.positions.put(a, slot, ordinal);
        self.route_store = try @import("../serverless/graph_segment/page_store.zig").PrivatePages.beginRun(&self.routes);
        var mappings = @import("lake_index_page_mutations.zig").Mutations.init(a, &self.manager, self.route_store);
        defer mappings.deinit();
        mappings.private_pages.phase = store.io_phase;
        var reads: u64 = 0;
        var writes: u64 = 0;
        var pages = self.owner.pageStore(&reads, &writes);
        pages.artifacts = store;
        pages.read_phase = store.io_phase;
        var physical = try tree.Cursor.init(a, pages.store(), self.owner.root.current.by_id, "", null);
        defer physical.deinit();
        var logical = try tree.Cursor.init(a, pages.store(), self.owner.space.by_id, "", null);
        defer logical.deinit();
        while (try physical.next()) |row| {
            const native = (try logical.next()) orelse return error.InvalidLogicalRowIdentity;
            if (row.key.len != 32 or row.value.len != 56 or native.value.len != 56 or !std.mem.eql(u8, row.key, native.key)) return error.InvalidLogicalRowIdentity;
            const coordinate = try identity.logical.Coordinate.decode(native.value[0..16]);
            const file = self.positions.get(coordinate.file) orelse return error.InvalidLogicalRowIdentity;
            if (self.needed[file]) try mappings.addRecord(row.value[0..16], native.value[0..16]);
        }
        if ((try logical.next()) != null) return error.InvalidLogicalRowIdentity;
        const mapping = try mappings.finish(null);
        var by_physical = try tree.Cursor.init(a, self.route_store, mapping, "", null);
        defer by_physical.deinit();
        var payload = try tree.Cursor.init(a, capture.route_store, capture.route_root, "", null);
        defer payload.deinit();
        var current = try payload.next();
        var edits = @import("lake_index_page_mutations.zig").Mutations.init(a, &self.manager, self.route_store);
        defer edits.deinit();
        edits.private_pages.phase = store.io_phase;
        while (try by_physical.next()) |row| {
            while (current != null and std.mem.lessThan(u8, current.?.key, row.key)) current = try payload.next();
            if (current == null or !std.mem.eql(u8, row.key, current.?.key) or current.?.value.len != 24) return error.InvalidLogicalRowIdentity;
            try edits.addRecord(row.value, current.?.value);
        }
        self.route_root = try edits.finish(null);
        self.routes.phase = null;
    }
    fn open(self: *Spool, a: A, binding: local.serverless_segment_source_binding.Binding, file: ?usize, token: Token, phase: ?*const stores.IoPhase) !rows.Source {
        const cursor = try a.create(Cursor);
        errdefer a.destroy(cursor);
        cursor.* = .{ .a = a, .owner = self, .binding = binding, .file = file, .token = token, .store = self.owner.store };
        cursor.binding.column_bindings = try a.dupe([]const u8, binding.column_bindings);
        cursor.store.io_phase = phase;
        return .{ .kind = if (self.owner.physical.inventory.format == .iceberg) .external_iceberg else .external_parquet, .ctx = cursor, .next_batch = Cursor.next, .deinit_fn = Cursor.close, .retain_columns = Cursor.retain };
    }
    const Cursor = struct {
        const Item = struct { coordinate: identity.logical.Coordinate, offset: u64, row: usize, bytes: u64, position: usize = 0 };
        a: A,
        owner: *Spool,
        binding: local.serverless_segment_source_binding.Binding,
        file: ?usize,
        token: Token,
        store: stores.ArtifactStore,
        cursor: ?tree.Cursor = null,
        page: ?*Page = null,
        pending: ?Item = null,
        exhausted: bool = false,
        fn item(self: *Cursor) !?Item {
            if (self.pending) |value| {
                self.pending = null;
                return value;
            }
            const record = (try self.cursor.?.next()) orelse return null;
            if (record.key.len != 16 or record.value.len != 24) return error.InvalidSqlSpill;
            const coordinate = try identity.logical.Coordinate.decode(record.key);
            if (self.file) |file| if (coordinate.file != self.owner.owner.slots[file]) return null;
            return .{ .coordinate = coordinate, .offset = std.mem.readInt(u64, record.value[0..8], .big), .row = @intCast(std.mem.readInt(u64, record.value[8..16], .big)), .bytes = std.mem.readInt(u64, record.value[16..24], .big) };
        }
        fn next(raw: *anyopaque, a: A) !?rows.ColumnBatch {
            const self: *Cursor = @ptrCast(@alignCast(raw));
            try self.token.check();
            try Build.check(self.owner.owner);
            if (self.page) |page| page.release();
            self.page = null;
            if (self.exhausted) return null;
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const ta = arena.allocator();
            var phase = try self.store.beginIo(1);
            defer phase.abort();
            if (self.cursor == null) {
                var lower: [4]u8 = undefined;
                if (self.file) |file| std.mem.writeInt(u32, &lower, self.owner.owner.slots[file], .big);
                self.cursor = tree.Cursor.init(a, self.owner.storage(), self.owner.route_root, if (self.file != null) &lower else "", null) catch |err| return phase.failed(err);
            }
            var items: [256]Item = undefined;
            var count: usize = 0;
            var bytes: u64 = 0;
            while (count < items.len) {
                var value = (self.item() catch |err| return phase.failed(err)) orelse {
                    self.exhausted = true;
                    break;
                };
                if (count != 0 and bytes +| value.bytes > 8 * 1024 * 1024) {
                    self.pending = value;
                    break;
                }
                value.position = count;
                items[count] = value;
                count += 1;
                bytes +|= value.bytes;
            }
            if (count == 0) {
                try phase.finish();
                return null;
            }
            const refs = try ta.alloc(rows.RowRef, count);
            for (items[0..count], refs) |value, *ref| {
                const file = self.owner.positions.get(value.coordinate.file) orelse return error.InvalidSqlSpill;
                ref.* = .{ .external = .{ .source_id = self.binding.source_id, .snapshot_id = self.binding.snapshot_id, .identity_version = self.owner.owner.source.inventory.row_identity_version, .file_id = self.owner.owner.fragments[file].name, .row_group_ordinal = 0, .row_ordinal = value.coordinate.row } };
            }
            std.mem.sort(Item, items[0..count], {}, struct {
                fn less(_: void, left: Item, right: Item) bool {
                    return left.offset < right.offset;
                }
            }.less);
            var positions: [256]Page.Position = undefined;
            var chunks: std.ArrayList(*Page) = .empty;
            defer {
                for (chunks.items) |chunk| chunk.release();
                chunks.deinit(a);
            }
            var begin: usize = 0;
            while (begin < count) {
                var end = begin + 1;
                while (end < count and items[end].offset == items[begin].offset) end += 1;
                try self.owner.mutex.lock(self.owner.manager.io);
                const payloads = if (self.owner.captured) |capture| &capture.payloads else &self.owner.payloads;
                const block = payloads.read(items[begin].offset) catch |err| {
                    self.owner.mutex.unlock(self.owner.manager.io);
                    return phase.failed(err);
                };
                self.owner.mutex.unlock(self.owner.manager.io);
                defer block.release();
                var decode_arena = std.heap.ArenaAllocator.init(a);
                defer decode_arena.deinit();
                const da = decode_arena.allocator();
                const columns = try da.alloc(rows.ColumnVector, self.binding.column_bindings.len);
                for (self.binding.column_bindings, columns) |name, *column| {
                    const ordinal = for (self.owner.columns, 0..) |candidate, i| {
                        if (std.mem.eql(u8, name, candidate)) break i;
                    } else return error.RowSourceColumnKindMismatch;
                    const spec = self.owner.specs[ordinal];
                    column.* = try block.columnVector(da, ordinal, if (spec.kind == .decimal128) .bytes else spec.kind);
                    column.name = name;
                    if (spec.kind == .decimal128) {
                        const values = try da.alloc(i128, block.count());
                        for (values, 0..) |*value, row| value.* = if (column.nulls.isNull(row)) 0 else blk: {
                            const raw_value = try column.bytesAt(row);
                            if (raw_value.len != 16) return error.InvalidSqlSpill;
                            break :blk std.mem.readInt(i128, raw_value[0..16], .big);
                        };
                        column.values = .{ .decimal128 = .{ .values = values, .precision = spec.precision, .scale = spec.scale } };
                    }
                }
                const row_refs = try da.alloc(rows.RowRef, block.count());
                @memset(row_refs, .{ .relational_key = "spool" });
                var selection: [256]usize = undefined;
                for (items[begin..end], selection[0 .. end - begin]) |value, *selected| selected.* = value.row;
                const page = try Page.copy(a, .{ .snapshot = .{ .table_id = self.binding.source_id, .snapshot_id = self.binding.snapshot_id }, .row_refs = row_refs, .columns = columns }, selection[0 .. end - begin]);
                chunks.append(a, page) catch |err| {
                    page.release();
                    return err;
                };
                for (items[begin..end], 0..) |value, row| positions[value.position] = .{ .page = page, .row = row };
                begin = end;
            }
            try phase.finish();
            const page = try Page.gatherPositions(a, positions[0..count]);
            errdefer page.release();
            const owned_refs = try page.arena.allocator().dupe(rows.RowRef, refs);
            for (owned_refs) |*ref| ref.external.file_id = try page.arena.allocator().dupe(u8, ref.external.file_id);
            self.page = page;
            return .{ .snapshot = .{ .table_id = self.binding.source_id, .snapshot_id = self.binding.snapshot_id }, .row_refs = owned_refs, .columns = page.columns };
        }
        fn retain(raw: *anyopaque, _: A) !?rows.ColumnOwner {
            const self: *Cursor = @ptrCast(@alignCast(raw));
            const page = self.page orelse return null;
            page.retain();
            return .{ .ptr = page, .release_fn = Gather.release, .retained_bytes = page.arena.queryCapacity() };
        }
        fn close(raw: *anyopaque, a: A) void {
            const self: *Cursor = @ptrCast(@alignCast(raw));
            if (self.page) |page| page.release();
            if (self.cursor) |*cursor| cursor.deinit();
            a.free(self.binding.column_bindings);
            a.destroy(self);
        }
    };
};
pub fn prepareSpool(self: *Build, provider: *Provider, store: *stores.ArtifactStore) !void {
    return prepareCapturedSpool(self, provider, store, null);
}
pub fn prepareCapturedSpool(self: *Build, provider: *Provider, store: *stores.ArtifactStore, capture: ?*@import("lake_row_payloads.zig").Capture) !void {
    if ((capture == null and self.root.current.rows < 128) or self.provider.fragment_views.len == 0) return;
    const needed = try self.a.alloc(bool, self.fragments.len);
    defer self.a.free(needed);
    @memset(needed, false);
    for (self.provider.fragment_views) |view| if (provider.wantsIndex(view.name)) {
        for (needed, 0..) |*flag, i| flag.* = flag.* or view.delta.isChanged(i);
    };
    var rows_needed: u64 = 0;
    for (needed, self.fragments) |flag, fragment| if (flag) {
        rows_needed += fragment.count;
    };
    if (rows_needed == 0 or (capture == null and rows_needed < self.root.current.rows / 8)) return;
    const scheduler = local.sql_parallel_scheduler.global();
    if (!scheduler.reservePull(0, Spool.workspace_bytes)) return;
    const spool = self.a.create(Spool) catch |err| {
        scheduler.releasePull(0, Spool.workspace_bytes);
        return err;
    };
    spool.* = .{ .a = self.a, .owner = self, .budget = .{ .backing = self.a, .limit = Spool.workspace_bytes } };
    const a = spool.budget.allocator();
    spool.manager = .{ .alloc = a, .io = self.context.io.?, .context = self, .checkpoint = Build.check, .max_bytes = provider.identity_spill_bytes, .async_writes = false };
    if (capture) |value| spool.manager.max_bytes -|= value.manager.live_bytes;
    spool.arena = .init(a);
    defer if (self.spool == null) spool.deinit();
    spool.needed = try spool.arena.allocator().dupe(bool, needed);
    spool.routes = .{ .manager = &spool.manager, .domain = self.root.domain, .attempt = self.store.upload_scope.?.attempt };
    spool.payloads = try Spool.spill.JoinPayloads.init(&spool.manager);
    spool.initialized = true;
    var execution: local.sql_parallel_scheduler.Execution = .{ .scheduler = scheduler, .bytes = 8 * 1024 * 1024, .checkpoint = .{ .ptr = self, .check = Build.check } };
    defer execution.pause();
    try execution.enter(self.context.io.?);
    var io: stores.ExecutionIo = .{ .execution = &execution, .io = self.context.io.? };
    var backend = store.*;
    backend.io_phase = io.capability();
    spool.routes.phase = backend.io_phase;
    const captured: anyerror!void = if (capture) |value| spool.adopt(value, &backend) else spool.scanCapture(provider, &backend);
    captured catch |err| {
        if (err == error.SqlProgramLimitExceeded or err == error.UnsupportedExternalLakeIndex or (err == error.OutOfMemory and spool.budget.isLocallyExhausted())) return;
        return err;
    };
    self.spool = spool;
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
        return @import("lake_row_payloads.zig").selectedBytes(batch, selection);
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
            pages.artifacts = &self.store;
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
    for (0..4) |step| {
        const builder = try @import("lake_row_identity_delta.zig").Builder.create(a, &manager, &pages, &.{"physical"}, null, null, &.{}, "source", "one");
        defer builder.deinit();
        for (0..(if (step == 0) @as(usize, 128) else 129)) |row| {
            if (step >= 2 and row == 17) continue;
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
        try std.testing.expect(next.census_rows_hashed <= if (step == 0) @as(u64, 128) else if (step == 3) @as(u64, 0) else 9);
        var changed: usize = 0;
        const delta = next.provider.native_delta.?;
        for (0..delta.count()) |i| if (delta.isChanged(i)) {
            changed += 1;
        };
        try std.testing.expectEqual(@as(usize, if (step == 0) 16 else if (step == 3) 0 else 1), changed);
        translation = (try @import("lake_row_translation_delta.zig").buildWithOriginal(a, &pages, next.space, root.current, if (previous) |old| old.current else null, if (previous) |old| old.native_space else null, translation, .{ .io = std.testing.io }, 1024 * 1024)).?;
        try std.testing.expectEqual(next.space.rows, translation.?.forward.records);
        var reader: identity.Reader = .{ .root = root, .pages = &pages };
        for ([_]u64{ 0, 16, 17, 128 }) |row| {
            if ((step == 0 and row == 128) or (step >= 2 and row == 17)) continue;
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

test "external lake logical fragments route broad Parquet rebuilds through one projected typed spill pass" {
    try broadSpoolScenario(false, false);
}
test "external lake logical fragments reuse verification payloads without another Parquet scan" {
    try broadSpoolScenario(true, false);
}
test "external lake logical fragments reuse captured payloads for selective changed fragments" {
    try broadSpoolScenario(true, true);
}
fn broadSpoolScenario(fused: bool, selective: bool) !void {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("logical-fragment-spool");
    defer directory.cleanup();
    var storage = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer storage.deinit();
    var client = storage.client();
    try client.makeBucket("antfly");
    const payload = try a.alloc(u8, 1024);
    defer a.free(payload);
    @memset(payload, 'x');
    var ids: [257]i64 = undefined;
    var prices: [257]i64 = undefined;
    var bodies: [257][]const u8 = undefined;
    for (&ids, &bodies, 0..) |*id, *body, i| {
        id.* = @intCast(i);
        body.* = payload;
        prices[i] = 9007199254740993 - @as(i64, @intCast(i));
    }
    const bytes = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{ .{ .column_id = "id", .field_id = 1, .values = &ids }, .{ .column_id = "price", .field_id = 3, .converted_type = 5, .decimal_precision = 18, .decimal_scale = 2, .values = &prices } }, &.{.{ .column_id = "body", .field_id = 2, .converted_type = 0, .values = &bodies }});
    defer a.free(bytes);
    var uploaded = try client.putObject("antfly", "wide.parquet", bytes, .{});
    uploaded.deinit(a);
    const schema = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"source","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema","row_identity":{{"key_columns":["id"]}}}},"document_schemas":{{"row":{{"schema":{{"type":"object","additionalProperties":false,"properties":{{"id":{{"type":"integer"}},"body":{{"type":"string"}},"price":{{"type":"number","x-antfly-sql-type":"numeric"}}}}}}}}}}}}
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
    const builder = try @import("lake_row_identity_delta.zig").Builder.create(a, &manager, &pages, &.{physical.inventory.files[0].file_id}, null, null, &.{.{ .name = "text", .digest = @splat(8), .columns = &.{ "id", "body", "price" } }}, physical.inventory.source_id, physical.inventory.snapshot_id);
    defer builder.deinit();
    for (0..257) |row| {
        var id: [32]u8 = @splat(3);
        id[0] = if (selective) @intCast((row % 16) << 4) else 0;
        std.mem.writeInt(u64, id[24..32], row, .big);
        try builder.addWithInputs(.{ .id = id, .values = @splat(4) }, .{ .file = 0, .group = 0, .row = 256 - row }, &.{@splat(5)});
    }
    const root = try builder.finish(@splat(1), @splat(2), @splat(3));
    const native = (try prepare(a, &store, table, &physical, &provider, root, null)).?;
    defer native.deinit();
    var selection_arena = std.heap.ArenaAllocator.init(a);
    defer selection_arena.deinit();
    if (selective) for (@constCast(native.provider.fragment_views)) |*view| {
        const files = try selection_arena.allocator().alloc(@import("lake_index_native_state.zig").File, view.files.len);
        const changed = try selection_arena.allocator().alloc(bool, view.files.len);
        for (files, changed, 0..) |*file, *flag, i| {
            file.* = try view.delta.file(i);
            flag.* = i == 0;
        }
        view.delta.selection = null;
        view.delta.files = files;
        view.delta.changed = changed;
    };
    provider.identity_spill_bytes = 1;
    try prepareSpool(native, &provider, &store);
    try std.testing.expect(native.spool == null);
    provider.identity_spill_bytes = 1024 * 1024 * 1024;
    var captured: ?*@import("lake_row_payloads.zig").Capture = null;
    defer if (captured) |value| value.deinit();
    if (fused) {
        captured = (try @import("lake_row_payloads.zig").Capture.start(a, &provider, &store, &.{ "id", "body", "price" })).?;
        var by_file: std.StringHashMapUnmanaged(u32) = .empty;
        defer by_file.deinit(a);
        try by_file.put(a, physical.inventory.files[0].file_id, 0);
        const opening = provider.provider();
        const source = try opening.open_with_cancellation_fn.?(opening.ptr, a, .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = physical.inventory.source_id, .snapshot_id = physical.inventory.snapshot_id, .schema_fingerprint = "schema", .index_config_hash = "test", .column_bindings = &.{ "id", "body", "price" } }, .none);
        defer source.deinit(a);
        while (try source.next(a)) |batch| try captured.?.add(batch, &by_file, &store);
        try captured.?.finish(root.current.rows);
    }
    try prepareCapturedSpool(native, &provider, &store, captured);
    try std.testing.expect(native.spool != null);
    try std.testing.expectEqual(@as(u64, if (fused) 0 else 1), native.spool.?.source_passes);
    for (0..2) |_| {
        const input = try Build.open(native, a, .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = physical.inventory.source_id, .snapshot_id = physical.inventory.snapshot_id, .schema_fingerprint = "schema", .column_bindings = &.{ "id", "body", "price" }, .index_config_hash = "test" }, 0, .none, null);
        defer input.deinit(a);
        var count: usize = 0;
        while (try input.next(a)) |batch| {
            try std.testing.expect(batch.rowCount() <= 256);
            for (0..batch.rowCount()) |row| {
                try std.testing.expectEqual(@as(i64, @intCast(256 - count * @as(usize, if (selective) 16 else 1))), try batch.findColumn("id").?.integerAt(row));
                try std.testing.expectEqual(payload.len, (try batch.findColumn("body").?.bytesAt(row)).len);
                const price = batch.findColumn("price").?.values.decimal128;
                try std.testing.expectEqual(@as(i32, 18), price.precision);
                try std.testing.expectEqual(@as(i32, 2), price.scale);
                try std.testing.expectEqual(@as(i128, prices[256 - count * @as(usize, if (selective) 16 else 1)]), price.values[row]);
                try std.testing.expectEqual(@as(u64, @intCast(count)), batch.rowRef(row).external.row_ordinal);
                count += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, if (selective) 17 else 257), count);
    }
    try std.testing.expectEqual(@as(u64, if (fused) 0 else 1), native.spool.?.source_passes);
}

test "external lake logical fragments skip unchanged input cohorts and hash only changed leaves" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("logical-input-census");
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
    var last: ?*Build = null;
    defer if (last) |value| value.deinit();
    var metadata = std.heap.ArenaAllocator.init(a);
    defer metadata.deinit();
    for (0..3) |step| {
        const builder = try @import("lake_row_identity_delta.zig").Builder.create(a, &manager, &pages, &.{"physical"}, null, null, &.{
            .{ .name = "dense", .digest = @splat(8), .columns = &.{"dense"} },
            .{ .name = "text", .digest = @splat(9), .columns = &.{"body"} },
        }, "source", "one");
        defer builder.deinit();
        for (0..128) |row| {
            var id: [32]u8 = @splat(3);
            id[0] = @intCast((row % 16) << 4);
            std.mem.writeInt(u64, id[24..32], row, .big);
            try builder.addWithInputs(.{ .id = id, .values = @splat(if (step != 0 and row == 0) @as(u8, 7) else 6) }, .{ .file = 0, .group = 0, .row = if (step == 2) 127 - row else row }, &.{ @splat(4), @splat(if (step != 0 and row == 0) @as(u8, 6) else 5) });
        }
        var root = try builder.finish(@splat(1), @splat(2), @splat(3));
        root.values_format = 2;
        const next = (try prepare(a, &store, table, &physical, &provider, root, previous)).?;
        const views = next.provider.fragment_views;
        try std.testing.expectEqual(@as(usize, 2), views.len);
        for (views, 0..) |view, index| {
            var count: usize = 0;
            for (0..view.delta.count()) |i| if (view.delta.isChanged(i)) {
                count += 1;
            };
            try std.testing.expectEqual(@as(usize, if (step == 0) 16 else if (step == 1 and index == 1) 1 else 0), count);
        }
        // Selecting a cohort must preserve a file-major reached frontier.
        var partial_source = next.source;
        partial_source.inventory.files = partial_source.inventory.files[0..3];
        var append_work: Provider.ResumeWork = .{ .file = 0, .end = 3 };
        var partial = next.provider;
        partial.resume_work = &append_work;
        var selected = partial.forIndex(&partial_source, views[0].name);
        try std.testing.expect(selected.native_delta == null);
        var order = @import("lake_index_native_state.zig").ScanOrder.init(&selected);
        for (0..3) |ordinal| try std.testing.expectEqual(@as(?usize, ordinal), try order.next());
        try std.testing.expectEqual(@as(?usize, null), try order.next());
        try std.testing.expectEqualSlices(u8, &(try views[0].delta.file(0)).digest, &try @import("lake_index_native_state.zig").identity(a, &selected, partial_source.inventory.files[0]));
        try std.testing.expectEqual(@as(u64, if (step == 0) 256 else if (step == 1) 8 else 0), next.input_rows_hashed);
        try std.testing.expectEqual(@as(u64, if (step == 0) 128 else if (step == 1) 8 else 0), next.census_rows_hashed);
        root.native_space = next.space;
        const bindings = try metadata.allocator().alloc(identity.ReusedIndex, 2);
        for (bindings, views) |*binding, view| binding.* = .{ .name = view.name, .space = indexSpace(next, view.name) };
        root.reused = bindings;
        root.artifacts = @splat(7);
        try root.reseal(a);
        try root.validate(a);
        if (last) |value| value.deinit();
        last = next;
        previous = root;
    }
}
