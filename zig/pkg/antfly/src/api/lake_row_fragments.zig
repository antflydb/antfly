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
const Fragment = struct { prefix: [32]u8, bits: u16, name: []const u8, count: u64, digest: [32]u8, dirty: bool = false, membership_dirty: bool = true, incremental: bool = false };

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
    fn replayable(raw: *anyopaque, binding: local.serverless_segment_source_binding.Binding) bool {
        const self: *Build = @ptrCast(@alignCast(raw));
        const spool = self.spool orelse return false;
        for (binding.column_bindings) |name| {
            const covered = for (spool.columns) |candidate| {
                if (std.mem.eql(u8, name, candidate)) break true;
            } else false;
            if (!covered) return false;
        }
        return true;
    }
    fn open(raw: *anyopaque, a: A, binding: local.serverless_segment_source_binding.Binding, file: ?usize, token: Token, phase: ?*const stores.IoPhase) !rows.Source {
        const self: *Build = @ptrCast(@alignCast(raw));
        if (self.spool) |spool| return spool.open(a, binding, file, token, phase);
        return openGather(raw, a, binding, file, token, phase);
    }
    fn openSelected(raw: *anyopaque, a: A, binding: local.serverless_segment_source_binding.Binding, file: usize, ordinals: []const u64, token: Token, phase: ?*const stores.IoPhase) !rows.Source {
        const self: *Build = @ptrCast(@alignCast(raw));
        if (file >= self.fragments.len) return error.InvalidLogicalRowIdentity;
        for (ordinals, 0..) |row, i| if (row >= self.fragments[file].count or (i != 0 and row <= ordinals[i - 1])) return error.InvalidLogicalRowIdentity;
        const source = try openGather(raw, a, binding, file, token, phase);
        const cursor: *Gather = @ptrCast(@alignCast(source.ctx));
        cursor.selected_rows = ordinals;
        return source;
    }
    fn openGather(raw: *anyopaque, a: A, binding: local.serverless_segment_source_binding.Binding, file: ?usize, token: Token, phase: ?*const stores.IoPhase) !rows.Source {
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
        const resolved = try identity.decodeState(self.root.current, record.key, record.value);
        self.census_rows_hashed += 1;
        hash.update(record.key);
        hash.update(&resolved.state.values);
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
                const before = if (left) |row| try identity.decodeState(self_.owner.root.current, row.key, row.value) else null;
                const after = if (right) |row| try identity.decodeState(self_.owner.root.current, row.key, row.value) else null;
                if (before != null and after != null and before.?.state.unchanged(after.?.state)) return;
                const id = (right orelse left.?).key;
                const fragment = &self_.fragments[try fragmentFor(self_.fragments, id)];
                fragment.dirty = true;
                if (before == null or after == null) fragment.membership_dirty = true;
            }
        };
        var changed: Changes = .{ .owner = self, .fragments = old_fragments };
        try tree.visitDifferences(a, pages.store(), previous.?.current.by_id, self.root.current.by_id, &changed);
        for (old_fragments) |fragment| {
            if (fragment.dirty) {
                const first = fragments.items.len;
                try planRange(self, a, &pages, fragment.prefix, fragment.bits, &fragments);
                if (fragments.items.len == first + 1 and std.mem.eql(u8, fragments.items[first].name, fragment.name)) fragments.items[first].membership_dirty = fragment.membership_dirty;
            } else try fragments.append(out, fragment);
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
    self.provider.fragment_rows = .{ .ptr = self, .open = Build.open, .replayable = Build.replayable, .open_selected = Build.openSelected };
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
    const builder = try @import("lake_row_identity_delta.zig").Builder.createWithOptions(a, &manager, &pages, names, if (prior != null) old_root else null, if (prior != null) nav.delta else null, &.{}, inventory.source_id, inventory.snapshot_id, self.root.current.compact);
    defer builder.deinit();
    for (self.fragments, builder.slots, builder.scan) |fragment, slot, changed| if (changed) {
        const upper = upperBound(fragment.prefix, fragment.bits);
        var cursor = try tree.Cursor.init(a, pages.store(), self.root.current.by_id, &fragment.prefix, if (upper) |*value| value else null);
        defer cursor.deinit();
        var row: u64 = 0;
        while (try cursor.next()) |record| {
            try Build.check(self);
            const resolved = try identity.decodeState(self.root.current, record.key, record.value);
            try builder.add(resolved.state, .{ .file = slot, .group = 0, .row = row });
            row += 1;
        }
        if (row != fragment.count) return error.InvalidLogicalRowIdentity;
    };
    self.slots = try out.dupe(u32, builder.slots);
    self.space = (try builder.finish(self.root.namespace, self.root.values_recipe, self.root.cut)).current;
    self.space.navigation = nav.ref;
    if (self.space.rows != self.root.current.rows) return error.InvalidLogicalRowIdentity;
    try prepareInputs(self, a, &pages, previous, table.indexes_json);
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
        const version = if (std.mem.startsWith(u8, file.version_id, "iv1-") or std.mem.startsWith(u8, file.version_id, "iv2-") or std.mem.startsWith(u8, file.version_id, "iv3-")) file.version_id[4..] else file.version_id;
        if (version.len != 64) return error.InvalidLogicalRowIdentity;
        var digest: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&digest, version) catch return error.InvalidLogicalRowIdentity;
        try fragments.append(out, .{ .name = try out.dupe(u8, file.file_id), .prefix = prefix.prefix, .bits = prefix.bits, .count = file.row_count, .digest = digest, .membership_dirty = false });
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
fn prepareInputs(self: *Build, a: A, pages: *Pages, previous: ?identity.Root, indexes_json: []const u8) !void {
    var configs = try std.json.parseFromSlice(std.json.Value, a, indexes_json, .{});
    defer configs.deinit();
    const out = self.arena.allocator();
    const recipes = self.root.current.input_recipes;
    const views = try out.alloc(Provider.FragmentView, recipes.len);
    const owners = try a.alloc(usize, recipes.len);
    defer a.free(owners);
    const dirty = try a.alloc([]bool, recipes.len);
    defer a.free(dirty);
    var initialized: usize = 0;
    defer for (dirty[0..initialized]) |mask| a.free(mask);
    const changed_ids = try a.alloc(std.ArrayList([32]u8), recipes.len);
    defer a.free(changed_ids);
    for (changed_ids) |*ids| ids.* = .empty;
    defer for (changed_ids) |*ids| ids.deinit(a);
    const overflow = try a.alloc(bool, recipes.len);
    defer a.free(overflow);
    @memset(overflow, false);
    for (recipes, 0..) |recipe, i| {
        owners[i] = i;
        for (recipes[0..i], 0..) |candidate, j| if (sameColumns(recipe.columns, candidate.columns) and vectorRecipe(configs.value, recipe.name) == vectorRecipe(configs.value, candidate.name)) {
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
            changed_ids: []std.ArrayList([32]u8),
            overflow: []bool,
            a: A,
            pub fn change(self_: *@This(), left: ?tree.Cursor.Record, right: ?tree.Cursor.Record) !void {
                try Build.check(self_.owner);
                if (left != null and right != null and std.mem.eql(u8, left.?.value, right.?.value)) return;
                const row = right orelse left.?;
                if (row.key.len != 64 or row.value.len != 32) return error.InvalidLogicalRowIdentity;
                for (self_.recipes, 0..) |recipe, i| if (self_.owners[i] == i and std.mem.eql(u8, &recipe.digest, row.key[0..32])) {
                    const owner = self_.owners[i];
                    self_.dirty[owner][try fragmentFor(self_.owner.fragments, row.key[32..64])] = true;
                    if (!self_.overflow[owner]) {
                        if (self_.changed_ids[owner].items.len == 32768) {
                            self_.overflow[owner] = true;
                            self_.changed_ids[owner].clearAndFree(self_.a);
                        } else try self_.changed_ids[owner].append(self_.a, row.key[32..64].*);
                    }
                };
            }
        };
        var changed: Changes = .{ .owner = self, .recipes = recipes, .owners = owners, .dirty = dirty, .changed_ids = changed_ids, .overflow = overflow, .a = a };
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
            const prior_name = for (prior.current.input_recipes) |candidate| {
                if (std.mem.eql(u8, &candidate.digest, &recipe.digest) and vectorRecipe(configs.value, candidate.name) == vectorRecipe(configs.value, recipe.name)) break candidate.name;
            } else recipe.name;
            const space = prior.spaceForIndex(prior_name);
            if (space.navigation) |ref| {
                old = try navigation.load(out, self.store, ref, self.token, null);
                // Legacy global versions lack the input-specific domain marker.
                var records = try tree.Cursor.init(a, pages.store(), old.?.files, "", null);
                defer records.deinit();
                while (try records.next()) |record| {
                    var arena = std.heap.ArenaAllocator.init(a);
                    defer arena.deinit();
                    const file = (try std.json.parseFromSliceLeaky(navigation.Record, arena.allocator(), record.value, .{})).file;
                    if (!std.mem.startsWith(u8, file.version_id, "iv1-") and !std.mem.startsWith(u8, file.version_id, "iv2-") and !std.mem.startsWith(u8, file.version_id, "iv3-")) continue;
                    const prefix = try parseName(file.file_id);
                    var digest: [32]u8 = undefined;
                    _ = std.fmt.hexToBytes(&digest, file.version_id[4..]) catch return error.InvalidLogicalRowIdentity;
                    try versions.put(a, try out.dupe(u8, file.file_id), .{ .prefix = prefix.prefix, .bits = prefix.bits, .name = "", .count = file.row_count, .digest = digest, .incremental = !std.mem.startsWith(u8, file.version_id, "iv1-") });
                }
            }
        };
        const row_changes = try changedRows(self, a, pages, previous, recipe, if (overflow[i]) null else changed_ids[i].items);
        const deltas = try a.alloc([32]u8, self.fragments.len);
        defer a.free(deltas);
        @memset(deltas, @as([32]u8, @splat(0)));
        if (previous != null and !overflow[i]) for (changed_ids[i].items) |id| {
            const file = try fragmentFor(self.fragments, &id);
            if (row_changes[file] == null) continue;
            const cached = versions.get(self.fragments[file].name) orelse continue;
            if (!cached.incremental) continue;
            const before = try inputFingerprint(a, pages.store(), previous.?.current.input_versions, recipe.digest, id);
            const after = try inputFingerprint(a, pages.store(), self.root.current.input_versions, recipe.digest, id);
            xorInput(&deltas[file], id, before orelse return error.InvalidLogicalRowIdentity);
            xorInput(&deltas[file], id, after orelse return error.InvalidLogicalRowIdentity);
            self.input_rows_hashed += 2;
        };
        const key_digests = try out.alloc([32]u8, if (vectorRecipe(configs.value, recipe.name)) self.fragments.len else 0);
        for (self.fragments[0..key_digests.len], key_digests) |fragment, *digest| {
            var hash = std.crypto.hash.Blake3.init(.{});
            hash.update("native-lake-logical-coordinate-file-v1");
            hash.update(&self.root.namespace);
            hash.update(fragment.name);
            hash.final(digest);
        }
        const files = try out.dupe(local.serverless_external_source_types.FileEntry, self.source.inventory.files);
        for (self.fragments, files, dirty[i], 0..) |fragment, *file, changed, file_ordinal| {
            const cached = versions.get(fragment.name);
            const incremental = if (row_changes[file_ordinal]) |_| cached != null and cached.?.incremental and previous != null else false;
            const digest = if (!changed and cached != null and cached.?.count == fragment.count and cached.?.incremental) cached.?.digest else if (incremental) delta_hash: {
                var digest = cached.?.digest;
                for (&digest, deltas[file_ordinal]) |*byte, delta| byte.* ^= delta;
                break :delta_hash digest;
            } else hash: {
                var hash = std.crypto.hash.Blake3.init(.{});
                hash.update("native-lake-logical-input-fragment-v2");
                hash.update(&self.root.namespace);
                hash.update(&recipe.digest);
                hash.update(&fragment.prefix);
                var bits: [2]u8 = undefined;
                std.mem.writeInt(u16, &bits, fragment.bits, .big);
                hash.update(&bits);
                var digest: [32]u8 = undefined;
                hash.final(&digest);
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
                    xorInput(&digest, record.key[32..64].*, record.value[0..32].*);
                    count += 1;
                    self.input_rows_hashed += 1;
                }
                if (count != fragment.count) return error.InvalidLogicalRowIdentity;
                break :hash digest;
            };
            file.version_id = try std.fmt.allocPrint(out, "{s}-{s}", .{ if (key_digests.len == 0) "iv2" else "iv3", std.fmt.bytesToHex(&digest, .lower) });
        }
        var source = self.source;
        source.inventory.files = files;
        var provider = self.provider;
        provider.source = &source;
        // The parent census describes the union of inputs. Recompute native
        // identities from this cohort's version descriptors.
        provider.native_delta = null;
        provider.key_digests = key_digests;
        const nav = try navigation.build(a, out, &self.store, &provider, old, self.token);
        view.* = .{ .name = recipe.name, .files = files, .delta = nav.delta, .navigation = nav.ref, .changed_rows = row_changes, .key_digests = key_digests };
    }
    self.provider.fragment_views = views;
}
fn vectorRecipe(configs: std.json.Value, name: []const u8) bool {
    if (configs != .object) return false;
    const config = configs.object.get(name) orelse return false;
    if (config != .object) return false;
    const kind = config.object.get("type") orelse return false;
    return kind == .string and std.mem.eql(u8, kind.string, "embeddings");
}
fn xorInput(digest: *[32]u8, id: [32]u8, input: [32]u8) void {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("native-lake-logical-input-row-v2");
    hash.update(&id);
    hash.update(&input);
    var row: [32]u8 = undefined;
    hash.final(&row);
    for (digest, row) |*byte, other| byte.* ^= other;
}
fn inputFingerprint(a: A, storage: tree.Store, root: ?tree.Ref, cohort: [32]u8, id: [32]u8) !?[32]u8 {
    const key = cohort ++ id;
    var cursor = try tree.Cursor.init(a, storage, root, &key, null);
    defer cursor.deinit();
    const found = (try cursor.next()) orelse return null;
    if (!std.mem.eql(u8, found.key, &key)) return null;
    if (found.value.len != 32) return error.InvalidLogicalRowIdentity;
    return found.value[0..32].*;
}
/// Optional bounded row deltas. A null file entry means rebuild that fragment.
/// Membership changes, incompatible retained coordinates and new input cohorts
/// cannot reuse row addresses and deliberately keep the fragment path.
fn changedRows(self: *Build, a: A, pages: *Pages, previous: ?identity.Root, recipe: @import("lake_row_input_recipes.zig").Recipe, ids: ?[]const [32]u8) ![]const ?[]const u64 {
    const out = self.arena.allocator();
    const result = try out.alloc(?[]const u64, self.fragments.len);
    @memset(result, null);
    const prior = previous orelse return result;
    const changes = ids orelse return result;
    const old_native = prior.native_space orelse return result;
    const old = prior.spaceForIndex(recipe.name);
    if (!std.meta.eql(old.files, old_native.files) or !std.meta.eql(old.by_id, old_native.by_id)) return result;
    if (for (prior.current.input_recipes) |candidate| {
        if (std.mem.eql(u8, &candidate.digest, &recipe.digest)) break false;
    } else true) return result;
    const positions = try a.alloc(std.ArrayList(u64), self.fragments.len);
    defer a.free(positions);
    for (positions) |*list| list.* = .empty;
    defer for (positions) |*list| list.deinit(a);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var reader: identity.Reader = .{ .root = self.root, .pages = pages };
    for (changes) |id| {
        const file = try fragmentFor(self.fragments, &id);
        if (self.fragments[file].membership_dirty) continue;
        _ = scratch.reset(.retain_capacity);
        const before = (try reader.byId(scratch.allocator(), old, id)) orelse return error.InvalidLogicalRowIdentity;
        const after = (try reader.byId(scratch.allocator(), self.space, id)) orelse return error.InvalidLogicalRowIdentity;
        if (!std.meta.eql(before.coordinate, after.coordinate) or after.coordinate.file != self.slots[file] or after.coordinate.group != 0) return error.InvalidLogicalRowIdentity;
        try positions[file].append(a, after.coordinate.row);
    }
    for (positions, 0..) |*list, file| if (list.items.len != 0) {
        std.mem.sort(u64, list.items, {}, std.sort.asc(u64));
        result[file] = try out.dupe(u64, list.items);
    };
    return result;
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
    const CachedPayload = struct { offset: u64, page: *Page, used: u64 };
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
    adoption_rows: u64 = 0,
    payload_decodes: u64 = 0,
    captured: ?*@import("lake_row_payloads.zig").Capture = null,
    decoded: [8]?CachedPayload = @splat(null),
    decoded_bytes: usize = 0,
    decoded_clock: u64 = 0,
    payload_cache_hits: u64 = 0,
    fn evictPayload(self: *Spool) bool {
        var oldest: ?usize = null;
        for (self.decoded, 0..) |entry, i| if (entry) |value| {
            if (oldest == null or value.used < self.decoded[oldest.?].?.used) oldest = i;
        };
        const i = oldest orelse return false;
        const page = self.decoded[i].?.page;
        self.decoded_bytes -= page.retainedBytes();
        self.decoded[i] = null;
        page.release();
        return true;
    }
    fn releaseBlock(raw: *anyopaque) void {
        @as(*spill.Sequential.OwnedBlock, @ptrCast(@alignCast(raw))).release();
    }
    fn decodePayload(self: *Spool, offset: u64) !*Page {
        const a = self.budget.allocator();
        const payloads = if (self.captured) |capture| &capture.payloads else &self.payloads;
        const block = try payloads.read(offset);
        var owns = true;
        defer if (owns) block.release();
        const page = try a.create(Page);
        page.* = .{ .a = a, .arena = .init(a), .columns = &.{}, .len = block.count(), .owner = .{ .ptr = block, .release_fn = releaseBlock, .retained_bytes = block.retainedBytes() } };
        owns = false;
        errdefer page.release();
        const pa = page.arena.allocator();
        const columns = try pa.alloc(rows.ColumnVector, self.columns.len);
        for (self.columns, self.specs, columns, 0..) |name, spec, *column, ordinal| {
            column.* = try block.columnVector(pa, ordinal, if (spec.kind == .decimal128) .bytes else spec.kind);
            column.name = try pa.dupe(u8, name);
            if (spec.kind == .decimal128) {
                const values = try pa.alloc(i128, block.count());
                for (values, 0..) |*value, row| value.* = if (column.nulls.isNull(row)) 0 else blk: {
                    const bytes = try column.bytesAt(row);
                    if (bytes.len != 16) return error.InvalidSqlSpill;
                    break :blk std.mem.readInt(i128, bytes[0..16], .big);
                };
                column.values = .{ .decimal128 = .{ .values = values, .precision = spec.precision, .scale = spec.scale } };
            }
        }
        page.columns = columns;
        self.payload_decodes += 1;
        return page;
    }
    fn readPayload(self: *Spool, offset: u64) !*Page {
        try self.mutex.lock(self.manager.io);
        defer self.mutex.unlock(self.manager.io);
        self.decoded_clock +|= 1;
        for (&self.decoded) |*entry| if (entry.*) |*value| if (value.offset == offset) {
            value.used = self.decoded_clock;
            self.payload_cache_hits += 1;
            value.page.retain();
            return value.page;
        };
        const page = self.decodePayload(offset) catch |err| retry: {
            const exhausted = self.budget.isExhausted() or (if (self.captured) |capture| capture.budget.isExhausted() else false);
            if (err != error.OutOfMemory or !exhausted) return err;
            // Optional residency cannot take memory from a required decode.
            while (self.evictPayload()) {}
            break :retry try self.decodePayload(offset);
        };
        const cost = page.retainedBytes();
        if (cost <= 8 * 1024 * 1024) {
            while (self.decoded_bytes + cost > 8 * 1024 * 1024) if (!self.evictPayload()) break;
            while (true) {
                for (&self.decoded) |*entry| if (entry.* == null) {
                    page.retain();
                    entry.* = .{ .offset = offset, .page = page, .used = self.decoded_clock };
                    self.decoded_bytes += cost;
                    return page;
                };
                if (!self.evictPayload()) break;
            }
        }
        return page;
    }
    fn deinit(self: *Spool) void {
        while (self.evictPayload()) {}
        if (self.initialized) {
            self.routes.deinit();
            self.payloads.close();
        }
        self.positions.deinit(self.budget.allocator());
        self.arena.deinit();
        if (self.captured) |capture| capture.deinit();
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
        var cluster = @import("lake_row_payloads.zig").Cluster.init(a, &self.manager, columns.len);
        defer cluster.deinit();
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
            const sizing = try @import("lake_row_payloads.zig").Sizer.bind(pa, batch);
            defer sizing.deinit();
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
                const keys = try ta.alloc([]const u8, count);
                const encoded = try ta.alloc([16]u8, count);
                for (coordinates[0..count], encoded, keys) |coordinate, *bytes, *key| {
                    bytes.* = coordinate.encode();
                    key.* = bytes;
                }
                var writing = try store.beginIo(1);
                defer writing.abort();
                cluster.add(ta, vectors, keys, selected[0..count], sizing) catch |err| return writing.failed(err);
                try writing.finish();
                begin = end;
            }
        }
        try cluster.finish(&self.payloads, &edits);
        self.route_root = try edits.finish(null);
        self.routes.phase = null;
    }
    /// Merge the verified ID stream with captured offsets; sort only final routes.
    /// Typed payload pages were clustered by verified ID before adoption.
    fn adopt(self: *Spool, capture: *@import("lake_row_payloads.zig").Capture, store: *stores.ArtifactStore) !void {
        if (!capture.complete or capture.rows != self.owner.root.current.rows) return error.InvalidLogicalRowIdentity;
        const a = self.budget.allocator();
        capture.routes.phase = store.io_phase;
        defer capture.routes.phase = null;
        self.columns = capture.columns;
        self.specs = capture.specs;
        capture.retain();
        self.captured = capture;
        for (self.owner.slots, 0..) |slot, ordinal| try self.positions.put(a, slot, ordinal);
        self.route_store = try @import("../serverless/graph_segment/page_store.zig").PrivatePages.beginRun(&self.routes);
        var reads: u64 = 0;
        var writes: u64 = 0;
        var pages = self.owner.pageStore(&reads, &writes);
        pages.artifacts = store;
        pages.read_phase = store.io_phase;
        // Both streams are keyed by verified logical ID. No physical census,
        // physical-to-logical sort, or source payload reread is needed.
        var edits = @import("lake_index_page_mutations.zig").Mutations.init(a, &self.manager, self.route_store);
        defer edits.deinit();
        edits.private_pages.phase = store.io_phase;
        // Verification already certified the complete capture ID census. Join
        // only selected radix ranges, coalescing adjacent dirty leaves.
        var first: usize = 0;
        while (first < self.needed.len) {
            if (!self.needed[first]) {
                first += 1;
                continue;
            }
            var end = first;
            var expected: u64 = 0;
            while (end < self.needed.len and self.needed[end]) : (end += 1) expected += self.owner.fragments[end].count;
            const upper = upperBound(self.owner.fragments[end - 1].prefix, self.owner.fragments[end - 1].bits);
            const lower = &self.owner.fragments[first].prefix;
            var logical = try tree.Cursor.init(a, pages.store(), self.owner.space.by_id, lower, if (upper) |*value| value else null);
            defer logical.deinit();
            var payload = try tree.Cursor.init(a, capture.route_store, capture.route_root, lower, if (upper) |*value| value else null);
            defer payload.deinit();
            var count: u64 = 0;
            while (try logical.next()) |row| {
                try Build.check(self.owner);
                const captured = (try payload.next()) orelse return error.InvalidLogicalRowIdentity;
                const resolved = try identity.decodeState(self.owner.space, row.key, row.value);
                if (captured.value.len != 24 or !std.mem.eql(u8, row.key, captured.key)) return error.InvalidLogicalRowIdentity;
                const file = self.positions.get(resolved.coordinate.file) orelse return error.InvalidLogicalRowIdentity;
                if (file < first or file >= end) return error.InvalidLogicalRowIdentity;
                try edits.addRecord(row.value[0..16], captured.value);
                count += 1;
            }
            if (count != expected or (try payload.next()) != null) return error.InvalidLogicalRowIdentity;
            self.adoption_rows += count;
            first = end;
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
                const page = self.owner.readPayload(items[begin].offset) catch |err| return phase.failed(err);
                chunks.append(a, page) catch |err| {
                    page.release();
                    return err;
                };
                for (items[begin..end]) |value| positions[value.position] = .{ .page = page, .row = value.row };
                begin = end;
            }
            try phase.finish();
            const projected = try ta.alloc(usize, self.binding.column_bindings.len);
            for (self.binding.column_bindings, projected) |name, *ordinal| ordinal.* = for (self.owner.columns, 0..) |candidate, ci| {
                if (std.mem.eql(u8, name, candidate)) break ci;
            } else return error.RowSourceColumnKindMismatch;
            const page = try Page.gatherProjectedPositions(a, positions[0..count], projected);
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
            return .{ .ptr = page, .release_fn = Gather.release, .retained_bytes = page.retainedBytes() };
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
    selected_rows: ?[]const u64 = null,
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
            const total = if (self.selected_rows) |selected| selected.len else fragment.count;
            if (self.row == total) {
                if (self.file != null) return null;
                self.position += 1;
                self.row = 0;
                continue;
            }
            const count: usize = @intCast(@min(self.tile_rows, total - self.row));
            const sa = self.arena.allocator();
            const refs = try sa.alloc(rows.RowRef, count);
            for (refs, self.row..) |*ref, row| ref.* = .{ .external = .{ .identity_version = self.owner.source.inventory.row_identity_version, .source_id = self.binding.source_id, .snapshot_id = self.binding.snapshot_id, .file_id = fragment.name, .row_group_ordinal = 0, .row_ordinal = if (self.selected_rows) |selected| selected[@intCast(row)] else row } };
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
        return .{ .ptr = page, .release_fn = release, .retained_bytes = page.retainedBytes() };
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
    try broadSpoolScenario(false, false, false);
}
test "external lake logical fragments reuse verification payloads without another Parquet scan" {
    try broadSpoolScenario(true, false, false);
}
test "external lake logical fragments reuse captured payloads for selective changed fragments" {
    try broadSpoolScenario(true, true, false);
}
fn broadSpoolScenario(comptime fused: bool, comptime selective: bool, comptime hashed: bool) !void {
    const a = std.testing.allocator;
    const row_count = if (hashed) 8193 else 257;
    var directory = try local.common_test_directory.TestDirectory.init("logical-fragment-spool");
    defer directory.cleanup();
    var storage = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer storage.deinit();
    var client = storage.client();
    try client.makeBucket("antfly");
    const payload = try a.alloc(u8, 1024);
    defer a.free(payload);
    @memset(payload, 'x');
    var ids: [row_count]i64 = undefined;
    var prices: [row_count]i64 = undefined;
    var bodies: [row_count][]const u8 = undefined;
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
    for (0..row_count) |row| {
        var id: [32]u8 = @splat(3);
        if (hashed) std.crypto.hash.Blake3.hash(std.mem.asBytes(&row), &id, .{}) else {
            id[0] = if (selective) @intCast((row % 16) << 4) else 0;
            std.mem.writeInt(u64, id[24..32], row, .big);
        }
        try builder.addWithInputs(.{ .id = id, .values = @splat(4) }, .{ .file = 0, .group = 0, .row = row_count - 1 - row }, &.{@splat(5)});
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
        const opening = provider.provider();
        const source = try opening.open_with_cancellation_fn.?(opening.ptr, a, .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = physical.inventory.source_id, .snapshot_id = physical.inventory.snapshot_id, .schema_fingerprint = "schema", .index_config_hash = "test", .column_bindings = &.{ "id", "body", "price" } }, .none);
        defer source.deinit(a);
        while (try source.next(a)) |batch| {
            const keys = try a.alloc([32]u8, batch.rowCount());
            defer a.free(keys);
            for (keys, 0..) |*id, row| {
                const ordinal: usize = @intCast(row_count - 1 - batch.rowRef(row).external.row_ordinal);
                id.* = @splat(3);
                if (hashed) std.crypto.hash.Blake3.hash(std.mem.asBytes(&ordinal), id, .{}) else {
                    id[0] = if (selective) @intCast((ordinal % 16) << 4) else 0;
                    std.mem.writeInt(u64, id[24..32], ordinal, .big);
                }
            }
            try captured.?.add(batch, keys, &store);
        }
        try captured.?.finish(root.current.rows);
    }
    try prepareCapturedSpool(native, &provider, &store, captured);
    try std.testing.expect(native.spool != null);
    if (fused) try std.testing.expectEqual(@as(u64, if (selective) 17 else row_count), native.spool.?.adoption_rows);
    try std.testing.expectEqual(@as(u64, if (fused) 0 else 1), native.spool.?.source_passes);
    {
        const Replay = @import("lake_index_build_replay.zig").Replay;
        var replay = Replay.init(a, &native.provider, &.{"body"});
        defer replay.deinit();
        replay.max_spill_bytes = 1;
        const bound: local.serverless_segment_source_binding.Binding = .{ .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = native.source.inventory.source_id, .snapshot_id = native.source.inventory.snapshot_id, .schema_fingerprint = native.source.inventory.schema_fingerprint, .sidecar_kind = .text, .index_config_hash = "direct-replay", .column_bindings = &.{"body"} };
        const file = for (native.fragments, 0..) |fragment, i| {
            if (fragment.count != 0 and native.spool.?.needed[i]) break i;
        } else return error.TestUnexpectedResult;
        const first = (try replay.open(a, bound, file, .none)).?;
        const second = (try replay.open(a, bound, file, .none)).?;
        defer second.deinit(a);
        const left = (try first.next(a)).?;
        const held = (try first.retainColumns(a)).?;
        defer held.release();
        first.deinit(a);
        const right = (try second.next(a)).?;
        try std.testing.expectEqual(left.rowCount(), right.rowCount());
        for (0..left.rowCount()) |row| try std.testing.expectEqualStrings(try left.columns[0].bytesAt(row), try right.columns[0].bytesAt(row));
        try std.testing.expect(replay.direct and replay.run == null);
        try std.testing.expectEqual(@as(u64, 0), replay.manager.live_bytes);
        try std.testing.expectEqual(@as(usize, 1), replay.direct_replays);
    }
    // Two independent configurations reuse the same decoded first tile even
    // when their projections differ. Closing either cursor cannot invalidate
    // the other's borrowed vectors or cached dependencies.
    {
        const bound: local.serverless_segment_source_binding.Binding = .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = physical.inventory.source_id, .snapshot_id = physical.inventory.snapshot_id, .schema_fingerprint = "schema", .column_bindings = &.{ "id", "body", "price" }, .index_config_hash = "shared-tile-test" };
        const first = try Build.open(native, a, bound, if (hashed) null else 0, .none, null);
        defer first.deinit(a);
        const first_batch = (try first.next(a)).?;
        var projected = bound;
        projected.column_bindings = &.{ "price", "id" };
        const second = try Build.open(native, a, projected, if (hashed) null else 0, .none, null);
        defer second.deinit(a);
        const before = native.spool.?.payload_decodes;
        const hits = native.spool.?.payload_cache_hits;
        const second_batch = (try second.next(a)).?;
        try std.testing.expectEqual(first_batch.rowCount(), second_batch.rowCount());
        try std.testing.expectEqual(before, native.spool.?.payload_decodes);
        try std.testing.expect(native.spool.?.payload_cache_hits > hits);
        try std.testing.expectEqual(@as(usize, 2), second_batch.columns.len);
        try std.testing.expectEqual(try first_batch.findColumn("id").?.integerAt(0), try second_batch.findColumn("id").?.integerAt(0));
    }
    if (hashed) {
        const spool = native.spool.?;
        var routes = try tree.Cursor.init(a, spool.storage(), spool.route_root, "", null);
        defer routes.deinit();
        const offset = while (try routes.next()) |record| {
            const candidate = std.mem.readInt(u64, record.value[0..8], .big);
            const resident = for (spool.decoded) |entry| {
                if (entry != null and entry.?.offset == candidate) break true;
            } else false;
            if (!resident) break candidate;
        } else return error.TestUnexpectedResult;
        // Deny the next decode until optional residency is released. Capture
        // blocks use their original allocator; standalone spools use their own.
        const budget = if (captured) |capture| &capture.budget else &spool.budget;
        const limit = budget.limit;
        budget.limit = budget.live;
        defer budget.limit = limit;
        const page = try spool.readPayload(offset);
        defer page.release();
        try std.testing.expect(page.len != 0);
    }
    for (0..2) |_| {
        native.spool.?.payload_decodes = 0;
        const input = try Build.open(native, a, .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = physical.inventory.source_id, .snapshot_id = physical.inventory.snapshot_id, .schema_fingerprint = "schema", .column_bindings = &.{ "id", "body", "price" }, .index_config_hash = "test" }, if (hashed) null else 0, .none, null);
        defer input.deinit(a);
        var count: usize = 0;
        while (try input.next(a)) |batch| {
            try std.testing.expect(batch.rowCount() <= 256);
            for (0..batch.rowCount()) |row| {
                const physical_id = try batch.findColumn("id").?.integerAt(row);
                if (!hashed) try std.testing.expectEqual(@as(i64, @intCast(row_count - 1 - count * @as(usize, if (selective) 16 else 1))), physical_id);
                try std.testing.expectEqual(payload.len, (try batch.findColumn("body").?.bytesAt(row)).len);
                const price = batch.findColumn("price").?.values.decimal128;
                try std.testing.expectEqual(@as(i32, 18), price.precision);
                try std.testing.expectEqual(@as(i32, 2), price.scale);
                try std.testing.expectEqual(@as(i128, prices[@intCast(physical_id)]), price.values[row]);
                if (!hashed) try std.testing.expectEqual(@as(u64, @intCast(count)), batch.rowRef(row).external.row_ordinal);
                count += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, if (selective) 17 else row_count), count);
        if (hashed) {
            const encoded = if (captured) |capture| capture.payloads.encoded_pages else native.spool.?.payloads.encoded_pages;
            // Random stable IDs defeated physical-order captures by >27x.
            // Allow page/window and fragment boundary overlap, independent of N.
            try std.testing.expect(native.spool.?.payload_decodes <= encoded * 4);
            try std.testing.expect(encoded <= row_count / 32 + 32);
        }
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
        if (step == 1) {
            var changed_count: usize = 0;
            for (views[1].changed_rows) |selected| if (selected) |ordinals| {
                changed_count += ordinals.len;
                try std.testing.expectEqualSlices(u64, &.{0}, ordinals);
            };
            try std.testing.expectEqual(@as(usize, 1), changed_count);
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
        try std.testing.expectEqual(@as(u64, if (step == 0) 256 else if (step == 1) 2 else 0), next.input_rows_hashed);
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

test "external lake hashed payload capture clusters spilled pages for repeated consumers" {
    try broadSpoolScenario(true, false, true);
}
test "external lake broad typed spool clusters random coordinates without verification capture" {
    try broadSpoolScenario(false, false, true);
}

test "external lake borrowed fragment leases report complete retained ownership" {
    const a = std.testing.allocator;
    const Page = local.storage_db_types.ColumnSourcePage;
    const parent = try a.create(Page);
    parent.* = .{ .a = a, .arena = .init(a), .columns = &.{}, .len = 4 };
    defer parent.release();
    const wide = try parent.arena.allocator().alloc(u8, 5 * 1024 * 1024);
    @memset(wide, 'x');
    const columns = try parent.arena.allocator().alloc(rows.ColumnVector, 2);
    columns[0] = .{ .name = "body", .values = .{ .bytes = &.{ "a", "b", "c", "d" } } };
    const wide_values = try parent.arena.allocator().alloc([]const u8, 4);
    @memset(wide_values, wide);
    columns[1] = .{ .name = "wide", .values = .{ .bytes = wide_values } };
    parent.columns = columns;
    const selected = try Page.gatherProjectedPositions(a, &.{ .{ .page = parent, .row = 0 }, .{ .page = parent, .row = 1 } }, &.{0});
    defer selected.release();
    try std.testing.expect(selected.owner != null);
    var cursor: Spool.Cursor = .{ .a = a, .owner = undefined, .binding = undefined, .file = null, .token = .none, .store = undefined, .page = selected };
    const lease = (try Spool.Cursor.retain(&cursor, a)).?;
    defer lease.release();
    try std.testing.expect(lease.retained_bytes >= selected.retainedBytes());
}
