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

//! Derived reverse bindings maintained atomically with each native resolver
//! catalog. The index stores identities, never policy or provider state.
const std = @import("std");
const directory = @import("embedded_catalog.zig");
const A = std.mem.Allocator;
pub const catalog_key = "\x00\x00__metadata__:resolvers";
pub const marker = "\x00antfly/embedded/resolver-consumers/v6/ready";
pub const epoch_key = "\x00antfly/embedded/resolver-consumers/v6/epoch";
pub fn epoch(txn: anytype) !u64 {
    const raw = txn.get(epoch_key) catch |err| return if (err == error.NotFound) 0 else err;
    return std.fmt.parseInt(u64, raw, 10);
}
fn bumpEpoch(a: A, txn: anytype) !void {
    try txn.put(epoch_key, try std.fmt.allocPrint(a, "{d}", .{try std.math.add(u64, try epoch(txn), 1)}));
}
pub const routes_prefix = "\x00antfly/embedded/resolver-consumers/v6/routes/";
pub const namespace_prefix = "\x00antfly/embedded/resolver-consumers/v6/namespace/";
pub const prefix = "\x00antfly/embedded/resolver-consumers/v1/target/";
pub var test_binding_visits: std.atomic.Value(usize) = .init(0);
pub const static_profiles = @import("../db/resolver_static_profiles.zig");
pub const profiles_prefix = "\x00antfly/embedded/resolver-consumers/v6/static/";
pub const Binding = struct { profile_id: []const u8 = "", config_generation: u64 = 0, effective_generation: u64 = 0, source: []const u8, source_id: u64, resolver: []const u8, table: []const u8, key_template: []const u8, namespace_curation_table_id: u64 = 0, namespace_curation_generation: u64 = 0, namespace_curation_missing: bool = false };
pub const work_prefix = "\x00\x00__metadata__:resolver_projection_work:v1/";
pub const cursor_key = "\x00\x00__metadata__:resolver_projection_cursor:v1";
pub const Work = struct { table: []const u8, key_template: []const u8, namespace_curation_table_id: u64 = 0, namespace_curation_generation: u64 = 0, namespace_curation_missing: bool = false };
pub const Completion = struct { key: []const u8 };
/// Jobs outlive discovery cursors and are durable publication inputs. Bind the
/// identity to its row key before either replay or portable import accepts it.
pub fn validateWork(a: A, key: []const u8, work: Work) !void {
    if (work.table.len == 0 or work.key_template.len == 0 or std.mem.indexOfScalar(u8, work.table, 0) != null or std.mem.indexOfScalar(u8, work.key_template, 0) != null) return error.InvalidMetadataBatch;
    if (work.namespace_curation_table_id == 0 and (work.namespace_curation_generation != 0 or work.namespace_curation_missing)) return error.InvalidMetadataBatch;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(try std.json.Stringify.valueAlloc(a, .{ work.table, work.key_template }, .{}), &digest, .{});
    const expected = try std.mem.concat(a, u8, &.{ work_prefix, &std.fmt.bytesToHex(digest, .lower) });
    if (!std.mem.eql(u8, key, expected)) return error.InvalidMetadataBatch;
}

fn enqueue(a: A, txn: anytype, source_prefix: []const u8, work: Work) !void {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(try std.json.Stringify.valueAlloc(a, .{ work.table, work.key_template }, .{}), &digest, .{});
    const key = try std.fmt.allocPrint(a, "{s}{s}{s}", .{ source_prefix, work_prefix, std.fmt.bytesToHex(digest, .lower) });
    var desired = work;
    if (work.namespace_curation_table_id == 0) {
        if (txn.get(key)) |raw| {
            const old = try std.json.parseFromSliceLeaky(Work, a, raw, .{});
            if (old.namespace_curation_table_id != 0) desired = old;
        } else |err| if (err != error.NotFound) return err;
    }
    try txn.put(key, try std.json.Stringify.valueAlloc(a, desired, .{}));
    try txn.put("\x00\x00__metadata__:minimum_reader_version", "18");
    if (source_prefix.len > 0) try txn.put(try std.mem.concat(a, u8, &.{ source_prefix, "\x00\x00__metadata__:minimum_reader_version" }), "18");
}
pub fn invalidateTarget(a: A, txn: anytype, table: []const u8, template: ?[]const u8) !void {
    const target = if (template) |value| try namespacePrefix(a, table, value) else try targetPrefix(a, table);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const Pending = struct { source: []const u8, work: Work };
    var seen: std.StringHashMapUnmanaged(Pending) = .empty;
    var row = try seekEntry(&cursor, target);
    while (row) |entry| {
        if (!std.mem.startsWith(u8, entry.key, target)) break;
        if (comptime @import("builtin").is_test) _ = test_binding_visits.fetchAdd(1, .monotonic);
        // Pending work survives cursor.next()/close(); its strings must too.
        const binding = try std.json.parseFromSliceLeaky(Binding, a, entry.value, .{ .allocate = .alloc_always });
        if (template == null or std.mem.eql(u8, template.?, binding.key_template)) {
            const source = if (binding.source_id == 0) "" else try std.fmt.allocPrint(a, "\x02db/embedded/tables/{d}\x00", .{binding.source_id});
            const identity = try std.json.Stringify.valueAlloc(a, .{ source, binding.table, binding.key_template }, .{});
            const slot = try seen.getOrPut(a, identity);
            if (!slot.found_existing or binding.namespace_curation_table_id != 0) slot.value_ptr.* = .{ .source = source, .work = .{ .table = binding.table, .key_template = binding.key_template, .namespace_curation_table_id = binding.namespace_curation_table_id, .namespace_curation_generation = binding.namespace_curation_generation, .namespace_curation_missing = binding.namespace_curation_missing } };
        }
        row = try nextEntry(&cursor);
    }
    var pending = seen.valueIterator();
    while (pending.next()) |item| try enqueue(a, txn, item.source, item.work);
}
pub fn authorityChanged(a: A, txn: anytype, destination: []const u8, template: []const u8) !void {
    var name: []const u8 = "default";
    if (destination.len > 0) {
        const bytes = txn.get(directory.key) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        const tables = try std.json.parseFromSliceLeaky(directory.Catalog, a, bytes, .{});
        var found = false;
        for (tables.tables) |table| if (std.mem.eql(u8, destination, try std.fmt.allocPrint(a, "\x02db/embedded/tables/{d}\x00", .{table.id}))) {
            name = table.name;
            found = true;
            break;
        };
        if (!found) return;
    }
    try invalidateTarget(a, txn, name, template);
}
const Definition = struct { name: []const u8, table: []const u8, key_template: []const u8, aliases: []const @import("antfly_resolver").KeyAlias = &.{}, suppress_json: []const u8 = "", config_generation: u64 = 0, resolution_generation: u64 = 0, merge_generation: u64 = 0, namespace_curation_table_id: u64 = 0, namespace_curation_generation: u64 = 0, namespace_curation_missing: bool = false };
fn cursorValue(comptime Result: type) type {
    const value = @typeInfo(Result).error_union.payload;
    return if (@typeInfo(value) == .optional) @typeInfo(value).optional.child else value;
}
pub fn seekEntry(cursor: anytype, key: []const u8) anyerror!?cursorValue(@TypeOf(cursor.seekAtOrAfter(key))) {
    return cursor.seekAtOrAfter(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}
pub fn nextEntry(cursor: anytype) anyerror!?cursorValue(@TypeOf(cursor.next())) {
    return cursor.next() catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}
fn definitions(a: A, raw: []const u8) ![]Definition {
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
    const rows = if (root == .array) root else root.object.get("resolvers") orelse return error.InvalidResolverConfig;
    return std.json.parseFromValueLeaky([]Definition, a, rows, .{ .ignore_unknown_fields = true });
}
pub fn targetPrefix(a: A, table: []const u8) ![]u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(table, &hash, .{});
    return std.fmt.allocPrint(a, "{s}{s}/", .{ prefix, std.fmt.bytesToHex(hash, .lower) });
}
pub fn namespacePrefix(a: A, table: []const u8, template: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(try std.json.Stringify.valueAlloc(a, .{ table, template }, .{}), &digest, .{});
    return std.fmt.allocPrint(a, "{s}{s}/", .{ namespace_prefix, std.fmt.bytesToHex(digest, .lower) });
}
pub fn profileRoot(a: A, table: []const u8, template: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a, "{s}{s}/", .{ profiles_prefix, try routes.digest(a, try std.json.Stringify.valueAlloc(a, .{ table, template }, .{})) });
}
fn profile(cfg: Definition) static_profiles.Definition {
    return .{ .key_template = cfg.key_template, .aliases = cfg.aliases, .suppress_json = cfg.suppress_json };
}
fn sameProfile(a: A, x: Definition, y: Definition) !bool {
    return std.mem.eql(u8, try profile(x).identity(a), try profile(y).identity(a));
}
fn namespaceBindingKey(a: A, source: []const u8, cfg: Definition) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(try std.json.Stringify.valueAlloc(a, .{ source, cfg.name }, .{}), &digest, .{});
    return std.fmt.allocPrint(a, "{s}{s}", .{ try namespacePrefix(a, cfg.table, cfg.key_template), std.fmt.bytesToHex(digest, .lower) });
}
const routes = @import("../db/resolver_namespace_routes.zig");
fn routeRoot(a: A, table: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a, "\x00antfly/embedded/resolver-consumers/v6/routes/{s}/", .{try routes.digest(a, table)});
}
fn updateRoute(a: A, txn: anytype, cfg: Definition, add: bool) !void {
    try routes.update(a, txn, try routeRoot(a, cfg.table), cfg.key_template, add);
}
pub fn matchingNamespacePrefixes(a: A, txn: anytype, table: []const u8, key: []const u8) ![]const []const u8 {
    const templates = try routes.matching(a, txn, try routeRoot(a, table), key);
    const result = try a.alloc([]const u8, templates.len);
    for (templates, result) |template, *value| value.* = try namespacePrefix(a, table, template);
    return result;
}
fn bindingKey(a: A, source: []const u8, cfg: Definition) ![]u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(try std.json.Stringify.valueAlloc(a, .{ source, cfg.name }, .{}), &hash, .{});
    return std.fmt.allocPrint(a, "{s}{s}", .{ try targetPrefix(a, cfg.table), std.fmt.bytesToHex(hash, .lower) });
}
fn writeBinding(a: A, txn: anytype, source_prefix: []const u8, cfg: Definition, name: []const u8, id: u64) !void {
    if (std.mem.startsWith(u8, cfg.name, "__antfly_replacement/")) return;
    const definition = profile(cfg);
    const id_profile = try definition.identity(a);
    const binding: Binding = .{ .source = name, .source_id = id, .resolver = cfg.name, .table = cfg.table, .key_template = cfg.key_template, .profile_id = id_profile, .config_generation = cfg.config_generation, .effective_generation = @max(cfg.config_generation, @max(cfg.resolution_generation, cfg.merge_generation)), .namespace_curation_table_id = cfg.namespace_curation_table_id, .namespace_curation_generation = cfg.namespace_curation_generation, .namespace_curation_missing = cfg.namespace_curation_missing };
    const encoded = try std.json.Stringify.valueAlloc(a, binding, .{});
    const scoped = try namespaceBindingKey(a, source_prefix, cfg);
    const old = txn.get(scoped) catch |err| if (err == error.NotFound) null else return err;
    if (old) |raw| {
        const previous = try std.json.parseFromSliceLeaky(Binding, a, raw, .{});
        if (!std.mem.eql(u8, previous.profile_id, id_profile)) return error.ResolverCurationConflict;
        if (std.mem.eql(u8, raw, encoded)) return;
    } else try static_profiles.synchronize(a, txn, try profileRoot(a, cfg.table, cfg.key_template), &.{}, &.{definition});
    try txn.put(try bindingKey(a, source_prefix, cfg), encoded);
    try txn.put(scoped, encoded);
    try updateRoute(a, txn, cfg, true);
}
fn writeBindings(a: A, txn: anytype, source_prefix: []const u8, raw: []const u8, name: []const u8, id: u64) !void {
    for (try definitions(a, raw)) |cfg| try writeBinding(a, txn, source_prefix, cfg, name, id);
}
/// Enroll hidden/restored namespaces and retire dropped sources in the same
/// directory transaction. Ordinary DDL never invalidates the whole index.
pub fn directoryChanged(a: A, txn: anytype, before: ?[]const u8, after: ?[]const u8) !void {
    if (!std.mem.eql(u8, before orelse "", after orelse "")) try bumpEpoch(a, txn);
    const previous = if (before) |raw| try std.json.parseFromSliceLeaky(directory.Catalog, a, raw, .{}) else directory.Catalog{};
    const next = if (after) |raw| try std.json.parseFromSliceLeaky(directory.Catalog, a, raw, .{}) else directory.Catalog{};
    var prior_ids: std.AutoHashMapUnmanaged(u64, []const u8) = .empty;
    defer prior_ids.deinit(a);
    var next_ids: std.AutoHashMapUnmanaged(u64, []const u8) = .empty;
    defer next_ids.deinit(a);
    for (previous.tables) |table| try prior_ids.put(a, table.id, table.name);
    for (next.tables) |table| try next_ids.put(a, table.id, table.name);
    for (previous.tables) |table| {
        const kept = if (next_ids.get(table.id)) |name| std.mem.eql(u8, name, table.name) else false;
        if (kept) continue;
        try invalidateTarget(a, txn, table.name, null);
        const source = try std.fmt.allocPrint(a, "\x02db/embedded/tables/{d}\x00", .{table.id});
        if (txn.get(try std.mem.concat(a, u8, &.{ source, catalog_key }))) |raw| try synchronize(a, txn, source, raw, null) else |err| if (err != error.NotFound) return err;
    }
    for (next.tables) |table| {
        const existed = if (prior_ids.get(table.id)) |name| std.mem.eql(u8, name, table.name) else false;
        if (existed) continue;
        try invalidateTarget(a, txn, table.name, null);
        const source = try std.fmt.allocPrint(a, "\x02db/embedded/tables/{d}\x00", .{table.id});
        if (txn.get(try std.mem.concat(a, u8, &.{ source, catalog_key }))) |raw| {
            try writeBindings(a, txn, source, raw, table.name, table.id);
            for (try definitions(a, raw)) |cfg| try enqueue(a, txn, source, .{ .table = cfg.table, .key_template = cfg.key_template, .namespace_curation_table_id = cfg.namespace_curation_table_id, .namespace_curation_generation = cfg.namespace_curation_generation, .namespace_curation_missing = cfg.namespace_curation_missing });
        } else |err| if (err != error.NotFound) return err;
    }
}
/// `txn` exposes the physical file view. Callers preserve their logical prefix.
pub fn synchronize(a: A, txn: anytype, source_prefix: []const u8, before: ?[]const u8, after: ?[]const u8) !void {
    if (txn.get(marker)) |_| {
        // Existing handles may predate bootstrap's in-memory reader floor.
        // Every subsequent catalog write keeps the durable floor monotonic.
        try txn.put("\x00\x00__metadata__:minimum_reader_version", "18");
        if (source_prefix.len > 0) try txn.put(try std.mem.concat(a, u8, &.{ source_prefix, "\x00\x00__metadata__:minimum_reader_version" }), "18");
    } else |err| if (err != error.NotFound) return err;
    const old = if (before) |raw| try definitions(a, raw) else &.{};
    const next = if (after) |raw| try definitions(a, raw) else &.{};
    if ((old.len != 0 or next.len != 0) and !std.mem.eql(u8, before orelse "", after orelse "")) try bumpEpoch(a, txn);
    var prior: std.StringHashMapUnmanaged(Definition) = .empty;
    var desired: std.StringHashMapUnmanaged(Definition) = .empty;
    for (old) |cfg| try prior.put(a, cfg.name, cfg);
    for (next) |cfg| try desired.put(a, cfg.name, cfg);
    for (old) |cfg| {
        if (desired.get(cfg.name)) |item| if (std.mem.eql(u8, item.table, cfg.table) and std.mem.eql(u8, item.key_template, cfg.key_template) and try sameProfile(a, item, cfg)) continue;
        if (txn.get(try namespaceBindingKey(a, source_prefix, cfg))) |_| {
            try static_profiles.synchronize(a, txn, try profileRoot(a, cfg.table, cfg.key_template), &.{profile(cfg)}, &.{});
        } else |err| if (err != error.NotFound) return err;
        try txn.delete(try bindingKey(a, source_prefix, cfg));
        try txn.delete(try namespaceBindingKey(a, source_prefix, cfg));
        {
            const namespace_scope = try namespacePrefix(a, cfg.table, cfg.key_template);
            var cursor = try txn.openCursor();
            defer cursor.close();
            const first = try seekEntry(&cursor, namespace_scope);
            if (first == null or !std.mem.startsWith(u8, first.?.key, namespace_scope)) try updateRoute(a, txn, cfg, false);
        }
    }
    var name: []const u8 = "default";
    var id: u64 = 0;
    if (source_prefix.len != 0 and next.len != 0) {
        const bytes = txn.get(directory.key) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        const tables = try std.json.parseFromSliceLeaky(directory.Catalog, a, bytes, .{});
        var found = false;
        for (tables.tables) |table| {
            const candidate = try std.fmt.allocPrint(a, "\x02db/embedded/tables/{d}\x00", .{table.id});
            if (!std.mem.eql(u8, candidate, source_prefix)) continue;
            name = table.name;
            id = table.id;
            found = true;
            break;
        }
        if (!found) return;
    }
    for (next) |cfg| {
        if (prior.get(cfg.name)) |item| if (std.mem.eql(u8, item.table, cfg.table) and std.mem.eql(u8, item.key_template, cfg.key_template) and try sameProfile(a, item, cfg)) {
            try writeBinding(a, txn, source_prefix, cfg, name, id);
            continue;
        };
        try writeBinding(a, txn, source_prefix, cfg, name, id);
        if (!std.mem.startsWith(u8, cfg.name, "__antfly_replacement/")) try enqueue(a, txn, source_prefix, .{ .table = cfg.table, .key_template = cfg.key_template });
    }
}
/// Upgrade reads catalog records directly, without opening DBs, indexes or
/// providers. Publication of the ready marker and all bindings is atomic.
pub fn bootstrap(a: A, txn: anytype) !void {
    _ = txn.get(marker) catch |err| switch (err) {
        error.NotFound => blk: {
            // Pre-bootstrap rows are only hints: an older compatible writer
            // may have changed a source before the reader fence was installed.
            {
                for ([_][]const u8{ prefix, namespace_prefix, routes_prefix, profiles_prefix }) |scope| {
                    var cursor = try txn.openCursor();
                    defer cursor.close();
                    var row = try seekEntry(&cursor, scope);
                    while (row) |entry| {
                        if (!std.mem.startsWith(u8, entry.key, scope)) break;
                        try txn.delete(entry.key);
                        row = try nextEntry(&cursor);
                    }
                }
            }
            if (txn.get(catalog_key)) |raw| try synchronize(a, txn, "", null, raw) else |e| if (e != error.NotFound) return e;
            if (txn.get(directory.key)) |raw| {
                const catalog = try std.json.parseFromSliceLeaky(directory.Catalog, a, raw, .{});
                for (catalog.tables) |table| {
                    const source = try std.fmt.allocPrint(a, "\x02db/embedded/tables/{d}\x00", .{table.id});
                    if (txn.get(try std.mem.concat(a, u8, &.{ source, catalog_key }))) |cfg| try synchronize(a, txn, source, null, cfg) else |e| if (e != error.NotFound) return e;
                }
            } else |e| if (e != error.NotFound) return e;
            var cursor = try txn.openCursor();
            defer cursor.close();
            var binding_row = try seekEntry(&cursor, prefix);
            while (binding_row) |entry| {
                if (!std.mem.startsWith(u8, entry.key, prefix)) break;
                const binding = try std.json.parseFromSliceLeaky(Binding, a, entry.value, .{ .allocate = .alloc_always });
                const source = if (binding.source_id == 0) "" else try std.fmt.allocPrint(a, "\x02db/embedded/tables/{d}\x00", .{binding.source_id});
                try enqueue(a, txn, source, .{ .table = binding.table, .key_template = binding.key_template, .namespace_curation_table_id = binding.namespace_curation_table_id, .namespace_curation_generation = binding.namespace_curation_generation, .namespace_curation_missing = binding.namespace_curation_missing });
                binding_row = try nextEntry(&cursor);
            }
            try txn.put("\x00\x00__metadata__:minimum_reader_version", "18");
            try txn.put(marker, "1");
            break :blk "1";
        },
        else => return err,
    };
}
