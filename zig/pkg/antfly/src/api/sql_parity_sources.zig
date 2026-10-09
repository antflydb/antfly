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

//! Test-only routing of immutable, in-process statement inputs. This does not
//! simulate a distributed read cut: no fixture database may change during
//! capture or execution. All mutations are applied after their inputs close.
const std = @import("std");
const local = @import("antfly_local_sources");
const native = local.api_table_read_source;
const table_reads = @import("antfly_source_root").antfly_sources.table_reads;
const metadata = @import("../metadata/api.zig");
const catalog = local.system_catalog_domain;
const operation = local.api_operation;
const raft = @import("../raft/mod.zig");
const db = local.storage_db_selected_root.db;

pub fn Tables(comptime count: usize) type {
    if (count == 0 or count > 8) @compileError("bounded SQL parity table fixture required");
    return struct {
        const Self = @This();
        records: [count]local.common_topology_records.TableRecord,
        reads: [count]table_reads.BoundTableReadSource,
        captures: usize = 0,
        lookup_calls: std.atomic.Value(usize) = .init(0),
        unbounded_reads: std.atomic.Value(usize) = .init(0),
        ranges: []local.common_topology_records.RangeRecord = &.{},

        pub fn status(_: *anyopaque) !metadata.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn record(self: *Self, name: []const u8) !local.common_topology_records.TableRecord {
            for (self.records) |value| if (std.mem.eql(u8, value.name, name)) return value;
            return error.TableNotFound;
        }
        fn read(self: *Self, name: []const u8) !native.TableReadSource {
            for (&self.reads) |*value| if (std.mem.eql(u8, value.table_name, name)) return value.source();
            return error.TableNotFound;
        }
        pub fn systemCatalog(ptr: *anyopaque, a: std.mem.Allocator, context: operation.RequestContext, input: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try context.ensureActive();
            if (input == .policy_publication_status) return a.dupe(u8, "null");
            if (input == .write_validation) return std.json.Stringify.valueAlloc(a, .{ .schema_json = (try self.record(input.write_validation)).schema_json }, .{});
            if (input != .resolve_many) return error.UnexpectedCatalogCall;
            if (input.resolve_many.expected_revision) |revision| if (revision != 7) return error.CatalogGenerationChanged;
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const scratch = arena.allocator();
            if (input.resolve_many.storage_names.len != 0) {
                const names = try scratch.alloc(?[]const u8, input.resolve_many.storage_names.len);
                for (input.resolve_many.storage_names, names) |name, *logical| {
                    _ = try self.record(name);
                    logical.* = try (catalog.Target{ .table = name }).resourceNameAlloc(scratch);
                }
                return std.json.Stringify.valueAlloc(a, catalog.ResolvedMany{ .revision = 7, .tables = &.{}, .logical_names = names }, .{});
            }
            const tables = try scratch.alloc(?catalog.ResolvedTable, input.resolve_many.targets.len);
            for (input.resolve_many.targets, tables) |target, *table| {
                const value = try self.record(target.table);
                table.* = .{ .table_id = value.table_id, .name = value.name, .query_definition = if (input.resolve_many.include_query_definitions) .{ .table_id = value.table_id, .schema_json = value.schema_json, .read_schema_json = value.read_schema_json, .indexes_json = value.indexes_json } else null };
            }
            return std.json.Stringify.valueAlloc(a, catalog.ResolvedMany{ .revision = 7, .tables = tables }, .{});
        }
        pub fn snapshot(ptr: *anyopaque) !metadata.AdminSnapshot {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return .{ .status = try status(ptr), .tables = &self.records, .ranges = self.ranges, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        pub fn freeSnapshot(_: *anyopaque, _: *metadata.AdminSnapshot) void {}

        const Capture = struct {
            alloc: std.mem.Allocator,
            views: []native.RelationalReadView,
            owners: [count]?native.RelationalStatementRead = @splat(null),
            fn close(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                for (self.owners) |owner| if (owner) |value| value.deinit();
                self.alloc.free(self.views);
                self.alloc.destroy(self);
            }
        };
        fn open(ptr: *anyopaque, a: std.mem.Allocator, scans: []const native.RelationalStatementScan, consistency: raft.ReadConsistency) !native.RelationalStatementRead {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (scans.len == 0 or scans.len > 64) return error.SqlProgramLimitExceeded;
            for (scans) |request| _ = try self.read(request.table);
            const retained = try a.create(Capture);
            errdefer a.destroy(retained);
            const views = try a.alloc(native.RelationalReadView, scans.len);
            errdefer a.free(views);
            retained.* = .{ .alloc = a, .views = views };
            errdefer for (retained.owners) |owner| if (owner) |value| value.deinit();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            for (&self.reads, &retained.owners) |*bound, *owner| {
                var grouped: std.ArrayList(native.RelationalStatementScan) = .empty;
                var ordinals: std.ArrayList(usize) = .empty;
                for (scans, 0..) |request, i| if (std.mem.eql(u8, request.table, bound.table_name)) {
                    try grouped.append(arena.allocator(), request);
                    try ordinals.append(arena.allocator(), i);
                };
                if (grouped.items.len == 0) continue;
                owner.* = try bound.source().openRelationalStatement(a, grouped.items, consistency);
                try std.testing.expectEqual(ordinals.items.len, owner.*.?.views.len);
                for (ordinals.items, owner.*.?.views) |i, view| views[i] = view;
            }
            self.captures += 1;
            return .{ .ptr = retained, .views = views, .vtable = &.{ .close = Capture.close } };
        }
        fn lookup(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, key: []const u8, opts: db.types.LookupOptions, consistency: raft.ReadConsistency) !?native.LookupResponse {
            const self: *Self = @ptrCast(@alignCast(ptr));
            _ = self.lookup_calls.fetchAdd(1, .monotonic);
            return (try self.read(table)).lookup(a, table, key, opts, consistency);
        }
        fn openRead(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, from: []const u8, to: []const u8, opts: db.types.ScanOptions, consistency: raft.ReadConsistency) !?native.RelationalReadView {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (from.len == 0 and to.len == 0) _ = self.unbounded_reads.fetchAdd(1, .monotonic);
            return (try self.read(table)).openRelationalRead(a, table, from, to, opts, consistency);
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db.types.ScanOptions, _: raft.ReadConsistency) !?native.ScanResponse {
            return error.UnexpectedFallbackRead;
        }
        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db.types.SearchRequest, _: raft.ReadConsistency) !?local.api_query.QueryResponse {
            return error.UnexpectedFallbackRead;
        }
        pub fn source(self: *Self) native.TableReadSource {
            return .{ .ptr = self, .vtable = &.{ .lookup = lookup, .open_relational_read = openRead, .open_relational_statement = open, .scan = scan, .query = query } };
        }
    };
}
