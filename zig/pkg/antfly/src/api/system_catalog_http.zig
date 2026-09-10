// Copyright 2026 Antfly, Inc.
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

//! System catalog response shapes. Authentication and scoped authorization are
//! performed by the public handler before these operations are invoked.
const std = @import("std");
const domain = @import("../system_catalog/domain.zig");
const routes = @import("../system_catalog/routes.zig");
const operation = @import("operation.zig");
pub const Response = struct {
    status: u16,
    body: []const u8,
    json: bool = true,
    metadata_mutation_outcome: ?enum { unknown } = null,
    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub const Database = struct { database_id: u64, name: []const u8, settings_json: []const u8 = "{}", tablespace_name: ?[]const u8 = null };
pub const Namespace = struct { namespace_id: u64, database_id: u64, database_name: []const u8, name: []const u8, tablespace_name: ?[]const u8 = null };
pub const Tablespace = struct { tablespace_id: u64, name: []const u8, location_json: []const u8, placement_policy_json: []const u8 };

pub fn context() operation.RequestContext {
    return .{};
}

pub fn response(alloc: std.mem.Allocator, status: u16, value: anytype) !Response {
    return .{ .status = status, .body = try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false }) };
}

pub fn failure(alloc: std.mem.Allocator, err: anyerror) !Response {
    var result = try response(alloc, domain.httpStatus(err), .{ .error_code = @errorName(err) });
    if (err == error.MetadataMutationOutcomeUnknown) result.metadata_mutation_outcome = .unknown;
    return result;
}

pub fn execute(source: anytype, alloc: std.mem.Allocator, request: operation.RequestContext, route: routes.Route, action: ?domain.Action, body: []const u8) !Response {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    if (action) |mutation_action| {
        var mutation: domain.Mutation = .{ .action = mutation_action, .kind = route.kind, .name = route.name orelse return failure(alloc, error.InvalidCatalogName), .database = route.database, .namespace = route.namespace };
        switch (mutation_action) {
            .create => if (route.kind == .tablespace and body.len > 0) {
                const Create = struct { location_json: ?[]const u8 = null, placement_policy_json: ?[]const u8 = null };
                const input = std.json.parseFromSliceLeaky(Create, a, body, .{}) catch return failure(alloc, error.InvalidCatalogMutation);
                mutation.location_json = input.location_json orelse "null";
                mutation.placement_policy = std.json.parseFromSliceLeaky(domain.PlacementPolicy, a, input.placement_policy_json orelse "{}", .{}) catch return failure(alloc, error.InvalidTablespacePlacementPolicy);
            },
            .rename => {
                const Rename = struct { name: []const u8 };
                const input = std.json.parseFromSliceLeaky(Rename, a, body, .{}) catch return failure(alloc, error.InvalidCatalogMutation);
                mutation.new_name = input.name;
            },
            .set_tablespace => if (body.len > 0) {
                const Binding = struct { tablespace_name: []const u8 };
                const input = std.json.parseFromSliceLeaky(Binding, a, body, .{}) catch return failure(alloc, error.InvalidCatalogMutation);
                mutation.tablespace = input.tablespace_name;
            },
            .drop => {},
        }
        const result = source.systemCatalog(alloc, request, .{ .mutate = .{ .mutation = mutation } }) catch |err| return failure(alloc, err);
        alloc.free(result);
        if (mutation_action == .drop or mutation_action == .rename or route.kind == .table) return .{ .status = 204, .body = &.{} };
    }
    const bytes = source.systemCatalog(a, request, .{ .read = .{ .kind = route.kind, .database = route.database, .name = route.name } }) catch |err| {
        if (action != null) return visibilityPending(alloc);
        return failure(alloc, err);
    };
    return projectSnapshot(alloc, a, route, action, bytes) catch |err| {
        // Admission has already committed. A stale/malformed projection or a
        // concurrent rename/drop must not turn that result into a rejection.
        if (action != null) return visibilityPending(alloc);
        return failure(alloc, err);
    };
}

fn visibilityPending(alloc: std.mem.Allocator) !Response {
    return response(alloc, 202, .{ .status = "committed_visibility_pending" });
}

fn projectSnapshot(alloc: std.mem.Allocator, a: std.mem.Allocator, route: routes.Route, action: ?domain.Action, bytes: []const u8) !Response {
    var state = std.json.parseFromSliceLeaky(domain.State, a, bytes, .{ .allocate = .alloc_always }) catch return error.InvalidCatalogRecord;
    var inventory = std.ArrayListUnmanaged(domain.Resource).empty;
    try inventory.appendSlice(a, state.resources);
    if (state.resources.len == 0) {
        try inventory.append(a, domain.default_database);
        try inventory.append(a, domain.default_namespace);
    }
    state.resources = inventory.items;
    var index = try domain.StateIndex.init(a, state);
    defer index.deinit(a);
    const parent_id: u64 = if (route.kind == .namespace) (state.find(.database, 0, route.database) orelse return error.DatabaseNotFound).id else 0;
    const status: u16 = if (action == .create) 201 else 200;
    if (route.name) |name| {
        const resource = state.find(route.kind, parent_id, name) orelse return error.CatalogNotFound;
        return switch (route.kind) {
            .database => response(alloc, status, databaseValue(&index, resource)),
            .namespace => response(alloc, status, try namespaceValue(&index, resource)),
            .tablespace => response(alloc, status, try tablespaceValue(a, resource)),
            .table => failure(alloc, error.InvalidCatalogMutation),
        };
    }
    switch (route.kind) {
        .database => {
            var out = std.ArrayListUnmanaged(Database).empty;
            for (state.resources) |r| if (r.kind == .database) try out.append(a, databaseValue(&index, r));
            std.mem.sort(Database, out.items, {}, struct {
                fn less(_: void, l: Database, r: Database) bool {
                    return std.mem.lessThan(u8, l.name, r.name);
                }
            }.less);
            return response(alloc, status, out.items);
        },
        .namespace => {
            var out = std.ArrayListUnmanaged(Namespace).empty;
            for (state.resources) |r| if (r.kind == .namespace and r.parent_id == parent_id) try out.append(a, try namespaceValue(&index, r));
            std.mem.sort(Namespace, out.items, {}, struct {
                fn less(_: void, l: Namespace, r: Namespace) bool {
                    return std.mem.lessThan(u8, l.name, r.name);
                }
            }.less);
            return response(alloc, status, out.items);
        },
        .tablespace => {
            var out = std.ArrayListUnmanaged(Tablespace).empty;
            for (state.resources) |r| if (r.kind == .tablespace) try out.append(a, try tablespaceValue(a, r));
            std.mem.sort(Tablespace, out.items, {}, struct {
                fn less(_: void, l: Tablespace, r: Tablespace) bool {
                    return std.mem.lessThan(u8, l.name, r.name);
                }
            }.less);
            return response(alloc, status, out.items);
        },
        .table => return failure(alloc, error.InvalidCatalogMutation),
    }
}

fn bindingName(state: *const domain.StateIndex, resource: domain.Resource) ?[]const u8 {
    if (resource.tablespace_id == 0) return null;
    return if (state.byId(.tablespace, resource.tablespace_id)) |r| r.name else null;
}
fn databaseValue(state: *const domain.StateIndex, resource: domain.Resource) Database {
    return .{ .database_id = resource.id, .name = resource.name, .tablespace_name = bindingName(state, resource) };
}
fn namespaceValue(state: *const domain.StateIndex, resource: domain.Resource) !Namespace {
    return .{ .namespace_id = resource.id, .database_id = resource.parent_id, .database_name = (state.byId(.database, resource.parent_id) orelse return error.InvalidCatalogRecord).name, .name = resource.name, .tablespace_name = bindingName(state, resource) };
}
fn tablespaceValue(alloc: std.mem.Allocator, resource: domain.Resource) !Tablespace {
    return .{ .tablespace_id = resource.id, .name = resource.name, .location_json = resource.location_json, .placement_policy_json = try std.json.Stringify.valueAlloc(alloc, resource.placement_policy, .{ .emit_null_optional_fields = false }) };
}

test "system catalog committed mutations retain success when projection fails" {
    const Source = struct {
        snapshot: ?[]const u8,
        fn systemCatalog(self: @This(), alloc: std.mem.Allocator, _: operation.RequestContext, call: domain.Call) ![]const u8 {
            return switch (call) {
                .mutate => try alloc.dupe(u8, "{}"),
                .read => try alloc.dupe(u8, self.snapshot orelse return error.Timeout),
                else => error.UnexpectedCall,
            };
        }
    };
    for ([_]?[]const u8{ null, "invalid JSON", "{}" }) |snapshot| {
        var result = try execute(Source{ .snapshot = snapshot }, std.testing.allocator, .{}, .{ .kind = .database, .name = "created" }, .create, "{}");
        defer result.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(u16, 202), result.status);
        try std.testing.expectEqualStrings("{\"status\":\"committed_visibility_pending\"}", result.body);
    }
    var result = try execute(Source{ .snapshot = "invalid JSON" }, std.testing.allocator, .{}, .{ .kind = .database, .name = "created" }, null, "");
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 500), result.status);
}
