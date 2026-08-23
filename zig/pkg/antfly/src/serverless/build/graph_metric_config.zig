// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Minimal, storage-independent graph metric configuration parsing for the
//! serverless publication path. Keeping this beside the builder avoids pulling
//! LMDB-backed catalog code into official serverless builds.

const std = @import("std");
const Allocator = std.mem.Allocator;
const graph_mod = @import("../../graph/graph.zig");

pub const IndexSpec = struct {
    index_name: []u8,
    configs: []graph_mod.GraphMetricConfig,

    pub fn deinit(self: *IndexSpec, alloc: Allocator) void {
        alloc.free(self.index_name);
        graph_mod.freeGraphMetricConfigs(alloc, self.configs);
        self.* = undefined;
    }
};

pub fn freeIndexSpecs(alloc: Allocator, specs: []IndexSpec) void {
    for (specs) |*spec| spec.deinit(alloc);
    if (specs.len > 0) alloc.free(specs);
}

pub fn parseIndexSpecsAlloc(alloc: Allocator, indexes_json: []const u8) ![]IndexSpec {
    if (indexes_json.len == 0) return try alloc.alloc(IndexSpec, 0);
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidIndexConfig,
    };
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidIndexConfig;

    var specs = std.ArrayListUnmanaged(IndexSpec).empty;
    errdefer {
        for (specs.items) |*spec| spec.deinit(alloc);
        specs.deinit(alloc);
    }
    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        const value = entry.value_ptr.*;
        if (value != .object) return error.InvalidIndexConfig;
        const type_value = value.object.get("type") orelse continue;
        if (type_value != .string) return error.InvalidIndexConfig;
        if (!std.mem.eql(u8, type_value.string, "graph")) continue;
        const configs = try parseMetricConfigsAlloc(alloc, value);
        var configs_moved = false;
        errdefer if (!configs_moved) graph_mod.freeGraphMetricConfigs(alloc, configs);
        if (configs.len == 0) {
            graph_mod.freeGraphMetricConfigs(alloc, configs);
            configs_moved = true;
            continue;
        }
        const index_name = try alloc.dupe(u8, entry.key_ptr.*);
        var spec = IndexSpec{ .index_name = index_name, .configs = configs };
        configs_moved = true;
        var moved = false;
        errdefer if (!moved) spec.deinit(alloc);
        try specs.append(alloc, spec);
        moved = true;
    }
    std.mem.sort(IndexSpec, specs.items, {}, struct {
        fn lessThan(_: void, a: IndexSpec, b: IndexSpec) bool {
            return std.mem.lessThan(u8, a.index_name, b.index_name);
        }
    }.lessThan);
    return try specs.toOwnedSlice(alloc);
}

fn parseMetricConfigsAlloc(alloc: Allocator, index: std.json.Value) ![]graph_mod.GraphMetricConfig {
    const metrics = index.object.get("metrics") orelse return try alloc.alloc(graph_mod.GraphMetricConfig, 0);
    if (metrics != .object) return error.InvalidIndexConfig;
    var configs = std.ArrayListUnmanaged(graph_mod.GraphMetricConfig).empty;
    errdefer {
        for (configs.items) |*cfg| {
            alloc.free(cfg.name);
            cfg.edge_filter.deinit(alloc);
        }
        configs.deinit(alloc);
    }
    var it = metrics.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) return error.InvalidIndexConfig;
        const object = entry.value_ptr.*.object;
        const enabled = if (object.get("enabled")) |value| blk: {
            if (value != .bool) return error.InvalidIndexConfig;
            break :blk value.bool;
        } else true;
        if (!enabled) continue;

        const name = entry.key_ptr.*;
        const kind = if (object.get("kind")) |value| try parseKind(value) else try parseKind(.{ .string = name });
        const refresh = if (object.get("refresh")) |value| blk: {
            if (value != .string) return error.InvalidIndexConfig;
            if (std.mem.eql(u8, value.string, "background")) break :blk graph_mod.GraphMetricRefreshMode.background;
            // Serverless publication has no synchronous refresh endpoint. Do
            // not accept a mode whose operational contract cannot be honored.
            if (std.mem.eql(u8, value.string, "manual")) return error.UnsupportedGraphMetricRefreshMode;
            return error.InvalidIndexConfig;
        } else .background;
        const damping = if (object.get("damping")) |value| try numberAsF64(value) else 0.85;
        const tolerance = if (object.get("tolerance")) |value| try numberAsF64(value) else 0.000001;
        const max_iterations = if (object.get("max_iterations")) |value| try numberAsU32(value) else 50;
        if (!(damping > 0 and damping < 1) or !(tolerance > 0) or max_iterations == 0 or max_iterations > graph_mod.graph_metric_max_iterations) return error.InvalidIndexConfig;
        const owned_name = try alloc.dupe(u8, name);
        var owned_name_moved = false;
        errdefer if (!owned_name_moved) alloc.free(owned_name);
        var edge_filter = try parseEdgeFilterAlloc(alloc, object.get("edge_filter"));
        var edge_filter_moved = false;
        errdefer if (!edge_filter_moved) edge_filter.deinit(alloc);
        var config = graph_mod.GraphMetricConfig{
            .name = owned_name,
            .kind = kind,
            .damping = damping,
            .tolerance = tolerance,
            .max_iterations = max_iterations,
            .refresh = refresh,
            .edge_filter = edge_filter,
        };
        var moved = false;
        errdefer if (!moved) {
            alloc.free(config.name);
            config.edge_filter.deinit(alloc);
        };
        owned_name_moved = true;
        edge_filter_moved = true;
        try configs.append(alloc, config);
        moved = true;
    }
    std.mem.sort(graph_mod.GraphMetricConfig, configs.items, {}, struct {
        fn lessThan(_: void, a: graph_mod.GraphMetricConfig, b: graph_mod.GraphMetricConfig) bool {
            return std.mem.lessThan(u8, a.name, b.name);
        }
    }.lessThan);
    try graph_mod.validateGraphMetricEdgeFilters(&.{}, configs.items);
    return try configs.toOwnedSlice(alloc);
}

fn parseKind(value: std.json.Value) !graph_mod.GraphMetricKind {
    if (value != .string) return error.InvalidIndexConfig;
    inline for (std.meta.fields(graph_mod.GraphMetricKind)) |field| {
        if (std.mem.eql(u8, value.string, field.name)) return @enumFromInt(field.value);
    }
    return error.InvalidIndexConfig;
}

fn parseEdgeFilterAlloc(alloc: Allocator, maybe_value: ?std.json.Value) !graph_mod.GraphMetricEdgeFilter {
    const value = maybe_value orelse return .{};
    if (value != .object) return error.InvalidIndexConfig;
    if (value.object.get("types")) |types| {
        if (types != .array or types.array.items.len == 0) return error.InvalidIndexConfig;
        const out = try alloc.alloc([]const u8, types.array.items.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |item| alloc.free(item);
            alloc.free(out);
        }
        for (types.array.items, 0..) |item, i| {
            if (item != .string or item.string.len == 0) return error.InvalidIndexConfig;
            out[i] = try alloc.dupe(u8, item.string);
            initialized += 1;
        }
        return .{ .mode = .types, .types = out };
    }
    if (value.object.get("mode")) |mode| {
        if (mode != .string or !std.mem.eql(u8, mode.string, "all")) return error.InvalidIndexConfig;
    }
    return .{};
}

fn numberAsF64(value: std.json.Value) !f64 {
    const result: f64 = switch (value) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        else => return error.InvalidIndexConfig,
    };
    if (!std.math.isFinite(result)) return error.InvalidIndexConfig;
    return result;
}

fn numberAsU32(value: std.json.Value) !u32 {
    return switch (value) {
        .integer => |number| if (number > 0 and number <= std.math.maxInt(u32)) @intCast(number) else error.InvalidIndexConfig,
        .float => |number| if (std.math.isFinite(number) and number > 0 and number <= std.math.maxInt(u32) and @floor(number) == number) @intFromFloat(number) else error.InvalidIndexConfig,
        else => error.InvalidIndexConfig,
    };
}

test "serverless graph metric configs are deterministic and honor disabled metrics" {
    const specs = try parseIndexSpecsAlloc(std.testing.allocator,
        \\{"z":{"type":"graph","metrics":{"degree":{"enabled":false},"rank":{"kind":"pagerank","edge_filter":{"types":["cites"]}}}},"a":{"type":"text"}}
    );
    defer freeIndexSpecs(std.testing.allocator, specs);
    try std.testing.expectEqual(@as(usize, 1), specs.len);
    try std.testing.expectEqualStrings("z", specs[0].index_name);
    try std.testing.expectEqual(@as(usize, 1), specs[0].configs.len);
    try std.testing.expectEqualStrings("rank", specs[0].configs[0].name);
    try std.testing.expect(specs[0].configs[0].edge_filter.includesType("cites"));

    const AllocationRunner = struct {
        fn run(alloc: Allocator, json: []const u8) !void {
            const parsed_specs = try parseIndexSpecsAlloc(alloc, json);
            defer freeIndexSpecs(alloc, parsed_specs);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, AllocationRunner.run, .{
        "{\"graph\":{\"type\":\"graph\",\"metrics\":{\"rank\":{\"kind\":\"pagerank\",\"edge_filter\":{\"types\":[\"cites\",\"mentions\"]}}}}}",
    });
}

test "serverless graph metric configs reject manual refresh" {
    try std.testing.expectError(error.UnsupportedGraphMetricRefreshMode, parseIndexSpecsAlloc(std.testing.allocator,
        \\{"graph":{"type":"graph","metrics":{"rank":{"kind":"pagerank","refresh":"manual"}}}}
    ));
}
