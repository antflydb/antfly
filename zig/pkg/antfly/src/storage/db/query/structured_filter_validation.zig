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

//! Schema-independent admission for the structured-filter wire grammar.
//! This is deliberately separate from the physical query executor: API and
//! distributed-control units must be able to reject malformed wire values
//! without compiling DB, indexes, stored-document matching, or graph search.

const std = @import("std");
const pattern_filter_contract = @import("../../../search/pattern_filter_contract.zig");

const max_tree_depth: u8 = 64;
const max_tree_nodes: usize = 16 * 1024;
const max_leaf_values: usize = 16 * 1024;

pub fn validateStructuredFilterValueAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !void {
    _ = alloc;
    var remaining_nodes: usize = max_tree_nodes;
    try validateNode(value, 0, &remaining_nodes);
}

fn validateNode(value: std.json.Value, depth: u8, remaining_nodes: *usize) anyerror!void {
    if (depth >= max_tree_depth or remaining_nodes.* == 0) return error.InvalidArgument;
    _ = try pattern_filter_contract.requireSingleRoot(value);
    remaining_nodes.* -= 1;

    if (value.object.get("match_all") != null or value.object.get("match_none") != null) return;
    if (value.object.get("ref")) |reference| {
        if (reference != .string or reference.string.len == 0) return error.InvalidArgument;
        return;
    }
    if (value.object.get("doc_id")) |doc_id| return validateDocIds(doc_id);
    inline for ([_][]const u8{ "conjuncts", "disjuncts" }) |key| {
        if (value.object.get(key)) |children| return validateChildren(children, depth + 1, remaining_nodes);
    }
    if (value.object.get("bool")) |bool_query| {
        if (bool_query != .object) return error.InvalidArgument;
        try pattern_filter_contract.validateBool(bool_query.object);
        var should_len: usize = 0;
        var has_required = false;
        inline for ([_][]const u8{ "filter", "must", "should", "must_not" }) |key| {
            if (bool_query.object.get(key)) |children| {
                try validateChildren(children, depth + 1, remaining_nodes);
                if (comptime std.mem.eql(u8, key, "should")) should_len = children.array.items.len;
                if (comptime std.mem.eql(u8, key, "filter") or std.mem.eql(u8, key, "must")) has_required = true;
            }
        }
        _ = try pattern_filter_contract.minimumShould(bool_query.object, should_len, has_required);
        return;
    }
    if (value.object.get("match")) |match| return validateMatch(match);
    if (value.object.get("term")) |term| return validateFieldScalar(term, "term");
    if (value.object.get("terms")) |terms| return validateFieldScalars(terms);
    if (value.object.get("prefix")) |prefix| return validateFieldString(prefix, "prefix");
    if (value.object.get("wildcard")) |wildcard| return validateFieldString(wildcard, "pattern");
    if (value.object.get("regexp")) |regexp| return validateFieldString(regexp, "pattern");
    if (value.object.get("fuzzy")) |fuzzy| return validateFuzzy(fuzzy);
    if (value.object.get("exists")) |exists| return validateExists(exists);
    if (value.object.get("range")) |range| return validateStandardRange(range);
    if (value.object.get("numeric_range")) |range| return validateNumericRange(range);
    if (value.object.get("date_range")) |range| return validateDateRange(range);
    if (value.object.get("bool_field")) |query| return validateBoolField(query);
    if (value.object.get("term_range")) |range| return validateTermRange(range);
    if (value.object.get("ip_range")) |range| return validateIpRange(range);
    if (value.object.get("geo_distance")) |query| return validateGeoDistance(query);
    if (value.object.get("geo_bbox")) |query| return validateGeoBBox(query);
    if (value.object.get("geo_shape")) |query| return validateGeoShape(query);
    return error.UnsupportedQueryRequest;
}

fn validateChildren(value: std.json.Value, depth: u8, remaining_nodes: *usize) anyerror!void {
    if (value != .array or value.array.items.len == 0 or value.array.items.len > remaining_nodes.*) {
        return error.InvalidArgument;
    }
    for (value.array.items) |child| try validateNode(child, depth, remaining_nodes);
}

fn validateDocIds(value: std.json.Value) !void {
    const ids = switch (value) {
        .object => value.object.get("ids") orelse return error.InvalidArgument,
        .array => value,
        else => return error.InvalidArgument,
    };
    if (ids != .array or ids.array.items.len == 0 or ids.array.items.len > max_leaf_values) {
        return error.InvalidArgument;
    }
    for (ids.array.items) |id| if (id != .string) return error.InvalidArgument;
}

fn fieldOrPath(object: std.json.ObjectMap) ![]const u8 {
    const field = object.get("field");
    const path = object.get("path");
    if (field != null and path != null) {
        if (field.? != .string or path.? != .string or !std.mem.eql(u8, field.?.string, path.?.string)) {
            return error.InvalidArgument;
        }
        return field.?.string;
    }
    const value = field orelse path orelse return error.InvalidArgument;
    if (value != .string) return error.InvalidArgument;
    return value.string;
}

fn scalar(value: std.json.Value) !void {
    switch (value) {
        .null, .bool, .integer, .float, .number_string, .string => {},
        .object, .array => return error.InvalidArgument,
    }
}

fn validateFieldScalar(value: std.json.Value, value_key: []const u8) !void {
    if (value != .object) return error.InvalidArgument;
    if (value.object.get("field") != null or value.object.get("path") != null) {
        _ = try fieldOrPath(value.object);
        return scalar(value.object.get(value_key) orelse value.object.get("value") orelse return error.InvalidArgument);
    }
    if (value.object.count() != 1) return error.InvalidArgument;
    var it = value.object.iterator();
    const entry = it.next() orelse return error.InvalidArgument;
    if (entry.key_ptr.len == 0) return error.InvalidArgument;
    try scalar(entry.value_ptr.*);
}

fn validateFieldString(value: std.json.Value, value_key: []const u8) !void {
    if (value != .object) return error.InvalidArgument;
    if (value.object.get("field") != null or value.object.get("path") != null) {
        _ = try fieldOrPath(value.object);
        const raw = value.object.get(value_key) orelse value.object.get("value") orelse return error.InvalidArgument;
        if (raw != .string) return error.InvalidArgument;
        return;
    }
    if (value.object.count() != 1) return error.InvalidArgument;
    var it = value.object.iterator();
    const entry = it.next() orelse return error.InvalidArgument;
    if (entry.key_ptr.len == 0 or entry.value_ptr.* != .string) return error.InvalidArgument;
}

fn validateFieldScalars(value: std.json.Value) !void {
    if (value != .object) return error.InvalidArgument;
    const raw = if (value.object.get("field") != null or value.object.get("path") != null) blk: {
        _ = try fieldOrPath(value.object);
        break :blk value.object.get("values") orelse value.object.get("terms") orelse return error.InvalidArgument;
    } else blk: {
        if (value.object.count() != 1) return error.InvalidArgument;
        var it = value.object.iterator();
        const entry = it.next() orelse return error.InvalidArgument;
        if (entry.key_ptr.len == 0) return error.InvalidArgument;
        break :blk entry.value_ptr.*;
    };
    if (raw != .array or raw.array.items.len == 0 or raw.array.items.len > max_leaf_values) return error.InvalidArgument;
    for (raw.array.items) |item| try scalar(item);
}

fn validateMatch(value: std.json.Value) !void {
    if (value != .object) return error.InvalidArgument;
    const field = value.object.get("field");
    const path = value.object.get("path");
    if (field == null and path == null) {
        if (value.object.count() != 1) return error.InvalidArgument;
        var it = value.object.iterator();
        const entry = it.next() orelse return error.InvalidArgument;
        if (entry.key_ptr.len == 0 or entry.value_ptr.* != .string) return error.InvalidArgument;
        return;
    }
    _ = try fieldOrPath(value.object);
    const text = value.object.get("text");
    const alias = value.object.get("value");
    if ((text == null) == (alias == null)) return error.InvalidArgument;
    if ((text orelse alias.?) != .string) return error.InvalidArgument;
    if (value.object.get("analyzer")) |analyzer| if (analyzer != .string or analyzer.string.len == 0) return error.InvalidArgument;
    var recognized: usize = 2;
    if (value.object.get("analyzer") != null) recognized += 1;
    if (recognized != value.object.count()) return error.InvalidArgument;
}

fn validateFuzzy(value: std.json.Value) !void {
    if (value != .object) return error.InvalidArgument;
    var options: ?std.json.ObjectMap = null;
    if (value.object.get("field") != null or value.object.get("path") != null) {
        _ = try fieldOrPath(value.object);
        const query = value.object.get("query") orelse value.object.get("value") orelse return error.InvalidArgument;
        if (query != .string) return error.InvalidArgument;
        options = value.object;
    } else {
        if (value.object.count() != 1) return error.InvalidArgument;
        var it = value.object.iterator();
        const entry = it.next() orelse return error.InvalidArgument;
        if (entry.key_ptr.len == 0) return error.InvalidArgument;
        switch (entry.value_ptr.*) {
            .string => return,
            .object => |object| {
                const query = object.get("query") orelse object.get("value") orelse return error.InvalidArgument;
                if (query != .string) return error.InvalidArgument;
                options = object;
            },
            else => return error.InvalidArgument,
        }
    }
    const object = options.?;
    if (object.get("max_edits")) |edits| {
        const parsed = try jsonU8(edits);
        if (parsed > pattern_filter_contract.max_fuzzy_edits) return error.InvalidArgument;
    }
    if (object.get("prefix_length")) |prefix| _ = try jsonU8(prefix);
    if (object.get("auto_fuzzy")) |auto| if (auto != .bool) return error.InvalidArgument;
}

fn validateExists(value: std.json.Value) !void {
    switch (value) {
        .string => {},
        .object => _ = try fieldOrPath(value.object),
        else => return error.InvalidArgument,
    }
}

const RangeBound = struct { value: std.json.Value, inclusive: bool };

fn validateStandardRange(value: std.json.Value) !void {
    if (value != .object) return error.InvalidArgument;
    const bounds = if (value.object.get("field") != null or value.object.get("path") != null) blk: {
        _ = try fieldOrPath(value.object);
        break :blk value.object;
    } else blk: {
        if (value.object.count() != 1) return error.InvalidArgument;
        var it = value.object.iterator();
        const entry = it.next() orelse return error.InvalidArgument;
        if (entry.key_ptr.len == 0 or entry.value_ptr.* != .object) return error.InvalidArgument;
        break :blk entry.value_ptr.object;
    };
    var lower: ?RangeBound = null;
    var upper: ?RangeBound = null;
    if (bounds.get("gte")) |bound| try setBound(&lower, bound, true);
    if (bounds.get("gt")) |bound| try setBound(&lower, bound, false);
    if (bounds.get("from")) |bound| try setBound(&lower, bound, try boolOrDefault(bounds.get("include_lower"), true));
    if (bounds.get("min")) |bound| try setBound(&lower, bound, try boolOrDefault(bounds.get("inclusive_min"), true));
    if (bounds.get("lte")) |bound| try setBound(&upper, bound, true);
    if (bounds.get("lt")) |bound| try setBound(&upper, bound, false);
    if (bounds.get("to")) |bound| try setBound(&upper, bound, try boolOrDefault(bounds.get("include_upper"), true));
    if (bounds.get("max")) |bound| try setBound(&upper, bound, try boolOrDefault(bounds.get("inclusive_max"), false));
    if (lower == null and upper == null) return error.InvalidArgument;
    if (lower) |bound| try rangeScalar(bound.value);
    if (upper) |bound| try rangeScalar(bound.value);
}

fn validateNumericRange(value: std.json.Value) !void {
    const object = try rangeObject(value);
    const min = object.get("min");
    const max = object.get("max");
    if (min == null and max == null) return error.InvalidArgument;
    if (min) |bound| _ = try finiteNumber(bound);
    if (max) |bound| _ = try finiteNumber(bound);
    _ = try boolOrDefault(object.get("inclusive_min"), true);
    _ = try boolOrDefault(object.get("inclusive_max"), false);
}

fn validateDateRange(value: std.json.Value) !void {
    const object = try rangeObject(value);
    const start = object.get("start_ns") orelse object.get("start");
    const end = object.get("end_ns") orelse object.get("end");
    if (start == null and end == null) return error.InvalidArgument;
    if (start) |bound| try dateBound(bound);
    if (end) |bound| try dateBound(bound);
    _ = try boolOrDefault(object.get("inclusive_start"), true);
    _ = try boolOrDefault(object.get("inclusive_end"), false);
}

fn validateBoolField(value: std.json.Value) !void {
    const object = try rangeObject(value);
    const expected = object.get("value") orelse return error.InvalidArgument;
    if (expected != .bool) return error.InvalidArgument;
}

fn validateTermRange(value: std.json.Value) !void {
    const object = try rangeObject(value);
    const min = object.get("min");
    const max = object.get("max");
    if (min == null and max == null) return error.InvalidArgument;
    if (min) |bound| try scalar(bound);
    if (max) |bound| try scalar(bound);
    _ = try boolOrDefault(object.get("inclusive_min"), true);
    _ = try boolOrDefault(object.get("inclusive_max"), false);
}

fn validateIpRange(value: std.json.Value) !void {
    const object = try rangeObject(value);
    const cidr = object.get("cidr") orelse return error.InvalidArgument;
    if (cidr != .string or cidr.string.len == 0) return error.InvalidArgument;
}

fn validateGeoDistance(value: std.json.Value) !void {
    const object = try rangeObject(value);
    const lon = try finiteNumber(object.get("lon") orelse return error.InvalidArgument);
    const lat = try finiteNumber(object.get("lat") orelse return error.InvalidArgument);
    const radius = try finiteNumber(object.get("radius_meters") orelse return error.InvalidArgument);
    if (lat < -90 or lat > 90 or lon < -180 or lon > 180 or radius < 0) return error.InvalidArgument;
}

fn validateGeoBBox(value: std.json.Value) !void {
    const object = try rangeObject(value);
    const min_lat = try finiteNumber(object.get("min_lat") orelse return error.InvalidArgument);
    const min_lon = try finiteNumber(object.get("min_lon") orelse return error.InvalidArgument);
    const max_lat = try finiteNumber(object.get("max_lat") orelse return error.InvalidArgument);
    const max_lon = try finiteNumber(object.get("max_lon") orelse return error.InvalidArgument);
    if (min_lat < -90 or min_lat > 90 or max_lat < -90 or max_lat > 90 or
        min_lon < -180 or min_lon > 180 or max_lon < -180 or max_lon > 180 or min_lat > max_lat)
    {
        return error.InvalidArgument;
    }
}

fn validateGeoShape(value: std.json.Value) !void {
    const object = try rangeObject(value);
    const geometry = object.get("geometry") orelse object.get("shape") orelse return error.InvalidArgument;
    if (geometry != .object) return error.InvalidArgument;
    if (object.get("relation")) |relation| {
        if (relation != .string or
            (!std.mem.eql(u8, relation.string, "intersects") and
                !std.mem.eql(u8, relation.string, "within") and
                !std.mem.eql(u8, relation.string, "contains")))
        {
            return error.InvalidArgument;
        }
    }
}

fn rangeObject(value: std.json.Value) !std.json.ObjectMap {
    if (value != .object) return error.InvalidArgument;
    _ = try fieldOrPath(value.object);
    return value.object;
}

fn setBound(found: *?RangeBound, value: std.json.Value, inclusive: bool) !void {
    if (found.* != null or value == .null) return error.InvalidArgument;
    found.* = .{ .value = value, .inclusive = inclusive };
}

fn boolOrDefault(value: ?std.json.Value, default: bool) !bool {
    const actual = value orelse return default;
    if (actual != .bool) return error.InvalidArgument;
    return actual.bool;
}

fn rangeScalar(value: std.json.Value) !void {
    switch (value) {
        .integer, .string => {},
        .float => |number| if (!std.math.isFinite(number)) return error.InvalidArgument,
        .number_string => |text| {
            const number = std.fmt.parseFloat(f64, text) catch return error.InvalidArgument;
            if (!std.math.isFinite(number)) return error.InvalidArgument;
        },
        else => return error.InvalidArgument,
    }
}

fn dateBound(value: std.json.Value) !void {
    switch (value) {
        .integer, .string => {},
        .float => |number| if (!std.math.isFinite(number) or @round(number) != number) return error.InvalidArgument,
        else => return error.InvalidArgument,
    }
}

fn finiteNumber(value: std.json.Value) !f64 {
    const number: f64 = switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| float,
        .number_string => |text| std.fmt.parseFloat(f64, text) catch return error.InvalidArgument,
        else => return error.InvalidArgument,
    };
    if (!std.math.isFinite(number)) return error.InvalidArgument;
    return number;
}

fn jsonU8(value: std.json.Value) !u8 {
    return switch (value) {
        .integer => |number| std.math.cast(u8, number) orelse error.InvalidArgument,
        .float => |number| blk: {
            if (!std.math.isFinite(number) or @round(number) != number) return error.InvalidArgument;
            const parsed: i64 = @intFromFloat(number);
            break :blk std.math.cast(u8, parsed) orelse error.InvalidArgument;
        },
        else => error.InvalidArgument,
    };
}

test "control structured-filter admission covers canonical compounds and typed leaves" {
    const alloc = std.testing.allocator;
    inline for ([_][]const u8{
        \\{"bool":{"must":[{"range":{"price":{"gte":10,"lt":20}}}],"must_not":[{"term":{"path":"/deleted","value":true}}]}}
        ,
        \\{"conjuncts":[{"term":{"status":"active"}},{"bool_field":{"path":"/published","value":true}}]}
        ,
        \\{"geo_distance":{"path":"location","lat":37.8,"lon":-122.4,"radius_meters":1000}}
        ,
        \\{"ref":"published"}
        ,
    }) |encoded| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
        defer parsed.deinit();
        try validateStructuredFilterValueAlloc(alloc, parsed.value);
    }
}

test "control structured-filter admission rejects ambiguous and malformed values" {
    const alloc = std.testing.allocator;
    inline for ([_][]const u8{
        \\{"range":{"price":{}}}
        ,
        \\{"range":{"price":{"gte":null}}}
        ,
        \\{"range":{"price":{"gte":10,"gt":11}}}
        ,
        \\{"bool":{"should":[{"match_all":{}}],"unknown":true}}
        ,
        \\{"term":{"path":"status","value":{"nested":true}}}
        ,
        \\{"ref":"","match_all":{}}
        ,
    }) |encoded| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
        defer parsed.deinit();
        try std.testing.expectError(error.InvalidArgument, validateStructuredFilterValueAlloc(alloc, parsed.value));
    }
}
