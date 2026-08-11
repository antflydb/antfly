// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Pure path-fact constraint encoding used by distributed query control.
//! The physical Index implementation remains in `index.zig`.

const std = @import("std");
const Allocator = std.mem.Allocator;
const token = @import("token.zig");

pub const path_fact_exists_constraint_value = "pathfact-exists:v1";
const path_fact_any_constraint_tag = "pathfact-any:v1";
const path_fact_prefix_constraint_tag = "pathfact-prefix:v1";
const path_fact_match_constraint_tag = "pathfact-match:v1";
const path_fact_wildcard_constraint_tag = "pathfact-wildcard:v1";
const path_fact_regexp_constraint_tag = "pathfact-regexp:v1";
const path_fact_fuzzy_constraint_tag = "pathfact-fuzzy:v1";
const path_fact_numeric_range_constraint_tag = "pathfact-numeric-range:v1";
const path_fact_term_range_constraint_tag = "pathfact-term-range:v1";
const path_fact_date_range_constraint_tag = "pathfact-date-range:v1";
const path_fact_ip_range_constraint_tag = "pathfact-ip-range:v1";
const path_fact_geo_bbox_constraint_tag = "pathfact-geo-bbox:v1";
const path_fact_geo_distance_constraint_tag = "pathfact-geo-distance:v1";
const path_fact_geo_shape_constraint_tag = "pathfact-geo-shape:v1";

fn validGeoLatitude(lat: f64) bool {
    return std.math.isFinite(lat) and lat >= -90.0 and lat <= 90.0;
}

fn validGeoLongitude(lon: f64) bool {
    return std.math.isFinite(lon) and lon >= -180.0 and lon <= 180.0;
}

pub fn pathFactStringPrefixConstraintValueAlloc(alloc: Allocator, prefix: []const u8) ![]u8 {
    return token.canonicalTupleAlloc(alloc, &.{ path_fact_prefix_constraint_tag, "string", prefix });
}

pub fn pathFactAnyConstraintValueAlloc(alloc: Allocator, typed_values: []const []const u8) ![]u8 {
    if (typed_values.len == 0) return error.InvalidAlgebraicTensorExpr;
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    try parts.append(alloc, path_fact_any_constraint_tag);
    try parts.appendSlice(alloc, typed_values);
    return token.canonicalTupleAlloc(alloc, parts.items);
}

pub fn pathFactStringMatchConstraintValueAlloc(alloc: Allocator, text: []const u8) ![]u8 {
    return token.canonicalTupleAlloc(alloc, &.{ path_fact_match_constraint_tag, "string", text });
}

pub fn pathFactStringWildcardConstraintValueAlloc(alloc: Allocator, pattern: []const u8) ![]u8 {
    return token.canonicalTupleAlloc(alloc, &.{ path_fact_wildcard_constraint_tag, "string", pattern });
}

pub fn pathFactStringRegexpConstraintValueAlloc(alloc: Allocator, pattern: []const u8) ![]u8 {
    return token.canonicalTupleAlloc(alloc, &.{ path_fact_regexp_constraint_tag, "string", pattern });
}

pub fn pathFactStringFuzzyConstraintValueAlloc(alloc: Allocator, term: []const u8, max_edits: u8, prefix_len: u8) ![]u8 {
    const max_edits_text = try std.fmt.allocPrint(alloc, "{d}", .{max_edits});
    defer alloc.free(max_edits_text);
    const prefix_len_text = try std.fmt.allocPrint(alloc, "{d}", .{prefix_len});
    defer alloc.free(prefix_len_text);
    return token.canonicalTupleAlloc(alloc, &.{ path_fact_fuzzy_constraint_tag, "string", term, max_edits_text, prefix_len_text });
}

pub fn pathFactNumericRangeConstraintValueAlloc(alloc: Allocator, min: ?f64, max: ?f64, inclusive_min: bool, inclusive_max: bool) ![]u8 {
    const min_text = if (min) |value| try std.fmt.allocPrint(alloc, "{d}", .{value}) else try alloc.dupe(u8, "");
    defer alloc.free(min_text);
    const max_text = if (max) |value| try std.fmt.allocPrint(alloc, "{d}", .{value}) else try alloc.dupe(u8, "");
    defer alloc.free(max_text);
    return token.canonicalTupleAlloc(alloc, &.{
        path_fact_numeric_range_constraint_tag, "number",                        min_text, max_text,
        if (inclusive_min) "1" else "0",        if (inclusive_max) "1" else "0",
    });
}

pub fn pathFactTermRangeConstraintValueAlloc(alloc: Allocator, min: ?[]const u8, max: ?[]const u8, inclusive_min: bool, inclusive_max: bool) ![]u8 {
    return token.canonicalTupleAlloc(alloc, &.{
        path_fact_term_range_constraint_tag, "string",                        min orelse "", max orelse "",
        if (inclusive_min) "1" else "0",     if (inclusive_max) "1" else "0",
    });
}

pub fn pathFactDateRangeConstraintValueAlloc(alloc: Allocator, start: ?[]const u8, end: ?[]const u8, inclusive_start: bool, inclusive_end: bool) ![]u8 {
    return token.canonicalTupleAlloc(alloc, &.{
        path_fact_date_range_constraint_tag, "datetime",                      start orelse "", end orelse "",
        if (inclusive_start) "1" else "0",   if (inclusive_end) "1" else "0",
    });
}

pub fn pathFactIpRangeConstraintValueAlloc(alloc: Allocator, cidr: []const u8) ![]u8 {
    return token.canonicalTupleAlloc(alloc, &.{ path_fact_ip_range_constraint_tag, "ipv4", cidr });
}

pub fn pathFactGeoBBoxConstraintValueAlloc(alloc: Allocator, min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) ![]u8 {
    if (!validGeoLatitude(min_lat) or !validGeoLatitude(max_lat) or !validGeoLongitude(min_lon) or !validGeoLongitude(max_lon) or min_lat > max_lat)
        return error.InvalidAlgebraicTensorExpr;
    const min_lat_text = try std.fmt.allocPrint(alloc, "{d}", .{min_lat});
    defer alloc.free(min_lat_text);
    const min_lon_text = try std.fmt.allocPrint(alloc, "{d}", .{min_lon});
    defer alloc.free(min_lon_text);
    const max_lat_text = try std.fmt.allocPrint(alloc, "{d}", .{max_lat});
    defer alloc.free(max_lat_text);
    const max_lon_text = try std.fmt.allocPrint(alloc, "{d}", .{max_lon});
    defer alloc.free(max_lon_text);
    return token.canonicalTupleAlloc(alloc, &.{ path_fact_geo_bbox_constraint_tag, "geo_point", min_lat_text, min_lon_text, max_lat_text, max_lon_text });
}

pub fn pathFactGeoDistanceConstraintValueAlloc(alloc: Allocator, lat: f64, lon: f64, radius_meters: f64) ![]u8 {
    if (!validGeoLatitude(lat) or !validGeoLongitude(lon) or !std.math.isFinite(radius_meters) or radius_meters < 0)
        return error.InvalidAlgebraicTensorExpr;
    const lat_text = try std.fmt.allocPrint(alloc, "{d}", .{lat});
    defer alloc.free(lat_text);
    const lon_text = try std.fmt.allocPrint(alloc, "{d}", .{lon});
    defer alloc.free(lon_text);
    const radius_text = try std.fmt.allocPrint(alloc, "{d}", .{radius_meters});
    defer alloc.free(radius_text);
    return token.canonicalTupleAlloc(alloc, &.{ path_fact_geo_distance_constraint_tag, "geo_point", lat_text, lon_text, radius_text });
}

pub fn pathFactGeoShapeConstraintValueAlloc(alloc: Allocator, relation: []const u8, polygons: anytype) ![]u8 {
    if (!std.mem.eql(u8, relation, "intersects") and !std.mem.eql(u8, relation, "within")) return error.InvalidAlgebraicTensorExpr;
    if (polygons.len == 0) return error.InvalidAlgebraicTensorExpr;
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    var owned = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned.items) |item| alloc.free(item);
        owned.deinit(alloc);
    }
    try parts.appendSlice(alloc, &.{ path_fact_geo_shape_constraint_tag, "geo_point", relation });
    const polygon_count = try std.fmt.allocPrint(alloc, "{d}", .{polygons.len});
    try owned.append(alloc, polygon_count);
    try parts.append(alloc, polygon_count);
    for (polygons) |polygon| {
        if (polygon.len < 3) return error.InvalidAlgebraicTensorExpr;
        const point_count = try std.fmt.allocPrint(alloc, "{d}", .{polygon.len});
        try owned.append(alloc, point_count);
        try parts.append(alloc, point_count);
        for (polygon) |point| {
            if (!validGeoLatitude(point.lat) or !validGeoLongitude(point.lon)) return error.InvalidAlgebraicTensorExpr;
            const lat_text = try std.fmt.allocPrint(alloc, "{d}", .{point.lat});
            try owned.append(alloc, lat_text);
            try parts.append(alloc, lat_text);
            const lon_text = try std.fmt.allocPrint(alloc, "{d}", .{point.lon});
            try owned.append(alloc, lon_text);
            try parts.append(alloc, lon_text);
        }
    }
    return token.canonicalTupleAlloc(alloc, parts.items);
}
