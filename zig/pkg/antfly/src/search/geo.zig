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

//! Geohash encoding/decoding and geo distance utilities.
//!
//! Geohashes encode geographic coordinates as base-32 strings.
//! Higher precision = smaller cell = more characters.
//! Used for geo filtering: encode points as geohash terms in inverted index,
//! query by expanding geohash prefix to find candidate cells, refine with
//! exact distance from typed GeoPoint doc values.

const std = @import("std");
const Allocator = std.mem.Allocator;
const math = std.math;

pub const GeoPoint = struct {
    lat: f64,
    lon: f64,
};

/// Earth's mean radius in meters (WGS84).
const earth_radius: f64 = 6371008.8;

/// Base-32 alphabet for geohash encoding.
const base32_chars = "0123456789bcdefghjkmnpqrstuvwxyz";

/// Decode base32 character to 5-bit value.
fn base32Decode(c: u8) ?u5 {
    for (base32_chars, 0..) |bc, i| {
        if (bc == c) return @intCast(i);
    }
    return null;
}

// ============================================================================
// Encoding / Decoding
// ============================================================================

/// Encode a geographic point as a geohash string at the given precision (1-12).
pub fn encode(point: GeoPoint, precision: u8) [12]u8 {
    var result: [12]u8 = .{0} ** 12;
    const prec = @min(precision, 12);
    if (prec == 0) return result;

    var lat_min: f64 = -90.0;
    var lat_max: f64 = 90.0;
    var lon_min: f64 = -180.0;
    var lon_max: f64 = 180.0;

    var bits: u8 = 0;
    var char_val: u5 = 0;
    var char_idx: u8 = 0;
    var is_lon = true;

    while (char_idx < prec) {
        if (is_lon) {
            const mid = (lon_min + lon_max) / 2.0;
            if (point.lon >= mid) {
                char_val |= @as(u5, 1) << @intCast(4 - bits);
                lon_min = mid;
            } else {
                lon_max = mid;
            }
        } else {
            const mid = (lat_min + lat_max) / 2.0;
            if (point.lat >= mid) {
                char_val |= @as(u5, 1) << @intCast(4 - bits);
                lat_min = mid;
            } else {
                lat_max = mid;
            }
        }
        is_lon = !is_lon;
        bits += 1;
        if (bits == 5) {
            result[char_idx] = base32_chars[char_val];
            char_idx += 1;
            bits = 0;
            char_val = 0;
        }
    }

    return result;
}

/// Decode a geohash string to the center point of its cell.
pub fn decode(hash: []const u8) GeoPoint {
    var lat_min: f64 = -90.0;
    var lat_max: f64 = 90.0;
    var lon_min: f64 = -180.0;
    var lon_max: f64 = 180.0;
    var is_lon = true;

    for (hash) |c| {
        const val = base32Decode(c) orelse break;
        for (0..5) |bit_i| {
            const bit = (val >> @intCast(4 - bit_i)) & 1;
            if (is_lon) {
                const mid = (lon_min + lon_max) / 2.0;
                if (bit == 1) lon_min = mid else lon_max = mid;
            } else {
                const mid = (lat_min + lat_max) / 2.0;
                if (bit == 1) lat_min = mid else lat_max = mid;
            }
            is_lon = !is_lon;
        }
    }

    return .{
        .lat = (lat_min + lat_max) / 2.0,
        .lon = (lon_min + lon_max) / 2.0,
    };
}

// ============================================================================
// Distance
// ============================================================================

/// Haversine distance in meters between two points.
pub fn haversineDistance(a: GeoPoint, b: GeoPoint) f64 {
    const to_rad = math.pi / 180.0;
    const dlat = (b.lat - a.lat) * to_rad;
    const dlon = (b.lon - a.lon) * to_rad;
    const lat1 = a.lat * to_rad;
    const lat2 = b.lat * to_rad;

    const sin_dlat = @sin(dlat / 2.0);
    const sin_dlon = @sin(dlon / 2.0);
    const h = sin_dlat * sin_dlat + @cos(lat1) * @cos(lat2) * sin_dlon * sin_dlon;
    return 2.0 * earth_radius * math.asin(@sqrt(h));
}

// ============================================================================
// Neighbors
// ============================================================================

/// Compute the bounding box of a geohash cell.
pub fn bounds(hash: []const u8) struct { lat_min: f64, lat_max: f64, lon_min: f64, lon_max: f64 } {
    var lat_min: f64 = -90.0;
    var lat_max: f64 = 90.0;
    var lon_min: f64 = -180.0;
    var lon_max: f64 = 180.0;
    var is_lon = true;

    for (hash) |c| {
        const val = base32Decode(c) orelse break;
        for (0..5) |bit_i| {
            const bit = (val >> @intCast(4 - bit_i)) & 1;
            if (is_lon) {
                const mid = (lon_min + lon_max) / 2.0;
                if (bit == 1) lon_min = mid else lon_max = mid;
            } else {
                const mid = (lat_min + lat_max) / 2.0;
                if (bit == 1) lat_min = mid else lat_max = mid;
            }
            is_lon = !is_lon;
        }
    }

    return .{ .lat_min = lat_min, .lat_max = lat_max, .lon_min = lon_min, .lon_max = lon_max };
}

/// Get the 8 neighboring geohash cells by computing adjacent cell centers.
pub fn neighbors(hash: []const u8) [8][12]u8 {
    const b = bounds(hash);
    const lat_span = b.lat_max - b.lat_min;
    const lon_span = b.lon_max - b.lon_min;
    const center_lat = (b.lat_min + b.lat_max) / 2.0;
    const center_lon = (b.lon_min + b.lon_max) / 2.0;
    const prec: u8 = @intCast(hash.len);

    const offsets = [8][2]f64{
        .{ -lat_span, -lon_span }, // SW
        .{ -lat_span, 0 }, // S
        .{ -lat_span, lon_span }, // SE
        .{ 0, -lon_span }, // W
        .{ 0, lon_span }, // E
        .{ lat_span, -lon_span }, // NW
        .{ lat_span, 0 }, // N
        .{ lat_span, lon_span }, // NE
    };

    var result: [8][12]u8 = undefined;
    for (offsets, 0..) |off, i| {
        var lat = center_lat + off[0];
        var lon = center_lon + off[1];
        // Clamp
        lat = @max(-89.999, @min(89.999, lat));
        if (lon > 180.0) lon -= 360.0;
        if (lon < -180.0) lon += 360.0;
        result[i] = encode(.{ .lat = lat, .lon = lon }, prec);
    }

    return result;
}

// ============================================================================
// Cell covering
// ============================================================================

/// Compute geohash cells that cover a bounding box at given precision.
/// Returns geohash strings. Caller owns returned slice.
pub fn coverBoundingBox(
    alloc: Allocator,
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
    precision: u8,
) ![][12]u8 {
    var cells = std.ArrayListUnmanaged([12]u8).empty;
    defer cells.deinit(alloc);

    // Determine cell dimensions at this precision
    const sample = encode(.{ .lat = 0, .lon = 0 }, precision);
    const sample_bounds = bounds(sample[0..precision]);
    const lat_step = sample_bounds.lat_max - sample_bounds.lat_min;
    const lon_step = sample_bounds.lon_max - sample_bounds.lon_min;

    // Step by 0.9x cell size for coverage, then add an extra step to cover the boundary
    var lat = min_lat;
    while (lat <= max_lat + lat_step) : (lat += lat_step * 0.9) {
        var lon = min_lon;
        while (lon <= max_lon + lon_step) : (lon += lon_step * 0.9) {
            // Clamp to valid range
            const cell = encode(.{
                .lat = @min(@max(lat, -90.0), 90.0),
                .lon = @min(@max(lon, -180.0), 180.0),
            }, precision);
            // Deduplicate
            var found = false;
            for (cells.items) |existing| {
                if (std.mem.eql(u8, existing[0..precision], cell[0..precision])) {
                    found = true;
                    break;
                }
            }
            if (!found) try cells.append(alloc, cell);
        }
    }

    return try alloc.dupe([12]u8, cells.items);
}

/// Compute geohash cells that cover a circle (center + radius in meters).
/// Caller owns returned slice.
pub fn coverCircle(
    alloc: Allocator,
    center: GeoPoint,
    radius_meters: f64,
    precision: u8,
) ![][12]u8 {
    // Approximate bounding box for the circle
    const lat_delta = (radius_meters / earth_radius) * (180.0 / math.pi);
    const lon_delta = lat_delta / @cos(center.lat * math.pi / 180.0);

    return coverBoundingBox(
        alloc,
        center.lat - lat_delta,
        center.lon - lon_delta,
        center.lat + lat_delta,
        center.lon + lon_delta,
        precision,
    );
}

// ============================================================================
// Point-in-polygon (ray casting)
// ============================================================================

/// Test if a point is inside a polygon using the ray casting algorithm.
/// `polygon` is a slice of GeoPoints forming a closed ring (first == last).
pub fn pointInPolygon(point: GeoPoint, polygon: []const GeoPoint) bool {
    if (polygon.len < 4) return false; // need at least 3 vertices + closing point
    var inside = false;
    const n = polygon.len;
    var j: usize = n - 1;
    for (0..n) |i| {
        const yi = polygon[i].lat;
        const xi = polygon[i].lon;
        const yj = polygon[j].lat;
        const xj = polygon[j].lon;

        // Check if the ray from (point.lon, point.lat) going east crosses edge (i, j)
        if (((yi > point.lat) != (yj > point.lat)) and
            (point.lon < (xj - xi) * (point.lat - yi) / (yj - yi) + xi))
        {
            inside = !inside;
        }
        j = i;
    }
    return inside;
}

// ============================================================================
// Tests
// ============================================================================

test "point in polygon triangle" {
    // Triangle: (0,0), (10,0), (5,10), (0,0)
    const triangle = [_]GeoPoint{
        .{ .lat = 0, .lon = 0 },
        .{ .lat = 0, .lon = 10 },
        .{ .lat = 10, .lon = 5 },
        .{ .lat = 0, .lon = 0 },
    };

    // Inside
    try std.testing.expect(pointInPolygon(.{ .lat = 2, .lon = 5 }, &triangle));
    // Outside
    try std.testing.expect(!pointInPolygon(.{ .lat = -1, .lon = 5 }, &triangle));
    try std.testing.expect(!pointInPolygon(.{ .lat = 5, .lon = 0 }, &triangle));
}

test "point in polygon rectangle" {
    // Rectangle: (0,0), (0,10), (10,10), (10,0), (0,0)
    const rect = [_]GeoPoint{
        .{ .lat = 0, .lon = 0 },
        .{ .lat = 0, .lon = 10 },
        .{ .lat = 10, .lon = 10 },
        .{ .lat = 10, .lon = 0 },
        .{ .lat = 0, .lon = 0 },
    };

    try std.testing.expect(pointInPolygon(.{ .lat = 5, .lon = 5 }, &rect));
    try std.testing.expect(!pointInPolygon(.{ .lat = 15, .lon = 5 }, &rect));
    try std.testing.expect(!pointInPolygon(.{ .lat = 5, .lon = 15 }, &rect));
}

test "geohash encode/decode round-trip" {
    // San Francisco
    const sf = GeoPoint{ .lat = 37.7749, .lon = -122.4194 };
    const hash = encode(sf, 9);

    const decoded = decode(hash[0..9]);
    // At precision 9, error should be < 5 meters → ~0.0001 degrees
    try std.testing.expectApproxEqAbs(sf.lat, decoded.lat, 0.001);
    try std.testing.expectApproxEqAbs(sf.lon, decoded.lon, 0.001);
}

test "geohash known value" {
    // The geohash for (0, 0) at precision 1 should be "s"
    const hash = encode(.{ .lat = 0, .lon = 0 }, 1);
    try std.testing.expectEqual(@as(u8, 's'), hash[0]);
}

test "haversine distance" {
    // SF to NYC ≈ 4,129 km
    const sf = GeoPoint{ .lat = 37.7749, .lon = -122.4194 };
    const nyc = GeoPoint{ .lat = 40.7128, .lon = -74.0060 };
    const dist = haversineDistance(sf, nyc);
    // Should be approximately 4,129,000 meters (within 1%)
    try std.testing.expect(dist > 4_000_000);
    try std.testing.expect(dist < 4_200_000);
}

test "haversine distance zero" {
    const p = GeoPoint{ .lat = 37.7749, .lon = -122.4194 };
    const dist = haversineDistance(p, p);
    try std.testing.expectApproxEqAbs(@as(f64, 0), dist, 0.1);
}

test "geohash neighbors" {
    const center = encode(.{ .lat = 37.7749, .lon = -122.4194 }, 5);
    const nbrs = neighbors(center[0..5]);

    // All 8 neighbors should be different from center
    for (nbrs) |n| {
        try std.testing.expect(!std.mem.eql(u8, n[0..5], center[0..5]));
    }

    // Neighbors should decode to points near the center
    const center_point = decode(center[0..5]);
    for (nbrs) |n| {
        const nbr_point = decode(n[0..5]);
        const dist = haversineDistance(center_point, nbr_point);
        // At precision 5, cells are ~5km, so neighbors should be within ~10km
        try std.testing.expect(dist < 20_000);
    }
}

test "cover bounding box" {
    const alloc = std.testing.allocator;

    // Small bbox around SF
    const cells = try coverBoundingBox(alloc, 37.7, -122.5, 37.8, -122.4, 5);
    defer alloc.free(cells);

    // Should have at least 1 cell
    try std.testing.expect(cells.len >= 1);

    // All cells should decode to points within the bbox (approximately)
    for (cells) |cell| {
        const p = decode(cell[0..5]);
        try std.testing.expect(p.lat >= 37.5 and p.lat <= 38.0);
        try std.testing.expect(p.lon >= -122.7 and p.lon <= -122.2);
    }
}

test "cover circle" {
    const alloc = std.testing.allocator;

    // 1km circle around SF
    const center = GeoPoint{ .lat = 37.7749, .lon = -122.4194 };
    const cells = try coverCircle(alloc, center, 1000, 5);
    defer alloc.free(cells);

    try std.testing.expect(cells.len >= 1);
}
