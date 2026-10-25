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

//! Native replacement support uses immutable source text and geometry,
//! independently of mutable OCR candidates and model quality judgments.
const std = @import("std");
const c = @import("contract.zig");

fn equivalent(left: []const u8, right: []const u8) bool {
    var l = std.mem.tokenizeAny(u8, left, " \t\r\n");
    var r = std.mem.tokenizeAny(u8, right, " \t\r\n");
    while (l.next()) |word| if (!std.mem.eql(u8, word, r.next() orelse return false)) return false;
    return r.next() == null;
}
fn validBox(box: [4]f64) bool {
    for (box) |value| if (!std.math.isFinite(value)) return false;
    return box[0] < box[2] and box[1] < box[3];
}
fn contains(outer: [4]f64, inner: [4]f64) bool {
    return inner[0] >= outer[0] and inner[1] >= outer[1] and inner[2] <= outer[2] and inner[3] <= outer[3];
}
fn intersects(left: [4]f64, right: [4]f64) bool {
    return left[0] < right[2] and right[0] < left[2] and left[1] < right[3] and right[1] < left[3];
}

/// Native evidence is immutable and already in source coordinates. Matching a
/// different region or just a substring cannot establish replacement coverage.
/// Runs crossing a crop boundary require independent visual verification.
pub fn nativeSupports(backing: std.mem.Allocator, native_text: []const u8, native_regions: []const c.Region, region: ?c.Region, candidate: []const u8) !bool {
    if (std.mem.trim(u8, candidate, " \t\r\n").len == 0) return false;
    const selected = region orelse return equivalent(native_text, candidate);
    _ = backing;
    const box = selected.bbox orelse return false;
    if (!validBox(box)) return false;
    var observed = std.mem.tokenizeAny(u8, candidate, " \t\r\n");
    for (native_regions) |run| {
        if (run.origin != .native or std.mem.trim(u8, run.text, " \t\r\n").len == 0) continue;
        // Unknown geometry cannot prove exclusion from the crop. Neither a
        // crossing run nor a malformed box may silently disappear from proof.
        const run_box = run.bbox orelse return false;
        if (!validBox(run_box)) return false;
        if (!intersects(box, run_box)) continue;
        if (!contains(box, run_box)) return false;
        var expected = std.mem.tokenizeAny(u8, run.text, " \t\r\n");
        while (expected.next()) |word| if (!std.mem.eql(u8, word, observed.next() orelse return false)) return false;
    }
    return observed.next() == null;
}

test "document native grounding rejects text from another region and partial page coverage" {
    const a = std.testing.allocator;
    const runs = [_]c.Region{
        .{ .id = 0, .text = "10.00", .bbox = .{ 0, 0, 10, 10 }, .origin = .native },
        .{ .id = 1, .text = "20.00", .bbox = .{ 20, 0, 30, 10 }, .origin = .native },
    };
    const region = c.Region{ .id = 4, .text = "bad OCR", .bbox = .{ 0, 0, 10, 10 } };
    try std.testing.expect(!try nativeSupports(a, "10.00 20.00", &runs, region, "20.00"));
    try std.testing.expect(try nativeSupports(a, "10.00 20.00", &runs, region, "10.00"));
    try std.testing.expect(!try nativeSupports(a, "10.00 20.00", &runs, null, "10.00"));
    try std.testing.expect(try nativeSupports(a, "10.00 20.00", &runs, null, " 10.00\n20.00 "));
    try std.testing.expect(!try nativeSupports(a, "10.00", &runs, .{ .id = 5, .text = "", .bbox = .{ 5, 0, 10, 10 } }, "10.00"));
}

/// Linear ordered-token coverage permits additional visual text but detects
/// omissions and repeated-token loss. It is a coverage check, not a quality vote.
pub fn coversNative(native_text: []const u8, text: []const u8) bool {
    var expected = std.mem.tokenizeAny(u8, native_text, " \t\r\n");
    var observed = std.mem.tokenizeAny(u8, text, " \t\r\n");
    while (expected.next()) |word| {
        var found = false;
        while (observed.next()) |candidate| if (std.mem.eql(u8, word, candidate)) {
            found = true;
            break;
        };
        if (!found) return false;
    }
    return true;
}

test "document native coverage allows added visual text but counts repeats and order" {
    try std.testing.expect(coversNative("Header\nBody", "Header Figure Body Footer"));
    try std.testing.expect(!coversNative("Header Body", "Header"));
    try std.testing.expect(!coversNative("Total Total", "Total"));
    try std.testing.expect(!coversNative("Header Body", "Body Header"));
}

test "document native crop grounding requires complete known noncrossing geometry without allocation" {
    const known = c.Region{ .id = 0, .text = "Total", .bbox = .{ 1, 1, 4, 4 }, .origin = .native };
    const crop = c.Region{ .id = 2, .text = "Total", .bbox = .{ 0, 0, 10, 10 } };
    const a = std.testing.failing_allocator;
    for ([_]?[4]f64{
        .{ 5, 1, 15, 4 }, // Crosses the right boundary.
        .{ -1, 1, 2, 4 }, // Crosses the left boundary.
        .{ 1, -1, 4, 2 }, // Crosses the top boundary.
        .{ 1, 5, 4, 15 }, // Crosses the bottom boundary.
        .{ -1, -1, 11, 11 }, // Encloses the crop.
        null,
        .{ 2, 2, 1, 4 },
        .{ 1, 1, 1, 4 },
        .{ std.math.nan(f64), 1, 4, 4 },
    }) |box| {
        const unknown = c.Region{ .id = 1, .text = "12.50", .bbox = box, .origin = .native };
        // Crossing/unknown runs must be found regardless of enumeration order.
        try std.testing.expect(!try nativeSupports(a, "Total 12.50", &.{ known, unknown }, crop, "Total"));
        try std.testing.expect(!try nativeSupports(a, "12.50 Total", &.{ unknown, known }, crop, "Total"));
    }
    for ([_][4]f64{ .{ 10, 1, 15, 4 }, .{ 1, 10, 4, 15 }, .{ -4, 1, 0, 4 }, .{ 1, -4, 4, 0 }, .{ 20, 20, 30, 30 } }) |box| {
        const outside = c.Region{ .id = 1, .text = "12.50", .bbox = box, .origin = .native };
        try std.testing.expect(try nativeSupports(a, "Total 12.50", &.{ known, outside }, crop, "Total"));
    }
    const amount = c.Region{ .id = 1, .text = "12.50", .bbox = .{ 5, 1, 9, 4 }, .origin = .native };
    try std.testing.expect(try nativeSupports(a, "Total 12.50", &.{ known, amount }, crop, " Total \n12.50 "));
    try std.testing.expect(!try nativeSupports(a, "Total 12.50", &.{ known, amount }, crop, "Total"));
    var invalid_crop = crop;
    invalid_crop.bbox = .{ 10, 0, 0, 10 };
    try std.testing.expect(!try nativeSupports(a, "Total", &.{known}, invalid_crop, "Total"));
}
