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
fn contains(outer: [4]f64, inner: [4]f64) bool {
    for (outer ++ inner) |value| if (!std.math.isFinite(value)) return false;
    return outer[0] < outer[2] and outer[1] < outer[3] and inner[0] <= inner[2] and inner[1] <= inner[3] and
        inner[0] >= outer[0] and inner[1] >= outer[1] and inner[2] <= outer[2] and inner[3] <= outer[3];
}

/// Native evidence is immutable and already in source coordinates. Matching a
/// different region or just a substring cannot establish replacement coverage.
/// Runs crossing a crop boundary require independent visual verification.
pub fn nativeSupports(backing: std.mem.Allocator, native_text: []const u8, native_regions: []const c.Region, region: ?c.Region, candidate: []const u8) !bool {
    if (std.mem.trim(u8, candidate, " \t\r\n").len == 0) return false;
    const selected = region orelse return equivalent(native_text, candidate);
    const box = selected.bbox orelse return false;
    var output = std.Io.Writer.Allocating.init(backing);
    defer output.deinit();
    for (native_regions) |run| {
        if (run.origin != .native) continue;
        const run_box = run.bbox orelse continue;
        if (!contains(box, run_box)) continue;
        if (output.written().len > 0) try output.writer.writeByte('\n');
        try output.writer.writeAll(run.text);
    }
    return equivalent(output.written(), candidate);
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
