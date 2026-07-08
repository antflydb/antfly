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

const std = @import("std");
const Allocator = std.mem.Allocator;

const invalid_utf8_warning_limit = 8;
var invalid_utf8_repairs: std.atomic.Value(u64) = .init(0);

pub const BoundaryDirection = enum { backward, forward };

pub const SanitizedText = struct {
    text: []const u8,
    owned: ?[]u8 = null,
    repaired: bool = false,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.owned) |owned| alloc.free(owned);
        self.* = undefined;
    }
};

pub fn sanitizeAlloc(alloc: Allocator, text: []const u8, context: []const u8) !SanitizedText {
    if (std.unicode.utf8ValidateSlice(text)) return .{ .text = text };
    const repaired = try replacementAlloc(alloc, text, context);
    return .{
        .text = repaired,
        .owned = repaired,
        .repaired = true,
    };
}

pub fn replacementAlloc(alloc: Allocator, text: []const u8, context: []const u8) ![]u8 {
    const repaired = try std.fmt.allocPrint(alloc, "{f}", .{std.unicode.fmtUtf8(text)});
    noteInvalidUtf8Repair(context, text.len, repaired.len);
    return repaired;
}

pub fn invalidUtf8RepairCount() u64 {
    return invalid_utf8_repairs.load(.monotonic);
}

pub fn snapToBoundary(text: []const u8, index: usize, direction: BoundaryDirection) usize {
    var i = @min(index, text.len);
    switch (direction) {
        .backward => {
            while (i > 0 and i < text.len and isContinuation(text[i])) : (i -= 1) {}
        },
        .forward => {
            while (i < text.len and isContinuation(text[i])) : (i += 1) {}
        },
    }
    return i;
}

fn isContinuation(byte: u8) bool {
    return (byte & 0xc0) == 0x80;
}

fn noteInvalidUtf8Repair(context: []const u8, input_len: usize, output_len: usize) void {
    const previous = invalid_utf8_repairs.fetchAdd(1, .monotonic);
    if (previous < invalid_utf8_warning_limit) {
        std.log.warn(
            "{s}: replaced invalid utf8 before enrichment processing input_bytes={d} output_bytes={d}",
            .{ context, input_len, output_len },
        );
    } else if (previous == invalid_utf8_warning_limit) {
        std.log.warn(
            "suppressing further invalid utf8 enrichment repair warnings after {d} repairs",
            .{invalid_utf8_warning_limit},
        );
    }
}

test "enrichment utf8 sanitizer preserves valid input without allocation" {
    const alloc = std.testing.allocator;
    var sanitized = try sanitizeAlloc(alloc, "valid \xc3\xa9", "test");
    defer sanitized.deinit(alloc);

    try std.testing.expect(!sanitized.repaired);
    try std.testing.expect(sanitized.owned == null);
    try std.testing.expectEqualStrings("valid \xc3\xa9", sanitized.text);
}

test "enrichment utf8 sanitizer replaces invalid input" {
    const alloc = std.testing.allocator;
    const repairs_before = invalidUtf8RepairCount();

    var sanitized = try sanitizeAlloc(alloc, "bad\xc2 text", "test");
    defer sanitized.deinit(alloc);

    try std.testing.expect(sanitized.repaired);
    try std.testing.expect(std.unicode.utf8ValidateSlice(sanitized.text));
    try std.testing.expect(std.mem.indexOf(u8, sanitized.text, &std.unicode.replacement_character_utf8) != null);
    try std.testing.expect(invalidUtf8RepairCount() > repairs_before);
}
