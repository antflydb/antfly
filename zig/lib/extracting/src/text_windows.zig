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

//! Model-independent GLiNER source-word window geometry. Every byte remains
//! in its original coordinates; overlap ownership and terminal-word accounting
//! are shared with generated extractors and never truncate the document.
const std = @import("std");
const text_mod = @import("text.zig");
const Allocator = std.mem.Allocator;
const processor = text_mod;
const boundary = text_mod;
pub const Mode = enum { reject, windowed };
pub const Options = struct {
    mode: Mode = .reject,
    word_splitter: text_mod.WordSplitter = .whitespace,
    max_window_body_words: usize = 4096,
    overlap_words: usize = 128,
    max_document_bytes: usize = 1024 * 1024,
    max_document_words: usize = 131072,
    max_windows: usize = 64,
    max_window_scan_bytes: usize = 16 * 1024 * 1024,
    control: ?text_mod.Checkpoint = null,
};
pub const Window = struct {
    index: usize,
    bytes: text_mod.Offsets,
    words: text_mod.Offsets,
    owned_words: text_mod.Offsets,
    synthetic_terminal_word: bool,
    pub fn weight(self: Window) usize {
        return self.owned_words.end - self.owned_words.start;
    }
};

pub const Plan = struct {
    allocator: Allocator,
    words: []text_mod.ByteRange,
    windows: []Window,
    pub fn deinit(self: *Plan) void {
        self.allocator.free(self.words);
        self.allocator.free(self.windows);
        self.* = undefined;
    }
};
fn check(control: ?text_mod.Checkpoint) !void {
    if (control) |value| try value.check();
}
pub fn plan(a: Allocator, text: []const u8, _: [32]u8, options: Options) !Plan {
    const ranges = try text_mod.sourceWordRanges(a, text, .{ .word_splitter = options.word_splitter, .max_text_bytes = options.max_document_bytes, .max_words = options.max_document_words, .control = options.control });
    errdefer a.free(ranges);
    return .{ .allocator = a, .words = ranges, .windows = try planWindows(a, text, ranges, options) };
}
pub fn planWindows(a: Allocator, text: []const u8, ranges: []const text_mod.ByteRange, options: Options) ![]Window {
    if (options.max_window_body_words == 0 or options.max_window_body_words > 4096 or options.overlap_words >= options.max_window_body_words or options.max_windows == 0 or options.max_windows > 1024) return error.InvalidLongDocumentLimits;
    try check(options.control);
    if (text.len > options.max_window_scan_bytes) return error.LongDocumentWorkLimitExceeded;
    var scanned = text.len;
    const terminal = try processor.terminalWordAdded(a, text, options.word_splitter);
    try check(options.control);
    const full_count = try std.math.add(usize, ranges.len, @intFromBool(terminal));
    var windows = std.ArrayListUnmanaged(Window).empty;
    errdefer windows.deinit(a);
    if (full_count <= options.max_window_body_words) {
        try windows.append(a, .{ .index = 0, .bytes = .{ .start = 0, .end = text.len }, .words = .{ .start = 0, .end = ranges.len }, .owned_words = .{ .start = 0, .end = ranges.len }, .synthetic_terminal_word = terminal });
    } else {
        if (options.mode == .reject) return error.LongDocumentWindowingRequired;
        var start: usize = 0;
        while (start < ranges.len) {
            try check(options.control);
            if (windows.items.len >= options.max_windows) return error.LongDocumentWindowLimitExceeded;
            var end = @min(ranges.len, start + options.max_window_body_words);
            var bytes = boundary.Offsets{ .start = if (start == 0) 0 else ranges[start].start, .end = if (end == ranges.len) text.len else ranges[end - 1].end };
            if (bytes.end - bytes.start > options.max_window_scan_bytes - scanned) return error.LongDocumentWorkLimitExceeded;
            scanned += bytes.end - bytes.start;
            var extra = try processor.terminalWordAdded(a, text[bytes.start..bytes.end], options.word_splitter);
            if (end - start + @intFromBool(extra) > options.max_window_body_words) {
                end -= 1;
                if (end == start) return error.InvalidLongDocumentLimits;
                bytes.end = ranges[end - 1].end;
                if (bytes.end - bytes.start > options.max_window_scan_bytes - scanned) return error.LongDocumentWorkLimitExceeded;
                scanned += bytes.end - bytes.start;
                extra = try processor.terminalWordAdded(a, text[bytes.start..bytes.end], options.word_splitter);
            }
            if (end - start + @intFromBool(extra) > options.max_window_body_words) return error.InvalidLongDocumentLimits;
            try windows.append(a, .{ .index = windows.items.len, .bytes = bytes, .words = .{ .start = start, .end = end }, .owned_words = undefined, .synthetic_terminal_word = extra });
            if (end == ranges.len) break;
            if (end - start <= options.overlap_words) return error.InvalidLongDocumentLimits;
            start = end - options.overlap_words;
        }
        var owned_start: usize = 0;
        for (windows.items, 0..) |*window, i| {
            const owned_end = if (i + 1 == windows.items.len) ranges.len else (window.words.end + windows.items[i + 1].words.start) / 2;
            window.owned_words = .{ .start = owned_start, .end = owned_end };
            owned_start = owned_end;
        }
    }
    return windows.toOwnedSlice(a);
}

test "shared extraction windows cover original Unicode bytes without truncation" {
    const a = std.testing.allocator;
    const text = "Éva one two three four five six.";
    var result = try plan(a, text, @splat(0), .{ .mode = .windowed, .max_window_body_words = 4, .overlap_words = 1 });
    defer result.deinit();
    try std.testing.expect(result.windows.len > 1);
    try std.testing.expectEqual(@as(usize, 0), result.windows[0].bytes.start);
    try std.testing.expectEqual(text.len, result.windows[result.windows.len - 1].bytes.end);
    var owned: usize = 0;
    for (result.windows) |part| {
        try std.testing.expectEqual(owned, part.owned_words.start);
        owned = part.owned_words.end;
    }
    try std.testing.expectEqual(result.words.len, owned);
}
