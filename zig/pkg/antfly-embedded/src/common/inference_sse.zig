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

//! Shared bounded SSE framing for response rewriting and observation. Events
//! are normalized to LF; their slices remain valid until the next push/finish.
const std = @import("std");

pub const Framer = struct {
    limit: usize,
    overflow: enum { fail, drop } = .fail,
    pending: std.ArrayList(u8) = .empty,
    line_len: usize = 0,
    after_cr: bool = false,
    first_event: bool = true,
    ready: bool = false,
    dropping: bool = false,

    pub fn deinit(self: *Framer, a: std.mem.Allocator) void {
        self.pending.deinit(a);
    }
    pub fn push(self: *Framer, a: std.mem.Allocator, byte: u8) !?[]const u8 {
        if (self.ready) {
            self.pending.clearRetainingCapacity();
            self.ready = false;
        }
        if (self.after_cr and byte == '\n') {
            self.after_cr = false;
            return null;
        }
        self.after_cr = byte == '\r';
        const newline = byte == '\r' or byte == '\n';
        if (!self.dropping) {
            if (self.pending.items.len >= self.limit) {
                if (self.overflow == .fail) return error.SSEEventTooLarge;
                self.dropping = true;
            } else self.pending.append(a, if (newline) '\n' else byte) catch |err| {
                if (self.overflow == .fail) return err;
                self.dropping = true;
            };
        }
        if (!newline) {
            self.line_len +|= 1;
            return null;
        }
        const bom_only = self.first_event and self.line_len == 3 and self.pending.items.len == 4 and std.mem.startsWith(u8, self.pending.items, "\xef\xbb\xbf");
        const empty = self.line_len == 0 or bom_only;
        self.line_len = 0;
        if (!empty) return null;
        self.ready = true;
        const first = self.first_event;
        self.first_event = false;
        if (self.dropping) {
            self.dropping = false;
            return null;
        }
        var event = self.pending.items;
        if (first and std.mem.startsWith(u8, event, "\xef\xbb\xbf")) event = event[3..];
        return event;
    }
    /// EOF never dispatches an unterminated event.
    pub fn finish(self: *Framer) void {
        self.pending.clearRetainingCapacity();
        self.ready = false;
    }
};

pub fn isEventStream(content_type: []const u8) bool {
    var parts = std.mem.splitScalar(u8, content_type, ';');
    return std.ascii.eqlIgnoreCase(std.mem.trim(u8, parts.first(), " \t"), "text/event-stream");
}

/// Remove exactly one optional space after the colon. A bare data field is an
/// empty data line. Comments and unknown fields are opaque.
pub fn dataValue(line: []const u8) ?[]const u8 {
    const colon = std.mem.indexOfScalar(u8, line, ':') orelse line.len;
    if (!std.mem.eql(u8, line[0..colon], "data")) return null;
    var value = if (colon < line.len) line[colon + 1 ..] else "";
    if (std.mem.startsWith(u8, value, " ")) value = value[1..];
    return value;
}

pub fn dataAlloc(a: std.mem.Allocator, event: []const u8) !?[]u8 {
    var payload: std.ArrayList(u8) = .empty;
    errdefer payload.deinit(a);
    var lines = std.mem.splitScalar(u8, event, '\n');
    var has_data = false;
    while (lines.next()) |line| {
        if (dataValue(line)) |value| {
            if (has_data) try payload.append(a, '\n');
            try payload.appendSlice(a, value);
            has_data = true;
        }
    }
    if (!has_data) return null;
    return try payload.toOwnedSlice(a);
}
