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

//! Pure template value and marker handling shared with dependency inspection.
const std = @import("std");
const Allocator = std.mem.Allocator;
const hbs = @import("handlebars");
const Value = hbs.Value;
const HelperContext = hbs.HelperContext;
const ContentPart = @import("antfly_template_content").ContentPart;
pub const remote_credentials_argument = "credentials";

pub fn mediaHelper(ctx: HelperContext) anyerror!Value {
    const url = ctx.hash.get("url") orelse return .{ .string = "" };
    const url_str = switch (url) {
        .string => |s| s,
        else => return .{ .string = "" },
    };
    if (url_str.len == 0) return .{ .string = "" };

    const result = try std.fmt.allocPrint(ctx.arena, "<<<dotprompt:media:url {s}>>>", .{url_str});
    return .{ .safe_string = result };
}

pub fn stripErrorDirectives(alloc: Allocator, text: []const u8) ![]const u8 {
    var result = std.ArrayListUnmanaged(u8).empty;
    const prefix_with_status = "<<<error:status=";
    const prefix_no_status = "<<<error:message=";
    const suffix = ">>>";

    var pos: usize = 0;
    while (pos < text.len) {
        if ((pos + prefix_with_status.len <= text.len and
            std.mem.eql(u8, text[pos..][0..prefix_with_status.len], prefix_with_status)) or
            (pos + prefix_no_status.len <= text.len and
                std.mem.eql(u8, text[pos..][0..prefix_no_status.len], prefix_no_status)))
        {
            // Find the closing >>>
            const end_idx = std.mem.indexOfPos(u8, text, pos + 9, suffix) orelse {
                try result.append(alloc, text[pos]);
                pos += 1;
                continue;
            };
            pos = end_idx + suffix.len;
        } else {
            try result.append(alloc, text[pos]);
            pos += 1;
        }
    }

    return try result.toOwnedSlice(alloc);
}

pub fn jsonToValue(arena: Allocator, json: std.json.Value) Allocator.Error!Value {
    const Work = struct { source: std.json.Value, target: *Value };
    var result: Value = undefined;
    var pending: std.ArrayList(Work) = .empty;
    defer pending.deinit(arena);
    try pending.append(arena, .{ .source = json, .target = &result });
    while (pending.pop()) |work| {
        work.target.* = switch (work.source) {
            .null => .null,
            .bool => |b| .{ .boolean = b },
            .integer => |n| .{ .integer = n },
            .float => |n| .{ .float = n },
            .number_string => |raw| .{ .number_string = raw },
            .string => |text| .{ .string = text },
            .array => |array| blk: {
                const items = try arena.alloc(Value, array.items.len);
                for (array.items, items) |source, *target| try pending.append(arena, .{ .source = source, .target = target });
                break :blk .{ .array = items };
            },
            .object => |object| blk: {
                var map: hbs.ValueMap = .{};
                for (object.keys()) |key| {
                    if (key.len > 0 and key[0] == '_') continue;
                    try map.put(arena, key, .undefined);
                }
                // Finish map growth before taking pointers to its values.
                for (map.keys(), map.values()) |key, *target| try pending.append(arena, .{ .source = object.get(key).?, .target = target });
                break :blk .{ .map = map };
            },
        };
    }
    return result;
}

pub const TextPartIterator = struct {
    text: []const u8,
    pos: usize = 0,

    pub fn next(self: *@This()) ?ContentPart {
        const prefix = "<<<dotprompt:media:url ";
        while (self.pos < self.text.len) {
            const start = std.mem.indexOfPos(u8, self.text, self.pos, prefix) orelse self.text.len;
            if (start > self.pos) {
                const segment = std.mem.trim(u8, self.text[self.pos..start], &std.ascii.whitespace);
                self.pos = start;
                if (segment.len > 0) return .{ .text = segment };
                continue;
            }
            const begin = start + prefix.len;
            const end = std.mem.indexOfPos(u8, self.text, begin, ">>>") orelse {
                const segment = std.mem.trim(u8, self.text[self.pos..], &std.ascii.whitespace);
                self.pos = self.text.len;
                return .{ .text = segment };
            };
            self.pos = end + 3;
            return .{ .media_url = self.text[begin..end] };
        }
        return null;
    }
};

pub const ErrorDirective = struct {
    status: u16, // HTTP status code, or 0 for non-HTTP errors
    message: []const u8,

    /// Returns true if the status indicates the resource is permanently
    /// unavailable and should not be retried.
    pub fn isPermanent(self: ErrorDirective) bool {
        return self.status == 401 or self.status == 403 or
            self.status == 404 or self.status == 410 or self.status == 413;
    }
};

pub fn parseErrorDirectives(alloc: Allocator, text: []const u8) ![]ErrorDirective {
    var directives = std.ArrayListUnmanaged(ErrorDirective).empty;
    const prefix_with_status = "<<<error:status=";
    const prefix_no_status = "<<<error:message=";
    const suffix = ">>>";

    var pos: usize = 0;
    while (pos < text.len) {
        if (pos + prefix_with_status.len <= text.len and
            std.mem.eql(u8, text[pos..][0..prefix_with_status.len], prefix_with_status))
        {
            // Parse: <<<error:status=NNN message=...>>>
            const after_prefix = pos + prefix_with_status.len;
            const space_idx = std.mem.indexOfScalarPos(u8, text, after_prefix, ' ') orelse {
                pos += 1;
                continue;
            };
            const status_str = text[after_prefix..space_idx];
            const status = std.fmt.parseInt(u16, status_str, 10) catch {
                pos += 1;
                continue;
            };

            const msg_prefix = "message=";
            if (space_idx + 1 + msg_prefix.len > text.len or
                !std.mem.eql(u8, text[space_idx + 1 ..][0..msg_prefix.len], msg_prefix))
            {
                pos += 1;
                continue;
            }
            const msg_start = space_idx + 1 + msg_prefix.len;
            const end_idx = std.mem.indexOfPos(u8, text, msg_start, suffix) orelse {
                pos += 1;
                continue;
            };
            const message = try alloc.dupe(u8, text[msg_start..end_idx]);
            try directives.append(alloc, .{ .status = status, .message = message });
            pos = end_idx + suffix.len;
        } else if (pos + prefix_no_status.len <= text.len and
            std.mem.eql(u8, text[pos..][0..prefix_no_status.len], prefix_no_status))
        {
            // Parse: <<<error:message=...>>>
            const msg_start = pos + prefix_no_status.len;
            const end_idx = std.mem.indexOfPos(u8, text, msg_start, suffix) orelse {
                pos += 1;
                continue;
            };
            const message = try alloc.dupe(u8, text[msg_start..end_idx]);
            try directives.append(alloc, .{ .status = 0, .message = message });
            pos = end_idx + suffix.len;
        } else {
            pos += 1;
        }
    }

    return try directives.toOwnedSlice(alloc);
}

/// Free directives returned by parseErrorDirectives.
pub fn freeErrorDirectives(alloc: Allocator, directives: []const ErrorDirective) void {
    for (directives) |d| alloc.free(@constCast(d.message));
    alloc.free(directives);
}

pub fn validateRenderedTemplate(alloc: Allocator, rendered: []const u8) !void {
    const directives = try parseErrorDirectives(alloc, rendered);
    defer freeErrorDirectives(alloc, directives);
    if (directives.len == 0) return;
    if (directives[0].isPermanent()) return error.PermanentPromptFailure;
    return error.TransientPromptFailure;
}
