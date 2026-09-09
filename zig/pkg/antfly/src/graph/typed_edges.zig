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

//! Covering relationship-type postings. Values are empty: metric topology
//! needs edge identity, not timestamps, weights or document metadata. Each
//! type range preserves the original reverse-key partition order.
const std = @import("std");
const backend = @import("../storage/backend_erased.zig");
const keys = @import("../storage/internal_keys.zig");
const Allocator = std.mem.Allocator;
pub const prefix = "meta:metric_type_edges:v1/";
pub const ready_key = "meta:metric_type_edges_ready:v1";
pub const cursor_key = "meta:metric_type_edges_cursor:v1";

pub fn typePrefixAlloc(alloc: Allocator, kind: []const u8) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, prefix);
    try keys.appendEncodedComponent(&out, alloc, kind);
    return out.toOwnedSlice(alloc);
}

pub fn keyAlloc(alloc: Allocator, kind: []const u8, reverse_key: []const u8) ![]u8 {
    const start = try typePrefixAlloc(alloc, kind);
    defer alloc.free(start);
    return std.mem.concat(alloc, u8, &.{ start, reverse_key });
}

pub const Entry = struct { key: []const u8, cursor: []const u8 };

pub const Cursor = struct {
    alloc: Allocator,
    raw: backend.Cursor,
    types: []const []const u8,
    lower: []const u8,
    upper: []const u8,
    resume_key: []const u8,
    type_index: usize = 0,
    started: bool = false,
    type_prefix: []u8 = &.{},
    exhausted: bool = false,

    pub fn init(alloc: Allocator, txn: anytype, filter: anytype, lower: []const u8, upper: []const u8, resume_key: []const u8) !Cursor {
        const types = try alloc.dupe([]const u8, if (filter.mode == .all) &.{} else filter.types);
        errdefer alloc.free(types);
        std.mem.sort([]const u8, types, {}, struct {
            fn less(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.less);
        // Index preparation is a durable prerequisite, not a silent scan
        // fallback that can change cursor interpretation across checkpoints.
        if (types.len > 0) _ = txn.get(ready_key) catch |err| switch (err) {
            error.NotFound => return error.InvalidGraphMetricBuildManifest,
            else => return err,
        };
        if (resume_key.len > 0) {
            var raw_resume: ?[]const u8 = if (types.len == 0) resume_key else null;
            for (types) |kind| {
                const start = try typePrefixAlloc(alloc, kind);
                defer alloc.free(start);
                if (std.mem.startsWith(u8, resume_key, start)) {
                    raw_resume = resume_key[start.len..];
                    break;
                }
            }
            const key = raw_resume orelse return error.InvalidGraphMetricBuildManifest;
            if (key.len == 0 or std.mem.startsWith(u8, key, "meta:") or
                (lower.len > 0 and std.mem.order(u8, key, lower) == .lt) or
                (upper.len > 0 and std.mem.order(u8, key, upper) != .lt))
                return error.InvalidGraphMetricBuildManifest;
        }
        return .{ .alloc = alloc, .raw = try txn.openCursor(), .types = types, .lower = lower, .upper = upper, .resume_key = resume_key };
    }

    pub fn deinit(self: *@This()) void {
        self.raw.close();
        self.alloc.free(self.types);
        self.alloc.free(self.type_prefix);
    }

    pub fn next(self: *@This()) !?Entry {
        if (self.exhausted) return null;
        if (self.types.len == 0) {
            var found = if (self.started) try self.raw.next() else blk: {
                self.started = true;
                const start = if (self.resume_key.len > 0) self.resume_key else self.lower;
                var item = if (start.len > 0) try self.raw.seekAtOrAfter(start) else try self.raw.first();
                if (item) |entry| if (self.resume_key.len > 0 and std.mem.eql(u8, entry.key, self.resume_key)) {
                    item = try self.raw.next();
                };
                break :blk item;
            };
            if (found) |entry| if (std.mem.startsWith(u8, entry.key, "meta:")) {
                found = try self.raw.seekAtOrAfter("meta;");
            };
            if (found) |entry| {
                if (self.upper.len == 0 or std.mem.order(u8, entry.key, self.upper) == .lt)
                    return .{ .key = entry.key, .cursor = entry.key };
            }
            self.exhausted = true;
            return null;
        }
        while (self.type_index < self.types.len) {
            const found = if (self.started) try self.raw.next() else blk: {
                self.alloc.free(self.type_prefix);
                self.type_prefix = &.{};
                self.type_prefix = try typePrefixAlloc(self.alloc, self.types[self.type_index]);
                const lower = try std.mem.concat(self.alloc, u8, &.{ self.type_prefix, self.lower });
                defer self.alloc.free(lower);
                const start = if (self.resume_key.len > 0 and std.mem.order(u8, self.resume_key, lower) == .gt) self.resume_key else lower;
                self.started = true;
                var item = try self.raw.seekAtOrAfter(start);
                if (item) |entry| if (self.resume_key.len > 0 and std.mem.eql(u8, entry.key, self.resume_key)) {
                    item = try self.raw.next();
                };
                break :blk item;
            };
            if (found) |entry| if (std.mem.startsWith(u8, entry.key, self.type_prefix)) {
                const key = entry.key[self.type_prefix.len..];
                if (self.upper.len == 0 or std.mem.order(u8, key, self.upper) == .lt)
                    return .{ .key = key, .cursor = entry.key };
            };
            self.type_index += 1;
            self.started = false;
        }
        self.exhausted = true;
        return null;
    }
};
