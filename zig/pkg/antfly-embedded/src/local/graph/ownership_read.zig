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

//! Snapshot-owned visibility fence for private graph stores. The outer erased
//! transaction retains this immutable scope until its last cursor is closed.
const std = @import("std");
const backend = @import("../storage/backend_erased.zig");
const maintenance = @import("maintenance.zig");
const A = std.mem.Allocator;

pub fn begin(a: A, inner: backend.ReadTxn, raw: []const u8, incoming: bool, comptime accepts: fn (maintenance.RangeProgress, []const u8, bool) bool, comptime seekPast: fn (A, maintenance.RangeProgress, []const u8, bool, bool) anyerror![]u8) !backend.ReadTxn {
    const Handle = struct {
        alloc: A,
        inner: backend.ReadTxn,
        raw: []u8,
        scope: maintenance.RangeProgress,
        incoming: bool,
        owns_raw: bool = true,

        pub fn forkBorrowedRead(self: *@This()) !@This() {
            var fork = self.*;
            fork.inner = try self.inner.forkRead();
            fork.owns_raw = false;
            return fork;
        }

        pub fn abort(self: *@This()) void {
            self.inner.abort();
            if (self.owns_raw) self.alloc.free(self.raw);
        }
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            if (!accepts(self.scope, key, self.incoming)) return error.NotFound;
            return self.inner.get(key);
        }
        pub fn getManySorted(self: *@This(), keys: []const []const u8, values: []?[]const u8) !void {
            try self.inner.getManySorted(keys, values);
            for (keys, values) |key, *value| if (!accepts(self.scope, key, self.incoming)) {
                value.* = null;
            };
        }
        const Cursor = struct {
            alloc: A,
            inner: backend.Cursor,
            scope: maintenance.RangeProgress,
            incoming: bool,
            upper: ?[]const u8 = null,
            pub fn close(self: *@This()) void {
                self.inner.close();
            }
            fn visible(self: *@This(), initial: ?backend.Entry, backwards: bool) !?backend.Entry {
                var entry = initial;
                while (entry) |value| {
                    // Some erased backends do not implement native bounds.
                    // Enforce the borrowed bound here too, before visibility
                    // skipping can escape the caller's adjacency prefix.
                    if (self.upper) |upper| if (std.mem.order(u8, value.key, upper) != .lt) {
                        if (!backwards) return null;
                        entry = try self.inner.seekAtOrBefore(upper);
                        if (entry) |boundary| if (std.mem.eql(u8, boundary.key, upper)) {
                            entry = try self.inner.prev();
                        };
                        continue;
                    };
                    if (accepts(self.scope, value.key, self.incoming)) return value;
                    const boundary = try seekPast(self.alloc, self.scope, value.key, self.incoming, backwards);
                    defer self.alloc.free(boundary);
                    entry = if (backwards) try self.inner.seekAtOrBefore(boundary) else try self.inner.seekAtOrAfter(boundary);
                }
                return null;
            }
            pub fn first(self: *@This()) !?backend.Entry {
                return self.visible(try self.inner.first(), false);
            }
            pub fn last(self: *@This()) !?backend.Entry {
                return self.visible(try self.inner.last(), true);
            }
            pub fn next(self: *@This()) !?backend.Entry {
                return self.visible(try self.inner.next(), false);
            }
            pub fn prev(self: *@This()) !?backend.Entry {
                return self.visible(try self.inner.prev(), true);
            }
            pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?backend.Entry {
                return self.visible(try self.inner.seekAtOrAfter(key), false);
            }
            pub fn seekAtOrBefore(self: *@This(), key: []const u8) !?backend.Entry {
                return self.visible(try self.inner.seekAtOrBefore(key), true);
            }
            pub fn setUpperBound(self: *@This(), upper: ?[]const u8) void {
                self.upper = upper;
                self.inner.setUpperBound(upper);
            }
        };
        pub fn openCursor(self: *@This()) !Cursor {
            return .{ .alloc = self.alloc, .inner = try self.inner.openCursor(), .scope = self.scope, .incoming = self.incoming };
        }
    };
    const owned = try a.dupe(u8, raw);
    errdefer a.free(owned);
    return backend.readTxnFrom(a, Handle{ .alloc = a, .inner = inner, .raw = owned, .scope = try maintenance.RangeProgress.decode(owned), .incoming = incoming });
}
