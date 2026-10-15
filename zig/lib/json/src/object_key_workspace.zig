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

const std = @import("std");

/// Per-depth duplicate-key storage shared by scanners and streaming writers.
/// State belongs to the caller's traversal; key/table ownership and reuse policy
/// stay here. Completed frames retain at most 256 KiB in total between values.
/// Active ancestor sets may exceed that budget to preserve duplicate detection.
/// Supply a reclaimable backing allocator. No pointer into this movable owner
/// is retained by its frames after a method returns.
pub fn ObjectKeyWorkspace(comptime State: type) type {
    return struct {
        const Self = @This();
        pub const Frame = struct {
            state: State,
            keys: std.heap.ArenaAllocator,
            seen: std.StringHashMapUnmanaged(void) = .empty,
            underused: bool = false,
            bytes: usize = 0,
            active: bool = false,
        };
        allocator: std.mem.Allocator,
        stack: std.ArrayListUnmanaged(Frame) = .empty,
        table_bytes: usize = 0,
        key_bytes: usize = 0,
        // Backing bytes in completed frames only; active ancestors are required
        // state, not candidates for retirement. Updated at enter/finish in O(1).
        idle_bytes: usize = 0,
        pub const reuse_limit = 256 * 1024;
        // Count actual backing bytes, including arena headers, without depending
        // on private container layouts. Maps do not retain their allocator; arenas
        // use this adapter only during synchronous operations and restore their
        // backing allocator before returning. The workspace remains movable.
        const ScratchAllocator = struct {
            backing: std.mem.Allocator,
            bytes: *usize,
            frame_bytes: *usize,
            fn allocator(self: *@This()) std.mem.Allocator {
                return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
            }
            fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
                const self: *@This() = @ptrCast(@alignCast(ctx));
                const result = self.backing.rawAlloc(len, alignment, ra) orelse return null;
                self.bytes.* += len;
                self.frame_bytes.* += len;
                return result;
            }
            fn resize(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
                const self: *@This() = @ptrCast(@alignCast(ctx));
                if (!self.backing.rawResize(buf, alignment, len, ra)) return false;
                self.bytes.* = self.bytes.* - buf.len + len;
                self.frame_bytes.* = self.frame_bytes.* - buf.len + len;
                return true;
            }
            fn remap(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
                const self: *@This() = @ptrCast(@alignCast(ctx));
                const result = self.backing.rawRemap(buf, alignment, len, ra) orelse return null;
                self.bytes.* = self.bytes.* - buf.len + len;
                self.frame_bytes.* = self.frame_bytes.* - buf.len + len;
                return result;
            }
            fn free(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, ra: usize) void {
                const self: *@This() = @ptrCast(@alignCast(ctx));
                self.backing.rawFree(buf, alignment, ra);
                self.bytes.* -= buf.len;
                self.frame_bytes.* -= buf.len;
            }
        };

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{ .allocator = allocator };
        }
        pub fn deinit(self: *Self) void {
            for (self.stack.items) |*frame| {
                frame.seen.deinit(self.allocator);
                frame.keys.deinit();
            }
            self.stack.deinit(self.allocator);
            self.stack = .empty;
            self.table_bytes = 0;
            self.key_bytes = 0;
            self.idle_bytes = 0;
        }
        pub fn retainedBytes(self: *const Self) usize {
            return self.stack.capacity * @sizeOf(Frame) + self.table_bytes + self.key_bytes;
        }
        pub fn enter(self: *Self, depth: usize, state: State) !void {
            std.debug.assert(depth <= self.stack.items.len);
            if (depth == self.stack.items.len) try self.stack.append(self.allocator, .{ .state = state, .keys = .init(self.allocator) });
            const frame = &self.stack.items[depth];
            std.debug.assert(!frame.active);
            self.idle_bytes -= frame.bytes;
            frame.active = true;
            frame.state = state;
        }
        /// Decoded names borrow this frame until finishFrame; unescaped names
        /// borrow the scanner's input. No transient allocator escapes the call.
        pub fn nextKey(self: *Self, depth: usize, scanner: *std.json.Scanner, max_len: usize) !std.json.Token {
            var counted: ScratchAllocator = .{ .backing = self.allocator, .bytes = &self.key_bytes, .frame_bytes = &self.stack.items[depth].bytes };
            const keys = &self.stack.items[depth].keys;
            keys.child_allocator = counted.allocator();
            defer keys.child_allocator = self.allocator;
            return scanner.nextAllocMax(keys.allocator(), .alloc_if_needed, max_len);
        }
        /// Writers decode tokens in short-lived scratch. Copy those names into
        /// the frame so resetting token scratch cannot invalidate its key set.
        /// Names passed without copying must remain valid until finishFrame.
        pub fn remember(self: *Self, depth: usize, name: []const u8, copy: bool) !void {
            var tables_counted: ScratchAllocator = .{ .backing = self.allocator, .bytes = &self.table_bytes, .frame_bytes = &self.stack.items[depth].bytes };
            const frame = &self.stack.items[depth];
            const key = if (copy) copied: {
                var keys_counted: ScratchAllocator = .{ .backing = self.allocator, .bytes = &self.key_bytes, .frame_bytes = &self.stack.items[depth].bytes };
                frame.keys.child_allocator = keys_counted.allocator();
                defer frame.keys.child_allocator = self.allocator;
                break :copied try frame.keys.allocator().dupe(u8, name);
            } else name;
            if ((try frame.seen.getOrPut(tables_counted.allocator(), key)).found_existing) return error.DuplicateField;
        }
        pub fn finishFrame(self: *Self, depth: usize) !void {
            var tables_counted: ScratchAllocator = .{ .backing = self.allocator, .bytes = &self.table_bytes, .frame_bytes = &self.stack.items[depth].bytes };
            var keys_counted: ScratchAllocator = .{ .backing = self.allocator, .bytes = &self.key_bytes, .frame_bytes = &self.stack.items[depth].bytes };
            const frame = &self.stack.items[depth];
            frame.keys.child_allocator = keys_counted.allocator();
            defer frame.keys.child_allocator = self.allocator;
            // A wide historical object must not make every later tiny object
            // clear a wide metadata array. Drop sparse tables before clearing.
            // Small tables keep their allocation-free steady state; substantial
            // tables remain reusable when the current object actually uses them.
            // Require two consecutive sparse objects so alternating wide/tiny
            // values can retain their warm table without allocation churn.
            if (frame.seen.capacity() > 64) {
                const sparse = frame.seen.count() < frame.seen.capacity() / 16;
                const retire_sparse = sparse and frame.underused;
                frame.underused = sparse;
                if (retire_sparse) {
                    frame.seen.deinit(tables_counted.allocator());
                    frame.seen = .empty;
                    frame.underused = false;
                }
            }
            if (!frame.keys.reset(if (frame.keys.queryCapacity() > reuse_limit) .free_all else .retain_capacity)) return error.OutOfMemory;
            // Only completed storage competes for reuse. Required active ancestors
            // must not force allocation churn in every small sibling.
            if (self.stack.capacity * @sizeOf(Frame) + self.idle_bytes + frame.bytes > reuse_limit) {
                _ = frame.keys.reset(.free_all);
                if (self.stack.capacity * @sizeOf(Frame) + self.idle_bytes + frame.bytes > reuse_limit) {
                    frame.seen.deinit(tables_counted.allocator());
                    frame.seen = .empty;
                    frame.underused = false;
                }
            }
            // Clear only a retained table; retiring a wide table must not
            // first pay to clear its metadata. No key is read after arena reset.
            if (frame.seen.count() != 0) frame.seen.clearRetainingCapacity();
            std.debug.assert(frame.active);
            frame.active = false;
            self.idle_bytes += frame.bytes;
        }
        /// Call after every active frame has closed. This check is constant
        /// time even when earlier values reached a much greater depth.
        pub fn finishValue(self: *Self) void {
            if (self.retainedBytes() > reuse_limit) self.deinit();
        }
    };
}
