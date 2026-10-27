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

//! Direct native merge output. Only the flat decoder's temporary directory
//! lives in RAM; the stored inventory and field sections enter AFP1 once.
const std = @import("std");
const pack = @import("../field_pack.zig");
const segment = @import("../segment.zig");
const Source = @import("../segment_source.zig").Source;
const A = std.mem.Allocator;

pub fn Sink(comptime Buffered: type) type {
    return struct {
        allocator: A,
        encoder: *pack.Encoder,
        buffered: *Buffered,
        tail: std.ArrayListUnmanaged(u8) = .empty,
        sealed: bool = false,
        const Self = @This();
        pub fn deinit(self: *Self) void {
            self.tail.deinit(self.allocator);
        }
        pub fn sink(self: *Self) segment.SegmentSink {
            return .{ .ptr = self, .vtable = &vtable };
        }
        fn owner(ptr: *anyopaque) *Self {
            return @ptrCast(@alignCast(ptr));
        }
        fn length(ptr: *anyopaque) usize {
            const self = owner(ptr);
            return @as(usize, @intCast(self.encoder.payload_bytes)) + self.tail.items.len;
        }
        fn append(ptr: *anyopaque, bytes: []const u8) !void {
            const self = owner(ptr);
            if (!self.sealed) return self.encoder.append(bytes);
            if (bytes.len > 32 * 1024 * 1024 -| self.tail.items.len) return error.ResourceBudgetExceeded;
            try self.tail.appendSlice(self.allocator, bytes);
        }
        fn byte(ptr: *anyopaque, value: u8) !void {
            try append(ptr, &.{value});
        }
        fn repeat(ptr: *anyopaque, value: u8, count: usize) !void {
            var bytes: [4096]u8 = @splat(value);
            var remaining = count;
            while (remaining != 0) {
                const take = @min(remaining, bytes.len);
                try append(ptr, bytes[0..take]);
                remaining -= take;
            }
        }
        fn patch(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
            const self = owner(ptr);
            if (offset > length(ptr) or bytes.len > length(ptr) - offset) return error.InvalidAtomicWriteOffset;
            const body: usize = @intCast(self.encoder.payload_bytes);
            if (offset < body) {
                if (bytes.len > body - offset) return error.InvalidAtomicWriteOffset;
                return self.encoder.patch(offset, bytes);
            }
            @memcpy(self.tail.items[offset - body ..][0..bytes.len], bytes);
        }
        pub fn readInto(self: *Self, offset: u64, bytes: []u8) !void {
            if (offset > length(self) or bytes.len > length(self) - offset) return error.EndOfStream;
            const body = self.encoder.payload_bytes;
            const take: usize = @intCast(@min(bytes.len, body -| offset));
            if (take != 0) {
                try self.buffered.flush();
                try self.buffered.writer.readAt(@intCast(pack.header_bytes + offset), bytes[0..take]);
            }
            if (take < bytes.len) @memcpy(bytes[take..], self.tail.items[@intCast(offset + take - body)..][0 .. bytes.len - take]);
        }
        fn crcRange(ptr: *anyopaque, offset: usize, count: usize) !u32 {
            const self = owner(ptr);
            var crc = std.hash.Crc32.init();
            var bytes: [64 * 1024]u8 = undefined;
            var copied: usize = 0;
            while (copied < count) {
                const take = @min(bytes.len, count - copied);
                try self.readInto(offset + copied, bytes[0..take]);
                crc.update(bytes[0..take]);
                copied += take;
            }
            return crc.final();
        }
        fn crcPrefix(ptr: *anyopaque, count: usize) !u32 {
            return crcRange(ptr, 0, count);
        }
        fn seal(ptr: *anyopaque) !void {
            owner(ptr).sealed = true;
        }
        fn resident(ptr: *anyopaque) usize {
            return owner(ptr).tail.capacity;
        }
        pub fn preview(self: *Self, a: A, body: Source) !Source {
            if (!self.sealed or body.len() != self.encoder.payload_bytes) return error.InvalidFieldPack;
            return Preview.create(a, body, self.tail.items);
        }
        const vtable: segment.SegmentSink.VTable = .{ .len = length, .append_slice = append, .append_byte = byte, .append_ntimes = repeat, .write_at = patch, .crc32_prefix = crcPrefix, .crc32_range = crcRange, .seal_payload = seal, .resident_bytes = resident };
    };
}

const Preview = struct {
    allocator: A,
    body: Source,
    tail: []u8,
    fn create(a: A, body: Source, tail: []const u8) !Source {
        const self = try a.create(Preview);
        errdefer a.destroy(self);
        self.* = .{ .allocator = a, .body = body, .tail = try a.dupe(u8, tail) };
        return .{ .ranges = .{ .ptr = self, .length = body.len() + tail.len, .read_into = read, .close = close, .check_read_context = check, .retained_bytes = retained } };
    }
    fn owner(ptr: *anyopaque) *Preview {
        return @ptrCast(@alignCast(ptr));
    }
    fn read(ptr: *anyopaque, offset: u64, bytes: []u8) !void {
        const self = owner(ptr);
        const total = self.body.len() + self.tail.len;
        if (offset > total or bytes.len > total - offset) return error.EndOfStream;
        const take: usize = @intCast(@min(bytes.len, self.body.len() -| offset));
        if (take != 0) try self.body.readInto(offset, bytes[0..take]);
        if (take < bytes.len) @memcpy(bytes[take..], self.tail[@intCast(offset + take - self.body.len())..][0 .. bytes.len - take]);
    }
    fn close(ptr: *anyopaque) void {
        const self = owner(ptr);
        self.body.close();
        self.allocator.free(self.tail);
        self.allocator.destroy(self);
    }
    fn check(ptr: *anyopaque) !void {
        try owner(ptr).body.checkReadContext();
    }
    fn retained(ptr: *anyopaque) usize {
        const self = owner(ptr);
        return @sizeOf(Preview) + self.tail.len +| self.body.retainedBytes();
    }
};
