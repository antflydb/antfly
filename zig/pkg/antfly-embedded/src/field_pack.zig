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

//! Append-once field-resource packs. Metadata commits to page hashes, allowing
//! sparse authenticated reads without hashing an unchanged pack at each open.
//! Pack identities are tagged Merkle identities, never legacy segment byte SHA.
const std = @import("std");
const Source = @import("segment_source.zig").Source;
const SegmentSink = @import("segment.zig").SegmentSink;
const A = std.mem.Allocator;
const H = std.crypto.hash.sha2.Sha256;
pub const Digest = [32]u8;
pub const page_bytes = 64 * 1024;
pub const header_bytes = 8;
const footer_bytes = 24;
const domain = "antfly/immutable-field-pack/v1\x00";
pub const Limits = struct { metadata_bytes: usize = 32 * 1024 * 1024 };
pub const Metadata = struct { identity: Digest, payload_bytes: u64, pages: u64 };

fn header() [header_bytes]u8 {
    var bytes: [header_bytes]u8 = undefined;
    @memcpy(bytes[0..4], "AFP1");
    std.mem.writeInt(u32, bytes[4..8], page_bytes, .little);
    return bytes;
}
fn footer(payload: u64, pages: u64) [footer_bytes]u8 {
    var bytes: [footer_bytes]u8 = undefined;
    @memcpy(bytes[0..4], "AFPE");
    std.mem.writeInt(u64, bytes[4..12], payload, .little);
    std.mem.writeInt(u64, bytes[12..20], pages, .little);
    std.mem.writeInt(u32, bytes[20..24], page_bytes, .little);
    return bytes;
}

pub const Encoder = struct {
    allocator: A,
    sink: *SegmentSink,
    hashes: std.ArrayListUnmanaged(Digest) = .empty,
    dirty: std.ArrayListUnmanaged(bool) = .empty,
    partial: H = H.init(.{}),
    partial_bytes: usize = 0,
    payload_bytes: u64 = 0,
    limits: Limits,
    closed: bool = false,
    failed: bool = false,
    pub fn init(a: A, sink: *SegmentSink, limits: Limits) !Encoder {
        if (sink.len() != 0 or limits.metadata_bytes < header_bytes + footer_bytes + 32) return error.InvalidFieldPack;
        try sink.appendSlice(&header());
        return .{ .allocator = a, .sink = sink, .limits = limits };
    }
    pub fn deinit(self: *Encoder) void {
        self.hashes.deinit(self.allocator);
        self.dirty.deinit(self.allocator);
        self.* = undefined;
    }
    pub fn append(self: *Encoder, bytes: []const u8) !void {
        if (self.closed or self.failed) return error.FieldPackClosed;
        errdefer self.failed = true;
        const next_bytes = try std.math.add(u64, self.payload_bytes, bytes.len);
        const next_pages = (next_bytes / page_bytes) + @intFromBool(next_bytes % page_bytes != 0);
        if (next_pages > (self.limits.metadata_bytes - header_bytes - footer_bytes) / 32) return error.FieldPackBudgetExceeded;
        const additional = std.math.cast(usize, next_pages - self.hashes.items.len) orelse return error.FieldPackBudgetExceeded;
        try self.hashes.ensureUnusedCapacity(self.allocator, additional);
        var offset: usize = 0;
        while (offset < bytes.len) {
            const take = @min(bytes.len - offset, page_bytes - self.partial_bytes);
            self.partial.update(bytes[offset..][0..take]);
            self.partial_bytes += take;
            offset += take;
            if (self.partial_bytes == page_bytes) {
                self.hashes.appendAssumeCapacity(self.partial.finalResult());
                self.partial = H.init(.{});
                self.partial_bytes = 0;
            }
        }
        try self.sink.appendSlice(bytes);
        self.payload_bytes = next_bytes;
    }
    /// Only private construction may patch pages. Publication remains append
    /// once: the final Merkle directory is emitted after every patch is sealed.
    pub fn patch(self: *Encoder, offset: usize, bytes: []const u8) !void {
        if (self.closed or self.failed) return error.FieldPackClosed;
        errdefer self.failed = true;
        if (offset > self.payload_bytes or bytes.len > self.payload_bytes - offset) return error.InvalidFieldPack;
        if (bytes.len == 0) return;
        const pages: usize = @intCast(self.payload_bytes / page_bytes + @intFromBool(self.payload_bytes % page_bytes != 0));
        const before = self.dirty.items.len;
        try self.dirty.resize(self.allocator, pages);
        @memset(self.dirty.items[before..], false);
        const first = offset / page_bytes;
        const last = (offset + bytes.len - 1) / page_bytes;
        @memset(self.dirty.items[first .. last + 1], true);
        try self.sink.writeAt(header_bytes + offset, bytes);
    }

    pub fn finishPatched(self: *Encoder, reader: anytype) !Metadata {
        if (self.closed or self.failed) return error.FieldPackClosed;
        errdefer self.failed = true;
        if (self.partial_bytes != 0) {
            try self.hashes.append(self.allocator, self.partial.finalResult());
            self.partial_bytes = 0;
        }
        var buffer: [page_bytes]u8 = undefined;
        for (self.dirty.items, 0..) |*dirty, page| if (dirty.*) {
            const length: usize = @intCast(@min(page_bytes, self.payload_bytes - @as(u64, page) * page_bytes));
            try reader.readInto(@as(u64, page) * page_bytes, buffer[0..length]);
            H.hash(buffer[0..length], &self.hashes.items[page], .{});
            dirty.* = false;
        };
        return self.finish();
    }

    /// The caller finishes/forces its private atomic sink before admitting this
    /// metadata to a family. Metadata alone is neither publication nor a lease.
    pub fn finish(self: *Encoder) !Metadata {
        if (self.closed or self.failed) return error.FieldPackClosed;
        errdefer self.failed = true;
        if (self.payload_bytes == 0) return error.InvalidFieldPack;
        for (self.dirty.items) |dirty| if (dirty) return error.UnsealedFieldPackPatch;
        if (self.partial_bytes != 0) {
            try self.hashes.append(self.allocator, self.partial.finalResult());
            self.partial_bytes = 0;
        }
        const tail = footer(self.payload_bytes, self.hashes.items.len);
        var hash = H.init(.{});
        hash.update(domain);
        hash.update(&header());
        for (self.hashes.items) |*page| {
            hash.update(page);
            try self.sink.appendSlice(page);
        }
        hash.update(&tail);
        try self.sink.appendSlice(&tail);
        self.closed = true;
        return .{ .identity = hash.finalResult(), .payload_bytes = self.payload_bytes, .pages = self.hashes.items.len };
    }
};

const Context = struct {
    allocator: A,
    physical: Source,
    hashes: []Digest,
    verified: []std.atomic.Value(u8),
    payload_bytes: u64,
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.physical.close();
        self.allocator.free(self.verified);
        self.allocator.free(self.hashes);
        self.allocator.destroy(self);
    }
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.physical.checkReadContext();
    }
    fn retained(raw: *anyopaque) usize {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return @sizeOf(Context) + self.hashes.len * @sizeOf(Digest) + self.verified.len * @sizeOf(std.atomic.Value(u8)) +| self.physical.retainedBytes();
    }
    fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.physical.checkReadContext();
        if (offset > self.payload_bytes or out.len > self.payload_bytes - offset) return error.EndOfStream;
        var position = offset;
        var copied: usize = 0;
        while (copied < out.len) {
            const page: usize = @intCast(position / page_bytes);
            const within: usize = @intCast(position % page_bytes);
            const length: usize = @intCast(@min(page_bytes, self.payload_bytes - @as(u64, page) * page_bytes));
            const take = @min(out.len - copied, length - within);
            const target = out[copied..][0..take];
            switch (self.verified[page].load(.acquire)) {
                1 => try self.physical.readInto(header_bytes + position, target),
                2 => return error.FieldPackIntegrityMismatch,
                else => try self.readUnverified(page, within, target, length),
            }
            position += take;
            copied += take;
        }
    }
    // Cold-page staging stays off the verified fast path's stack. Concurrent
    // cold checks may duplicate hashing; they publish only immutable agreement,
    // and transient I/O failures never poison a page as corrupt.
    noinline fn readUnverified(self: *@This(), page: usize, within: usize, target: []u8, length: usize) !void {
        var bytes: [page_bytes]u8 = undefined;
        try self.physical.readInto(header_bytes + @as(u64, page) * page_bytes, bytes[0..length]);
        var actual: Digest = undefined;
        H.hash(bytes[0..length], &actual, .{});
        if (!std.mem.eql(u8, &actual, &self.hashes[page])) {
            self.verified[page].store(2, .release);
            return error.FieldPackIntegrityMismatch;
        }
        self.verified[page].store(1, .release);
        @memcpy(target, bytes[within..][0..target.len]);
    }
};

/// Consumes physical-source ownership only on success. The expected identity
/// must come from the held native field inventory, not an untrusted descriptor.
/// Reads authenticate required pages lazily and retain independent file leases.
pub fn open(a: A, physical: Source, expected: Digest, limits: Limits) !Source {
    try physical.checkReadContext();
    if (physical.len() < header_bytes + footer_bytes + 33) return error.InvalidFieldPack;
    var first: [header_bytes]u8 = undefined;
    var tail: [footer_bytes]u8 = undefined;
    try physical.readInto(0, &first);
    try physical.readInto(physical.len() - footer_bytes, &tail);
    if (!std.mem.eql(u8, &first, &header()) or !std.mem.eql(u8, tail[0..4], "AFPE") or std.mem.readInt(u32, tail[20..24], .little) != page_bytes) return error.InvalidFieldPack;
    const payload = std.mem.readInt(u64, tail[4..12], .little);
    const count = std.mem.readInt(u64, tail[12..20], .little);
    if (payload == 0 or count != payload / page_bytes + @intFromBool(payload % page_bytes != 0)) return error.InvalidFieldPack;
    const table_bytes = try std.math.mul(u64, count, 32);
    if (limits.metadata_bytes < header_bytes + footer_bytes or table_bytes > limits.metadata_bytes - header_bytes - footer_bytes) return error.FieldPackBudgetExceeded;
    const table_offset = try std.math.add(u64, header_bytes, payload);
    if (try std.math.add(u64, try std.math.add(u64, table_offset, table_bytes), footer_bytes) != physical.len()) return error.InvalidFieldPack;
    const hashes = try a.alloc(Digest, @intCast(count));
    errdefer a.free(hashes);
    try physical.readInto(table_offset, std.mem.sliceAsBytes(hashes));
    var hash = H.init(.{});
    hash.update(domain);
    hash.update(&first);
    hash.update(std.mem.sliceAsBytes(hashes));
    hash.update(&tail);
    if (!std.mem.eql(u8, &hash.finalResult(), &expected)) return error.FieldPackIntegrityMismatch;
    const verified = try a.alloc(std.atomic.Value(u8), @intCast(count));
    errdefer a.free(verified);
    for (verified) |*state| state.* = .init(0);
    const context = try a.create(Context);
    context.* = .{ .allocator = a, .physical = physical, .hashes = hashes, .verified = verified, .payload_bytes = payload };
    return .{ .ranges = .{ .ptr = context, .length = payload, .read_into = Context.read, .close = Context.close, .check_read_context = Context.check, .retained_bytes = Context.retained } };
}

test "persistent source publication private pack patches authenticate final pages" {
    const a = std.testing.allocator;
    var memory = @import("segment.zig").MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var encoder = try Encoder.init(a, &sink, .{});
    defer encoder.deinit();
    const body = try a.alloc(u8, page_bytes + 10);
    defer a.free(body);
    @memset(body, 'a');
    try encoder.append(body);
    try encoder.patch(page_bytes - 2, "xyz123");
    const Read = struct {
        memory: *@import("segment.zig").MemorySegmentSink,
        pub fn readInto(self: @This(), offset: u64, out: []u8) !void {
            @memcpy(out, self.memory.out.items[@intCast(header_bytes + offset)..][0..out.len]);
        }
    };
    const metadata = try encoder.finishPatched(Read{ .memory = &memory });
    var admitted = try open(a, .{ .contiguous = memory.out.items }, metadata.identity, .{});
    defer admitted.close();
    var bytes: [6]u8 = undefined;
    try admitted.readInto(page_bytes - 2, &bytes);
    try std.testing.expectEqualStrings("xyz123", &bytes);
    try std.testing.expectError(error.FieldPackClosed, encoder.patch(0, "bad"));
}
