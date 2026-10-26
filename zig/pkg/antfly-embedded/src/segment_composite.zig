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

//! Immutable segment extents. A compact descriptor composes retained physical
//! resources without parent-generation chains or copying unchanged sections.
//! Descriptors provide integrity and navigation only. The native provider must
//! authenticate resource membership and retain the actual storage/read owner.
const std = @import("std");
const source_mod = @import("segment_source.zig");
const abi = @import("runtime_callback_abi.zig");
const A = std.mem.Allocator;
pub const Source = source_mod.Source;
pub const Digest = [32]u8;
pub const Limits = struct { metadata_bytes: usize = 32 * 1024 * 1024, extents: usize = 65536, resources: usize = 65536 };
pub const ResourceSlice = struct { identity: Digest, source_bytes: u64, offset: u64, length: u64 };
pub const Extent = union(enum) { inline_bytes: []const u8, resource: ResourceSlice };
pub const Provider = struct {
    ptr: *anyopaque,
    resolve: *const fn (*anyopaque, A, Digest) anyerror!Source,
    retain: *const fn (*anyopaque) void,
    release: *const fn (*anyopaque) void,
    retained_bytes: ?*const fn (*anyopaque) usize = null,
    check: ?*const fn (*anyopaque) anyerror!void = null,
    dispatch: abi.CallbackDispatch = Boundary.local_dispatch,
};
const Boundary = abi.Boundary(struct {
    resolve: *const fn (*anyopaque, A, Digest) anyerror!Source,
    check: ?*const fn (*anyopaque) anyerror!void,
});
const magic = "ACE1";
const header_bytes = 16;

pub fn isDescriptor(raw: []const u8) bool {
    return std.mem.startsWith(u8, raw, magic);
}

fn extentBytes(extent: Extent) !u64 {
    return switch (extent) {
        .inline_bytes => |bytes| if (bytes.len == 0) error.InvalidCompositeSegment else bytes.len,
        .resource => |resource| blk: {
            if (resource.length == 0 or resource.offset > resource.source_bytes or resource.length > resource.source_bytes - resource.offset) return error.InvalidCompositeSegment;
            break :blk resource.length;
        },
    };
}

/// Encodes a bounded descriptor. Each resource identity commits to one direct
/// immutable object; callers flatten prior descriptors before reusing views.
pub fn encode(a: A, extents: []const Extent, limits: Limits) ![]u8 {
    if (extents.len == 0 or extents.len > limits.extents or extents.len > std.math.maxInt(u32)) return error.CompositeSegmentBudgetExceeded;
    var bytes: usize = header_bytes + 32;
    var logical: u64 = 0;
    for (extents) |extent| {
        logical = try std.math.add(u64, logical, try extentBytes(extent));
        const record_bytes = switch (extent) {
            .inline_bytes => |inline_bytes| blk: {
                if (inline_bytes.len > std.math.maxInt(u32)) return error.CompositeSegmentBudgetExceeded;
                break :blk try std.math.add(usize, 5, inline_bytes.len);
            },
            .resource => 57,
        };
        bytes = try std.math.add(usize, bytes, record_bytes);
        if (bytes > limits.metadata_bytes) return error.CompositeSegmentBudgetExceeded;
    }
    const raw = try a.alloc(u8, bytes);
    errdefer a.free(raw);
    @memcpy(raw[0..4], magic);
    std.mem.writeInt(u64, raw[4..12], logical, .little);
    std.mem.writeInt(u32, raw[12..16], @intCast(extents.len), .little);
    var offset: usize = header_bytes;
    for (extents) |extent| switch (extent) {
        .inline_bytes => |inline_bytes| {
            raw[offset] = 0;
            std.mem.writeInt(u32, raw[offset + 1 ..][0..4], @intCast(inline_bytes.len), .little);
            @memcpy(raw[offset + 5 ..][0..inline_bytes.len], inline_bytes);
            offset += 5 + inline_bytes.len;
        },
        .resource => |resource| {
            raw[offset] = 1;
            @memcpy(raw[offset + 1 ..][0..32], &resource.identity);
            std.mem.writeInt(u64, raw[offset + 33 ..][0..8], resource.source_bytes, .little);
            std.mem.writeInt(u64, raw[offset + 41 ..][0..8], resource.offset, .little);
            std.mem.writeInt(u64, raw[offset + 49 ..][0..8], resource.length, .little);
            offset += 57;
        },
    };
    std.crypto.hash.sha2.Sha256.hash(raw[0..offset], raw[offset..][0..32], .{});
    return raw;
}

const Span = struct {
    end: u64,
    value: union(enum) { inline_bytes: []const u8, resource: struct { ordinal: usize, offset: u64 } },
};
const Cursor = struct {
    raw: []const u8,
    offset: usize = header_bytes,
    fn take(self: *@This(), count: usize) ![]const u8 {
        if (self.offset > self.raw.len or count > self.raw.len - self.offset) return error.InvalidCompositeSegment;
        const bytes = self.raw[self.offset..][0..count];
        self.offset += count;
        return bytes;
    }
    fn integer(self: *@This(), comptime T: type) !T {
        return std.mem.readInt(T, (try self.take(@sizeOf(T)))[0..@sizeOf(T)], .little);
    }
};
const Context = struct {
    allocator: A,
    provider: Provider,
    raw: []u8 = &.{},
    spans: []Span = &.{},
    resources: std.ArrayListUnmanaged(Source) = .empty,
    identities: std.ArrayListUnmanaged(Digest) = .empty,
    logical_bytes: u64,
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        for (self.resources.items) |*resource| resource.close();
        self.resources.deinit(self.allocator);
        self.identities.deinit(self.allocator);
        self.allocator.free(self.spans);
        self.allocator.free(self.raw);
        self.provider.release(self.provider.ptr);
        self.allocator.destroy(self);
    }
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.provider.check) |callback| try Boundary.call("check", self.provider.dispatch, callback, .{self.provider.ptr});
    }
    fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try check(raw);
        if (offset > self.logical_bytes or out.len > self.logical_bytes - offset) return error.EndOfStream;
        if (out.len == 0) return;
        var low: usize = 0;
        var high = self.spans.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            if (self.spans[middle].end <= offset) low = middle + 1 else high = middle;
        }
        var position = offset;
        var written: usize = 0;
        while (written < out.len) : (low += 1) {
            const span = self.spans[low];
            const start = if (low == 0) 0 else self.spans[low - 1].end;
            const within = position - start;
            const take: usize = @intCast(@min(out.len - written, span.end - position));
            const target = out[written..][0..take];
            switch (span.value) {
                .inline_bytes => |bytes| @memcpy(target, bytes[@intCast(within)..][0..take]),
                .resource => |resource| try self.resources.items[resource.ordinal].readInto(resource.offset + within, target),
            }
            position += take;
            written += take;
        }
    }
    fn retained(raw: *anyopaque) usize {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var bytes = @sizeOf(Context) + self.raw.len + self.spans.len * @sizeOf(Span) + self.resources.capacity * @sizeOf(Source) + self.identities.capacity * @sizeOf(Digest);
        if (self.provider.retained_bytes) |retained_provider| bytes +|= retained_provider(self.provider.ptr);
        for (self.resources.items) |resource| bytes +|= resource.retainedBytes();
        return bytes;
    }
};

/// Opens independent native resource leases once per identity. Descriptors do
/// not contain usable paths, URLs, scopes or read grants. A failing open closes
/// every acquired resource and releases the retained provider on every path.
pub fn open(a: A, descriptor: []const u8, provider: Provider, limits: Limits) !Source {
    if (descriptor.len < header_bytes + 32 or descriptor.len > limits.metadata_bytes or !isDescriptor(descriptor)) return error.InvalidCompositeSegment;
    const payload = descriptor[0 .. descriptor.len - 32];
    var expected: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &expected, .{});
    if (!std.mem.eql(u8, &expected, descriptor[descriptor.len - 32 ..])) return error.InvalidCompositeSegment;
    const count = std.mem.readInt(u32, descriptor[12..16], .little);
    if (count == 0 or count > limits.extents) return error.CompositeSegmentBudgetExceeded;
    // Every encoded extent occupies at least six bytes. Check shape before
    // allocating navigation from a self-consistent but malicious header.
    if (count > (payload.len - header_bytes) / 6) return error.InvalidCompositeSegment;
    const logical = std.mem.readInt(u64, descriptor[4..12], .little);
    if (logical == 0) return error.InvalidCompositeSegment;
    const context = try a.create(Context);
    context.* = .{ .allocator = a, .provider = provider, .logical_bytes = logical };
    provider.retain(provider.ptr);
    errdefer Context.close(context);
    try Context.check(context);
    context.raw = try a.dupe(u8, payload);
    context.spans = try a.alloc(Span, count);
    var ids: std.AutoHashMapUnmanaged(Digest, usize) = .empty;
    defer ids.deinit(a);
    var cursor: Cursor = .{ .raw = context.raw };
    var end: u64 = 0;
    for (context.spans) |*span| {
        const tag = (try cursor.take(1))[0];
        var value: @FieldType(Span, "value") = undefined;
        var length: u64 = undefined;
        switch (tag) {
            0 => {
                length = try cursor.integer(u32);
                if (length == 0) return error.InvalidCompositeSegment;
                value = .{ .inline_bytes = try cursor.take(@intCast(length)) };
            },
            1 => {
                const identity: Digest = (try cursor.take(32))[0..32].*;
                const source_bytes = try cursor.integer(u64);
                const offset = try cursor.integer(u64);
                length = try cursor.integer(u64);
                _ = try extentBytes(.{ .resource = .{ .identity = identity, .source_bytes = source_bytes, .offset = offset, .length = length } });
                const ordinal = ids.get(identity) orelse resolved: {
                    if (context.resources.items.len >= limits.resources) return error.CompositeSegmentBudgetExceeded;
                    try Context.check(context);
                    var source = try Boundary.call("resolve", provider.dispatch, provider.resolve, .{ provider.ptr, a, identity });
                    var owned = true;
                    defer if (owned) source.close();
                    if (source.len() != source_bytes) return error.InvalidCompositeSegment;
                    try context.resources.ensureUnusedCapacity(a, 1);
                    try context.identities.ensureUnusedCapacity(a, 1);
                    const slot = context.resources.items.len;
                    try ids.put(a, identity, slot);
                    context.resources.appendAssumeCapacity(source);
                    context.identities.appendAssumeCapacity(identity);
                    owned = false;
                    break :resolved slot;
                };
                if (context.resources.items[ordinal].len() != source_bytes) return error.InvalidCompositeSegment;
                value = .{ .resource = .{ .ordinal = ordinal, .offset = offset } };
            },
            else => return error.InvalidCompositeSegment,
        }
        end = try std.math.add(u64, end, length);
        span.* = .{ .end = end, .value = value };
    }
    if (cursor.offset != cursor.raw.len or end != logical) return error.InvalidCompositeSegment;
    try Context.check(context);
    return .{ .ranges = .{ .ptr = context, .length = logical, .read_into = Context.read, .close = Context.close, .check_read_context = Context.check, .retained_bytes = Context.retained } };
}

/// Flattens a held composite view into direct physical-resource extents in
/// O(log(extents) + selected extents), without opening or copying body bytes.
/// Inline views borrow this source until encode copies them. These descriptors
/// confer no resource authority; the next generation must retain its own files.
/// Unknown source wrappers must expose their descriptor through their owner;
/// casting a foreign callback context into this module is never permitted.
pub fn slice(a: A, source: Source, offset: u64, length: u64) ![]Extent {
    if (source != .ranges or source.ranges.read_into != Context.read) return error.UnsupportedCompositeSource;
    const context: *Context = @ptrCast(@alignCast(source.ranges.ptr));
    try Context.check(context);
    if (offset > context.logical_bytes or length > context.logical_bytes - offset) return error.EndOfStream;
    if (length == 0) return a.alloc(Extent, 0);
    var low: usize = 0;
    var high = context.spans.len;
    while (low < high) {
        const middle = low + (high - low) / 2;
        if (context.spans[middle].end <= offset) low = middle + 1 else high = middle;
    }
    const first = low;
    const end = offset + length;
    while (low < context.spans.len and context.spans[low].end < end) : (low += 1) {}
    const result = try a.alloc(Extent, low - first + 1);
    var position = offset;
    for (result, first..) |*extent, ordinal| {
        const span = context.spans[ordinal];
        const start = if (ordinal == 0) 0 else context.spans[ordinal - 1].end;
        const within = position - start;
        const take = @min(end, span.end) - position;
        extent.* = switch (span.value) {
            .inline_bytes => |bytes| .{ .inline_bytes = bytes[@intCast(within)..][0..@intCast(take)] },
            .resource => |resource| .{ .resource = .{
                .identity = context.identities.items[resource.ordinal],
                .source_bytes = context.resources.items[resource.ordinal].len(),
                .offset = resource.offset + within,
                .length = take,
            } },
        };
        position += take;
    }
    return result;
}
