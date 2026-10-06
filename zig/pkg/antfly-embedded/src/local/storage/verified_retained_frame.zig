// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Immutable, authenticated retained-frame view. Validation happens once;
//! ordinal boundaries permit exact durable-cursor resumption without scanning
//! consumed effects. The owner must retain immutable bytes until deinit.
const std = @import("std");
const retained = @import("retained_effects.zig");
const streamed = @import("retained_frame.zig");

pub const Frame = struct {
    reader: retained.Reader,
    offsets: []u32,
    alloc: std.mem.Allocator,
    stream: ?streamed.View = null,
    chunk_cache: ?*streamed.View.ChunkCache = null,

    pub fn init(alloc: std.mem.Allocator, bytes: []const u8, sequence_number: u64) !Frame {
        return fromReader(alloc, try retained.Reader.init(bytes, sequence_number));
    }

    pub fn fromReader(alloc: std.mem.Allocator, original: retained.Reader) !Frame {
        var reader = original;
        const offsets = try alloc.alloc(u32, @as(usize, reader.remaining) + 1);
        errdefer alloc.free(offsets);
        for (offsets[0 .. offsets.len - 1]) |*offset| {
            offset.* = @intCast(reader.pos);
            _ = (try reader.next()) orelse return error.RetainedEffectsCorrupt;
        }
        offsets[offsets.len - 1] = @intCast(reader.pos);
        _ = try reader.next();
        reader.pos = 16;
        reader.remaining = @intCast(offsets.len - 1);
        return .{ .reader = reader, .offsets = offsets, .alloc = alloc };
    }

    pub fn deinit(self: *Frame) void {
        if (self.stream == null) self.alloc.free(self.offsets);
        self.* = undefined;
    }

    /// The owner retains the immutable descriptor, source lease and chunk
    /// cache. Construction performs bounded envelope IO, never a full-frame
    /// allocation. The descriptor's authenticated offset table is the index.
    pub fn fromStream(view: streamed.View, cache: *streamed.View.ChunkCache) !Frame {
        try view.validateEnvelope(cache);
        return .{ .reader = undefined, .offsets = &.{}, .alloc = undefined, .stream = view, .chunk_cache = cache };
    }

    pub fn digest(self: *const Frame) [32]u8 {
        return if (self.stream) |view| view.descriptor_digest else self.reader.frame_digest;
    }

    pub fn sequence(self: *const Frame) u64 {
        return if (self.stream) |view| view.sequence else std.mem.readInt(u64, self.reader.bytes[4..12], .little);
    }

    pub fn totalBytes(self: *const Frame) usize {
        return if (self.stream) |view| view.total else self.reader.encoded_frame.len;
    }

    pub const Effect = struct {
        key: []const u8,
        timestamp: u64,
        value_len: ?usize,
        inline_value: ?[]const u8 = null,
        frame: ?*const Frame = null,
        value_offset: u32 = 0,

        pub fn isIntegrity(self: Effect) bool {
            return @import("db/relational_integrity_contract.zig").isKey(self.key);
        }

        pub fn valueAlloc(self: Effect, alloc: std.mem.Allocator) !?[]const u8 {
            const len = self.value_len orelse return null;
            if (self.inline_value) |value| return value;
            const frame = self.frame orelse return error.RetainedEffectsCorrupt;
            const value = try alloc.alloc(u8, len);
            errdefer alloc.free(value);
            if (try frame.stream.?.readAt(self.value_offset, value, frame.chunk_cache.?) != len) return error.RetainedEffectsCorrupt;
            return value;
        }
    };

    pub const Cursor = struct {
        frame: *const Frame,
        legacy: ?retained.Reader,
        pos: u32,
        remaining: u32,
        frame_digest: [32]u8,

        pub fn next(self: *Cursor, alloc: std.mem.Allocator) !?Effect {
            if (self.legacy) |*reader| {
                const effect = try reader.next() orelse return null;
                self.pos = @intCast(reader.pos);
                self.remaining = reader.remaining;
                return .{ .key = effect.key, .timestamp = effect.timestamp, .value_len = if (effect.value) |value| value.len else null, .inline_value = effect.value };
            }
            if (self.remaining == 0) return null;
            const view = self.frame.stream.?;
            const ordinal = view.effect_count - self.remaining;
            const effect = try view.effectAt(ordinal, self.frame.chunk_cache.?);
            // Transfer keys share the public page cursor budget, independently
            // of the much larger bounded logical transaction payload.
            if (effect.key_len > 1024 * 1024) return error.RetainedEffectsCorrupt;
            const key = try alloc.alloc(u8, effect.key_len);
            errdefer alloc.free(key);
            if (try view.readAt(effect.key_offset, key, self.frame.chunk_cache.?) != key.len) return error.RetainedEffectsCorrupt;
            const integrity = @import("db/relational_integrity_contract.zig");
            if (integrity.isKey(key)) {
                _ = try integrity.parseKey(key);
                if (effect.timestamp != 0) return error.RetainedEffectsCorrupt;
            } else if (@import("db/online_vector_artifacts.zig").isKey(key)) {
                if (!view.direct_vectors or effect.timestamp != 0) return error.RetainedEffectsCorrupt;
            } else if (@import("db/online_graph_artifacts.zig").isKey(key)) {
                if (!view.graph_artifacts or effect.timestamp != 0) return error.RetainedEffectsCorrupt;
            } else if (!@import("internal_keys.zig").isStoredDocumentRowKey(key)) return error.RetainedEffectsCorrupt;
            // A resumed page checks its immediate predecessor directly, not
            // by rescanning/reallocating the entire consumed prefix.
            if (ordinal != 0) {
                const previous = try view.effectAt(ordinal - 1, self.frame.chunk_cache.?);
                if (previous.key_len > 1024 * 1024) return error.RetainedEffectsCorrupt;
                const previous_key = try alloc.alloc(u8, previous.key_len);
                defer alloc.free(previous_key);
                if (try view.readAt(previous.key_offset, previous_key, self.frame.chunk_cache.?) != previous_key.len or
                    std.mem.order(u8, previous_key, key) != .lt) return error.RetainedEffectsCorrupt;
            }
            self.pos = effect.next_offset;
            self.remaining -= 1;
            return .{ .key = key, .timestamp = effect.timestamp, .value_len = if (effect.value_len) |len| @as(usize, len) else null, .frame = self.frame, .value_offset = effect.value_offset };
        }
    };

    pub fn cursorAt(self: *const Frame, offset: u32, remaining: u32) !Cursor {
        if (self.stream) |view| {
            const left = if (offset == 0) view.effect_count else remaining;
            if ((offset == 0 and remaining != 0) or left > view.effect_count) return error.InvalidRestoreStagingRecord;
            const position = try view.offsetAt(view.effect_count - left);
            if (offset != 0 and offset != position) return error.InvalidRestoreStagingRecord;
            return .{ .frame = self, .legacy = null, .pos = position, .remaining = left, .frame_digest = self.digest() };
        }
        const reader = try self.readerAt(offset, remaining);
        return .{ .frame = self, .legacy = reader, .pos = @intCast(reader.pos), .remaining = reader.remaining, .frame_digest = self.digest() };
    }

    /// Both offset and remaining count are replicated progress. Direct ordinal
    /// addressing validates their correspondence in O(1), without trusting an
    /// arbitrary byte offset or reparsing the consumed prefix.
    pub fn readerAt(self: *const Frame, offset: u32, remaining: u32) !retained.Reader {
        if (self.stream != null) return error.RetainedEffectsUnsupported;
        if (offset == 0) {
            if (remaining != 0) return error.InvalidRestoreStagingRecord;
            return self.reader;
        }
        if (remaining > self.reader.remaining) return error.InvalidRestoreStagingRecord;
        const ordinal = self.reader.remaining - remaining;
        if (self.offsets[ordinal] != offset) return error.InvalidRestoreStagingRecord;
        var reader = self.reader;
        reader.pos = offset;
        reader.remaining = remaining;
        return reader;
    }
};
