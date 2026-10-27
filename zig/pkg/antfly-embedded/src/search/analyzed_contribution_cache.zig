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

//! Optional native compilation cache. Entries contain analyzed term contributions,
//! never document/source authority. Exact bytes and complete analyzer pipelines
//! determine identity. A miss always uses the ordinary compiler.
const std = @import("std");
const resources = @import("../storage/resource_manager.zig");
const analysis = @import("analysis.zig");
pub const Hit = struct { term: []const u8, positions: []const u32 };
pub const EncodedHit = struct {
    term: []const u8,
    positions: []const u8,
    count: u32,
    maximum_position: u32,
    pub const Iterator = struct {
        bytes: []const u8,
        previous: u64 = 0,
        remaining: u32,
        pub fn read(self: *Iterator, out: *u32) bool {
            if (self.remaining == 0) {
                std.debug.assert(self.bytes.len == 0);
                return false;
            }
            var encoded: u64 = 0;
            var shift: u6 = 0;
            while (true) {
                const byte = self.bytes[0];
                self.bytes = self.bytes[1..];
                encoded |= @as(u64, byte & 127) << shift;
                if (byte & 128 == 0) break;
                shift += 7;
            }
            const magnitude = encoded >> 1;
            if (encoded & 1 == 0) self.previous += magnitude else self.previous -= magnitude + 1;
            self.remaining -= 1;
            out.* = @intCast(self.previous);
            return true;
        }
    };
    pub fn iterator(self: EncodedHit) Iterator {
        return .{ .bytes = self.positions, .remaining = self.count };
    }
};
fn delta(previous: u32, position: u32) u64 {
    return if (position >= previous)
        @as(u64, position - previous) * 2
    else
        @as(u64, previous - position) * 2 - 1;
}
fn encodedSize(value: u64) usize {
    return @max(1, (@as(usize, 64 - @clz(value)) + 6) / 7);
}
// Native pointers would cost 40 bytes per distinct term. Four relative u32
// words keep retained dictionaries compact without constraining positions.
const Header = struct { term_end: u32, positions_end: u32, count: u32, maximum_position: u32 };
pub const minimum_input = 8 * 1024;
const slots = 32;
const entry_limit = 512 * 1024;
pub const Cache = struct {
    const Entry = struct {
        mutex: std.atomic.Mutex = .unlocked,
        key: [32]u8 = @splat(0),
        probation: [32]u8 = @splat(0),
        rejected: bool = false,
        data: ?[]align(@alignOf(Header)) u8 = null,
        headers: []const Header = &.{},
        terms: []const u8 = &.{},
        positions: []const u8 = &.{},
        tokens: u32 = 0,
        borrowers: usize = 0,
        reservation: ?resources.Reservation = null,
    };
    manager: *resources.ResourceManager,
    metadata: resources.Reservation,
    reclaimer: u64 = 0,
    hits: std.atomic.Value(u64) = .init(0),
    tokens_reused: std.atomic.Value(u64) = .init(0),
    entries: [slots]Entry = @splat(.{}),

    pub fn create(manager: *resources.ResourceManager) !*Cache {
        var reservation = try manager.reserveImmediate(.full_text_segment_residency, std.mem.alignForward(usize, @sizeOf(Cache), std.heap.pageSize()));
        errdefer reservation.release();
        const self = try std.heap.page_allocator.create(Cache);
        errdefer std.heap.page_allocator.destroy(self);
        self.* = .{ .manager = manager, .metadata = reservation };
        self.reclaimer = try manager.registerReclaimer(.full_text_segment_residency, self, reclaim);
        return self;
    }
    pub fn key(analyzer: *const analysis.Analyzer, text: []const u8) ![32]u8 {
        const H = std.crypto.hash.sha2.Sha256;
        var writer = std.Io.Writer.Hashing(H).init(&.{});
        try std.json.Stringify.value(analyzer.*, .{ .emit_null_optional_fields = true }, &writer.writer);
        try writer.writer.flush();
        var hash = H.init(.{});
        hash.update("antfly/analyzed-contribution/v1\x00");
        hash.update(&writer.hasher.finalResult());
        hash.update(text);
        return hash.finalResult();
    }
    fn entry(self: *Cache, digest: [32]u8) *Entry {
        return &self.entries[digest[0] & (slots - 1)];
    }
    pub const Lease = struct {
        slot: *Entry,
        tokens: u32,
        pub fn hitCount(self: *const Lease) usize {
            return self.slot.headers.len;
        }
        pub fn hitAt(self: *const Lease, index: usize) EncodedHit {
            const header = self.slot.headers[index];
            const term_start = if (index == 0) 0 else self.slot.headers[index - 1].term_end;
            const position_start = if (index == 0) 0 else self.slot.headers[index - 1].positions_end;
            return .{ .term = self.slot.terms[term_start..header.term_end], .positions = self.slot.positions[position_start..header.positions_end], .count = header.count, .maximum_position = header.maximum_position };
        }
        pub fn deinit(self: *Lease) void {
            while (!self.slot.mutex.tryLock()) std.atomic.spinLoopHint();
            std.debug.assert(self.slot.borrowers != 0);
            self.slot.borrowers -= 1;
            self.slot.mutex.unlock();
            self.* = undefined;
        }
    };
    pub fn borrow(self: *Cache, digest: [32]u8) ?Lease {
        const slot = self.entry(digest);
        if (!slot.mutex.tryLock()) return null;
        defer slot.mutex.unlock();
        if (slot.data == null or !std.mem.eql(u8, &slot.key, &digest)) return null;
        slot.borrowers += 1;
        _ = self.hits.fetchAdd(1, .monotonic);
        _ = self.tokens_reused.fetchAdd(slot.tokens, .monotonic);
        return .{ .slot = slot, .tokens = slot.tokens };
    }
    /// Demand admission: a one-time input does not allocate retained terms.
    pub fn observe(self: *Cache, digest: [32]u8, known_reuse: bool) bool {
        const slot = self.entry(digest);
        if (!slot.mutex.tryLock()) return false;
        defer slot.mutex.unlock();
        const repeated = std.mem.eql(u8, &slot.probation, &digest);
        if (!repeated) slot.rejected = false;
        slot.probation = digest;
        return (repeated or known_reuse) and !slot.rejected;
    }
    pub fn maximumTerms(input_bytes: usize) usize {
        return input_bytes / 2 / @sizeOf(Header);
    }
    pub fn reject(self: *Cache, digest: [32]u8) void {
        const slot = self.entry(digest);
        if (!slot.mutex.tryLock()) return;
        defer slot.mutex.unlock();
        if (std.mem.eql(u8, &slot.probation, &digest)) slot.rejected = true;
    }
    pub fn put(self: *Cache, digest: [32]u8, hits: []const Hit, tokens: u32, input_bytes: usize) void {
        if (hits.len == 0) return;
        var size = std.math.mul(usize, hits.len, @sizeOf(Header)) catch return;
        for (hits) |hit| {
            var previous: u32 = 0;
            for (hit.positions) |position| {
                size = std.math.add(usize, size, encodedSize(delta(previous, position))) catch return;
                previous = position;
            }
        }
        const terms_start = size;
        for (hits) |hit| size = std.math.add(usize, size, hit.term.len) catch return;
        if (size > entry_limit or size > input_bytes / 2) {
            const slot = self.entry(digest);
            if (slot.mutex.tryLock()) {
                if (std.mem.eql(u8, &slot.probation, &digest)) slot.rejected = true;
                slot.mutex.unlock();
            }
            return;
        }
        // Admission does not reclaim synchronously and never holds a cache lock.
        var reservation = self.manager.reserveImmediate(.full_text_segment_residency, std.mem.alignForward(usize, size, std.heap.pageSize())) catch return;
        var transferred = false;
        defer if (!transferred) reservation.release();
        const slot = self.entry(digest);
        if (!slot.mutex.tryLock()) return;
        defer slot.mutex.unlock();
        if (slot.borrowers != 0 or (slot.data != null and std.mem.eql(u8, &slot.key, &digest))) return;
        // Free before allocating: replacement never doubles the resident bound.
        const old = slot.data;
        var old_reservation = slot.reservation;
        slot.data = null;
        slot.headers = &.{};
        slot.terms = &.{};
        slot.positions = &.{};
        slot.reservation = null;
        if (old) |bytes| {
            std.heap.page_allocator.free(bytes);
            old_reservation.?.release();
        }
        const data = std.heap.page_allocator.alignedAlloc(u8, .of(Header), size) catch return;
        const output: []Header = @alignCast(std.mem.bytesAsSlice(Header, data[0 .. hits.len * @sizeOf(Header)]));
        const headers_bytes = hits.len * @sizeOf(Header);
        var positions_offset = headers_bytes;
        var terms_offset = terms_start;
        for (hits, output) |hit, *out| {
            var previous: u32 = 0;
            var maximum: u32 = 0;
            for (hit.positions) |position| {
                var value = delta(previous, position);
                previous = position;
                maximum = @max(maximum, position);
                while (value >= 128) {
                    data[positions_offset] = @as(u8, @truncate(value)) | 128;
                    positions_offset += 1;
                    value >>= 7;
                }
                data[positions_offset] = @intCast(value);
                positions_offset += 1;
            }
            const term = data[terms_offset..][0..hit.term.len];
            @memcpy(term, hit.term);
            terms_offset += term.len;
            out.* = .{ .term_end = @intCast(terms_offset - terms_start), .positions_end = @intCast(positions_offset - headers_bytes), .count = @intCast(hit.positions.len), .maximum_position = maximum };
        }
        std.debug.assert(positions_offset == terms_start and terms_offset == size);
        slot.*.data = data;
        slot.headers = output;
        slot.positions = data[headers_bytes..terms_start];
        slot.terms = data[terms_start..];
        slot.tokens = tokens;
        slot.key = digest;
        slot.reservation = reservation;
        transferred = true;
    }
    pub fn reclaim(ptr: *anyopaque, target: u64) u64 {
        const self: *Cache = @ptrCast(@alignCast(ptr));
        var freed: u64 = 0;
        for (&self.entries) |*slot| {
            if (freed >= target) break;
            if (!slot.mutex.tryLock()) continue;
            if (slot.borrowers != 0) {
                slot.mutex.unlock();
                continue;
            }
            const data = slot.data;
            var reservation = slot.reservation;
            slot.data = null;
            slot.headers = &.{};
            slot.terms = &.{};
            slot.positions = &.{};
            slot.reservation = null;
            slot.mutex.unlock();
            if (data) |bytes| {
                freed += reservation.?.bytes;
                std.heap.page_allocator.free(bytes);
                reservation.?.release();
            }
        }
        return freed;
    }
    pub fn deinit(self: *Cache) void {
        self.manager.unregisterReclaimer(self.reclaimer);
        _ = reclaim(self, std.math.maxInt(u64));
        for (self.entries) |slot| std.debug.assert(slot.borrowers == 0);
        self.metadata.release();
        std.heap.page_allocator.destroy(self);
    }
};
