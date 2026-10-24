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

//! Replay sequence references with native emission provenance. Only assembler-
//! registered metadata is inspected; user bytes never become numeric fixups.
//! Symbolic generation uses the widest representation, reserving enough owned
//! backing for later JSON growth without allocating in the binding operation.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const mutations = @import("../transaction_batch_admission.zig");
const identity = @import("doc_identity.zig");
const groups = @import("group_source_index.zig");
const transfer = @import("group_source_transfer.zig");
const queue = @import("group_source_queue.zig");
const debt = @import("group_source_debt.zig");
const A = std.mem.Allocator;
pub const symbolic = std.math.maxInt(u64);
pub const Origin = enum { identity, group, child, artifact_revision, primary_revision, journal };
const WriteIdentity = struct { key: usize, value: usize, key_len: usize, value_len: usize };
const KeyIdentity = struct { ptr: usize, len: usize };
pub const Sources = struct {
    a: A,
    writes: std.AutoHashMapUnmanaged(WriteIdentity, Origin) = .empty,
    deletes: std.AutoHashMapUnmanaged(KeyIdentity, Origin) = .empty,
    pub fn init(a: A) Sources {
        return .{ .a = a };
    }
    pub fn deinit(self: *Sources) void {
        self.writes.deinit(self.a);
        self.deletes.deinit(self.a);
    }
    fn id(key: []const u8, value: []const u8) WriteIdentity {
        return .{ .key = @intFromPtr(key.ptr), .value = @intFromPtr(value.ptr), .key_len = key.len, .value_len = value.len };
    }
    pub fn remember(self: *Sources, origin: Origin, key: []const u8, value: []const u8) !void {
        try self.writes.put(self.a, id(key, value), origin);
    }
    pub fn rememberDelete(self: *Sources, origin: Origin, key: []const u8) !void {
        try self.deletes.put(self.a, .{ .ptr = @intFromPtr(key.ptr), .len = key.len }, origin);
    }
};
const Target = enum { key, value, deletion, replay };
const Kind = enum { big, little, identity, event, debt_pointer, journal };
const Number = struct { offset: usize, len: usize = 20 };
const Blob = struct {
    target: Target,
    index: usize,
    kind: Kind,
    offset: usize = 0,
    capacity: usize,
    fields: [2]?Number = .{ null, null },
    digest: [32]u8 = undefined,
    fn bytes(self: Blob, batch: *const mutations.OwnedBatch) []const u8 {
        return switch (self.target) {
            .key => batch.writes[self.index].key,
            .value => batch.writes[self.index].value,
            .deletion => batch.deletes[self.index],
            .replay => batch.replay.?.payload,
        };
    }
    fn mutable(self: Blob, batch: *mutations.OwnedBatch) []u8 {
        const offset = @intFromPtr(self.bytes(batch).ptr) - @intFromPtr(batch.backing.ptr);
        return batch.backing[offset..][0..self.capacity];
    }
    fn setLength(self: Blob, batch: *mutations.OwnedBatch, len: usize) void {
        const raw = self.mutable(batch)[0..len];
        switch (self.target) {
            .key => @constCast(batch.writes)[self.index].key = raw,
            .value => @constCast(batch.writes)[self.index].value = raw,
            .deletion => @constCast(batch.deletes)[self.index] = raw,
            .replay => batch.replay.?.payload = raw,
        }
    }
};
fn hash(raw: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &result, .{});
    return result;
}
fn check(original: ?admission.Port) !void {
    if (original) |owner| try owner.checkpoint();
}
const Generations = struct {
    raw: []const u8,
    threshold: ?u64 = null,
    found: bool = false,
    original: ?admission.Port,
    pub fn big(self: *Generations, offset: usize) !void {
        try check(self.original);
        const value = std.mem.readInt(u64, self.raw[offset..][0..8], .big);
        if (self.threshold) |limit| {
            if (value >= limit) return error.PreparedGenerationChanged;
        } else if (value == symbolic) self.found = true;
    }
};
const ReplaceGenerations = struct {
    raw: []u8,
    from: u64,
    to: u64,
    pub fn big(self: *@This(), offset: usize) !void {
        if (std.mem.readInt(u64, self.raw[offset..][0..8], .big) == self.from) std.mem.writeInt(u64, self.raw[offset..][0..8], self.to, .big);
    }
};
pub const Bindings = struct {
    a: A,
    current: u64 = symbolic,
    blobs: std.ArrayListUnmanaged(Blob) = .empty,
    pub fn deinit(self: *Bindings) void {
        self.blobs.deinit(self.a);
        self.* = undefined;
    }
    pub fn capture(a: A, store: anytype, input_writes: anytype, input_deletes: []const []const u8, batch: *mutations.OwnedBatch, sources: *const Sources, next: u64, original: ?admission.Port) !Bindings {
        if (next == 0 or next >= symbolic) return error.ResourceLimitExceeded;
        var result: Bindings = .{ .a = a };
        errdefer result.deinit();
        var read = if (original) |port| try store.beginReadTxnWithAdmission(.retain, port) else try store.beginReadTxn();
        defer read.abort();
        for (input_writes, 0..) |write, index| {
            try check(original);
            const origin = sources.writes.get(Sources.id(write.key, write.value)) orelse continue;
            const owned = batch.writes[index];
            switch (origin) {
                .identity => {
                    var probe: Generations = .{ .raw = owned.value, .original = original };
                    _ = try identity.visitReplayGenerations(owned.key, owned.value, &probe);
                    if (!probe.found) continue;
                    const previous = read.get(owned.key) catch |cause| if (cause == error.NotFound) null else return cause;
                    if (previous) |raw| {
                        var old: Generations = .{ .raw = raw, .threshold = next, .original = original };
                        _ = try identity.visitReplayGenerations(owned.key, raw, &old);
                    }
                    try result.add(batch, .{ .target = .value, .index = index, .kind = .identity, .capacity = owned.value.len });
                },
                .primary_revision, .artifact_revision => {
                    if (owned.value.len != 8 or std.mem.readInt(u64, owned.value[0..8], if (origin == .primary_revision) .little else .big) != symbolic) return error.InvalidDerivedSequence;
                    try result.add(batch, .{ .target = .value, .index = index, .kind = if (origin == .primary_revision) .little else .big, .capacity = 8 });
                },
                .child => try result.addKey(batch, index, .key, 3),
                .journal => try result.addJournal(batch, index, .value, original),
                .group => {
                    if (std.mem.startsWith(u8, owned.key, debt.namespace)) {
                        if (owned.key.len != debt.namespace.len + 16 + 32 + 8 + 32) return error.GroupedSourceCorrupt;
                        const offset = debt.namespace.len + 16 + 32;
                        if (std.mem.readInt(u64, owned.key[offset..][0..8], .big) != symbolic) continue;
                        _ = try queue.decode(owned.key, owned.value);
                        try result.addKey(batch, index, .key, offset);
                        try result.add(batch, .{ .target = .value, .index = index, .kind = .debt_pointer, .capacity = owned.value.len });
                    } else if (transfer.isKey(owned.key)) {
                        var arena = std.heap.ArenaAllocator.init(a);
                        defer arena.deinit();
                        const info = try transfer.info(arena.allocator(), owned.key);
                        if (info.kind != groups.outbox_kind) continue;
                        const event = try groups.readInvalidation(arena.allocator(), owned.key, owned.value);
                        if (event.through_sequence != symbolic) continue;
                        const previous = read.get(owned.key) catch |cause| if (cause == error.NotFound) null else return cause;
                        if (previous) |raw| {
                            const old = try groups.readInvalidation(arena.allocator(), owned.key, raw);
                            if (old.through_sequence >= next) return error.PreparedGenerationChanged;
                        }
                        var blob: Blob = .{ .target = .value, .index = index, .kind = .event, .capacity = owned.value.len };
                        blob.fields[0] = try field(owned.value, "since_sequence");
                        blob.fields[1] = try field(owned.value, "through_sequence");
                        if (blob.fields[1] == null) return error.InvalidDerivedSequence;
                        if (blob.fields[0]) |first| if (first.offset >= blob.fields[1].?.offset) return error.InvalidDerivedSequence;
                        try result.add(batch, blob);
                    }
                },
            }
        }
        for (input_deletes, 0..) |key, index| {
            try check(original);
            const origin = sources.deletes.get(.{ .ptr = @intFromPtr(key.ptr), .len = key.len }) orelse continue;
            if (origin == .group and std.mem.startsWith(u8, key, debt.namespace)) try result.addKey(batch, index, .deletion, debt.namespace.len + 16 + 32);
        }
        if (batch.replay != null) try result.addJournal(batch, 0, .replay, original);
        try result.validate(batch, next, original);
        return result;
    }
    fn field(raw: []const u8, comptime name: []const u8) !?Number {
        // GSI1's typed Invalidation encoder emits unspaced JSON field names.
        // Quotes in string values are escaped, and no nested schema object has
        // these fields. Typed decode above rejects duplicate/unknown members.
        const pattern = "\"" ++ name ++ "\":";
        const payload = raw[4 .. raw.len - 32];
        const at = std.mem.indexOf(u8, payload, pattern) orelse return error.GroupedSourceCorrupt;
        const start = at + pattern.len;
        var end = start;
        while (end < payload.len and std.ascii.isDigit(payload[end])) : (end += 1) {}
        if (end == start or end == payload.len or (payload[end] != ',' and payload[end] != '}')) return error.GroupedSourceCorrupt;
        const value = std.fmt.parseInt(u64, payload[start..end], 10) catch return error.GroupedSourceCorrupt;
        return if (value == symbolic) .{ .offset = 4 + start, .len = end - start } else null;
    }
    fn add(self: *Bindings, batch: *const mutations.OwnedBatch, value: Blob) !void {
        var blob = value;
        blob.digest = hash(blob.bytes(batch));
        try self.blobs.append(self.a, blob);
    }
    fn addKey(self: *Bindings, batch: *const mutations.OwnedBatch, index: usize, target: Target, offset: usize) !void {
        const raw = if (target == .key) batch.writes[index].key else batch.deletes[index];
        if (offset > raw.len or raw.len - offset < 8) return error.InvalidDerivedSequence;
        if (std.mem.readInt(u64, raw[offset..][0..8], .big) == symbolic) try self.add(batch, .{ .target = target, .index = index, .kind = .big, .offset = offset, .capacity = raw.len });
    }
    fn addJournal(self: *Bindings, batch: *const mutations.OwnedBatch, index: usize, target: Target, original: ?admission.Port) !void {
        const raw = if (target == .replay) batch.replay.?.payload else batch.writes[index].value;
        _ = try @import("derived/change_journal.zig").inspectBinaryRecord(raw, original);
        if (std.mem.readInt(u64, raw[6..14], .little) != symbolic) return error.InvalidDerivedSequence;
        try self.add(batch, .{ .target = target, .index = index, .kind = .journal, .offset = 6, .capacity = raw.len });
    }
    /// All fallible admission, shape and ownership checks precede mutation.
    /// No sequence is reserved here and no source data is published.
    pub fn validate(self: *const Bindings, batch: *const mutations.OwnedBatch, next: u64, original: ?admission.Port) !void {
        if (next == 0 or next >= symbolic or (self.current != symbolic and next < self.current)) return error.PreparedGenerationChanged;
        if (batch.replay) |replay| if (replay.sequence != self.current) return error.InvalidDerivedSequence;
        for (self.blobs.items) |blob| {
            try check(original);
            const raw = blob.bytes(batch);
            const begin = @intFromPtr(raw.ptr);
            const owner = @intFromPtr(batch.backing.ptr);
            if (begin < owner or begin - owner > batch.backing.len or blob.capacity > batch.backing.len - (begin - owner) or raw.len > blob.capacity) return error.InvalidPreparedEffects;
            var digest = std.crypto.hash.sha2.Sha256.init(.{});
            var offset: usize = 0;
            while (offset < raw.len) {
                try check(original);
                const end = offset + @min(raw.len - offset, 4096);
                digest.update(raw[offset..end]);
                offset = end;
            }
            if (!std.mem.eql(u8, &blob.digest, &digest.finalResult())) return error.PreparedEffectsMismatch;
        }
        try check(original);
    }
    pub fn bindValidated(self: *Bindings, batch: *mutations.OwnedBatch, next: u64) void {
        // Keys must move before recomputing their key-bound value checksums.
        for (self.blobs.items) |*blob| if (blob.target == .key or blob.target == .deletion) self.bindBlob(blob, batch, next);
        for (self.blobs.items) |*blob| if (blob.target != .key and blob.target != .deletion) self.bindBlob(blob, batch, next);
        if (batch.replay) |*replay| replay.sequence = next;
        self.current = next;
    }
    fn bindBlob(self: *Bindings, blob: *Blob, batch: *mutations.OwnedBatch, next: u64) void {
        var len = blob.bytes(batch).len;
        const raw = blob.mutable(batch);
        switch (blob.kind) {
            .big => std.mem.writeInt(u64, raw[blob.offset..][0..8], next, .big),
            .little, .journal => std.mem.writeInt(u64, raw[blob.offset..][0..8], next, .little),
            .identity => {
                var replace: ReplaceGenerations = .{ .raw = raw[0..len], .from = self.current, .to = next };
                _ = identity.visitReplayGenerations(batch.writes[blob.index].key, raw[0..len], &replace) catch unreachable;
            },
            .event => {
                var buffer: [20]u8 = undefined;
                const digits = std.fmt.bufPrint(&buffer, "{d}", .{next}) catch unreachable;
                var index: usize = blob.fields.len;
                while (index != 0) {
                    index -= 1;
                    const ref = blob.fields[index] orelse continue;
                    const tail = ref.offset + ref.len;
                    const new_tail = ref.offset + digits.len;
                    const end = len - ref.len + digits.len;
                    if (new_tail > tail) std.mem.copyBackwards(u8, raw[new_tail..end], raw[tail..len]) else std.mem.copyForwards(u8, raw[new_tail..end], raw[tail..len]);
                    @memcpy(raw[ref.offset..new_tail], digits);
                    len = end;
                }
                var shift: isize = 0;
                for (&blob.fields) |*maybe| if (maybe.*) |*ref| {
                    const old_len = ref.len;
                    ref.offset = @intCast(@as(isize, @intCast(ref.offset)) + shift);
                    ref.len = digits.len;
                    shift += @as(isize, @intCast(digits.len)) - @as(isize, @intCast(old_len));
                };
                blob.setLength(batch, len);
            },
            .debt_pointer => {},
        }
        if (blob.kind == .event or blob.kind == .debt_pointer) {
            const key = batch.writes[blob.index].key;
            @memcpy(raw[len - 32 .. len], &queue.checksum(key, raw[4 .. len - 32]));
        }
        blob.digest = hash(raw[0..len]);
    }
};
