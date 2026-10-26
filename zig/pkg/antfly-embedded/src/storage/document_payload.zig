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

//! Row-owned immutable JSON values in the same MVCC keyspace as their manifest.
//! Atomic KV publication and deletion retain the engine's snapshot/backup rules;
//! a digest is an internal location, never a read grant.
const std = @import("std");
const projection = @import("document_projection.zig");
const logical = @import("document_commitment.zig");
pub const ownership = @import("document_value_ownership.zig");
test {
    _ = logical;
    _ = ownership;
}
const keys = @import("internal_keys.zig");
const A = std.mem.Allocator;
pub const magic = "\x00AFDOC02";
pub const prefix = ownership.value_prefix;
pub const BlobKey = ownership.ValueKey;
pub const minimum_value_bytes = 64 * 1024;
const max_parts = projection.maximum_fields * 2 + 1;
const header_bytes = magic.len + 8 + 32 + 2 + 4;
/// Owner-local absence proof, following the existing retention-catalog hint.
/// 0 unknown; 1 this owner has observed only a pre-payload schema; 2 values may
/// exist. Publication marks 2 before commit; aborts only cause extra future work.
/// Explicit format writes/import replacement invalidate or conservatively widen
/// the hint. A positive hint never authorizes a read or replaces a row lookup.
pub fn valuesPossible(reader: anytype, cache: ?*std.atomic.Value(u8)) !bool {
    if (cache) |hint| switch (hint.load(.acquire)) {
        1 => return false,
        2 => return true,
        else => {},
    };
    const schema = @import("schema.zig");
    const raw = rawGet(reader, schema.schema_key) catch |err| {
        if (err == error.NotFound) return true;
        return err;
    };
    const absent = raw.len >= 8 and std.mem.eql(u8, raw[0..4], "ASCH") and std.mem.readInt(u32, raw[4..8], .little) < 26;
    if (cache) |hint| {
        if (!absent) hint.store(2, .release) else _ = hint.cmpxchgStrong(0, 1, .acq_rel, .acquire);
        return hint.load(.acquire) != 1;
    }
    return !absent;
}
pub fn observeFormatWrite(cache: ?*std.atomic.Value(u8), key: []const u8, value: []const u8) void {
    const hint = cache orelse return;
    if (!std.mem.eql(u8, key, @import("schema.zig").schema_key)) return;
    if (value.len >= 8 and std.mem.eql(u8, value[0..4], "ASCH") and std.mem.readInt(u32, value[4..8], .little) >= 26) hint.store(2, .release) else _ = hint.cmpxchgStrong(1, 0, .acq_rel, .acquire);
}
pub fn shouldStage(reader: anytype, cache: ?*std.atomic.Value(u8), publication: anytype, bytes: usize) !bool {
    if (bytes >= minimum_value_bytes and publication.directory != null and !std.mem.startsWith(u8, publication.directory.?, "DPX1")) return true;
    return valuesPossible(reader, cache);
}
pub const Admission = enum { unchecked, inline_only, current };
pub const Stats = struct { manifest_bytes: usize = 0, blob_bytes_written: usize = 0, blob_bytes_reused: usize = 0, blobs_retired: usize = 0, current_values_removed: usize = 0 };
pub fn isManifest(value: []const u8) bool {
    return std.mem.startsWith(u8, value, magic);
}
pub const blobKey = ownership.valueKey;
pub fn rawGet(reader: anytype, key: []const u8) ![]const u8 {
    const T = switch (@typeInfo(@TypeOf(reader))) {
        .pointer => |pointer| pointer.child,
        else => @TypeOf(reader),
    };
    if (comptime @hasDecl(T, "getPhysical")) return reader.getPhysical(key);
    if (comptime @hasDecl(T, "get")) return reader.get(key);
    var out: [1]?[]const u8 = .{null};
    try reader.getManySorted(&.{key}, &out);
    return out[0] orelse error.NotFound;
}
pub const Part = union(enum) {
    inline_json: []const u8,
    blob: struct { length: usize, digest: [32]u8 },
    pub fn length(self: @This()) usize {
        return switch (self) {
            .inline_json => |v| v.len,
            .blob => |v| v.length,
        };
    }
};
pub const Manifest = struct {
    logical_bytes: usize,
    logical_digest: [32]u8,
    physical_commitment: [32]u8,
    logical_record: []const u8,
    parts: []Part,
    rooted: bool = false,
    pub fn decode(a: A, raw: []const u8) !Manifest {
        if (!isManifest(raw) or raw.len < header_bytes + 32) return error.InvalidDocumentManifest;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], &digest, .{});
        if (!std.mem.eql(u8, &digest, raw[raw.len - 32 ..])) return error.InvalidDocumentManifest;
        var cursor: usize = magic.len;
        const logical_bytes = std.math.cast(usize, std.mem.readInt(u64, raw[cursor..][0..8], .little)) orelse return error.InvalidDocumentManifest;
        cursor += 8;
        const logical_digest = raw[cursor..][0..32].*;
        cursor += 32;
        const physical_commitment = digest;
        const count = std.mem.readInt(u16, raw[cursor..][0..2], .little);
        cursor += 2;
        const logical_length = std.mem.readInt(u32, raw[cursor..][0..4], .little);
        cursor += 4;
        if (logical_length > 64 * 1024 or logical_length > raw.len - 32 -| cursor) return error.InvalidDocumentManifest;
        const logical_record = raw[cursor..][0..logical_length];
        cursor += logical_length;
        if (logical_record.len != 0) {
            if (try logical.openTree(a, logical_record, logical_digest)) |tree| {
                defer tree.deinit(a);
                if (std.mem.readInt(u64, tree.raw[4..12], .little) != logical_bytes) return error.InvalidDocumentManifest;
            } else {
                const checked = try logical.decode(a, logical_record, logical_digest);
                checked.deinit(a);
            }
        }
        if (count == 0 or count > max_parts) return error.InvalidDocumentManifest;
        const parts = try a.alloc(Part, count);
        errdefer a.free(parts);
        var length: usize = 0;
        for (parts) |*part| {
            if (raw.len - 32 -| cursor < 9) return error.InvalidDocumentManifest;
            const tag = raw[cursor];
            const size = std.math.cast(usize, std.mem.readInt(u64, raw[cursor + 1 ..][0..8], .little)) orelse return error.InvalidDocumentManifest;
            cursor += 9;
            length = std.math.add(usize, length, size) catch return error.InvalidDocumentManifest;
            switch (tag) {
                0 => {
                    if (size > raw.len - 32 -| cursor) return error.InvalidDocumentManifest;
                    part.* = .{ .inline_json = raw[cursor..][0..size] };
                    cursor += size;
                },
                1 => {
                    if (size < minimum_value_bytes or raw.len - 32 -| cursor < 32) return error.InvalidDocumentManifest;
                    part.* = .{ .blob = .{ .length = size, .digest = raw[cursor..][0..32].* } };
                    cursor += 32;
                },
                else => return error.InvalidDocumentManifest,
            }
        }
        if (cursor != raw.len - 32 or length != logical_bytes) return error.InvalidDocumentManifest;
        if (logical_record.len == 0 and !std.mem.eql(u8, &logical_digest, &partsDigest(logical_bytes, parts))) return error.InvalidDocumentManifest;
        return .{ .logical_bytes = logical_bytes, .logical_digest = logical_digest, .physical_commitment = physical_commitment, .logical_record = logical_record, .parts = parts, .rooted = true };
    }
    pub const Navigation = struct {
        // Borrows this manifest's immutable logical-record owner.
        directory: []const u8,
        certificate: []u8,
        pub fn deinit(self: Navigation, a: A) void {
            a.free(self.certificate);
        }
    };
    /// Adapt the immutable unified tree to legacy scalar-cache consumers.
    /// No mutable directory lookup or immutable body read is required.
    pub fn navigation(self: Manifest, a: A, kind: u8) !?Navigation {
        const tree = (try logical.openTree(a, self.logical_record, self.logical_digest)) orelse return null;
        defer tree.deinit(a);
        if (std.mem.readInt(u64, tree.raw[4..12], .little) != self.logical_bytes or tree.raw[44] != kind) return error.InvalidDocumentManifest;
        const identities = @import("source_value_identity.zig");
        var records: [projection.maximum_fields]identities.Record = undefined;
        var count: usize = 0;
        for (tree.directory.fields) |field| if (field.kind == .leaf) {
            records[count] = .{ .index = field.index, .digest = tree.canonical[field.index] };
            count += 1;
        };
        return .{ .directory = tree.raw, .certificate = try identities.encodeRecords(a, self.logical_bytes, kind, self.logical_digest, tree.raw, records[0..count]) };
    }
    pub fn contains(self: Manifest, digest: [32]u8, length: usize) bool {
        for (self.parts) |part| if (part == .blob and part.blob.length == length and std.mem.eql(u8, &part.blob.digest, &digest)) return true;
        return false;
    }
    /// Use a disposable physical scope from the same pinned parent view. Its
    /// decoder scratch is reclaimed independently of the returned JSON buffer.
    /// Unsupported adapters retain the ordinary transaction-owned read path.
    pub fn readOwned(self: Manifest, a: A, reader: anytype, physical: []const u8) ![]u8 {
        const T = switch (@typeInfo(@TypeOf(reader))) {
            .pointer => |p| p.child,
            else => @TypeOf(reader),
        };
        if (comptime @hasDecl(T, "openReadScope")) {
            var scope = reader.openReadScope(@import("antfly_platform").allocator.reclaimingScratch(a)) catch |err| {
                if (err == error.Unsupported) return self.readRange(a, reader, physical, 0, self.logical_bytes);
                return err;
            };
            if (scope.supportsDirectInto()) {
                const value = self.readRange(a, &scope, physical, 0, self.logical_bytes) catch |err| {
                    scope.close();
                    if (err == error.Unsupported) return self.readRange(a, reader, physical, 0, self.logical_bytes);
                    return err;
                };
                scope.close();
                return value;
            }
            scope.close();
        }
        return self.readRange(a, reader, physical, 0, self.logical_bytes);
    }

    pub fn readRange(self: Manifest, a: A, reader: anytype, physical: []const u8, offset: usize, length: usize) ![]u8 {
        const output = try a.alloc(u8, length);
        errdefer a.free(output);
        try self.readInto(reader, physical, offset, output);
        return output;
    }
    /// Fill the caller's final output buffer. Range-capable backends fetch only
    /// overlapping immutable bytes; neither an intermediate JSON body nor a
    /// whole-value copy is needed for a small logical read.
    pub fn readInto(self: Manifest, reader: anytype, physical: []const u8, offset: usize, output: []u8) anyerror!void {
        if (offset > self.logical_bytes or output.len > self.logical_bytes - offset) return error.EndOfStream;
        const T = switch (@typeInfo(@TypeOf(reader))) {
            .pointer => |p| p.child,
            else => @TypeOf(reader),
        };
        if (comptime @hasDecl(T, "getManySortedInto")) {
            // Whole immutable extents have known final sizes. Partial extents
            // retain range reads, so a small excerpt never expands a full blob.
            var whole = true;
            var position: usize = 0;
            for (self.parts) |part| {
                const end = position + part.length();
                if (part == .blob and offset < end and offset + output.len > position and
                    (offset > position or offset + output.len < end)) whole = false;
                position = end;
            }
            if (whole) return self.readWholeExtentsInto(reader, physical, offset, output);
        }
        if (comptime @hasDecl(T, "getRangesManyPhysical")) return self.readBatched(reader, physical, offset, output);
        var position: usize = 0;
        for (self.parts) |part| {
            const end = position + part.length();
            const start = @max(position, offset);
            const finish = @min(end, offset + output.len);
            if (start < finish) {
                const target = output[start - offset ..][0 .. finish - start];
                switch (part) {
                    .inline_json => |value| @memcpy(target, value[start - position ..][0..target.len]),
                    .blob => |blob| {
                        if (comptime @hasDecl(T, "getRange")) {
                            const bytes = reader.getRange(&blobKey(physical, blob.digest), @intCast(start - position), target.len) catch |err| {
                                if (err != error.Unsupported) return err;
                                const full = try rawGet(reader, &blobKey(physical, blob.digest));
                                if (full.len != blob.length) return error.InvalidDocumentManifest;
                                @memcpy(target, full[start - position ..][0..target.len]);
                                position = end;
                                continue;
                            };
                            if (bytes.len != target.len) return error.InvalidDocumentManifest;
                            @memcpy(target, bytes);
                        } else {
                            const bytes = try rawGet(reader, &blobKey(physical, blob.digest));
                            if (bytes.len != blob.length) return error.InvalidDocumentManifest;
                            @memcpy(target, bytes[start - position ..][0..target.len]);
                        }
                    },
                }
            }
            position = end;
            if (position >= offset + output.len) break;
        }
    }
    fn readWholeExtentsInto(self: Manifest, reader: anytype, physical: []const u8, offset: usize, output: []u8) anyerror!void {
        const Batch = struct {
            const Entry = struct { key: BlobKey, target: []u8 };
            entries: [64]Entry = undefined,
            count: usize = 0,
            fn flush(batch: *@This(), source: @TypeOf(reader)) !void {
                if (batch.count == 0) return;
                std.mem.sort(Entry, batch.entries[0..batch.count], {}, struct {
                    fn less(_: void, left: Entry, right: Entry) bool {
                        return std.mem.order(u8, &left.key, &right.key) == .lt;
                    }
                }.less);
                var names: [64][]const u8 = undefined;
                var buffers: [64][]u8 = undefined;
                var values: [64]?[]const u8 = undefined;
                for (batch.entries[0..batch.count], names[0..batch.count], buffers[0..batch.count]) |*entry, *name, *buffer| {
                    name.* = &entry.key;
                    buffer.* = entry.target;
                }
                @memset(values[0..batch.count], null);
                source.getManySortedInto(names[0..batch.count], buffers[0..batch.count], values[0..batch.count]) catch |err| {
                    if (err == error.BufferTooSmall) return error.InvalidDocumentManifest;
                    return err;
                };
                for (batch.entries[0..batch.count], values[0..batch.count]) |entry, value| {
                    const bytes = value orelse return error.NotFound;
                    if (bytes.len != entry.target.len) return error.InvalidDocumentManifest;
                    // Adapters may share duplicate-key results. Preserve the
                    // caller-buffer contract even on their compatibility path.
                    if (bytes.ptr != entry.target.ptr) @memcpy(entry.target, bytes);
                }
                batch.count = 0;
            }
        };
        var batch: Batch = .{};
        var position: usize = 0;
        for (self.parts) |part| {
            const end = position + part.length();
            const start = @max(position, offset);
            const finish = @min(end, offset + output.len);
            if (start < finish) switch (part) {
                .inline_json => |bytes| @memcpy(output[start - offset ..][0 .. finish - start], bytes[start - position ..][0 .. finish - start]),
                .blob => |blob| {
                    std.debug.assert(start == position and finish == end);
                    batch.entries[batch.count] = .{ .key = blobKey(physical, blob.digest), .target = output[start - offset ..][0 .. finish - start] };
                    batch.count += 1;
                    if (batch.count == batch.entries.len) try batch.flush(reader);
                },
            };
            position = end;
            if (position >= offset + output.len) break;
        }
        try batch.flush(reader);
    }

    fn readBatched(self: Manifest, reader: anytype, physical: []const u8, offset: usize, output: []u8) anyerror!void {
        const ranges = @import("value_ranges.zig");
        const Batch = struct {
            const Entry = struct { key: BlobKey, range: [1]ranges.Range, bytes: [1][]const u8 = undefined, output_offset: usize };
            entries: [64]Entry = undefined,
            count: usize = 0,
            fn flush(batch: *@This(), source: @TypeOf(reader), destination: []u8) !void {
                if (batch.count == 0) return;
                std.mem.sort(Entry, batch.entries[0..batch.count], {}, struct {
                    fn less(_: void, l: Entry, r: Entry) bool {
                        return std.mem.order(u8, &l.key, &r.key) == .lt;
                    }
                }.less);
                var requests: [64]ranges.Request = undefined;
                for (batch.entries[0..batch.count], requests[0..batch.count]) |*entry, *request| request.* = .{ .key = &entry.key, .ranges = &entry.range, .output = &entry.bytes };
                try source.getRangesManyPhysical(requests[0..batch.count]);
                for (batch.entries[0..batch.count]) |entry| {
                    if (entry.bytes[0].len != entry.range[0].length) return error.InvalidDocumentManifest;
                    @memcpy(destination[entry.output_offset..][0..entry.bytes[0].len], entry.bytes[0]);
                }
                batch.count = 0;
            }
        };
        var batch: Batch = .{};
        var position: usize = 0;
        for (self.parts) |part| {
            const end = position + part.length();
            const start = @max(position, offset);
            const finish = @min(end, offset + output.len);
            if (start < finish) switch (part) {
                .inline_json => |bytes| @memcpy(output[start - offset ..][0 .. finish - start], bytes[start - position ..][0 .. finish - start]),
                .blob => |blob| {
                    batch.entries[batch.count] = .{ .key = blobKey(physical, blob.digest), .range = .{.{ .offset = @intCast(start - position), .length = finish - start }}, .output_offset = start - offset };
                    batch.count += 1;
                    if (batch.count == batch.entries.len) try batch.flush(reader, output);
                },
            };
            position = end;
            if (position >= offset + output.len) break;
        }
        try batch.flush(reader, output);
    }
};
/// Disposable decoded metadata, sharded and byte bounded. Entries never contain
/// payload bytes or access grants. Every hit verifies the current manifest's
/// checksum before reusing an immutable plan. Leased entries cannot be evicted.
pub const ManifestCache = struct {
    const shard_count = 16;
    const slots_per_shard = 4;
    pub const maximum_bytes = 2 * 1024 * 1024;
    const Entry = struct {
        key: [32]u8,
        raw: []u8,
        plan: Manifest,
        bytes: usize,
        stamp: u64,
        references: std.atomic.Value(usize) = .init(1),
    };
    const Shard = struct {
        mutex: std.Io.Mutex = .init,
        entries: [slots_per_shard]?*Entry = @splat(null),
        bytes: usize = 0,
        clock: u64 = 0,
    };
    pub const Lease = struct {
        entry: *Entry,
        pub fn manifest(self: Lease) Manifest {
            return self.entry.plan;
        }
        pub fn release(self: Lease) void {
            std.debug.assert(self.entry.references.fetchSub(1, .release) > 1);
        }
    };
    allocator: A,
    shards: [shard_count]Shard = @splat(.{}),
    row_hints: [256]std.atomic.Value(u64) = @splat(.init(0)),
    fn rowTag(physical: []const u8) u64 {
        const hash = std.hash.Wyhash.hash(0x4146444f43, physical);
        return if (hash == 0) 1 else hash;
    }
    pub fn noteRow(self: *ManifestCache, physical: []const u8) void {
        const tag = rowTag(physical);
        self.row_hints[tag % self.row_hints.len].store(tag, .release);
    }
    pub fn knowsRow(self: *ManifestCache, physical: []const u8) bool {
        const tag = rowTag(physical);
        return self.row_hints[tag % self.row_hints.len].load(.acquire) == tag;
    }
    pub fn forgetRow(self: *ManifestCache, physical: []const u8) void {
        const tag = rowTag(physical);
        _ = self.row_hints[tag % self.row_hints.len].cmpxchgStrong(tag, 0, .acq_rel, .acquire);
    }
    pub fn init(a: A) ManifestCache {
        return .{ .allocator = @import("antfly_platform").allocator.reclaimingScratch(a) };
    }
    fn io() std.Io {
        return if (@import("builtin").os.tag == .freestanding) .failing else std.Io.Threaded.global_single_threaded.io();
    }
    fn destroy(self: *ManifestCache, entry: *Entry) void {
        std.debug.assert(entry.references.load(.acquire) == 1);
        self.allocator.free(entry.plan.parts);
        self.allocator.free(entry.raw);
        self.allocator.destroy(entry);
    }
    pub fn deinit(self: *ManifestCache) void {
        for (&self.shards) |*shard| for (&shard.entries) |*slot| if (slot.*) |entry| {
            self.destroy(entry);
            slot.* = null;
        };
    }
    pub fn retainedBytes(self: *ManifestCache) usize {
        var bytes: usize = 0;
        for (&self.shards) |*shard| {
            shard.mutex.lockUncancelable(io());
            bytes += shard.bytes;
            shard.mutex.unlock(io());
        }
        return bytes;
    }
    fn make(self: *ManifestCache, raw: []const u8, key: [32]u8) !*Entry {
        const owned = try self.allocator.dupe(u8, raw);
        errdefer self.allocator.free(owned);
        const plan = try Manifest.decode(self.allocator, owned);
        errdefer self.allocator.free(plan.parts);
        const entry = try self.allocator.create(Entry);
        entry.* = .{ .key = key, .raw = owned, .plan = plan, .bytes = owned.len + plan.parts.len * @sizeOf(Part) + @sizeOf(Entry), .stamp = 0 };
        return entry;
    }
    pub fn acquire(self: *ManifestCache, raw: []const u8) !?Lease {
        if (raw.len > 32 * 1024) return null;
        if (!isManifest(raw) or raw.len < header_bytes + 32) return error.InvalidDocumentManifest;
        var key: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], &key, .{});
        if (!std.mem.eql(u8, &key, raw[raw.len - 32 ..])) return error.InvalidDocumentManifest;
        const shard = &self.shards[key[0] % shard_count];
        shard.mutex.lockUncancelable(io());
        defer shard.mutex.unlock(io());
        shard.clock +%= 1;
        for (shard.entries) |slot| if (slot) |entry| if (std.mem.eql(u8, &entry.key, &key)) {
            entry.stamp = shard.clock;
            _ = entry.references.fetchAdd(1, .monotonic);
            return .{ .entry = entry };
        };
        const entry = self.make(raw, key) catch |err| return if (err == error.OutOfMemory) null else err;
        var installed = false;
        defer if (!installed) self.destroy(entry);
        if (entry.bytes > maximum_bytes / shard_count) return null;
        while (true) {
            const free = for (shard.entries, 0..) |slot, index| {
                if (slot == null) break index;
            } else null;
            if (free != null and entry.bytes <= maximum_bytes / shard_count - shard.bytes) {
                entry.stamp = shard.clock;
                shard.entries[free.?] = entry;
                shard.bytes += entry.bytes;
                _ = entry.references.fetchAdd(1, .monotonic);
                installed = true;
                return .{ .entry = entry };
            }
            var victim: ?usize = null;
            for (shard.entries, 0..) |slot, index| if (slot) |candidate| if (candidate.references.load(.acquire) == 1) {
                if (victim == null or shard.clock -% candidate.stamp > shard.clock -% shard.entries[victim.?].?.stamp) victim = index;
            };
            const index = victim orelse return null;
            shard.bytes -= shard.entries[index].?.bytes;
            self.destroy(shard.entries[index].?);
            shard.entries[index] = null;
        }
    }
};
pub const CachePort = struct {
    context: *anyopaque,
    get: *const fn (*anyopaque) ?*ManifestCache,
};
const PlanOwner = struct {
    a: A,
    value: Manifest,
    raw: ?[]u8 = null,
    lease: ?ManifestCache.Lease = null,
    fn deinit(self: PlanOwner) void {
        if (self.lease) |lease| lease.release() else {
            self.a.free(self.value.parts);
            self.a.free(self.raw.?);
        }
    }
};
fn openPlan(a: A, reader: anytype, physical: []const u8, raw: []const u8) !PlanOwner {
    const T = switch (@typeInfo(@TypeOf(reader))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(reader),
    };
    if (comptime @hasDecl(T, "documentManifestCache")) if (reader.documentManifestCache()) |cache| {
        cache.noteRow(physical);
        if (try cache.acquire(raw)) |lease| return .{ .a = a, .value = lease.manifest(), .lease = lease };
    };
    const owned = try a.dupe(u8, raw);
    errdefer a.free(owned);
    return .{ .a = a, .value = try Manifest.decode(a, owned), .raw = owned };
}

pub fn read(a: A, reader: anytype, physical: []const u8, raw: []const u8) ![]const u8 {
    if (!keys.isStoredDocumentRowKey(physical) or !isManifest(raw)) return raw;
    // Physical reads may retire prior buffers. Own the small manifest before
    // resolving any of its immutable values at this same snapshot.
    const plan = try openPlan(a, reader, physical, raw);
    defer plan.deinit();
    const manifest = plan.value;
    const result = try manifest.readOwned(a, reader, physical);
    errdefer a.free(result);
    return result;
}

/// A logical import/schema replacement must not downgrade the reader fence
/// while physical manifests may still exist. Snapshot restore replaces the
/// entire store and therefore uses the backend's physical restore boundary.
pub fn requireReaderFence(reader: anytype, key: []const u8, value: []const u8) !void {
    const schema = @import("schema.zig");
    if (!std.mem.eql(u8, key, schema.schema_key) or value.len < 8 or !std.mem.eql(u8, value[0..4], "ASCH")) return;
    const incoming = std.mem.readInt(u32, value[4..8], .little);
    const previous = rawGet(reader, key) catch |err| {
        if (err == error.NotFound) return;
        return err;
    };
    if (previous.len >= 8 and std.mem.eql(u8, previous[0..4], "ASCH")) {
        const minimum = std.mem.readInt(u32, previous[4..8], .little);
        if (minimum >= 26 and incoming < minimum) return error.UnsupportedVersion;
    }
}

/// Only for manifests just produced by stage()/prepareMetadataPatch().
pub fn publishedCommitment(raw: []const u8) [32]u8 {
    std.debug.assert(isManifest(raw) and raw.len >= header_bytes + 32);
    return raw[magic.len + 8 ..][0..32].*;
}

/// Offline/scrub path: validate immutable values against their committed digest.
/// Ordinary reads retain the original inline backend checksum policy instead.
pub fn readVerified(a: A, reader: anytype, physical: []const u8, raw: []const u8) ![]const u8 {
    if (!keys.isStoredDocumentRowKey(physical) or !isManifest(raw)) return raw;
    const plan = try openPlan(a, reader, physical, raw);
    defer plan.deinit();
    const manifest = plan.value;
    const result = try manifest.readRange(a, reader, physical, 0, manifest.logical_bytes);
    errdefer a.free(result);
    var offset: usize = 0;
    for (manifest.parts) |part| {
        if (part == .blob) {
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(result[offset..][0..part.length()], &digest, .{});
            if (!std.mem.eql(u8, &digest, &part.blob.digest)) return error.InvalidDocumentManifest;
        }
        offset += part.length();
    }
    if (manifest.logical_record.len != 0 and !std.mem.eql(u8, &try logical.compute(a, result), &manifest.logical_digest)) return error.InvalidDocumentManifest;
    return result;
}

/// Reader-version fencing is committed with the first migrated document.
/// Logical schema semantics are unchanged; ASCH 26 admits this hybrid layout.
pub fn admit(a: A, writer: anytype, admission: *Admission) !bool {
    return admitWithMinimum(a, writer, admission, 26);
}

/// Below the immutable-value threshold, keep a legacy table on its original
/// authority algorithm. Once a table is admitted, all eligible JSON identities
/// must use its canonical algorithm, including medium inline documents.
pub fn admitForWrite(a: A, writer: anytype, admission: *Admission, minimum: u32, bytes: usize, automatic: bool) !bool {
    if (automatic and bytes < minimum_value_bytes and admission.* == .unchecked) {
        const schema = @import("schema.zig");
        const raw = rawGet(writer, schema.schema_key) catch |err| {
            if (err == error.NotFound) return false;
            return err;
        };
        if (try schema.serializedReaderFormat(raw) < 26) return false;
    }
    return admitWithMinimum(a, writer, admission, minimum);
}

pub fn admitWithMinimum(a: A, writer: anytype, admission: *Admission, minimum: u32) !bool {
    if (minimum < 26 or minimum > @import("schema.zig").maximum_reader_format_version) return error.UnsupportedVersion;
    if (admission.* != .unchecked) return admission.* == .current;
    const schema = @import("schema.zig");
    const raw = rawGet(writer, schema.schema_key) catch |err| {
        if (err == error.NotFound) {
            admission.* = .inline_only;
            return false;
        }
        return err;
    };
    const value = try schema.deserializeSchema(a, raw);
    defer schema.freeSchema(a, value);
    if (value.storage_mode != .document) {
        admission.* = .inline_only;
        return false;
    }
    if (value.decoded_format_version < minimum) {
        const upgraded = try schema.serializeSchemaWithMinimumFormat(a, value, minimum);
        defer a.free(upgraded);
        try writer.put(schema.schema_key, upgraded);
    }
    admission.* = .current;
    return true;
}

/// Incoming bytes are a full replacement and have already been validated and
/// hashed by the common writer. Reuse never trusts caller revision strings.
/// First-time migration requires evidence of substantial unchanged large values;
/// write-once and continuously changing documents keep the inline read path.
pub fn stage(a: A, writer: anytype, physical: []const u8, body: []const u8, publication: anytype, admission: *Admission, stats: *Stats) !?[]u8 {
    if (!keys.isPrimaryDocumentKey(physical)) return null;
    if (publication.directory == null or std.mem.startsWith(u8, publication.directory.?, "DPX1")) {
        // Small/opaque/wide replacements stay inline. Retire by ownership keys
        // without loading an old large body or interpreting a negative directory.
        try retire(a, writer, physical, stats);
        return null;
    }
    var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
    defer arena.deinit();
    const temp = arena.allocator();
    // Common scalar replacements need no previous primary read: the writer's
    // qualified certificate reuse already proves whether large bytes survived.
    const known_inline = publication.prior_manifest != null and !publication.prior_manifest.? and !try ownership.hasCurrent(writer, physical);
    const reuse_threshold = @max(@as(usize, minimum_value_bytes), body.len / 2);
    const qualified_stable = known_inline and publication.reused_bytes >= reuse_threshold;
    // A qualified inline row with no substantial reuse keeps its existing fast
    // path; do not load/hash its previous large body merely to reject migration.
    if (known_inline and (!qualified_stable or publication.proof.layout_cooldown != 0)) return null;
    var previous_length: usize = 0;
    var previous_digest: [32]u8 = undefined;
    var previous: ?Manifest = null;
    if (!qualified_stable) {
        const raw_previous = rawGet(writer, physical) catch |err| {
            if (err == error.NotFound) return null;
            return err;
        };
        previous_length = raw_previous.len;
        previous = if (isManifest(raw_previous)) try Manifest.decode(temp, try temp.dupe(u8, raw_previous)) else null;
        if (previous == null) {
            if (publication.directory == null) return null;
            std.crypto.hash.sha2.Sha256.hash(raw_previous, &previous_digest, .{});
        }
    }
    const raw_directory = publication.directory orelse return retireAll(temp, writer, physical, previous, stats);
    const directory = (try projection.Directory.open(temp, raw_directory, publication.proof)) orelse return error.InvalidDocumentManifest;
    var values: std.ArrayList(projection.Field) = .empty;
    for (directory.fields) |field| if (field.kind != .object and field.length >= minimum_value_bytes) try values.append(temp, field);
    if (values.items.len == 0) return retireAll(temp, writer, physical, previous, stats);
    std.mem.sort(projection.Field, values.items, {}, struct {
        fn less(_: void, l: projection.Field, r: projection.Field) bool {
            return l.offset < r.offset;
        }
    }.less);
    var stable_bytes: u64 = if (qualified_stable) publication.reused_bytes else 0;
    if (qualified_stable) {} else if (previous) |old| {
        for (values.items) |field| if (old.contains(field.digest, @intCast(field.length))) {
            stable_bytes += field.length;
        };
    } else {
        const revisions = @import("document_mutation_revision.zig");
        const proof = (try revisions.loadContent(writer, physical)) orelse return null;
        if (proof.bytes != previous_length or !std.mem.eql(u8, &proof.digest, &previous_digest)) return null;
        const encoded = rawGet(writer, &projection.keyForRevision(revisions.keyForPhysical(physical).?)) catch |err| {
            if (err == error.NotFound) return null;
            return err;
        };
        const old = (try projection.Directory.open(temp, encoded, proof)) orelse return null;
        for (values.items) |field| for (old.fields) |prior| {
            if (prior.kind != .object and prior.length == field.length and std.mem.eql(u8, &prior.digest, &field.digest)) {
                stable_bytes += field.length;
                break;
            }
        };
    }
    const minimum_reuse = if (previous != null) minimum_value_bytes else @max(@as(usize, minimum_value_bytes), body.len / 2);
    if (stable_bytes < minimum_reuse) return retireAll(temp, writer, physical, previous, stats);
    // Ordinary inline writes do not look up or decode the table schema merely
    // to reject an inapplicable layout. Fence only an actual manifest write.
    if (publication.logical_record == null) return retireAll(temp, writer, physical, previous, stats);
    if (!try admit(temp, writer, admission)) return retireAll(temp, writer, physical, previous, stats);
    var parts: std.ArrayList(Part) = .empty;
    var cursor: usize = 0;
    var next_values: std.ArrayList(ownership.Value) = .empty;
    for (values.items) |field| {
        const offset: usize = @intCast(field.offset);
        const length: usize = @intCast(field.length);
        if (offset < cursor or offset > body.len or length > body.len - offset) return error.InvalidDocumentManifest;
        try parts.append(temp, .{ .inline_json = body[cursor..offset] });
        try parts.append(temp, .{ .blob = .{ .length = length, .digest = field.digest } });
        try next_values.append(temp, .{ .digest = field.digest, .length = length, .bytes = body[offset..][0..length] });
        cursor = offset + length;
    }
    try parts.append(temp, .{ .inline_json = body[cursor..] });
    // Estimate the exact manifest plus row-membership bookkeeping before
    // admission. Entry needs a 16:1 byte payoff; an existing layout needs 8:1
    // to stay, avoiding flips when small inline metadata changes its size.
    var descriptor_bytes = header_bytes + publication.logical_record.?.len + 32 + 128 + next_values.items.len * 40;
    for (parts.items) |part| descriptor_bytes = try std.math.add(usize, descriptor_bytes, 9 + switch (part) {
        .inline_json => |v| v.len,
        .blob => 32,
    });
    const payoff: u64 = if (previous != null) 8 else 16;
    if (stable_bytes / descriptor_bytes < payoff) return retireAll(temp, writer, physical, previous, stats);
    const encoded = try encodeLogical(a, parts.items, publication.proof.digest, publication.logical_record.?);
    errdefer a.free(encoded);
    const delta = try ownership.publishCurrent(temp, writer, physical, next_values.items);
    stats.blob_bytes_written += delta.written;
    stats.blob_bytes_reused += delta.reused;
    stats.blobs_retired += delta.retired;
    stats.current_values_removed += delta.removed_current;
    stats.manifest_bytes += encoded.len;
    return encoded;
}
fn retireAll(a: A, writer: anytype, physical: []const u8, previous: ?Manifest, stats: *Stats) !?[]u8 {
    _ = previous;
    const delta = try ownership.publishCurrent(a, writer, physical, &.{});
    stats.blobs_retired += delta.retired;
    stats.current_values_removed += delta.removed_current;
    return null;
}

/// The directory's selected field digests validate these reconstructed extents
/// once, just as for ordinary primary ranges. Omitted blobs are never loaded.
pub fn readSelected(a: A, reader: anytype, physical: []const u8, proof: anytype, fields: []const projection.Field, output: [][]const u8) !void {
    const plan = try openPlan(a, reader, physical, try rawGet(reader, physical));
    defer plan.deinit();
    const manifest = plan.value;
    if (manifest.logical_bytes != proof.bytes or !std.mem.eql(u8, &manifest.logical_digest, &proof.digest) or fields.len != output.len) return error.InvalidDocumentContentAuthority;
    for (fields, output) |field, *value| {
        const bytes = try manifest.readRange(a, reader, physical, @intCast(field.offset), @intCast(field.length));
        errdefer a.free(bytes);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        if (!std.mem.eql(u8, &digest, &field.digest)) return error.InvalidDocumentContentAuthority;
        value.* = bytes;
    }
}
pub fn retire(a: A, writer: anytype, physical: []const u8, stats: *Stats) !void {
    if (!keys.isPrimaryDocumentKey(physical)) return;
    // The compact current-membership record avoids fetching the old document
    // or payloads, and dropping a row cannot collect values held by history.
    const delta = try ownership.publishCurrent(a, writer, physical, &.{});
    stats.blobs_retired += delta.retired;
    stats.current_values_removed += delta.removed_current;
}

pub fn immutableValues(a: A, manifest: Manifest) ![]ownership.Value {
    var values: std.ArrayList(ownership.Value) = .empty;
    errdefer values.deinit(a);
    for (manifest.parts) |part| if (part == .blob) try values.append(a, .{ .digest = part.blob.digest, .length = part.blob.length });
    return values.toOwnedSlice(a);
}

/// Domain-separated commitment to logical order, lengths and immutable values.
/// Unlike SHA256(concatenated JSON), this can be updated without reading blobs.
pub fn partsDigest(bytes: usize, parts: []const Part) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.document.parts/2\x00");
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, bytes, .little);
    hash.update(&length);
    for (parts) |part| {
        hash.update(&.{if (part == .blob) @as(u8, 1) else @as(u8, 0)});
        std.mem.writeInt(u64, &length, part.length(), .little);
        hash.update(&length);
        switch (part) {
            .blob => |blob| hash.update(&blob.digest),
            .inline_json => |value| {
                var digest: [32]u8 = undefined;
                std.crypto.hash.sha2.Sha256.hash(value, &digest, .{});
                hash.update(&digest);
            },
        }
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

pub fn encodeRooted(a: A, parts: []const Part) ![]u8 {
    var bytes: usize = 0;
    for (parts) |part| bytes = try std.math.add(usize, bytes, part.length());
    return encodeLogical(a, parts, partsDigest(bytes, parts), &.{});
}
pub fn encodeLogical(a: A, parts: []const Part, logical_digest: logical.Digest, logical_record: []const u8) ![]u8 {
    return encodeRootedAllocating(a, parts, logical_digest, logical_record) catch |err| switch (err) {
        error.WriteFailed => error.OutOfMemory,
        else => err,
    };
}
fn encodeRootedAllocating(a: A, parts: []const Part, logical_digest: logical.Digest, logical_record: []const u8) ![]u8 {
    if (logical_record.len > 64 * 1024) return error.InvalidDocumentCommitment;
    var tree_bytes: ?u64 = null;
    if (logical_record.len != 0) {
        if (try logical.openTree(a, logical_record, logical_digest)) |tree| {
            defer tree.deinit(a);
            tree_bytes = std.mem.readInt(u64, tree.raw[4..12], .little);
        } else {
            const checked = try logical.decode(a, logical_record, logical_digest);
            checked.deinit(a);
        }
    }
    if (parts.len == 0 or parts.len > max_parts) return error.InvalidDocumentManifest;
    var bytes: usize = 0;
    for (parts) |part| {
        if (part == .blob and part.blob.length < minimum_value_bytes) return error.InvalidDocumentManifest;
        bytes = try std.math.add(usize, bytes, part.length());
    }
    if (tree_bytes) |expected_bytes| if (expected_bytes != bytes) return error.InvalidDocumentManifest;
    var out: std.Io.Writer.Allocating = .init(a);
    defer out.deinit();
    try out.writer.writeAll(magic);
    try out.writer.writeInt(u64, bytes, .little);
    try out.writer.writeAll(&logical_digest);
    try out.writer.writeInt(u16, @intCast(parts.len), .little);
    try out.writer.writeInt(u32, @intCast(logical_record.len), .little);
    try out.writer.writeAll(logical_record);
    for (parts) |part| {
        try out.writer.writeByte(if (part == .blob) 1 else 0);
        try out.writer.writeInt(u64, part.length(), .little);
        switch (part) {
            .inline_json => |value| try out.writer.writeAll(value),
            .blob => |blob| try out.writer.writeAll(&blob.digest),
        }
    }
    var checksum: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(out.written(), &checksum, .{});
    try out.writer.writeAll(&checksum);
    return out.toOwnedSlice();
}

/// A storage-level top-level field replacement. A null value removes the field;
/// the JSON text "null" stores JSON null. Large payload changes use normal put.
pub const MetadataUpdate = struct { name: []const u8, value_json: ?[]const u8 };
pub const MetadataPatch = struct {
    manifest: []u8,
    directory: []u8,
    identities: []u8,
    logical_record: []u8,
    proof: @import("document_mutation_revision.zig").Content,
    reused_payload_bytes: usize,
    pub fn deinit(self: @This(), a: A) void {
        a.free(self.manifest);
        a.free(self.directory);
        a.free(self.identities);
        a.free(self.logical_record);
    }
};

/// An internal optimization receipt. The ordinary complete logical value stays
/// available for validation, indexing, replay and compatibility fallbacks. Only
/// the common writer may consume the prepared physical publication after checking
/// the current root and exact logical input. Never accepted from public JSON.
pub const PreparedWrite = struct {
    before: @import("document_mutation_revision.zig").Content,
    before_manifest_digest: [32]u8,
    patch: MetadataPatch,
    expected_json: []const u8,
    pub fn deinit(self: PreparedWrite, a: A) void {
        self.patch.deinit(a);
    }
    pub fn sameInput(self: PreparedWrite, value: []const u8) bool {
        return value.len == self.expected_json.len and (value.ptr == self.expected_json.ptr or std.mem.eql(u8, value, self.expected_json));
    }
    pub fn qualifies(self: PreparedWrite, reader: anytype, key: []const u8, value: []const u8) !bool {
        if (!self.sameInput(value)) return false;
        // Logical roots ignore layout and representation. Bind to the exact
        // current manifest as well: only it owns these row-local raw values.
        const raw = rawGet(reader, key) catch |err| return if (err == error.NotFound) error.PreparedReadSetChanged else err;
        if (!isManifest(raw)) {
            // Inline demotion can preserve the same logical document while
            // retiring its old raw references. Only a current matching source
            // authority can authorize the complete-value fallback.
            const revisions = @import("document_mutation_revision.zig");
            const current = (try revisions.loadContent(reader, key)) orelse return error.PreparedReadSetChanged;
            if (current.algorithm == .logical_json_v1 and std.mem.eql(u8, &current.digest, &self.before.digest)) return false;
            return error.PreparedReadSetChanged;
        }
        const T = switch (@typeInfo(@TypeOf(reader))) {
            .pointer => |ptr| ptr.child,
            else => @TypeOf(reader),
        };
        const a = if (comptime @hasField(T, "alloc")) @import("antfly_platform").allocator.reclaimingScratch(reader.alloc) else std.heap.page_allocator;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const manifest = try Manifest.decode(scratch.allocator(), raw);
        if (!std.mem.eql(u8, &manifest.logical_digest, &self.before.digest)) return error.PreparedReadSetChanged;
        // A representation-only rewrite can use the ordinary writer. A changed
        // source cannot: expected_json was computed from the previous root.
        return manifest.logical_bytes == self.before.bytes and std.mem.eql(u8, &manifest.physical_commitment, &self.before_manifest_digest);
    }
};
pub const SourceMetadata = struct {
    manifest: []u8,
    directory: []u8,
    certificate: []u8,
    proof: @import("document_mutation_revision.zig").Content,
    revision_key: ?@import("document_mutation_revision.zig").Key = null,
    pub fn deinit(self: SourceMetadata, a: A) void {
        a.free(self.manifest);
        a.free(self.directory);
        a.free(self.certificate);
    }
    pub fn capture(a: A, reader: anytype, key: []const u8) !?SourceMetadata {
        const raw = rawGet(reader, key) catch |err| return if (err == error.NotFound) null else err;
        return capturePhysical(a, reader, key, raw);
    }
    /// Reuse an already fetched physical value from this same pinned reader.
    /// The caller must not mix a manifest from another MVCC cut with this reader.
    pub fn capturePhysical(a: A, reader: anytype, key: []const u8, raw: []const u8) !?SourceMetadata {
        if (!isManifest(raw)) return null;
        const manifest = try a.dupe(u8, raw);
        var directory: ?[]u8 = null;
        var certificate: ?[]u8 = null;
        var keep = false;
        defer if (!keep) {
            a.free(manifest);
            if (directory) |v| a.free(v);
            if (certificate) |v| a.free(v);
        };
        const revisions = @import("document_mutation_revision.zig");
        const cached = try revisions.loadContent(reader, key);
        const parsed = try Manifest.decode(a, manifest);
        defer a.free(parsed.parts);
        const revision_key = revisions.keyForPhysical(key).?;
        const proof: revisions.Content = .{ .revision = if (cached) |value| value.revision else try revisions.load(reader, revision_key), .bytes = parsed.logical_bytes, .kind = key[key.len - 1], .digest = parsed.logical_digest, .algorithm = .logical_json_v1, .manifest = true };
        if (try parsed.navigation(a, proof.kind)) |navigation| {
            defer navigation.deinit(a);
            directory = try a.dupe(u8, navigation.directory);
            certificate = try a.dupe(u8, navigation.certificate);
            keep = true;
            return .{ .manifest = manifest, .directory = directory.?, .certificate = certificate.?, .proof = proof, .revision_key = revision_key };
        }
        const paths = rawGet(reader, &projection.keyForRevision(revision_key)) catch |err| return if (err == error.NotFound) null else err;
        directory = try a.dupe(u8, paths);
        const canonical = rawGet(reader, &@import("source_value_identity.zig").keyForRevision(revision_key)) catch |err| return if (err == error.NotFound) null else err;
        certificate = try a.dupe(u8, canonical);
        keep = true;
        return .{ .manifest = manifest, .directory = directory.?, .certificate = certificate.?, .proof = proof, .revision_key = revision_key };
    }
    pub fn prepare(self: SourceMetadata, a: A, before_json: []const u8, after_json: []const u8, updates: []const MetadataUpdate, order: []const []const u8) !?PreparedWrite {
        const patch = prepareMetadataPatchOrdered(a, self.manifest, self.proof, self.directory, self.certificate, updates, order) catch |err| switch (err) {
            error.Unsupported => return null,
            else => return err,
        };
        var keep = false;
        defer if (!keep) patch.deinit(a);
        var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
        defer arena.deinit();
        const old = try Manifest.decode(arena.allocator(), self.manifest);
        const next = try Manifest.decode(arena.allocator(), patch.manifest);
        if (old.logical_bytes != before_json.len or next.logical_bytes != after_json.len) return null;
        var offset: usize = 0;
        for (next.parts) |part| {
            const expected = after_json[offset..][0..part.length()];
            switch (part) {
                .inline_json => |bytes| if (!std.mem.eql(u8, expected, bytes)) return null,
                .blob => |blob| {
                    var prior_offset: usize = 0;
                    const previous = for (old.parts) |prior| {
                        if (prior == .blob and prior.blob.length == blob.length and std.mem.eql(u8, &prior.blob.digest, &blob.digest)) break before_json[prior_offset..][0..blob.length];
                        prior_offset += prior.length();
                    } else return null;
                    if (!std.mem.eql(u8, expected, previous)) return null;
                },
            }
            offset += part.length();
        }
        keep = true;
        return .{ .before = self.proof, .before_manifest_digest = old.physical_commitment, .patch = patch, .expected_json = after_json };
    }
};

/// Graft unchanged branches into the new layout without reading immutable
/// values. Paths stay structural, so literal dots never collide with children.
/// Width/metadata limits retain root-level opaque commitments as before.
fn graftProjection(a: A, previous: ?logical.FieldTree, roots: []const projection.Field, root_digests: []const logical.Digest, updates: []const MetadataUpdate, bytes: usize, kind: u8, digest: logical.Digest) !struct { directory: []u8, record: []u8 } {
    var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
    defer arena.deinit();
    const temp = arena.allocator();
    var fields: std.ArrayList(projection.Field) = .empty;
    var canonical: std.ArrayList(logical.Digest) = .empty;
    var sources: std.ArrayList(?u16) = .empty;
    for (roots, root_digests) |root, identity| {
        var field = root;
        var source: ?u16 = null;
        if (previous) |tree| {
            const changed = for (updates) |update| {
                if (std.mem.eql(u8, update.name, root.name)) break true;
            } else false;
            if (!changed) if (tree.directory.find(root.name)) |old| {
                if (old.length == root.length and std.mem.eql(u8, &old.digest, &root.digest) and std.mem.eql(u8, &tree.canonical[old.index], &identity)) {
                    field.kind = old.kind;
                    source = old.index;
                }
            };
        }
        try fields.append(temp, field);
        try canonical.append(temp, identity);
        try sources.append(temp, source);
    }
    var cursor: usize = 0;
    var size: usize = 47;
    for (roots) |root| size += 50 + root.name.len;
    if (previous) |tree| while (cursor < fields.items.len) : (cursor += 1) {
        const old = tree.directory.fields[sources.items[cursor] orelse continue];
        if (old.kind != .object) continue;
        var extra: usize = 0;
        for (tree.directory.fields[old.first_child..][0..old.child_count]) |child| extra += 50 + child.name.len;
        if (fields.items.len + old.child_count > projection.maximum_fields or size + extra > projection.maximum_bytes) {
            fields.items[cursor].kind = .subtree;
            continue;
        }
        size += extra;
        const parent_offset = fields.items[cursor].offset;
        for (tree.directory.fields[old.first_child..][0..old.child_count]) |child| {
            var next = child;
            next.index = @intCast(fields.items.len);
            next.parent = @intCast(cursor);
            next.offset = parent_offset + child.offset - old.offset;
            next.first_child = 0;
            next.child_count = 0;
            try fields.append(temp, next);
            try canonical.append(temp, tree.canonical[child.index]);
            try sources.append(temp, child.index);
        }
    };
    const directory = try projection.encodeFields(a, bytes, kind, digest, fields.items);
    errdefer a.free(directory);
    const record = try logical.encodeTree(a, digest, directory, canonical.items);
    return .{ .directory = directory, .record = record };
}

/// No reader/writer capability is accepted: unchanged immutable values cannot
/// accidentally be fetched or rewritten. Unsupported shapes must fall back to
/// the ordinary logical transform, before any transaction mutation is staged.
pub fn prepareMetadataPatch(a: A, raw: []const u8, proof: @import("document_mutation_revision.zig").Content, encoded_directory: []const u8, encoded_certificate: []const u8, updates: []const MetadataUpdate) !MetadataPatch {
    return prepareMetadataPatchAllocating(a, raw, proof, encoded_directory, encoded_certificate, updates, null) catch |err| switch (err) {
        error.WriteFailed => error.OutOfMemory,
        else => err,
    };
}
pub fn prepareMetadataPatchOrdered(a: A, raw: []const u8, proof: @import("document_mutation_revision.zig").Content, directory: []const u8, certificate: []const u8, updates: []const MetadataUpdate, order: []const []const u8) !MetadataPatch {
    return prepareMetadataPatchAllocating(a, raw, proof, directory, certificate, updates, order) catch |err| switch (err) {
        error.WriteFailed => error.OutOfMemory,
        else => err,
    };
}
fn prepareMetadataPatchAllocating(a: A, raw: []const u8, proof: @import("document_mutation_revision.zig").Content, encoded_directory: []const u8, encoded_certificate: []const u8, updates: []const MetadataUpdate, member_order: ?[]const []const u8) !MetadataPatch {
    const identities = @import("source_value_identity.zig");
    const inline_limit = 32 * 1024;
    if (!proof.manifest or proof.algorithm != .logical_json_v1 or updates.len == 0 or updates.len > projection.maximum_fields) return error.Unsupported;
    var request_bytes: usize = 0;
    for (updates) |update| {
        request_bytes = std.math.add(usize, request_bytes, update.name.len) catch return error.Unsupported;
        if (update.value_json) |value| request_bytes = std.math.add(usize, request_bytes, value.len) catch return error.Unsupported;
        if (request_bytes > inline_limit) return error.Unsupported;
    }
    var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
    defer arena.deinit();
    const temp = arena.allocator();
    const old = try Manifest.decode(temp, raw);
    if (old.logical_bytes != proof.bytes or !std.mem.eql(u8, &old.logical_digest, &proof.digest)) return error.InvalidDocumentContentAuthority;
    if (old.logical_record.len == 0) return error.Unsupported;
    const prior_logical = try logical.decode(temp, old.logical_record, proof.digest);
    const directory = (try projection.Directory.open(temp, encoded_directory, proof)) orelse return error.Unsupported;
    const certificate = identities.Certificate.open(encoded_certificate, encoded_directory, proof) orelse return error.Unsupported;
    const Item = struct { name: []const u8, part: Part, kind: projection.Kind, canonical: ?[32]u8, logical_digest: logical.Digest };
    var items: std.ArrayList(Item) = .empty;
    var inline_bytes: usize = 0;
    for (old.parts) |part| if (part == .inline_json) {
        inline_bytes = try std.math.add(usize, inline_bytes, part.inline_json.len);
    };
    if (inline_bytes > inline_limit) return error.Unsupported;
    for (directory.fields[0..directory.root_count]) |field| {
        var position: usize = 0;
        var part_value: ?Part = null;
        var overlaps_blob = false;
        for (old.parts) |part| {
            const end = position + part.length();
            if (part == .blob and field.offset < end and field.offset + field.length > position) {
                overlaps_blob = true;
                if (field.offset == position and field.length == part.length()) part_value = part;
            }
            position = end;
        }
        if (overlaps_blob and part_value == null) return error.Unsupported;
        if (part_value == null) {
            const value = try temp.alloc(u8, @intCast(field.length));
            var copied: usize = 0;
            position = 0;
            for (old.parts) |part| {
                const end = position + part.length();
                const first = @max(position, field.offset);
                const last = @min(end, field.offset + field.length);
                if (first < last) {
                    if (part != .inline_json) return error.Unsupported;
                    @memcpy(value[copied..][0 .. last - first], part.inline_json[first - position ..][0 .. last - first]);
                    copied += last - first;
                }
                position = end;
            }
            if (copied != value.len) return error.InvalidDocumentManifest;
            part_value = .{ .inline_json = value };
        }
        const child_digest = for (prior_logical.members) |member| {
            if (std.mem.eql(u8, member.name, field.name)) break member.digest;
        } else return error.InvalidDocumentCommitment;
        try items.append(temp, .{ .name = field.name, .part = part_value.?, .kind = if (field.kind == .object) .subtree else field.kind, .canonical = certificate.find(field.index), .logical_digest = child_digest });
    }
    for (updates, 0..) |update, i| {
        if (update.name.len == 0 or update.name.len > 1024 or update.name[0] == '_' or !std.unicode.utf8ValidateSlice(update.name) or std.mem.indexOfScalar(u8, update.name, 0) != null) return error.InvalidArgument;
        for (updates[0..i]) |prior| if (std.mem.eql(u8, prior.name, update.name)) return error.InvalidArgument;
        const found = for (items.items, 0..) |item, index| {
            if (std.mem.eql(u8, item.name, update.name)) break index;
        } else null;
        if (found) |index| if (items.items[index].part == .blob) return error.Unsupported;
        if (update.value_json) |value| {
            if (value.len > inline_limit) return error.Unsupported;
            const parsed = try std.json.parseFromSliceLeaky(std.json.Value, temp, value, .{ .allocate = .alloc_always });
            const canonical = try identities.validatedScalarDigest(temp, value);
            const item: Item = .{ .name = update.name, .part = .{ .inline_json = value }, .kind = if (parsed == .object or parsed == .array) .subtree else .leaf, .canonical = canonical, .logical_digest = try logical.compute(temp, value) };
            if (found) |index| items.items[index] = item else try items.append(temp, item);
        } else if (found) |index| _ = items.orderedRemove(index);
    }
    if (items.items.len > projection.maximum_fields) return error.Unsupported;
    std.mem.sort(Item, items.items, {}, struct {
        fn less(_: void, l: Item, r: Item) bool {
            return std.mem.order(u8, l.name, r.name) == .lt;
        }
    }.less);
    if (member_order) |order| {
        if (order.len != items.items.len) return error.Unsupported;
        const reordered = try temp.alloc(Item, order.len);
        for (order, reordered, 0..) |name, *item, i| {
            for (order[0..i]) |prior| if (std.mem.eql(u8, prior, name)) return error.InvalidArgument;
            item.* = for (items.items) |candidate| {
                if (std.mem.eql(u8, candidate.name, name)) break candidate;
            } else return error.Unsupported;
        }
        @memcpy(items.items, reordered);
    }
    var inline_out: std.Io.Writer.Allocating = .init(temp);
    var parts: std.ArrayList(Part) = .empty;
    var fields: std.ArrayList(projection.Field) = .empty;
    var records: std.ArrayList(identities.Record) = .empty;
    var logical_bytes: usize = 0;
    try inline_out.writer.writeByte('{');
    for (items.items, 0..) |item, index| {
        if (index != 0) try inline_out.writer.writeByte(',');
        try std.json.Stringify.value(item.name, .{}, &inline_out.writer);
        try inline_out.writer.writeByte(':');
        const offset = logical_bytes + inline_out.written().len;
        var digest: [32]u8 = undefined;
        switch (item.part) {
            .blob => |blob| {
                const prefix_bytes = try temp.dupe(u8, inline_out.written());
                try parts.append(temp, .{ .inline_json = prefix_bytes });
                try parts.append(temp, item.part);
                logical_bytes += prefix_bytes.len + blob.length;
                inline_out.clearRetainingCapacity();
                digest = blob.digest;
            },
            .inline_json => |value| {
                try inline_out.writer.writeAll(value);
                std.crypto.hash.sha2.Sha256.hash(value, &digest, .{});
            },
        }
        try fields.append(temp, .{ .name = item.name, .offset = offset, .length = item.part.length(), .digest = digest, .index = @intCast(index), .kind = item.kind });
        // This bounds all patch work independently of external payload size.
        if (inline_out.written().len > inline_limit) return error.Unsupported;
    }
    try inline_out.writer.writeByte('}');
    const tail = try temp.dupe(u8, inline_out.written());
    try parts.append(temp, .{ .inline_json = tail });
    logical_bytes += tail.len;
    var total_inline: usize = 0;
    var reused_bytes: usize = 0;
    for (parts.items) |part| switch (part) {
        .inline_json => |value| total_inline += value.len,
        .blob => |blob| reused_bytes += blob.length,
    };
    if (total_inline > inline_limit) return error.Unsupported;
    std.mem.sort(projection.Field, fields.items, {}, struct {
        fn less(_: void, l: projection.Field, r: projection.Field) bool {
            return std.mem.order(u8, l.name, r.name) == .lt;
        }
    }.less);
    for (fields.items, 0..) |*field, i| {
        field.index = @intCast(i);
        for (items.items) |item| if (std.mem.eql(u8, field.name, item.name)) {
            if (item.canonical) |canonical| if (item.kind == .leaf) try records.append(temp, .{ .index = @intCast(i), .digest = canonical });
            break;
        };
    }
    const children = try temp.alloc(logical.Member, items.items.len);
    for (items.items, children) |item, *child| child.* = .{ .name = item.name, .digest = item.logical_digest };
    std.mem.sort(logical.Member, children, {}, struct {
        fn less(_: void, l: logical.Member, r: logical.Member) bool {
            return std.mem.order(u8, l.name, r.name) == .lt;
        }
    }.less);
    const digest = try logical.objectDigest(children);
    const root_digests = try temp.alloc(logical.Digest, children.len);
    for (children, root_digests) |child, *identity| identity.* = child.digest;
    const grafted = try graftProjection(a, prior_logical.tree, fields.items, root_digests, updates, logical_bytes, proof.kind, digest);
    const logical_record = grafted.record;
    errdefer a.free(logical_record);
    const paths = grafted.directory;
    errdefer a.free(paths);
    const manifest = try encodeLogical(a, parts.items, digest, logical_record);
    errdefer a.free(manifest);
    // The disposable scalar cache follows the final packed tree indices.
    // Immutable manifests need no separate certificate: DLC2 carries them.
    const final_directory = (try projection.Directory.open(temp, paths, .{ .bytes = @as(u64, logical_bytes), .kind = proof.kind, .digest = digest })).?;
    const final_root = try logical.decode(temp, logical_record, digest);
    records.clearRetainingCapacity();
    for (final_directory.fields) |field| if (field.kind == .leaf) try records.append(temp, .{ .index = field.index, .digest = final_root.tree.?.canonical[field.index] });
    const canonical = try identities.encodeRecords(a, logical_bytes, proof.kind, digest, paths, records.items);
    return .{ .manifest = manifest, .directory = paths, .identities = canonical, .logical_record = logical_record, .proof = .{ .revision = proof.revision, .bytes = logical_bytes, .kind = proof.kind, .digest = digest, .manifest = true, .algorithm = .logical_json_v1 }, .reused_payload_bytes = reused_bytes };
}

test "document payload metadata patch work and allocations do not depend on payload length" {
    const identities = @import("source_value_identity.zig");
    const revisions = @import("document_mutation_revision.zig");
    const a = std.testing.allocator;
    for ([_]usize{ 64 * 1024, 4 * 1024 * 1024, 64 * 1024 * 1024 }) |length| {
        // Deliberately no blob reader or blob allocation. These are authenticated
        // stored descriptors, not uploaded payloads or caller-supplied identities.
        const parts = [_]Part{ .{ .inline_json = "{\"body\":" }, .{ .blob = .{ .length = length, .digest = @splat(0xab) } }, .{ .inline_json = ",\"metadata\":1}" } };
        const logical_length = 8 + length + 14;
        const children = [_]logical.Member{ .{ .name = "body", .digest = @splat(0xcd) }, .{ .name = "metadata", .digest = try logical.compute(a, "1") } };
        const digest = try logical.objectDigest(&children);
        const logical_record = try logical.encode(a, .{ .digest = digest, .members = @constCast(&children) });
        defer a.free(logical_record);
        const proof: revisions.Content = .{ .revision = 1, .bytes = logical_length, .kind = keys.primary_kind, .digest = digest, .manifest = true, .algorithm = .logical_json_v1 };
        const manifest = try encodeLogical(a, &parts, digest, logical_record);
        defer a.free(manifest);
        var number_digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash("1", &number_digest, .{});
        const fields = [_]projection.Field{
            .{ .name = "body", .offset = 8, .length = length, .digest = @splat(0xab), .kind = .leaf },
            .{ .name = "metadata", .offset = 8 + length + 12, .length = 1, .digest = number_digest, .kind = .leaf, .index = 1 },
        };
        const directory = try projection.encodeRoots(a, logical_length, proof.kind, digest, &fields);
        defer a.free(directory);
        const certificate = try identities.encodeRecords(a, logical_length, proof.kind, digest, directory, &.{ .{ .index = 0, .digest = @splat(0xcd) }, .{ .index = 1, .digest = (try identities.validatedScalarDigest(a, "1")).? } });
        defer a.free(certificate);
        const Case = struct {
            fn run(alloc: A, raw: []const u8, current: revisions.Content, paths: []const u8, canonical: []const u8) !void {
                const patch = try prepareMetadataPatch(alloc, raw, current, paths, canonical, &.{ .{ .name = "metadata", .value_json = "2" }, .{ .name = "added", .value_json = "true" } });
                defer patch.deinit(alloc);
                try std.testing.expectEqual(current.bytes - 22, patch.reused_payload_bytes);
                try std.testing.expect(patch.manifest.len < 1024);
                try std.testing.expect(patch.directory.len < 512);
                try std.testing.expect(patch.identities.len < 256);
                const decoded = try Manifest.decode(alloc, patch.manifest);
                defer alloc.free(decoded.parts);
                try std.testing.expect(decoded.rooted);
                try std.testing.expect(!std.mem.eql(u8, &patch.proof.digest, &current.digest));
            }
        };
        try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Case.run, .{ manifest, proof, directory, certificate });
        const start = @import("antfly_platform").time.monotonicNs();
        const rounds = 128;
        for (0..rounds) |_| try Case.run(a, manifest, proof, directory, certificate);
        const elapsed = @import("antfly_platform").time.monotonicNs() - start;
        std.debug.print("DOCUMENT_PATCH_PLAN payload_bytes={d} manifest_bytes={d} payload_read_bytes=0 payload_write_bytes=0 rounds={d} ns_per_patch={d}\n", .{ length, manifest.len, rounds, elapsed / rounds });
        try std.testing.expectError(error.Unsupported, prepareMetadataPatch(a, manifest, proof, directory, certificate, &.{.{ .name = "body", .value_json = "null" }}));
        try std.testing.expectError(error.InvalidArgument, prepareMetadataPatch(a, manifest, proof, directory, certificate, &.{ .{ .name = "metadata", .value_json = "2" }, .{ .name = "metadata", .value_json = "3" } }));
    }
}

test "document payload commitments reject malformed manifests and explicit scrubs detect damaged values" {
    const a = std.testing.allocator;
    const body = try a.alloc(u8, minimum_value_bytes);
    defer a.free(body);
    @memset(body, 'x');
    body[0] = '"';
    body[body.len - 1] = '"';
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
    const parts = [_]Part{ .{ .inline_json = "{\"body\":" }, .{ .blob = .{ .length = body.len, .digest = digest } }, .{ .inline_json = "}" } };
    const encoded = try encodeRooted(a, &parts);
    defer a.free(encoded);
    for (0..encoded.len) |len| try std.testing.expectError(error.InvalidDocumentManifest, Manifest.decode(a, encoded[0..len]));
    const changed = try a.dupe(u8, encoded);
    defer a.free(changed);
    changed[header_bytes + 9] ^= 1;
    // Repairing only the outer checksum cannot change the committed document.
    std.crypto.hash.sha2.Sha256.hash(changed[0 .. changed.len - 32], changed[changed.len - 32 ..][0..32], .{});
    try std.testing.expectError(error.InvalidDocumentManifest, Manifest.decode(a, changed));
    const physical = try keys.documentKeyAlloc(a, "doc");
    defer a.free(physical);
    const Reader = struct {
        bytes: []const u8,
        pub fn get(self: *@This(), _: []const u8) ![]const u8 {
            return self.bytes;
        }
    };
    var reader: Reader = .{ .bytes = body };
    const valid = try readVerified(a, &reader, physical, encoded);
    defer a.free(valid);
    try std.testing.expectEqual(body.len + 9, valid.len);
    body[1] = 'y';
    try std.testing.expectError(error.InvalidDocumentManifest, readVerified(a, &reader, physical, encoded));
    const shortened: Reader = .{ .bytes = body[0 .. body.len - 1] };
    var missing = shortened;
    try std.testing.expectError(error.InvalidDocumentManifest, read(a, &missing, physical, encoded));
}

test "document payload decoded plans retain leases bound memory and reject corruption on cache hits" {
    const a = std.testing.allocator;
    var cache = ManifestCache.init(a);
    defer cache.deinit();
    const first = try encodeRooted(a, &.{.{ .inline_json = "{}" }});
    defer a.free(first);
    const pinned = (try cache.acquire(first)).?;
    defer pinned.release();
    for (0..256) |index| {
        const value = try std.fmt.allocPrint(a, "{{\"n\":{d}}}", .{index});
        defer a.free(value);
        const raw = try encodeRooted(a, &.{.{ .inline_json = value }});
        defer a.free(raw);
        if (try cache.acquire(raw)) |lease| lease.release();
    }
    const same = (try cache.acquire(first)).?;
    defer same.release();
    try std.testing.expect(pinned.entry == same.entry);
    try std.testing.expect(cache.retainedBytes() <= ManifestCache.maximum_bytes);
    const damaged = try a.dupe(u8, first);
    defer a.free(damaged);
    damaged[header_bytes + 9] ^= 1;
    try std.testing.expectError(error.InvalidDocumentManifest, cache.acquire(damaged));
    cache.noteRow("a");
    try std.testing.expect(cache.knowsRow("a"));
    cache.forgetRow("a");
    try std.testing.expect(!cache.knowsRow("a"));
}

test "document payload range reads batch sorted immutable keys directly into final output" {
    const a = std.testing.allocator;
    const Reader = struct {
        calls: usize = 0,
        bytes: usize = 0,
        pub fn getRangesManyPhysical(self: *@This(), requests: []const @import("value_ranges.zig").Request) !void {
            self.calls += 1;
            for (requests, 0..) |request, index| {
                if (index > 0) try std.testing.expect(std.mem.order(u8, requests[index - 1].key, request.key) != .gt);
                for (request.ranges, request.output) |range, *out| {
                    try std.testing.expect(range.length == 1);
                    self.bytes += range.length;
                    out.* = if (request.key[request.key.len - 1] == 1) "a" else "b";
                }
            }
        }
    };
    const parts = try a.alloc(Part, 130);
    defer a.free(parts);
    var d: [32]u8 = @splat(0);
    for (parts, 0..) |*part, i| {
        d[31] = if (i % 2 == 0) 2 else 1;
        part.* = .{ .blob = .{ .length = 1, .digest = d } };
    }
    // Direct planner test uses tiny synthetic parts; persisted manifests still
    // require the normal minimum immutable-value size and validated checksums.
    const manifest: Manifest = .{ .logical_bytes = 130, .logical_digest = @splat(0), .physical_commitment = @splat(0), .logical_record = "", .parts = parts };
    var reader: Reader = .{};
    var result: [129]u8 = undefined;
    try manifest.readInto(&reader, "row", 1, &result);
    try std.testing.expectEqual(@as(usize, 3), reader.calls);
    try std.testing.expectEqual(result.len, reader.bytes);
    for (result, 0..) |byte, i| try std.testing.expectEqual(@as(u8, if (i % 2 == 0) 'a' else 'b'), byte);
}

test "document payload ineligible writes use a conservative invalidated absence hint" {
    const Reader = struct {
        reads: usize = 0,
        raw: [8]u8 = "ASCH\x19\x00\x00\x00".*,
        pub fn get(self: *@This(), _: []const u8) ![]const u8 {
            self.reads += 1;
            return &self.raw;
        }
    };
    var reader: Reader = .{};
    var cache: std.atomic.Value(u8) = .init(0);
    const publication = .{ .directory = @as(?[]const u8, null), .prior_manifest = @as(?bool, null) };
    try std.testing.expect(!try shouldStage(&reader, &cache, publication, 4096));
    try std.testing.expect(!try shouldStage(&reader, &cache, publication, 4096));
    try std.testing.expectEqual(@as(usize, 1), reader.reads);
    // A writer marks possible values before publication. An abort is merely a
    // conservative false positive; a schema write cannot reset it to absence.
    cache.store(2, .release);
    observeFormatWrite(&cache, @import("schema.zig").schema_key, &reader.raw);
    try std.testing.expect(try shouldStage(&reader, &cache, publication, 4096));
    try std.testing.expectEqual(@as(usize, 1), reader.reads);
    cache.store(0, .release);
    reader.raw[4] = 26;
    try std.testing.expect(try shouldStage(&reader, &cache, publication, 4096));
    try std.testing.expectEqual(@as(u8, 2), cache.load(.acquire));
    cache.store(1, .release);
    observeFormatWrite(&cache, @import("schema.zig").schema_key, &reader.raw);
    try std.testing.expectEqual(@as(u8, 2), cache.load(.acquire));
}

test "document payload direct scopes fill sorted caller buffers and preserve partial range fallbacks" {
    const a = std.testing.allocator;
    const Reader = struct {
        const Parent = @This();
        opened: usize = 0,
        closed: usize = 0,
        batches: usize = 0,
        ranges: usize = 0,
        physical: usize = 0,
        direct: bool = true,
        missing: bool = false,
        oversized: bool = false,
        unsupported: bool = false,
        const Scope = struct {
            owner: *Parent,
            pub fn supportsDirectInto(self: *@This()) bool {
                return self.owner.direct;
            }
            pub fn close(self: *@This()) void {
                self.owner.closed += 1;
            }
            pub fn getManySortedInto(self: *@This(), names: []const []const u8, buffers: []const []u8, values: []?[]const u8) anyerror!void {
                self.owner.batches += 1;
                if (self.owner.oversized) return error.BufferTooSmall;
                if (self.owner.unsupported) return error.Unsupported;
                for (names, buffers, values, 0..) |name, buffer, *value, i| {
                    if (i != 0) try std.testing.expect(std.mem.order(u8, names[i - 1], name) != .gt);
                    if (self.owner.missing) {
                        value.* = null;
                    } else if (i != 0 and std.mem.eql(u8, names[i - 1], name)) {
                        // A compatible provider can share duplicate-key results.
                        value.* = values[i - 1];
                    } else {
                        @memset(buffer, if (name[name.len - 1] == 1) 'a' else 'b');
                        value.* = buffer;
                    }
                }
            }
            pub fn getRangesManyPhysical(self: *@This(), requests: []const @import("value_ranges.zig").Request) anyerror!void {
                self.owner.ranges += 1;
                for (requests) |request| for (request.ranges, request.output) |range, *out| {
                    try std.testing.expectEqual(@as(u64, 1), range.offset);
                    try std.testing.expectEqual(@as(usize, 2), range.length);
                    out.* = "bc";
                };
            }
        };
        pub fn openReadScope(self: *@This(), _: A) anyerror!Scope {
            self.opened += 1;
            return .{ .owner = self };
        }
        pub fn getPhysical(self: *@This(), name: []const u8) anyerror![]const u8 {
            try std.testing.expectEqual(self.opened, self.closed);
            self.physical += 1;
            return if (name[name.len - 1] == 1) "a" else "b";
        }
    };
    var parts: [130]Part = undefined;
    for (&parts, 0..) |*part, i| {
        var digest: [32]u8 = @splat(0);
        digest[31] = if (i % 2 == 0) 1 else 2;
        part.* = .{ .blob = .{ .length = 1, .digest = digest } };
    }
    const manifest: Manifest = .{ .logical_bytes = parts.len, .logical_digest = @splat(0), .physical_commitment = @splat(0), .logical_record = "", .parts = &parts };
    for ([_]bool{ true, false }) |direct| {
        var reader: Reader = .{ .direct = direct };
        const value = try manifest.readOwned(a, &reader, "row");
        defer a.free(value);
        try std.testing.expectEqual(@as(usize, 1), reader.opened);
        try std.testing.expectEqual(@as(usize, 1), reader.closed);
        try std.testing.expectEqual(@as(usize, if (direct) 3 else 0), reader.batches);
        try std.testing.expectEqual(@as(usize, if (direct) 0 else parts.len), reader.physical);
        for (value, 0..) |byte, i| try std.testing.expectEqual(@as(u8, if (i % 2 == 0) 'a' else 'b'), byte);
    }
    var unsupported: Reader = .{ .unsupported = true };
    const compatibility = try manifest.readOwned(a, &unsupported, "row");
    defer a.free(compatibility);
    try std.testing.expectEqual(@as(usize, 1), unsupported.closed);
    try std.testing.expectEqual(parts.len, unsupported.physical);
    var missing: Reader = .{ .missing = true };
    try std.testing.expectError(error.NotFound, manifest.readOwned(a, &missing, "row"));
    try std.testing.expectEqual(@as(usize, 1), missing.closed);
    var oversized: Reader = .{ .oversized = true };
    try std.testing.expectError(error.InvalidDocumentManifest, manifest.readOwned(a, &oversized, "row"));
    try std.testing.expectEqual(@as(usize, 1), oversized.closed);
    var partial: Reader = .{};
    var scope = try partial.openReadScope(a);
    defer scope.close();
    var one_part = [_]Part{.{ .blob = .{ .length = 4, .digest = @splat(0) } }};
    const one: Manifest = .{ .logical_bytes = 4, .logical_digest = @splat(0), .physical_commitment = @splat(0), .logical_record = "", .parts = &one_part };
    const excerpt = try one.readRange(a, &scope, "row", 1, 2);
    defer a.free(excerpt);
    try std.testing.expectEqualStrings("bc", excerpt);
    try std.testing.expectEqual(@as(usize, 0), partial.batches);
    try std.testing.expectEqual(@as(usize, 1), partial.ranges);
}
