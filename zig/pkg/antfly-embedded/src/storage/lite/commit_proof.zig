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

//! Checked postimage/anchor encodings for native commit recovery. This module
//! deliberately owns no IO, acknowledgement, page allocation or publication.
//! The pager must retain every referenced frame and validate its unknown
//! ancestry before selecting a checkpoint. It must write a durability witness
//! only AFTER successful file synchronization.
const std = @import("std");
const Hash = std.crypto.hash.sha2.Sha256;
pub const Digest = [32]u8;
pub const header_size = 160;
pub const entry_size = 40;
pub const witness_size = 128;
const frame_magic = "AFCP1\x00\x00\x00";
const witness_magic = "AFCW1\x00\x00\x00";
pub const max_frame_bytes = 1024 * 1024;
pub const max_postimage_entries = 16 * 1024;
pub const max_postimage_bytes = 64 * 1024 * 1024;

pub const Reference = struct {
    root: u64 = 0,
    length: u64 = 0,
    digest: Digest = @splat(0),
};
pub const Entry = struct {
    page: u64,
    digest: Digest,
    pub fn postimage(page: u64, bytes: []const u8) Entry {
        return .{ .page = page, .digest = hash(bytes) };
    }
};
pub const Header = struct {
    namespace: Digest,
    sequence: u64,
    anchor: u64,
    parent: ?Reference = null,
    checkpoint: Digest,
};
pub fn hash(bytes: []const u8) Digest {
    var digest: Digest = undefined;
    Hash.hash(bytes, &digest, .{});
    return digest;
}
pub fn encodedSize(capacity: usize) !usize {
    const size = std.math.add(usize, header_size, std.math.mul(usize, capacity, entry_size) catch return error.InvalidCommitProof) catch return error.InvalidCommitProof;
    if (size > max_frame_bytes) return error.InvalidCommitProof;
    return size;
}
fn validateHeader(header: Header) !void {
    if (header.sequence == 0 or header.anchor >= header.sequence) return error.InvalidCommitProof;
    if (header.anchor == header.sequence - 1) {
        if (header.parent != null) return error.InvalidCommitProof;
    } else {
        const parent = header.parent orelse return error.InvalidCommitProof;
        if (parent.root == 0 or parent.length < header_size or parent.length > max_frame_bytes) return error.InvalidCommitProof;
    }
}

/// Capacity is retained in the encoding so the pager can reserve a bounded
/// frame before its final allocator/certificate writes are known. Unused slots
/// must be zero and are authenticated with the rest of the frame.
pub fn encode(out: []u8, header: Header, entries: []const Entry, capacity: usize) !void {
    try validateHeader(header);
    if (out.len != try encodedSize(capacity) or entries.len > capacity or entries.len > max_postimage_entries) return error.InvalidCommitProof;
    var previous: u64 = 0;
    for (entries) |entry| {
        if (entry.page <= previous) return error.InvalidCommitProof;
        previous = entry.page;
    }
    @memset(out, 0);
    @memcpy(out[0..8], frame_magic);
    @memcpy(out[8..40], &header.namespace);
    std.mem.writeInt(u64, out[40..48], header.sequence, .little);
    std.mem.writeInt(u64, out[48..56], header.anchor, .little);
    if (header.parent) |parent| {
        std.mem.writeInt(u64, out[56..64], parent.root, .little);
        std.mem.writeInt(u64, out[64..72], parent.length, .little);
        @memcpy(out[72..104], &parent.digest);
    }
    @memcpy(out[104..136], &header.checkpoint);
    std.mem.writeInt(u64, out[136..144], @intCast(entries.len), .little);
    std.mem.writeInt(u64, out[144..152], @intCast(capacity), .little);
    for (entries, 0..) |entry, i| {
        const offset = header_size + i * entry_size;
        std.mem.writeInt(u64, out[offset..][0..8], entry.page, .little);
        @memcpy(out[offset + 8 ..][0..32], &entry.digest);
    }
}

pub const Frame = struct {
    header: Header,
    bytes: []const u8,
    count: usize,
    pub fn entry(self: Frame, index: usize) Entry {
        std.debug.assert(index < self.count);
        const offset = header_size + index * entry_size;
        return .{ .page = std.mem.readInt(u64, self.bytes[offset..][0..8], .little), .digest = self.bytes[offset + 8 ..][0..32].* };
    }
    pub fn matches(self: Frame, reference: Reference) bool {
        return reference.length == self.bytes.len and std.mem.eql(u8, &reference.digest, &hash(self.bytes));
    }
    pub fn requireCheckpoint(self: Frame, namespace: Digest, sequence: u64, checkpoint: Digest) !void {
        if (self.header.sequence != sequence or !std.mem.eql(u8, &self.header.namespace, &namespace) or
            !std.mem.eql(u8, &self.header.checkpoint, &checkpoint)) return error.InvalidCommitProof;
    }
    /// This validates a dependency edge, not the whole recovery suffix. The
    /// pager must continue until the selected frame's proven anchor is reached.
    pub fn requireParent(self: Frame, parent: Frame) !void {
        const reference = self.header.parent orelse return error.InvalidCommitProof;
        if (!parent.matches(reference) or parent.header.sequence != self.header.sequence - 1 or
            !std.mem.eql(u8, &parent.header.namespace, &self.header.namespace)) return error.InvalidCommitProof;
    }
    pub fn requirePostimage(self: Frame, index: usize, page: u64, bytes: []const u8) !void {
        const expected = self.entry(index);
        if (expected.page != page or !std.mem.eql(u8, &expected.digest, &hash(bytes))) return error.CommitPostimageMismatch;
    }
};

pub const Prefix = struct {
    header: Header,
    count: usize,
    capacity: usize,
};

/// The GC needs only the immutable dependency edge in the first value page.
/// The pager authenticates the complete frame before selecting a checkpoint;
/// prefix decoding alone must never authorize recovery or durable anchoring.
pub fn decodePrefix(bytes: []const u8, encoded_length: u64) !Prefix {
    if (bytes.len < header_size or !std.mem.eql(u8, bytes[0..8], frame_magic)) return error.InvalidCommitProof;
    const count = std.math.cast(usize, std.mem.readInt(u64, bytes[136..144], .little)) orelse return error.InvalidCommitProof;
    const capacity = std.math.cast(usize, std.mem.readInt(u64, bytes[144..152], .little)) orelse return error.InvalidCommitProof;
    if (encoded_length != try encodedSize(capacity) or count > capacity or count > max_postimage_entries or !std.mem.allEqual(u8, bytes[152..160], 0)) return error.InvalidCommitProof;
    const parent_root = std.mem.readInt(u64, bytes[56..64], .little);
    const header: Header = .{
        .namespace = bytes[8..40].*,
        .sequence = std.mem.readInt(u64, bytes[40..48], .little),
        .anchor = std.mem.readInt(u64, bytes[48..56], .little),
        .parent = if (parent_root != 0) .{ .root = parent_root, .length = std.mem.readInt(u64, bytes[64..72], .little), .digest = bytes[72..104].* } else null,
        .checkpoint = bytes[104..136].*,
    };
    if (parent_root == 0 and !std.mem.allEqual(u8, bytes[64..104], 0)) return error.InvalidCommitProof;
    try validateHeader(header);
    return .{ .header = header, .count = count, .capacity = capacity };
}

pub fn decode(bytes: []const u8, page_count: u64) !Frame {
    const prefix = try decodePrefix(bytes, bytes.len);
    const frame: Frame = .{ .header = prefix.header, .bytes = bytes, .count = prefix.count };
    var previous: u64 = 0;
    for (0..frame.count) |i| {
        const item = frame.entry(i);
        if (item.page <= previous or item.page >= page_count) return error.InvalidCommitProof;
        previous = item.page;
    }
    if (!std.mem.allEqual(u8, bytes[header_size + frame.count * entry_size ..], 0)) return error.InvalidCommitProof;
    return frame;
}

pub const Verification = struct {
    frames: usize = 0,
    checked_pages: usize = 0,
    shadowed_pages: usize = 0,
    anchor: u64 = 0,
};

fn requireReference(reference: Reference, page_count: u64) !void {
    if (reference.root == 0 or reference.root >= page_count or reference.length < header_size or
        reference.length > max_frame_bytes or (reference.length - header_size) % entry_size != 0) return error.InvalidCommitProof;
}

/// Reader returns allocator-owned bytes from readFrame(allocator, reference)
/// and readPage(allocator, page). The caller supplies the selected checkpoint's
/// exact fingerprint, namespace and physical page bound. Newest postimages win:
/// an old intermediate page may have been safely reused by a later unknown
/// transaction, so checking the old image again would reject valid recovery.
pub fn verify(
    allocator: std.mem.Allocator,
    reader: anytype,
    reference: Reference,
    namespace: Digest,
    sequence: u64,
    checkpoint: Digest,
    page_count: u64,
) !Verification {
    var result: Verification = .{};
    var current = reference;
    var expected_sequence = sequence;
    var seen: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer seen.deinit(allocator);
    var needs_seen = false;
    while (true) {
        try requireReference(current, page_count);
        const bytes = try reader.readFrame(allocator, current);
        defer allocator.free(bytes);
        if (bytes.len != current.length or !std.mem.eql(u8, &hash(bytes), &current.digest)) return error.InvalidCommitProof;
        const frame = try decode(bytes, page_count);
        if (result.frames == 0) {
            try frame.requireCheckpoint(namespace, sequence, checkpoint);
            result.anchor = frame.header.anchor;
            needs_seen = frame.header.parent != null;
        } else if (frame.header.sequence != expected_sequence or
            !std.mem.eql(u8, &frame.header.namespace, &namespace) or frame.header.anchor > result.anchor) return error.InvalidCommitProof;
        result.frames = try std.math.add(usize, result.frames, 1);
        for (0..frame.count) |index| {
            const item = frame.entry(index);
            if (needs_seen) {
                const entry_ = try seen.getOrPut(allocator, item.page);
                if (entry_.found_existing) {
                    result.shadowed_pages = try std.math.add(usize, result.shadowed_pages, 1);
                    continue;
                }
            }
            const page = try reader.readPage(allocator, item.page);
            defer allocator.free(page);
            try frame.requirePostimage(index, item.page, page);
            result.checked_pages = try std.math.add(usize, result.checked_pages, 1);
        }
        if (frame.header.sequence == result.anchor + 1) return result;
        current = frame.header.parent orelse return error.InvalidCommitProof;
        expected_sequence = frame.header.sequence - 1;
    }
}

/// Optional transaction-local optimization state. Exhausting its fixed bound
/// or allocator disables proof publication for this transaction; the pager must
/// then use its safe data-before-checkpoint protocol. It must never publish a
/// manifest of a truncated write set.
pub const Postimages = struct {
    pub const max_entries = max_postimage_entries;
    pages: std.AutoHashMapUnmanaged(u64, Digest) = .empty,
    overflowed: bool = false,
    bytes: usize = 0,
    page_size: usize = 0,
    pub fn deinit(self: *Postimages, allocator: std.mem.Allocator) void {
        self.pages.deinit(allocator);
        self.* = undefined;
    }
    pub fn note(self: *Postimages, allocator: std.mem.Allocator, page: u64, bytes: []const u8) void {
        if (self.overflowed) return;
        if (bytes.len == 0 or (self.page_size != 0 and self.page_size != bytes.len)) {
            self.disable(allocator);
            return;
        }
        self.page_size = bytes.len;
        const exists = self.pages.contains(page);
        if (page == 0 or (!exists and (self.pages.count() >= max_entries or bytes.len > max_postimage_bytes -| self.bytes))) {
            self.disable(allocator);
            return;
        }
        self.pages.put(allocator, page, hash(bytes)) catch {
            self.disable(allocator);
            return;
        };
        if (!exists) self.bytes += bytes.len;
    }
    fn disable(self: *Postimages, allocator: std.mem.Allocator) void {
        self.pages.clearAndFree(allocator);
        self.bytes = 0;
        self.page_size = 0;
        self.overflowed = true;
    }
    pub fn sorted(self: *const Postimages, allocator: std.mem.Allocator) ![]Entry {
        if (self.overflowed) return error.IncompleteCommitProof;
        const entries = try allocator.alloc(Entry, self.pages.count());
        var it = self.pages.iterator();
        var i: usize = 0;
        while (it.next()) |entry_| : (i += 1) entries[i] = .{ .page = entry_.key_ptr.*, .digest = entry_.value_ptr.* };
        std.mem.sort(Entry, entries, {}, struct {
            fn less(_: void, left: Entry, right: Entry) bool {
                return left.page < right.page;
            }
        }.less);
        return entries;
    }
};

pub const Witness = struct {
    namespace: Digest,
    sequence: u64,
    checkpoint: Digest,
    proof: Digest,
    inode: u64,
    pub fn encode(self: Witness) [witness_size]u8 {
        var out: [witness_size]u8 = @splat(0);
        @memcpy(out[0..8], witness_magic);
        @memcpy(out[8..40], &self.namespace);
        std.mem.writeInt(u64, out[40..48], self.sequence, .little);
        @memcpy(out[48..80], &self.checkpoint);
        @memcpy(out[80..112], &self.proof);
        std.mem.writeInt(u64, out[112..120], self.inode, .little);
        std.mem.writeInt(u32, out[124..128], std.hash.crc.@"CRC-32/ISO-HDLC".hash(out[0..124]), .little);
        return out;
    }
    pub fn decode(bytes: []const u8) !Witness {
        if (bytes.len != witness_size or !std.mem.eql(u8, bytes[0..8], witness_magic) or
            !std.mem.allEqual(u8, bytes[120..124], 0) or
            std.mem.readInt(u32, bytes[124..128], .little) != std.hash.crc.@"CRC-32/ISO-HDLC".hash(bytes[0..124])) return error.InvalidDurabilityWitness;
        return .{ .namespace = bytes[8..40].*, .sequence = std.mem.readInt(u64, bytes[40..48], .little), .checkpoint = bytes[48..80].*, .proof = bytes[80..112].*, .inode = std.mem.readInt(u64, bytes[112..120], .little) };
    }
    pub fn matches(self: Witness, expected: Witness) bool {
        return std.meta.eql(self, expected);
    }
};

test "commit proof rejects old valid postimages and binds exact ancestry" {
    const namespace: Digest = @splat(1);
    var parent_bytes: [header_size + entry_size]u8 = undefined;
    try encode(&parent_bytes, .{ .namespace = namespace, .sequence = 2, .anchor = 1, .checkpoint = @splat(2) }, &.{.postimage(9, "parent new page")}, 1);
    const parent = try decode(&parent_bytes, 20);
    var child_bytes: [header_size + 2 * entry_size]u8 = undefined;
    try encode(&child_bytes, .{ .namespace = namespace, .sequence = 3, .anchor = 1, .parent = .{ .root = 11, .length = parent_bytes.len, .digest = hash(&parent_bytes) }, .checkpoint = @splat(3) }, &.{.postimage(12, "child new page")}, 2);
    const child = try decode(&child_bytes, 20);
    try child.requireParent(parent);
    try child.requirePostimage(0, 12, "child new page");
    try std.testing.expectError(error.CommitPostimageMismatch, parent.requirePostimage(0, 9, "parent old valid page"));
    try std.testing.expectError(error.CommitPostimageMismatch, child.requirePostimage(0, 9, "child new page"));
    try std.testing.expectError(error.InvalidCommitProof, child.requireCheckpoint(@splat(7), 3, @splat(3)));
    // A valid child alone cannot qualify a lost unknown parent postimage.
    try child.requirePostimage(0, 12, "child new page");
    try std.testing.expectError(error.CommitPostimageMismatch, parent.requirePostimage(0, 9, "lost parent"));
    parent_bytes[header_size + 9] ^= 1;
    const changed_parent = try decode(&parent_bytes, 20);
    try std.testing.expectError(error.InvalidCommitProof, child.requireParent(changed_parent));
}

test "commit proof encoding refuses hidden duplicate overflow and unchecked parents" {
    var bytes: [header_size + 2 * entry_size]u8 = undefined;
    const header: Header = .{ .namespace = @splat(1), .sequence = 2, .anchor = 1, .checkpoint = @splat(2) };
    try std.testing.expectError(error.InvalidCommitProof, encode(&bytes, header, &.{ .postimage(3, "a"), .postimage(3, "b") }, 2));
    try std.testing.expectError(error.InvalidCommitProof, encodedSize(std.math.maxInt(usize)));
    var missing = header;
    missing.anchor = 0;
    try std.testing.expectError(error.InvalidCommitProof, encode(&bytes, missing, &.{.postimage(3, "a")}, 2));
    try encode(&bytes, header, &.{.postimage(3, "a")}, 2);
    _ = try decode(&bytes, 4);
    try std.testing.expectError(error.InvalidCommitProof, decode(&bytes, 3));
    bytes[bytes.len - 1] = 1;
    try std.testing.expectError(error.InvalidCommitProof, decode(&bytes, 4));
}

test "durability witness rejects every single byte tear and mismatched ownership" {
    const witness: Witness = .{ .namespace = @splat(1), .sequence = 8, .checkpoint = @splat(2), .proof = @splat(3), .inode = 17 };
    const encoded = witness.encode();
    try std.testing.expect(witness.matches(try Witness.decode(&encoded)));
    for (0..encoded.len) |index| {
        var torn = encoded;
        torn[index] ^= 1;
        try std.testing.expectError(error.InvalidDurabilityWitness, Witness.decode(&torn));
    }
    var foreign = witness;
    foreign.inode += 1;
    try std.testing.expect(!witness.matches(foreign));
    foreign = witness;
    foreign.sequence -= 1;
    try std.testing.expect(!witness.matches(foreign));
}

test "commit proof recovery validates unknown parents and only the newest reused postimage" {
    const a = std.testing.allocator;
    const namespace: Digest = @splat(7);
    var parent_bytes: [header_size + 2 * entry_size]u8 = undefined;
    try encode(&parent_bytes, .{ .namespace = namespace, .sequence = 2, .anchor = 1, .checkpoint = @splat(2) }, &.{ .postimage(9, "parent data"), .postimage(13, "old intermediate page") }, 2);
    var child_bytes: [header_size + entry_size]u8 = undefined;
    try encode(&child_bytes, .{ .namespace = namespace, .sequence = 3, .anchor = 1, .parent = .{ .root = 11, .length = parent_bytes.len, .digest = hash(&parent_bytes) }, .checkpoint = @splat(3) }, &.{.postimage(13, "reused latest page")}, 1);
    const reference: Reference = .{ .root = 12, .length = child_bytes.len, .digest = hash(&child_bytes) };
    const Reader = struct {
        parent: []const u8,
        child: []const u8,
        persisted: u4,
        pub fn readFrame(self: *@This(), allocator: std.mem.Allocator, ref: Reference) ![]u8 {
            const value = switch (ref.root) {
                11 => self.parent,
                12 => self.child,
                else => return error.InvalidCommitProof,
            };
            const out = try allocator.dupe(u8, value);
            if ((self.persisted & (if (ref.root == 11) @as(u4, 4) else @as(u4, 8))) == 0) out[0] ^= 1;
            return out;
        }
        pub fn readPage(self: *@This(), allocator: std.mem.Allocator, page: u64) ![]u8 {
            return allocator.dupe(u8, switch (page) {
                9 => if (self.persisted & 1 != 0) "parent data" else "old valid page",
                13 => if (self.persisted & 2 != 0) "reused latest page" else "old intermediate page",
                else => return error.InvalidCommitProof,
            });
        }
    };
    for (0..16) |mask| {
        var reader: Reader = .{ .parent = &parent_bytes, .child = &child_bytes, .persisted = @intCast(mask) };
        if (verify(a, &reader, reference, namespace, 3, @splat(3), 20)) |result| {
            try std.testing.expectEqual(@as(usize, 15), mask);
            try std.testing.expectEqual(@as(usize, 2), result.frames);
            try std.testing.expectEqual(@as(usize, 2), result.checked_pages);
            try std.testing.expectEqual(@as(usize, 1), result.shadowed_pages);
        } else |_| try std.testing.expect(mask != 15);
    }
}

test "commit proof postimage collection owns final images and refuses allocation-truncated proofs" {
    const a = std.testing.allocator;
    var images: Postimages = .{};
    defer images.deinit(a);
    images.note(a, 7, "first image");
    images.note(a, 3, "other image");
    images.note(a, 7, "final image");
    const sorted = try images.sorted(a);
    defer a.free(sorted);
    try std.testing.expectEqual(@as(usize, 2), sorted.len);
    try std.testing.expectEqual(@as(u64, 3), sorted[0].page);
    try std.testing.expectEqualSlices(u8, &hash("final image"), &sorted[1].digest);
    var denied: Postimages = .{};
    defer denied.deinit(std.testing.failing_allocator);
    denied.note(std.testing.failing_allocator, 1, "cannot retain this proof entry");
    try std.testing.expect(denied.overflowed);
    try std.testing.expectError(error.IncompleteCommitProof, denied.sorted(a));
}
