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

//! Replicated, bounded upload envelope for an ordered artifact publication.
//! The upload is inert until one final ordered publication decision consumes
//! its complete authenticated command. No local file path is an authority.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const codec = @import("artifact_publication_transport_codec.zig");
const Allocator = std.mem.Allocator;
pub const Digest = publication.Digest;
pub const Namespace = publication.Namespace;

const manifest_magic = "APM1";
const manifest_domain = "antfly:artifact-publication-upload-manifest:v1:";
const chunk_domain = "antfly:artifact-publication-upload-chunk:v1:";
const manifest_prefix = "\x00\x00__artifact_publication_upload__:manifest:";
const chunk_prefix = "\x00\x00__artifact_publication_upload__:chunk:";
const quota_key = "\x00\x00__artifact_publication_upload__:quota";
const quota_magic = "APQ1";
const progress_prefix = "\x00\x00__artifact_publication_upload__:progress:";
const terminal_prefix = "\x00\x00__artifact_publication_upload__:terminal:";
const terminal_magic = "APT1";
const terminal_ring_head_key = "\x00\x00__artifact_publication_upload__:terminal_head";
const terminal_ring_prefix = "\x00\x00__artifact_publication_upload__:terminal_slot:";
/// Transport acknowledgements are a retransmission cache, not semantic
/// receipts. Bound both their count and retirement work independently of
/// table size and wall time. An evicted upload may be staged again; final
/// publication still goes through the durable semantic receipt/guard path.
pub const max_terminal_records: u64 = 1024;
pub const max_active_uploads: usize = 8;
pub const max_staged_bytes: usize = 256 * 1024 * 1024;
pub const control_upload_slots: usize = 2;
pub const control_staged_bytes: usize = 16 * 1024 * 1024;
pub const max_producer_uploads = max_active_uploads - control_upload_slots;
pub const max_upload_age_entries: u64 = 10_000;
pub const manifest_header_len: usize = 4 + 24 + 32 + 32 + 4 + 2 + 8 + 32 + 1;
pub const max_manifest_len: usize = manifest_header_len + codec.max_chunks * 32;

/// Private owner-leader control. Every variant remains far below the Raft
/// request ceiling; only one chunk's base64 text is present in a stage entry.
/// The value is still authenticated by the signed internal Raft proposal and
/// by the durable manifest's per-chunk digest at deterministic apply.
pub const Request = struct {
    action: enum { begin, chunk, finalize, prune, abandon },
    namespace: Namespace = @splat(0),
    publication_digest: Digest = @splat(0),
    manifest_root: Digest = @splat(0),
    command_digest: Digest = @splat(0),
    encoded_len: u32 = 0,
    chunk_hashes: []const Digest = &.{},
    ordinal: u16 = 0,
    chunk_base64: []const u8 = "",
    control: bool = false,
    /// Incarnation fence; required for abandonment, optional for finalization.
    created_index: u64 = 0,
    observed_progress: Digest = @splat(0),

    pub fn validate(self: Request) !void {
        if (self.action != .begin and self.control) return error.InvalidBatchRequest;
        if (self.action != .finalize and self.action != .abandon and self.created_index != 0) return error.InvalidBatchRequest;
        if (self.action != .abandon and !std.mem.allEqual(u8, &self.observed_progress, 0)) return error.InvalidBatchRequest;
        if (self.action == .abandon and (self.created_index == 0 or std.mem.allEqual(u8, &self.observed_progress, 0))) return error.InvalidBatchRequest;
        switch (self.action) {
            .begin => {
                if (self.ordinal != 0 or self.chunk_base64.len != 0 or
                    !std.mem.allEqual(u8, &self.manifest_root, 0)) return error.InvalidBatchRequest;
                const manifest = self.proposedManifest();
                try manifest.validate();
            },
            .chunk => {
                if (std.mem.allEqual(u8, &self.namespace, 0) or
                    std.mem.allEqual(u8, &self.publication_digest, 0) or
                    std.mem.allEqual(u8, &self.manifest_root, 0) or
                    !std.mem.allEqual(u8, &self.command_digest, 0) or
                    self.encoded_len != 0 or self.chunk_hashes.len != 0 or
                    self.chunk_base64.len == 0 or
                    self.chunk_base64.len > std.base64.standard.Encoder.calcSize(codec.chunk_bytes)) return error.InvalidBatchRequest;
            },
            .finalize, .abandon => {
                if (std.mem.allEqual(u8, &self.namespace, 0) or
                    std.mem.allEqual(u8, &self.publication_digest, 0) or
                    std.mem.allEqual(u8, &self.manifest_root, 0) or
                    !std.mem.allEqual(u8, &self.command_digest, 0) or
                    self.encoded_len != 0 or self.chunk_hashes.len != 0 or
                    self.ordinal != 0 or self.chunk_base64.len != 0) return error.InvalidBatchRequest;
            },
            .prune => {
                if (!std.mem.allEqual(u8, &self.namespace, 0) or
                    !std.mem.allEqual(u8, &self.publication_digest, 0) or
                    !std.mem.allEqual(u8, &self.manifest_root, 0) or
                    !std.mem.allEqual(u8, &self.command_digest, 0) or
                    self.encoded_len != 0 or self.chunk_hashes.len != 0 or
                    self.ordinal != 0 or self.chunk_base64.len != 0) return error.InvalidBatchRequest;
            },
        }
    }

    pub fn proposedManifest(self: Request) Manifest {
        return .{ .namespace = self.namespace, .publication_digest = self.publication_digest, .command_digest = self.command_digest, .encoded_len = self.encoded_len, .created_index = 0, .chunk_hashes = self.chunk_hashes, .control = self.control };
    }

    pub fn decodedChunkAlloc(self: Request, alloc: Allocator) ![]u8 {
        if (self.action != .chunk) return error.InvalidBatchRequest;
        try self.validate();
        const size = std.base64.standard.Decoder.calcSizeForSlice(self.chunk_base64) catch return error.InvalidBatchRequest;
        if (size == 0 or size > codec.chunk_bytes) return error.InvalidBatchRequest;
        const out = try alloc.alloc(u8, size);
        errdefer alloc.free(out);
        std.base64.standard.Decoder.decode(out, self.chunk_base64) catch return error.InvalidBatchRequest;
        return out;
    }
};

pub fn validateBatchRequest(request: anytype) !void {
    const control = request.artifact_publication_transport orelse return;
    try control.validate();
    const defaults: @TypeOf(request) = .{};
    inline for (@typeInfo(@TypeOf(request)).@"struct".field_names, @typeInfo(@TypeOf(request)).@"struct".field_types) |reflected_name, field_type| {
        if (comptime !std.mem.eql(u8, reflected_name, "artifact_publication_transport") and
            !std.mem.eql(u8, reflected_name, "sync_level") and !std.mem.eql(u8, reflected_name, "timestamp"))
        {
            if (comptime @typeInfo(field_type) == .pointer and @typeInfo(field_type).pointer.size == .slice) {
                if (@field(request, reflected_name).len != 0) return error.InvalidBatchRequest;
            } else if (!std.meta.eql(@field(request, reflected_name), @field(defaults, reflected_name))) return error.InvalidBatchRequest;
        }
    }
}

pub const Manifest = struct {
    namespace: Namespace,
    publication_digest: Digest,
    command_digest: Digest,
    encoded_len: u32,
    created_index: u64,
    chunk_hashes: []const Digest,
    control: bool = false,

    pub fn validate(self: Manifest) !void {
        if (self.control and reservedBytes(self.encoded_len) > control_staged_bytes / control_upload_slots) return error.InvalidBatchRequest;
        if (std.mem.allEqual(u8, &self.namespace, 0) or
            std.mem.allEqual(u8, &self.publication_digest, 0) or
            std.mem.allEqual(u8, &self.command_digest, 0) or
            self.encoded_len == 0 or
            self.encoded_len > codec.max_encoded_bytes or
            self.chunk_hashes.len == 0 or self.chunk_hashes.len > codec.max_chunks or
            self.chunk_hashes.len != (@as(usize, self.encoded_len) + codec.chunk_bytes - 1) / codec.chunk_bytes)
            return error.InvalidBatchRequest;
    }

    /// This is the immutable upload identity. The root binds the entire
    /// chunk list and command identity, not just a claimed checksum. The
    /// ordered first-stage position is deliberately excluded so a Begin
    /// retry after a lost reply has the same root.
    pub fn root(self: Manifest) Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update(manifest_domain);
        hash.update(&self.namespace);
        hash.update(&self.publication_digest);
        hash.update(&self.command_digest);
        hash.update(&.{@intFromBool(self.control)});
        var num: [8]u8 = undefined;
        std.mem.writeInt(u64, &num, self.encoded_len, .little);
        hash.update(&num);
        std.mem.writeInt(u64, &num, self.chunk_hashes.len, .little);
        hash.update(&num);
        for (self.chunk_hashes) |chunk_hash| hash.update(&chunk_hash);
        var result: Digest = undefined;
        hash.final(&result);
        return result;
    }

    pub fn chunkLen(self: Manifest, ordinal: usize) !usize {
        try self.validate();
        if (ordinal >= self.chunk_hashes.len) return error.InvalidBatchRequest;
        const start = ordinal * codec.chunk_bytes;
        return @min(codec.chunk_bytes, @as(usize, self.encoded_len) - start);
    }

    pub fn verifyChunk(self: Manifest, ordinal: usize, bytes: []const u8) !void {
        if (bytes.len != try self.chunkLen(ordinal)) return error.InvalidBatchRequest;
        const observed = chunkDigest(ordinal, bytes);
        if (!std.mem.eql(u8, &observed, &self.chunk_hashes[ordinal])) return error.ArtifactCatalogCorrupt;
    }
};

pub fn chunkDigest(ordinal: usize, bytes: []const u8) Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(chunk_domain);
    var num: [8]u8 = undefined;
    std.mem.writeInt(u64, &num, ordinal, .little);
    hash.update(&num);
    std.mem.writeInt(u64, &num, bytes.len, .little);
    hash.update(&num);
    hash.update(bytes);
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}

pub fn manifestKey(namespace: Namespace, publication_digest: Digest) [manifest_prefix.len + 24 + 32]u8 {
    var key: [manifest_prefix.len + 24 + 32]u8 = undefined;
    @memcpy(key[0..manifest_prefix.len], manifest_prefix);
    @memcpy(key[manifest_prefix.len..][0..24], &namespace);
    @memcpy(key[manifest_prefix.len + 24 ..], &publication_digest);
    return key;
}

pub fn chunkKey(namespace: Namespace, publication_digest: Digest, ordinal: usize) ![chunk_prefix.len + 24 + 32 + 2]u8 {
    if (ordinal >= codec.max_chunks) return error.InvalidBatchRequest;
    var key: [chunk_prefix.len + 24 + 32 + 2]u8 = undefined;
    @memcpy(key[0..chunk_prefix.len], chunk_prefix);
    @memcpy(key[chunk_prefix.len..][0..24], &namespace);
    @memcpy(key[chunk_prefix.len + 24 ..][0..32], &publication_digest);
    std.mem.writeInt(u16, key[chunk_prefix.len + 24 + 32 ..][0..2], @intCast(ordinal), .big);
    return key;
}

fn terminalKey(namespace: Namespace, publication_digest: Digest) [terminal_prefix.len + 24 + 32]u8 {
    var key: [terminal_prefix.len + 24 + 32]u8 = undefined;
    @memcpy(key[0..terminal_prefix.len], terminal_prefix);
    @memcpy(key[terminal_prefix.len..][0..24], &namespace);
    @memcpy(key[terminal_prefix.len + 24 ..], &publication_digest);
    return key;
}

pub const Terminal = struct { root: Digest, command_digest: Digest, decided_index: u64 };
/// Captured only after authenticating the complete immutable upload. The
/// first applied begin index fences prune/recreate ABA without rereading
/// large chunk bodies while holding the final writer lock.
pub const Finalization = struct { control: Request, created_index: u64 };
fn terminalDigest(bytes: []const u8) Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:artifact-publication-upload-terminal:v1:");
    hash.update(bytes);
    var out: Digest = undefined;
    hash.final(&out);
    return out;
}
fn decodeTerminal(raw: []const u8) !Terminal {
    if (raw.len != 4 + 32 + 32 + 8 + 32 or !std.mem.eql(u8, raw[0..4], terminal_magic)) return error.ArtifactCatalogCorrupt;
    const expected = terminalDigest(raw[0 .. raw.len - 32]);
    if (!std.mem.eql(u8, &expected, raw[raw.len - 32 ..])) return error.ArtifactCatalogCorrupt;
    const result: Terminal = .{ .root = raw[4..36].*, .command_digest = raw[36..68].*, .decided_index = std.mem.readInt(u64, raw[68..76], .little) };
    if (result.decided_index == 0) return error.ArtifactCatalogCorrupt;
    return result;
}

fn encodeTerminal(value: Terminal) [4 + 32 + 32 + 8 + 32]u8 {
    var raw: [4 + 32 + 32 + 8 + 32]u8 = undefined;
    @memcpy(raw[0..4], terminal_magic);
    @memcpy(raw[4..36], &value.root);
    @memcpy(raw[36..68], &value.command_digest);
    std.mem.writeInt(u64, raw[68..76], value.decided_index, .little);
    const checksum = terminalDigest(raw[0..76]);
    @memcpy(raw[76..108], &checksum);
    return raw;
}

/// The terminal record proves an ordered accept-or-reject decision occurred;
/// it does not claim which result. The existing publication receipt/rejection
/// remains the semantic outcome and must be read by callers.
pub fn terminal(txn: anytype, namespace: Namespace, publication_digest: Digest) !?Terminal {
    const key = terminalKey(namespace, publication_digest);
    const raw = (try optionalGet(txn, &key)) orelse return null;
    return try decodeTerminal(raw);
}

fn terminalSlotKey(slot: u64) [terminal_ring_prefix.len + 8]u8 {
    var key: [terminal_ring_prefix.len + 8]u8 = undefined;
    @memcpy(key[0..terminal_ring_prefix.len], terminal_ring_prefix);
    std.mem.writeInt(u64, key[terminal_ring_prefix.len..][0..8], slot, .big);
    return key;
}

/// Three bounded point writes and at most one point retirement, atomically
/// with the publication. Copy all borrowed identities before mutating the
/// transaction: some storage backends invalidate get() values on mutation.
fn stageTerminal(txn: anytype, namespace: Namespace, publication_digest: Digest, value: Terminal) !void {
    var sequence: u64 = 0;
    if (try optionalGet(txn, terminal_ring_head_key)) |raw| {
        if (raw.len != 40) return error.ArtifactCatalogCorrupt;
        const expected = terminalDigest(raw[0..8]);
        if (!std.mem.eql(u8, &expected, raw[8..40])) return error.ArtifactCatalogCorrupt;
        sequence = std.mem.readInt(u64, raw[0..8], .little);
    }
    const next = std.math.add(u64, sequence, 1) catch return error.ResourceLimitExceeded;
    const slot_key = terminalSlotKey(sequence % max_terminal_records);
    var previous_key: ?@TypeOf(terminalKey(namespace, publication_digest)) = null;
    if (try optionalGet(txn, &slot_key)) |raw| {
        if (sequence < max_terminal_records or raw.len != 96) return error.ArtifactCatalogCorrupt;
        const expected = terminalDigest(raw[0..64]);
        if (!std.mem.eql(u8, &expected, raw[64..96]) or
            std.mem.readInt(u64, raw[56..64], .little) != sequence - max_terminal_records)
            return error.ArtifactCatalogCorrupt;
        const previous_namespace: Namespace = raw[0..24].*;
        const previous_publication: Digest = raw[24..56].*;
        previous_key = terminalKey(previous_namespace, previous_publication);
        // A missing terminal means the ring and acknowledgement set disagree.
        _ = (try terminal(txn, previous_namespace, previous_publication)) orelse return error.ArtifactCatalogCorrupt;
    } else if (sequence >= max_terminal_records) return error.ArtifactCatalogCorrupt;
    var slot: [96]u8 = undefined;
    @memcpy(slot[0..24], &namespace);
    @memcpy(slot[24..56], &publication_digest);
    std.mem.writeInt(u64, slot[56..64], sequence, .little);
    const slot_checksum = terminalDigest(slot[0..64]);
    @memcpy(slot[64..96], &slot_checksum);
    var head: [40]u8 = undefined;
    std.mem.writeInt(u64, head[0..8], next, .little);
    const head_checksum = terminalDigest(head[0..8]);
    @memcpy(head[8..40], &head_checksum);
    const key = terminalKey(namespace, publication_digest);
    const raw = encodeTerminal(value);
    if (previous_key) |retired| try txn.delete(&retired);
    try txn.put(&key, &raw);
    try txn.put(&slot_key, &slot);
    try txn.put(terminal_ring_head_key, &head);
}

pub fn encodeManifestAlloc(alloc: Allocator, manifest: Manifest) ![]u8 {
    try manifest.validate();
    const len = manifest_header_len + manifest.chunk_hashes.len * 32;
    const out = try alloc.alloc(u8, len);
    @memcpy(out[0..4], manifest_magic);
    @memcpy(out[4..28], &manifest.namespace);
    @memcpy(out[28..60], &manifest.publication_digest);
    @memcpy(out[60..92], &manifest.command_digest);
    std.mem.writeInt(u32, out[92..96], manifest.encoded_len, .little);
    std.mem.writeInt(u16, out[96..98], @intCast(manifest.chunk_hashes.len), .little);
    std.mem.writeInt(u64, out[98..106], manifest.created_index, .little);
    const root = manifest.root();
    @memcpy(out[106..138], &root);
    out[138] = @intFromBool(manifest.control);
    for (manifest.chunk_hashes, 0..) |chunk_hash, i| @memcpy(out[manifest_header_len + i * 32 ..][0..32], &chunk_hash);
    return out;
}

/// The returned chunk hashes borrow `bytes`, which must outlive the Manifest.
pub fn decodeManifest(bytes: []const u8) !Manifest {
    if (bytes.len < manifest_header_len or bytes.len > max_manifest_len or !std.mem.eql(u8, bytes[0..4], manifest_magic) or bytes[138] > 1) return error.ArtifactCatalogCorrupt;
    const count = std.mem.readInt(u16, bytes[96..98], .little);
    if (count == 0 or count > codec.max_chunks or bytes.len != manifest_header_len + @as(usize, count) * 32) return error.ArtifactCatalogCorrupt;
    const hash_ptr: [*]const Digest = @ptrCast(bytes[manifest_header_len..].ptr);
    const hashes = hash_ptr[0..count];
    const manifest: Manifest = .{
        .namespace = bytes[4..28].*,
        .publication_digest = bytes[28..60].*,
        .command_digest = bytes[60..92].*,
        .encoded_len = std.mem.readInt(u32, bytes[92..96], .little),
        .created_index = std.mem.readInt(u64, bytes[98..106], .little),
        .chunk_hashes = hashes,
        .control = bytes[138] == 1,
    };
    manifest.validate() catch return error.ArtifactCatalogCorrupt;
    if (manifest.created_index == 0) return error.ArtifactCatalogCorrupt;
    const root = manifest.root();
    if (!std.mem.eql(u8, &root, bytes[106..138])) return error.ArtifactCatalogCorrupt;
    return manifest;
}

const QuotaEntry = struct {
    namespace: Namespace,
    publication_digest: Digest,
    root: Digest,
    encoded_len: u32,
    created_index: u64,
    control: bool,
};
const quota_entry_len = 24 + 32 + 32 + 4 + 8 + 1;
const quota_len = 4 + 1 + max_active_uploads * quota_entry_len + 32;
fn reservedBytes(encoded_len: u32) usize {
    const chunks = (@as(usize, encoded_len) + codec.chunk_bytes - 1) / codec.chunk_bytes;
    return @as(usize, encoded_len) + manifest_header_len + chunks * (32 + chunk_prefix.len + 24 + 32 + 2) + manifest_prefix.len + 24 + 32 + quota_len;
}
const Quota = struct {
    entries: [max_active_uploads]QuotaEntry = undefined,
    count: usize = 0,
    bytes: usize = 0,

    fn contains(self: *const Quota, manifest: Manifest) !bool {
        for (self.entries[0..self.count]) |entry| {
            if (!std.mem.eql(u8, &entry.namespace, &manifest.namespace) or
                !std.mem.eql(u8, &entry.publication_digest, &manifest.publication_digest)) continue;
            const expected_root = manifest.root();
            if (!std.mem.eql(u8, &entry.root, &expected_root) or
                entry.encoded_len != manifest.encoded_len or entry.control != manifest.control) return error.ArtifactPublicationUploadChanged;
            return true;
        }
        return false;
    }

    fn add(self: *Quota, manifest: Manifest) !void {
        if (try self.contains(manifest)) return;
        const charge = reservedBytes(manifest.encoded_len);
        if (self.count >= max_active_uploads or
            charge > max_staged_bytes - self.bytes) return error.ResourceLimitExceeded;
        if (!manifest.control) {
            var producer_count: usize = 0;
            var producer_bytes: usize = 0;
            for (self.entries[0..self.count]) |entry| if (!entry.control) {
                producer_count += 1;
                producer_bytes += reservedBytes(entry.encoded_len);
            };
            if (producer_count >= max_producer_uploads or charge > (max_staged_bytes - control_staged_bytes) -| producer_bytes) return error.ResourceLimitExceeded;
        }
        self.entries[self.count] = .{ .namespace = manifest.namespace, .publication_digest = manifest.publication_digest, .root = manifest.root(), .encoded_len = manifest.encoded_len, .created_index = manifest.created_index, .control = manifest.control };
        self.count += 1;
        self.bytes += charge;
    }

    fn remove(self: *Quota, manifest: Manifest) !void {
        for (self.entries[0..self.count], 0..) |entry, i| {
            if (!std.mem.eql(u8, &entry.namespace, &manifest.namespace) or
                !std.mem.eql(u8, &entry.publication_digest, &manifest.publication_digest)) continue;
            const expected_root = manifest.root();
            if (!std.mem.eql(u8, &entry.root, &expected_root) or entry.control != manifest.control) return error.ArtifactPublicationUploadChanged;
            self.bytes -= reservedBytes(entry.encoded_len);
            const remaining = self.count - i - 1;
            std.mem.copyForwards(QuotaEntry, self.entries[i .. i + remaining], self.entries[i + 1 ..][0..remaining]);
            self.count -= 1;
            return;
        }
        return error.ArtifactCatalogCorrupt;
    }
};

fn quotaDigest(bytes: []const u8) Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:artifact-publication-upload-quota:v1:");
    hash.update(bytes);
    var out: Digest = undefined;
    hash.final(&out);
    return out;
}

fn decodeQuota(raw: ?[]const u8) !Quota {
    const bytes = raw orelse return .{};
    if (bytes.len != quota_len or !std.mem.eql(u8, bytes[0..4], quota_magic) or bytes[4] > max_active_uploads) return error.ArtifactCatalogCorrupt;
    const expected = quotaDigest(bytes[0 .. bytes.len - 32]);
    if (!std.mem.eql(u8, &expected, bytes[bytes.len - 32 ..])) return error.ArtifactCatalogCorrupt;
    var quota: Quota = .{};
    var producer_count: usize = 0;
    var producer_bytes: usize = 0;
    for (0..bytes[4]) |i| {
        const entry_raw = bytes[5 + i * quota_entry_len ..][0..quota_entry_len];
        if (entry_raw[100] > 1) return error.ArtifactCatalogCorrupt;
        const entry: QuotaEntry = .{ .namespace = entry_raw[0..24].*, .publication_digest = entry_raw[24..56].*, .root = entry_raw[56..88].*, .encoded_len = std.mem.readInt(u32, entry_raw[88..92], .little), .created_index = std.mem.readInt(u64, entry_raw[92..100], .little), .control = entry_raw[100] == 1 };
        if (entry.encoded_len == 0 or entry.encoded_len > codec.max_encoded_bytes or entry.created_index == 0 or
            reservedBytes(entry.encoded_len) > max_staged_bytes - quota.bytes) return error.ArtifactCatalogCorrupt;
        if (entry.control) {
            if (reservedBytes(entry.encoded_len) > control_staged_bytes / control_upload_slots) return error.ArtifactCatalogCorrupt;
        } else {
            producer_count += 1;
            producer_bytes += reservedBytes(entry.encoded_len);
            if (producer_count > max_producer_uploads or producer_bytes > max_staged_bytes - control_staged_bytes) return error.ArtifactCatalogCorrupt;
        }
        for (quota.entries[0..quota.count]) |previous| if (std.mem.eql(u8, &previous.namespace, &entry.namespace) and std.mem.eql(u8, &previous.publication_digest, &entry.publication_digest)) return error.ArtifactCatalogCorrupt;
        quota.entries[quota.count] = entry;
        quota.count += 1;
        quota.bytes += reservedBytes(entry.encoded_len);
    }
    if (!std.mem.allEqual(u8, bytes[5 + @as(usize, bytes[4]) * quota_entry_len .. quota_len - 32], 0)) return error.ArtifactCatalogCorrupt;
    return quota;
}

fn encodeQuota(quota: Quota) [quota_len]u8 {
    var bytes: [quota_len]u8 = @splat(0);
    @memcpy(bytes[0..4], quota_magic);
    bytes[4] = @intCast(quota.count);
    for (quota.entries[0..quota.count], 0..) |entry, i| {
        const raw = bytes[5 + i * quota_entry_len ..][0..quota_entry_len];
        @memcpy(raw[0..24], &entry.namespace);
        @memcpy(raw[24..56], &entry.publication_digest);
        @memcpy(raw[56..88], &entry.root);
        std.mem.writeInt(u32, raw[88..92], entry.encoded_len, .little);
        std.mem.writeInt(u64, raw[92..100], entry.created_index, .little);
        raw[100] = @intFromBool(entry.control);
    }
    const checksum = quotaDigest(bytes[0 .. quota_len - 32]);
    @memcpy(bytes[quota_len - 32 ..], &checksum);
    return bytes;
}

test "ordered artifact inventory upload quota preserves authenticated control headroom after restart" {
    const hashes = [_]Digest{@splat(4)};
    var producer: Manifest = .{ .namespace = @splat(1), .publication_digest = @splat(2), .command_digest = @splat(3), .encoded_len = 1, .created_index = 1, .chunk_hashes = &hashes };
    var quota: Quota = .{};
    for (0..max_producer_uploads) |i| {
        producer.publication_digest[0] = @intCast(i);
        try quota.add(producer);
    }
    producer.publication_digest[0] = 90;
    try std.testing.expectError(error.ResourceLimitExceeded, quota.add(producer));
    const encoded = encodeQuota(quota);
    var reopened = try decodeQuota(&encoded);
    var control = producer;
    control.control = true;
    try std.testing.expect(!std.mem.eql(u8, &producer.root(), &control.root()));
    for (0..control_upload_slots) |i| {
        control.publication_digest[0] = @intCast(100 + i);
        try reopened.add(control);
    }
    control.publication_digest[0] = 110;
    try std.testing.expectError(error.ResourceLimitExceeded, reopened.add(control));
    const full_bytes = encodeQuota(reopened);
    reopened = try decodeQuota(&full_bytes);
    control.publication_digest[0] = 100;
    try reopened.remove(control);
    try std.testing.expectError(error.ResourceLimitExceeded, reopened.add(producer));
    try reopened.add(control);
    const manifest_bytes = try encodeManifestAlloc(std.testing.allocator, control);
    defer std.testing.allocator.free(manifest_bytes);
    try std.testing.expectEqualDeep(control, try decodeManifest(manifest_bytes));
    manifest_bytes[138] = 0;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, decodeManifest(manifest_bytes));
}

fn optionalGet(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}

fn progressKey(namespace: Namespace, digest: Digest) [progress_prefix.len + 56]u8 {
    var result: [progress_prefix.len + 56]u8 = undefined;
    @memcpy(result[0..progress_prefix.len], progress_prefix);
    @memcpy(result[progress_prefix.len..][0..24], &namespace);
    @memcpy(result[progress_prefix.len + 24 ..], &digest);
    return result;
}

// This bounded bitmap is discovery metadata, never authority to publish.
// Finalization still authenticates every chunk and rechecks the incarnation.
const Progress = struct {
    const len = 92;
    received: u128 = 0,

    fn encode(self: Progress, manifest: Manifest) [len]u8 {
        var raw: [len]u8 = undefined;
        @memcpy(raw[0..4], "APP1");
        std.mem.writeInt(u64, raw[4..12], manifest.created_index, .little);
        @memcpy(raw[12..44], &manifest.root());
        std.mem.writeInt(u128, raw[44..60], self.received, .little);
        std.crypto.hash.Blake3.hash(raw[0..60], raw[60..92], .{});
        return raw;
    }

    fn load(txn: anytype, manifest: Manifest) !Progress {
        const raw = (try optionalGet(txn, &progressKey(manifest.namespace, manifest.publication_digest))) orelse return error.ArtifactCatalogCorrupt;
        if (raw.len != len or !std.mem.eql(u8, raw[0..4], "APP1") or
            std.mem.readInt(u64, raw[4..12], .little) != manifest.created_index or
            !std.mem.eql(u8, raw[12..44], &manifest.root())) return error.ArtifactCatalogCorrupt;
        var checksum: Digest = undefined;
        std.crypto.hash.Blake3.hash(raw[0..60], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, raw[60..92])) return error.ArtifactCatalogCorrupt;
        const received = std.mem.readInt(u128, raw[44..60], .little);
        if (received & ~mask(manifest) != 0) return error.ArtifactCatalogCorrupt;
        return .{ .received = received };
    }

    fn mask(manifest: Manifest) u128 {
        comptime std.debug.assert(codec.max_chunks < 128);
        return (@as(u128, 1) << @as(u7, @intCast(manifest.chunk_hashes.len))) - 1;
    }
};

/// Tiny owned queue envelope. It borrows no DB, manifest, or provider output.
/// The ordered writer rechecks the incarnation and progress before acting.
pub const RecoveryHint = struct {
    namespace: Namespace,
    publication_digest: Digest,
    root: Digest,
    created_index: u64,
    action: enum { finalize, abandon } = .finalize,
    observed_progress: Digest = @splat(0),
    pub const encoded_len = 133;

    pub fn encode(self: RecoveryHint) [encoded_len]u8 {
        var raw: [encoded_len]u8 = undefined;
        @memcpy(raw[0..4], "APR1");
        @memcpy(raw[4..28], &self.namespace);
        @memcpy(raw[28..60], &self.publication_digest);
        @memcpy(raw[60..92], &self.root);
        std.mem.writeInt(u64, raw[92..100], self.created_index, .little);
        raw[100] = @backingInt(self.action);
        @memcpy(raw[101..133], &self.observed_progress);
        return raw;
    }

    pub fn decode(raw: []const u8) !?RecoveryHint {
        if (raw.len < 4 or !std.mem.eql(u8, raw[0..4], "APR1")) return null;
        if (raw.len != encoded_len) return error.InvalidBatchRequest;
        const hint: RecoveryHint = .{ .namespace = raw[4..28].*, .publication_digest = raw[28..60].*, .root = raw[60..92].*, .created_index = std.mem.readInt(u64, raw[92..100], .little), .action = std.enums.fromInt(@FieldType(RecoveryHint, "action"), raw[100]) orelse return error.InvalidBatchRequest, .observed_progress = raw[101..133].* };
        if (hint.created_index == 0) return error.InvalidBatchRequest;
        try hint.request().validate();
        return hint;
    }

    pub fn request(self: RecoveryHint) Request {
        return .{ .action = if (self.action == .finalize) .finalize else .abandon, .namespace = self.namespace, .publication_digest = self.publication_digest, .manifest_root = self.root, .created_index = self.created_index, .observed_progress = self.observed_progress };
    }
};

/// At most eight manifest/progress pairs, no payload reads or allocations.
/// Wrap the ordered begin-index cursor for fairness when a ready upload is
/// temporarily blocked on coverage. The cursor is local scheduling state,
/// not a durable completion watermark; restart safely starts at zero.
pub fn nextRecovery(txn: anytype, namespace: Namespace, after: u64) !?RecoveryHint {
    const inventory = try recoveryInventory(txn, namespace);
    return inventory.nextReady(after);
}

pub const RecoveryInventory = struct {
    entries: [max_active_uploads]Entry = undefined,
    count: usize = 0,
    pub const Entry = struct { hint: RecoveryHint, complete: bool, progress: Digest };

    pub fn nextReady(self: RecoveryInventory, after: u64) ?RecoveryHint {
        var first: ?RecoveryHint = null;
        var next: ?RecoveryHint = null;
        for (self.entries[0..self.count]) |entry| {
            if (!entry.complete) continue;
            const hint = entry.hint;
            if (first == null or hint.created_index < first.?.created_index) first = hint;
            if (hint.created_index > after and (next == null or hint.created_index < next.?.created_index)) next = hint;
        }
        return next orelse first;
    }
};

pub fn recoveryInventory(txn: anytype, namespace: Namespace) !RecoveryInventory {
    const quota = try decodeQuota(try optionalGet(txn, quota_key));
    var result: RecoveryInventory = .{};
    for (quota.entries[0..quota.count]) |entry| {
        if (!std.mem.eql(u8, &namespace, &entry.namespace)) continue;
        const raw = (try optionalGet(txn, &manifestKey(namespace, entry.publication_digest))) orelse return error.ArtifactCatalogCorrupt;
        const manifest = try decodeManifest(raw);
        if (manifest.created_index != entry.created_index or manifest.encoded_len != entry.encoded_len or manifest.control != entry.control or
            !std.mem.eql(u8, &manifest.namespace, &entry.namespace) or !std.mem.eql(u8, &manifest.publication_digest, &entry.publication_digest) or
            !std.mem.eql(u8, &manifest.root(), &entry.root)) return error.ArtifactCatalogCorrupt;
        const progress = try Progress.load(txn, manifest);
        const hint: RecoveryHint = .{ .namespace = namespace, .publication_digest = entry.publication_digest, .root = entry.root, .created_index = entry.created_index };
        const encoded = progress.encode(manifest);
        result.entries[result.count] = .{ .hint = hint, .complete = progress.received == Progress.mask(manifest), .progress = encoded[60..92].* };
        result.count += 1;
    }
    return result;
}

/// No accepted receipt, rejection, or completion credit is created here.
/// Losing incomplete transport bytes leaves the original durable producer
/// obligation pending. A racing chunk, completed upload, or reincarnation is
/// an inert ordered outcome, never a failed committed Raft entry.
pub fn stageAbandon(txn: anytype, request: Request) !bool {
    try request.validate();
    if (request.action != .abandon) return error.InvalidBatchRequest;
    const raw = (try optionalGet(txn, &manifestKey(request.namespace, request.publication_digest))) orelse return false;
    const manifest = try decodeManifest(raw);
    if (manifest.created_index != request.created_index or !std.mem.eql(u8, &manifest.root(), &request.manifest_root)) return false;
    const progress = try Progress.load(txn, manifest);
    const encoded = progress.encode(manifest);
    if (progress.received == Progress.mask(manifest) or !std.mem.eql(u8, encoded[60..92], &request.observed_progress)) return false;
    var quota = try decodeQuota(try optionalGet(txn, quota_key));
    try quota.remove(manifest);
    const chunk_count = manifest.chunk_hashes.len;
    const quota_raw = encodeQuota(quota);
    for (0..chunk_count) |ordinal| try txn.delete(&try chunkKey(request.namespace, request.publication_digest, ordinal));
    try txn.delete(&manifestKey(request.namespace, request.publication_digest));
    try txn.delete(&progressKey(request.namespace, request.publication_digest));
    try txn.put(quota_key, &quota_raw);
    return true;
}

/// Called inside the ordered stage-entry write transaction. The first stage
/// reserves quota; an exact replay is inert, and a changed upload for the
/// same publication is never permitted to replace already replicated bytes.
pub fn stageBegin(alloc: Allocator, txn: anytype, proposed: Manifest, applied_index: u64) !void {
    if (applied_index == 0 or proposed.created_index != 0) return error.InvalidBatchRequest;
    try proposed.validate();
    var manifest = proposed;
    manifest.created_index = applied_index;
    if (try terminal(txn, manifest.namespace, manifest.publication_digest)) |decided| {
        const current_root = manifest.root();
        if (!std.mem.eql(u8, &decided.root, &current_root) or
            !std.mem.eql(u8, &decided.command_digest, &manifest.command_digest)) return error.ArtifactPublicationUploadChanged;
        return;
    }
    const key = manifestKey(manifest.namespace, manifest.publication_digest);
    if (try optionalGet(txn, &key)) |raw| {
        const previous = try decodeManifest(raw);
        const previous_root = previous.root();
        const current_root = manifest.root();
        if (!std.mem.eql(u8, &previous_root, &current_root)) return error.ArtifactPublicationUploadChanged;
        return;
    }
    var quota = try decodeQuota(try optionalGet(txn, quota_key));
    try quota.add(manifest);
    const raw = try encodeManifestAlloc(alloc, manifest);
    defer alloc.free(raw);
    const quota_raw = encodeQuota(quota);
    const progress = (Progress{}).encode(manifest);
    try txn.put(&key, raw);
    try txn.put(&progressKey(manifest.namespace, manifest.publication_digest), &progress);
    try txn.put(quota_key, &quota_raw);
}

/// Each chunk is one bounded ordered entry. A duplicate must contain exactly
/// the same authenticated bytes; a lost reply cannot allocate extra quota.
pub fn stageChunk(txn: anytype, namespace: Namespace, publication_digest: Digest, root: Digest, ordinal: usize, bytes: []const u8) !void {
    if (try terminal(txn, namespace, publication_digest)) |decided| {
        if (!std.mem.eql(u8, &decided.root, &root)) return error.ArtifactPublicationUploadChanged;
        return;
    }
    const manifest_key = manifestKey(namespace, publication_digest);
    const manifest = try decodeManifest((try optionalGet(txn, &manifest_key)) orelse return error.ArtifactPublicationUploadMissing);
    const actual_root = manifest.root();
    if (!std.mem.eql(u8, &actual_root, &root)) return error.ArtifactPublicationUploadChanged;
    try manifest.verifyChunk(ordinal, bytes);
    var progress = try Progress.load(txn, manifest);
    const bit = @as(u128, 1) << @as(u7, @intCast(ordinal));
    const key = try chunkKey(namespace, publication_digest, ordinal);
    if (try optionalGet(txn, &key)) |previous| {
        if (!std.mem.eql(u8, previous, bytes)) return error.ArtifactPublicationUploadChanged;
        if (progress.received & bit == 0) return error.ArtifactCatalogCorrupt;
        return;
    }
    if (progress.received & bit != 0) return error.ArtifactCatalogCorrupt;
    progress.received |= bit;
    const progress_raw = progress.encode(manifest);
    try txn.put(&key, bytes);
    try txn.put(&progressKey(namespace, publication_digest), &progress_raw);
}

/// Assembles only after every point-addressed chunk was verified. This is an
/// off-lock preparation step; final apply rechecks the immutable upload root
/// under the writer lock before committing the semantic publication.
pub fn assembleAlloc(alloc: Allocator, txn: anytype, namespace: Namespace, publication_digest: Digest, root: Digest) !codec.Decoded {
    if (try terminal(txn, namespace, publication_digest) != null) return error.ArtifactPublicationAlreadyDecided;
    const manifest_key = manifestKey(namespace, publication_digest);
    const manifest = try decodeManifest((try optionalGet(txn, &manifest_key)) orelse return error.ArtifactPublicationUploadMissing);
    const actual_root = manifest.root();
    if (!std.mem.eql(u8, &actual_root, &root)) return error.ArtifactPublicationUploadChanged;
    const encoded = try alloc.alloc(u8, manifest.encoded_len);
    var encoded_owned = true;
    defer if (encoded_owned) alloc.free(encoded);
    var offset: usize = 0;
    for (0..manifest.chunk_hashes.len) |ordinal| {
        const key = try chunkKey(namespace, publication_digest, ordinal);
        const chunk = (try optionalGet(txn, &key)) orelse return error.ArtifactPublicationUploadIncomplete;
        try manifest.verifyChunk(ordinal, chunk);
        @memcpy(encoded[offset..][0..chunk.len], chunk);
        offset += chunk.len;
    }
    if (offset != encoded.len) return error.ArtifactCatalogCorrupt;
    encoded_owned = false; // decodeOwned consumes on either path.
    var decoded = try codec.decodeOwned(alloc, encoded);
    errdefer decoded.deinit();
    // The codec has already recomputed and verified this digest. Reuse it
    // rather than scanning the entire assembled command a second time.
    const decoded_digest = decoded.command.publication_digest;
    if (manifest.control != (decoded.command.mode != .publish)) return error.ArtifactPublicationUploadChanged;
    if (!std.mem.eql(u8, &decoded.command.namespace, &namespace) or
        !std.mem.eql(u8, &decoded.command.publication_digest, &publication_digest) or
        !std.mem.eql(u8, &decoded_digest, &manifest.command_digest)) return error.ArtifactPublicationUploadChanged;
    return decoded;
}

/// Called in the *same* write transaction as accepted publication effects or
/// a durable rejection. Never invoke it after an ambiguous commit response.
/// All source receipts, terminal identity, quota release and chunk retirement
/// then share one atomic decision boundary.
pub fn stageCompletion(txn: anytype, namespace: Namespace, publication_digest: Digest, root: Digest, created_index: u64, decided_index: u64) !void {
    if (created_index == 0 or decided_index == 0) return error.InvalidBatchRequest;
    if (try terminal(txn, namespace, publication_digest)) |previous| {
        if (!std.mem.eql(u8, &previous.root, &root)) return error.ArtifactPublicationUploadChanged;
        return;
    }
    const manifest_key = manifestKey(namespace, publication_digest);
    const manifest = try decodeManifest((try optionalGet(txn, &manifest_key)) orelse return error.ArtifactPublicationUploadMissing);
    const actual_root = manifest.root();
    if (!std.mem.eql(u8, &actual_root, &root)) return error.ArtifactPublicationUploadChanged;
    if (manifest.created_index != created_index) return error.ArtifactPublicationUploadChanged;
    // Chunks are immutable in this incarnation; the only removal path also
    // removes its manifest atomically. Rechecking the root AND incarnation
    // therefore certifies the off-lock assembly without 68 MiB of point IO.
    var quota = try decodeQuota(try optionalGet(txn, quota_key));
    try quota.remove(manifest);
    const command_digest = manifest.command_digest;
    const chunk_count = manifest.chunk_hashes.len;
    const quota_raw = encodeQuota(quota);
    try stageTerminal(txn, namespace, publication_digest, .{ .root = root, .command_digest = command_digest, .decided_index = decided_index });
    try txn.put(quota_key, &quota_raw);
    for (0..chunk_count) |ordinal| {
        const key = try chunkKey(namespace, publication_digest, ordinal);
        try txn.delete(&key);
    }
    try txn.delete(&manifest_key);
    try txn.delete(&progressKey(namespace, publication_digest));
}

/// A separate ordered maintenance entry reclaims at most one abandoned
/// upload. Its age is measured exclusively in applied-entry indexes; clocks
/// and process restarts cannot affect replicated application.
pub fn pruneOneExpired(txn: anytype, applied_index: u64) !bool {
    if (applied_index == 0) return error.InvalidBatchRequest;
    var quota = try decodeQuota(try optionalGet(txn, quota_key));
    for (quota.entries[0..quota.count]) |entry| {
        if (applied_index < entry.created_index or applied_index - entry.created_index < max_upload_age_entries) continue;
        const key = manifestKey(entry.namespace, entry.publication_digest);
        const manifest = try decodeManifest((try optionalGet(txn, &key)) orelse return error.ArtifactCatalogCorrupt);
        const actual_root = manifest.root();
        if (!std.mem.eql(u8, &entry.root, &actual_root) or entry.created_index != manifest.created_index) return error.ArtifactCatalogCorrupt;
        // The manifest borrows transaction bytes. Consume its hashes before
        // deleting any records: a backend may invalidate borrowed values on
        // mutation (and deleting the manifest necessarily retires them).
        try quota.remove(manifest);
        for (0..manifest.chunk_hashes.len) |ordinal| {
            const chunk_key = try chunkKey(entry.namespace, entry.publication_digest, ordinal);
            try txn.delete(&chunk_key);
        }
        try txn.delete(&key);
        try txn.delete(&progressKey(entry.namespace, entry.publication_digest));
        const quota_raw = encodeQuota(quota);
        try txn.put(quota_key, &quota_raw);
        return true;
    }
    return false;
}

test "artifact publication upload manifest binds exact chunks and command" {
    const alloc = std.testing.allocator;
    const a = chunkDigest(0, "first");
    const hashes = [_]Digest{a};
    const manifest: Manifest = .{ .namespace = @splat(1), .publication_digest = @splat(2), .command_digest = @splat(3), .encoded_len = 5, .created_index = 9, .chunk_hashes = &hashes };
    const bytes = try encodeManifestAlloc(alloc, manifest);
    defer alloc.free(bytes);
    const decoded = try decodeManifest(bytes);
    try std.testing.expectEqualDeep(manifest.root(), decoded.root());
    try decoded.verifyChunk(0, "first");
    try std.testing.expectError(error.ArtifactCatalogCorrupt, decoded.verifyChunk(0, "other"));
    bytes[60] ^= 1;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, decodeManifest(bytes));
}

test "artifact publication upload controls are single-purpose and bound to one bounded chunk" {
    const alloc = std.testing.allocator;
    const hash = chunkDigest(0, "data");
    const hashes = [_]Digest{hash};
    const begin: Request = .{ .action = .begin, .namespace = @splat(1), .publication_digest = @splat(2), .command_digest = @splat(3), .encoded_len = 4, .chunk_hashes = &hashes };
    try begin.validate();
    const root = begin.proposedManifest().root();
    const chunk: Request = .{ .action = .chunk, .namespace = begin.namespace, .publication_digest = begin.publication_digest, .manifest_root = root, .chunk_base64 = "ZGF0YQ==" };
    const decoded = try chunk.decodedChunkAlloc(alloc);
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings("data", decoded);
    var mixed = chunk;
    mixed.encoded_len = 4;
    try std.testing.expectError(error.InvalidBatchRequest, mixed.validate());
    try std.testing.expectError(error.InvalidBatchRequest, (Request{ .action = .prune, .namespace = @splat(1) }).validate());
}

test "artifact publication upload stages exact retries and atomically retires quota" {
    const Fake = struct {
        alloc: Allocator,
        map: std.StringHashMapUnmanaged([]u8) = .empty,
        deny_payload_reads: bool = false,
        reads: usize = 0,

        pub fn deinit(self: *@This()) void {
            var it = self.map.iterator();
            while (it.next()) |entry| {
                self.alloc.free(entry.key_ptr.*);
                self.alloc.free(entry.value_ptr.*);
            }
            self.map.deinit(self.alloc);
        }
        fn get(self: *@This(), key: []const u8) ![]const u8 {
            self.reads += 1;
            if (self.deny_payload_reads and std.mem.startsWith(u8, key, chunk_prefix)) return error.UnexpectedPayloadRead;
            return self.map.get(key) orelse error.NotFound;
        }
        fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            const copy_key = try self.alloc.dupe(u8, key);
            errdefer self.alloc.free(copy_key);
            const copy_value = try self.alloc.dupe(u8, value);
            errdefer self.alloc.free(copy_value);
            const slot = try self.map.getOrPut(self.alloc, copy_key);
            if (slot.found_existing) {
                self.alloc.free(copy_key);
                self.alloc.free(slot.value_ptr.*);
            }
            slot.value_ptr.* = copy_value;
        }
        fn delete(self: *@This(), key: []const u8) !void {
            if (self.map.fetchRemove(key)) |entry| {
                self.alloc.free(entry.key);
                self.alloc.free(entry.value);
            }
        }
    };
    const alloc = std.testing.allocator;
    var txn: Fake = .{ .alloc = alloc };
    defer txn.deinit();
    const hash = chunkDigest(0, "bytes");
    const hashes = [_]Digest{hash};
    const proposal: Manifest = .{ .namespace = @splat(1), .publication_digest = @splat(2), .command_digest = @splat(3), .encoded_len = 5, .created_index = 0, .chunk_hashes = &hashes };
    try stageBegin(alloc, &txn, proposal, 7);
    try stageBegin(alloc, &txn, proposal, 8); // lost first reply
    try std.testing.expect((try nextRecovery(&txn, proposal.namespace, 0)) == null);
    const stored_key = manifestKey(proposal.namespace, proposal.publication_digest);
    const stored = try decodeManifest(try txn.get(&stored_key));
    try std.testing.expectEqual(@as(u64, 7), stored.created_index);
    try stageChunk(&txn, proposal.namespace, proposal.publication_digest, proposal.root(), 0, "bytes");
    try stageChunk(&txn, proposal.namespace, proposal.publication_digest, proposal.root(), 0, "bytes");
    txn.deny_payload_reads = true;
    const recovery = (try nextRecovery(&txn, proposal.namespace, 0)).?;
    txn.deny_payload_reads = false;
    try std.testing.expectEqual(@as(u64, 7), recovery.created_index);
    const hint_bytes = recovery.encode();
    try std.testing.expectEqualDeep(recovery, (try RecoveryHint.decode(&hint_bytes)).?);
    try std.testing.expectError(error.InvalidBatchRequest, RecoveryHint.decode(hint_bytes[0..99]));
    try std.testing.expect((try RecoveryHint.decode("APB1")) == null);
    try std.testing.expectError(error.ArtifactCatalogCorrupt, stageChunk(&txn, proposal.namespace, proposal.publication_digest, proposal.root(), 0, "other"));
    try std.testing.expectEqual(reservedBytes(5), (try decodeQuota(try optionalGet(&txn, quota_key))).bytes);
    try std.testing.expectError(error.ArtifactPublicationUploadChanged, stageCompletion(&txn, proposal.namespace, proposal.publication_digest, proposal.root(), 8, 9));
    try stageCompletion(&txn, proposal.namespace, proposal.publication_digest, proposal.root(), 7, 9);
    try std.testing.expectEqual(@as(usize, 0), (try decodeQuota(try optionalGet(&txn, quota_key))).bytes);
    try std.testing.expect((try optionalGet(&txn, &stored_key)) == null);
    try std.testing.expect((try optionalGet(&txn, &progressKey(proposal.namespace, proposal.publication_digest))) == null);
    try std.testing.expect((try terminal(&txn, proposal.namespace, proposal.publication_digest)) != null);
    try stageBegin(alloc, &txn, proposal, 10); // terminal duplicate
    try stageChunk(&txn, proposal.namespace, proposal.publication_digest, proposal.root(), 0, "bytes");
    try stageCompletion(&txn, proposal.namespace, proposal.publication_digest, proposal.root(), 7, 11);
    const head = try txn.get(terminal_ring_head_key);
    try std.testing.expectEqual(@as(u64, 1), std.mem.readInt(u64, head[0..8], .little));

    // More than one full ring of distinct decisions, including namespace
    // changes, bounds acknowledgement storage without scanning any rows.
    for (0..max_terminal_records + 7) |i| {
        var next = proposal;
        next.namespace = @splat(4);
        std.mem.writeInt(u64, next.publication_digest[0..8], i, .little);
        const index: u64 = 20 + @as(u64, @intCast(i)) * 3;
        try stageBegin(alloc, &txn, next, index);
        try stageChunk(&txn, next.namespace, next.publication_digest, next.root(), 0, "bytes");
        try stageCompletion(&txn, next.namespace, next.publication_digest, next.root(), index, index + 1);
        try std.testing.expect((try terminal(&txn, next.namespace, next.publication_digest)) != null);
        try std.testing.expect(txn.map.count() <= 2 * max_terminal_records + 2);
    }
    try std.testing.expect((try terminal(&txn, proposal.namespace, proposal.publication_digest)) == null);
    // Cache eviction permits retransmission, not a fabricated success. The
    // caller must assemble and obtain a semantic decision again.
    try stageBegin(alloc, &txn, proposal, 10_000);
    try std.testing.expect((try optionalGet(&txn, &stored_key)) != null);
    try std.testing.expect((try terminal(&txn, proposal.namespace, proposal.publication_digest)) == null);
    try stageChunk(&txn, proposal.namespace, proposal.publication_digest, proposal.root(), 0, "bytes");
    try stageCompletion(&txn, proposal.namespace, proposal.publication_digest, proposal.root(), 10_000, 10_001);
    try std.testing.expect(txn.map.count() <= 2 * max_terminal_records + 2);

    // Fair recovery skips an incomplete upload and wraps around a temporarily
    // blocked ready upload. Discovery performs only fixed-size metadata IO.
    var ready = proposal;
    ready.namespace = @splat(9);
    for (0..3) |i| {
        ready.publication_digest[0] = @intCast(i + 10);
        try stageBegin(alloc, &txn, ready, 20_000 + i);
        if (i != 1) try stageChunk(&txn, ready.namespace, ready.publication_digest, ready.root(), 0, "bytes");
    }
    txn.deny_payload_reads = true;
    txn.reads = 0;
    try std.testing.expectEqual(@as(u64, 20_000), (try nextRecovery(&txn, ready.namespace, 0)).?.created_index);
    try std.testing.expectEqual(@as(usize, 7), txn.reads);
    try std.testing.expectEqual(@as(u64, 20_002), (try nextRecovery(&txn, ready.namespace, 20_000)).?.created_index);
    try std.testing.expectEqual(@as(u64, 20_000), (try nextRecovery(&txn, ready.namespace, 20_002)).?.created_index);
    const inventory = try recoveryInventory(&txn, ready.namespace);
    const abandoned = blk: {
        for (inventory.entries[0..inventory.count]) |entry| {
            if (entry.hint.created_index != 20_001) continue;
            var hint = entry.hint;
            hint.action = .abandon;
            hint.observed_progress = entry.progress;
            break :blk hint;
        }
        return error.TestUnexpectedResult;
    };
    const abandonment_bytes = abandoned.encode();
    try std.testing.expectEqualDeep(abandoned, (try RecoveryHint.decode(&abandonment_bytes)).?);
    // Clocks and process memory cannot grant durable retirement authority.
    var changed = abandoned.request();
    changed.observed_progress[0] ^= 1;
    try std.testing.expect(!try stageAbandon(&txn, changed));
    try std.testing.expect(try stageAbandon(&txn, abandoned.request()));
    try std.testing.expect(!try stageAbandon(&txn, abandoned.request()));
    try std.testing.expect((try terminal(&txn, abandoned.namespace, abandoned.publication_digest)) == null);
    try std.testing.expectEqual(@as(usize, 2), (try recoveryInventory(&txn, ready.namespace)).count);
    var replacement = proposal;
    replacement.namespace = abandoned.namespace;
    replacement.publication_digest = abandoned.publication_digest;
    try stageBegin(alloc, &txn, replacement, 21_000);
    try std.testing.expect(!try stageAbandon(&txn, abandoned.request()));
    const replacing = try recoveryInventory(&txn, ready.namespace);
    const retire_replacement = blk: {
        for (replacing.entries[0..replacing.count]) |entry| {
            if (entry.hint.created_index != 21_000) continue;
            var hint = entry.hint;
            hint.action = .abandon;
            hint.observed_progress = entry.progress;
            break :blk hint;
        }
        return error.TestUnexpectedResult;
    };
    txn.deny_payload_reads = false;
    try stageChunk(&txn, replacement.namespace, replacement.publication_digest, replacement.root(), 0, "bytes");
    txn.deny_payload_reads = true;
    try std.testing.expect(!try stageAbandon(&txn, retire_replacement.request())); // final chunk won
    try std.testing.expectEqual(@as(usize, 3), (try recoveryInventory(&txn, ready.namespace)).count);
    const manifest = try decodeManifest(try txn.get(&manifestKey(ready.namespace, ready.publication_digest)));
    const corrupt = (Progress{ .received = @as(u128, 1) << 100 }).encode(manifest);
    try txn.put(&progressKey(ready.namespace, ready.publication_digest), &corrupt);
    try std.testing.expectError(error.ArtifactCatalogCorrupt, nextRecovery(&txn, ready.namespace, 0));
}

test "artifact publication upload prunes only by ordered age" {
    const Fake = struct {
        alloc: Allocator,
        map: std.StringHashMapUnmanaged([]u8) = .empty,
        pub fn deinit(self: *@This()) void {
            var it = self.map.iterator();
            while (it.next()) |entry| {
                self.alloc.free(entry.key_ptr.*);
                self.alloc.free(entry.value_ptr.*);
            }
            self.map.deinit(self.alloc);
        }
        fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.map.get(key) orelse error.NotFound;
        }
        fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            const copy_key = try self.alloc.dupe(u8, key);
            errdefer self.alloc.free(copy_key);
            const copy_value = try self.alloc.dupe(u8, value);
            errdefer self.alloc.free(copy_value);
            const slot = try self.map.getOrPut(self.alloc, copy_key);
            if (slot.found_existing) {
                self.alloc.free(copy_key);
                self.alloc.free(slot.value_ptr.*);
            }
            slot.value_ptr.* = copy_value;
        }
        fn delete(self: *@This(), key: []const u8) !void {
            if (self.map.fetchRemove(key)) |entry| {
                self.alloc.free(entry.key);
                self.alloc.free(entry.value);
            }
        }
    };
    const alloc = std.testing.allocator;
    var txn: Fake = .{ .alloc = alloc };
    defer txn.deinit();
    const hash = chunkDigest(0, "bytes");
    const hashes = [_]Digest{hash};
    const proposal: Manifest = .{ .namespace = @splat(1), .publication_digest = @splat(2), .command_digest = @splat(3), .encoded_len = 5, .created_index = 0, .chunk_hashes = &hashes };
    try stageBegin(alloc, &txn, proposal, 7);
    try stageChunk(&txn, proposal.namespace, proposal.publication_digest, proposal.root(), 0, "bytes");
    try std.testing.expect(!(try pruneOneExpired(&txn, 7 + max_upload_age_entries - 1)));
    try std.testing.expect(try pruneOneExpired(&txn, 7 + max_upload_age_entries));
    try std.testing.expectEqual(@as(usize, 0), (try decodeQuota(try optionalGet(&txn, quota_key))).bytes);
    try std.testing.expect((try terminal(&txn, proposal.namespace, proposal.publication_digest)) == null);
    var command: publication.Command = .{
        .mode = .baseline,
        .namespace = @splat(1),
        .authority_epoch = 1,
        .catalog_digest = @splat(2),
        .producer_name = "",
        .producer_generation = 0,
        .sources = &.{},
        .mutations = &.{},
        .publication_digest = @splat(0),
        .baseline = .{ .observed_term = 1, .observed_index = 1, .expected_cursor = "", .next_cursor = "", .upper_bound = "", .row_keys = &.{}, .at_end = true },
    };
    command.publication_digest = command.digest();
    const encoded = try codec.encodeAlloc(alloc, command);
    defer alloc.free(encoded);
    const control_hashes = [_]Digest{chunkDigest(0, encoded)};
    var control: Manifest = .{ .namespace = command.namespace, .publication_digest = command.publication_digest, .command_digest = command.publication_digest, .encoded_len = @intCast(encoded.len), .created_index = 0, .chunk_hashes = &control_hashes };
    try stageBegin(alloc, &txn, control, 20_000);
    try stageChunk(&txn, control.namespace, control.publication_digest, control.root(), 0, encoded);
    try std.testing.expectError(error.ArtifactPublicationUploadChanged, assembleAlloc(alloc, &txn, control.namespace, control.publication_digest, control.root()));
    try std.testing.expect(try pruneOneExpired(&txn, 20_000 + max_upload_age_entries));
    control.control = true;
    try stageBegin(alloc, &txn, control, 30_001);
    try stageChunk(&txn, control.namespace, control.publication_digest, control.root(), 0, encoded);
    var decoded = try assembleAlloc(alloc, &txn, control.namespace, control.publication_digest, control.root());
    defer decoded.deinit();
    try std.testing.expectEqualDeep(command, decoded.command);
    try stageCompletion(&txn, control.namespace, control.publication_digest, control.root(), 30_001, 30_002);
    try std.testing.expectEqual(@as(usize, 0), (try decodeQuota(try optionalGet(&txn, quota_key))).bytes);
}
