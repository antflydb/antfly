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

//! Native KV backing for frozen source input. Every mutation borrows the
//! caller's native transaction and must commit with its ordinary apply marker.
//! No age expiry: only explicit source-owner fencing authorizes reclamation.
const std = @import("std");
const completion = @import("transaction_completion.zig");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
pub const frozen_header_bytes: usize = 64;
pub const max_admission_bytes: usize = 16 * 1024 * 1024;
pub const max_participant_inputs_bytes: usize = 64 * 1024 * 1024;
pub const max_participants: usize = 1024;
pub const max_bytes: usize = frozen_header_bytes + max_admission_bytes + max_participant_inputs_bytes + 4 * max_participants;
pub const chunk_bytes: usize = 1024 * 1024;
pub const max_chunks: usize = (max_bytes + chunk_bytes - 1) / chunk_bytes;
pub const max_active: usize = 8;
pub const max_reserved_bytes: u64 = 256 * 1024 * 1024;
pub const max_record_bytes: usize = 64 * 1024;
pub const max_reclaim_chunks: usize = 8;
const prefix = "\x00\x00__source_plan_artifact__:";
const quota_prefix = prefix ++ "quota:";
const record_prefix = prefix ++ "record:";
const chunk_prefix = prefix ++ "chunk:";
fn nz(value: anytype) bool {
    return !std.mem.allEqual(u8, &value, 0);
}
fn same(a: anytype, b: @TypeOf(a)) bool {
    return std.mem.eql(u8, &a, &b);
}
pub fn isProtectedKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, prefix);
}

/// The applying facade supplies independently qualified physical root scope.
/// A digest supplied by a caller does not qualify a root by itself.
pub const Root = struct {
    authority: enum { native, raft },
    namespace: [24]u8,
    generation: u64,
    fence: Digest,
    pub fn validate(self: Root) !void {
        if (std.mem.readInt(u64, self.namespace[0..8], .big) == 0 or !nz(self.fence)) return error.InvalidSourceAdmission;
    }
};
fn validatePosition(root: Root, position: Digest) !void {
    try completion.validatePosition(position);
    if (position[31] != @as(u8, if (root.authority == .native) 2 else 1)) return error.InvalidTxnRecord;
}
pub fn namespaceFor(root: Root, epoch: u64) Digest {
    return valueDigest("antfly:native-source-plan-namespace:v1", .{ .root = root, .epoch = epoch });
}
pub fn domainFor(binding: Digest) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:source-plan-upload-domain:v1");
    hash.update(&binding);
    return hash.finalResult();
}
pub const Spec = struct {
    namespace: Digest,
    storage_epoch: u64,
    binding: Digest,
    domain: Digest,
    attempt: [16]u8,
    bytes: u64,
    checksum: Digest,
    plan_digest: Digest,
    chunk_hashes: []const Digest,
    pub fn validate(self: Spec, root: Root) !void {
        try root.validate();
        if (self.storage_epoch == 0 or self.storage_epoch == std.math.maxInt(u64) or !same(self.namespace, namespaceFor(root, self.storage_epoch)) or !nz(self.binding) or !same(self.domain, domainFor(self.binding)) or std.mem.readInt(u64, self.attempt[0..8], .big) == 0 or !nz(self.attempt[8..16].*) or self.bytes < frozen_header_bytes or self.bytes > max_bytes or !nz(self.checksum) or !nz(self.plan_digest) or self.chunk_hashes.len != (self.bytes + chunk_bytes - 1) / chunk_bytes or self.chunk_hashes.len > max_chunks) return error.InvalidSourceAdmission;
        for (self.chunk_hashes) |hash| if (!nz(hash)) return error.InvalidSourceAdmission;
    }
    pub fn id(self: Spec) Digest {
        return valueDigest("antfly:native-source-plan-spec:v1", self);
    }
    pub fn chunkLen(self: Spec, ordinal: usize) !usize {
        if (ordinal >= self.chunk_hashes.len) return error.InvalidSourceAdmission;
        return @intCast(@min(chunk_bytes, self.bytes - ordinal * chunk_bytes));
    }
    pub fn reservationBytes(self: Spec) u64 {
        // Payload plus a bounded manifest and all point-addressed chunk keys.
        return self.bytes + max_record_bytes + @as(u64, @intCast(self.chunk_hashes.len)) * 128;
    }
};
pub fn chunkDigest(ordinal: usize, bytes: []const u8) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:native-source-plan-chunk:v1");
    var number: [8]u8 = undefined;
    std.mem.writeInt(u64, &number, ordinal, .little);
    hash.update(&number);
    hash.update(bytes);
    return hash.finalResult();
}

pub const FrozenHasher = struct {
    hash: std.crypto.hash.sha2.Sha256,
    bytes: usize = 0,
    pub fn init() FrozenHasher {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:frozen-completion-plan:v1");
        return .{ .hash = hash };
    }
    pub fn update(self: *FrozenHasher, bytes: []const u8) !void {
        const end = std.math.add(usize, self.bytes, bytes.len) catch return error.InvalidSourceAdmission;
        if (end > max_bytes) return error.InvalidSourceAdmission;
        if (self.bytes < 32) self.hash.update(bytes[0..@min(bytes.len, 32 - self.bytes)]);
        if (end > frozen_header_bytes) self.hash.update(bytes[@min(bytes.len, frozen_header_bytes -| self.bytes)..]);
        self.bytes = end;
    }
    pub fn finish(self: FrozenHasher) !Digest {
        if (self.bytes < frozen_header_bytes) return error.InvalidSourceAdmission;
        var hash = self.hash;
        return hash.finalResult();
    }
};
pub fn frozenDigest(bytes: []const u8) !Digest {
    if (bytes.len < frozen_header_bytes or bytes.len > max_bytes) return error.InvalidTxnRecord;
    var hash = FrozenHasher.init();
    try hash.update(bytes);
    return hash.finish();
}

pub const Quota = struct { epoch: u64 = 1, active: u16 = 0, reserved: u64 = 0 };
fn quotaKey(root: Root) [quota_prefix.len + 32]u8 {
    return scopedKey(quota_prefix, root);
}
fn scopedKey(comptime key_prefix: []const u8, root: Root) [key_prefix.len + 32]u8 {
    var out: [key_prefix.len + 32]u8 = undefined;
    @memcpy(out[0..key_prefix.len], key_prefix);
    @memcpy(out[key_prefix.len..][0..24], &root.namespace);
    std.mem.writeInt(u64, out[key_prefix.len + 24 ..][0..8], root.generation, .big);
    return out;
}
fn recordKey(root: Root, id: Digest) [record_prefix.len + 64]u8 {
    var out: [record_prefix.len + 64]u8 = undefined;
    @memcpy(out[0 .. record_prefix.len + 32], &scopedKey(record_prefix, root));
    @memcpy(out[record_prefix.len + 32 ..], &id);
    return out;
}
fn chunkKey(root: Root, id: Digest, ordinal: usize) ![chunk_prefix.len + 66]u8 {
    if (ordinal >= max_chunks) return error.InvalidSourceAdmission;
    var out: [chunk_prefix.len + 66]u8 = undefined;
    @memcpy(out[0 .. chunk_prefix.len + 32], &scopedKey(chunk_prefix, root));
    @memcpy(out[chunk_prefix.len + 32 ..][0..32], &id);
    std.mem.writeInt(u16, out[chunk_prefix.len + 64 ..][0..2], @intCast(ordinal), .big);
    return out;
}
fn optionalGet(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| if (err == error.NotFound) null else err;
}
fn quotaBytes(quota: Quota) [64]u8 {
    var out: [64]u8 = @splat(0);
    @memcpy(out[0..4], "SFQ1");
    std.mem.writeInt(u64, out[4..12], quota.epoch, .little);
    std.mem.writeInt(u64, out[12..20], quota.reserved, .little);
    std.mem.writeInt(u16, out[20..22], quota.active, .little);
    std.crypto.hash.sha2.Sha256.hash(out[0..32], out[32..64], .{});
    return out;
}
pub fn readQuota(txn: anytype, root: Root) !Quota {
    try root.validate();
    const raw = (try optionalGet(txn, &quotaKey(root))) orelse return .{};
    if (raw.len != 64 or !std.mem.eql(u8, raw[0..4], "SFQ1") or !std.mem.allEqual(u8, raw[22..32], 0)) return error.SourceAdmissionCorrupt;
    var digest: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw[0..32], &digest, .{});
    if (!std.mem.eql(u8, &digest, raw[32..64])) return error.SourceAdmissionCorrupt;
    const quota: Quota = .{ .epoch = std.mem.readInt(u64, raw[4..12], .little), .reserved = std.mem.readInt(u64, raw[12..20], .little), .active = std.mem.readInt(u16, raw[20..22], .little) };
    if (quota.epoch == 0 or quota.active > max_active or quota.reserved > max_reserved_bytes or ((quota.active == 0) != (quota.reserved == 0))) return error.SourceAdmissionCorrupt;
    return quota;
}
pub fn hasActive(txn: anytype, root: Root) !bool {
    return (try readQuota(txn, root)).active != 0;
}

/// Topology drain is a cold path, like the retained transaction scan. Include
/// every retained physical generation: a stale generation may still owe cleanup.
/// No payload chunks or manifests are read by this accounting check.
pub fn hasRetainedArtifacts(txn: anytype) !bool {
    var cursor = try txn.openCursor();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(quota_prefix);
    while (entry) |item| {
        if (!std.mem.startsWith(u8, item.key, quota_prefix)) break;
        if (item.key.len != quota_prefix.len + 32) return error.SourceAdmissionCorrupt;
        const root: Root = .{ .authority = .native, .namespace = item.key[quota_prefix.len..][0..24].*, .generation = std.mem.readInt(u64, item.key[quota_prefix.len + 24 ..][0..8], .big), .fence = @splat(1) };
        if (try hasActive(txn, root)) return true;
        entry = try cursor.next();
    }
    return false;
}

const Record = struct {
    spec: Spec,
    received: [16]u8 = @splat(0),
    sealed_position: ?Digest = null,
    fenced_position: ?Digest = null,
    cleanup_cursor: u16 = 0,
    fn has(self: Record, ordinal: usize) bool {
        return self.received[ordinal / 8] & (@as(u8, 1) << @as(u3, @intCast(ordinal % 8))) != 0;
    }
    fn set(self: *Record, ordinal: usize, present: bool) void {
        const mask = @as(u8, 1) << @as(u3, @intCast(ordinal % 8));
        if (present) self.received[ordinal / 8] |= mask else self.received[ordinal / 8] &= ~mask;
    }
    fn validate(self: Record, root: Root) !void {
        try self.spec.validate(root);
        if (self.cleanup_cursor > self.spec.chunk_hashes.len or (self.cleanup_cursor != 0 and self.fenced_position == null)) return error.SourceAdmissionCorrupt;
        for (0..128) |ordinal| {
            if ((ordinal < self.cleanup_cursor or ordinal >= self.spec.chunk_hashes.len) and self.has(ordinal)) return error.SourceAdmissionCorrupt;
            if (self.sealed_position != null and self.fenced_position == null and ordinal < self.spec.chunk_hashes.len and !self.has(ordinal)) return error.SourceAdmissionCorrupt;
        }
        if (self.sealed_position) |position| try validatePosition(root, position);
        if (self.fenced_position) |position| try validatePosition(root, position);
    }
};
const Envelope = struct { record: Record, checksum: Digest };
fn valueDigest(domain: []const u8, value: anytype) Digest {
    const Output = struct {
        writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain, .flush = std.Io.Writer.noopFlush }, .buffer = &.{} },
        hash: std.crypto.hash.sha2.Sha256 = .init(.{}),
        fn drain(writer: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *@This() = @alignCast(@fieldParentPtr("writer", writer));
            for (data[0 .. data.len - 1]) |part| self.hash.update(part);
            for (0..splat) |_| self.hash.update(data[data.len - 1]);
            return std.Io.Writer.countSplat(data, splat);
        }
    };
    var out: Output = .{};
    out.hash.update(domain);
    std.json.Stringify.value(value, .{}, &out.writer) catch unreachable;
    return out.hash.finalResult();
}
fn writeRecord(a: A, txn: anytype, root: Root, record: Record) !void {
    try record.validate(root);
    const bytes = try std.json.Stringify.valueAlloc(a, Envelope{ .record = record, .checksum = valueDigest("antfly:native-source-plan-record:v1", record) }, .{});
    defer a.free(bytes);
    if (bytes.len > max_record_bytes) return error.HistoryTooLarge;
    try txn.put(&recordKey(root, record.spec.id()), bytes);
}
fn loadRecord(a: A, txn: anytype, root: Root, spec: Spec) !?std.json.Parsed(Envelope) {
    const raw = (try optionalGet(txn, &recordKey(root, spec.id()))) orelse return null;
    if (raw.len > max_record_bytes) return error.SourceAdmissionCorrupt;
    var parsed = std.json.parseFromSlice(Envelope, a, raw, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    errdefer parsed.deinit();
    try parsed.value.record.validate(root);
    const quota = try readQuota(txn, root);
    if (quota.active == 0 or quota.reserved < spec.reservationBytes() or quota.epoch < spec.storage_epoch or (parsed.value.record.fenced_position != null and quota.epoch <= spec.storage_epoch)) return error.SourceAdmissionCorrupt;
    if (!same(parsed.value.checksum, valueDigest("antfly:native-source-plan-record:v1", parsed.value.record)) or !same(parsed.value.record.spec.id(), spec.id())) return error.SourceAdmissionCorrupt;
    return parsed;
}

/// A persistent root epoch fences old, absent grants without per-upload tombstones.
/// Existing active records survive another upload's epoch advance.
pub const Status = struct {
    present: bool = false,
    received: [16]u8 = @splat(0),
    sealed_position: ?Digest = null,
    fenced_position: ?Digest = null,
    seal: ?Digest = null,
    fence: ?Digest = null,
    reclamation: ?Digest = null,
};
/// A snapshot observation, not authority by itself. The serving facade binds
/// it to its independently qualified root and actual native/Raft read cut.
pub fn inspect(a: A, txn: anytype, root: Root, spec: Spec) !Status {
    try spec.validate(root);
    if (try loadRecord(a, txn, root, spec)) |saved| {
        var owned = saved;
        defer owned.deinit();
        const record = owned.value.record;
        return .{
            .present = true,
            .received = record.received,
            .sealed_position = record.sealed_position,
            .fenced_position = record.fenced_position,
            .seal = if (record.sealed_position) |position| sealProof(spec, position) else null,
            .fence = if (record.fenced_position != null) fenceProof(spec) else null,
        };
    }
    return .{ .reclamation = if (try reclaimed(txn, root, spec)) try reclamationProof(txn, root, spec) else null };
}

pub fn stageBegin(a: A, txn: anytype, root: Root, spec: Spec) !void {
    try spec.validate(root);
    if (try loadRecord(a, txn, root, spec)) |saved| {
        var owned = saved;
        defer owned.deinit();
        if (owned.value.record.fenced_position != null) return error.SourceReceiptExpired;
        return;
    }
    var quota = try readQuota(txn, root);
    if (spec.storage_epoch != quota.epoch) return error.SourceReceiptExpired;
    const reserved = spec.reservationBytes();
    if (quota.active == max_active or reserved > max_reserved_bytes - quota.reserved) return error.SourceAdmissionBackpressure;
    quota.active += 1;
    quota.reserved += reserved;
    try writeRecord(a, txn, root, .{ .spec = spec });
    try txn.put(&quotaKey(root), &quotaBytes(quota));
}
pub fn stageChunk(a: A, txn: anytype, root: Root, spec: Spec, ordinal: usize, bytes: []const u8) !void {
    try spec.validate(root);
    if (bytes.len != try spec.chunkLen(ordinal) or !same(chunkDigest(ordinal, bytes), spec.chunk_hashes[ordinal])) return error.SourcePayloadConflict;
    var saved = (try loadRecord(a, txn, root, spec)) orelse return error.SourcePendingWork;
    defer saved.deinit();
    var record = saved.value.record;
    if (record.fenced_position != null) return error.SourceReceiptExpired;
    const key = try chunkKey(root, spec.id(), ordinal);
    if (try optionalGet(txn, &key)) |prior| {
        if (!record.has(ordinal) or !std.mem.eql(u8, prior, bytes)) return error.SourceAdmissionCorrupt;
        return;
    }
    if (record.has(ordinal) or record.sealed_position != null) return error.SourceAdmissionCorrupt;
    record.set(ordinal, true);
    try txn.put(&key, bytes);
    try writeRecord(a, txn, root, record);
}
pub fn stageSeal(a: A, txn: anytype, root: Root, spec: Spec, position: Digest) !Digest {
    try spec.validate(root);
    try validatePosition(root, position);
    var saved = (try loadRecord(a, txn, root, spec)) orelse return error.SourcePendingWork;
    defer saved.deinit();
    var record = saved.value.record;
    if (record.fenced_position != null) return error.SourceReceiptExpired;
    if (record.sealed_position) |original| return sealProof(spec, original);
    var object = std.crypto.hash.sha2.Sha256.init(.{});
    var plan = FrozenHasher.init();
    for (0..spec.chunk_hashes.len) |ordinal| {
        const bytes = (try optionalGet(txn, &try chunkKey(root, spec.id(), ordinal))) orelse return error.SourcePendingWork;
        if (!record.has(ordinal) or bytes.len != try spec.chunkLen(ordinal) or !same(chunkDigest(ordinal, bytes), spec.chunk_hashes[ordinal])) return error.SourceAdmissionCorrupt;
        if (ordinal == 0) {
            if (!std.mem.eql(u8, bytes[0..4], "AFP1") or !std.mem.eql(u8, bytes[32..64], &spec.plan_digest)) return error.SourcePayloadConflict;
            const count = std.mem.readInt(u32, bytes[4..8], .little);
            const admission_len = std.mem.readInt(u64, bytes[24..32], .little);
            if (count == 0 or count > max_participants or admission_len == 0 or admission_len > max_admission_bytes or admission_len > spec.bytes - frozen_header_bytes or std.mem.readInt(u64, bytes[8..16], .little) == 0 or std.mem.readInt(u64, bytes[16..24], .little) == 0) return error.SourcePayloadConflict;
        }
        object.update(bytes);
        try plan.update(bytes);
    }
    if (!same(object.finalResult(), spec.checksum) or !same(try plan.finish(), spec.plan_digest)) return error.SourcePayloadConflict;
    record.sealed_position = position;
    try writeRecord(a, txn, root, record);
    return sealProof(spec, position);
}
fn sealProof(spec: Spec, position: Digest) Digest {
    return valueDigest("antfly:native-source-plan-sealed:v1", .{ .id = spec.id(), .position = position });
}
pub fn readSealedAlloc(a: A, txn: anytype, root: Root, spec: Spec, expected_seal: Digest) ![]u8 {
    try spec.validate(root);
    var saved = (try loadRecord(a, txn, root, spec)) orelse return error.SourcePendingWork;
    defer saved.deinit();
    const record = saved.value.record;
    if (record.fenced_position != null) return error.SourceReceiptExpired;
    const position = record.sealed_position orelse return error.SourcePendingWork;
    if (!same(expected_seal, sealProof(spec, position))) return error.SourcePayloadConflict;
    const out = try a.alloc(u8, @intCast(spec.bytes));
    errdefer a.free(out);
    for (0..spec.chunk_hashes.len) |ordinal| {
        const bytes = (try optionalGet(txn, &try chunkKey(root, spec.id(), ordinal))) orelse return error.SourceAdmissionCorrupt;
        if (!record.has(ordinal) or bytes.len != try spec.chunkLen(ordinal) or !same(chunkDigest(ordinal, bytes), spec.chunk_hashes[ordinal])) return error.SourceAdmissionCorrupt;
        @memcpy(out[ordinal * chunk_bytes ..][0..bytes.len], bytes);
    }
    var checksum: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(out, &checksum, .{});
    if (!same(checksum, spec.checksum) or !same(try frozenDigest(out), spec.plan_digest)) return error.SourceAdmissionCorrupt;
    return out;
}
pub fn readSealedChunkAlloc(a: A, txn: anytype, root: Root, spec: Spec, expected_seal: Digest, ordinal: usize) ![]u8 {
    try spec.validate(root);
    const expected_len = try spec.chunkLen(ordinal);
    const status = try inspect(a, txn, root, spec);
    if (status.fenced_position != null or status.reclamation != null) return error.SourceReceiptExpired;
    const seal = status.seal orelse return error.SourcePendingWork;
    if (!same(seal, expected_seal)) return error.SourcePayloadConflict;
    const bytes = (try optionalGet(txn, &try chunkKey(root, spec.id(), ordinal))) orelse return error.SourceAdmissionCorrupt;
    if (bytes.len != expected_len or !same(chunkDigest(ordinal, bytes), spec.chunk_hashes[ordinal])) return error.SourceAdmissionCorrupt;
    return a.dupe(u8, bytes);
}

pub fn stageFence(a: A, txn: anytype, root: Root, spec: Spec, position: Digest) !Digest {
    try spec.validate(root);
    try validatePosition(root, position);
    var quota = try readQuota(txn, root);
    if (spec.storage_epoch > quota.epoch) return error.SourceReceiptExpired;
    if (try loadRecord(a, txn, root, spec)) |saved| {
        var owned = saved;
        defer owned.deinit();
        var record = owned.value.record;
        if (record.fenced_position != null) return fenceProof(spec);
        quota.epoch = @max(quota.epoch, spec.storage_epoch + 1);
        record.fenced_position = position;
        try writeRecord(a, txn, root, record);
        try txn.put(&quotaKey(root), &quotaBytes(quota));
    } else {
        // Closing an unstarted grant is also durable and prevents late BEGIN.
        quota.epoch = @max(quota.epoch, spec.storage_epoch + 1);
        try txn.put(&quotaKey(root), &quotaBytes(quota));
    }
    return fenceProof(spec);
}
fn fenceProof(spec: Spec) Digest {
    return valueDigest("antfly:native-source-plan-fenced:v1", spec.id());
}
pub fn stageReclaim(a: A, txn: anytype, root: Root, spec: Spec, limit: usize) !bool {
    try spec.validate(root);
    if (limit == 0 or limit > max_reclaim_chunks) return error.InvalidSourceAdmission;
    var saved = (try loadRecord(a, txn, root, spec)) orelse return try reclaimed(txn, root, spec);
    defer saved.deinit();
    var record = saved.value.record;
    if (record.fenced_position == null or (try readQuota(txn, root)).epoch <= spec.storage_epoch) return error.SourcePendingWork;
    const stop = @min(record.spec.chunk_hashes.len, @as(usize, record.cleanup_cursor) + limit);
    for (record.cleanup_cursor..stop) |ordinal| {
        txn.delete(&try chunkKey(root, spec.id(), ordinal)) catch |err| if (err != error.NotFound) return err;
        record.set(ordinal, false);
    }
    record.cleanup_cursor = @intCast(stop);
    if (stop != record.spec.chunk_hashes.len) {
        try writeRecord(a, txn, root, record);
        return false;
    }
    var quota = try readQuota(txn, root);
    if (quota.active == 0 or quota.reserved < spec.reservationBytes()) return error.SourceAdmissionCorrupt;
    quota.active -= 1;
    quota.reserved -= spec.reservationBytes();
    try txn.delete(&recordKey(root, spec.id()));
    try txn.put(&quotaKey(root), &quotaBytes(quota));
    return true;
}
pub fn reclaimed(txn: anytype, root: Root, spec: Spec) !bool {
    try spec.validate(root);
    if ((try readQuota(txn, root)).epoch <= spec.storage_epoch or try optionalGet(txn, &recordKey(root, spec.id())) != null) return false;
    for (0..spec.chunk_hashes.len) |ordinal| if (try optionalGet(txn, &try chunkKey(root, spec.id(), ordinal)) != null) return error.SourceAdmissionCorrupt;
    return true;
}
pub fn reclamationProof(txn: anytype, root: Root, spec: Spec) !Digest {
    if (!try reclaimed(txn, root, spec)) return error.SourcePendingWork;
    return valueDigest("antfly:native-source-plan-reclaimed:v1", spec.id());
}

const TestTxn = struct {
    map: std.StringHashMapUnmanaged([]u8) = .empty,
    fn deinit(self: *@This()) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            std.testing.allocator.free(entry.key_ptr.*);
            std.testing.allocator.free(entry.value_ptr.*);
        }
        self.map.deinit(std.testing.allocator);
    }
    fn get(self: *@This(), key: []const u8) ![]const u8 {
        return self.map.get(key) orelse error.NotFound;
    }
    fn put(self: *@This(), key: []const u8, value: []const u8) !void {
        const a = std.testing.allocator;
        const k = try a.dupe(u8, key);
        errdefer a.free(k);
        const v = try a.dupe(u8, value);
        errdefer a.free(v);
        const slot = try self.map.getOrPut(a, k);
        if (slot.found_existing) {
            a.free(k);
            a.free(slot.value_ptr.*);
        }
        slot.value_ptr.* = v;
    }
    fn delete(self: *@This(), key: []const u8) !void {
        const entry = self.map.fetchRemove(key) orelse return error.NotFound;
        std.testing.allocator.free(entry.key);
        std.testing.allocator.free(entry.value);
    }
};
fn testRoot() Root {
    var root: Root = .{ .authority = .native, .namespace = @splat(0), .generation = 0, .fence = @splat(1) };
    std.mem.writeInt(u64, root.namespace[0..8], 1, .big);
    return root;
}
fn testPosition(n: u64) Digest {
    var position: Digest = @splat(0);
    std.mem.writeInt(u64, position[0..8], n, .little);
    position[31] = 2;
    return position;
}
fn testSpec(root: Root, bytes: []u8, hashes: []Digest, token: u64) !Spec {
    @memset(bytes, 7);
    @memcpy(bytes[0..4], "AFP1");
    std.mem.writeInt(u32, bytes[4..8], 1, .little);
    std.mem.writeInt(u64, bytes[8..16], 10, .little);
    std.mem.writeInt(u64, bytes[16..24], 11, .little);
    std.mem.writeInt(u64, bytes[24..32], 1, .little);
    const plan = try frozenDigest(bytes);
    @memcpy(bytes[32..64], &plan);
    var checksum: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &checksum, .{});
    for (hashes, 0..) |*hash, ordinal| hash.* = chunkDigest(ordinal, bytes[ordinal * chunk_bytes .. @min(bytes.len, (ordinal + 1) * chunk_bytes)]);
    var attempt: [16]u8 = @splat(1);
    std.mem.writeInt(u64, attempt[0..8], token, .big);
    return .{ .namespace = namespaceFor(root, 1), .storage_epoch = 1, .binding = @splat(2), .domain = domainFor(@splat(2)), .attempt = attempt, .bytes = bytes.len, .checksum = checksum, .plan_digest = plan, .chunk_hashes = hashes };
}

test "source artifact seal retries retain the original cut and owned payload" {
    const a = std.testing.allocator;
    var txn: TestTxn = .{};
    defer txn.deinit();
    const root = testRoot();
    var bytes: [128]u8 = undefined;
    var hashes: [1]Digest = undefined;
    const spec = try testSpec(root, &bytes, &hashes, 1);
    try stageBegin(a, &txn, root, spec);
    try stageBegin(a, &txn, root, spec);
    try std.testing.expectEqual(@as(u16, 1), (try readQuota(&txn, root)).active);
    try std.testing.expectError(error.SourcePendingWork, stageSeal(a, &txn, root, spec, testPosition(1)));
    try stageChunk(a, &txn, root, spec, 0, &bytes);
    try stageChunk(a, &txn, root, spec, 0, &bytes);
    const proof = try stageSeal(a, &txn, root, spec, testPosition(2));
    try std.testing.expectEqual(proof, try stageSeal(a, &txn, root, spec, testPosition(3)));
    const owned = try readSealedAlloc(a, &txn, root, spec, proof);
    defer a.free(owned);
    try std.testing.expectEqualSlices(u8, &bytes, owned);
    try std.testing.expectError(error.SourcePayloadConflict, readSealedAlloc(a, &txn, root, spec, @splat(9)));
    var wrong = testPosition(4);
    wrong[31] = 1;
    std.mem.writeInt(u64, wrong[8..16], 1, .little);
    try std.testing.expectError(error.InvalidTxnRecord, stageFence(a, &txn, root, spec, wrong));
    const fence = try stageFence(a, &txn, root, spec, testPosition(4));
    try std.testing.expectEqual(fence, try stageFence(a, &txn, root, spec, testPosition(5)));
    try std.testing.expectError(error.SourceReceiptExpired, readSealedAlloc(a, &txn, root, spec, proof));
    try std.testing.expect(try stageReclaim(a, &txn, root, spec, 1));
    try std.testing.expect(try reclaimed(&txn, root, spec));
    try std.testing.expectEqual(fence, try stageFence(a, &txn, root, spec, testPosition(6)));
    try std.testing.expectError(error.SourceReceiptExpired, stageBegin(a, &txn, root, spec));
    try std.testing.expectEqual(@as(u64, 0), (try readQuota(&txn, root)).reserved);
}

test "source artifact partial cleanup retains quota and fences only absent old grants" {
    const a = std.testing.allocator;
    var txn: TestTxn = .{};
    defer txn.deinit();
    const root = testRoot();
    const bytes = try a.alloc(u8, chunk_bytes + 1);
    defer a.free(bytes);
    var hashes: [2]Digest = undefined;
    const spec = try testSpec(root, bytes, &hashes, 1);
    var second = spec;
    std.mem.writeInt(u64, second.attempt[0..8], 2, .big);
    try stageBegin(a, &txn, root, spec);
    try stageBegin(a, &txn, root, second);
    try stageChunk(a, &txn, root, spec, 0, bytes[0..chunk_bytes]);
    _ = try stageFence(a, &txn, root, spec, testPosition(1));
    const before = try readQuota(&txn, root);
    try std.testing.expect(!try stageReclaim(a, &txn, root, spec, 1));
    try std.testing.expectEqual(before, try readQuota(&txn, root));
    // Missing chunks in an interrupted upload are harmless during reclamation.
    try std.testing.expect(try stageReclaim(a, &txn, root, spec, 1));
    try stageBegin(a, &txn, root, second);
    try stageChunk(a, &txn, root, second, 1, bytes[chunk_bytes..]);
    var absent = spec;
    std.mem.writeInt(u64, absent.attempt[0..8], 3, .big);
    _ = try stageFence(a, &txn, root, absent, testPosition(2));
    try std.testing.expectError(error.SourceReceiptExpired, stageBegin(a, &txn, root, absent));
    try std.testing.expectEqual(@as(u16, 1), (try readQuota(&txn, root)).active);
    _ = try stageFence(a, &txn, root, second, testPosition(3));
    try std.testing.expect(try stageReclaim(a, &txn, root, second, 2));
    try std.testing.expectEqual(@as(u16, 0), (try readQuota(&txn, root)).active);
}

test "source artifact streaming digest ignores only its checksum slot" {
    var bytes: [128]u8 = @splat(4);
    const expected = try frozenDigest(&bytes);
    for (1..bytes.len) |stride| {
        var hash = FrozenHasher.init();
        var offset: usize = 0;
        while (offset < bytes.len) {
            const end = @min(bytes.len, offset + stride);
            try hash.update(bytes[offset..end]);
            offset = end;
        }
        try std.testing.expectEqual(expected, try hash.finish());
    }
    @memset(bytes[32..64], 9);
    try std.testing.expectEqual(expected, try frozenDigest(&bytes));
    bytes[64] ^= 1;
    try std.testing.expect(!same(expected, try frozenDigest(&bytes)));
}

test "source artifact admission caps active uploads and validates root epochs" {
    const a = std.testing.allocator;
    var txn: TestTxn = .{};
    defer txn.deinit();
    const root = testRoot();
    var bytes: [128]u8 = undefined;
    var hashes: [1]Digest = undefined;
    var spec = try testSpec(root, &bytes, &hashes, 1);
    for (1..max_active + 1) |token| {
        std.mem.writeInt(u64, spec.attempt[0..8], token, .big);
        try stageBegin(a, &txn, root, spec);
    }
    std.mem.writeInt(u64, spec.attempt[0..8], max_active + 1, .big);
    try std.testing.expectError(error.SourceAdmissionBackpressure, stageBegin(a, &txn, root, spec));
    spec.storage_epoch = 2;
    spec.namespace = namespaceFor(root, 2);
    try std.testing.expectError(error.SourceReceiptExpired, stageFence(a, &txn, root, spec, testPosition(1)));
    spec.storage_epoch = std.math.maxInt(u64);
    spec.namespace = namespaceFor(root, spec.storage_epoch);
    try std.testing.expectError(error.InvalidSourceAdmission, stageBegin(a, &txn, root, spec));
    spec.storage_epoch = 1;
    spec.namespace = namespaceFor(root, 1);
    var changed_root = root;
    changed_root.generation += 1;
    try std.testing.expectError(error.InvalidSourceAdmission, stageBegin(a, &txn, changed_root, spec));
    changed_root = root;
    changed_root.fence[0] ^= 1;
    try std.testing.expectError(error.InvalidSourceAdmission, stageBegin(a, &txn, changed_root, spec));
}

test "source artifact missing quota and corrupted retained chunks fail closed" {
    const a = std.testing.allocator;
    var txn: TestTxn = .{};
    defer txn.deinit();
    const root = testRoot();
    var bytes: [128]u8 = undefined;
    var hashes: [1]Digest = undefined;
    const spec = try testSpec(root, &bytes, &hashes, 1);
    try stageBegin(a, &txn, root, spec);
    try stageChunk(a, &txn, root, spec, 0, &bytes);
    const seal = try stageSeal(a, &txn, root, spec, testPosition(1));
    const key = try chunkKey(root, spec.id(), 0);
    txn.map.getPtr(&key).?.*[127] ^= 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, readSealedAlloc(a, &txn, root, spec, seal));
    try txn.delete(&quotaKey(root));
    try std.testing.expectError(error.SourceAdmissionCorrupt, stageBegin(a, &txn, root, spec));
}
