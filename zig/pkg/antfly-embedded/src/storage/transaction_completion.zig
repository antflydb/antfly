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

//! Private distributed completion evidence. Structural validation is not a
//! substitute for native authority: producers must persist each resolution
//! atomically with the indicated frame or zero-effect decision, then retain the
//! complete certificate independently of coordinator transaction GC. Consumers
//! must verify the serving owner/cut before trusting this private evidence.
//! Participant membership and diagnostic counts must never cross the public
//! authorized-projection boundary.
const std = @import("std");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
pub const Namespace = [24]u8;
pub const max_participants = 1024;
pub const max_locator_bytes = 1024;
pub const max_bytes = 16 * 1024 * 1024;
pub const admission_prefix = "\x00\x00__txn_completion_admission__:";
pub const record_prefix = "\x00\x00__txn_records__:";
pub const resolution_request_prefix = "\x00\x00__txn_completion_request__:";
pub const resolution_prefix = "\x00\x00__txn_completion_resolution__:";
pub const max_resolution_bytes = 2048;
pub fn requestKey(id: [16]u8) [resolution_request_prefix.len + 16]u8 {
    return resolution_request_prefix.* ++ id;
}
pub fn resolutionKey(id: [16]u8) [resolution_prefix.len + 16]u8 {
    return resolution_prefix.* ++ id;
}
/// An internal staging instruction, removed before physical commit. The exact
/// resolved transaction record is bound so unrelated metadata cannot certify it.
pub const ResolutionRequest = struct {
    transaction_id: [16]u8,
    /// Admission.digest(), binding the complete frozen plan; this is distinct
    /// from the source/input fingerprint stored in Admission.admission_digest.
    admission_digest: Digest,
    commit_version: u64,
    record_digest: Digest,
    pub fn encode(self: ResolutionRequest) ![124]u8 {
        if (!nonzero(self.transaction_id) or !nonzero(self.admission_digest) or !nonzero(self.record_digest) or self.commit_version == 0) return error.InvalidTxnRecord;
        var raw: [124]u8 = undefined;
        @memcpy(raw[0..4], "CRQ1");
        @memcpy(raw[4..20], &self.transaction_id);
        @memcpy(raw[20..52], &self.admission_digest);
        std.mem.writeInt(u64, raw[52..60], self.commit_version, .little);
        @memcpy(raw[60..92], &self.record_digest);
        @memcpy(raw[92..124], &bytesDigest(raw[0..92]));
        return raw;
    }
    pub fn decode(raw: []const u8) !ResolutionRequest {
        if (raw.len != 124 or !std.mem.eql(u8, raw[0..4], "CRQ1") or !std.mem.eql(u8, raw[92..124], &bytesDigest(raw[0..92]))) return error.InvalidTxnRecord;
        const result: ResolutionRequest = .{ .transaction_id = raw[4..20].*, .admission_digest = raw[20..52].*, .commit_version = std.mem.readInt(u64, raw[52..60], .little), .record_digest = raw[60..92].* };
        _ = try result.encode();
        return result;
    }
};
pub fn bytesDigest(bytes: []const u8) Digest {
    var digest: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return digest;
}
pub const Roles = struct {
    pub const effects: u8 = 1;
    pub const guards: u8 = 2;
    pub const checkpoint: u8 = 4;
};
fn nonzero(value: anytype) bool {
    return !std.mem.allEqual(u8, &value, 0);
}
fn same(left: anytype, right: @TypeOf(left)) bool {
    return std.mem.eql(u8, &left, &right);
}
fn number(sha: *std.crypto.hash.sha2.Sha256, value: u64) void {
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, value, .big);
    sha.update(&raw);
}
pub const Participant = struct {
    namespace: Namespace,
    owner_fence: Digest,
    roles: u8,
    /// Exact original routing locator, not a name resolved against a later
    /// topology. Its owner fence binds the physical generation.
    locator: []const u8,
    fn validate(self: Participant) !void {
        if (!nonzero(self.namespace) or !nonzero(self.owner_fence) or self.roles == 0 or self.roles & ~@as(u8, 7) != 0 or self.locator.len == 0 or self.locator.len > max_locator_bytes or !std.unicode.utf8ValidateSlice(self.locator)) return error.InvalidTxnRecord;
    }
};
/// Frozen before prepare. The final commit version is chosen only after votes;
/// it must not be guessed or folded into the source admission fingerprint.
pub const Admission = struct {
    version: u16 = 1,
    transaction_id: [16]u8,
    admission_digest: Digest,
    coordinator_namespace: Namespace,
    coordinator_fence: Digest,
    participants: []const Participant,
    /// Includes the frozen admission itself, a complete certificate, and
    /// bounded key/chunk overhead. Each resolution has five bounded binary
    /// identities, scalar versions and one tagged outcome; 2048 JSON bytes is
    /// a conservative ceiling even with maximal byte escaping. No capacity
    /// is inferred from the number of participants that happen to emit rows.
    pub fn reservationBytes(self: Admission, encoded_admission_bytes: usize) !u64 {
        try self.validate();
        if (encoded_admission_bytes > max_bytes) return error.HistoryTooLarge;
        var certificate: u64 = 4096;
        for (self.participants) |participant| certificate += 512 + 6 * @as(u64, @intCast(participant.locator.len)) + 2048;
        if (certificate > max_bytes) return error.HistoryTooLarge;
        return certificate + encoded_admission_bytes + 128;
    }
    pub fn validate(self: Admission) !void {
        if (self.version != 1 or !nonzero(self.transaction_id) or !nonzero(self.admission_digest) or !nonzero(self.coordinator_namespace) or !nonzero(self.coordinator_fence) or self.participants.len == 0 or self.participants.len > max_participants) return error.InvalidTxnRecord;
        for (self.participants, 0..) |participant, i| {
            try participant.validate();
            if (i != 0 and std.mem.order(u8, &self.participants[i - 1].namespace, &participant.namespace) != .lt) return error.InvalidTxnRecord;
        }
        const coordinator = self.find(self.coordinator_namespace) orelse return error.InvalidTxnRecord;
        if (!same(coordinator.owner_fence, self.coordinator_fence)) return error.InvalidTxnRecord;
    }
    fn find(self: Admission, namespace: Namespace) ?Participant {
        var low: usize = 0;
        var high = self.participants.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            switch (std.mem.order(u8, &self.participants[mid].namespace, &namespace)) {
                .lt => low = mid + 1,
                .gt => high = mid,
                .eq => return self.participants[mid],
            }
        }
        return null;
    }
    pub fn member(self: Admission, namespace: Namespace) !Participant {
        try self.validate();
        return self.find(namespace) orelse error.InvalidParticipant;
    }
    pub fn manifest(self: Admission, commit_version: u64) !Manifest {
        const result: Manifest = .{ .version = self.version, .transaction_id = self.transaction_id, .admission_digest = self.admission_digest, .coordinator_namespace = self.coordinator_namespace, .coordinator_fence = self.coordinator_fence, .commit_version = commit_version, .participants = self.participants };
        try result.validate();
        return result;
    }
    pub fn digest(self: Admission) Digest {
        var sha = std.crypto.hash.sha2.Sha256.init(.{});
        sha.update("antfly:transaction-completion-admission:v1");
        number(&sha, self.version);
        sha.update(&self.transaction_id);
        sha.update(&self.admission_digest);
        sha.update(&self.coordinator_namespace);
        sha.update(&self.coordinator_fence);
        number(&sha, self.participants.len);
        for (self.participants) |participant| {
            sha.update(&participant.namespace);
            sha.update(&participant.owner_fence);
            sha.update(&.{participant.roles});
            number(&sha, participant.locator.len);
            sha.update(participant.locator);
        }
        return sha.finalResult();
    }
};
pub const AdmissionEnvelope = struct { admission: Admission, digest: Digest };
pub fn encodeAdmission(a: A, admission: Admission) ![]u8 {
    try admission.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, AdmissionEnvelope{ .admission = admission, .digest = admission.digest() }, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_bytes) return error.HistoryTooLarge;
    return bytes;
}
pub fn parseAdmission(a: A, bytes: []const u8) !std.json.Parsed(AdmissionEnvelope) {
    if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidTxnRecord;
    var parsed = std.json.parseFromSlice(AdmissionEnvelope, a, bytes, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidTxnRecord,
    };
    errdefer parsed.deinit();
    try parsed.value.admission.validate();
    if (!same(parsed.value.digest, parsed.value.admission.digest())) return error.InvalidTxnRecord;
    return parsed;
}
pub const Manifest = struct {
    version: u16 = 1,
    transaction_id: [16]u8,
    admission_digest: Digest,
    coordinator_namespace: Namespace,
    coordinator_fence: Digest,
    commit_version: u64,
    participants: []const Participant,
    pub fn admission(self: Manifest) Admission {
        return .{ .version = self.version, .transaction_id = self.transaction_id, .admission_digest = self.admission_digest, .coordinator_namespace = self.coordinator_namespace, .coordinator_fence = self.coordinator_fence, .participants = self.participants };
    }
    pub fn validate(self: Manifest) !void {
        try self.admission().validate();
        if (self.commit_version == 0) return error.InvalidTxnRecord;
    }
    pub fn digest(self: Manifest) Digest {
        var sha = std.crypto.hash.sha2.Sha256.init(.{});
        sha.update("antfly:transaction-completion-manifest:v1");
        number(&sha, self.version);
        sha.update(&self.transaction_id);
        sha.update(&self.admission_digest);
        sha.update(&self.coordinator_namespace);
        sha.update(&self.coordinator_fence);
        number(&sha, self.commit_version);
        number(&sha, self.participants.len);
        for (self.participants) |participant| {
            sha.update(&participant.namespace);
            sha.update(&participant.owner_fence);
            sha.update(&.{participant.roles});
            number(&sha, participant.locator.len);
            sha.update(participant.locator);
        }
        return sha.finalResult();
    }
};
/// Same canonical native/Raft source position carried by committed frames.
/// Native: positive sequence in bytes 0..8, zeros 8..31, kind 2 in byte 31.
/// Raft: positive term/index in bytes 0..16, zeros 16..31, kind 1 in byte 31.
pub fn validatePosition(position: Digest) !void {
    const end: usize = switch (position[31]) {
        1 => blk: {
            if (std.mem.readInt(u64, position[0..8], .little) == 0 or std.mem.readInt(u64, position[8..16], .little) == 0) return error.InvalidTxnRecord;
            break :blk 16;
        },
        2 => blk: {
            if (std.mem.readInt(u64, position[0..8], .little) == 0) return error.InvalidTxnRecord;
            break :blk 8;
        },
        else => return error.InvalidTxnRecord,
    };
    if (!std.mem.allEqual(u8, position[end..31], 0)) return error.InvalidTxnRecord;
}
/// frame_digest hashes the exact authenticated committed-frame descriptor,
/// which in turn authenticates every chunk, event offset and frame header.
pub const Effects = struct { sequence: u64, frame_digest: Digest };
pub const Outcome = union(enum) {
    effects: Effects,
    /// Positive native resolution evidence. Absence of a frame or a filtered
    /// public event is never evidence that a participant had no effects.
    no_effect: Digest,
};
pub const Resolution = struct {
    namespace: Namespace,
    owner_fence: Digest,
    manifest_digest: Digest,
    commit_version: u64,
    source_position: Digest,
    outcome: Outcome,
    pub fn validateStructural(self: Resolution) !void {
        if (!nonzero(self.namespace) or !nonzero(self.owner_fence) or !nonzero(self.manifest_digest) or self.commit_version == 0) return error.InvalidTxnRecord;
        try validatePosition(self.source_position);
        switch (self.outcome) {
            .effects => |effects| if (effects.sequence == 0 or !nonzero(effects.frame_digest)) return error.InvalidTxnRecord,
            .no_effect => |proof| if (!nonzero(proof)) return error.InvalidTxnRecord,
        }
    }
    pub fn digest(self: Resolution) Digest {
        var sha = std.crypto.hash.sha2.Sha256.init(.{});
        sha.update("antfly:transaction-completion-resolution:v1");
        self.hashInto(&sha);
        return sha.finalResult();
    }
    pub fn validate(self: Resolution, manifest: Manifest) !void {
        try manifest.validate();
        var low: usize = 0;
        var high = manifest.participants.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            switch (std.mem.order(u8, &manifest.participants[mid].namespace, &self.namespace)) {
                .lt => low = mid + 1,
                .gt => high = mid,
                .eq => return self.validateBound(manifest.digest(), manifest.commit_version, manifest.participants[mid]),
            }
        }
        return error.InvalidTxnRecord;
    }
    fn validateBound(self: Resolution, manifest_digest: Digest, commit_version: u64, participant: Participant) !void {
        try self.validateStructural();
        if (!same(self.namespace, participant.namespace) or !same(self.owner_fence, participant.owner_fence) or !same(self.manifest_digest, manifest_digest) or self.commit_version != commit_version) return error.InvalidTxnRecord;
        try validatePosition(self.source_position);
        switch (self.outcome) {
            .effects => |effects| if (participant.roles & Roles.effects == 0 or effects.sequence == 0 or !nonzero(effects.frame_digest)) return error.InvalidTxnRecord,
            .no_effect => |proof| if (!nonzero(proof)) return error.InvalidTxnRecord,
        }
    }
    fn hashInto(self: Resolution, sha: *std.crypto.hash.sha2.Sha256) void {
        sha.update(&self.namespace);
        sha.update(&self.owner_fence);
        sha.update(&self.manifest_digest);
        number(sha, self.commit_version);
        sha.update(&self.source_position);
        switch (self.outcome) {
            .effects => |effects| {
                sha.update(&.{1});
                number(sha, effects.sequence);
                sha.update(&effects.frame_digest);
            },
            .no_effect => |proof| {
                sha.update(&.{2});
                sha.update(&proof);
            },
        }
    }
};
pub const ResolutionEnvelope = struct { resolution: Resolution, digest: Digest };
pub fn encodeResolution(a: A, manifest: Manifest, resolution: Resolution) ![]u8 {
    try resolution.validate(manifest);
    const raw = try std.json.Stringify.valueAlloc(a, ResolutionEnvelope{ .resolution = resolution, .digest = resolution.digest() }, .{});
    errdefer a.free(raw);
    if (raw.len > max_resolution_bytes) return error.HistoryTooLarge;
    return raw;
}
pub fn parseResolution(a: A, manifest: Manifest, raw: []const u8) !std.json.Parsed(ResolutionEnvelope) {
    var parsed = try parseStoredResolution(a, raw);
    errdefer parsed.deinit();
    try parsed.value.resolution.validate(manifest);
    return parsed;
}
/// A retained receipt is independent of primary transaction-record GC. The
/// reader must additionally bind the expected namespace/owner/manifest/version.
pub fn parseStoredResolution(a: A, raw: []const u8) !std.json.Parsed(ResolutionEnvelope) {
    if (raw.len == 0 or raw.len > max_resolution_bytes) return error.InvalidTxnRecord;
    var parsed = std.json.parseFromSlice(ResolutionEnvelope, a, raw, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidTxnRecord,
    };
    errdefer parsed.deinit();
    try parsed.value.resolution.validateStructural();
    if (!same(parsed.value.digest, parsed.value.resolution.digest())) return error.InvalidTxnRecord;
    return parsed;
}
pub fn noEffectProof(manifest: Manifest, namespace: Namespace, position: Digest) Digest {
    var sha = std.crypto.hash.sha2.Sha256.init(.{});
    sha.update("antfly:transaction-completion-zero-effects:v1");
    sha.update(&manifest.digest());
    sha.update(&namespace);
    sha.update(&position);
    number(&sha, manifest.commit_version);
    return sha.finalResult();
}
pub const Certificate = struct {
    version: u16 = 1,
    manifest: Manifest,
    resolutions: []const Resolution,
    pub fn validate(self: Certificate) !void {
        if (self.version != 1) return error.InvalidTxnRecord;
        try self.manifest.validate();
        if (self.resolutions.len != self.manifest.participants.len) return error.InvalidTxnRecord;
        const manifest_digest = self.manifest.digest();
        for (self.resolutions, self.manifest.participants) |resolution, participant| try resolution.validateBound(manifest_digest, self.manifest.commit_version, participant);
    }
    pub fn digest(self: Certificate) Digest {
        var sha = std.crypto.hash.sha2.Sha256.init(.{});
        sha.update("antfly:transaction-completion-certificate:v1");
        number(&sha, self.version);
        sha.update(&self.manifest.digest());
        number(&sha, self.resolutions.len);
        for (self.resolutions) |resolution| resolution.hashInto(&sha);
        return sha.finalResult();
    }
};
pub const Envelope = struct { certificate: Certificate, digest: Digest };
pub fn encode(a: A, certificate: Certificate) ![]u8 {
    try certificate.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, Envelope{ .certificate = certificate, .digest = certificate.digest() }, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_bytes) return error.HistoryTooLarge;
    return bytes;
}
pub fn parse(a: A, bytes: []const u8) !std.json.Parsed(Envelope) {
    if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidTxnRecord;
    var parsed = std.json.parseFromSlice(Envelope, a, bytes, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidTxnRecord,
    };
    errdefer parsed.deinit();
    try parsed.value.certificate.validate();
    if (!same(parsed.value.digest, parsed.value.certificate.digest())) return error.InvalidTxnRecord;
    return parsed;
}

test "transaction completion requires exact frozen members including positive zero effect resolution" {
    const a = std.testing.allocator;
    const members = [_]Participant{
        .{ .namespace = @splat(1), .owner_fence = @splat(2), .roles = Roles.effects, .locator = "original-writer" },
        .{ .namespace = @splat(3), .owner_fence = @splat(4), .roles = Roles.guards | Roles.checkpoint, .locator = "original-read-only" },
    };
    const manifest: Manifest = .{ .transaction_id = @splat(5), .admission_digest = @splat(6), .coordinator_namespace = @splat(1), .coordinator_fence = @splat(2), .commit_version = 7, .participants = &members };
    var position: Digest = @splat(0);
    std.mem.writeInt(u64, position[0..8], 8, .little);
    position[31] = 2;
    var resolutions = [_]Resolution{
        .{ .namespace = members[0].namespace, .owner_fence = members[0].owner_fence, .manifest_digest = manifest.digest(), .commit_version = 7, .source_position = position, .outcome = .{ .effects = .{ .sequence = 9, .frame_digest = @splat(10) } } },
        .{ .namespace = members[1].namespace, .owner_fence = members[1].owner_fence, .manifest_digest = manifest.digest(), .commit_version = 7, .source_position = position, .outcome = .{ .no_effect = @splat(11) } },
    };
    const certificate: Certificate = .{ .manifest = manifest, .resolutions = &resolutions };
    try certificate.validate();
    try resolutions[0].validate(manifest);
    var foreign = resolutions[0];
    foreign.namespace = @splat(20);
    try std.testing.expectError(error.InvalidTxnRecord, foreign.validate(manifest));
    const bytes = try encode(a, certificate);
    defer a.free(bytes);
    var decoded = try parse(a, bytes);
    defer decoded.deinit();
    try std.testing.expectEqual(certificate.digest(), decoded.value.certificate.digest());
    var partial = certificate;
    partial.resolutions = resolutions[0..1];
    try std.testing.expectError(error.InvalidTxnRecord, partial.validate());
    const original = resolutions[1];
    resolutions[1].outcome = .{ .no_effect = @splat(0) };
    try std.testing.expectError(error.InvalidTxnRecord, certificate.validate());
    resolutions[1] = original;
    resolutions[1].namespace = resolutions[0].namespace;
    try std.testing.expectError(error.InvalidTxnRecord, certificate.validate());
    resolutions[1] = original;
    resolutions[1].owner_fence[0] ^= 1;
    try std.testing.expectError(error.InvalidTxnRecord, certificate.validate());
    resolutions[1] = original;
    resolutions[1].manifest_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidTxnRecord, certificate.validate());
    resolutions[1] = original;
    resolutions[1].commit_version += 1;
    try std.testing.expectError(error.InvalidTxnRecord, certificate.validate());
    resolutions[1] = original;
    resolutions[1].source_position[30] = 1;
    try std.testing.expectError(error.InvalidTxnRecord, certificate.validate());
    resolutions[1] = original;
    resolutions[1].outcome = resolutions[0].outcome;
    try std.testing.expectError(error.InvalidTxnRecord, certificate.validate());
    resolutions[1] = original;
    var changed = certificate;
    changed.manifest.admission_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidTxnRecord, changed.validate());
    changed = certificate;
    changed.manifest.coordinator_fence[0] ^= 1;
    try std.testing.expectError(error.InvalidTxnRecord, changed.validate());
    var reversed = members;
    std.mem.swap(Participant, &reversed[0], &reversed[1]);
    changed = certificate;
    changed.manifest.participants = &reversed;
    try std.testing.expectError(error.InvalidTxnRecord, changed.validate());
    const corrupt = try std.json.Stringify.valueAlloc(a, Envelope{ .certificate = certificate, .digest = @splat(0) }, .{});
    defer a.free(corrupt);
    try std.testing.expectError(error.InvalidTxnRecord, parse(a, corrupt));
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, parse(failing.allocator(), bytes));
    try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
}

test "transaction completion source positions require canonical positive native or raft authority" {
    var position: Digest = @splat(0);
    try std.testing.expectError(error.InvalidTxnRecord, validatePosition(position));
    position[31] = 1;
    std.mem.writeInt(u64, position[0..8], 9, .little);
    try std.testing.expectError(error.InvalidTxnRecord, validatePosition(position));
    std.mem.writeInt(u64, position[8..16], 12, .little);
    try validatePosition(position);
    position[16] = 1;
    try std.testing.expectError(error.InvalidTxnRecord, validatePosition(position));
    position[16] = 0;
    position[31] = 2;
    try std.testing.expectError(error.InvalidTxnRecord, validatePosition(position));
    @memset(position[8..16], 0);
    try validatePosition(position);
    position[31] = 3;
    try std.testing.expectError(error.InvalidTxnRecord, validatePosition(position));
}

test "transaction completion reservation covers escaped plans and every participant outcome" {
    const a = std.testing.allocator;
    const locator: [max_locator_bytes]u8 = @splat(1);
    var members: [16]Participant = undefined;
    for (&members, 0..) |*member, i| {
        var namespace: Namespace = @splat(0);
        std.mem.writeInt(u64, namespace[16..24], i + 1, .big);
        member.* = .{ .namespace = namespace, .owner_fence = @splat(255), .roles = Roles.effects | Roles.guards | Roles.checkpoint, .locator = &locator };
    }
    const admission: Admission = .{ .transaction_id = @splat(255), .admission_digest = @splat(255), .coordinator_namespace = members[0].namespace, .coordinator_fence = @splat(255), .participants = &members };
    const manifest = try admission.manifest(std.math.maxInt(u64));
    var position: Digest = @splat(0);
    @memset(position[0..16], 255);
    position[31] = 1;
    var resolutions: [members.len]Resolution = undefined;
    for (&resolutions, members, 0..) |*resolution, member, i| resolution.* = .{ .namespace = member.namespace, .owner_fence = member.owner_fence, .manifest_digest = manifest.digest(), .commit_version = manifest.commit_version, .source_position = position, .outcome = if (i % 2 == 0) .{ .effects = .{ .sequence = std.math.maxInt(u64), .frame_digest = @splat(255) } } else .{ .no_effect = @splat(255) } };
    const plan = try encodeAdmission(a, admission);
    defer a.free(plan);
    const certificate = try encode(a, .{ .manifest = manifest, .resolutions = &resolutions });
    defer a.free(certificate);
    const reserved = try admission.reservationBytes(plan.len);
    try std.testing.expect(plan.len + certificate.len + 128 <= reserved);
    try std.testing.expectError(error.HistoryTooLarge, admission.reservationBytes(max_bytes + 1));
}
