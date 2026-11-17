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

//! Durable quota and upload authority for one source's frozen completion plan.
//! Storage evidence is trusted owner input, never a public proof. Returning
//! quota requires a fenced storage reclamation proof, not acceptance or ACK.
const std = @import("std");
pub const Digest = [32]u8;
pub const protocol_version: u16 = 102;
pub const max_payload_bytes: u64 = 80 * 1024 * 1024 + 4160;
fn nz(value: anytype) bool {
    return !std.mem.allEqual(u8, &value, 0);
}
fn same(a: anytype, b: @TypeOf(a)) bool {
    return std.mem.eql(u8, &a, &b);
}
pub fn domainFor(binding: Digest) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:source-plan-upload-domain:v1");
    hash.update(&binding);
    return hash.finalResult();
}
pub const Spec = struct {
    namespace: Digest,
    binding: Digest,
    domain: Digest,
    attempt: [16]u8,
    bytes: u64,
    /// Object SHA-256 and frozen-plan identity are separate commitments.
    checksum: Digest,
    plan_digest: Digest,
    pub fn token(self: Spec) u64 {
        return std.mem.readInt(u64, self.attempt[0..8], .big);
    }
    pub fn validate(self: Spec) !void {
        if (!nz(self.namespace) or !nz(self.binding) or !nz(self.domain) or !same(self.domain, domainFor(self.binding)) or self.token() == 0 or !nz(self.attempt[8..16].*) or self.bytes < 64 or self.bytes > max_payload_bytes or !nz(self.checksum) or !nz(self.plan_digest)) return error.InvalidSourceControl;
    }
};
pub const Phase = enum { reserved, sealed, accepted, garbage };
pub const max_manifest_bytes = 64 * 1024;
pub const RecoveryManifest = struct {
    digest: Digest,
    bytes: []const u8,
    pub fn validate(self: RecoveryManifest) !void {
        if (!nz(self.digest) or self.bytes.len == 0 or self.bytes.len > max_manifest_bytes or !std.unicode.utf8ValidateSlice(self.bytes)) return error.InvalidSourceControl;
    }
};
pub const Record = struct {
    reservation: Digest,
    spec: Spec,
    phase: Phase = .reserved,
    stamp: ?Digest = null,
    retirement: ?Digest = null,
    recovery: ?RecoveryManifest = null,
    pub fn jsonStringify(self: Record, stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        inline for (@typeInfo(Record).@"struct".field_names) |name| {
            if (comptime !std.mem.eql(u8, name, "recovery")) {
                try stream.objectField(name);
                try stream.write(@field(self, name));
            } else if (self.recovery != null) {
                try stream.objectField(name);
                try stream.write(self.recovery);
            }
        }
        try stream.endObject();
    }
};
pub const Reclaimed = struct { reservation: Digest, spec: Spec, retirement: Digest, proof: Digest };
pub const State = struct {
    next_token: u64 = 0,
    current: ?Record = null,
    last_reclaimed: ?Reclaimed = null,
    pub fn chargedBytes(self: State) u64 {
        return if (self.current) |record| record.spec.bytes else 0;
    }
    pub fn validate(self: State) !void {
        if (self.current) |record| {
            try record.spec.validate();
            if (record.recovery) |manifest| try manifest.validate();
            if (!nz(record.reservation) or record.spec.token() != self.next_token) return error.InvalidSourceControl;
            if (record.stamp) |stamp| if (!nz(stamp)) return error.InvalidSourceControl;
            if (record.retirement) |proof| if (!nz(proof)) return error.InvalidSourceControl;
            switch (record.phase) {
                .reserved => if (record.stamp != null or record.retirement != null) return error.InvalidSourceControl,
                .sealed, .accepted => if (record.stamp == null or record.retirement != null) return error.InvalidSourceControl,
                .garbage => if (record.retirement == null) return error.InvalidSourceControl,
            }
        }
        if (self.last_reclaimed) |last| {
            try last.spec.validate();
            if (!nz(last.reservation) or !nz(last.retirement) or !nz(last.proof) or last.spec.token() > self.next_token) return error.InvalidSourceControl;
            if (self.current) |record| if (same(last.reservation, record.reservation) or last.spec.token() >= record.spec.token() or !same(last.spec.binding, record.spec.binding)) return error.InvalidSourceControl;
        }
        if (self.next_token != 0 and self.current == null and self.last_reclaimed == null) return error.InvalidSourceControl;
        if (self.next_token == 0 and (self.current != null or self.last_reclaimed != null)) return error.InvalidSourceControl;
    }
    pub fn reserve(self: State, reservation: Digest, spec: Spec, binding: Digest) !State {
        try self.validate();
        try spec.validate();
        if (!nz(reservation) or !same(spec.binding, binding)) return error.SourceBindingConflict;
        if (self.current) |record| {
            if (same(record.reservation, reservation) and std.meta.eql(record.spec, spec)) return self;
            return error.SourceAdmissionBackpressure;
        }
        if (spec.token() != (std.math.add(u64, self.next_token, 1) catch return error.SourceReceiptExpired)) return error.SourceReceiptExpired;
        var next = self;
        next.next_token = spec.token();
        next.current = .{ .reservation = reservation, .spec = spec };
        try next.validate();
        return next;
    }
    pub fn retainManifest(self: State, reservation: Digest, manifest: RecoveryManifest) !State {
        try self.validate();
        try manifest.validate();
        const record = self.current orelse return error.SourcePendingWork;
        if (!same(record.reservation, reservation)) return error.SourcePayloadConflict;
        if (record.recovery) |saved| {
            if (!same(saved.digest, manifest.digest) or !std.mem.eql(u8, saved.bytes, manifest.bytes)) return error.SourcePayloadConflict;
            return self;
        }
        if (record.phase != .reserved) return error.SourcePendingWork;
        var next = self;
        next.current.?.recovery = manifest;
        try next.validate();
        return next;
    }
    pub fn seal(self: State, reservation: Digest, stamp: Digest) !State {
        try self.validate();
        const record = self.current orelse return error.SourcePendingWork;
        if (!same(record.reservation, reservation) or !nz(stamp)) return error.SourcePayloadConflict;
        if (record.phase == .sealed and same(record.stamp.?, stamp)) return self;
        if (record.phase != .reserved) return error.SourcePendingWork;
        var next = self;
        next.current.?.phase = .sealed;
        next.current.?.stamp = stamp;
        try next.validate();
        return next;
    }
    pub fn accept(self: State, reservation: Digest) !State {
        try self.validate();
        const record = self.current orelse return error.SourcePendingWork;
        if (!same(record.reservation, reservation)) return error.SourcePayloadConflict;
        if (record.phase == .accepted) return self;
        if (record.phase != .sealed) return error.SourcePendingWork;
        var next = self;
        next.current.?.phase = .accepted;
        try next.validate();
        return next;
    }
    pub fn retire(self: State, reservation: Digest, proof: Digest) !State {
        try self.validate();
        const record = self.current orelse return error.SourcePendingWork;
        if (!same(record.reservation, reservation) or !nz(proof)) return error.SourcePayloadConflict;
        if (record.phase == .garbage) {
            if (!same(record.retirement.?, proof)) return error.SourcePayloadConflict;
            return self;
        }
        var next = self;
        next.current.?.phase = .garbage;
        next.current.?.retirement = proof;
        try next.validate();
        return next;
    }
    pub fn reclaim(self: State, reservation: Digest, proof: Digest) !State {
        try self.validate();
        if (!nz(proof)) return error.SourcePayloadConflict;
        if (self.last_reclaimed) |last| if (same(last.reservation, reservation)) {
            if (!same(last.proof, proof)) return error.SourcePayloadConflict;
            return self;
        };
        const record = self.current orelse {
            if (self.last_reclaimed) |last| if (same(last.reservation, reservation) and same(last.proof, proof)) return self;
            return error.SourcePayloadConflict;
        };
        if (!same(record.reservation, reservation) or record.phase != .garbage) return error.SourcePendingWork;
        var next = self;
        next.last_reclaimed = .{ .reservation = reservation, .spec = record.spec, .retirement = record.retirement.?, .proof = proof };
        next.current = null;
        try next.validate();
        return next;
    }
};
