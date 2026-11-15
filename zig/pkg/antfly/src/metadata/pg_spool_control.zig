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

//! Durable accounting for a slot's scoped immutable pgoutput uploads. The
//! metadata owner reserves before the first put and never returns capacity on
//! feedback alone. Reclamation proofs are trusted internal storage evidence.
const std = @import("std");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
pub const max_records = 256;
pub const max_storage_bytes: u64 = 2 * 1024 * 1024 * 1024;
fn nz(value: anytype) bool {
    return !std.mem.allEqual(u8, &value, 0);
}
fn same(a: anytype, b: @TypeOf(a)) bool {
    return std.mem.eql(u8, &a, &b);
}
pub const Scope = struct {
    namespace: Digest,
    domain: Digest,
    attempt: [16]u8,
    pub fn validate(self: Scope) !void {
        if (!nz(self.namespace) or !nz(self.domain) or std.mem.readInt(u64, self.attempt[0..8], .big) == 0 or !nz(self.attempt[8..16].*)) return error.InvalidChangefeed;
    }
    pub fn domainFor(feed: [16]u8, epoch: u64) Digest {
        var sha = std.crypto.hash.sha2.Sha256.init(.{});
        sha.update("antfly:pg-slot-spool-domain:v1");
        sha.update(&feed);
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, epoch, .big);
        sha.update(&bytes);
        return sha.finalResult();
    }
    pub fn storageIdentity(self: Scope) Digest {
        var sha = std.crypto.hash.sha2.Sha256.init(.{});
        sha.update("antfly:pg-spool-storage:v1");
        sha.update(&self.namespace);
        sha.update(&self.domain);
        sha.update(&self.attempt);
        return sha.finalResult();
    }
};
/// A frozen serialization plan, before any object upload. Its storage digest
/// binds the independently persisted scope. Completion is internal authority,
/// never a proof accepted from a public client.
pub const Spec = struct {
    feed: [16]u8,
    epoch: u64,
    definition: Digest,
    projection: Digest,
    storage: Digest,
    before: Digest,
    after: Digest,
    completion: Digest,
    start_lsn: u64,
    bytes: u64,
    commit_offset: u64,
    xid: u32,
    commit_time: i64,
    /// Frozen preflight identity of the exact length-framed pgoutput messages.
    /// Zero together admits legacy private reservations; runtime upload rejects it.
    wire: Digest = @splat(0),
    messages: u32 = 0,
    pub fn fromPlan(plan: anytype) Spec {
        return .{ .feed = plan.binding.feed, .epoch = plan.binding.epoch, .definition = plan.binding.definition, .projection = plan.binding.projection, .storage = plan.binding.storage, .before = plan.before, .after = plan.after, .completion = plan.completion, .start_lsn = plan.start_lsn, .bytes = plan.bytes, .commit_offset = plan.commit_offset, .xid = plan.xid, .commit_time = plan.commit_time };
    }
    pub fn matchesPlan(self: Spec, plan: anytype) bool {
        var expected = fromPlan(plan);
        expected.wire = self.wire;
        expected.messages = self.messages;
        return std.meta.eql(self, expected);
    }
    pub fn validate(self: Spec) !void {
        if (nz(self.wire) != (self.messages != 0) or (self.messages != 0 and (self.messages < 2 or self.messages > 65536))) return error.InvalidChangefeed;
        if (!nz(self.feed) or self.epoch == 0 or !nz(self.definition) or !nz(self.projection) or !nz(self.storage) or !nz(self.before) or !nz(self.after) or same(self.before, self.after) or !nz(self.completion) or self.start_lsn == 0 or self.bytes < 47 or self.bytes > 1024 * 1024 * 1024 or self.commit_offset != self.bytes - 26 or self.xid == 0) return error.InvalidChangefeed;
        _ = std.math.add(u64, self.start_lsn, self.bytes) catch return error.InvalidChangefeed;
    }
};
/// Conservative bound for unique payload objects, including leaf/index/manifest
/// bytes. The store separately owns journal/filesystem metadata overhead. This
/// remains charged for abandoned, acknowledged and partially uploaded attempts.
pub fn reservationBytes(bytes: u64) !u64 {
    if (bytes < 47 or bytes > 1024 * 1024 * 1024) return error.InvalidChangefeed;
    const messages: u64 = @min(bytes - 45, 65536);
    const parts = messages + bytes / (256 * 1024);
    var nodes: u64 = 3;
    var count = messages;
    for (0..3) |_| {
        count = (count + 63) / 64;
        nodes += count;
    }
    return bytes + 16 * messages + 32 * parts + 8 * nodes + 48 * (messages + nodes) + 304;
}
pub const max_cleanup_token_bytes = 2048;
pub const Cleanup = struct {
    revision: u64 = 0,
    /// Canonical base64url without padding; never a public cursor.
    continuation: []const u8 = "",
    fenced: bool = false,
    complete: bool = false,
    proof: Digest = @splat(0),
    pub fn validate(self: Cleanup) !void {
        if (self.continuation.len > max_cleanup_token_bytes) return error.InvalidChangefeed;
        for (self.continuation) |byte| if (!std.ascii.isAlphanumeric(byte) and byte != '-' and byte != '_') return error.InvalidChangefeed;
        const codec = std.base64.url_safe_no_pad;
        const length = codec.Decoder.calcSizeForSlice(self.continuation) catch return error.InvalidChangefeed;
        var decoded: [max_cleanup_token_bytes]u8 = undefined;
        codec.Decoder.decode(decoded[0..length], self.continuation) catch return error.InvalidChangefeed;
        var encoded: [max_cleanup_token_bytes]u8 = undefined;
        if (!std.mem.eql(u8, codec.Encoder.encode(&encoded, decoded[0..length]), self.continuation)) return error.InvalidChangefeed;
        if ((self.revision != 0 and !self.fenced) or self.complete != nz(self.proof) or (self.complete and !self.fenced) or (self.revision == 0 and (self.fenced or self.complete or self.continuation.len != 0))) return error.InvalidChangefeed;
    }
    pub fn eql(self: Cleanup, other: Cleanup) bool {
        return self.revision == other.revision and self.fenced == other.fenced and self.complete == other.complete and same(self.proof, other.proof) and std.mem.eql(u8, self.continuation, other.continuation);
    }
};
pub const Record = struct {
    id: u64,
    scope: Scope,
    spec: Spec,
    charged: u64,
    phase: enum { uploading, live, garbage } = .uploading,
    manifest: Digest = @splat(0),
    content: Digest = @splat(0),
    cleanup: Cleanup = .{},
    build: ?[16]u8 = null,
    pub fn validate(self: Record) !void {
        try self.scope.validate();
        try self.spec.validate();
        try self.cleanup.validate();
        if (self.build) |token| if (!nz(token)) return error.InvalidChangefeed;
        if (self.phase == .live and nz(self.spec.wire) and self.build == null) return error.InvalidChangefeed;
        if (self.phase != .garbage and self.cleanup.revision != 0) return error.InvalidChangefeed;
        if (self.id == 0 or std.mem.readInt(u64, self.scope.attempt[0..8], .big) != self.id or !same(self.scope.domain, Scope.domainFor(self.spec.feed, self.spec.epoch)) or !same(self.spec.storage, self.scope.storageIdentity()) or self.charged != try reservationBytes(self.spec.bytes)) return error.InvalidChangefeed;
        if (nz(self.manifest) != nz(self.content) or (self.phase == .uploading and nz(self.manifest)) or (self.phase == .live and !nz(self.manifest))) return error.InvalidChangefeed;
    }
};
pub const Ledger = struct {
    next_id: u64 = 1,
    collected_through: u64 = 0,
    charged_bytes: u64 = 0,
    /// First reservation permanently binds the slot serialization definition.
    projection: ?Digest = null,
    definition: ?Digest = null,
    records: []const Record = &.{},
    pub fn validate(self: Ledger) !void {
        if (self.next_id == 0 or self.collected_through >= self.next_id or self.records.len > max_records or self.charged_bytes > max_storage_bytes or (self.projection == null) != (self.definition == null) or (self.next_id == 1) != (self.projection == null)) return error.InvalidChangefeed;
        if (self.projection) |value| if (!nz(value) or !nz(self.definition.?)) return error.InvalidChangefeed;
        if (self.next_id - self.collected_through - 1 != self.records.len) return error.InvalidChangefeed;
        var charged: u64 = 0;
        var uploading: u32 = 0;
        for (self.records, 0..) |record, index| {
            try record.validate();
            if (index != 0 and record.cleanup.revision != 0) return error.InvalidChangefeed;
            if (record.id != self.collected_through + index + 1 or !same(record.spec.projection, self.projection.?) or !same(record.spec.definition, self.definition.?)) return error.InvalidChangefeed;
            if (record.phase == .uploading) uploading += 1;
            charged = std.math.add(u64, charged, record.charged) catch return error.InvalidChangefeed;
        }
        if (charged != self.charged_bytes or uploading > 1) return error.InvalidChangefeed;
    }
    pub fn find(self: Ledger, id: u64) ?Record {
        if (id <= self.collected_through or id >= self.next_id) return null;
        return self.records[@intCast(id - self.collected_through - 1)];
    }
    pub fn reserve(self: Ledger, a: A, id: u64, scope: Scope, spec: Spec) !Ledger {
        try self.validate();
        const record: Record = .{ .id = id, .scope = scope, .spec = spec, .charged = try reservationBytes(spec.bytes) };
        try record.validate();
        if (self.find(id)) |old| {
            if (!std.meta.eql(old.scope, scope) or !std.meta.eql(old.spec, spec) or old.phase == .garbage) return error.ChangefeedConflict;
            return self;
        }
        if (id != self.next_id or id == std.math.maxInt(u64)) return error.ChangefeedConflict;
        if (self.projection) |projection| if (!same(projection, spec.projection) or !same(self.definition.?, spec.definition)) return error.ChangefeedConflict;
        for (self.records) |old| if (old.phase == .uploading) return error.ChangefeedConflict;
        if (self.records.len == max_records or record.charged > max_storage_bytes - self.charged_bytes) return error.ChangefeedBackpressure;
        const records = try a.alloc(Record, self.records.len + 1);
        @memcpy(records[0..self.records.len], self.records);
        records[self.records.len] = record;
        var next = self;
        next.records = records;
        next.next_id += 1;
        next.charged_bytes += record.charged;
        next.projection = spec.projection;
        next.definition = spec.definition;
        return next;
    }
    pub fn begin(self: Ledger, a: A, id: u64, token: [16]u8) !Ledger {
        try self.validate();
        if (!nz(token)) return error.InvalidChangefeed;
        const old = self.find(id) orelse return error.ChangefeedConflict;
        if (old.phase != .uploading) return error.ChangefeedConflict;
        if (old.build) |owner| {
            if (!same(owner, token)) return error.ChangefeedConflict;
            return self;
        }
        const records = try a.dupe(Record, self.records);
        records[@intCast(id - self.collected_through - 1)].build = token;
        var next = self;
        next.records = records;
        return next;
    }
    pub fn publish(self: Ledger, a: A, id: u64, manifest: Digest, content: Digest) !Ledger {
        try self.validate();
        const old = self.find(id) orelse return error.ChangefeedConflict;
        if (!nz(manifest) or !nz(content) or old.phase == .garbage or (nz(old.spec.wire) and old.build == null)) return error.ChangefeedConflict;
        if (old.phase == .live) {
            if (!same(old.manifest, manifest) or !same(old.content, content)) return error.ChangefeedConflict;
            return self;
        }
        var next = self;
        const records = try a.dupe(Record, self.records);
        const index: usize = @intCast(id - self.collected_through - 1);
        records[index].phase = .live;
        records[index].manifest = manifest;
        records[index].content = content;
        next.records = records;
        return next;
    }
    pub fn abandon(self: Ledger, a: A, id: u64) !Ledger {
        try self.validate();
        const old = self.find(id) orelse return error.ChangefeedConflict;
        if (old.phase == .live) return error.ChangefeedConflict;
        if (old.phase == .garbage) return self;
        var next = self;
        const records = try a.dupe(Record, self.records);
        records[@intCast(id - self.collected_through - 1)].phase = .garbage;
        next.records = records;
        return next;
    }
    pub fn retire(self: Ledger, a: A, confirmed: u64, all: bool) !Ledger {
        try self.validate();
        var changed = false;
        for (self.records) |record| if (record.phase != .garbage and (all or (record.phase == .live and record.spec.start_lsn + record.spec.bytes <= confirmed))) {
            changed = true;
        };
        if (!changed) return self;
        const records = try a.dupe(Record, self.records);
        for (records) |*record| if (record.phase != .garbage and (all or (record.phase == .live and record.spec.start_lsn + record.spec.bytes <= confirmed))) {
            record.phase = .garbage;
        };
        var next = self;
        next.records = records;
        return next;
    }
    pub fn checkpoint(self: Ledger, a: A, id: u64, storage: Digest, expected: u64, progress: Cleanup) !Ledger {
        try self.validate();
        try progress.validate();
        if (self.records.len == 0 or self.records[0].id != id or self.records[0].phase != .garbage or !same(self.records[0].spec.storage, storage)) return error.ChangefeedConflict;
        const old = self.records[0].cleanup;
        if (progress.eql(old)) return self;
        if (old.complete or old.revision != expected or progress.revision != (std.math.add(u64, expected, 1) catch return error.ChangefeedConflict) or (old.fenced and !progress.fenced)) return error.ChangefeedConflict;
        const records = try a.dupe(Record, self.records);
        records[0].cleanup = progress;
        var next = self;
        next.records = records;
        return next;
    }
    /// A monotonic prefix floor makes lost cleanup responses idempotent with
    /// constant memory. Storage must durably fence late writes and reclaim the
    /// entire recorded attempt before supplying this internal completion proof.
    pub fn reclaimed(self: Ledger, a: A, id: u64, storage: Digest, proof: Digest) !Ledger {
        try self.validate();
        if (id == 0 or !nz(storage) or !nz(proof)) return error.InvalidChangefeed;
        if (id <= self.collected_through) return self;
        if (self.records.len == 0 or self.records[0].id != id or self.records[0].phase != .garbage or !same(self.records[0].spec.storage, storage) or !self.records[0].cleanup.complete or !same(self.records[0].cleanup.proof, proof)) return error.ChangefeedConflict;
        var next = self;
        next.records = try a.dupe(Record, self.records[1..]);
        next.collected_through = id;
        next.charged_bytes -= self.records[0].charged;
        return next;
    }
};
