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

//! Trusted owner commands for bounded frozen-plan staging. Public batch JSON
//! cannot construct these commands. Transport adapters qualify the root before
//! invoking the native owner, and ordered apply supplies the real entry cut.
const std = @import("std");
pub const storage = @import("source_plan_storage.zig");
const A = std.mem.Allocator;
pub const Digest = storage.Digest;
pub const Request = struct {
    root: storage.Root,
    spec: storage.Spec,
    action: enum { begin, chunk, seal, fence, reclaim },
    ordinal: u16 = 0,
    chunk: []const u8 = "",
    max_chunks: u8 = storage.max_reclaim_chunks,
    pub fn validate(self: Request) !void {
        try self.spec.validate(self.root);
        if (self.action == .chunk) {
            if (self.chunk.len != try self.spec.chunkLen(self.ordinal) or !std.mem.eql(u8, &storage.chunkDigest(self.ordinal, self.chunk), &self.spec.chunk_hashes[self.ordinal])) return error.SourcePayloadConflict;
        } else if (self.ordinal != 0 or self.chunk.len != 0) return error.InvalidSourceAdmission;
        if (self.max_chunks == 0 or self.max_chunks > storage.max_reclaim_chunks or (self.action != .reclaim and self.max_chunks != storage.max_reclaim_chunks)) return error.InvalidSourceAdmission;
    }
    pub fn digest(self: Request) Digest {
        return valueDigest("antfly:source-plan-command:v1", self);
    }
};
pub const Rejection = enum {
    invalid,
    scope_changed,
    expired,
    pending,
    conflict,
    backpressure,
    pub fn asError(self: Rejection) anyerror {
        return switch (self) {
            .invalid => error.InvalidSourceAdmission,
            .scope_changed => error.HistoryResynchronizationRequired,
            .expired => error.SourceReceiptExpired,
            .pending => error.SourcePendingWork,
            .conflict => error.SourcePayloadConflict,
            .backpressure => error.SourceAdmissionBackpressure,
        };
    }
    pub fn fromError(err: anyerror) ?Rejection {
        return switch (err) {
            error.InvalidSourceAdmission, error.InvalidTxnRecord => .invalid,
            error.IdentityNamespaceMismatch, error.OnlineSourceScopeChanged, error.HistoryResynchronizationRequired => .scope_changed,
            error.SourceReceiptExpired => .expired,
            error.SourcePendingWork, error.IntegrityTopologyBusy, error.ArtifactReconcileInProgress => .pending,
            error.SourcePayloadConflict => .conflict,
            error.SourceAdmissionBackpressure, error.HistoryTooLarge => .backpressure,
            else => null,
        };
    }
};
pub const Outcome = union(enum) { applied: storage.Status, rejected: Rejection, superseded: void };
pub const Entry = struct { term: u64, index: u64 };
pub const Observation = struct {
    root: storage.Root,
    cut: Digest,
    quota: storage.Quota,
    status: ?storage.Status = null,
};
pub const Query = struct {
    root: storage.Root,
    spec: ?storage.Spec = null,
    minimum_applied_index: u64 = 0,
    minimum_native_sequence: u64 = 0,
    pub fn validate(self: Query) !void {
        try self.root.validate();
        if (self.spec) |spec| try spec.validate(self.root);
        switch (self.root.authority) {
            .native => if (self.minimum_applied_index != 0) return error.InvalidSourceAdmission,
            .raft => if (self.minimum_applied_index == 0 or self.minimum_native_sequence != 0) return error.InvalidSourceAdmission,
        }
    }
};
pub const ReadRequest = struct {
    query: Query,
    seal: Digest,
    ordinal: u16,
    pub fn validate(self: ReadRequest) !void {
        try self.query.validate();
        const spec = self.query.spec orelse return error.InvalidSourceAdmission;
        _ = try spec.chunkLen(self.ordinal);
        if (std.mem.allEqual(u8, &self.seal, 0)) return error.InvalidSourceAdmission;
    }
};
pub const Read = struct { observation: Observation, bytes: []u8 };
pub const Recovery = struct { request: Request, entry: Entry, minimum_applied_index: u64 };
pub fn apply(a: A, txn: anytype, request: Request, position: Digest) !storage.Status {
    try request.validate();
    switch (request.action) {
        .begin => try storage.stageBegin(a, txn, request.root, request.spec),
        .chunk => try storage.stageChunk(a, txn, request.root, request.spec, request.ordinal, request.chunk),
        .seal => _ = try storage.stageSeal(a, txn, request.root, request.spec, position),
        .fence => _ = try storage.stageFence(a, txn, request.root, request.spec, position),
        .reclaim => _ = try storage.stageReclaim(a, txn, request.root, request.spec, request.max_chunks),
    }
    return storage.inspect(a, txn, request.root, request.spec);
}
// Only the latest command outcome is retained. A superseded response requires
// a qualified status read; it never authorizes blind replay or reclamation.
const key = "\x00\x00__source_plan_artifact__:ordered_outcome";
const Record = struct { namespace: [24]u8, generation: u64, entry: Entry, command: Digest, outcome: Outcome };
const Envelope = struct { record: Record, checksum: Digest };
const max_record_bytes = 4096;
fn valueDigest(domain: []const u8, value: anytype) Digest {
    // Bounded command hashing streams JSON without constructing a second chunk.
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
pub fn stageOutcome(a: A, txn: anytype, request: Request, entry: Entry, outcome: Outcome) !void {
    if (entry.term == 0 or entry.index == 0 or outcome == .superseded) return error.InvalidSourceAdmission;
    const record: Record = .{ .namespace = request.root.namespace, .generation = request.root.generation, .entry = entry, .command = request.digest(), .outcome = outcome };
    const bytes = try std.json.Stringify.valueAlloc(a, Envelope{ .record = record, .checksum = valueDigest("antfly:source-plan-outcome:v1", record) }, .{});
    defer a.free(bytes);
    if (bytes.len > max_record_bytes) return error.SourceAdmissionCorrupt;
    try txn.put(key, bytes);
}
pub fn replay(a: A, txn: anytype, request: Request, entry: Entry) !Outcome {
    if (entry.term == 0 or entry.index == 0) return error.InvalidSourceAdmission;
    const bytes = txn.get(key) catch |err| return if (err == error.NotFound) .superseded else err;
    if (bytes.len > max_record_bytes) return error.SourceAdmissionCorrupt;
    var parsed = std.json.parseFromSlice(Envelope, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer parsed.deinit();
    const record = parsed.value.record;
    if (!std.mem.eql(u8, &parsed.value.checksum, &valueDigest("antfly:source-plan-outcome:v1", record)) or record.entry.term == 0 or record.entry.index == 0 or record.outcome == .superseded) return error.SourceAdmissionCorrupt;
    if (!std.mem.eql(u8, &record.namespace, &request.root.namespace) or record.generation != request.root.generation) return error.HistoryResynchronizationRequired;
    if (record.entry.index != entry.index) return .superseded;
    if (record.entry.term != entry.term or !std.mem.eql(u8, &record.command, &request.digest())) return error.ConflictingOrderedApplyReceipt;
    return record.outcome;
}
