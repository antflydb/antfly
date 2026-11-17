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

//! Durable physical-root producer closure. Classification comes from trusted
//! ingress, never row shape or public request JSON. Closing this policy is not
//! proof that old transaction intents or already accepted work have drained.
const std = @import("std");
const scope = @import("source_scope.zig");
const A = std.mem.Allocator;
pub const key = "\x00\x00__metadata__:source_producer:closure_v1";
pub const max_bytes = 4096;
/// An absent old transport label must remain unlabelled through forwarding.
/// Only authenticated ordinary ingress may attach ordinary. Scoped ingress
/// separately validates its retained source scope and original admission.
pub const Producer = enum { unlabelled, ordinary, legacy_cdc, scoped_source };
/// Merge explicit native admission with the retained private row envelope.
/// Neither a missing label nor a conflicting label upgrades another origin.
pub fn rowProducer(request: Producer, admission: Producer) !Producer {
    if (request == .scoped_source) return error.InvalidSourceAdmission;
    if (request != .unlabelled and admission != .unlabelled and request != admission) return error.SourceTransactionIdentityConflict;
    return if (request != .unlabelled) request else admission;
}
pub const Closure = struct {
    version: u8 = 1,
    root: scope.Basis,
    first_closed_at: scope.Cut,
    pub fn validate(self: Closure) !void {
        try self.root.validate();
        if (self.version != 1 or self.first_closed_at.term == 0 or self.first_closed_at.index == 0) return error.SourceAdmissionCorrupt;
    }
};
/// Serving owners establish ReadIndex and verify the full catalog owner
/// before and after this native snapshot. A lower bound is never an ACK.
pub const Query = struct {
    version: u8,
    owner: @import("transaction_completion.zig").RoutedOwner,
    minimum_applied_index: u64,
    pub fn validate(self: Query) !void {
        try self.owner.validate();
        if (self.version != 1 or self.minimum_applied_index == 0) return error.InvalidSourceAdmission;
    }
};
pub const max_query_bytes = 4096;
pub const max_observation_bytes = 8192;
pub const Observation = struct {
    version: u8,
    owner: @import("transaction_completion.zig").RoutedOwner,
    cut: scope.Cut,
    /// Absence is not a transaction outcome or a legacy-drain proof.
    closure: ?Closure,
    pub fn validate(self: Observation, query: Query) !void {
        try query.validate();
        if (self.version != 1 or !std.meta.eql(self.owner, query.owner) or self.cut.term == 0 or self.cut.index < query.minimum_applied_index) return error.SourceAdmissionCorrupt;
        if (self.closure) |closed| {
            try closed.validate();
            if (!std.meta.eql(closed.root, try scope.Basis.fromOwner(query.owner)) or closed.first_closed_at.index > self.cut.index or closed.first_closed_at.term > self.cut.term or (closed.first_closed_at.index == self.cut.index and closed.first_closed_at.term != self.cut.term)) return error.SourceAdmissionCorrupt;
        }
    }
};
pub fn observe(a: A, txn: anytype, query: Query, applied: scope.Cut) !Observation {
    try query.validate();
    const result: Observation = .{ .version = 1, .owner = query.owner, .cut = applied, .closure = try Store.read(a, txn, try scope.Basis.fromOwner(query.owner)) };
    try result.validate(query);
    return result;
}
pub fn encodeObservation(a: A, query: Query, observed: Observation) ![]u8 {
    try observed.validate(query);
    const bytes = try std.json.Stringify.valueAlloc(a, observed, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_observation_bytes) return error.SourceAdmissionCorrupt;
    return bytes;
}
pub fn decodeObservation(a: A, bytes: []const u8, query: Query) !Observation {
    try query.validate();
    if (bytes.len == 0 or bytes.len > max_observation_bytes) return error.SourceAdmissionCorrupt;
    var parsed = std.json.parseFromSlice(Observation, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer parsed.deinit();
    try parsed.value.validate(query);
    return parsed.value;
}
const Envelope = struct { closure: Closure, checksum: scope.Digest };
fn digest(a: A, closure: Closure) !scope.Digest {
    const bytes = try std.json.Stringify.valueAlloc(a, closure, .{});
    defer a.free(bytes);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:source-producer-closure:v1");
    hash.update(bytes);
    return hash.finalResult();
}
pub const Store = struct {
    pub fn read(a: A, txn: anytype, root: scope.Basis) !?Closure {
        try root.validate();
        const closure = (try readStored(a, txn)) orelse return null;
        if (!std.meta.eql(closure.root, root)) return error.HistoryResynchronizationRequired;
        return closure;
    }
    pub fn readStored(a: A, txn: anytype) !?Closure {
        const bytes = txn.get(key) catch |err| return if (err == error.NotFound) null else err;
        if (bytes.len == 0 or bytes.len > max_bytes) return error.SourceAdmissionCorrupt;
        var parsed = std.json.parseFromSlice(Envelope, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
        defer parsed.deinit();
        const closure = parsed.value.closure;
        try closure.validate();
        if (!std.meta.eql(parsed.value.checksum, try digest(a, closure))) return error.SourceAdmissionCorrupt;
        return closure;
    }
    /// The trusted ordered caller validates its actual physical owner/entry,
    /// holds the apply fence, and stages the real apply marker in this writer.
    /// The first closure cut is immutable; later closure cannot reopen intake.
    pub fn close(a: A, txn: anytype, root: scope.Basis, entry: scope.Cut) !Closure {
        try root.validate();
        if (entry.term == 0 or entry.index == 0) return error.InvalidSourceAdmission;
        if (try read(a, txn, root)) |closed| {
            if (entry.term < closed.first_closed_at.term or entry.index < closed.first_closed_at.index or (entry.index == closed.first_closed_at.index and entry.term != closed.first_closed_at.term)) return error.ConflictingOrderedApplyReceipt;
            return closed;
        }
        const closure: Closure = .{ .root = root, .first_closed_at = entry };
        const bytes = try std.json.Stringify.valueAlloc(a, Envelope{ .closure = closure, .checksum = try digest(a, closure) }, .{});
        defer a.free(bytes);
        if (bytes.len > max_bytes) return error.SourceAdmissionCorrupt;
        try txn.put(key, bytes);
        return closure;
    }
    pub fn requirePhysical(a: A, txn: anytype, namespace: [24]u8, generation: u64, producer: Producer) !void {
        const closed = (try readStored(a, txn)) orelse return;
        if (!std.meta.eql(closed.root.namespace, namespace) or closed.root.root_generation != generation) return error.HistoryResynchronizationRequired;
        try requireProducer(producer);
    }
    fn requireProducer(producer: Producer) !void {
        switch (producer) {
            .unlabelled, .legacy_cdc => return error.SourcePendingWork,
            .ordinary, .scoped_source => {},
        }
    }
    /// Must run inside the same writer as the attempted effects, before those
    /// effects. There is deliberately no older-cut bypass: ordered replay must
    /// be recognized by the caller's durable marker before entering mutation.
    pub fn require(a: A, txn: anytype, root: scope.Basis, producer: Producer) !void {
        if (try read(a, txn, root) == null) return;
        try requireProducer(producer);
    }
};
