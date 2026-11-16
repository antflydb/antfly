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

//! Private request-bound reads of immutable local completion receipts. Absence
//! at a qualified snapshot is unproven, never a zero-effect outcome or a grant
//! to replay an uncertain mutation. A Raft caller establishes ReadIndex first.
const std = @import("std");
pub const completion = @import("transaction_completion.zig");
pub const max_bytes = 4096;
const A = std.mem.Allocator;
fn nz(value: anytype) bool {
    return !std.mem.allEqual(u8, &value, 0);
}
fn equal(left: anytype, right: @TypeOf(left)) bool {
    return std.mem.eql(u8, &left, &right);
}
pub const Request = struct {
    version: u8 = 1,
    authority: enum(u8) { native = 1, raft = 2 },
    namespace: completion.Namespace,
    transaction_id: [16]u8,
    owner_fence: completion.Digest,
    manifest_digest: completion.Digest,
    commit_version: u64,
    minimum_applied_index: u64 = 0,
    minimum_native_sequence: u64 = 0,
    pub fn validate(self: Request) !void {
        if (self.version != 1 or !nz(self.namespace) or !nz(self.transaction_id) or !nz(self.owner_fence) or !nz(self.manifest_digest) or self.commit_version == 0) return error.InvalidHistoryAdmission;
        if (self.authority == .native) {
            if (self.minimum_applied_index != 0) return error.InvalidHistoryAdmission;
        } else if (self.minimum_applied_index == 0 or self.minimum_native_sequence != 0) return error.InvalidHistoryAdmission;
    }
    pub fn digest(self: Request) completion.Digest {
        var sha = std.crypto.hash.sha2.Sha256.init(.{});
        sha.update("antfly:history-completion-resolution-read:v1");
        sha.update(&.{ self.version, @backingInt(self.authority) });
        sha.update(&self.namespace);
        sha.update(&self.transaction_id);
        sha.update(&self.owner_fence);
        sha.update(&self.manifest_digest);
        for ([_]u64{ self.commit_version, self.minimum_applied_index, self.minimum_native_sequence }) |value| {
            var raw: [8]u8 = undefined;
            std.mem.writeInt(u64, &raw, value, .little);
            sha.update(&raw);
        }
        return sha.finalResult();
    }
};
pub const Cut = struct { native_sequence: u64 = 0, term: u64 = 0, index: u64 = 0 };
pub const Reply = struct {
    version: u8 = 1,
    request_digest: completion.Digest,
    cut: Cut,
    resolution: ?completion.Resolution = null,
    pub fn init(request: Request, cut: Cut, resolution: ?completion.Resolution) !Reply {
        const result: Reply = .{ .request_digest = request.digest(), .cut = cut, .resolution = resolution };
        try result.validate(request);
        return result;
    }
    pub fn validate(self: Reply, request: Request) !void {
        try request.validate();
        if (self.version != 1 or !equal(self.request_digest, request.digest())) return error.HistoryCorrupt;
        switch (request.authority) {
            .native => if (self.cut.term != 0 or self.cut.index != 0 or self.cut.native_sequence < request.minimum_native_sequence) return error.HistoryCorrupt,
            .raft => if (self.cut.native_sequence != 0 or self.cut.term == 0 or self.cut.index < request.minimum_applied_index) return error.HistoryCorrupt,
        }
        if (self.resolution) |receipt| {
            try receipt.validateStructural();
            if (!equal(receipt.namespace, request.namespace) or !equal(receipt.owner_fence, request.owner_fence) or !equal(receipt.manifest_digest, request.manifest_digest) or receipt.commit_version != request.commit_version) return error.HistoryCorrupt;
            switch (request.authority) {
                .native => if (receipt.source_position[31] != 2 or std.mem.readInt(u64, receipt.source_position[0..8], .little) > self.cut.native_sequence) return error.HistoryCorrupt,
                .raft => {
                    const term = std.mem.readInt(u64, receipt.source_position[0..8], .little);
                    const index = std.mem.readInt(u64, receipt.source_position[8..16], .little);
                    if (receipt.source_position[31] != 1 or index > self.cut.index or term > self.cut.term or (index == self.cut.index and term != self.cut.term)) return error.HistoryCorrupt;
                },
            }
        }
    }
};
/// Collects outputs of trusted, cut-qualified owner reads. Wire validation
/// does not authenticate a producer: the caller must use the frozen owner read
/// port and preserve its original routing, authorization and deadline checks.
/// The complete plan is owned so recovery collection cannot borrow a newer
/// routing projection or a caller's temporary manifest buffers.
pub const Assembly = struct {
    allocator: A,
    plan: std.json.Parsed(completion.AdmissionEnvelope),
    commit_version: u64,
    receipts: []?completion.Resolution,
    received: usize = 0,

    pub fn init(a: A, frozen: completion.Manifest) !Assembly {
        try frozen.validate();
        const raw = try completion.encodeAdmission(a, frozen.admission());
        defer a.free(raw);
        var plan = try completion.parseAdmission(a, raw);
        errdefer plan.deinit();
        const receipts = try a.alloc(?completion.Resolution, frozen.participants.len);
        @memset(receipts, null);
        return .{ .allocator = a, .plan = plan, .commit_version = frozen.commit_version, .receipts = receipts };
    }
    pub fn deinit(self: *Assembly) void {
        self.allocator.free(self.receipts);
        self.plan.deinit();
        self.* = undefined;
    }
    pub fn manifest(self: *const Assembly) completion.Manifest {
        return self.plan.value.admission.manifest(self.commit_version) catch unreachable;
    }
    /// Absence is unproven and does not clear a previously qualified receipt.
    /// Repeated identical receipts are idempotent even at a stronger read cut.
    pub fn add(self: *Assembly, request: Request, reply: Reply) !bool {
        try reply.validate(request);
        const frozen = self.manifest();
        if (!equal(request.transaction_id, frozen.transaction_id) or !equal(request.manifest_digest, frozen.digest()) or request.commit_version != frozen.commit_version) return error.HistoryCorrupt;
        var low: usize = 0;
        var high = frozen.participants.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            switch (std.mem.order(u8, &frozen.participants[middle].namespace, &request.namespace)) {
                .lt => low = middle + 1,
                .gt => high = middle,
                .eq => {
                    const participant = frozen.participants[middle];
                    if (!equal(request.owner_fence, participant.owner_fence)) return error.HistoryCorrupt;
                    const receipt = reply.resolution orelse return false;
                    try receipt.validate(frozen);
                    if (self.receipts[middle]) |previous| {
                        if (!equal(previous.digest(), receipt.digest())) return error.DecisionConflict;
                    } else {
                        self.receipts[middle] = receipt;
                        self.received += 1;
                    }
                    return true;
                },
            }
        }
        return error.InvalidParticipant;
    }
    pub fn encode(self: *const Assembly, a: A) ![]u8 {
        if (self.received != self.receipts.len) return error.SourceOutcomeUnresolved;
        const receipts = try a.alloc(completion.Resolution, self.receipts.len);
        defer a.free(receipts);
        for (self.receipts, receipts) |receipt, *destination| destination.* = receipt orelse return error.SourceOutcomeUnresolved;
        return completion.encode(a, .{ .manifest = self.manifest(), .resolutions = receipts });
    }
};

pub fn decodeRequest(a: A, raw: []const u8) !Request {
    if (raw.len == 0 or raw.len > max_bytes) return error.InvalidHistoryAdmission;
    var parsed = std.json.parseFromSlice(Request, a, raw, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidHistoryAdmission;
    defer parsed.deinit();
    try parsed.value.validate();
    return parsed.value;
}
pub fn decodeReply(a: A, raw: []const u8, request: Request) !Reply {
    if (raw.len == 0 or raw.len > max_bytes) return error.HistoryCorrupt;
    var parsed = std.json.parseFromSlice(Reply, a, raw, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.HistoryCorrupt;
    defer parsed.deinit();
    try parsed.value.validate(request);
    return parsed.value;
}
/// The caller validates authority, namespace and the persisted read cut in this
/// same transaction. Retained receipt integrity does not depend on a surviving
/// primary coordinator record or a current-row lookup.
pub fn read(a: A, txn: anytype, request: Request, cut: Cut) !Reply {
    try request.validate();
    const raw = txn.get(&completion.resolutionKey(request.transaction_id)) catch |err| switch (err) {
        error.NotFound => return Reply.init(request, cut, null),
        else => return err,
    };
    var stored = try completion.parseStoredResolution(a, raw);
    defer stored.deinit();
    return Reply.init(request, cut, stored.value.resolution);
}

test "history resolution wire binds identity authority and actual read cut without inventing absent outcomes" {
    const a = std.testing.allocator;
    var request: Request = .{ .authority = .raft, .namespace = @splat(1), .transaction_id = @splat(2), .owner_fence = @splat(3), .manifest_digest = @splat(4), .commit_version = 5, .minimum_applied_index = 7 };
    var position: completion.Digest = @splat(0);
    std.mem.writeInt(u64, position[0..8], 2, .little);
    std.mem.writeInt(u64, position[8..16], 7, .little);
    position[31] = 1;
    const receipt: completion.Resolution = .{ .namespace = request.namespace, .owner_fence = request.owner_fence, .manifest_digest = request.manifest_digest, .commit_version = request.commit_version, .source_position = position, .outcome = .{ .no_effect = @splat(6) } };
    const reply = try Reply.init(request, .{ .term = 2, .index = 8 }, receipt);
    const encoded = try std.json.Stringify.valueAlloc(a, reply, .{});
    defer a.free(encoded);
    try std.testing.expectEqualDeep(reply, try decodeReply(a, encoded, request));
    var wrong = request;
    wrong.transaction_id[0] ^= 1;
    try std.testing.expectError(error.HistoryCorrupt, reply.validate(wrong));
    wrong = request;
    wrong.owner_fence[0] ^= 1;
    try std.testing.expectError(error.HistoryCorrupt, Reply.init(wrong, reply.cut, receipt));
    try std.testing.expectError(error.HistoryCorrupt, Reply.init(request, .{ .term = 2, .index = 6 }, receipt));
    try std.testing.expectError(error.HistoryCorrupt, Reply.init(request, .{ .term = 1, .index = 8 }, receipt));
    const absent = try Reply.init(request, reply.cut, null);
    try std.testing.expect(absent.resolution == null);
    request.authority = .native;
    request.minimum_applied_index = 0;
    try std.testing.expectError(error.HistoryCorrupt, Reply.init(request, .{ .native_sequence = 99 }, receipt));
    try std.testing.expect((try Reply.init(request, .{}, null)).resolution == null);
}

test "completion receipt assembly requires every qualified frozen participant" {
    const a = std.testing.allocator;
    const participants = [_]completion.Participant{
        .{ .namespace = @splat(1), .owner_fence = @splat(3), .roles = completion.Roles.effects, .locator = "one", .input_digest = @splat(7) },
        .{ .namespace = @splat(2), .owner_fence = @splat(4), .roles = completion.Roles.guards | completion.Roles.checkpoint, .locator = "two", .input_digest = @splat(8) },
    };
    const manifest: completion.Manifest = .{ .version = 2, .transaction_id = @splat(5), .admission_digest = @splat(6), .coordinator_namespace = participants[0].namespace, .coordinator_fence = participants[0].owner_fence, .commit_version = 10, .participants = &participants };
    var collection = try Assembly.init(a, manifest);
    defer collection.deinit();
    var position: completion.Digest = @splat(0);
    std.mem.writeInt(u64, position[0..8], 1, .little);
    std.mem.writeInt(u64, position[8..16], 7, .little);
    position[31] = 1;
    const effect: completion.Resolution = .{ .namespace = participants[0].namespace, .owner_fence = participants[0].owner_fence, .manifest_digest = manifest.digest(), .commit_version = 10, .source_position = position, .outcome = .{ .effects = .{ .sequence = 1, .frame_digest = @splat(9) } } };
    const empty: completion.Resolution = .{ .namespace = participants[1].namespace, .owner_fence = participants[1].owner_fence, .manifest_digest = manifest.digest(), .commit_version = 10, .source_position = position, .outcome = .{ .no_effect = completion.noEffectProof(manifest, participants[1].namespace, position) } };
    const first: Request = .{ .authority = .raft, .namespace = effect.namespace, .transaction_id = manifest.transaction_id, .owner_fence = effect.owner_fence, .manifest_digest = manifest.digest(), .commit_version = 10, .minimum_applied_index = 1 };
    var second = first;
    second.namespace = empty.namespace;
    second.owner_fence = empty.owner_fence;
    const cut: Cut = .{ .term = 1, .index = 7 };
    try std.testing.expect(!try collection.add(first, try Reply.init(first, cut, null)));
    try std.testing.expectError(error.SourceOutcomeUnresolved, collection.encode(a));
    try std.testing.expect(try collection.add(second, try Reply.init(second, cut, empty)));
    try std.testing.expect(try collection.add(second, try Reply.init(second, cut, empty)));
    try std.testing.expectEqual(@as(usize, 1), collection.received);
    try std.testing.expectError(error.SourceOutcomeUnresolved, collection.encode(a));
    var foreign = first;
    foreign.transaction_id[0] ^= 1;
    try std.testing.expectError(error.HistoryCorrupt, collection.add(foreign, try Reply.init(foreign, cut, null)));
    foreign = first;
    foreign.owner_fence[0] ^= 1;
    try std.testing.expectError(error.HistoryCorrupt, collection.add(foreign, try Reply.init(foreign, cut, null)));
    foreign = first;
    foreign.namespace = @splat(11);
    try std.testing.expectError(error.InvalidParticipant, collection.add(foreign, try Reply.init(foreign, cut, null)));
    var forged = try Reply.init(first, cut, effect);
    forged.request_digest[0] ^= 1;
    try std.testing.expectError(error.HistoryCorrupt, collection.add(first, forged));
    try std.testing.expectEqual(@as(usize, 1), collection.received);
    try std.testing.expect(try collection.add(first, try Reply.init(first, cut, effect)));
    try std.testing.expect(!try collection.add(first, try Reply.init(first, cut, null)));
    var changed = effect;
    changed.outcome.effects.frame_digest[0] ^= 1;
    try std.testing.expectError(error.DecisionConflict, collection.add(first, try Reply.init(first, cut, changed)));
    const raw = try collection.encode(a);
    defer a.free(raw);
    var parsed = try completion.parse(a, raw);
    defer parsed.deinit();
    try std.testing.expectEqualDeep(effect, parsed.value.certificate.resolutions[0]);
    try std.testing.expectEqualDeep(empty, parsed.value.certificate.resolutions[1]);
}
