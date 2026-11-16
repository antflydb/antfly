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

//! Private coordinator decision/admission observation from one native snapshot.
//! Absence never authorizes replay, presume-abort, or checkpoint advancement.
const std = @import("std");
pub const completion = @import("transaction_completion.zig");
const publication = @import("history_publication_wire.zig");
pub const max_request_bytes = 4096;
pub const max_reply_bytes = completion.max_bytes + 4096;
pub const Query = struct {
    version: u8 = 1,
    owner: completion.RoutedOwner,
    transaction_id: [16]u8,
    minimum_applied_index: u64 = 1,
    pub fn validate(self: Query) !void {
        try self.owner.validate();
        if (self.version != 1 or self.minimum_applied_index == 0 or std.mem.allEqual(u8, &self.transaction_id, 0)) return error.InvalidHistoryAdmission;
    }
    pub fn digest(self: Query) [32]u8 {
        var sha = std.crypto.hash.sha2.Sha256.init(.{});
        sha.update("antfly:history-completion-decision-read:v1");
        sha.update(&.{self.version});
        sha.update(&self.owner.digest());
        sha.update(&self.transaction_id);
        var raw: [8]u8 = undefined;
        std.mem.writeInt(u64, &raw, self.minimum_applied_index, .little);
        sha.update(&raw);
        return sha.finalResult();
    }
};
pub const Record = struct {
    status: enum { pending, committed, aborted },
    begin_timestamp: u64,
    created_at_ns: u64,
    commit_version: u64,
    finalized_at_ns: u64,
    prepared: bool,
    intents_resolved: bool,
    digest: [32]u8,
    /// Retained source ownership, independent of ordinary expiry/recovery jobs.
    source_purpose: ?[32]u8 = null,
    pub fn jsonStringify(self: Record, stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        inline for (@typeInfo(Record).@"struct".field_names) |name| {
            if (!std.mem.eql(u8, name, "source_purpose") or self.source_purpose != null) {
                try stream.objectField(name);
                try stream.write(@field(self, name));
            }
        }
        try stream.endObject();
    }
    pub fn validate(self: Record) !void {
        if (self.begin_timestamp == 0 or self.created_at_ns == 0 or std.mem.allEqual(u8, &self.digest, 0)) return error.HistoryCorrupt;
        if (self.source_purpose) |purpose| if (std.mem.allEqual(u8, &purpose, 0)) return error.HistoryCorrupt;
        switch (self.status) {
            .pending => if (self.commit_version != 0 or self.finalized_at_ns != 0 or self.intents_resolved) return error.HistoryCorrupt,
            .committed => if (self.commit_version <= self.begin_timestamp or self.finalized_at_ns == 0 or !self.prepared) return error.HistoryCorrupt,
            .aborted => if (self.commit_version != 0 or self.finalized_at_ns == 0) return error.HistoryCorrupt,
        }
    }
};
pub const Decision = struct {
    record: Record,
    admission: completion.AdmissionEnvelope,
    pub fn validate(self: Decision, query: Query) !void {
        try self.record.validate();
        try self.admission.admission.validate();
        const admission = self.admission.admission;
        if (admission.version != 3) return error.ChangefeedCapabilityUnsupported;
        if (!std.mem.eql(u8, &admission.transaction_id, &query.transaction_id) or
            !std.mem.eql(u8, &admission.coordinator_namespace, &query.owner.namespace) or
            !std.mem.eql(u8, &admission.coordinator_fence, &query.owner.digest()) or
            !std.mem.eql(u8, &self.admission.digest, &admission.digest())) return error.HistoryCorrupt;
        try admission.requireRoutedOwner(query.owner);
    }
    pub fn committedManifest(self: Decision) !completion.Manifest {
        try self.record.validate();
        try self.admission.admission.validate();
        if (self.admission.admission.version != 3) return error.ChangefeedCapabilityUnsupported;
        if (!std.mem.eql(u8, &self.admission.digest, &self.admission.admission.digest())) return error.HistoryCorrupt;
        if (self.record.status != .committed) return error.SourceOutcomeUnresolved;
        return self.admission.admission.manifest(self.record.commit_version);
    }
};
pub const Reply = struct {
    version: u8 = 1,
    request_digest: [32]u8,
    cut: publication.CompletionPublicationPosition,
    decision: ?Decision,
    pub fn validate(self: Reply, query: Query) !void {
        try query.validate();
        if (self.version != 1 or self.cut.term == 0 or self.cut.index < query.minimum_applied_index or !std.mem.eql(u8, &self.request_digest, &query.digest())) return error.HistoryCorrupt;
        if (self.decision) |decision| try decision.validate(query);
    }
};
pub const OwnedReply = std.json.Parsed(Reply);
pub fn decodeQuery(a: std.mem.Allocator, raw: []const u8) !Query {
    if (raw.len == 0 or raw.len > max_request_bytes) return error.InvalidHistoryAdmission;
    var parsed = std.json.parseFromSlice(Query, a, raw, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidHistoryAdmission;
    defer parsed.deinit();
    try parsed.value.validate();
    return parsed.value;
}
pub fn encodeReply(a: std.mem.Allocator, query: Query, reply: Reply) ![]u8 {
    try reply.validate(query);
    const raw = try std.json.Stringify.valueAlloc(a, reply, .{});
    errdefer a.free(raw);
    if (raw.len > max_reply_bytes) return error.HistoryTooLarge;
    return raw;
}
pub fn decodeReply(a: std.mem.Allocator, raw: []const u8, query: Query) !OwnedReply {
    if (raw.len == 0 or raw.len > max_reply_bytes) return error.HistoryCorrupt;
    var parsed = std.json.parseFromSlice(Reply, a, raw, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.HistoryCorrupt;
    errdefer parsed.deinit();
    try parsed.value.validate(query);
    return parsed;
}

test "history decision read binds retained owners record and actual cut with owned decoding" {
    const a = std.testing.allocator;
    const query: Query = .{ .owner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 7, .group_id = 9, .topology_epoch = 4 }, .transaction_id = @splat(3), .minimum_applied_index = 5 };
    const members = [_]completion.Participant{.{ .namespace = query.owner.namespace, .owner_fence = query.owner.digest(), .input_digest = @splat(4), .roles = completion.Roles.guards | completion.Roles.checkpoint, .locator = "original-checkpoint-owner", .routed_owner = query.owner }};
    const admission: completion.Admission = .{ .version = 3, .transaction_id = query.transaction_id, .admission_digest = @splat(5), .coordinator_namespace = query.owner.namespace, .coordinator_fence = query.owner.digest(), .participants = &members };
    var reply: Reply = .{ .request_digest = query.digest(), .cut = .{ .term = 2, .index = 6 }, .decision = .{ .record = .{ .status = .committed, .begin_timestamp = 10, .created_at_ns = 11, .commit_version = 20, .finalized_at_ns = 20, .prepared = true, .intents_resolved = true, .digest = @splat(6) }, .admission = .{ .admission = admission, .digest = admission.digest() } } };
    const ordinary = try encodeReply(a, query, reply);
    defer a.free(ordinary);
    try std.testing.expect(std.mem.indexOf(u8, ordinary, "source_purpose") == null);
    reply.decision.?.record.source_purpose = @splat(19);
    const source_owned = try encodeReply(a, query, reply);
    defer a.free(source_owned);
    var source_parsed = try decodeReply(a, source_owned, query);
    defer source_parsed.deinit();
    try std.testing.expectEqual(@as([32]u8, @splat(19)), source_parsed.value.decision.?.record.source_purpose.?);
    reply.decision.?.record.source_purpose = @splat(0);
    try std.testing.expectError(error.HistoryCorrupt, reply.validate(query));
    reply.decision.?.record.source_purpose = null;
    const raw = try encodeReply(a, query, reply);
    defer a.free(raw);
    var parsed = try decodeReply(a, raw, query);
    defer parsed.deinit();
    @memset(raw, 0);
    try std.testing.expectEqualStrings("original-checkpoint-owner", parsed.value.decision.?.admission.admission.participants[0].locator);
    try std.testing.expectEqual((try admission.manifest(20)).digest(), (try parsed.value.decision.?.committedManifest()).digest());
    var changed_query = query;
    changed_query.owner.root_generation += 1;
    try std.testing.expectError(error.HistoryCorrupt, reply.validate(changed_query));
    changed_query = query;
    changed_query.transaction_id[0] ^= 1;
    try std.testing.expectError(error.HistoryCorrupt, reply.validate(changed_query));
    reply.cut.index = 4;
    try std.testing.expectError(error.HistoryCorrupt, reply.validate(query));
    reply.cut.index = 6;
    reply.decision.?.admission.digest[0] ^= 1;
    try std.testing.expectError(error.HistoryCorrupt, reply.validate(query));
    try std.testing.expectError(error.HistoryCorrupt, reply.decision.?.committedManifest());
    reply.decision.?.admission.digest = admission.digest();
    reply.decision.?.record.status = .pending;
    reply.decision.?.record.commit_version = 0;
    reply.decision.?.record.finalized_at_ns = 0;
    reply.decision.?.record.intents_resolved = false;
    try reply.validate(query);
    try std.testing.expectError(error.SourceOutcomeUnresolved, reply.decision.?.committedManifest());
    reply.decision.?.record.status = .aborted;
    reply.decision.?.record.finalized_at_ns = 30;
    reply.decision.?.record.intents_resolved = true;
    try reply.validate(query);
    try std.testing.expectError(error.SourceOutcomeUnresolved, reply.decision.?.committedManifest());
    reply.decision = null;
    try reply.validate(query);
    const absent = try encodeReply(a, query, reply);
    defer a.free(absent);
    const duplicate = try std.fmt.allocPrint(a, "{s},\"version\":1}}", .{absent[0 .. absent.len - 1]});
    defer a.free(duplicate);
    try std.testing.expectError(error.HistoryCorrupt, decodeReply(a, duplicate, query));
    try std.testing.expectError(error.InvalidHistoryAdmission, decodeQuery(a, "{\"version\":1,\"version\":1}"));
    const request = try std.json.Stringify.valueAlloc(a, query, .{});
    defer a.free(request);
    try std.testing.expectEqualDeep(query, try decodeQuery(a, request));
    const Failure = struct {
        fn run(allocator: std.mem.Allocator, bytes: []const u8, input: Query) !void {
            var owned = try decodeReply(allocator, bytes, input);
            defer owned.deinit();
            try std.testing.expect(owned.value.decision == null);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Failure.run, .{ absent, query });
}
