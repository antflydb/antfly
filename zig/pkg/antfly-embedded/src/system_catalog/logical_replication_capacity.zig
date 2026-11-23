// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Read-only participant capacity observation. No source clock is advanced;
//! OCC pins only affected before-images and frozen schema/roster/request.
const std = @import("std");
const capture = @import("logical_replication_capture.zig");
const publisher = @import("logical_replication_publisher.zig");
const namespace = @import("../storage/db/doc_identity_namespace.zig");
const owner = @import("../storage/transaction_decision_owner.zig");
pub const max_request_bytes = @import("../storage/db/table_catalog.zig").default_transaction_admission_bytes + publisher.max_roster_bytes + 64 * 1024;
pub const Request = struct {
    demand: capture.Demand,
    intent: @import("../storage/db/types.zig").TransactionIntentRequest,
    /// Exact private publication-prepare frame. The owner decoder recovers the
    /// same typed intent, including opaque keys and lossless JSON values.
    frame: []const u8,
    pub fn init(a: std.mem.Allocator, demand: capture.Demand, intent: @import("../storage/db/types.zig").TransactionIntentRequest) !Request {
        if (intent.publication_capture != null) return error.InvalidLogicalCaptureControl;
        var batch = intent.asBatch(demand.transaction_id, demand.topology_epoch);
        batch.transaction.?.prepare.publication_capture = demand;
        const frame = try @import("../api/batch_internal.zig").encodeBatchRequest(a, batch);
        return .{ .demand = demand, .intent = intent, .frame = frame };
    }
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginWriteRaw();
        try writer.writer.writeAll(self.frame);
        writer.endWriteRaw();
    }
    pub fn jsonParse(a: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) std.json.ParseError(@TypeOf(source.*))!Request {
        var lossless = options;
        lossless.parse_numbers = false;
        return jsonParseFromValue(a, try std.json.innerParse(std.json.Value, a, source, lossless), lossless);
    }
    pub fn jsonParseFromValue(a: std.mem.Allocator, value: std.json.Value, _: std.json.ParseOptions) std.json.ParseFromValueError!Request {
        const frame = try std.json.Stringify.valueAlloc(a, value, .{});
        // All child parser allocations belong to this enclosing JSON arena;
        // only borrowed typed views survive until that arena's deinit.
        const parsed = @import("../api/batch_internal.zig").parseInternalBatchRequest(a, frame) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.UnexpectedToken,
        };
        const command = parsed.req.transaction orelse return error.UnexpectedToken;
        if (command != .prepare or command.prepare.publication_capture == null) return error.UnexpectedToken;
        var intent = @import("../storage/db/types.zig").TransactionIntentRequest.fromBatch(parsed.req) catch return error.UnexpectedToken;
        intent.publication_capture = null;
        const demand = command.prepare.publication_capture.?;
        return .{ .demand = demand, .intent = intent, .frame = frame };
    }
};
const Position = @import("../storage/db/receipt_position.zig").Position;

/// Signed after ABORT against an image with an exact late-prepare fence and
/// no capture locator, header or terminal row for this grant.
pub const AbortAbsence = struct {
    owner: owner.External,
    transaction_id: [16]u8,
    member: u16,
    group_id: u64,
    namespace: namespace.Namespace,
    physical_root_incarnation: u128,
    position: @FieldType(Position, "raft"),
    capacity_digest: [32]u8,
    evidence: AbortFence,

    pub fn validate(self: @This()) !void {
        try self.owner.validate();
        switch (self.evidence) {
            .aborted_record => |digest| if (std.mem.allEqual(u8, &digest, 0)) return error.InvalidLogicalCaptureControl,
            .expiry_floor => |floor| if (floor < self.owner.expires_at_unix_ns) return error.InvalidLogicalCaptureControl,
        }
        if (self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0) return error.InvalidLogicalCaptureControl;
        try (Position{ .raft = self.position }).validate();
        if (self.member >= publisher.max_capture_participants or self.group_id == 0 or self.physical_root_incarnation == 0 or
            std.mem.allEqual(u8, &self.transaction_id, 0) or std.mem.allEqual(u8, &self.capacity_digest, 0))
            return error.InvalidLogicalCaptureControl;
    }

    pub fn require(self: @This(), a: std.mem.Allocator, grant: Observation) !void {
        try self.validate();
        if (!self.owner.eql(grant.owner) or !std.mem.eql(u8, &self.transaction_id, &grant.transaction_id) or
            self.member != grant.member or self.group_id != grant.owner_group_id or !self.namespace.eql(grant.namespace) or
            !(Position{ .raft = self.position }).strictlyAfter(grant.position) or
            !std.mem.eql(u8, &self.capacity_digest, &try grant.digest(a))) return error.ExternalTransactionDecisionChanged;
    }
};

pub const TerminalObservation = union(enum) {
    captured: publisher.OrderedTerminal,
    absent: AbortAbsence,

    pub fn require(self: @This(), a: std.mem.Allocator, grant: Observation) !void {
        switch (self) {
            .captured => |terminal| {
                try terminal.validate();
                try grant.requireCapture(a, terminal.capture_ref);
            },
            .absent => |fence| try fence.require(a, grant),
        }
    }
};

pub const Observation = struct {
    owner: owner.External,
    transaction_id: [16]u8,
    member: u16,
    owner_group_id: u64,
    topology_epoch: u64,
    namespace: namespace.Namespace,
    physical_root_incarnation: u128,
    position: @import("../storage/db/receipt_position.zig").Position,
    reference: capture.DemandReference,
    demand_digest: [32]u8,
    before_digest: [32]u8,
    max_payload_bytes: u64,
    pub fn require(self: @This(), a: std.mem.Allocator, demand: capture.Demand) !void {
        try demand.validate(a);
        _ = try self.grant(a);
        if (!self.owner.eql(demand.owner) or !std.mem.eql(u8, &self.transaction_id, &demand.transaction_id) or self.member != demand.member or
            self.owner_group_id != demand.owner_group_id or self.topology_epoch != demand.topology_epoch or
            self.namespace.table_id != demand.roster.relation.physical_table_id or !self.reference.eql(demand.reference) or
            !std.mem.eql(u8, &self.demand_digest, &try demand.preCapacityDigest(a))) return error.LogicalCaptureControlChanged;
    }
    pub fn validate(self: @This()) !void {
        try self.owner.validate();
        try self.reference.validate();
        try self.position.requireNamespace(self.namespace);
        if (std.mem.allEqual(u8, &self.transaction_id, 0) or self.member >= publisher.max_capture_participants or self.owner_group_id == 0 or self.topology_epoch == 0 or
            self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or self.physical_root_incarnation == 0 or
            std.mem.allEqual(u8, &self.demand_digest, 0) or std.mem.allEqual(u8, &self.before_digest, 0) or self.max_payload_bytes == 0 or
            self.max_payload_bytes > @import("../storage/db/table_catalog.zig").default_transaction_admission_bytes) return error.InvalidLogicalCaptureControl;
    }
    pub fn digest(self: @This(), a: std.mem.Allocator) ![32]u8 {
        try self.validate();
        return capture.canonicalDigest(a, "antfly/logical-capture-capacity/v1\x00", self);
    }
    pub fn requireCapture(self: @This(), a: std.mem.Allocator, value: publisher.CaptureRef) !void {
        try value.validate();
        if (value.participant != self.member or !value.owner.eql(self.owner) or
            !std.mem.eql(u8, &value.transaction_id, &self.transaction_id) or value.group_id != self.owner_group_id or
            !value.namespace.eql(self.namespace) or !value.position.strictlyAfter(self.position) or value.encoded_bytes > self.max_payload_bytes or
            !std.mem.eql(u8, &value.capacity_digest, &try self.digest(a)) or !std.mem.eql(u8, &value.before_digest, &self.before_digest) or
            !std.mem.eql(u8, &value.guard_digest, &self.reference.guard_digest) or !std.mem.eql(u8, &value.roster_digest, &self.reference.roster_digest) or
            !std.mem.eql(u8, &value.pregrant_body_digest, &self.reference.pregrant_body_digest))
            return error.ExternalTransactionDecisionChanged;
    }

    pub fn grant(self: @This(), a: std.mem.Allocator) !capture.CapacityGrant {
        const result: capture.CapacityGrant = .{ .bound_digest = try self.digest(a), .before_digest = self.before_digest, .max_payload_bytes = self.max_payload_bytes };
        try result.validate();
        return result;
    }
};

/// Read-only after genuine canonical ABORT delivery. A missing local record
/// is sufficient ONLY if the existing durable expiry floor fences this owner.
pub const AbortPermission = struct {
    realm: publisher.Realm,
    bound: Observation,
    decision_version: u64,
    table_name: []const u8,
    pub fn validate(self: @This()) !void {
        try self.realm.validate();
        try self.bound.validate();
        if (self.decision_version == 0 or !self.realm.eql(self.bound.reference.realm) or self.table_name.len == 0 or self.table_name.len > 1024) return error.InvalidLogicalCaptureControl;
    }
};
pub const AbortFence = union(enum) { aborted_record: [32]u8, expiry_floor: u64 };
pub const AbortObservation = struct {
    permission_digest: [32]u8,
    bound_digest: [32]u8,
    physical_root_incarnation: u128,
    position: @import("../storage/db/receipt_position.zig").Position,
    fence: AbortFence,
    reference: ?publisher.CaptureRef = null,
    pub fn validate(self: @This(), a: std.mem.Allocator, permission: AbortPermission) !void {
        try permission.validate();
        if (self.physical_root_incarnation == 0 or !std.mem.eql(u8, &self.bound_digest, &try permission.bound.digest(a)) or
            !std.mem.eql(u8, &self.permission_digest, &try capture.canonicalDigest(a, "antfly/logical-capacity-abort-permission/v1\x00", permission))) return error.InvalidLogicalCaptureControl;
        try self.position.requireNamespace(permission.bound.namespace);
        switch (self.fence) {
            .aborted_record => |record_digest| if (std.mem.allEqual(u8, &record_digest, 0)) return error.InvalidLogicalCaptureControl,
            .expiry_floor => |floor| if (floor < permission.bound.owner.expires_at_unix_ns) return error.InvalidLogicalCaptureControl,
        }
        if (self.reference) |reference| {
            try reference.validate();
            if (!reference.owner.eql(permission.bound.owner) or reference.participant != permission.bound.member or
                !std.mem.eql(u8, &reference.transaction_id, &permission.bound.transaction_id) or !reference.namespace.eql(permission.bound.namespace) or
                reference.group_id != permission.bound.owner_group_id or reference.encoded_bytes > permission.bound.max_payload_bytes) return error.InvalidLogicalCaptureControl;
        }
    }
    pub fn digest(self: @This(), a: std.mem.Allocator) ![32]u8 {
        return capture.canonicalDigest(a, "antfly/logical-capacity-abort-observation/v1\x00", self);
    }
};

/// Native permission readback, not a storage execution/prepare receipt.
pub const PreparePermission = struct { request_digest: [32]u8, pregrant_body_digest: [32]u8, bound_digest: [32]u8 };

test "logical publication capacity request roundtrip preserves binary keys exact JSON numbers and explicit revision mode" {
    const a = std.testing.allocator;
    const types = @import("../storage/db/types.zig");
    const hashing = @import("../storage/db/logical_replication_request.zig");
    const ns: namespace.Namespace = .{ .table_id = 50, .shard_id = 1, .range_id = 2 };
    const decision: owner.External = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(7), .admission_digest = @splat(8), .expires_at_unix_ns = 1000 };
    const txn: [16]u8 = @splat(9);
    const writes = [_]types.BatchWrite{.{ .key = "binary:\xff\x00", .value = "{\"id\":\"text remains text\",\"n\":9007199254740993,\"nil\":null}" }};
    const intent: types.TransactionIntentRequest = .{ .document_version_scheme = .revision_v1, .writes = &writes, .external_decision_owner = decision };
    const roster: publisher.PublishedRelation = .{ .relation = .{ .logical_relation_id = 40, .physical_table_id = 50, .database_id = 2, .namespace_id = 3, .relation_oid = 40, .namespace = "public", .name = "published", .schema_version = 1, .schema_digest = @splat(5), .replica_identity = .default_key, .columns = &.{.{ .attnum = 1, .name = "id", .type_oid = 25, .key = true }} }, .memberships = &.{.{ .publication = .{ .id = 10, .generation = 1, .digest = @splat(6) }, .parameters = .{} }} };
    const demand: capture.Demand = .{ .owner = decision, .transaction_id = txn, .member = 0, .owner_group_id = 9, .topology_epoch = 1, .reference = .{ .realm = .{ .group_id = 1, .incarnation = @splat('1'), .database_id = 2 }, .guard_digest = @splat(3), .roster_digest = try capture.canonicalDigest(a, "antfly/logical-capture-roster/v1\x00", roster), .pregrant_body_digest = try hashing.pregrantBodyDigest(a, .{ .namespace = ns, .owner_group_id = 9, .transaction_id = txn, .member = 0, .topology_epoch = 1 }, intent), .request_digest = try hashing.digest(a, txn, intent) }, .roster = roster };
    const request = try Request.init(a, demand, intent);
    defer a.free(request.frame);
    const encoded = try std.json.Stringify.valueAlloc(a, request, .{});
    defer a.free(encoded);
    const decoded = try std.json.parseFromSlice(Request, a, encoded, .{ .allocate = .alloc_always });
    defer decoded.deinit();
    try std.testing.expectEqual(types.DocumentVersionScheme.revision_v1, decoded.value.intent.document_version_scheme);
    try std.testing.expectEqualStrings(writes[0].key, decoded.value.intent.writes[0].key);
    try std.testing.expectEqualDeep(try hashing.digest(a, txn, intent), try hashing.digest(a, txn, decoded.value.intent));
    try std.testing.expectEqualDeep(try demand.preCapacityDigest(a), try decoded.value.demand.preCapacityDigest(a));
    var changed = decoded.value.intent;
    changed.document_version_scheme = .legacy;
    try std.testing.expect(!std.mem.eql(u8, &demand.reference.request_digest, &try hashing.digest(a, txn, changed)));
}
