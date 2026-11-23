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

const std = @import("std");
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const Allocator = std.mem.Allocator;

pub const SourceKind = enum {
    postgres,
};

pub const Config = struct {
    kind: SourceKind,
    dsn: []u8,

    pub fn deinit(self: *Config, alloc: Allocator) void {
        alloc.free(self.dsn);
        self.* = undefined;
    }
};

pub const Column = struct {
    name: []u8,
    data_type: []u8,
    nullable: bool,

    pub fn deinit(self: *Column, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.data_type);
        self.* = undefined;
    }
};

pub const SortField = struct {
    field: []u8,
    desc: bool = false,

    pub fn deinit(self: *SortField, alloc: Allocator) void {
        alloc.free(self.field);
        self.* = undefined;
    }
};

pub const TableStatistics = struct {
    row_count: i64 = 0,
    size_bytes: i64 = 0,
};

pub const QueryParams = struct {
    table: []u8,
    fields: [][]u8 = &.{},
    filter_query_json: ?[]u8 = null,
    columns: []Column = &.{},
    limit: ?usize = null,
    offset: usize = 0,
    order_by: []SortField = &.{},
    execution_deadline_ns: ?u64 = null,
    /// Borrowed from the HTTP request; never retained by a source.
    cancellation: ?CancellationToken = null,

    pub fn deinit(self: *QueryParams, alloc: Allocator) void {
        alloc.free(self.table);
        for (self.fields) |field| alloc.free(field);
        if (self.fields.len > 0) alloc.free(self.fields);
        if (self.filter_query_json) |query| alloc.free(query);
        for (self.columns) |*column| column.deinit(alloc);
        if (self.columns.len > 0) alloc.free(self.columns);
        for (self.order_by) |*sort| sort.deinit(alloc);
        if (self.order_by.len > 0) alloc.free(self.order_by);
        self.* = undefined;
    }
};

pub const AggregationDef = struct {
    type_name: []u8,
    field: ?[]u8 = null,
    size: ?usize = null,

    pub fn deinit(self: *AggregationDef, alloc: Allocator) void {
        alloc.free(self.type_name);
        if (self.field) |field| alloc.free(field);
        self.* = undefined;
    }
};

pub const AggregateParams = struct {
    table: []u8,
    filter_query_json: ?[]u8 = null,
    columns: []Column = &.{},
    aggregations: []NamedAggregation = &.{},
    execution_deadline_ns: ?u64 = null,
    /// Borrowed from the HTTP request; never retained by a source.
    cancellation: ?CancellationToken = null,

    pub fn deinit(self: *AggregateParams, alloc: Allocator) void {
        alloc.free(self.table);
        if (self.filter_query_json) |query| alloc.free(query);
        for (self.columns) |*column| column.deinit(alloc);
        if (self.columns.len > 0) alloc.free(self.columns);
        for (self.aggregations) |*aggregation| aggregation.deinit(alloc);
        if (self.aggregations.len > 0) alloc.free(self.aggregations);
        self.* = undefined;
    }
};

pub const NamedAggregation = struct {
    name: []u8,
    definition: AggregationDef,

    pub fn deinit(self: *NamedAggregation, alloc: Allocator) void {
        alloc.free(self.name);
        self.definition.deinit(alloc);
        self.* = undefined;
    }
};

pub const QueryResult = struct {
    rows: []std.json.Value = &.{},
    /// Present only for the dedicated replication snapshot reader. A JSON
    /// null value differs from SQL NULL in typed-row ingestion.
    json_null_fields: []const []const []const u8 = &.{},
    total: usize = 0,

    pub fn deinit(self: *QueryResult, alloc: Allocator) void {
        for (self.json_null_fields) |fields| {
            for (fields) |field| alloc.free(field);
            alloc.free(fields);
        }
        if (self.json_null_fields.len != 0) alloc.free(self.json_null_fields);
        for (self.rows) |*row| deinitJsonValue(alloc, row);
        if (self.rows.len > 0) alloc.free(self.rows);
        self.* = undefined;
    }
};

pub const ReplicationIdentity = struct {
    provider_identity: [32]u8,
    relation_id: u32,
};

pub const SnapshotReader = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        deinit: *const fn (ptr: *anyopaque, alloc: Allocator) void,
        capture_query: ?*const fn (*anyopaque, Allocator, QueryParams) anyerror!QueryResult = null,
        replication_identity: ?*const fn (*anyopaque, Allocator, []const u8, ?u64) anyerror!ReplicationIdentity = null,
        query: *const fn (ptr: *anyopaque, alloc: Allocator, params: QueryParams) anyerror!QueryResult,
    };

    /// One server cursor over the pinned snapshot, consumed until empty.
    /// A repeated call advances that cursor; it never opens a fresh SELECT.
    pub fn captureQuery(self: SnapshotReader, a: Allocator, params: QueryParams) !QueryResult {
        const callback = self.vtable.capture_query orelse return error.UnsupportedConsistentSnapshot;
        if (params.offset != 0 or params.limit == null or params.limit.? == 0 or params.limit.? > 4096) return error.InvalidReplicationSnapshotCursor;
        return callback(self.ptr, a, params);
    }

    pub fn replicationIdentity(self: SnapshotReader, a: Allocator, table: []const u8, deadline: ?u64) !?ReplicationIdentity {
        const callback = self.vtable.replication_identity orelse return null;
        return try callback(self.ptr, a, table, deadline);
    }

    pub fn deinit(self: *SnapshotReader, alloc: Allocator) void {
        self.vtable.deinit(self.ptr, alloc);
        self.* = undefined;
    }

    pub fn query(self: SnapshotReader, alloc: Allocator, params: QueryParams) !QueryResult {
        return try self.vtable.query(self.ptr, alloc, params);
    }
};

pub const PreparedReplicationSnapshot = struct {
    origin: ?ReplicationIdentity = null,
    checkpoint: []u8,
    /// Actual PostgreSQL exported token, provenance only after import. Never
    /// authority to resume a lost reader or reconstruct its snapshot view.
    snapshot_token: ?[]u8 = null,
    reader: SnapshotReader,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.checkpoint);
        if (self.snapshot_token) |token| alloc.free(token);
        self.reader.deinit(alloc);
        self.* = undefined;
    }
};

pub const ReplicationOp = enum {
    insert,
    update,
    delete,
};

pub const ReplicationRelation = struct {
    id: u32,
    namespace: []u8,
    name: []u8,

    pub fn deinit(self: *ReplicationRelation, alloc: Allocator) void {
        alloc.free(self.namespace);
        alloc.free(self.name);
    }
};

pub const ReplicationChange = struct {
    op: ReplicationOp,
    /// PostgreSQL relation identity at decode time, retained even when several
    /// publications share a slot. A target table name cannot substitute it.
    relation: ?ReplicationRelation = null,
    /// JSON-typed PostgreSQL columns containing a JSON null datum. A tuple's
    /// SQL-NULL marker deliberately does not populate this provenance.
    json_null_fields: []const []const u8 = &.{},
    checkpoint: []u8,
    row: ?std.json.Value = null,
    /// Before image carried by UPDATE K/O, owned independently of row.
    /// K contains replica-identity columns only; O is the full old tuple.
    old_row: ?std.json.Value = null,
    old_json_null_fields: []const []const u8 = &.{},
    /// UPDATE old_row or DELETE row came from an actual O tuple marker.
    old_row_full: bool = false,
    key: ?[]u8 = null,
    lag_records: u64 = 0,
    commit_timestamp_ms: u64 = 0,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.checkpoint);
        if (self.row) |*row| deinitJsonValue(alloc, row);
        if (self.old_row) |*row| deinitJsonValue(alloc, row);
        for (self.old_json_null_fields) |field| alloc.free(field);
        if (self.old_json_null_fields.len != 0) alloc.free(self.old_json_null_fields);
        if (self.key) |key| alloc.free(key);
        if (self.relation) |*relation| relation.deinit(alloc);
        for (self.json_null_fields) |field| alloc.free(field);
        if (self.json_null_fields.len != 0) alloc.free(self.json_null_fields);
        self.* = undefined;
    }
};

/// Synchronous durability barrier invoked after the provider inspects the
/// physical slot, but before it deletes or creates persistent provider state.
/// The callback lifetime is limited to the enclosing
/// beginPreparedReplicationSnapshot call.
/// Joined, checked observation from the actual provider connection before
/// publication/slot creation or deletion. It binds only genuine provider/OID
/// identity; a snapshot point does not exist yet and must not be invented.
pub const InitializationOrigin = struct {
    ptr: *anyopaque,
    observe_fn: *const fn (*anyopaque, ReplicationIdentity) anyerror!void,
    check_fn: *const fn (*anyopaque) anyerror!void,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    const VTable = struct { observe_fn: *const fn (*anyopaque, ReplicationIdentity) anyerror!void, check_fn: *const fn (*anyopaque) anyerror!void };
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(VTable);
    pub fn check(self: @This()) !void {
        return Boundary.call("check_fn", self.boundary_dispatch, self.check_fn, .{self.ptr});
    }
    pub fn observe(self: @This(), origin: ReplicationIdentity) !void {
        if (origin.relation_id == 0 or std.mem.allEqual(u8, &origin.provider_identity, 0)) return error.ForeignProviderIdentityMismatch;
        return Boundary.call("observe_fn", self.boundary_dispatch, self.observe_fn, .{ self.ptr, origin });
    }
};

pub const ExactCutoverIntent = struct {
    pub const ProviderIdentity = [std.crypto.hash.sha2.Sha256.digest_length]u8;

    ptr: *anyopaque,
    persist_fn: *const fn (
        ptr: *anyopaque,
        provider_identity: ProviderIdentity,
    ) anyerror!void,
    check_fn: *const fn (ptr: *anyopaque) anyerror!void,
    retirement_complete_fn: *const fn (ptr: *anyopaque) anyerror!void,

    pub fn persist(self: @This(), provider_identity: ProviderIdentity) !void {
        try self.persist_fn(self.ptr, provider_identity);
    }

    pub fn check(self: @This()) !void {
        try self.check_fn(self.ptr);
    }

    pub fn retirementComplete(self: @This()) !void {
        try self.retirement_complete_fn(self.ptr);
    }
};

pub const SnapshotCustody = union(enum) {
    legacy: ExactCutoverIntent,
    initialization: InitializationOrigin,
    pub fn check(self: @This()) !void {
        switch (self) {
            .legacy => |v| try v.check(),
            .initialization => |v| try v.check(),
        }
    }
    pub fn bindOrigin(self: @This(), actual: ReplicationIdentity) !void {
        switch (self) {
            .legacy => |v| try v.persist(actual.provider_identity),
            .initialization => |v| try v.observe(actual),
        }
    }
    pub fn retirementComplete(self: @This()) !void {
        switch (self) {
            .legacy => |v| try v.retirementComplete(),
            .initialization => return error.CutoverAuthorityLost,
        }
    }
};

pub const SubscriptionOrigin = @import("antfly_local_sources").system_catalog_logical_replication_origin;

/// Joined native Source authority, borrowed only for this provider operation.
/// Binding persists genuine provider/roster facts before any upstream effect;
/// acknowledgement proves the corresponding local native checkpoint and P ACK.
pub const SubscriptionAuthority = struct {
    ptr: *anyopaque,
    start_fn: ?*const fn (*anyopaque, [32]u8) anyerror!void = null,
    exported_fn: ?*const fn (*anyopaque, [32]u8, u64) anyerror!void = null,
    refused_fn: ?*const fn (*anyopaque) anyerror!void = null,
    observe_fn: *const fn (*anyopaque, SubscriptionOrigin.Observation) anyerror!void,
    check_fn: *const fn (*anyopaque) anyerror!void,
    acknowledged_fn: *const fn (*anyopaque, []const u8) anyerror!void,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    const VTable = struct {
        start_fn: *const fn (*anyopaque, [32]u8) anyerror!void,
        observe_fn: *const fn (*anyopaque, SubscriptionOrigin.Observation) anyerror!void,
        check_fn: *const fn (*anyopaque) anyerror!void,
        acknowledged_fn: *const fn (*anyopaque, []const u8) anyerror!void,
        exported_fn: *const fn (*anyopaque, [32]u8, u64) anyerror!void,
        refused_fn: *const fn (*anyopaque) anyerror!void,
    };
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(VTable);
    pub fn check(self: @This()) !void {
        return Boundary.call("check_fn", self.boundary_dispatch, self.check_fn, .{self.ptr});
    }
    /// Fence the first possible slot mutation after provider preflight.
    pub fn start(self: @This(), identity: [32]u8) !void {
        if (self.start_fn) |start_fn|
            try Boundary.call("start_fn", self.boundary_dispatch, start_fn, .{ self.ptr, identity });
        try self.check();
    }
    pub fn observe(self: @This(), observed: SubscriptionOrigin.Observation) !void {
        try self.check();
        try Boundary.call("observe_fn", self.boundary_dispatch, self.observe_fn, .{ self.ptr, observed });
        try self.check();
    }
    /// Record the provider-confirmed slot before importing the snapshot.
    pub fn exported(self: @This(), provider: [32]u8, checkpoint: u64) !void {
        if (self.exported_fn) |exported_fn|
            try Boundary.call("exported_fn", self.boundary_dispatch, exported_fn, .{ self.ptr, provider, checkpoint });
    }
    pub fn refused(self: @This()) !void {
        if (self.refused_fn) |refused_fn|
            try Boundary.call("refused_fn", self.boundary_dispatch, refused_fn, .{self.ptr});
    }
    pub fn acknowledged(self: @This(), checkpoint: []const u8) !void {
        try self.check();
        try Boundary.call("acknowledged_fn", self.boundary_dispatch, self.acknowledged_fn, .{ self.ptr, checkpoint });
        try self.check();
    }
};

/// Existing publication names belong to the publisher. A subscriber never
/// creates, renames or drops them, and never substitutes an anchor table.
pub const SubscriptionRequest = struct {
    publications: []const []const u8,
    slot_name: []const u8,
    lock_name: []const u8,
    expected: ?SubscriptionOrigin.Observation,
    authority: SubscriptionAuthority,
    checkpoint: ?[]const u8 = null,
    password_required: bool = false,
    /// Rejoin and advance a known durable subscriber checkpoint before peek.
    advance_durable_checkpoint: bool = false,
    limit: usize = 256,
    execution_deadline_ns: u64,

    pub fn validate(self: @This(), a: Allocator) !void {
        if (self.publications.len == 0 or self.publications.len > 4096 or self.slot_name.len == 0 or self.slot_name.len > 63 or
            self.lock_name.len == 0 or self.lock_name.len > 256 or std.mem.indexOfScalar(u8, self.lock_name, 0) != null or
            !std.unicode.utf8ValidateSlice(self.lock_name) or self.limit == 0 or self.limit > 65536) return error.InvalidQueryRequest;
        for (self.slot_name) |byte| if (!(std.ascii.isLower(byte) or std.ascii.isDigit(byte) or byte == '_')) return error.InvalidQueryRequest;
        var names: std.StringHashMapUnmanaged(void) = .empty;
        defer names.deinit(a);
        for (self.publications) |name| {
            if (name.len == 0 or name.len > 63 or std.mem.indexOfScalar(u8, name, 0) != null or !std.unicode.utf8ValidateSlice(name) or
                (try names.getOrPut(a, name)).found_existing) return error.InvalidQueryRequest;
        }
        if (self.expected) |expected| try expected.validate(a);
        if (self.checkpoint) |checkpoint| {
            // ACK compares the exact provider-returned canonical LSN. Refuse
            // alternate spellings before any slot effect or native callback.
            _ = parseReplicationCheckpoint(checkpoint) catch return error.InvalidQueryRequest;
        }
    }
    pub fn observe(self: @This(), a: Allocator, actual: SubscriptionOrigin.Observation) !void {
        try actual.validate(a);
        if (self.expected) |expected| {
            if (!std.mem.eql(u8, &try expected.digest(a), &try actual.digest(a))) return error.ForeignProviderIdentityMismatch;
        }
        try self.authority.observe(actual);
    }
};

/// Poll and feedback share the same continuity fence. A provider can lag a
/// durable subscriber receipt after a crash, but it cannot be ahead of it.
/// Returning the parsed point grants no feedback permission.
pub fn requireSubscriptionCheckpoint(confirmed: u64, expected: ?[]const u8) !u64 {
    const checkpoint = parseReplicationCheckpoint(expected orelse return error.InvalidQueryRequest) catch return error.InvalidQueryRequest;
    if (confirmed > checkpoint) return error.ForeignProviderIdentityMismatch;
    return checkpoint;
}

test "foreign transaction envelopes fence subscription gaps while allowing durable replay prefixes" {
    try std.testing.expectEqual(@as(u64, 32), try requireSubscriptionCheckpoint(31, "0/20"));
    try std.testing.expectEqual(@as(u64, 32), try requireSubscriptionCheckpoint(32, "0/20"));
    try std.testing.expectError(error.ForeignProviderIdentityMismatch, requireSubscriptionCheckpoint(33, "0/20"));
    try std.testing.expectError(error.InvalidQueryRequest, requireSubscriptionCheckpoint(0, null));
    try std.testing.expectError(error.InvalidQueryRequest, requireSubscriptionCheckpoint(0, "0/020"));
}

/// One actual exported/imported provider cut covering the entire frozen roster.
/// Header slices borrow the reader; the worker must keep it alive until READY
/// has retained every captured page. A token never reopens a lost reader.
pub const SubscriptionSnapshot = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    origin: SubscriptionOrigin.Observation,
    checkpoint: []const u8,
    snapshot_token: []const u8,
    pub const VTable = struct {
        deinit: *const fn (*anyopaque, Allocator) void,
        abort: ?*const fn (*anyopaque, Allocator) anyerror!void = null,
        capture_relation: *const fn (*anyopaque, Allocator, u32, usize, u64, CancellationToken) anyerror!QueryResult,
    };
    pub fn captureRelation(self: @This(), a: Allocator, relation: u32, rows: usize, deadline: u64, authority: SubscriptionAuthority) !QueryResult {
        if (rows == 0 or rows > 4096) return error.InvalidReplicationSnapshotCursor;
        try authority.check();
        const Checkpoint = struct {
            fn check(raw: *const anyopaque) !void {
                const current: *const SubscriptionAuthority = @ptrCast(@alignCast(raw));
                try current.check();
            }
        };
        // Borrow only for this call. A retained reader never owns the old
        // quantum's deadline, cancellation, allocator or actor context.
        const cancellation: CancellationToken = .{ .ptr = &authority, .check_fn = Checkpoint.check };
        var result = try self.vtable.capture_relation(self.ptr, a, relation, rows, deadline, cancellation);
        errdefer result.deinit(a);
        try authority.check();
        return result;
    }
    pub fn abort(self: @This(), a: Allocator) !void {
        const cancel = self.vtable.abort orelse return error.UnsupportedReplicationCleanup;
        try cancel(self.ptr, a);
    }
    pub fn deinit(self: *@This(), a: Allocator) void {
        self.vtable.deinit(self.ptr, a);
        self.* = undefined;
    }
};

pub const SubscriptionPollResult = struct {
    origin: SubscriptionOrigin.Observation,
    poll: ReplicationPollResult,
    pub fn deinit(self: *@This(), a: Allocator) void {
        self.origin.deinit(a);
        self.poll.deinit(a);
        self.* = undefined;
    }
};

pub const ReplicationPollParams = struct {
    /// A retained native source anchor, checked before provider mutation or
    /// checkpoint advancement. A returned identity is too late for this guard.
    expected_origin: ?ReplicationIdentity = null,
    table: []u8,
    slot_name: ?[]u8 = null,
    publication_name: ?[]u8 = null,
    cutover_lock_name: ?[]u8 = null,
    retired_slot_name: ?[]u8 = null,
    retired_publication_name: ?[]u8 = null,
    filter_query_json: ?[]u8 = null,
    checkpoint: ?[]u8 = null,
    limit: ?usize = null,
    execution_deadline_ns: ?u64 = null,
    /// A replicated pending intent proves that this attempt-scoped physical
    /// slot belongs to an interrupted exact cutover and may be reclaimed.
    /// Never set this based only on a configured/provider-visible name.
    reclaim_exact_cutover_slot: bool = false,
    exact_cutover_intent: ?ExactCutoverIntent = null,
    initialization_origin: ?InitializationOrigin = null,

    pub fn snapshotCustody(self: ReplicationPollParams) !SnapshotCustody {
        if (self.initialization_origin) |owner| {
            if (self.exact_cutover_intent != null or self.retired_slot_name != null or self.retired_publication_name != null or self.reclaim_exact_cutover_slot) return error.InvalidQueryRequest;
            return .{ .initialization = owner };
        }
        return .{ .legacy = self.exact_cutover_intent orelse return error.InvalidQueryRequest };
    }

    pub fn observeInitializationOrigin(self: ReplicationPollParams, actual: ReplicationIdentity) !void {
        try self.checkOrigin(actual);
        if (self.initialization_origin) |observer| try observer.observe(actual);
    }

    pub fn checkOrigin(self: ReplicationPollParams, actual: ReplicationIdentity) !void {
        if (self.expected_origin) |expected| {
            if (expected.relation_id != actual.relation_id or !std.mem.eql(u8, &expected.provider_identity, &actual.provider_identity)) return error.ForeignProviderIdentityMismatch;
        }
    }

    pub fn deinit(self: *ReplicationPollParams, alloc: Allocator) void {
        alloc.free(self.table);
        if (self.slot_name) |slot_name| alloc.free(slot_name);
        if (self.publication_name) |publication_name| alloc.free(publication_name);
        if (self.cutover_lock_name) |name| alloc.free(name);
        if (self.retired_slot_name) |name| alloc.free(name);
        if (self.retired_publication_name) |name| alloc.free(name);
        if (self.filter_query_json) |query| alloc.free(query);
        if (self.checkpoint) |checkpoint| alloc.free(checkpoint);
        self.* = undefined;
    }
};

/// A complete committed source transaction, referring into the poll's flat
/// change array. Zero-row commits remain meaningful advancement positions.
/// Canonical PostgreSQL WAL position shared by snapshot, polling and ACK
/// contracts. Numeric end_lsn and feedback text must identify the same cut.
pub fn parseReplicationCheckpoint(text: []const u8) error{InvalidReplicationCheckpoint}!u64 {
    const slash = std.mem.indexOfScalar(u8, text, '/') orelse return error.InvalidReplicationCheckpoint;
    if (slash == 0 or slash > 8 or text.len - slash - 1 == 0 or text.len - slash - 1 > 8) return error.InvalidReplicationCheckpoint;
    const high = std.fmt.parseInt(u32, text[0..slash], 16) catch return error.InvalidReplicationCheckpoint;
    const low = std.fmt.parseInt(u32, text[slash + 1 ..], 16) catch return error.InvalidReplicationCheckpoint;
    var encoded: [17]u8 = undefined;
    const canonical = std.fmt.bufPrint(&encoded, "{X}/{X}", .{ high, low }) catch return error.InvalidReplicationCheckpoint;
    if (!std.mem.eql(u8, canonical, text)) return error.InvalidReplicationCheckpoint;
    return (@as(u64, high) << 32) | low;
}

pub const ReplicationTransaction = struct {
    xid: u32,
    final_lsn: u64,
    commit_lsn: u64,
    end_lsn: u64,
    commit_timestamp_ms: u64,
    first_change: usize,
    change_count: usize,
    checkpoint: []u8,

    pub fn deinit(self: *ReplicationTransaction, alloc: Allocator) void {
        alloc.free(self.checkpoint);
    }
};

pub const ReplicationPollResult = struct {
    changes: []ReplicationChange = &.{},
    /// null is the existing row-checkpoint provider contract. A non-null roster
    /// promises complete transaction boundaries covering every returned change.
    transactions: ?[]ReplicationTransaction = null,
    /// Actual PostgreSQL cluster+database-incarnation handshake for this poll.
    /// Required by transaction-aware consumers, never inferred from DSN/slot.
    provider_identity: ?ExactCutoverIntent.ProviderIdentity = null,
    source_relation_id: ?u32 = null,
    checkpoint: []u8 = &.{},
    lag_records: u64 = 0,
    lag_millis: u64 = 0,

    /// Validate before any destination mutation, including malformed metadata
    /// after an otherwise valid prefix. Never publish a partial source commit.
    pub fn validateTransactions(self: ReplicationPollResult) !void {
        const transactions = self.transactions orelse return;
        var next_change: usize = 0;
        var previous_end: u64 = 0;
        for (transactions) |transaction| {
            if (transaction.xid == 0 or transaction.final_lsn == 0 or transaction.commit_lsn != transaction.final_lsn or
                transaction.end_lsn <= transaction.commit_lsn or transaction.end_lsn <= previous_end or transaction.commit_lsn < previous_end or
                transaction.first_change != next_change or transaction.checkpoint.len == 0)
                return error.InvalidReplicationTransaction;
            const checkpoint = parseReplicationCheckpoint(transaction.checkpoint) catch return error.InvalidReplicationTransaction;
            if (checkpoint != transaction.end_lsn) return error.InvalidReplicationTransaction;
            const end = std.math.add(usize, next_change, transaction.change_count) catch return error.InvalidReplicationTransaction;
            if (end > self.changes.len) return error.InvalidReplicationTransaction;
            for (self.changes[next_change..end]) |change| {
                if (!std.mem.eql(u8, change.checkpoint, transaction.checkpoint) or change.commit_timestamp_ms != transaction.commit_timestamp_ms)
                    return error.InvalidReplicationTransaction;
            }
            next_change = end;
            previous_end = transaction.end_lsn;
        }
        if (next_change != self.changes.len) return error.InvalidReplicationTransaction;
        if (transactions.len != 0 and !std.mem.eql(u8, self.checkpoint, transactions[transactions.len - 1].checkpoint))
            return error.InvalidReplicationTransaction;
        if (transactions.len == 0 and self.changes.len != 0) return error.InvalidReplicationTransaction;
    }

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.changes) |*change| change.deinit(alloc);
        if (self.changes.len > 0) alloc.free(self.changes);
        if (self.transactions) |transactions| {
            for (transactions) |*transaction| transaction.deinit(alloc);
            alloc.free(transactions);
        }
        if (self.checkpoint.len > 0) alloc.free(self.checkpoint);
        self.* = undefined;
    }
};

pub const ReplicationPrepareResult = struct {
    origin: ?ReplicationIdentity = null,
    checkpoint: []u8 = "",
    slot_existed: bool = false,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.checkpoint);
        self.* = undefined;
    }
};

pub const ReplicationCleanupParams = struct {
    slot_name: []u8,
    /// Null for subscriptions: upstream configured publications are user-owned.
    publication_name: ?[]u8 = null,
    native_source: ?struct {
        provider_identity: ExactCutoverIntent.ProviderIdentity,
        snapshot_checkpoint: u64,
        durable_checkpoint: u64,
        require_absent: bool = false,
        /// Same provider lock used by capture/poll/ACK; borrowed for this call.
        lock_name: []const u8,
        authority: SubscriptionAuthority,
    } = null,
    execution_deadline_ns: ?u64 = null,
    password_required: bool = false,

    pub fn validate(self: @This()) !void {
        if (self.slot_name.len == 0 or self.slot_name.len > 63) return error.InvalidQueryRequest;
        for (self.slot_name) |byte| if (!(std.ascii.isLower(byte) or std.ascii.isDigit(byte) or byte == '_')) return error.InvalidQueryRequest;
        if (self.publication_name) |name| {
            if (name.len == 0 or name.len > 63 or std.mem.indexOfScalar(u8, name, 0) != null or !std.unicode.utf8ValidateSlice(name)) return error.InvalidQueryRequest;
        } else if (self.native_source == null) return error.InvalidQueryRequest;
        if (self.native_source) |native| {
            if (self.publication_name != null) return error.InvalidQueryRequest;
            if (native.require_absent) {
                if (native.snapshot_checkpoint != 0 or native.durable_checkpoint != 0) return error.InvalidQueryRequest;
            } else if (native.snapshot_checkpoint == 0 or native.durable_checkpoint < native.snapshot_checkpoint) {
                return error.InvalidQueryRequest;
            }
            if (std.mem.allEqual(u8, &native.provider_identity, 0) or native.lock_name.len == 0 or native.lock_name.len > 256 or
                std.mem.indexOfScalar(u8, native.lock_name, 0) != null or !std.unicode.utf8ValidateSlice(native.lock_name)) return error.InvalidQueryRequest;
        }
    }
    pub fn check(self: @This()) !void {
        try self.validate();
        if (self.native_source) |native| try native.authority.check();
    }
    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.slot_name);
        if (self.publication_name) |name| alloc.free(name);
        self.* = undefined;
    }
};

pub const NamedValue = struct {
    name: []u8,
    value: std.json.Value,

    pub fn deinit(self: *NamedValue, alloc: Allocator) void {
        alloc.free(self.name);
        deinitJsonValue(alloc, &self.value);
        self.* = undefined;
    }
};

pub const AggregateResult = struct {
    results: []NamedValue = &.{},

    pub fn deinit(self: *AggregateResult, alloc: Allocator) void {
        for (self.results) |*result| result.deinit(alloc);
        if (self.results.len > 0) alloc.free(self.results);
        self.* = undefined;
    }
};

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        transaction_envelopes: bool = false,
        subscription_transactions: bool = false,
        begin_subscription_snapshot: ?*const fn (*anyopaque, Allocator, SubscriptionRequest) anyerror!SubscriptionSnapshot = null,
        poll_subscription: ?*const fn (*anyopaque, Allocator, SubscriptionRequest) anyerror!SubscriptionPollResult = null,
        acknowledge_subscription: ?*const fn (*anyopaque, Allocator, SubscriptionRequest) anyerror!void = null,
        begin_replication_snapshot_query: ?*const fn (*anyopaque, Allocator) anyerror!SnapshotReader = null,
        deinit: *const fn (ptr: *anyopaque, alloc: Allocator) void,
        query: *const fn (ptr: *anyopaque, alloc: Allocator, params: QueryParams) anyerror!QueryResult,
        aggregate: ?*const fn (ptr: *anyopaque, alloc: Allocator, params: AggregateParams) anyerror!AggregateResult = null,
        statistics: *const fn (ptr: *anyopaque, table: []const u8) anyerror!TableStatistics,
        statistics_with_deadline: ?*const fn (ptr: *anyopaque, table: []const u8, execution_deadline_ns: ?u64) anyerror!TableStatistics = null,
        begin_snapshot_query: ?*const fn (ptr: *anyopaque, alloc: Allocator) anyerror!SnapshotReader = null,
        begin_prepared_replication_snapshot: ?*const fn (ptr: *anyopaque, alloc: Allocator, params: ReplicationPollParams, execution_deadline_ns: u64) anyerror!PreparedReplicationSnapshot = null,
        prepare_replication: ?*const fn (ptr: *anyopaque, alloc: Allocator, params: ReplicationPollParams) anyerror!ReplicationPrepareResult = null,
        poll_changes: ?*const fn (ptr: *anyopaque, alloc: Allocator, params: ReplicationPollParams) anyerror!ReplicationPollResult = null,
        cleanup_replication: ?*const fn (ptr: *anyopaque, alloc: Allocator, params: ReplicationCleanupParams) anyerror!void = null,
    };

    pub fn deinit(self: *Source, alloc: Allocator) void {
        self.vtable.deinit(self.ptr, alloc);
        self.* = undefined;
    }

    pub fn query(self: Source, alloc: Allocator, params: QueryParams) !QueryResult {
        return try self.vtable.query(self.ptr, alloc, params);
    }

    pub fn aggregate(self: Source, alloc: Allocator, params: AggregateParams) !AggregateResult {
        const aggregate_fn = self.vtable.aggregate orelse return error.UnsupportedAggregate;
        return try aggregate_fn(self.ptr, alloc, params);
    }

    pub fn statistics(self: Source, table: []const u8) !TableStatistics {
        return try self.vtable.statistics(self.ptr, table);
    }

    pub fn statisticsWithDeadline(self: Source, table: []const u8, execution_deadline_ns: ?u64) !TableStatistics {
        if (self.vtable.statistics_with_deadline) |fn_ptr| {
            return try fn_ptr(self.ptr, table, execution_deadline_ns);
        }
        if (execution_deadline_ns != null) return error.UnsupportedDeadline;
        return try self.statistics(table);
    }

    pub fn beginSnapshotQuery(self: Source, alloc: Allocator) !SnapshotReader {
        const begin_fn = self.vtable.begin_snapshot_query orelse return error.UnsupportedConsistentSnapshot;
        return try begin_fn(self.ptr, alloc);
    }

    pub fn beginReplicationSnapshotQuery(self: Source, a: Allocator) !SnapshotReader {
        if (self.vtable.begin_replication_snapshot_query) |callback| return try callback(self.ptr, a);
        if (self.vtable.transaction_envelopes) return error.UnsupportedConsistentSnapshot;
        return self.beginSnapshotQuery(a);
    }

    pub fn supportsSubscription(self: Source) bool {
        return self.vtable.transaction_envelopes and self.vtable.subscription_transactions and
            self.vtable.begin_subscription_snapshot != null and self.vtable.poll_subscription != null and self.vtable.acknowledge_subscription != null;
    }
    pub fn beginSubscriptionSnapshot(self: Source, a: Allocator, request: SubscriptionRequest) !SubscriptionSnapshot {
        if (!self.supportsSubscription()) return error.UnsupportedExactCutover;
        try request.validate(a);
        return self.vtable.begin_subscription_snapshot.?(self.ptr, a, request);
    }
    pub fn pollSubscription(self: Source, a: Allocator, request: SubscriptionRequest) !SubscriptionPollResult {
        if (!self.supportsSubscription()) return error.UnsupportedReplicationStreaming;
        try request.validate(a);
        return self.vtable.poll_subscription.?(self.ptr, a, request);
    }
    pub fn acknowledgeSubscription(self: Source, a: Allocator, request: SubscriptionRequest) !void {
        if (!self.supportsSubscription()) return error.UnsupportedReplicationStreaming;
        try request.validate(a);
        return self.vtable.acknowledge_subscription.?(self.ptr, a, request);
    }

    pub fn beginPreparedReplicationSnapshot(
        self: Source,
        alloc: Allocator,
        params: ReplicationPollParams,
        execution_deadline_ns: u64,
    ) !PreparedReplicationSnapshot {
        const begin_fn = self.vtable.begin_prepared_replication_snapshot orelse return error.UnsupportedExactCutover;
        return try begin_fn(self.ptr, alloc, params, execution_deadline_ns);
    }

    pub fn prepareReplication(self: Source, alloc: Allocator, params: ReplicationPollParams) !ReplicationPrepareResult {
        const prepare_fn = self.vtable.prepare_replication orelse return error.UnsupportedReplicationStreaming;
        return try prepare_fn(self.ptr, alloc, params);
    }

    pub fn pollChanges(self: Source, alloc: Allocator, params: ReplicationPollParams) !ReplicationPollResult {
        const poll_fn = self.vtable.poll_changes orelse return error.UnsupportedReplicationStreaming;
        return try poll_fn(self.ptr, alloc, params);
    }

    pub fn cleanupReplication(self: Source, alloc: Allocator, params: ReplicationCleanupParams) !void {
        try params.check();
        const cleanup_fn = self.vtable.cleanup_replication orelse return error.UnsupportedReplicationCleanup;
        return try cleanup_fn(self.ptr, alloc, params);
    }
};

pub const Factory = *const fn (alloc: Allocator, config: Config) anyerror!Source;
pub const ContextFactory = *const fn (ctx: *anyopaque, alloc: Allocator, config: Config) anyerror!Source;
pub const ContextDeinit = *const fn (ctx: *anyopaque, alloc: Allocator) void;

pub const FactoryEntry = union(enum) {
    plain: Factory,
    with_ctx: struct {
        ctx: *anyopaque,
        factory: ContextFactory,
        deinit_ctx: ?ContextDeinit = null,
    },

    pub fn create(self: @This(), alloc: Allocator, config: Config) !Source {
        return switch (self) {
            .plain => |factory| try factory(alloc, config),
            .with_ctx => |value| try value.factory(value.ctx, alloc, config),
        };
    }

    pub fn deinit(self: @This(), alloc: Allocator) void {
        switch (self) {
            .plain => {},
            .with_ctx => |value| if (value.deinit_ctx) |deinit_ctx| deinit_ctx(value.ctx, alloc),
        }
    }
};

pub const Registry = struct {
    factories: std.AutoHashMapUnmanaged(SourceKind, FactoryEntry) = .empty,

    pub fn deinit(self: *Registry, alloc: Allocator) void {
        var it = self.factories.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(alloc);
        self.factories.deinit(alloc);
        self.* = undefined;
    }

    pub fn register(self: *Registry, alloc: Allocator, kind: SourceKind, factory: Factory) !void {
        try self.factories.put(alloc, kind, .{ .plain = factory });
    }

    pub fn registerWithContext(
        self: *Registry,
        alloc: Allocator,
        kind: SourceKind,
        ctx: *anyopaque,
        factory: ContextFactory,
        deinit_ctx: ?ContextDeinit,
    ) !void {
        try self.factories.put(alloc, kind, .{
            .with_ctx = .{
                .ctx = ctx,
                .factory = factory,
                .deinit_ctx = deinit_ctx,
            },
        });
    }

    pub fn create(self: Registry, alloc: Allocator, config: Config) !Source {
        const entry = self.factories.get(config.kind) orelse return error.UnsupportedSourceKind;
        return try entry.create(alloc, config);
    }
};

pub fn deinitJsonValue(alloc: Allocator, value: *std.json.Value) void {
    switch (value.*) {
        .string => |text| alloc.free(text),
        .number_string => |text| alloc.free(text),
        .array => |*arr| {
            for (arr.items) |*item| deinitJsonValue(alloc, item);
            arr.deinit();
        },
        .object => |*obj| {
            var it = obj.iterator();
            while (it.next()) |entry| {
                alloc.free(@constCast(entry.key_ptr.*));
                deinitJsonValue(alloc, entry.value_ptr);
            }
            obj.deinit(alloc);
        },
        else => {},
    }
    value.* = undefined;
}

test "foreign source types compile" {
    _ = Config;
    _ = SourceKind;
    _ = Column;
    _ = SortField;
    _ = TableStatistics;
    _ = QueryParams;
    _ = AggregationDef;
    _ = NamedAggregation;
    _ = AggregateParams;
    _ = QueryResult;
    _ = NamedValue;
    _ = AggregateResult;
    _ = ReplicationOp;
    _ = ReplicationChange;
    _ = ReplicationPollParams;
    _ = ReplicationPollResult;
    _ = Source;
    _ = Factory;
    _ = ContextFactory;
    _ = ContextDeinit;
    _ = FactoryEntry;
    _ = Registry;
}

test "foreign source registry creates registered source" {
    const alloc = std.testing.allocator;

    const Dummy = struct {
        value: u32,

        fn destroy(ptr: *anyopaque, inner_alloc: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            inner_alloc.destroy(self);
        }

        fn query(ptr: *anyopaque, inner_alloc: Allocator, _: QueryParams) !QueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var rows = try inner_alloc.alloc(std.json.Value, 1);
            rows[0] = .{ .integer = self.value };
            return .{ .rows = rows, .total = 1 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !TableStatistics {
            return .{ .row_count = 7, .size_bytes = 128 };
        }

        fn factory(inner_alloc: Allocator, config: Config) !Source {
            var owned_config = config;
            defer owned_config.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            self.* = .{ .value = 42 };
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                },
            };
        }
    };

    var registry = Registry{};
    defer registry.deinit(alloc);
    try registry.register(alloc, .postgres, Dummy.factory);

    const config = Config{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://example"),
    };
    var source_instance = try registry.create(alloc, config);
    defer source_instance.deinit(alloc);

    var params = QueryParams{
        .table = try alloc.dupe(u8, "customers"),
    };
    defer params.deinit(alloc);
    var result = try source_instance.query(alloc, params);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), result.total);
    try std.testing.expectEqual(@as(i64, 42), result.rows[0].integer);
    try std.testing.expectEqual(@as(i64, 7), (try source_instance.statistics("customers")).row_count);
    try std.testing.expectError(
        error.UnsupportedDeadline,
        source_instance.statisticsWithDeadline("customers", 1),
    );
    var poll_params = ReplicationPollParams{
        .table = try alloc.dupe(u8, "customers"),
    };
    defer poll_params.deinit(alloc);
    try std.testing.expectError(error.UnsupportedReplicationStreaming, source_instance.pollChanges(alloc, poll_params));
}

test "foreign source registry rejects unsupported source kinds" {
    const alloc = std.testing.allocator;
    var registry = Registry{};
    defer registry.deinit(alloc);

    var config = Config{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://example"),
    };
    defer config.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSourceKind, registry.create(alloc, config));
}

test "foreign source registry creates context-bound source" {
    const alloc = std.testing.allocator;

    const Dummy = struct {
        label: []const u8,

        fn destroy(ptr: *anyopaque, inner_alloc: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            inner_alloc.destroy(self);
        }

        fn query(ptr: *anyopaque, inner_alloc: Allocator, params: QueryParams) !QueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var owned_params = params;
            defer owned_params.deinit(inner_alloc);
            const row_text = try std.fmt.allocPrint(inner_alloc, "{{\"label\":\"{s}\"}}", .{self.label});
            defer inner_alloc.free(row_text);
            const rows = try inner_alloc.alloc(std.json.Value, 1);
            rows[0] = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, row_text, .{});
            return .{ .rows = rows, .total = 1 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !TableStatistics {
            return .{ .row_count = 1, .size_bytes = 32 };
        }

        const Context = struct {
            label: []u8,
        };

        fn contextDeinit(ptr: *anyopaque, inner_alloc: Allocator) void {
            const ctx: *Context = @ptrCast(@alignCast(ptr));
            inner_alloc.free(ctx.label);
            inner_alloc.destroy(ctx);
        }

        fn factory(ctx_ptr: *anyopaque, inner_alloc: Allocator, config: Config) !Source {
            const ctx: *Context = @ptrCast(@alignCast(ctx_ptr));
            var owned = config;
            defer owned.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            self.* = .{ .label = ctx.label };
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                },
            };
        }
    };

    var registry = Registry{};
    defer registry.deinit(alloc);

    const ctx = try alloc.create(Dummy.Context);
    ctx.* = .{ .label = try alloc.dupe(u8, "ctx") };
    try registry.registerWithContext(alloc, .postgres, ctx, Dummy.factory, Dummy.contextDeinit);

    var src = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://example"),
    });
    defer src.deinit(alloc);

    var result = try src.query(alloc, .{
        .table = try alloc.dupe(u8, "customers"),
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("ctx", result.rows[0].object.get("label").?.string);
}

test "foreign transaction envelopes validate exact contiguous committed source coverage" {
    var changes = [_]ReplicationChange{
        .{ .op = .insert, .checkpoint = @constCast("0/20"), .commit_timestamp_ms = 9 },
        .{ .op = .update, .checkpoint = @constCast("0/20"), .commit_timestamp_ms = 9 },
        .{ .op = .delete, .checkpoint = @constCast("0/40"), .commit_timestamp_ms = 10 },
    };
    var transactions = [_]ReplicationTransaction{
        .{ .xid = 7, .final_lsn = 20, .commit_lsn = 20, .end_lsn = 32, .commit_timestamp_ms = 9, .first_change = 0, .change_count = 2, .checkpoint = @constCast("0/20") },
        .{ .xid = 8, .final_lsn = 40, .commit_lsn = 40, .end_lsn = 64, .commit_timestamp_ms = 10, .first_change = 2, .change_count = 1, .checkpoint = @constCast("0/40") },
    };
    const result: ReplicationPollResult = .{ .changes = &changes, .transactions = &transactions, .checkpoint = @constCast("0/40") };
    try result.validateTransactions();
    const original_second = transactions[1];
    transactions[1].final_lsn = 24;
    transactions[1].commit_lsn = 24;
    try std.testing.expectError(error.InvalidReplicationTransaction, result.validateTransactions());
    transactions[1] = original_second;
    for ([_][]const u8{ "0/41", "00/40", "0/0040", "0/ff", "invalid" }) |checkpoint| {
        transactions[1].checkpoint = @constCast(checkpoint);
        try std.testing.expectError(error.InvalidReplicationTransaction, result.validateTransactions());
    }
    transactions[1] = original_second;
    // Corruption in a later envelope is rejected before consuming the prefix.
    transactions[1].first_change = 1;
    try std.testing.expectError(error.InvalidReplicationTransaction, result.validateTransactions());
    transactions[1].first_change = 2;
    changes[1].checkpoint = @constCast("0/30");
    try std.testing.expectError(error.InvalidReplicationTransaction, result.validateTransactions());
    // Existing providers retain the independent-row contract explicitly.
    const legacy: ReplicationPollResult = .{ .changes = &changes };
    try legacy.validateTransactions();
}

test "replication initialization observes only genuine provider origin through a joined checked callback" {
    const Probe = struct {
        calls: usize = 0,
        fail: bool = false,
        fn check(_: *anyopaque) !void {}
        fn observe(raw: *anyopaque, origin: ReplicationIdentity) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expectEqual(@as(u32, 42), origin.relation_id);
            try std.testing.expectEqual(@as(u8, 7), origin.provider_identity[0]);
            if (self.fail) return error.CutoverAuthorityLost;
        }
    };
    var probe: Probe = .{};
    const actual: ReplicationIdentity = .{ .provider_identity = @splat(7), .relation_id = 42 };
    const params: ReplicationPollParams = .{ .table = @constCast("items"), .expected_origin = actual, .initialization_origin = .{ .ptr = &probe, .observe_fn = Probe.observe, .check_fn = Probe.check } };
    const custody = try params.snapshotCustody();
    try std.testing.expect(custody == .initialization);
    try custody.check();
    try custody.bindOrigin(actual);
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    var mixed = params;
    mixed.retired_slot_name = @constCast("legacy-slot");
    try std.testing.expectError(error.InvalidQueryRequest, mixed.snapshotCustody());
    try std.testing.expectError(error.CutoverAuthorityLost, custody.retirementComplete());
    try std.testing.expectError(error.ForeignProviderIdentityMismatch, params.observeInitializationOrigin(.{ .provider_identity = @splat(8), .relation_id = 42 }));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    probe.fail = true;
    try std.testing.expectError(error.CutoverAuthorityLost, params.observeInitializationOrigin(actual));
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
}

test "logical subscription provider request refuses ambiguous slot lock and ACK identities before callbacks" {
    const a = std.testing.allocator;
    var request: SubscriptionRequest = .{ .publications = &.{ "first", "second" }, .slot_name = "birth_17", .lock_name = "subscription:3:17:birth:4", .expected = null, .authority = undefined, .execution_deadline_ns = 1, .checkpoint = "A/FFFFFFFF" };
    try request.validate(a);
    const checkpoints = [_][]const u8{ "a/FFFFFFFF", "0A/1", "A/0001", "A/", "/1", "1/100000000", "1/1/1", "1/1\x00" };
    for (checkpoints) |checkpoint| {
        request.checkpoint = checkpoint;
        try std.testing.expectError(error.InvalidQueryRequest, request.validate(a));
    }
    request.checkpoint = "0/0";
    try request.validate(a);
    request.lock_name = "birth\x00other";
    try std.testing.expectError(error.InvalidQueryRequest, request.validate(a));
    request.lock_name = "birth";
    request.slot_name = "Birth_17";
    try std.testing.expectError(error.InvalidQueryRequest, request.validate(a));
    request.slot_name = "birth_17";
    request.publications = &.{ "first", "first" };
    try std.testing.expectError(error.InvalidQueryRequest, request.validate(a));
}

test "subscription retained capture uses only the current quantum checkpoint and cutoff" {
    const Checkpoint = struct {
        checks: usize = 0,
        active: bool = true,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (!self.active) return error.DeadlineExceeded;
        }
        fn observe(_: *anyopaque, _: SubscriptionOrigin.Observation) !void {
            return error.TestUnexpectedResult;
        }
        fn acknowledged(_: *anyopaque, _: []const u8) !void {
            return error.TestUnexpectedResult;
        }
        fn authority(self: *@This()) SubscriptionAuthority {
            return .{ .ptr = self, .check_fn = check, .observe_fn = observe, .acknowledged_fn = acknowledged };
        }
    };
    const Reader = struct {
        calls: usize = 0,
        cutoff: u64 = 0,
        fn capture(raw: *anyopaque, _: Allocator, _: u32, _: usize, deadline: u64, cancellation: CancellationToken) !QueryResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try cancellation.check();
            self.calls += 1;
            self.cutoff = deadline;
            return .{};
        }
        fn close(_: *anyopaque, _: Allocator) void {}
    };
    var reader: Reader = .{};
    const snapshot: SubscriptionSnapshot = .{ .ptr = &reader, .vtable = &.{ .capture_relation = Reader.capture, .deinit = Reader.close }, .origin = .{ .provider_identity = @splat(1), .relations = &.{} }, .checkpoint = "0/1", .snapshot_token = "actual-reader-owned-token" };
    var first: Checkpoint = .{};
    var second: Checkpoint = .{};
    var initial = try snapshot.captureRelation(std.testing.allocator, 7, 8, 100, first.authority());
    initial.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), first.checks);
    first.active = false;
    var later = try snapshot.captureRelation(std.testing.allocator, 7, 8, 200, second.authority());
    later.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), first.checks);
    try std.testing.expectEqual(@as(usize, 3), second.checks);
    try std.testing.expectEqual(@as(u64, 200), reader.cutoff);
    second.active = false;
    try std.testing.expectError(error.DeadlineExceeded, snapshot.captureRelation(std.testing.allocator, 7, 8, 300, second.authority()));
    try std.testing.expectEqual(@as(usize, 2), reader.calls);
}
