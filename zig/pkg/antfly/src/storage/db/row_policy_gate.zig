// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Per-owner admission barrier for row-policy publication. Metadata must first
//! fence every owner into `preparing`, wait for in-flight leases to drain, and
//! only then announce an active generation. An owner never treats a policy
//! definition alone as permission to serve filtered reads.
const std = @import("std");
const catalog = @import("table_catalog.zig");
const policies = @import("../../system_catalog/policies.zig");
const scalar = @import("../../sql/scalar.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const role_authority = @import("../../usermgr/row_policy_authority.zig");
const platform_time = @import("antfly_platform").time;

/// Evaluate only referenced typed ordinals before projection/pagination. A
/// JSON-null cell remains a non-SQL-null datum, while absent and SQL-NULL
/// cells are SQL unknown. The row codec verifies physical integrity as each
/// referenced cell is fetched; any failure aborts the read, never grants it.
pub fn permitsOrdinal(evaluator: *const policies.Evaluator, alloc: std.mem.Allocator, row: codec.OrdinalRowView) !bool {
    var scratch = try EvaluationScratch.init(alloc, evaluator);
    defer scratch.deinit();
    return scratch.permits(row);
}

/// One workspace per retained cursor. Only referenced ordinals are touched
/// for each row, and temporary cell/predicate allocations reuse the arena's
/// retained capacity. The full Datum vector is allocated once, not per row.
pub const EvaluationScratch = struct {
    evaluator: *const policies.Evaluator,
    alloc: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    cells: []scalar.Datum,

    pub fn init(alloc: std.mem.Allocator, evaluator: *const policies.Evaluator) !EvaluationScratch {
        const cells = try alloc.alloc(scalar.Datum, evaluator.column_count);
        @memset(cells, .{});
        return .{ .evaluator = evaluator, .alloc = alloc, .arena = std.heap.ArenaAllocator.init(alloc), .cells = cells };
    }

    pub fn deinit(self: *EvaluationScratch) void {
        self.arena.deinit();
        self.alloc.free(self.cells);
        self.* = undefined;
    }

    pub fn permits(self: *EvaluationScratch, row: codec.OrdinalRowView) !bool {
        const evaluator = self.evaluator;
        if (row.table_schema.version != evaluator.schema_version or row.table_schema.relational_columns.len != evaluator.column_count)
            return error.RowPolicyCatalogChanged;
        _ = self.arena.reset(.retain_capacity);
        const scratch = self.arena.allocator();
        for (evaluator.required_columns) |ordinal| {
            const index: usize = @intCast(ordinal);
            self.cells[index] = .{};
            const cell = (try row.findCell(index)) orelse continue;
            self.cells[index] = .{ .value = try row.materializeCellAlloc(scratch, cell), .sql_null = cell.is_null };
        }
        return evaluator.permitsWithScratch(scratch, self.cells);
    }
};

pub const Gate = struct {
    phase: std.atomic.Value(u8) = .init(@intFromEnum(catalog.RowPolicyPhase.disabled)),
    generation: std.atomic.Value(u64) = .init(0),
    catalog_epoch: std.atomic.Value(u64) = .init(0),
    schema_version: std.atomic.Value(u32) = .init(0),
    readers: std.atomic.Value(usize) = .init(0),

    pub fn init(state: catalog.Catalog) Gate {
        return .{
            .phase = .init(@intFromEnum(state.row_policy_phase)),
            .generation = .init(state.row_policy_generation),
            .catalog_epoch = .init(state.row_policy_catalog_epoch),
            .schema_version = .init(state.active_schema_version),
        };
    }

    pub fn currentPhase(self: *const Gate) catalog.RowPolicyPhase {
        return @enumFromInt(self.phase.load(.acquire));
    }

    pub const Lease = struct {
        gate: *Gate,
        released: bool = false,
        expires_at_seconds: ?i64 = null,
        expires_at_monotonic_ns: ?u64 = null,
        /// Only read leases are revoked by publication. Previously admitted
        /// writes retain atomic commit semantics while the owner waits for
        /// them to drain; a stale read must never emit a late unfiltered page.
        read_phase: ?catalog.RowPolicyPhase = null,

        pub fn checkAt(self: *const Lease, now_seconds: i64) !void {
            if (self.released) return error.RowPolicyCatalogChanged;
            if (self.read_phase) |phase| if (self.gate.currentPhase() != phase) return error.RowPolicyCatalogChanged;
            if (self.expires_at_seconds) |expiry| if (now_seconds > expiry) return error.RowPolicyAuthenticationRequired;
            if (self.expires_at_monotonic_ns) |deadline| if (platform_time.monotonicNs() >= deadline) return error.RowPolicyAuthenticationRequired;
        }

        /// A delayed page of the same pinned statement may retain its
        /// existing admission while publication drains. This does not admit
        /// a new statement: the parent lease must still be live, and its
        /// caller must enforce the original statement deadline.
        pub fn clone(self: *const Lease) Lease {
            std.debug.assert(!self.released);
            const prior = self.gate.readers.fetchAdd(1, .acq_rel);
            std.debug.assert(prior > 0);
            return .{ .gate = self.gate, .expires_at_seconds = self.expires_at_seconds, .expires_at_monotonic_ns = self.expires_at_monotonic_ns, .read_phase = self.read_phase };
        }

        pub fn release(self: *Lease) void {
            if (self.released) return;
            self.released = true;
            const prior = self.gate.readers.fetchSub(1, .acq_rel);
            std.debug.assert(prior > 0);
        }
    };

    /// A raw local/Lite operation is safe only before any policy transition.
    /// The second phase check closes the race with beginPreparing(). The
    /// caller must retain this lease through the last byte it returns or the
    /// final write commit, including across paginated read sessions.
    pub fn enterRaw(self: *Gate) !Lease {
        if (self.currentPhase() != .disabled) return error.RowPolicyAuthenticationRequired;
        _ = self.readers.fetchAdd(1, .acq_rel);
        if (self.currentPhase() != .disabled) {
            _ = self.readers.fetchSub(1, .acq_rel);
            return error.RowPolicyAuthenticationRequired;
        }
        return .{ .gate = self };
    }

    pub fn enterRawRead(self: *Gate) !Lease {
        var lease = try self.enterRaw();
        lease.read_phase = .disabled;
        return lease;
    }

    /// Search/aggregation needs owner-side candidate filtering before top-k,
    /// not a caller-side post-filter. Report this as unsupported rather than
    /// suggesting that supplying an authentication token alone would help.
    pub fn enterUnsupportedSearch(self: *Gate) !Lease {
        if (self.currentPhase() != .disabled) return error.RowPolicyUnsupported;
        return self.enterRaw() catch return error.RowPolicyUnsupported;
    }

    /// A trusted front end captures one immutable policy/setting view, bound
    /// to the authenticated principal and current owner epoch. Every row in
    /// this lease must be evaluated before projection, limit or mutation.
    fn enterBound(self: *Gate, view: *const policies.View) !Lease {
        if (self.currentPhase() != .active) return error.RowPolicyCatalogChanged;
        _ = self.readers.fetchAdd(1, .acq_rel);
        errdefer _ = self.readers.fetchSub(1, .acq_rel);
        if (self.currentPhase() != .active) return error.RowPolicyCatalogChanged;
        try view.requireCurrent(self.generation.load(.acquire), self.catalog_epoch.load(.acquire), self.schema_version.load(.acquire));
        return .{ .gate = self };
    }

    /// The caller must obtain `principal` only from a successful
    /// role_authority.verify() using this owner's provisioned verifier key.
    /// The policy program itself must come from owner-local committed state,
    /// never from the same request that supplied the signed principal token.
    pub fn enterBoundWithVerifiedPrincipal(self: *Gate, view: *const policies.View, principal: *const role_authority.Payload, now_seconds: i64) !Lease {
        if (principal.table_id != view.snapshot.table_id or
            principal.policy_generation != view.snapshot.policy_generation or
            principal.catalog_epoch != view.snapshot.catalog_epoch or
            !std.mem.eql(u8, principal.principal, view.snapshot.principal) or
            principal.auth_revision == 0 or principal.expires < now_seconds or
            principal.expires > now_seconds +| role_authority.ttl_seconds or
            view.snapshot.roles.len != 0)
            return error.RowPolicyAuthenticationRequired;
        var lease = try self.enterBound(view);
        if (principal.access == .read) lease.read_phase = .active;
        lease.expires_at_seconds = principal.expires;
        const remaining_seconds: u64 = @intCast(principal.expires - now_seconds);
        lease.expires_at_monotonic_ns = platform_time.monotonicNs() +| remaining_seconds *| std.time.ns_per_s;
        return lease;
    }

    /// Pin the active generation before dereferencing the owner-local bundle.
    /// This closes the race with publication replacing that bundle after old
    /// readers drain. The caller must separately verify the signed token and
    /// then build/evaluate a View from the pinned immutable bundle.
    pub fn enterVerifiedPrincipal(self: *Gate, principal: *const role_authority.Payload, now_seconds: i64) !Lease {
        if (principal.table_id == 0 or principal.auth_revision == 0 or
            principal.expires < now_seconds or principal.expires > now_seconds +| role_authority.ttl_seconds or
            self.currentPhase() != .active or
            principal.policy_generation != self.generation.load(.acquire) or
            principal.catalog_epoch != self.catalog_epoch.load(.acquire))
            return error.RowPolicyAuthenticationRequired;
        _ = self.readers.fetchAdd(1, .acq_rel);
        errdefer _ = self.readers.fetchSub(1, .acq_rel);
        if (self.currentPhase() != .active or
            principal.policy_generation != self.generation.load(.acquire) or
            principal.catalog_epoch != self.catalog_epoch.load(.acquire))
            return error.RowPolicyCatalogChanged;
        const remaining_seconds: u64 = @intCast(principal.expires - now_seconds);
        return .{
            .gate = self,
            .expires_at_seconds = principal.expires,
            .expires_at_monotonic_ns = platform_time.monotonicNs() +| remaining_seconds *| std.time.ns_per_s,
            .read_phase = if (principal.access == .read) .active else null,
        };
    }

    /// A private data-Raft entry was admitted with a fresh signed proof before
    /// proposal. Replicas validate that proof against the admission instant,
    /// then pin this exact owner generation without consulting replay-time
    /// wall clocks. This method must never be used for a direct request.
    pub fn enterReplicatedPrincipal(self: *Gate, principal: *const role_authority.Payload) !Lease {
        if (principal.table_id == 0 or principal.auth_revision == 0 or
            principal.access != .write or self.currentPhase() != .active or
            principal.policy_generation != self.generation.load(.acquire) or
            principal.catalog_epoch != self.catalog_epoch.load(.acquire))
            return error.RowPolicyAuthenticationRequired;
        _ = self.readers.fetchAdd(1, .acq_rel);
        errdefer _ = self.readers.fetchSub(1, .acq_rel);
        if (self.currentPhase() != .active or
            principal.policy_generation != self.generation.load(.acquire) or
            principal.catalog_epoch != self.catalog_epoch.load(.acquire))
            return error.RowPolicyCatalogChanged;
        return .{ .gate = self };
    }

    /// Start a distributed policy generation transition. This memory fence
    /// precedes the durable preparing commit; on failure, the coordinator may
    /// roll back only if metadata has not activated the generation. Existing
    /// leases remain valid until they release, and all new raw/bound requests
    /// fail closed while the coordinator waits for quiescence.
    pub fn beginPreparing(self: *Gate, previous: catalog.RowPolicyPhase) !void {
        if (previous == .preparing) return error.RowPolicyCatalogChanged;
        if (self.phase.cmpxchgStrong(@intFromEnum(previous), @intFromEnum(catalog.RowPolicyPhase.preparing), .acq_rel, .acquire) != null)
            return error.RowPolicyCatalogChanged;
    }

    pub fn quiesced(self: *const Gate) bool {
        return self.readers.load(.acquire) == 0;
    }

    /// Called only after the matching owner-local catalog transaction commits
    /// and all old leases have drained. The phase release-publishes the exact
    /// generation/epoch/schema tuple to later bound admissions.
    pub fn publishCommitted(self: *Gate, state: catalog.Catalog) !void {
        if (!self.quiesced()) return error.RowPolicyReadersActive;
        if (self.currentPhase() != .preparing) return error.RowPolicyCatalogChanged;
        if (state.row_policy_phase == .disabled or state.row_policy_phase == .active) {
            if (state.row_policy_phase == .active and
                (state.row_policy_generation == 0 or state.row_policy_catalog_epoch == 0 or
                    state.active_schema_version == 0 or state.storage_mode != .relational or
                    state.row_policy_generation < self.generation.load(.acquire))) return error.RowPolicyCatalogChanged;
            if (state.row_policy_phase == .disabled and
                (state.row_policy_generation < self.generation.load(.acquire) or
                    state.row_policy_catalog_epoch == 0)) return error.RowPolicyCatalogChanged;
            self.generation.store(state.row_policy_generation, .monotonic);
            self.catalog_epoch.store(state.row_policy_catalog_epoch, .monotonic);
            self.schema_version.store(state.active_schema_version, .monotonic);
            self.phase.store(@intFromEnum(state.row_policy_phase), .release);
            return;
        }
        return error.RowPolicyCatalogChanged;
    }
};

test "row-policy gate drains raw leases before publishing a generation" {
    var gate = Gate.init(.{ .mode_initialized = true, .storage_mode = .relational, .active_schema_version = 3 });
    var lease = try gate.enterRaw();
    try gate.beginPreparing(.disabled);
    try std.testing.expectError(error.RowPolicyUnsupported, gate.enterUnsupportedSearch());
    var delayed_page = lease.clone();
    try std.testing.expectError(error.RowPolicyAuthenticationRequired, gate.enterRaw());
    try std.testing.expectError(error.RowPolicyUnsupported, gate.enterUnsupportedSearch());
    try std.testing.expect(!gate.quiesced());
    try std.testing.expectError(error.RowPolicyReadersActive, gate.publishCommitted(.{ .mode_initialized = true, .storage_mode = .relational, .active_schema_version = 3, .row_policy_phase = .active, .row_policy_generation = 7, .row_policy_catalog_epoch = 9 }));
    lease.release();
    try std.testing.expect(!gate.quiesced());
    delayed_page.release();
    try std.testing.expect(gate.quiesced());
    try gate.publishCommitted(.{ .mode_initialized = true, .storage_mode = .relational, .active_schema_version = 3, .row_policy_phase = .active, .row_policy_generation = 7, .row_policy_catalog_epoch = 9 });
    try std.testing.expectError(error.RowPolicyAuthenticationRequired, gate.enterRaw());
    var view: policies.View = .{ .arena = std.heap.ArenaAllocator.init(std.testing.allocator), .snapshot = .{
        .table_id = 4,
        .schema_version = 3,
        .schema_digest = @splat(0),
        .policy_generation = 7,
        .catalog_epoch = 9,
        .principal = "alice",
        .database = "main",
        .records = &.{},
    } };
    defer view.deinit();
    const secret = "1234567890abcdef1234567890abcdef";
    const scope: role_authority.Scope = .{ .table_id = 4, .table = "orders", .database = "main", .policy_generation = 7, .catalog_epoch = 9 };
    const token = try role_authority.sign(std.testing.allocator, secret, "cluster-a", .{ .principal = "alice", .roles = &.{}, .auth_revision = 2 }, scope, 100);
    defer std.testing.allocator.free(token);
    var principal = try role_authority.verify(std.testing.allocator, secret, "cluster-a", scope, 100, token);
    defer principal.deinit();
    var bound = try gate.enterBoundWithVerifiedPrincipal(&view, &principal.value, 100);
    try std.testing.expect(!gate.quiesced());
    try bound.checkAt(130);
    try std.testing.expectError(error.RowPolicyAuthenticationRequired, bound.checkAt(131));
    bound.release();
    view.snapshot.policy_generation = 6;
    try std.testing.expectError(error.RowPolicyAuthenticationRequired, gate.enterBoundWithVerifiedPrincipal(&view, &principal.value, 100));
    try std.testing.expect(gate.quiesced());
    try gate.beginPreparing(.active);
    try gate.publishCommitted(.{ .mode_initialized = true, .storage_mode = .relational, .active_schema_version = 3, .row_policy_phase = .disabled, .row_policy_generation = 8, .row_policy_catalog_epoch = 10 });
    try std.testing.expectEqual(@as(u64, 8), gate.generation.load(.acquire));
    var after_revoke = try gate.enterRaw();
    after_revoke.release();
}

test "row-policy gate revokes a retained raw read before late output" {
    var gate = Gate.init(.{ .mode_initialized = true, .storage_mode = .relational, .active_schema_version = 3 });
    var reader = try gate.enterRawRead();
    defer reader.release();
    try reader.checkAt(100);
    try gate.beginPreparing(.disabled);
    try std.testing.expectError(error.RowPolicyCatalogChanged, reader.checkAt(100));
    var delayed = reader.clone();
    defer delayed.release();
    try std.testing.expectError(error.RowPolicyCatalogChanged, delayed.checkAt(100));
    try std.testing.expect(!gate.quiesced());
}
