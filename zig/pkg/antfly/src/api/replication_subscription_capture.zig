// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Retain one live provider cut in bounded work quanta.
const std = @import("std");
const local = @import("antfly_local_sources");
const foreign = @import("../foreign/source.zig");
const codec = @import("../system_catalog/replication/snapshot_codec.zig");
const registry = @import("../system_catalog/prepared_transactions.zig");
const purpose = local.api_replication_source_purpose;
const payload = @import("../system_catalog/replication/payload.zig");
const artifacts = @import("../serverless/artifacts/store.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;

const admission = @import("replication_subscription_admission.zig");
const Attestation = @import("../metadata/replication_initialization_attestation.zig");
const Lease = local.system_catalog_logical_replication_catalog.SubscriptionWorkLease;
const Context = local.api_operation.RequestContext;
const Bootstrap = @FieldType(@FieldType(registry.Mutation, "operation"), "replication_admit");

const Slots = @import("../system_catalog/replication/decision_journal.zig");
const Publisher = local.system_catalog_logical_replication_publisher;
const Work = @import("../metadata/replication_subscription_work.zig");

const UserManager = @import("../usermgr/mod.zig").UserManager;
const Actor = local.system_catalog_logical_replication_catalog.ExecutionActor;

pub const Credential = struct {
    auth_enabled: bool,
    manager: ?*UserManager,
    actor: Actor,

    pub fn requiresProviderPassword(self: Credential) bool {
        return self.actor.session_user.kind != .bootstrap or self.actor.current_role != null;
    }

    pub fn require(self: Credential, table: []const u8, privileges: u16) !void {
        try self.actor.validate();
        if (!self.auth_enabled and self.actor.session_user.kind == .bootstrap) return;
        const manager = self.manager orelse return error.Forbidden;
        if (self.actor.session_user.kind != .user or
            !try manager.sqlRetainedCredentialPermits(self.actor.session_user.instance, self.actor.credential, table, privileges))
            return error.Forbidden;
    }
};

/// All provider attempts originate from the committed metadata control lane.
pub const CaptureControl = struct {
    const ExportAction = @FieldType(@FieldType(Publisher.Control, "transition_subscription_export"), "action");
    source: @import("http_server.zig").StatusSource,
    io: std.Io,
    realm: Publisher.Realm,
    context: Context,
    auth_enabled: bool = false,
    user_manager: ?*UserManager = null,

    pub fn claim(self: @This(), a: A, item: Work.Item, worker: [16]u8) !Lease {
        if (item.phase != .initialize) return error.ReplicationCutoverAuthorityLost;
        try Work.requireDatabase(item, self.realm.database_id);
        const now = @import("antfly_platform").time.realtimeNs();
        const lease = try self.issue(a, .{ .claim_subscription = .{
            .item = item,
            .worker = worker,
            .observed_at_unix_ns = now,
            .expires_at_unix_ns = now +| 60 * std.time.ns_per_s,
        } });
        if (!std.meta.eql(lease.item, item) or !std.mem.eql(u8, &lease.worker, &worker))
            return error.InvalidSqlBackendResponse;
        return lease;
    }

    fn start(self: @This(), a: A, lease: Lease, identity: Publisher.Digest) !Lease {
        if (lease.provider_attempt != null) {
            if (!std.meta.eql(lease.provider_preflight_identity, @as(?Publisher.Digest, identity)))
                return error.ForeignProviderIdentityMismatch;
            return lease;
        }
        var attempt: [16]u8 = undefined;
        try self.io.randomSecure(&attempt);
        const started = try self.issue(a, .{ .start_subscription = .{
            .lease = lease,
            .attempt = attempt,
            .provider_preflight_identity = identity,
            .observed_at_unix_ns = @import("antfly_platform").time.realtimeNs(),
        } });
        if (!std.meta.eql(started.item, lease.item) or started.generation != lease.generation or
            !std.mem.eql(u8, &started.worker, &lease.worker) or !std.meta.eql(started.provider_attempt, @as(?[16]u8, attempt)) or
            !std.meta.eql(started.provider_preflight_identity, @as(?Publisher.Digest, identity)))
            return error.InvalidSqlBackendResponse;
        return started;
    }

    pub fn bindExport(self: @This(), a: A, lease: Lease, origin: Publisher.Digest, receipt: local.system_catalog_logical_replication_catalog.SubscriptionExport) !Lease {
        const bound = try self.issue(a, .{ .bind_subscription_export = .{
            .lease = lease,
            .attempt = lease.provider_attempt orelse return error.ReplicationCutoverAuthorityLost,
            .origin = origin,
            .receipt = receipt,
            .observed_at_unix_ns = @import("antfly_platform").time.realtimeNs(),
        } });
        var expected = lease;
        expected.provider_origin = origin;
        expected.provider_export = receipt;
        if (!std.meta.eql(bound, expected)) return error.InvalidSqlBackendResponse;
        return bound;
    }

    fn transitionExport(self: @This(), a: A, lease: Lease, action: ExportAction) !Lease {
        const updated = try self.issue(a, .{ .transition_subscription_export = .{
            .lease = lease,
            .action = action,
            .observed_at_unix_ns = @import("antfly_platform").time.realtimeNs(),
        } });
        var expected = lease;
        expected.generation = std.math.add(u64, expected.generation, 1) catch return error.InvalidSqlBackendResponse;
        if (action != .fence) {
            expected.provider_attempt = null;
            expected.provider_preflight_identity = null;
            expected.provider_origin = null;
            expected.provider_export = null;
        } else expected.provider_export.?.retiring = true;
        if (!std.meta.eql(updated, expected)) return error.InvalidSqlBackendResponse;
        return updated;
    }

    pub fn check(self: @This(), a: A, lease: Lease) !void {
        const attempt = lease.provider_attempt orelse return error.ReplicationCutoverAuthorityLost;
        if (!try self.read(a, bool, .{ .subscription_permission = .{
            .realm = self.realm,
            .lease = lease,
            .attempt = attempt,
            .origin = lease.provider_origin,
        } })) return error.ReplicationCutoverAuthorityLost;
    }

    pub fn definition(self: @This(), a: A, lease: Lease) !Work.CaptureDefinition {
        return self.read(a, Work.CaptureDefinition, .{ .subscription_capture_definition = .{
            .realm = self.realm,
            .lease = lease,
            .attempt = lease.provider_attempt,
        } });
    }

    fn read(self: @This(), a: A, comptime T: type, query: @import("../system_catalog/replication/installation.zig").SlotRead) !T {
        if (!self.context.setting_admin) return error.Forbidden;
        try self.context.ensureActive();
        const bytes = try self.source.systemCatalog(a, self.context, .{ .sql_publication_capture_work = .{ .slot = query } });
        defer a.free(bytes);
        return std.json.parseFromSliceLeaky(T, a, bytes, .{ .allocate = .alloc_always });
    }

    fn issueControl(self: @This(), a: A, action: Publisher.Control) !Slots.ControlResult {
        const head = try self.read(a, u64, .{ .head = self.realm });
        var nonce: [16]u8 = undefined;
        try self.io.randomSecure(&nonce);
        const request: Slots.ControlRequest = .{ .realm = self.realm, .nonce = nonce, .observed_head = head, .action = action };
        const result = result: {
            const bytes = self.source.systemCatalog(a, self.context, .{ .sql_publication_slot_control = request }) catch |err| switch (err) {
                error.MetadataMutationOutcomeUnknown => break :result (try self.read(a, ?Slots.ControlResult, .{ .outcome = request })) orelse return err,
                else => return err,
            };
            defer a.free(bytes);
            break :result try std.json.parseFromSliceLeaky(Slots.ControlResult, a, bytes, .{});
        };
        return result;
    }

    pub fn cleanupDefinition(self: @This(), a: A, item: Work.Item, disabling: bool) !Work.CleanupDefinition {
        return self.read(a, Work.CleanupDefinition, .{ .subscription_cleanup_definition = .{ .realm = self.realm, .item = item, .disabling = disabling } });
    }

    pub fn finishDrop(self: @This(), a: A, item: Work.Item, checkpoint: u64) !void {
        const result = try self.issueControl(a, .{ .finish_subscription_drop = .{ .item = item, .checkpoint = checkpoint } });
        switch (result) {
            .none => {},
            .refused => |refusal| return controlRefusal(refusal),
            else => return error.InvalidSqlBackendResponse,
        }
    }

    pub fn finishDisable(self: @This(), a: A, item: Work.Item, checkpoint: ?u64) !void {
        const result = try self.issueControl(a, .{ .finish_subscription_disable = .{ .item = item, .checkpoint = checkpoint } });
        switch (result) {
            .none => {},
            .refused => |refusal| return controlRefusal(refusal),
            else => return error.InvalidSqlBackendResponse,
        }
    }

    fn issue(self: @This(), a: A, action: Publisher.Control) !Lease {
        return switch (try self.issueControl(a, action)) {
            .subscription_lease => |lease| blk: {
                try lease.validate();
                break :blk lease;
            },
            .refused => |refusal| controlRefusal(refusal),
            else => error.InvalidSqlBackendResponse,
        };
    }
    fn controlRefusal(refusal: @FieldType(Slots.ControlResult, "refused")) anyerror {
        return switch (refusal) {
            .ReplicationSlotLeaseChanged => error.ReplicationSlotLeaseChanged,
            .ReplicationCutoverAuthorityLost => error.ReplicationCutoverAuthorityLost,
            .LogicalReplicationLifecyclePending => error.LogicalReplicationLifecyclePending,
            else => error.InvalidReplicationCutoverIntent,
        };
    }
};

/// Owns the origin binding across one open provider snapshot. The allocator
/// and slot name must outlive the provider connection.
pub const CaptureAuthority = struct {
    allocator: A,
    control: CaptureControl,
    lease: Lease,
    slot_name: []const u8,
    identity: ?foreign.SubscriptionOrigin.Identity = null,
    credential: ?Credential = null,

    pub fn provider(self: *CaptureAuthority) foreign.SubscriptionAuthority {
        return .{ .ptr = self, .start_fn = startLease, .exported_fn = bindExported, .refused_fn = cancelUnexported, .check_fn = checkLease, .observe_fn = bindObserved, .acknowledged_fn = refuseAck };
    }

    fn startLease(raw: *anyopaque, identity: Publisher.Digest) !void {
        const self: *CaptureAuthority = @ptrCast(@alignCast(raw));
        var scratch: std.heap.ArenaAllocator = .init(self.allocator);
        defer scratch.deinit();
        if (self.credential) |credential| try credential.require("", 0);
        self.lease = try self.control.start(scratch.allocator(), self.lease, identity);
    }

    fn checkLease(raw: *anyopaque) !void {
        const self: *CaptureAuthority = @ptrCast(@alignCast(raw));
        var scratch: std.heap.ArenaAllocator = .init(self.allocator);
        defer scratch.deinit();
        if (self.credential) |credential| try credential.require("", 0);
        if (self.lease.provider_attempt != null) {
            try self.control.check(scratch.allocator(), self.lease);
        } else {
            _ = try self.control.definition(scratch.allocator(), self.lease);
        }
    }

    fn bindObserved(raw: *anyopaque, observed: foreign.SubscriptionOrigin.Observation) !void {
        const self: *CaptureAuthority = @ptrCast(@alignCast(raw));
        var scratch: std.heap.ArenaAllocator = .init(self.allocator);
        defer scratch.deinit();
        const a = scratch.allocator();
        const identity: foreign.SubscriptionOrigin.Identity = .{
            .owner = self.lease.item.owner,
            .origin_generation = self.lease.item.generation,
            .provider_identity = observed.provider_identity,
            .recipe_digest = self.lease.item.recipe_digest,
            .slot_name = self.slot_name,
            .roster_digest = try observed.digest(a),
        };
        try identity.validate();
        if (self.lease.provider_origin) |bound| {
            const realm = self.control.realm;
            const digest = try @import("../metadata/replication_digest.zig").digest("antfly/source-origin/v3", .{ realm.group_id, realm.incarnation, identity }, foreign.SubscriptionOrigin.max_encoded_bytes);
            if (!std.mem.eql(u8, &bound, &digest)) return error.ForeignProviderIdentityMismatch;
        }
        self.identity = identity;
    }

    fn bindExported(raw: *anyopaque, provider_identity: [32]u8, checkpoint: u64) !void {
        const self: *CaptureAuthority = @ptrCast(@alignCast(raw));
        const identity = self.identity orelse return error.ReplicationCutoverAuthorityLost;
        if (!std.mem.eql(u8, &identity.provider_identity, &provider_identity)) return error.ForeignProviderIdentityMismatch;
        var scratch: std.heap.ArenaAllocator = .init(self.allocator);
        defer scratch.deinit();
        if (self.credential) |credential| try credential.require("", 0);
        const a = scratch.allocator();
        const realm = self.control.realm;
        const digest = try @import("../metadata/replication_digest.zig").digest("antfly/source-origin/v3", .{ realm.group_id, realm.incarnation, identity }, foreign.SubscriptionOrigin.max_encoded_bytes);
        self.lease = try self.control.bindExport(a, self.lease, digest, .{
            .provider_identity = provider_identity,
            .checkpoint = checkpoint,
        });
    }

    fn cancelUnexported(raw: *anyopaque) !void {
        const self: *CaptureAuthority = @ptrCast(@alignCast(raw));
        var scratch: std.heap.ArenaAllocator = .init(self.allocator);
        defer scratch.deinit();
        self.lease = try self.control.transitionExport(scratch.allocator(), self.lease, .cancel_unexported);
    }

    fn refuseAck(_: *anyopaque, _: []const u8) !void {
        return error.ReplicationCutoverAuthorityLost;
    }
};

/// Holds one exported PostgreSQL snapshot across bounded coordinator turns.
/// All callback addresses remain stable until the connection is closed.
pub const ProviderCapture = struct {
    backing: A,
    arena: std.heap.ArenaAllocator,
    store: artifacts.ArtifactStore,
    authorization: CaptureAuthority,
    source: foreign.Source,
    snapshot: foreign.SubscriptionSnapshot,
    capture: Capture,
    ready: ?Bootstrap,

    pub fn open(backing: A, providers: *const foreign.Registry, store: artifacts.ArtifactStore, control: CaptureControl, item: Work.Item, worker: [16]u8) !*ProviderCapture {
        const self = try backing.create(ProviderCapture);
        errdefer backing.destroy(self);
        self.backing = backing;
        self.arena = .init(backing);
        errdefer self.arena.deinit();
        self.store = store;
        self.ready = null;
        const a = self.arena.allocator();
        const lease = try control.claim(a, item, worker);
        const definition = try control.definition(a, lease);
        const credential: Credential = .{
            .auth_enabled = control.auth_enabled,
            .manager = control.user_manager,
            .actor = definition.actor orelse return error.Forbidden,
        };
        try credential.require("", 0);
        if (!definition.options.connect or !definition.options.create_slot or definition.options.slot_name != .name)
            return error.UnsupportedExactCutover;
        const password_required = credential.requiresProviderPassword();
        const deadline = (try control.context.platformDeadline()).deadline_ns orelse return error.UnsupportedDeadline;
        self.authorization = .{ .allocator = backing, .control = control, .lease = lease, .slot_name = definition.options.slot_name.name, .credential = credential };
        self.source = try providers.create(a, .{ .kind = .postgres, .dsn = @constCast(definition.connection) });
        // The registered PostgreSQL factory consumes the configuration.
        errdefer self.source.deinit(a);
        const lock_name = try std.fmt.allocPrint(a, "antfly/subscription/{d}/{d}/{d}", .{
            control.realm.group_id, control.realm.database_id, item.owner.logical_subscription.subscription_id,
        });
        if (lease.provider_export) |receipt| {
            // Refuse delayed bootstrap commands from the abandoned capture
            // before relinquishing the source slot that they would adopt.
            self.authorization.lease = try control.transitionExport(a, lease, .fence);
            try self.source.cleanupReplication(a, .{
                .slot_name = @constCast(self.authorization.slot_name),
                .native_source = .{
                    .provider_identity = receipt.provider_identity,
                    .snapshot_checkpoint = receipt.checkpoint,
                    .durable_checkpoint = receipt.checkpoint,
                    .lock_name = lock_name,
                    .authority = self.authorization.provider(),
                },
                .execution_deadline_ns = deadline,
                .password_required = password_required,
            });
            _ = try control.transitionExport(a, self.authorization.lease, .retire);
            self.authorization.lease = try control.claim(a, item, worker);
        }
        self.snapshot = try self.source.beginSubscriptionSnapshot(a, .{
            .publications = definition.publications,
            .slot_name = self.authorization.slot_name,
            .lock_name = lock_name,
            .expected = null,
            .password_required = password_required,
            .authority = self.authorization.provider(),
            .execution_deadline_ns = deadline,
        });
        errdefer {
            self.snapshot.abort(a) catch {};
            self.snapshot.deinit(a);
        }
        const attempt = self.authorization.lease.provider_attempt orelse return error.ReplicationCutoverAuthorityLost;
        const identity = self.authorization.identity orelse return error.ReplicationCutoverAuthorityLost;
        const receipt = self.authorization.lease.provider_export orelse return error.ReplicationCutoverAuthorityLost;
        if (!std.meta.eql(receipt, @as(@TypeOf(receipt), .{
            .provider_identity = self.snapshot.origin.provider_identity,
            .checkpoint = try foreign.parseReplicationCheckpoint(self.snapshot.checkpoint),
        }))) return error.ForeignProviderIdentityMismatch;
        const bound = self.authorization.lease;
        self.capture = try Capture.init(a, &self.store, identity, .{
            .metadata_group = control.realm.group_id,
            .incarnation = control.realm.incarnation,
            .owner = item.owner,
            .birth = item.generation,
            .source_digest = bound.provider_origin orelse return error.ReplicationCutoverAuthorityLost,
            .recipe_digest = item.recipe_digest,
            .roster_digest = identity.roster_digest,
            .upload = .{ .token = bound.generation, .nonce = attempt, .expires_at_unix_ns = bound.expires_at_ns },
        }, &self.snapshot, definition.options.copy_data);
        return self;
    }

    pub fn deinit(self: *ProviderCapture) void {
        const a = self.arena.allocator();
        self.capture.deinit();
        // READY may already have an ambiguous metadata admission outcome.
        // Only an unpublished capture can cancel its live provider export.
        if (self.ready == null) self.snapshot.abort(a) catch {};
        self.snapshot.deinit(a);
        self.source.deinit(a);
        self.arena.deinit();
        self.backing.destroy(self);
    }

    pub fn step(self: *ProviderCapture, context: Context, max_rows: usize, signer: admission.SnapshotSigningDriver) !?Bootstrap {
        const bounded = try context.platformDeadline();
        const deadline = bounded.deadline_ns orelse return error.UnsupportedDeadline;
        self.authorization.control.context = bounded;
        const authority = self.authorization.provider();
        if (self.ready) |command| {
            try requireFreshBootstrap(command, @import("antfly_platform").time.realtimeNs());
            try authority.check();
            return command;
        }
        const current = self.authorization.lease;
        const remaining = current.expires_at_ns -| @import("antfly_platform").time.realtimeNs();
        if (remaining < 15 * std.time.ns_per_s) {
            var scratch: std.heap.ArenaAllocator = .init(self.backing);
            defer scratch.deinit();
            const renewed = try self.authorization.control.claim(scratch.allocator(), current.item, current.worker);
            if (renewed.generation != current.generation or !std.meta.eql(renewed.provider_attempt, current.provider_attempt) or
                !std.meta.eql(renewed.provider_preflight_identity, current.provider_preflight_identity) or
                !std.meta.eql(renewed.provider_origin, current.provider_origin) or !std.meta.eql(renewed.provider_export, current.provider_export)) return error.ReplicationCutoverAuthorityLost;
            self.authorization.lease = renewed;
            self.capture.held.upload.expires_at_unix_ns = renewed.expires_at_ns;
        }
        if (!try self.capture.step(max_rows, deadline, authority)) return null;
        const lease = self.authorization.lease;
        self.ready = self.capture.sealBootstrap(authority, lease, lease.provider_attempt.?, @import("antfly_platform").time.realtimeNs(), signer, bounded) catch |err| switch (err) {
            error.GroupLeaderUnavailable, error.MetadataMutationOutcomeUnknown => return null,
            else => return err,
        };
        return self.ready;
    }
};

fn requireFreshBootstrap(command: Bootstrap, now_ns: u64) !void {
    if (now_ns >= command.input.identity.expires_at_unix_ns)
        return error.ExternalTransactionAdmissionExpired;
}

pub fn reopenRetainedSnapshot(a: A, store: *artifacts.ArtifactStore, progress: registry.ReplicationProgress, cancellation: Cancellation) !codec.RetainedSnapshot {
    const boundary = progress.snapshot_boundary orelse return error.InvalidReplicationSnapshotCursor;
    const custody = boundary.custody orelse return error.InvalidReplicationSnapshotCursor;
    if (progress.pending != null or progress.checkpoint != 0 or progress.snapshot_cut != boundary.source_point or
        custody.purpose.birth != progress.birth or !std.mem.eql(u8, &custody.purpose.source_digest, &progress.origin) or
        custody.ready.source_point != boundary.source_point or custody.ready.total_rows != boundary.total_rows or
        !std.mem.eql(u8, &custody.ready.capture.content_digest, &boundary.capture_digest))
        return error.InvalidReplicationSnapshotCursor;
    var retained = try codec.RetainedSnapshot.open(a, store, custody.purpose, custody.ready, boundary.page_chain, progress.snapshot_offset, cancellation);
    errdefer retained.deinit();
    if (!std.mem.eql(u8, &try retained.snapshot.currentCommitment(), &progress.snapshot_next_page))
        return error.InvalidReplicationSnapshotCursor;
    return retained;
}

pub const Capture = struct {
    allocator: A,
    store: *artifacts.ArtifactStore,
    held: purpose.Purpose,
    source: foreign.SubscriptionOrigin.Identity,
    snapshot: *const foreign.SubscriptionSnapshot,
    scope: artifacts.UploadScope,
    source_point: u64,
    snapshot_digest: [32]u8,
    copy_rows: bool,
    relation_index: usize = 0,
    relation_start: u64 = 0,
    total_rows: u64 = 0,
    relations: std.ArrayList(codec.CaptureRelation) = .empty,
    pages: std.ArrayList(codec.CaptureEntry) = .empty,
    failed: bool = false,

    pub fn init(a: A, store: *artifacts.ArtifactStore, identity: foreign.SubscriptionOrigin.Identity, held: purpose.Purpose, snapshot: *const foreign.SubscriptionSnapshot, copy_rows: bool) !Capture {
        try held.validate();
        const scope = try codec.sourceScope(identity, held);
        if (!std.mem.eql(u8, &identity.provider_identity, &snapshot.origin.provider_identity) or
            !std.mem.eql(u8, &held.roster_digest, &try snapshot.origin.digest(a)) or snapshot.snapshot_token.len == 0)
            return error.ReplicationCutoverAuthorityLost;
        const point = try foreign.parseReplicationCheckpoint(snapshot.checkpoint);
        if (point == 0) return error.InvalidReplicationCheckpoint;
        var snapshot_digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(snapshot.snapshot_token, &snapshot_digest, .{});
        return .{ .allocator = a, .store = store, .held = held, .source = identity, .snapshot = snapshot, .scope = scope, .source_point = point, .snapshot_digest = snapshot_digest, .copy_rows = copy_rows };
    }

    pub fn deinit(self: *Capture) void {
        self.relations.deinit(self.allocator);
        self.pages.deinit(self.allocator);
        self.* = undefined;
    }

    fn check(raw: *const anyopaque) !void {
        const authority: *const foreign.SubscriptionAuthority = @ptrCast(@alignCast(raw));
        try authority.check();
    }

    /// One provider page or relation EOF. A failed quantum invalidates this
    /// capture because the live upstream cursor may already have advanced.
    pub fn step(self: *Capture, max_rows: usize, deadline: u64, authority: foreign.SubscriptionAuthority) !bool {
        if (self.failed or max_rows == 0 or max_rows > 4096) return error.InvalidReplicationSnapshotCursor;
        errdefer self.failed = true;
        try authority.check();
        const roster = self.snapshot.origin.relations;
        if (self.relation_index == roster.len) return true;
        const relation = roster[self.relation_index];
        if (!self.copy_rows) return self.finishRelation(relation.id);
        var result = try self.snapshot.captureRelation(self.allocator, relation.id, max_rows, deadline, authority);
        defer result.deinit(self.allocator);
        if (result.rows.len == 0) return self.finishRelation(relation.id);
        if (self.pages.items.len >= codec.max_capture_pages or result.rows.len > max_rows or result.json_null_fields.len != result.rows.len)
            return error.InvalidReplicationSnapshotCursor;

        const rows = try self.allocator.alloc(codec.Row, result.rows.len);
        defer self.allocator.free(rows);
        for (result.rows, result.json_null_fields, rows) |value, fields, *row|
            row.* = .{ .value = value, .json_null_fields = fields };
        try self.pages.ensureUnusedCapacity(self.allocator, 1);
        const next_offset = std.math.add(u64, self.total_rows, rows.len) catch return error.InvalidReplicationSnapshotCursor;
        const cancellation: Cancellation = .{ .ptr = &authority, .check_fn = check };
        const page: codec.CapturePage = .{ .relation_id = relation.id, .offset = self.total_rows, .rows = rows };
        const entry = try codec.CaptureEntry.write(self.allocator, self.store, self.scope, relation, page, cancellation);
        try authority.check();
        self.pages.appendAssumeCapacity(entry);
        self.total_rows = next_offset;
        return false;
    }

    fn finishRelation(self: *Capture, relation_id: u32) !bool {
        try self.relations.append(self.allocator, .{ .relation_id = relation_id, .first_offset = self.relation_start, .count = self.total_rows - self.relation_start });
        self.relation_start = self.total_rows;
        self.relation_index += 1;
        return self.relation_index == self.snapshot.origin.relations.len;
    }

    /// Seal a completed capture as a metadata-only admission. The first
    /// destination transaction can run after the provider session closes.
    pub fn sealBootstrap(self: *const Capture, authority: foreign.SubscriptionAuthority, lease: Lease, attempt: [16]u8, admitted_at_ns: u64, signer: admission.SnapshotSigningDriver, context: Context) !Bootstrap {
        try lease.validate();
        if (!context.setting_admin) return error.Forbidden;
        try context.ensureActive();
        const origin = lease.provider_origin orelse return error.ReplicationCutoverAuthorityLost;
        if (lease.item.phase != .initialize or !lease.item.owner.eql(self.held.owner) or lease.item.generation != self.held.birth or
            !std.mem.eql(u8, &lease.item.recipe_digest, &self.held.recipe_digest) or
            !std.mem.eql(u8, &origin, &self.held.source_digest) or !std.meta.eql(lease.provider_attempt, @as(?[16]u8, attempt)) or
            admitted_at_ns >= lease.expires_at_ns or admitted_at_ns >= self.held.upload.expires_at_unix_ns)
            return error.ReplicationCutoverAuthorityLost;
        const retained = try self.retainCompleted(authority);
        const expected = try Attestation.Snapshot.expected(self.held, retained.ready, 1, attempt, retained.page_chain);
        const proof = try signer.sign(signer.ptr, self.allocator, expected, context);
        try proof.verify(expected);
        try authority.check();
        return admission.seal(self.allocator, null, .{
            .metadata_group = self.held.metadata_group,
            .incarnation = self.held.incarnation,
            .owner = self.held.owner,
            .birth = self.held.birth,
            .generation = 1,
            .origin = self.held.source_digest,
            .txn_id = attempt,
            .recipe_digest = self.held.recipe_digest,
            .upload_token = 0,
            .begin_timestamp = admitted_at_ns / std.time.ns_per_ms,
            .admitted_at_ns = admitted_at_ns,
            .expires_at_ns = @min(lease.expires_at_ns, self.held.upload.expires_at_unix_ns),
            .event = .{
                .kind = .snapshot_begin,
                .source_point = retained.ready.source_point,
                .end_point = retained.ready.source_point,
                .changes_digest = retained.ready.capture.content_digest,
            },
            .initialization = .{ .lease = lease, .attempt = attempt, .snapshot = .{
                .purpose = self.held,
                .ready = retained.ready,
                .page_chain = retained.page_chain,
                .proof = proof,
            } },
        }, &.{}, context.cancellation);
    }

    fn retainCompleted(self: *const Capture, authority: foreign.SubscriptionAuthority) !struct { ready: purpose.Ready, page_chain: [32]u8 } {
        if (self.failed or self.relation_index != self.snapshot.origin.relations.len) return error.InvalidReplicationSnapshotCursor;
        const cancellation: Cancellation = .{ .ptr = &authority, .check_fn = check };
        try cancellation.check();
        const replay_origin: codec.ReplayOrigin = .{ .source = self.source, .observed = self.snapshot.origin };
        const bytes = try std.json.Stringify.valueAlloc(self.allocator, replay_origin, .{});
        defer self.allocator.free(bytes);
        if (bytes.len > codec.ReplayOrigin.max_bytes) return error.InvalidReplicationSnapshotCursor;
        const execution_context = try payload.retain(self.allocator, self.store, self.scope, bytes, cancellation);
        const ready = try self.finish(execution_context, authority);
        const page_chain = try codec.pageChain(self.pages.items, null);
        return .{ .ready = ready, .page_chain = page_chain };
    }

    pub fn finish(self: *const Capture, execution_context: payload.Retained, authority: foreign.SubscriptionAuthority) !purpose.Ready {
        if (self.failed or self.relation_index != self.snapshot.origin.relations.len) return error.InvalidReplicationSnapshotCursor;
        try execution_context.validate();
        if (!std.meta.eql(execution_context.scope, self.scope)) return error.ArtifactIntegrityMismatch;
        const cancellation: Cancellation = .{ .ptr = &authority, .check_fn = check };
        try cancellation.check();
        const manifest: codec.CaptureManifest = .{
            .owner = self.held.owner,
            .birth = self.held.birth,
            .provider_identity = self.snapshot.origin.provider_identity,
            .roster_digest = self.held.roster_digest,
            .source_point = self.source_point,
            .snapshot_token_digest = self.snapshot_digest,
            .total_rows = self.total_rows,
            .relations = self.relations.items,
            .pages = self.pages.items,
        };
        const bytes = try std.json.Stringify.valueAlloc(self.allocator, manifest, .{});
        defer self.allocator.free(bytes);
        const ready: purpose.Ready = .{
            .upload = self.held.upload,
            .capture = try payload.retain(self.allocator, self.store, self.scope, bytes, cancellation),
            .execution_context = execution_context,
            .source_point = self.source_point,
            .snapshot_token_digest = self.snapshot_digest,
            .total_rows = self.total_rows,
        };
        try manifest.validate(self.allocator, self.held, ready, self.snapshot.origin);
        try cancellation.check();
        return ready;
    }
};

test "subscription capture retains provider pages across bounded quanta" {
    const Provider = struct {
        offset: usize = 0,
        canceled: bool = false,
        fn close(_: *anyopaque, _: A) void {}
        fn checkLease(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.canceled) return error.Canceled;
        }
        fn observe(_: *anyopaque, _: foreign.SubscriptionOrigin.Observation) !void {}
        fn ack(_: *anyopaque, _: []const u8) !void {
            return error.TestUnexpectedResult;
        }
        fn fetch(raw: *anyopaque, a: A, id: u32, limit: usize, _: u64, cancellation: Cancellation) !foreign.QueryResult {
            try cancellation.check();
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(u32, 7), id);
            try std.testing.expectEqual(@as(usize, 1), limit);
            if (self.offset == 2) return .{};
            var row: std.json.ObjectMap = .empty;
            errdefer row.deinit(a);
            try row.put(a, try a.dupe(u8, "id"), .{ .integer = @intCast(10 + self.offset) });
            const rows = try a.alloc(std.json.Value, 1);
            errdefer a.free(rows);
            const nulls = try a.alloc([]const []const u8, 1);
            rows[0] = .{ .object = row };
            nulls[0] = &.{};
            self.offset += 1;
            return .{ .rows = rows, .json_null_fields = nulls };
        }
    };
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var provider: Provider = .{};
    const authority: foreign.SubscriptionAuthority = .{ .ptr = &provider, .check_fn = Provider.checkLease, .observe_fn = Provider.observe, .acknowledged_fn = Provider.ack };
    const snapshot: foreign.SubscriptionSnapshot = .{
        .ptr = &provider,
        .vtable = &.{ .deinit = Provider.close, .capture_relation = Provider.fetch },
        .checkpoint = "0/1",
        .snapshot_token = "provider-cut",
        .origin = .{ .provider_identity = @splat(1), .relations = &.{.{
            .id = 7,
            .namespace = "public",
            .name = "records",
            .replica_identity = .default,
            .columns = &.{.{ .name = "id", .type_oid = 20, .type_modifier = -1, .key = true }},
        }} },
    };
    const identity: foreign.SubscriptionOrigin.Identity = .{
        .owner = .{ .logical_subscription = .{ .database_id = 2, .subscription_id = 3 } },
        .origin_generation = 1,
        .provider_identity = snapshot.origin.provider_identity,
        .recipe_digest = @splat(4),
        .slot_name = "actual_slot",
        .roster_digest = try snapshot.origin.digest(a),
    };
    const incarnation = "0123456789abcdef0123456789abcdef".*;
    const held: purpose.Purpose = .{
        .metadata_group = 1,
        .incarnation = incarnation,
        .owner = identity.owner,
        .birth = 1,
        .source_digest = try @import("../metadata/replication_digest.zig").digest("antfly/source-origin/v3", .{ @as(u64, 1), incarnation, identity }, foreign.SubscriptionOrigin.max_encoded_bytes),
        .recipe_digest = identity.recipe_digest,
        .roster_digest = identity.roster_digest,
        .upload = .{ .token = 2, .nonce = @splat(5), .expires_at_unix_ns = 10 * std.time.ns_per_s },
    };
    var directory = try local.common_test_directory.TestDirectory.init("replication-live-capture");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    var capture = try Capture.init(a, &store, identity, held, &snapshot, true);
    defer capture.deinit();
    const context = try payload.retain(a, &store, capture.scope, "test execution context", .none);
    try std.testing.expectError(error.InvalidReplicationSnapshotCursor, capture.finish(context, authority));
    try std.testing.expect(!try capture.step(1, 100, authority));
    try std.testing.expect(!try capture.step(1, 200, authority));
    try std.testing.expect(try capture.step(1, 300, authority));
    const seed: [32]u8 = @splat(10);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: local.storage_db_root_signing_identity.State = .{ .root_incarnation = 1, .seed = seed, .public_key = pair.public_key.toBytes() };
    const Sign = struct {
        root: local.storage_db_root_signing_identity.State,

        fn run(raw: *anyopaque, _: A, expected: Attestation.Expected, ctx: Context) !Attestation.Proof {
            try ctx.ensureActive();
            const self: *@This() = @ptrCast(@alignCast(raw));
            return Attestation.Proof.sign(expected, .{
                .metadata_incarnation = expected.metadata_incarnation,
                .node_id = 1,
                .store_id = 1,
                .root_incarnation = self.root.root_incarnation,
                .public_key = self.root.public_key,
            }, 1, self.root);
        }
    };
    var signing: Sign = .{ .root = root };
    const lease: Lease = .{
        .item = .{
            .owner = held.owner,
            .generation = held.birth,
            .recipe_digest = held.recipe_digest,
            .operation = .{ .id = @splat(12), .body_digest = @splat(13) },
            .phase = .initialize,
        },
        .worker = @splat(2),
        .generation = 1,
        .expires_at_ns = 10 * std.time.ns_per_s,
        .provider_attempt = @splat(11),
        .provider_origin = held.source_digest,
    };
    const signer: admission.SnapshotSigningDriver = .{ .ptr = &signing, .sign = Sign.run };
    const trusted: Context = .{ .setting_admin = true };
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, capture.sealBootstrap(authority, lease, @splat(15), std.time.ns_per_s, signer, trusted));
    const bootstrap = try capture.sealBootstrap(authority, lease, @splat(11), std.time.ns_per_s, signer, trusted);
    const expires = bootstrap.input.identity.expires_at_unix_ns;
    try requireFreshBootstrap(bootstrap, expires - 1);
    try std.testing.expectError(error.ExternalTransactionAdmissionExpired, requireFreshBootstrap(bootstrap, expires));
    try std.testing.expectEqual(.snapshot_begin, bootstrap.event.kind);
    try std.testing.expectEqual(@as(usize, 0), bootstrap.input.participants.len);
    var epoch: [16]u8 = undefined;
    _ = try std.fmt.hexToBytes(&epoch, &incarnation);
    var mutation: registry.Mutation = .{
        .incarnation = epoch,
        .admission = .{ .nonce = @splat(18), .head = 0 },
        .database_id = held.owner.logical_subscription.database_id,
        .actor = .{ .session_user = .bootstrap, .credential = "" },
        .operation = .{ .replication_admit = bootstrap },
    };
    try mutation.validate();
    mutation.operation.replication_admit.input.identity.expires_at_unix_ns += 1;
    try std.testing.expectError(error.InvalidCatalogMutation, mutation.validate());
    const attested = bootstrap.initialization.?.snapshot;
    const ready = attested.ready;
    try attested.proof.verify(try Attestation.Snapshot.expected(held, ready, 1, @splat(11), attested.page_chain));
    var truncated = ready;
    truncated.total_rows -= 1;
    try std.testing.expectError(error.InvalidReplicationPayloadAdmission, attested.proof.verify(try Attestation.Snapshot.expected(held, truncated, 1, @splat(11), attested.page_chain)));
    try std.testing.expectEqual(@as(u64, 2), ready.total_rows);
    var replay = try codec.Snapshot.open(a, &store, identity, held, ready, snapshot.origin, .none);
    defer replay.deinit();
    try std.testing.expectEqual(attested.page_chain, replay.page_chain);
    const rows = @import("replication_subscription_rows.zig");
    const catalog = @import("table_catalog.zig");
    const Range = @import("../metadata/table_manager.zig").RangeRecord;
    const range: Range = .{ .group_id = 7, .range_id = 8, .table_id = 9, .start_key = "" };
    const routed: catalog.AuthoritativeTableRoutingSnapshot = .{
        .snapshot = .{ .source = undefined, .value = .{ .metadata_group_id = 1, .metadata_incarnation = incarnation, .catalog_revision = 5, .tables = &.{}, .ranges = &.{} } },
        .table_id = 9,
        .ranges = &.{&range},
        .topology_epoch = 1,
    };
    var plan_arena = std.heap.ArenaAllocator.init(a);
    defer plan_arena.deinit();
    const plan_alloc = plan_arena.allocator();
    var previous_key: ?[]const u8 = null;
    var next = attested.page_chain;
    for ([_][]const u8{ "10", "11" }) |expected| {
        const item = (try replay.nextWithProof(a, .none)).?;
        defer item.page.deinit();
        try std.testing.expectEqual(next, try item.proof.commitment());
        const planned = try rows.planPage(plan_alloc, &routed, "records", 2, held.source_digest, ready.source_point, next, snapshot.origin.relations[0], item.page.value, item.proof);
        try std.testing.expectEqual(@as(usize, 1), planned.groups.len);
        const group = planned.groups[0];
        try std.testing.expectEqual(@as(u64, 7), group.participant.route.route.group_id);
        try std.testing.expectEqual(.insert, group.intent.predicates[0].row_policy_requirement);
        try std.testing.expectEqualStrings(group.intent.writes[0].key, group.intent.predicates[0].key);
        // WAL decodes bigint as an integer. Retained JSON uses number_string.
        var wal_key: std.json.ObjectMap = .empty;
        try wal_key.put(plan_alloc, "id", .{ .integer = try std.fmt.parseInt(i64, expected, 10) });
        const wal_address = (try rows.replicaKey(plan_alloc, held.source_digest, snapshot.origin.relations[0], .{
            .value = .{ .object = wal_key },
            .json_null_fields = &.{},
        })).?;
        try std.testing.expectEqualStrings(group.intent.writes[0].key, wal_address);
        if (previous_key) |key| try std.testing.expect(!std.mem.eql(u8, key, group.intent.writes[0].key));
        previous_key = group.intent.writes[0].key;
        var changed = item.page.value;
        changed.version += 1;
        try std.testing.expectError(error.InvalidReplicationSnapshotCursor, rows.planPage(plan_alloc, &routed, "records", 2, held.source_digest, ready.source_point, next, snapshot.origin.relations[0], changed, item.proof));
        const sealed = try @import("replication_subscription_admission.zig").sealSnapshotPage(plan_alloc, &store, .{
            .metadata_group = held.metadata_group,
            .incarnation = incarnation,
            .owner = held.owner,
            .birth = held.birth,
            .generation = item.proof.offset + 1,
            .origin = held.source_digest,
            .txn_id = @splat(@as(u8, @intCast(11 + item.proof.offset))),
            .recipe_digest = held.recipe_digest,
            .upload_token = 9,
            .begin_timestamp = 10,
            .admitted_at_ns = 20,
            .expires_at_ns = 100,
            .event = planned.event,
        }, .{
            .routing = &routed,
            .destination = .{ .table_id = 9, .storage_name = "records", .schema_version = 2, .schema_digest = @splat(12) },
            .expected_page = next,
            .relation = snapshot.origin.relations[0],
            .page = item.page.value,
            .proof = item.proof,
        }, &.{null}, .{ .identity = attested.proof.identity, .reporter = 1, .root = root }, .none);
        try std.testing.expectEqualDeep(item.proof, sealed.event.snapshot_page.?);
        try std.testing.expect(sealed.intents != null and sealed.permits.len == 1);
        const page_expected = try @import("../metadata/replication_initialization_attestation.zig").expectedPage(held.metadata_group, incarnation, sealed.input.identity.replication_source.?, sealed.input.identity.plan_digest);
        try sealed.page_proof.?.verify(page_expected);
        var substituted_plan = page_expected;
        substituted_plan.retained_digest[0] ^= 1;
        try std.testing.expectError(error.InvalidReplicationPayloadAdmission, sealed.page_proof.?.verify(substituted_plan));
        next = item.proof.next;
        try std.testing.expectEqualStrings(expected, item.page.value.rows[0].value.object.get("id").?.number_string);
    }
    try std.testing.expect(std.mem.allEqual(u8, &next, 0));
    try std.testing.expect((try replay.nextWithProof(a, .none)) == null);
    provider.canceled = true;
    try std.testing.expectError(error.Canceled, capture.finish(context, authority));
    const progress: registry.ReplicationProgress = .{
        .birth = held.birth,
        .origin = held.source_digest,
        .snapshot_cut = ready.source_point,
        .snapshot_offset = 1,
        .snapshot_next_page = replay.links[0],
        .snapshot_boundary = .{
            .source_point = ready.source_point,
            .total_rows = ready.total_rows,
            .capture_digest = ready.capture.content_digest,
            .page_chain = attested.page_chain,
            .custody = .{ .purpose = held, .ready = ready },
        },
    };
    const DocStore = local.storage_docstore.DocStore;
    const journal_path = try std.fmt.allocPrintSentinel(a, "{s}/bootstrap-journal", .{directory.path()}, 0);
    const boundary = progress.snapshot_boundary.?;
    const reference = bootstrap.input.identity.replication_source.?;
    const decision_owner = try bootstrap.input.identity.owner(a);
    {
        var journal = try DocStore.open(a, journal_path, .{});
        defer journal.close();
        var txn = try journal.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, registry.planReplicationAdmission(a, &txn, held.metadata_group, 1, bootstrap.input, bootstrap.event, &.{}, null, null));
        const plan = try registry.planReplicationAdmission(a, &txn, held.metadata_group, 1, bootstrap.input, bootstrap.event, &.{}, null, boundary);
        try plan.apply(&txn);
        try txn.commit();
    }
    {
        var journal = try DocStore.open(a, journal_path, .{});
        defer journal.close();
        var txn = try journal.beginWriteTxn();
        defer txn.abort();
        const state = (try registry.readReplicationRecovery(a, &txn, held.metadata_group, held.owner, held.birth, held.source_digest)).?;
        try std.testing.expectEqualDeep(boundary, state.progress.snapshot_boundary.?);
        try std.testing.expectEqual(@as(u64, 0), state.progress.snapshot_offset);
        try std.testing.expectEqual(@as(u64, 0), state.progress.checkpoint);
        try std.testing.expect(state.progress.initialCompletion() == null);
        try std.testing.expectError(error.InvalidReplicationSnapshotCursor, reopenRetainedSnapshot(a, &store, state.progress, .none));
        const prepared = try registry.planReplicationAdvance(a, &txn, held.metadata_group, 1, reference, decision_owner, state.transaction.?.revision, .prepare);
        try prepared.plan.apply(&txn);
        const committed = try registry.planReplicationAdvance(a, &txn, held.metadata_group, 1, reference, decision_owner, prepared.record.revision, .{ .commit = .{
            .version = try local.api_local_transaction_contract.commitVersion(bootstrap.input.identity.begin_timestamp),
        } });
        try committed.plan.apply(&txn);
        const Verifier = struct {
            pub fn requireAcknowledgedRoot(_: @This(), _: anytype, _: anytype, _: anytype) !void {
                return error.TestUnexpectedResult;
            }
        };
        const checkpoint = try registry.planReplicationCheckpoint(a, &txn, held.metadata_group, 1, reference, decision_owner, committed.record.revision, &.{}, Verifier{});
        try checkpoint.apply(&txn);
        const resumed = (try registry.readReplicationRecovery(a, &txn, held.metadata_group, held.owner, held.birth, held.source_digest)).?;
        try std.testing.expectEqual(@as(u64, 0), resumed.progress.checkpoint);
        try std.testing.expect(resumed.transaction == null);
        var beginning = try reopenRetainedSnapshot(a, &store, resumed.progress, .none);
        defer beginning.deinit();
        const first = (try beginning.snapshot.nextWithProof(a, .none)).?;
        defer first.page.deinit();
        try std.testing.expectEqualStrings("10", first.page.value.rows[0].value.object.get("id").?.number_string);
    }
    const record = try std.json.Stringify.valueAlloc(a, progress, .{});
    const ReadCut = struct {
        value: []const u8,

        pub fn get(self: @This(), key: []const u8) ![]const u8 {
            if (std.mem.indexOf(u8, key, ":replication_progress:") == null) return error.NotFound;
            return self.value;
        }
    };
    const cut: ReadCut = .{ .value = record };
    const recovered = (try registry.readReplicationRecovery(a, cut, held.metadata_group, held.owner, held.birth, held.source_digest)).?.progress;
    try std.testing.expectError(error.InvalidCatalogRecord, registry.readReplicationRecovery(a, cut, held.metadata_group + 1, held.owner, held.birth, held.source_digest));
    var retained = try reopenRetainedSnapshot(a, &store, recovered, .none);
    defer retained.deinit();
    const remaining = (try retained.snapshot.nextWithProof(a, .none)).?;
    defer remaining.page.deinit();
    try std.testing.expectEqualStrings("11", remaining.page.value.rows[0].value.object.get("id").?.number_string);
    try std.testing.expectEqual(recovered.snapshot_next_page, try remaining.proof.commitment());
    try std.testing.expect((try retained.snapshot.nextWithProof(a, .none)) == null);
    var wrong_cursor = recovered;
    wrong_cursor.snapshot_next_page[0] ^= 1;
    try std.testing.expectError(error.InvalidReplicationSnapshotCursor, reopenRetainedSnapshot(a, &store, wrong_cursor, .none));
    try std.testing.expect(recovered.initialCompletion() == null);
    var terminal = recovered;
    terminal.snapshot_offset = ready.total_rows;
    try std.testing.expect(terminal.initialCompletion() == null);
    terminal.snapshot_next_page = remaining.proof.next;
    const finish = terminal.initialCompletion().?;
    try std.testing.expectEqual(.initial_copy, finish.kind);
    try std.testing.expectEqual(ready.source_point, finish.end_point);
    terminal.checkpoint = finish.end_point;
    try std.testing.expect(terminal.initialCompletion() == null);
}
test "subscription capture starts only the committed provider attempt when a reply is lost" {
    const Mock = struct {
        lease: ?Lease = null,
        lost: ?Slots.ControlRequest = null,

        fn call(ptr: *anyopaque, a: A, context: Context, input: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(context.setting_admin);
            switch (input) {
                .sql_publication_capture_work => switch (input.sql_publication_capture_work.slot.?) {
                    .head => return a.dupe(u8, "1"),
                    .outcome => |request| {
                        try std.testing.expectEqualDeep(self.lost.?, request);
                        return std.json.Stringify.valueAlloc(a, Slots.ControlResult{ .subscription_lease = self.lease.? }, .{});
                    },
                    .subscription_permission => |request| return a.dupe(u8, if (std.meta.eql(request.origin, self.lease.?.provider_origin) and std.mem.eql(u8, &request.attempt, &self.lease.?.provider_attempt.?)) "true" else "false"),
                    .subscription_capture_definition => |request| {
                        try std.testing.expectEqual(self.lease.?.provider_attempt, request.attempt);
                        return a.dupe(u8, "{\"connection\":\"host=private.example\",\"publications\":[\"pub\"],\"options\":{\"slot_name\":{\"name\":\"sub_slot\"}}}");
                    },
                    else => return error.TestUnexpectedResult,
                },
                .sql_publication_slot_control => |request| switch (request.action) {
                    .claim_subscription => |claim| {
                        self.lease = .{ .item = claim.item, .worker = claim.worker, .generation = 1, .expires_at_ns = claim.expires_at_unix_ns };
                        return std.json.Stringify.valueAlloc(a, Slots.ControlResult{ .subscription_lease = self.lease.? }, .{});
                    },
                    .bind_subscription_export => |bind| {
                        self.lease.?.provider_origin = bind.origin;
                        self.lease.?.provider_export = bind.receipt;
                        return std.json.Stringify.valueAlloc(a, Slots.ControlResult{ .subscription_lease = self.lease.? }, .{});
                    },
                    .start_subscription => |start| {
                        try std.testing.expect(self.lost == null);
                        self.lease.?.provider_attempt = start.attempt;
                        self.lease.?.provider_preflight_identity = start.provider_preflight_identity;
                        self.lost = request;
                        return error.MetadataMutationOutcomeUnknown;
                    },
                    else => return error.TestUnexpectedResult,
                },
                else => return error.TestUnexpectedResult,
            }
        }
    };
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var mock: Mock = .{};
    var client: CaptureControl = .{
        .source = .{ .ptr = &mock, .vtable = &.{ .status = undefined, .system_catalog = Mock.call } },
        .io = std.testing.io,
        .realm = .{ .group_id = 1, .incarnation = "0123456789abcdef0123456789abcdef".*, .database_id = 2 },
        .context = .{},
    };
    const item: Work.Item = .{
        .owner = .{ .logical_subscription = .{ .database_id = 2, .subscription_id = 3 } },
        .generation = 1,
        .recipe_digest = @splat(4),
        .operation = .{ .id = @splat(5), .body_digest = @splat(6) },
        .phase = .initialize,
    };
    try std.testing.expectError(error.Forbidden, client.claim(a, item, @splat(7)));
    client.context.setting_admin = true;
    const claimed = try client.claim(a, item, @splat(7));
    _ = try client.definition(a, claimed);
    try std.testing.expect(mock.lease.?.provider_attempt == null and mock.lost == null);
    const lease = try client.start(a, claimed, @splat(8));
    try std.testing.expectEqualDeep(mock.lease.?, lease);
    try std.testing.expectEqual(lease.provider_attempt.?, mock.lost.?.action.start_subscription.attempt);
    const definition = try client.definition(a, lease);
    try std.testing.expectEqualStrings("pub", definition.publications[0]);
    var binding: CaptureAuthority = .{ .allocator = a, .control = client, .lease = lease, .slot_name = definition.options.slot_name.name };
    const provider = binding.provider();
    try provider.observe(.{ .provider_identity = @splat(8), .relations = &.{} });
    try std.testing.expectEqualStrings("sub_slot", binding.identity.?.slot_name);
    try std.testing.expect(binding.lease.provider_origin == null);
    try provider.exported(@splat(8), 100);
    try std.testing.expect(binding.lease.provider_origin != null);
    try std.testing.expectEqual(@as(u64, 100), binding.lease.provider_export.?.checkpoint);
    try std.testing.expectError(error.ForeignProviderIdentityMismatch, provider.observe(.{ .provider_identity = @splat(9), .relations = &.{} }));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, provider.acknowledged("0/1"));
}
