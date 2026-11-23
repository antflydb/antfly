// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const local = @import("antfly_local_sources");
const registry = @import("../system_catalog/prepared_transactions.zig");
const installation = @import("../system_catalog/replication/installation.zig");
const receipts = @import("replication_source_apply_attestation.zig");
const forwarding = @import("internal_batch_forwarding.zig");
const time = @import("antfly_platform").time;
const A = std.mem.Allocator;
const Server = @import("http_server.zig").ApiHttpServer;
const Context = local.api_operation.RequestContext;
const admission = @import("replication_subscription_admission.zig");
const Query = @FieldType(installation.SlotRead, "subscription_apply");
const Command = @FieldType(registry.Mutation, "operation");
const Batch = local.storage_db_types.BatchRequest;

pub const Result = enum { idle, waiting, advanced };

const ArtifactStore = @import("../serverless/artifacts/store.zig").ArtifactStore;

fn requireSharedArtifacts(configured: ?ArtifactStore) !ArtifactStore {
    const store = configured orelse return error.ArtifactUnavailable;
    const identity = store.namespace_identity orelse return error.ArtifactUnavailable;
    if (!store.shared_across_owners or std.mem.allEqual(u8, &identity, 0)) return error.ArtifactUnavailable;
    return store;
}

test "subscription receipt RPC requires durable shared artifact custody before provider work" {
    var store: ArtifactStore = .{ .allocator = std.testing.allocator, .ptr = undefined, .vtable = undefined };
    try std.testing.expectError(error.ArtifactUnavailable, requireSharedArtifacts(null));
    try std.testing.expectError(error.ArtifactUnavailable, requireSharedArtifacts(store));
    store.namespace_identity = @splat(1);
    try std.testing.expectError(error.ArtifactUnavailable, requireSharedArtifacts(store));
    store.shared_across_owners = true;
    try std.testing.expectEqual(store.namespace_identity, (try requireSharedArtifacts(store)).namespace_identity);
    store.namespace_identity = @splat(0);
    try std.testing.expectError(error.ArtifactUnavailable, requireSharedArtifacts(store));
}

/// Admit one retained page, or the cutover after every page is acknowledged.
/// The writer rechecks the cursor and recipe in the admission transaction.
pub fn stepInitialSnapshot(server: *Server, allocator: A, after: *?u64, context: Context) !void {
    if (!context.setting_admin) return error.Forbidden;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var bounded = try context.platformDeadline();
    const deadline = time.monotonicNs() +| 5 * std.time.ns_per_s;
    bounded.deadline_ns = @min(bounded.deadline_ns orelse deadline, deadline);
    if (server.subscription_capture) |active| {
        const signer = server.subscriptionSnapshotSigner() orelse {
            active.deinit();
            server.subscription_capture = null;
            return;
        };
        const command = active.step(bounded, 128, signer) catch |err| {
            active.deinit();
            server.subscription_capture = null;
            return err;
        } orelse return;
        submitAdmission(server, a, command, bounded) catch |err| switch (err) {
            error.MetadataMutationOutcomeUnknown, error.CatalogGenerationChanged, error.GroupLeaderUnavailable, error.DeadlineExceeded => return err,
            else => {
                active.deinit();
                server.subscription_capture = null;
                return err;
            },
        };
        active.deinit();
        server.subscription_capture = null;
        return;
    }
    const bytes = try server.source.systemCatalog(a, bounded, .{ .sql_publication_capture_work = .{ .subscription = .{ .after = after.* } } });
    const Page = @import("../metadata/replication_subscription_work.zig").Page;
    const page = try std.json.parseFromSliceLeaky(Page, a, bytes, .{});
    after.* = page.next_after;
    if (page.disable) |work| {
        const owner = work.item.owner;
        if (owner != .logical_subscription) return error.InvalidSqlBackendResponse;
        after.* = owner.logical_subscription.subscription_id;
        if (work.cleanup) return cleanupSubscription(server, a, work.group, page.incarnation, work.item, true, bounded);
        const control: @import("replication_subscription_capture.zig").CaptureControl = .{
            .source = server.source,
            .io = server.embedding_provider_runtime.io,
            .realm = .{ .group_id = work.group, .incarnation = page.incarnation, .database_id = owner.logical_subscription.database_id },
            .context = bounded,
        };
        return control.finishDisable(a, work.item, null);
    }
    if (page.drop) |work| {
        if (work.item.owner != .logical_subscription) return error.InvalidSqlBackendResponse;
        after.* = work.item.owner.logical_subscription.subscription_id;
        return cleanupSubscription(server, a, work.group, page.incarnation, work.item, false, bounded);
    }
    const candidate = page.snapshot orelse {
        const work = page.capture orelse return;
        if (work.item.owner != .logical_subscription or work.item.phase != .initialize)
            return error.InvalidSqlBackendResponse;
        const owner = work.item.owner.logical_subscription;
        after.* = owner.subscription_id;
        if (server.subscriptionSnapshotSigner() == null) return;
        const store = try requireSharedArtifacts(server.cfg.document_artifact_store);
        const io = server.embedding_provider_runtime.io;
        if (std.mem.allEqual(u8, &server.subscription_capture_worker, 0))
            try io.randomSecure(&server.subscription_capture_worker);
        const capture = @import("replication_subscription_capture.zig");
        server.subscription_capture = try capture.ProviderCapture.open(allocator, try server.ensureForeignRegistry(), store, .{
            .source = server.source,
            .io = io,
            .realm = .{ .group_id = work.group, .incarnation = page.incarnation, .database_id = owner.database_id },
            .context = bounded,
            .auth_enabled = server.cfg.auth_enabled,
            .user_manager = server.cfg.user_manager,
        }, work.item, server.subscription_capture_worker);
        return;
    };
    const owner = candidate.item.owner;
    if (owner != .logical_subscription or candidate.item.phase != .initialize)
        return error.InvalidSqlBackendResponse;
    after.* = owner.logical_subscription.subscription_id;
    const io = server.embedding_provider_runtime.io;
    var transaction: [16]u8 = undefined;
    try io.randomSecure(&transaction);
    const now = time.realtimeNs();
    var attempt: admission.Attempt = .{
        .metadata_group = candidate.group,
        .incarnation = page.incarnation,
        .owner = owner,
        .birth = candidate.source.birth,
        .generation = std.math.add(u64, candidate.generation, 1) catch return error.CatalogGenerationExhausted,
        .origin = candidate.source.origin_digest,
        .txn_id = transaction,
        .recipe_digest = candidate.item.recipe_digest,
        .upload_token = 0,
        .begin_timestamp = now / std.time.ns_per_ms,
        .admitted_at_ns = now,
        .expires_at_ns = now +| 30 * std.time.ns_per_s,
        .event = candidate.event,
    };
    const command = if (candidate.event != null)
        try admission.seal(a, null, attempt, &.{}, bounded.cancellation)
    else blk: {
        const signer = server.subscriptionSnapshotSigner() orelse return;
        attempt.upload_token = attempt.generation;
        break :blk (try makePageCommand(server, a, attempt, signer, bounded)) orelse return;
    };
    try submitAdmission(server, a, command, bounded);
    std.log.debug("subscription snapshot admitted event={s}", .{@tagName(command.event.kind)});
}

const CleanupAuthority = struct {
    control: @import("replication_subscription_capture.zig").CaptureControl,
    allocator: A,
    item: @import("../metadata/replication_subscription_work.zig").Item,
    checkpoint: u64,
    disabling: bool,

    fn check(raw: *anyopaque) !void {
        const self: *CleanupAuthority = @ptrCast(@alignCast(raw));
        var scratch: std.heap.ArenaAllocator = .init(self.allocator);
        defer scratch.deinit();
        const current = try self.control.cleanupDefinition(scratch.allocator(), self.item, self.disabling);
        if (current.checkpoint != self.checkpoint) return error.ReplicationCutoverAuthorityLost;
        const Credential = @import("replication_subscription_capture.zig").Credential;
        try (Credential{ .auth_enabled = self.control.auth_enabled, .manager = self.control.user_manager, .actor = current.actor }).require("", 0);
    }

    fn observe(_: *anyopaque, _: foreign.SubscriptionOrigin.Observation) !void {
        return error.ReplicationCutoverAuthorityLost;
    }

    fn acknowledged(_: *anyopaque, _: []const u8) !void {
        return error.ReplicationCutoverAuthorityLost;
    }
};

fn cleanupSubscription(server: *Server, a: A, group: u64, incarnation: [32]u8, item: @import("../metadata/replication_subscription_work.zig").Item, disabling: bool, context: Context) !void {
    const identity = item.owner.logical_subscription;
    const control: @import("replication_subscription_capture.zig").CaptureControl = .{
        .source = server.source,
        .io = server.embedding_provider_runtime.io,
        .realm = .{ .group_id = group, .incarnation = incarnation, .database_id = identity.database_id },
        .context = context,
        .auth_enabled = server.cfg.auth_enabled,
        .user_manager = server.cfg.user_manager,
    };
    const definition = try control.cleanupDefinition(a, item, disabling);
    const provider_cut: struct { identity: [32]u8, snapshot_checkpoint: u64 } = switch (definition.source) {
        .exported => |receipt| .{ .identity = receipt.provider_identity, .snapshot_checkpoint = receipt.checkpoint },
        .unexported => |provider| .{ .identity = provider, .snapshot_checkpoint = 0 },
        .committed => |boundary| blk: {
            const custody = boundary.custody orelse return error.InvalidSqlBackendResponse;
            var store = try requireSharedArtifacts(server.cfg.document_artifact_store);
            const codec = @import("../system_catalog/replication/snapshot_codec.zig");
            const retained = try codec.readReplayOrigin(a, &store, custody.purpose, custody.ready, context.cancellation);
            defer retained.deinit();
            if (!std.mem.eql(u8, definition.slot_name, retained.value.source.slot_name)) return error.ForeignProviderIdentityMismatch;
            break :blk .{ .identity = retained.value.observed.provider_identity, .snapshot_checkpoint = boundary.source_point };
        },
    };
    var authority: CleanupAuthority = .{ .control = control, .allocator = a, .item = item, .checkpoint = definition.checkpoint, .disabling = disabling };
    const providers = try server.ensureForeignRegistry();
    var provider = try providers.create(a, .{ .kind = .postgres, .dsn = @constCast(definition.connection) });
    defer provider.deinit(a);
    try provider.cleanupReplication(a, .{
        .slot_name = @constCast(definition.slot_name),
        .native_source = .{
            .provider_identity = provider_cut.identity,
            .snapshot_checkpoint = provider_cut.snapshot_checkpoint,
            .require_absent = definition.source == .unexported,
            .durable_checkpoint = definition.checkpoint,
            .lock_name = try std.fmt.allocPrint(a, "antfly/subscription/{d}/{d}/{d}", .{ group, identity.database_id, identity.subscription_id }),
            .authority = .{ .ptr = &authority, .check_fn = CleanupAuthority.check, .observe_fn = CleanupAuthority.observe, .acknowledged_fn = CleanupAuthority.acknowledged },
        },
        .execution_deadline_ns = context.deadline_ns,
        .password_required = (@import("replication_subscription_capture.zig").Credential{
            .auth_enabled = control.auth_enabled,
            .manager = control.user_manager,
            .actor = definition.actor,
        }).requiresProviderPassword(),
    });
    if (disabling) return control.finishDisable(a, item, definition.checkpoint);
    try control.finishDrop(a, item, definition.checkpoint);
}

fn submitAdmission(server: *Server, a: A, command: @FieldType(Command, "replication_admit"), bounded: Context) !void {
    const identity = command.input.identity;
    const transaction = identity.operation_id;
    const io = server.embedding_provider_runtime.io;
    const snapshot_bytes = try server.source.systemCatalog(a, bounded, .{ .sql_prepared_read = .{ .operation = transaction } });
    const snapshot = try std.json.parseFromSliceLeaky(registry.Snapshot, a, snapshot_bytes, .{});
    if (snapshot.standalone or !std.mem.eql(u8, &identity.metadata_incarnation, &std.fmt.bytesToHex(snapshot.incarnation, .lower)))
        return error.CatalogGenerationChanged;
    if (snapshot.records.len != 0) {
        if (snapshot.records.len != 1 or !(try snapshot.records[0].identity.owner(a)).eql(try identity.owner(a)))
            return error.CatalogGenerationChanged;
        return;
    }
    if (time.realtimeNs() >= identity.expires_at_unix_ns) return error.ExternalTransactionAdmissionExpired;
    var nonce: [16]u8 = undefined;
    try io.randomSecure(&nonce);
    const mutation: registry.Mutation = .{
        .admission = .{ .nonce = nonce, .head = snapshot.head },
        .incarnation = snapshot.incarnation,
        .actor = .{ .session_user = .bootstrap, .credential = "" },
        .database_id = identity.database_id,
        .operation = .{ .replication_admit = command },
    };
    const response = try server.source.systemCatalog(a, bounded, .{ .sql_prepared_mutate = mutation });
    const outcome = (try std.json.parseFromSliceLeaky(?registry.outcomes.Outcome, a, response, .{})) orelse return error.MetadataMutationOutcomeUnknown;
    _ = try outcome.result();
}

fn readRecovery(server: *Server, a: A, context: Context, query: Query) !?registry.ReplicationRecovery {
    const bytes = try server.source.systemCatalog(a, context, .{ .sql_publication_capture_work = .{ .slot = .{ .subscription_apply = query } } });
    return std.json.parseFromSliceLeaky(?registry.ReplicationRecovery, a, bytes, .{});
}

fn requireDestinationCredential(server: *Server, binding: registry.ReplicationRecovery, rights: u16) !void {
    const selected = binding.destination_credential orelse return error.Forbidden;
    const credential: @import("replication_subscription_capture.zig").Credential = .{
        .auth_enabled = server.cfg.auth_enabled,
        .manager = server.cfg.user_manager,
        .actor = selected.actor,
    };
    try credential.require(selected.resource, rights);
}

fn replayStillCurrent(progress: registry.ReplicationProgress, attempt: admission.Attempt) bool {
    return progress.pending == null and progress.checkpoint == 0 and
        progress.birth == attempt.birth and progress.generation == attempt.generation - 1 and
        std.mem.eql(u8, &progress.origin, &attempt.origin);
}

/// Plan physical constraint effects before sealing immutable participant
/// payloads. All allocations belong to the coordinator attempt arena.
fn coordinateRelationalEffects(server: *Server, a: A, members: []const wal_rows.WalMember, context: Context) ![]const wal_rows.WalMember {
    if (members.len == 0) return members;
    const txn = @import("distributed_txn.zig");
    const integrity = local.api_relational_integrity_commit;
    const Grouped = struct {
        member: wal_rows.WalMember,
        writes: std.ArrayList(local.storage_db_types.TransactionWrite) = .empty,
        deletes: std.ArrayList([]const u8) = .empty,
        predicates: std.ArrayList(local.storage_db_types.TransactionVersionPredicate) = .empty,
    };
    var grouped: std.AutoArrayHashMapUnmanaged(u64, Grouped) = .empty;
    const cut = members[0].group.participant.route;
    const reader = server.table_reads orelse return error.IntegrityCatalogUnavailable;
    for (members) |member| {
        const part = member.group.participant;
        if (!sameRoutingCut(cut, part.route)) return error.CatalogGenerationChanged;
        const entry = try grouped.getOrPut(a, part.route.table_id);
        if (!entry.found_existing) entry.value_ptr.* = .{ .member = member };
        try entry.value_ptr.writes.appendSlice(a, member.group.intent.writes);
        try entry.value_ptr.deletes.appendSlice(a, member.group.intent.deletes);
        try entry.value_ptr.predicates.appendSlice(a, member.group.intent.predicates);
    }
    const catalog = server.catalogSource();
    var observed = try server.authoritativeIntegritySnapshot(context);
    defer server.source.freeAdminSnapshot(&observed);
    if (observed.status.metadata_group_id != cut.metadata_group_id or
        !std.meta.eql(observed.status.metadata_incarnation, cut.metadata_incarnation)) return error.CatalogGenerationChanged;
    const requests = try a.alloc(txn.TableCommitRequest, grouped.count());
    for (requests, grouped.values()) |*request, entry| {
        const part = entry.member.group.participant;
        request.* = .{
            .table_name = part.table_name,
            .schema_version = part.schema_version,
            .relational_schema_version = part.schema_version,
            .writes = entry.writes.items,
            .deletes = entry.deletes.items,
            .predicates = entry.predicates.items,
        };
    }
    if (!try integrity.metadataRequiresCoordination(a, observed.tables, requests)) return members;
    const planned = try integrity.prepareWithCoverageControlled(a, reader, observed.tables, observed.ranges, requests, context);
    const routed = try txn.ExternalPreparation.init(a, catalog, planned.tables);
    const result = try a.alloc(wal_rows.WalMember, routed.manifest.len);
    for (result, routed.manifest, 0..) |*out, part, index| {
        const original = (grouped.getPtr(part.route.table_id) orelse return error.UnsupportedSqlExecution).member;
        if (part.route.route.execution != .raft or !sameRoutingCut(cut, part.route) or
            part.schema_version != original.group.participant.schema_version or
            !std.mem.eql(u8, part.table_name, original.group.participant.table_name)) return error.CatalogGenerationChanged;
        out.* = .{ .group = .{ .participant = part, .intent = routed.intentAt(index) }, .schema_digest = original.schema_digest, .policy_epoch = original.policy_epoch };
    }
    return result;
}

fn sameRoutingCut(left: @FieldType(registry.Participant, "route"), right: @TypeOf(left)) bool {
    return left.catalog_revision == right.catalog_revision and left.metadata_group_id == right.metadata_group_id and
        std.meta.eql(left.metadata_incarnation, right.metadata_incarnation);
}

fn makePageCommand(server: *Server, a: A, input: admission.Attempt, signer: admission.SnapshotSigningDriver, context: Context) !?@FieldType(Command, "replication_admit") {
    var store = try requireSharedArtifacts(server.cfg.document_artifact_store);
    const owner = input.owner.logical_subscription;
    var query: Query = .{
        .realm = .{ .group_id = input.metadata_group, .incarnation = input.incarnation, .database_id = owner.database_id },
        .subscription_id = owner.subscription_id,
        .birth = input.birth,
        .origin = input.origin,
    };
    const recovery = (try readRecovery(server, a, context, query)) orelse return null;
    if (!replayStillCurrent(recovery.progress, input)) return null;
    var retained = try @import("replication_subscription_capture.zig").reopenRetainedSnapshot(a, &store, recovery.progress, context.cancellation);
    defer retained.deinit();
    const item = (try retained.snapshot.nextWithProof(a, context.cancellation)) orelse return null;
    defer item.page.deinit();
    const relation = retained.context.value.observed.find(item.page.value.relation_id) orelse return error.InvalidReplicationSnapshotCursor;
    const insert = local.system_catalog_privileges.Access.bit(.insert);
    query.destination = .{ .namespace = relation.namespace, .relation = relation.name, .rights = insert };
    const resolved = (try readRecovery(server, a, context, query)) orelse return null;
    if (!replayStillCurrent(resolved.progress, input)) return null;
    const destination = resolved.destination orelse return error.InvalidSqlBackendResponse;
    try requireDestinationCredential(server, resolved, insert);
    const routing_catalog = @import("table_catalog.zig");
    var routing = (try routing_catalog.authoritativeTableRoutingSnapshot(a, server.catalogSource(), destination.storage_name, context.deadline_ns)) orelse return error.CatalogGenerationChanged;
    defer routing.deinit(a);
    var attempt = input;
    attempt.event = .{
        .kind = .snapshot_page,
        .source_point = recovery.progress.snapshot_cut,
        .end_point = recovery.progress.snapshot_cut,
        .changes_digest = item.proof.content_digest,
    };
    var plan = try admission.planSnapshotPage(a, attempt, .{
        .routing = &routing,
        .destination = destination,
        .expected_page = recovery.progress.snapshot_next_page,
        .relation = relation,
        .page = item.page.value,
        .proof = item.proof,
    });
    const raw = try a.alloc(wal_rows.WalMember, plan.groups.len);
    for (raw, plan.groups) |*member, part|
        member.* = .{ .group = part, .schema_digest = destination.schema_digest, .policy_epoch = destination.policy_epoch };
    const coordinated = try coordinateRelationalEffects(server, a, raw, context);
    const groups = try a.alloc(wal_rows.Group, coordinated.len);
    for (groups, coordinated) |*out, member| out.* = member.group;
    plan.groups = groups;
    const namespaces = try a.alloc(local.storage_db_doc_identity_namespace.Namespace, plan.groups.len);
    for (namespaces, plan.groups) |*namespace, part| {
        const route = part.participant.route.route.identity_namespace;
        namespace.* = .{ .table_id = route.table_id, .shard_id = route.shard_id, .range_id = route.range_id };
    }
    query.targets = namespaces;
    const current = (try readRecovery(server, a, context, query)) orelse return null;
    if (!replayStillCurrent(current.progress, input)) return null;
    try requireDestinationCredential(server, current, insert);
    const checked = current.destination orelse return error.InvalidSqlBackendResponse;
    if (checked.table_id != destination.table_id or checked.schema_version != destination.schema_version or
        !std.meta.eql(checked.policy_epoch, destination.policy_epoch) or
        !std.mem.eql(u8, checked.storage_name, destination.storage_name) or
        !std.mem.eql(u8, &checked.schema_digest, &destination.schema_digest))
        return error.CatalogGenerationChanged;
    var command = try admission.sealPlannedSnapshotPage(a, &store, attempt, plan, destination, current.targets, context.cancellation);
    const attestation = @import("../metadata/replication_initialization_attestation.zig");
    const expected = try attestation.expectedPage(attempt.metadata_group, attempt.incarnation, command.input.identity.replication_source.?, command.input.identity.plan_digest);
    command.page_proof = try signer.sign(signer.ptr, a, expected, context);
    try command.page_proof.?.verify(expected);
    return command;
}

/// Visit one indexed decision and advance the cursor before I/O so a slow
/// participant yields to the rest of the pending cohort.
pub fn stepPending(server: *Server, alloc: A, after: *?[16]u8, context: Context) !Result {
    if (!context.setting_admin) return error.Forbidden;
    while (true) {
        const bytes = try server.source.systemCatalog(alloc, context, .{ .sql_publication_capture_work = .{ .subscription = .{ .recovery = .{ .after = after.* } } } });
        defer alloc.free(bytes);
        const parsed = try std.json.parseFromSlice(?installation.SubscriptionApplyWork, alloc, bytes, .{});
        defer parsed.deinit();
        const item = parsed.value orelse {
            if (after.* == null) return .idle;
            // Wrap inside this pass so a single decision can advance every tick.
            after.* = null;
            continue;
        };
        try item.realm.validate();
        try item.source.validate();
        if (item.source.owner != .logical_subscription or item.source.owner.logical_subscription.database_id != item.realm.database_id)
            return error.InvalidSqlBackendResponse;
        after.* = item.source.txn_id;
        return step(server, alloc, .{
            .realm = item.realm,
            .subscription_id = item.source.owner.logical_subscription.subscription_id,
            .birth = item.source.birth,
            .origin = item.source.source_digest,
        }, context);
    }
}

/// One bounded recovery quantum for an already admitted source transaction.
/// The retained input producer owns BEGIN and PREPARE delivery. After a durable
/// decision this worker can finish the event with only metadata and owner RPCs.
pub fn step(server: *Server, allocator: A, query: Query, context: Context) !Result {
    if (!context.setting_admin) return error.Forbidden;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const context_with_deadline = try context.platformDeadline();
    var bounded = context_with_deadline;
    const deadline = time.monotonicNs() +| 5 * std.time.ns_per_s;
    bounded.deadline_ns = @min(bounded.deadline_ns orelse deadline, deadline);
    const bytes = try server.source.systemCatalog(a, bounded, .{ .sql_publication_capture_work = .{ .slot = .{ .subscription_apply = query } } });
    const recovery = (try std.json.parseFromSliceLeaky(?registry.ReplicationRecovery, a, bytes, .{})) orelse return .idle;
    const record = recovery.transaction orelse return .idle;
    try record.validate(a);
    const reference = record.identity.replication_source orelse return error.InvalidSqlBackendResponse;
    if (!reference.eql(recovery.progress.pending orelse return error.InvalidSqlBackendResponse) or
        reference.owner != .logical_subscription or reference.owner.logical_subscription.subscription_id != query.subscription_id or
        reference.owner.logical_subscription.database_id != query.realm.database_id or reference.birth != query.birth or
        !std.mem.eql(u8, &reference.source_digest, &query.origin) or
        !std.mem.eql(u8, &record.identity.metadata_incarnation, &query.realm.incarnation))
        return error.ReplicationCutoverAuthorityLost;
    var work: Work = .{ .server = server, .a = a, .context = bounded, .record = record, .reference = reference, .owner = try record.identity.owner(a) };
    const result = work.run();
    std.log.debug("subscription recovery quantum event={s} phase={s} votes={x} activated={x} resolved={x} result={s}", .{
        @tagName(record.replication_event.?.kind),
        @tagName(record.phase),
        record.votes,
        record.activations,
        record.resolutions,
        if (result) |value| @tagName(value) else |err| @errorName(err),
    });
    return result;
}

const Work = struct {
    server: *Server,
    a: A,
    context: Context,
    record: registry.Record,
    reference: registry.ReplicationReference,
    owner: registry.Owner,

    fn run(self: *Work) !Result {
        const record = self.record;
        const version = try local.api_local_transaction_contract.commitVersion(record.identity.begin_timestamp);
        return switch (record.phase) {
            .preparing => blk: {
                if (nextMissing(record.mask(), record.votes)) |member| {
                    if (time.realtimeNs() >= record.identity.expires_at_unix_ns) break :blk try self.advance(.{ .abort = version });
                    const receipt = (try self.observeStage(member, .prepared)) orelse break :blk try self.replayPrepare(member);
                    const part = record.participants[member];
                    var publication: ?@import("logical_replication_publisher_attestation.zig").OutboxReceipt = null;
                    if (part.publication_capture != null) {
                        const demand = record.publicationCapacity(member) orelse break :blk .waiting;
                        publication = (try self.server.routedPreparedPublicationReceipt(self.a, part.table_name, part.route, .outbox, demand, self.context)) orelse break :blk .waiting;
                    }
                    break :blk try self.advance(.{ .vote = .{ .receipt = receipt, .publication = publication } });
                }
                break :blk try self.advance(.prepare);
            },
            .prepared => self.advance(.{ .commit = .{
                .version = version,
                .commit_time_unix_us = if (record.publicationCaptureMask() == 0) null else time.realtimeNs() / std.time.ns_per_us,
            } }),
            .decided => if (record.decision.?.status == .committed)
                self.activate(nextMissing(record.mask(), record.activations) orelse return error.InvalidCatalogRecord)
            else
                self.resolve(nextMissing(record.mask(), record.resolutions) orelse return error.InvalidCatalogRecord),
            .active => self.resolve(nextMissing(record.mask(), record.resolutions) orelse return error.InvalidCatalogRecord),
            .resolved => self.finish(),
            .locking, .retired => error.InvalidCatalogRecord,
        };
    }

    fn replayPrepare(self: *Work, member: usize) !Result {
        const a = self.a;
        const retained = self.record.replication_intents orelse return error.InvalidCatalogRecord;
        const permit = self.record.replication_permits[member];
        const part = self.record.participants[member];
        var store = try requireSharedArtifacts(self.server.cfg.document_artifact_store);
        const payload = @import("../system_catalog/replication/payload.zig");
        const scope = try payload.uploadScope(self.reference, part.route.metadata_group_id, self.owner.metadata_incarnation, retained.upload_token, self.reference.txn_id);
        const parsed = try payload.readIntent(a, &store, retained.members[member], scope, permit, self.context.cancellation);
        defer parsed.deinit();
        var intent = parsed.value.intent;
        intent.external_decision_owner = self.owner;
        intent.replication_source_permit = permit;
        try self.record.requireReplicationIntent(a, part.route.metadata_group_id, permit, intent);

        if (part.publication_capture) |reference| {
            const bytes = try self.server.source.systemCatalog(a, self.context, .{ .sql_publication_capture_work = .{ .demand = .{
                .route = part.route,
                .pregrant_body_digest = permit.pregrant_body_digest,
                .request_digest = permit.request_digest,
            } } });
            const resolved = (try std.json.parseFromSliceLeaky(?installation.ResolvedDemand, a, bytes, .{ .allocate = .alloc_always })) orelse return error.CatalogPublicationProofPending;
            if (!resolved.reference.eql(reference)) return error.ExternalTransactionDecisionChanged;
            const wire = @import("logical_replication_capture_worker.zig");
            var demand: wire.Capture.Demand = .{
                .owner = self.owner,
                .transaction_id = self.reference.txn_id,
                .member = @intCast(member),
                .owner_group_id = permit.owner_group_id,
                .topology_epoch = permit.topology_epoch,
                .reference = reference,
                .roster = resolved.roster,
            };
            if (self.record.publicationCapacity(member)) |capacity| {
                try capacity.require(a, demand);
                demand.capacity = try capacity.grant(a);
                intent.publication_capture = demand;
            } else {
                var capacity_intent = intent;
                capacity_intent.replication_source_permit = null;
                const request = try wire.Capacity.Request.init(a, demand, capacity_intent);
                const receipt = try self.server.routedPreparedPublicationReceipt(a, part.table_name, part.route, .capacity, request, self.context);
                try receipt.value.require(a, demand);
                return self.advance(.{ .publication_capacity = receipt });
            }
        }

        const ids = try self.participantIds();
        const now = time.realtimeNs();
        _ = try self.deliver(member, .{ .timestamp_ns = now, .sync_level = .write, .transaction = .{ .begin = .{
            .txn_id = self.reference.txn_id,
            .external_decision_owner = self.owner,
            .begin_timestamp = self.record.identity.begin_timestamp,
            .created_at_ns = now,
            .topology_epoch = permit.topology_epoch,
            .retain_terminal = true,
            .participants = ids,
        } } });
        return self.deliver(member, intent.asBatch(self.reference.txn_id, permit.topology_epoch));
    }

    fn participantIds(self: *Work) ![]const []const u8 {
        const ids = try self.a.alloc([]const u8, self.record.participants.len);
        for (ids, self.record.participants) |*id, part|
            id.* = try local.api_local_transaction_contract.participantIdForGroup(self.a, part.table_name, part.route.route.group_id);
        return ids;
    }

    fn observe(self: *Work, member: usize, request: receipts.Request) !?receipts.Observation {
        const part = self.record.participants[member];
        return self.server.routedReplicationReceipt(self.a, part.table_name, part.route, request, self.context);
    }

    fn observeStage(self: *Work, member: usize, stage: local.api_replication_source_apply.ApplyReceipt.Stage) !?receipts.Receipt {
        const permit = self.record.replication_permits[member];
        const value = (try self.observe(member, .{ .stage = .{ .permit = permit, .stage = stage } })) orelse return null;
        return value.stage;
    }

    fn deliver(self: *Work, member: usize, batch: Batch) !Result {
        const part = self.record.participants[member];
        const budget = @min((try self.context.remainingTimeoutMs()) orelse forwarding.max_remaining_ms, forwarding.max_remaining_ms);
        const route_budget: forwarding.Context = .{ .remaining_ms = budget, .forwards_remaining = forwarding.max_forwards, .campaign_allowed = false };
        const group = part.route.route.group_id;
        if (self.server.cfg.routed_raft_batch_writer) |writer| {
            _ = (try writer.write(self.a, .transaction, group, part.table_name, batch, route_budget, self.context)) orelse return error.GroupLeaderUnavailable;
        } else {
            const router = @import("table_router.zig");
            const catalog = self.server.catalogSource();
            var fallback = router.CatalogBackedGroupRouter.init(catalog, self.server.localSessionNodeId());
            var route = (try router.resolveCapturedGroupRoute(self.a, catalog, self.server.cfg.session_router orelse fallback.router(), part.route, .prefer_leader)) orelse return error.GroupLeaderUnavailable;
            defer route.deinit(self.a);
            if (route != .remote) return error.GroupLeaderUnavailable;
            const transport = @import("http_client.zig");
            var client = transport.ApiHttpClient.init(self.a, self.server.cfg.session_executor orelse return error.HttpExecutorUnavailable);
            _ = client.withInternalServiceAuth(self.server.cfg.internal_service_secret, self.server.cfg.internal_service_issuer);
            const table = try transport.percentEncodePathComponent(self.a, part.table_name);
            const body = try local.api_internal_batch.encodeBatchRequest(self.a, batch);
            var cancellation = @import("../raft/transport/http_common.zig").RequestCancellation.fromToken(self.context.cancellation);
            var response = client.fetchGroupBatchWithForwarding(route.remote.base_uri, group, table, body, budget, route_budget, &cancellation, null) catch |err| {
                if (err == error.UnexpectedHttpStatus) {
                    const phase = batch.transaction orelse return err;
                    std.log.warn("subscription participant delivery refused group={d} phase={s} activation={}", .{
                        group, @tagName(phase), phase == .prepare and phase.prepare.external_activation,
                    });
                }
                return err;
            };
            response.deinit(self.a);
        }
        return .advanced;
    }

    fn activate(self: *Work, member: usize) !Result {
        if (try self.observeStage(member, .activated)) |receipt| return self.advance(.{ .activate = receipt });
        const permit = self.record.replication_permits[member];
        return self.deliver(member, .{ .sync_level = .write, .transaction = .{ .prepare = .{
            .txn_id = self.reference.txn_id,
            .topology_epoch = permit.topology_epoch,
            .external_decision_owner = self.owner,
            .external_activation = true,
            .replication_source_permit = permit,
        } } });
    }

    fn resolve(self: *Work, member: usize) !Result {
        const decision = self.record.decision orelse return error.InvalidCatalogRecord;
        const committed = decision.status == .committed;
        if (try self.observeStage(member, if (committed) .applied else .aborted)) |receipt| return self.advance(.{ .resolve = receipt });
        return self.deliver(member, .{ .sync_level = .write, .transaction = .{ .resolve = .{
            .txn_id = self.reference.txn_id,
            .status = if (committed) .committed else .aborted,
            .commit_version = decision.version,
            .external_decision_owner = self.owner,
            .replication_source_permit = self.record.replication_permits[member],
        } } });
    }

    fn collectPublicationTerminal(self: *Work) !?Result {
        const record = self.record;
        if (record.publication_capacities.len == record.publication_terminals.len + record.publication_abort_fences.len)
            return null;
        var retained: u64 = 0;
        for (record.publication_terminals) |terminal| retained |= @as(u64, 1) << @intCast(terminal.capture_ref.participant);
        for (record.publication_abort_fences) |fence| retained |= @as(u64, 1) << @intCast(fence.member);
        for (record.publication_capacities) |capacity| {
            if (retained & (@as(u64, 1) << @intCast(capacity.member)) != 0) continue;
            const part = record.participants[capacity.member];
            const receipt = (try self.server.routedPreparedPublicationReceipt(self.a, part.table_name, part.route, .terminal, capacity, self.context)) orelse continue;
            switch (receipt) {
                .captured => |terminal| {
                    if (terminal.value.capture_ref.participant != capacity.member) return error.ExternalTransactionDecisionChanged;
                    return try self.advance(.{ .publication_terminal = terminal });
                },
                .absent => |fence| {
                    if (fence.value.member != capacity.member) return error.ExternalTransactionDecisionChanged;
                    return try self.advance(.{ .publication_abort_fence = fence });
                },
            }
        }
        return .waiting;
    }

    fn finish(self: *Work) !Result {
        if (try self.collectPublicationTerminal()) |result| return result;
        const n = self.record.participants.len;
        if (n != 0 and self.record.replication_ack == null) {
            const terminal = try self.a.alloc(receipts.Receipt, n);
            const stage: local.api_replication_source_apply.ApplyReceipt.Stage = if (self.record.decision.?.status == .committed) .applied else .aborted;
            for (terminal, 0..) |*receipt, member| receipt.* = (try self.observeStage(member, stage)) orelse return .waiting;
            return self.submit(.{ .replication_ack_publish = .{
                .owner = self.owner,
                .reference = self.reference,
                .expected_revision = self.record.revision,
                .receipts = terminal,
            } });
        }
        const acknowledged = try self.a.alloc(receipts.Acknowledged, n);
        for (acknowledged, 0..) |*receipt, member| {
            const completion = (try registry.replicationAckForMember(self.a, self.record, member)) orelse return error.InvalidCatalogRecord;
            if (try self.observe(member, .{ .acknowledged = completion })) |observed| {
                receipt.* = observed.acknowledged;
                continue;
            }
            // A participant that missed BEGIN must recover its frozen roster
            // from the full set checked against the metadata ACK grant.
            const ids = try self.participantIds();
            return self.deliver(member, .{ .sync_level = .write, .transaction = .{ .acknowledge_many = .{
                .txn_id = self.reference.txn_id,
                .external_decision_owner = self.owner,
                .participants = ids,
                .replication_source_ack = completion,
            } } });
        }
        return self.submit(.{ .replication_checkpoint = .{
            .owner = self.owner,
            .reference = self.reference,
            .expected_revision = self.record.revision,
            .receipts = acknowledged,
        } });
    }

    fn advance(self: *Work, operation: registry.ReplicationAdvance) !Result {
        return self.submit(.{ .replication_advance = .{
            .owner = self.owner,
            .reference = self.reference,
            .expected_revision = self.record.revision,
            .operation = operation,
        } });
    }

    fn submit(self: *Work, command: Command) !Result {
        const a = self.a;
        const bytes = try self.server.source.systemCatalog(a, self.context, .{ .sql_prepared_read = .{ .operation = self.record.identity.operation_id } });
        const snapshot = try std.json.parseFromSliceLeaky(registry.Snapshot, a, bytes, .{});
        if (snapshot.standalone or !std.mem.eql(u8, &self.owner.metadata_incarnation, &std.fmt.bytesToHex(snapshot.incarnation, .lower)))
            return error.CatalogGenerationChanged;
        if (snapshot.records.len != 1 or snapshot.records[0].revision != self.record.revision or
            !(try snapshot.records[0].identity.owner(a)).eql(self.owner)) return error.CatalogGenerationChanged;
        var nonce: [16]u8 = undefined;
        try self.server.embedding_provider_runtime.io.randomSecure(&nonce);
        const response = try self.server.source.systemCatalog(a, self.context, .{ .sql_prepared_mutate = .{
            .admission = .{ .nonce = nonce, .head = snapshot.head },
            .incarnation = snapshot.incarnation,
            .actor = .{ .session_user = .bootstrap, .credential = "" },
            .database_id = self.record.identity.database_id,
            .operation = command,
        } });
        const outcome = (try std.json.parseFromSliceLeaky(?registry.outcomes.Outcome, a, response, .{})) orelse return error.MetadataMutationOutcomeUnknown;
        _ = try outcome.result();
        return .advanced;
    }
};

fn nextMissing(all: u64, done: u64) ?usize {
    const missing = all & ~done;
    return if (missing == 0) null else @ctz(missing);
}

test "subscription receipt RPC coordinator rejoins commit after losing the metadata reply" {
    const a = std.testing.allocator;
    const epoch: [16]u8 = @splat(0x11);
    const incarnation = std.fmt.bytesToHex(epoch, .lower);
    const event: registry.ReplicationEvent = .{ .kind = .initial_copy, .source_point = 11, .end_point = 11, .changes_digest = @splat(7) };
    const manifest = try registry.manifestDigest(a, &.{}, incarnation);
    const reference: registry.ReplicationReference = .{
        .owner = .{ .logical_subscription = .{ .database_id = 1, .subscription_id = 2 } },
        .birth = 3,
        .generation = 1,
        .source_digest = @splat(6),
        .event_digest = try event.eventDigest(),
        .intent_digest = @splat(8),
        .manifest_digest = manifest,
        .txn_id = @splat(5),
    };
    const identity: registry.Identity = .{
        .kind = .replication,
        .replication_source = reference,
        .gid = "",
        .database_id = 1,
        .principal = .bootstrap,
        .txn_id = reference.txn_id,
        .operation_id = reference.txn_id,
        .metadata_incarnation = incarnation,
        .begin_timestamp = 10,
        .expires_at_unix_ns = 100,
        .plan_digest = try registry.planDigest("[]"),
        .manifest_digest = manifest,
    };
    const Fixture = struct {
        record: ?registry.Record,
        completion: ?@import("../metadata/replication_subscription_work.zig").SnapshotWork = null,
        wal: ?@import("../metadata/replication_subscription_work.zig").WalWork = null,
        head: u64 = 3,
        lose_commit_reply: bool = true,
        fn call(ptr: *anyopaque, alloc: A, ctx: Context, input: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(ctx.setting_admin);
            switch (input) {
                .sql_publication_capture_work => {
                    if (input.sql_publication_capture_work.subscription) |work| {
                        if (self.wal != null and work.recovery == null)
                            try std.testing.expect(ctx.deadline_ns.? <= time.monotonicNs() +| 5 * std.time.ns_per_s);
                        if (work.recovery == null) return std.json.Stringify.valueAlloc(alloc, @import("../metadata/replication_subscription_work.zig").Page{
                            .incarnation = @splat('1'),
                            .items = &.{},
                            .next_after = null,
                            .snapshot = if (work.after == null) self.completion else null,
                            .wal = if (work.after == null) self.wal else null,
                        }, .{});
                        const record = self.record orelse return alloc.dupe(u8, "null");
                        if (work.recovery.?.after != null) return alloc.dupe(u8, "null");
                        return std.json.Stringify.valueAlloc(alloc, installation.SubscriptionApplyWork{
                            .realm = .{ .group_id = 1, .incarnation = record.identity.metadata_incarnation, .database_id = record.identity.database_id },
                            .source = record.identity.replication_source.?,
                        }, .{});
                    }
                    const probe = input.sql_publication_capture_work.slot.?.subscription_apply;
                    if (self.record) |record| {
                        const pending = record.identity.replication_source.?;
                        try std.testing.expectEqual(pending.owner.logical_subscription.subscription_id, probe.subscription_id);
                        try std.testing.expectEqual(pending.birth, probe.birth);
                        try std.testing.expectEqual(pending.source_digest, probe.origin);
                    }
                    const value: registry.ReplicationRecovery = .{
                        .progress = .{ .birth = probe.birth, .origin = probe.origin, .pending = if (self.record) |record| record.identity.replication_source else null },
                        .transaction = self.record,
                    };
                    return std.json.Stringify.valueAlloc(alloc, value, .{});
                },
                .sql_prepared_read => {
                    const records: []const registry.Record = if (self.record) |record| &.{record} else &.{};
                    return std.json.Stringify.valueAlloc(alloc, registry.Snapshot{
                        .incarnation = @splat(0x11),
                        .head = self.head,
                        .active = records.len,
                        .records = records,
                    }, .{});
                },
                .sql_prepared_mutate => {
                    const mutation = input.sql_prepared_mutate;
                    try mutation.validate();
                    try std.testing.expectEqual(self.head, mutation.admission.head);
                    if (mutation.operation == .replication_admit) {
                        const admitted = mutation.operation.replication_admit;
                        const expected = self.completion orelse return error.TestUnexpectedResult;
                        try std.testing.expect(self.record == null);
                        try std.testing.expectEqual(expected.event.?, admitted.event);
                        try std.testing.expectEqual(expected.generation + 1, admitted.input.identity.replication_source.?.generation);
                        self.record = .{ .identity = admitted.input.identity, .participants = &.{}, .replication_event = admitted.event };
                        self.completion = null;
                        self.head += 1;
                        return error.MetadataMutationOutcomeUnknown;
                    }
                    const record = self.record orelse return error.TestUnexpectedResult;
                    switch (mutation.operation) {
                        .replication_advance => |change| {
                            const action: registry.Action = switch (change.operation) {
                                .prepare => .prepare,
                                .commit => |decision| .{ .commit = decision },
                                else => return error.TestUnexpectedResult,
                            };
                            self.record = try registry.advance(alloc, record, change.expected_revision, change.owner, action);
                            self.head += 1;
                            if (action == .commit and self.lose_commit_reply) {
                                self.lose_commit_reply = false;
                                return error.MetadataMutationOutcomeUnknown;
                            }
                        },
                        .replication_checkpoint => |checkpoint| {
                            try std.testing.expectEqual(.resolved, record.phase);
                            try std.testing.expectEqual(record.revision, checkpoint.expected_revision);
                            try std.testing.expectEqual(@as(usize, 0), checkpoint.receipts.len);
                            self.record = null;
                            self.head += 1;
                        },
                        else => return error.TestUnexpectedResult,
                    }
                    return std.json.Stringify.valueAlloc(alloc, registry.outcomes.Outcome{ .success = .{ .revision = 1, .identity = null, .removed = 0 } }, .{});
                },
                else => return error.TestUnexpectedResult,
            }
        }
    };
    var mock: Fixture = .{ .record = .{ .identity = identity, .participants = &.{}, .replication_event = event } };
    var server = Server.init(a, .{}, .{ .ptr = &mock, .vtable = &.{ .status = undefined, .system_catalog = Fixture.call } }, null, null);
    defer server.deinit();
    const trusted: Context = .{ .setting_admin = true };
    var after: ?[16]u8 = null;
    try std.testing.expectEqual(.advanced, try stepPending(&server, a, &after, trusted));
    try std.testing.expectEqual(reference.txn_id, after.?);
    try std.testing.expectEqual(.prepared, mock.record.?.phase);
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, stepPending(&server, a, &after, trusted));
    try std.testing.expectEqual(reference.txn_id, after.?);
    try std.testing.expectEqual(.resolved, mock.record.?.phase);
    try std.testing.expectEqual(.advanced, try stepPending(&server, a, &after, trusted));
    try std.testing.expectEqual(.idle, try stepPending(&server, a, &after, trusted));
    try std.testing.expectEqual(@as(?[16]u8, null), after);
    mock.completion = .{
        .group = 1,
        .item = .{ .owner = reference.owner, .generation = 4, .recipe_digest = @splat(7), .operation = .{ .id = @splat(8), .body_digest = @splat(9) }, .phase = .initialize },
        .source = .{ .birth = reference.birth, .origin_digest = reference.source_digest },
        .generation = 9,
        .event = event,
    };
    var initial_after: ?u64 = null;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, stepInitialSnapshot(&server, a, &initial_after, trusted));
    try std.testing.expectEqual(@as(?u64, 2), initial_after);
    try stepInitialSnapshot(&server, a, &initial_after, trusted);
    try std.testing.expectEqual(@as(?u64, null), initial_after);
    try std.testing.expectEqual(.advanced, try stepPending(&server, a, &after, trusted));
    try std.testing.expectEqual(.prepared, mock.record.?.phase);
    mock.wal = .{ .group = 1, .owner = reference.owner, .source = .{ .birth = reference.birth, .origin_digest = reference.source_digest }, .recipe_digest = @splat(7) };
    var wal_after: ?u64 = null;
    try stepWal(&server, a, &wal_after, trusted);
    try std.testing.expectEqual(@as(?u64, 2), wal_after);
    try stepWal(&server, a, &wal_after, trusted);
    try std.testing.expectEqual(@as(?u64, null), wal_after);
}

const foreign = @import("../foreign/source.zig");

/// Rejoin the current subscriber receipt for both provider polling and
/// feedback. libpq invokes this again while holding the publisher slot lock.
pub const WalAuthority = struct {
    allocator: A,
    source: @import("http_server.zig").StatusSource,
    context: Context,
    reference: registry.ReplicationReference,
    checkpoint: u64,
    origin_digest: [32]u8,
    credential: ?@import("replication_subscription_capture.zig").Credential = null,

    pub fn provider(self: *WalAuthority) foreign.SubscriptionAuthority {
        return .{ .ptr = self, .check_fn = check, .observe_fn = observe, .acknowledged_fn = acknowledged };
    }

    fn check(raw: *anyopaque) !void {
        const self: *WalAuthority = @ptrCast(@alignCast(raw));
        if (!self.context.setting_admin) return error.Forbidden;
        try self.context.ensureActive();
        if (self.credential) |credential| try credential.require("", 0);
        const bytes = try self.source.systemCatalog(self.allocator, self.context, .{ .sql_prepared_read = .{
            .replication_checkpoint = .{ .reference = self.reference, .checkpoint = self.checkpoint },
        } });
        defer self.allocator.free(bytes);
        if (!std.mem.eql(u8, bytes, "true")) return error.ReplicationCutoverAuthorityLost;
    }

    fn observe(raw: *anyopaque, observed: foreign.SubscriptionOrigin.Observation) !void {
        const self: *WalAuthority = @ptrCast(@alignCast(raw));
        if (!std.mem.eql(u8, &try observed.digest(self.allocator), &self.origin_digest))
            return error.ForeignProviderIdentityMismatch;
    }

    fn acknowledged(raw: *anyopaque, checkpoint: []const u8) !void {
        const self: *WalAuthority = @ptrCast(@alignCast(raw));
        if (try foreign.parseReplicationCheckpoint(checkpoint) != self.checkpoint)
            return error.ReplicationCutoverAuthorityLost;
    }
};

/// Own the verified origin and provider connection configuration for one
/// coordinator quantum. Everything borrowed by the request uses the arena.
pub fn pollWalProvider(server: *Server, a: A, realm: @FieldType(Query, "realm"), progress: registry.ReplicationProgress, context: Context) !foreign.SubscriptionPollResult {
    if (!context.setting_admin) return error.Forbidden;
    var store = try requireSharedArtifacts(server.cfg.document_artifact_store);
    const reference = progress.last_acknowledged orelse return error.ReplicationCutoverAuthorityLost;
    const boundary = progress.snapshot_boundary orelse return error.ReplicationCutoverAuthorityLost;
    const custody = boundary.custody orelse return error.ReplicationCutoverAuthorityLost;
    if (progress.pending != null or progress.checkpoint == 0 or reference.owner != .logical_subscription or
        !reference.owner.eql(custody.purpose.owner) or reference.birth != custody.purpose.birth or
        realm.group_id != custody.purpose.metadata_group or realm.database_id != reference.owner.logical_subscription.database_id or
        !std.mem.eql(u8, &realm.incarnation, &custody.purpose.incarnation) or
        !std.mem.eql(u8, &reference.source_digest, &custody.purpose.source_digest))
        return error.ReplicationCutoverAuthorityLost;
    const bounded = try context.platformDeadline();
    const deadline = bounded.deadline_ns orelse return error.UnsupportedDeadline;
    const bytes = try server.source.systemCatalog(a, bounded, .{ .sql_publication_capture_work = .{ .slot = .{
        .subscription_wal_definition = .{ .realm = realm, .reference = reference, .checkpoint = progress.checkpoint },
    } } });
    defer a.free(bytes);
    const Definition = @import("../metadata/replication_subscription_work.zig").CaptureDefinition;
    const definition = try std.json.parseFromSliceLeaky(Definition, a, bytes, .{ .allocate = .alloc_always });
    const codec = @import("../system_catalog/replication/snapshot_codec.zig");
    const retained = try codec.readReplayOrigin(a, &store, custody.purpose, custody.ready, bounded.cancellation);
    defer retained.deinit();
    const slot = retained.value.source.slot_name;
    if (definition.options.slot_name != .name or !std.mem.eql(u8, definition.options.slot_name.name, slot))
        return error.ForeignProviderIdentityMismatch;
    const checkpoint = progress.checkpoint;
    var authority: WalAuthority = .{
        .allocator = a,
        .source = server.source,
        .context = bounded,
        .reference = reference,
        .checkpoint = checkpoint,
        .origin_digest = custody.purpose.roster_digest,
        .credential = .{
            .auth_enabled = server.cfg.auth_enabled,
            .manager = server.cfg.user_manager,
            .actor = definition.actor orelse return error.Forbidden,
        },
    };
    const request: foreign.SubscriptionRequest = .{
        .publications = definition.publications,
        .slot_name = slot,
        .lock_name = try std.fmt.allocPrint(a, "antfly/subscription/{d}/{d}/{d}", .{
            realm.group_id, realm.database_id, reference.owner.logical_subscription.subscription_id,
        }),
        .expected = retained.value.observed,
        .authority = authority.provider(),
        .checkpoint = try std.fmt.allocPrint(a, "{X}/{X}", .{ @as(u32, @intCast(checkpoint >> 32)), @as(u32, @truncate(checkpoint)) }),
        .advance_durable_checkpoint = true,
        .password_required = authority.credential.?.requiresProviderPassword(),
        .limit = 1,
        .execution_deadline_ns = deadline,
    };
    const providers = try server.ensureForeignRegistry();
    var provider = try providers.create(a, .{ .kind = .postgres, .dsn = @constCast(definition.connection) });
    defer provider.deinit(a);
    return provider.pollSubscription(a, request);
}

test "subscription receipt RPC checks the durable WAL checkpoint before feedback" {
    const Source = struct {
        allowed: bool = true,
        reads: usize = 0,

        fn call(raw: *anyopaque, a: A, context: Context, request: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(context.setting_admin);
            const probe = request.sql_prepared_read.replication_checkpoint;
            try std.testing.expectEqual(@as(u64, 32), probe.checkpoint);
            try std.testing.expectEqual(@as(u64, 3), probe.reference.generation);
            self.reads += 1;
            return a.dupe(u8, if (self.allowed) "true" else "false");
        }
    };
    const a = std.testing.allocator;
    try std.testing.expectError(error.Forbidden, pollWalProvider(undefined, a, undefined, undefined, .{}));
    var after: ?u64 = null;
    try std.testing.expectError(error.Forbidden, stepWal(undefined, a, &after, .{}));
    var transport: Source = .{};
    var owner: WalAuthority = .{
        .allocator = a,
        .source = .{ .ptr = &transport, .vtable = &.{ .status = undefined, .system_catalog = Source.call } },
        .context = .{ .setting_admin = true },
        .reference = .{
            .owner = .{ .logical_subscription = .{ .database_id = 1, .subscription_id = 2 } },
            .birth = 1,
            .generation = 3,
            .source_digest = @splat(1),
            .event_digest = @splat(2),
            .intent_digest = @splat(3),
            .manifest_digest = @splat(4),
            .txn_id = @splat(5),
        },
        .checkpoint = 32,
        .origin_digest = @splat(6),
    };
    const authority = owner.provider();
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, authority.acknowledged("0/21"));
    try authority.acknowledged("0/20");
    try std.testing.expectEqual(@as(usize, 3), transport.reads);
    transport.allowed = false;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, authority.acknowledged("0/20"));
    const progress: registry.ReplicationProgress = .{
        .birth = owner.reference.birth,
        .origin = owner.reference.source_digest,
        .generation = owner.reference.generation,
        .checkpoint = owner.checkpoint,
        .last_acknowledged = owner.reference,
    };
    try std.testing.expect(sameWalCursor(progress, progress));
    var replaced = progress;
    replaced.last_acknowledged.?.txn_id[0] ^= 1;
    try std.testing.expect(!sameWalCursor(replaced, progress));
    replaced = progress;
    replaced.birth += 1;
    try std.testing.expect(!sameWalCursor(replaced, progress));
}
/// One enabled subscription quantum, with a cursor independent of initial
/// COPY so a long capture cannot prevent other subscribers from applying.
pub fn stepWal(server: *Server, allocator: A, after: *?u64, context: Context) !void {
    if (!context.setting_admin) return error.Forbidden;
    var arena: std.heap.ArenaAllocator = .init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var bounded = try context.platformDeadline();
    const deadline = time.monotonicNs() +| 5 * std.time.ns_per_s;
    bounded.deadline_ns = @min(bounded.deadline_ns orelse deadline, deadline);
    const response = try server.source.systemCatalog(a, bounded, .{ .sql_publication_capture_work = .{ .subscription = .{ .after = after.* } } });
    const Page = @import("../metadata/replication_subscription_work.zig").Page;
    const page = try std.json.parseFromSliceLeaky(Page, a, response, .{});
    after.* = page.next_after;
    const work = page.wal orelse return;
    if (work.owner != .logical_subscription) return error.InvalidSqlBackendResponse;
    const owner = work.owner.logical_subscription;
    after.* = owner.subscription_id;
    const query: Query = .{
        .realm = .{ .group_id = work.group, .incarnation = page.incarnation, .database_id = owner.database_id },
        .subscription_id = owner.subscription_id,
        .birth = work.source.birth,
        .origin = work.source.origin_digest,
    };
    const recovery = (try readRecovery(server, a, bounded, query)) orelse return;
    const progress = recovery.progress;
    if (progress.pending != null or progress.checkpoint == 0) return;
    var polled = try pollWalProvider(server, a, query.realm, progress, bounded);
    defer polled.deinit(a);
    const rows = @import("replication_subscription_rows.zig");
    const transaction = try rows.nextWalTransaction(polled.poll, progress.checkpoint);
    const next = transaction orelse return;
    const prepared = planWalCommand(server, a, query, progress, work.recipe_digest, next, polled.origin, bounded) catch |err| {
        std.log.debug("subscription WAL planning deferred err={s}", .{@errorName(err)});
        return err;
    };
    submitAdmission(server, a, prepared, bounded) catch |err| {
        std.log.debug("subscription WAL admission deferred err={s}", .{@errorName(err)});
        return err;
    };
}

const wal_rows = @import("replication_subscription_rows.zig");
const wal_catalog = @import("table_catalog.zig");

const WalPreimage = struct {
    read_source: local.api_table_read_source.TableReadSource,
    name: []const u8,
    context: Context,

    fn read(raw: *anyopaque, a: A, key: []const u8) !?wal_rows.WalObserved {
        const self: *WalPreimage = @ptrCast(@alignCast(raw));
        const found = (try self.read_source.lookupMutationPreimage(a, self.name, key, .{
            .include_primary_digest = true,
            .include_json_null_fields = true,
            .execution_deadline_ns = self.context.deadline_ns,
            .cancellation = self.context.cancellation,
        })) orelse return null;
        return .{
            .key = key,
            .row = .{
                .value = try std.json.parseFromSliceLeaky(std.json.Value, a, found.json, .{ .parse_numbers = false }),
                .json_null_fields = found.json_null_fields,
            },
            .version = found.version,
            .content_digest = found.expected_content_digest orelse return error.InvalidSqlBackendResponse,
        };
    }
};

fn planWalCommand(server: *Server, a: A, query: Query, progress: registry.ReplicationProgress, recipe: [32]u8, transaction: wal_rows.WalWork, origin: foreign.SubscriptionOrigin.Observation, context: Context) !@FieldType(Command, "replication_admit") {
    var store = try requireSharedArtifacts(server.cfg.document_artifact_store);
    const limit = @min(transaction.changes.len, registry.max_participants);
    const tables = try a.alloc(wal_rows.WalTable, limit);
    const routing = try a.alloc(wal_catalog.AuthoritativeTableRoutingSnapshot, limit);
    const readers = try a.alloc(WalPreimage, limit);
    var count: usize = 0;
    defer for (routing[0..count]) |*route| route.deinit(a);
    const Access = local.system_catalog_privileges.Access;
    const bindings = try a.alloc(registry.ReplicationRecovery, limit);
    var seen: std.AutoArrayHashMapUnmanaged(u32, u16) = .empty;
    defer seen.deinit(a);
    for (transaction.changes) |change| {
        const source = change.relation orelse return error.InvalidReplicationSourceRow;
        const bit = switch (change.op) {
            .insert => Access.bit(.insert),
            .update => Access.bit(.update),
            .delete => Access.bit(.delete),
        };
        const entry = try seen.getOrPut(a, source.id);
        if (!entry.found_existing) {
            if (seen.count() > limit) return error.CatalogCommandTooLarge;
            entry.value_ptr.* = 0;
        }
        entry.value_ptr.* |= bit;
    }
    for (seen.keys(), seen.values()) |source_id, mask| {
        try context.ensureActive();
        const relation = origin.find(source_id) orelse return error.ForeignProviderIdentityMismatch;
        var resolve = query;
        resolve.destination = .{ .namespace = relation.namespace, .relation = relation.name, .rights = mask };
        const current = (try readRecovery(server, a, context, resolve)) orelse return error.ReplicationCutoverAuthorityLost;
        if (!sameWalCursor(current.progress, progress)) return error.ReplicationCutoverAuthorityLost;
        const destination = current.destination orelse return error.InvalidSqlBackendResponse;
        const reads = server.table_reads orelse return error.UnsupportedSqlExecution;
        routing[count] = (try wal_catalog.authoritativeTableRoutingSnapshot(a, server.catalogSource(), destination.storage_name, context.deadline_ns)) orelse return error.CatalogGenerationChanged;
        const route = &routing[count];
        if (route.table_id != destination.table_id or route.snapshot.value.metadata_group_id != query.realm.group_id or
            !std.meta.eql(route.snapshot.value.metadata_incarnation, @as(?[32]u8, query.realm.incarnation)) or
            (count != 0 and route.snapshot.value.catalog_revision != routing[0].snapshot.value.catalog_revision))
        {
            route.deinit(a);
            return error.CatalogGenerationChanged;
        }
        readers[count] = .{ .read_source = reads, .name = destination.storage_name, .context = context };
        tables[count] = .{
            .relation = relation,
            .destination = destination,
            .routing = route,
            .reader = .{ .ptr = &readers[count], .observe = WalPreimage.read },
        };
        bindings[count] = current;
        count += 1;
    }
    for (bindings[0..count], seen.values()) |binding, mask|
        try requireDestinationCredential(server, binding, mask);
    const planned = try coordinateRelationalEffects(server, a, try wal_rows.planWal(a, query.origin, transaction, tables[0..count]), context);
    try context.ensureActive();
    const wal_access = try a.alloc(registry.ReplicationWalAccess, count);
    for (wal_access, tables[0..count], seen.values()) |*access, table, rights| {
        const destination = table.destination;
        access.* = .{
            .table_id = destination.table_id,
            .schema_version = destination.schema_version,
            .schema_digest = destination.schema_digest,
            .policy_epoch = destination.policy_epoch,
            .catalog_revision = table.routing.snapshot.value.catalog_revision,
            .rights = rights,
        };
    }
    std.mem.sort(registry.ReplicationWalAccess, wal_access, {}, struct {
        fn lessThan(_: void, left: registry.ReplicationWalAccess, right: registry.ReplicationWalAccess) bool {
            return left.table_id < right.table_id;
        }
    }.lessThan);
    const namespaces = try a.alloc(local.storage_db_doc_identity_namespace.Namespace, planned.len);
    for (namespaces, planned) |*namespace, member| {
        const route = member.group.participant.route.route.identity_namespace;
        namespace.* = .{ .table_id = route.table_id, .shard_id = route.shard_id, .range_id = route.range_id };
    }
    var targets = query;
    targets.targets = namespaces;
    const current = (try readRecovery(server, a, context, targets)) orelse return error.ReplicationCutoverAuthorityLost;
    if (!sameWalCursor(current.progress, progress)) return error.ReplicationCutoverAuthorityLost;
    for (bindings[0..count], seen.values()) |binding, mask|
        try requireDestinationCredential(server, binding, mask);
    var id: [16]u8 = undefined;
    try server.embedding_provider_runtime.io.randomSecure(&id);
    const now = time.realtimeNs();
    const generation = std.math.add(u64, progress.generation, 1) catch return error.CatalogGenerationExhausted;
    return admission.sealWal(a, &store, .{
        .metadata_group = query.realm.group_id,
        .incarnation = query.realm.incarnation,
        .owner = progress.last_acknowledged.?.owner,
        .birth = query.birth,
        .generation = generation,
        .origin = query.origin,
        .txn_id = id,
        .recipe_digest = recipe,
        .upload_token = generation,
        .begin_timestamp = now / std.time.ns_per_ms,
        .admitted_at_ns = now,
        .expires_at_ns = now +| 30 * std.time.ns_per_s,
        .event = transaction.event,
        .wal_access = wal_access,
    }, planned, current.targets, context.cancellation);
}

fn sameWalCursor(current: registry.ReplicationProgress, expected: registry.ReplicationProgress) bool {
    const prior = expected.last_acknowledged orelse return false;
    const observed = current.last_acknowledged orelse return false;
    return current.pending == null and expected.pending == null and
        current.birth == expected.birth and std.mem.eql(u8, &current.origin, &expected.origin) and
        current.generation == expected.generation and current.checkpoint == expected.checkpoint and
        observed.eql(prior);
}
