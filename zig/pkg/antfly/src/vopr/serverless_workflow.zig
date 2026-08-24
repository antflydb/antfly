// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! End-to-end serverless workflow campaign over production stores and
//! orchestration. VOPR selects one immediate fault history; the real WAL,
//! builder, manifests, progress CAS, compactor, catalog, and query session
//! establish the recovery behavior.

const std = @import("std");
const vopr = @import("vopr");
const objectstore = @import("objectstore");
const artifacts_object_store = @import("../serverless/artifacts/object_store.zig");
const manifest_object_store = @import("../serverless/manifest/object_store.zig");
const wal_object_store = @import("../serverless/wal/object_store.zig");
const progress_object_store = @import("../serverless/catalog/object_progress_store.zig");
const catalog_object_store = @import("../serverless/catalog/object_store.zig");
const artifacts_mod = @import("../serverless/artifacts/mod.zig");
const manifest_mod = @import("../serverless/manifest/mod.zig");
const wal_mod = @import("../serverless/wal/mod.zig");
const catalog_mod = @import("../serverless/catalog/mod.zig");
const build_mod = @import("../serverless/build/mod.zig");
const api_mod = @import("../serverless/api/mod.zig");
const query_mod = @import("../serverless/query/mod.zig");
const document_segment = @import("../serverless/document_segment/mod.zig");
const runtime_manager = @import("../serverless/runtime/manager.zig");
const head_coordination = @import("../serverless/head_coordination.zig");
const maintenance_cancellation = @import("../serverless/maintenance_cancellation.zig");

pub const Scenario = struct {
    pub const name: []const u8 = "serverless-workflow-production-recovery";
    pub const version: u32 = 3;

    const no_lost_documents_id = vopr.id.stable(name, "no-lost-documents");
    const catalog_visible_id = vopr.id.stable(name, "catalog-visible-after-cutover");
    const stale_fenced_id = vopr.id.stable(name, "stale-owner-fenced-at-publication");
    const legacy_epoch_id = vopr.id.stable(name, "legacy-head-rewrite-preserves-fencing-epoch");
    const duplicate_serialized_id = vopr.id.stable(name, "duplicate-workers-serialized");
    const recovery_id = vopr.id.stable(name, "interrupted-workflow-recovers");
    const compacted_id = vopr.id.stable(name, "compaction-publishes-complete-head");

    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = no_lost_documents_id, .name = name ++ ".no-lost-documents", .kind = .always },
        .{ .id = catalog_visible_id, .name = name ++ ".catalog-visible-after-cutover", .kind = .always },
        .{ .id = stale_fenced_id, .name = name ++ ".stale-owner-fenced-at-publication", .kind = .always },
        .{ .id = legacy_epoch_id, .name = name ++ ".legacy-head-rewrite-preserves-fencing-epoch", .kind = .always },
        .{ .id = duplicate_serialized_id, .name = name ++ ".duplicate-workers-serialized", .kind = .always },
        .{ .id = recovery_id, .name = name ++ ".interrupted-workflow-recovers", .kind = .reachable },
        .{ .id = compacted_id, .name = name ++ ".compaction-publishes-complete-head", .kind = .reachable },
    };

    const Mode = enum {
        clean,
        duplicate_workers,
        lease_takeover,
        legacy_head_rewrite,
        ambiguous_publish,
        cancellation,
        retry,
        crash_recovery,
        compaction_crash,
    };

    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "clean"),
        vopr.id.stable(name, "duplicate-workers"),
        vopr.id.stable(name, "lease-takeover"),
        vopr.id.stable(name, "legacy-head-rewrite"),
        vopr.id.stable(name, "ambiguous-publish"),
        vopr.id.stable(name, "cancellation"),
        vopr.id.stable(name, "retry"),
        vopr.id.stable(name, "crash-recovery"),
        vopr.id.stable(name, "compaction-crash"),
    };
    const mode_names = [_][]const u8{
        name ++ ".clean",
        name ++ ".duplicate_workers",
        name ++ ".lease_takeover",
        name ++ ".legacy_head_rewrite",
        name ++ ".ambiguous_publish",
        name ++ ".cancellation",
        name ++ ".retry",
        name ++ ".crash_recovery",
        name ++ ".compaction_crash",
    };

    const State = struct {
        alloc: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        memory: objectstore.MemoryClient,
        artifact_faults: objectstore.ScriptedFaultClient,
        manifest_faults: objectstore.ScriptedFaultClient,
        wal_faults: objectstore.ScriptedFaultClient,
        progress_faults: objectstore.ScriptedFaultClient,
        catalog_faults: objectstore.ScriptedFaultClient,
        lease_faults: objectstore.ScriptedFaultClient,
        artifact_impl: artifacts_object_store.ObjectStore,
        artifacts: artifacts_mod.ArtifactStore,
        manifest_impl: manifest_object_store.ObjectStore,
        manifests: manifest_mod.ManifestStore,
        wal_impl: wal_object_store.ObjectStore,
        wal: wal_mod.WalStore,
        progress_impl: progress_object_store.ObjectProgressStore,
        progress: catalog_mod.ProgressStore,
        catalog_impl: catalog_object_store.ObjectStore,
        catalog_store: catalog_mod.CatalogStore,
        lease_impl: build_mod.ObjectWorkLeaseStore,
        builder: build_mod.Builder,
        catalog: catalog_mod.CatalogService,
        api: api_mod.Service,
        query: query_mod.QueryRuntime,
        runtime: runtime_manager.ManagedRuntime,
        runtime_live: bool = false,
        mode: Mode = .clean,
        complete: bool = false,
        first_attempt_interrupted: bool = false,
        duplicate_blocked: bool = false,
        stale_fenced: bool = false,
        legacy_epoch_preserved: bool = false,
        recovered: bool = false,
        final_head: u64 = 0,
        visible_document_mask: u8 = 0,
        compacted: bool = false,

        fn init(alloc: std.mem.Allocator) !*State {
            const self = try alloc.create(State);
            errdefer alloc.destroy(self);
            self.alloc = alloc;
            self.sim = try vopr.vopr_io.VoprIo.init(.{
                .seed = 0x5352564c,
                .realtime_ns = 1_000,
                .instrumentation = .{ .enabled = false, .map_digest = 0x5352564c },
            });
            errdefer self.sim.deinit();
            self.memory = objectstore.MemoryClient.init(alloc);
            errdefer self.memory.deinit();
            self.artifact_faults = objectstore.ScriptedFaultClient.init(alloc, self.memory.client());
            errdefer self.artifact_faults.deinit();
            self.manifest_faults = objectstore.ScriptedFaultClient.init(alloc, self.memory.client());
            errdefer self.manifest_faults.deinit();
            self.wal_faults = objectstore.ScriptedFaultClient.init(alloc, self.memory.client());
            errdefer self.wal_faults.deinit();
            self.progress_faults = objectstore.ScriptedFaultClient.init(alloc, self.memory.client());
            errdefer self.progress_faults.deinit();
            self.catalog_faults = objectstore.ScriptedFaultClient.init(alloc, self.memory.client());
            errdefer self.catalog_faults.deinit();
            self.lease_faults = objectstore.ScriptedFaultClient.init(alloc, self.memory.client());
            errdefer self.lease_faults.deinit();

            self.artifact_impl = try artifacts_object_store.ObjectStore.initWithClient(
                alloc,
                self.artifact_faults.client(),
                "workflow-artifacts",
                "tenant",
            );
            self.artifacts = self.artifact_impl.artifactStore();
            errdefer self.artifacts.deinit();
            self.manifest_impl = try manifest_object_store.ObjectStore.initWithClient(
                alloc,
                self.manifest_faults.client(),
                "workflow-manifests",
                "tenant",
            );
            self.manifests = self.manifest_impl.manifestStore();
            errdefer self.manifests.deinit();
            self.wal_impl = try wal_object_store.ObjectStore.initWithClient(
                alloc,
                self.wal_faults.client(),
                "workflow-wal",
                "tenant",
            );
            self.wal = self.wal_impl.walStore();
            errdefer self.wal.deinit();
            self.progress_impl = try progress_object_store.ObjectProgressStore.initWithClient(
                alloc,
                self.progress_faults.client(),
                "workflow-progress",
                "tenant",
            );
            self.progress = self.progress_impl.progressStore();
            errdefer self.progress.deinit();
            self.catalog_impl = try catalog_object_store.ObjectStore.initWithClient(
                alloc,
                self.catalog_faults.client(),
                "workflow-catalog",
                "tenant",
            );
            self.catalog_store = self.catalog_impl.catalogStore();
            errdefer self.catalog_store.deinit();
            self.lease_impl = try build_mod.ObjectWorkLeaseStore.initWithClient(
                alloc,
                self.lease_faults.client(),
                self.progress_impl.bucket,
                self.progress_impl.prefix,
            );
            errdefer self.lease_impl.deinit();

            self.builder = build_mod.Builder.init(
                alloc,
                &self.artifacts,
                &self.manifests,
                &self.progress,
                &self.wal,
            );
            self.catalog = catalog_mod.CatalogService.init(
                alloc,
                &self.artifacts,
                &self.manifests,
                &self.progress,
                &self.wal,
                &self.builder,
                &self.catalog_store,
            );
            errdefer self.catalog.deinit();
            self.api = api_mod.Service.init(alloc, &self.wal, &self.builder);
            self.query = query_mod.QueryRuntime.init(
                alloc,
                &self.artifacts,
                &self.manifests,
                &self.progress,
            );
            errdefer self.query.deinit();
            self.runtime_live = false;
            self.mode = .clean;
            self.complete = false;
            self.first_attempt_interrupted = false;
            self.duplicate_blocked = false;
            self.stale_fenced = false;
            self.legacy_epoch_preserved = false;
            self.recovered = false;
            self.final_head = 0;
            self.visible_document_mask = 0;
            self.compacted = false;

            try self.initRuntime("worker-primary");
            errdefer self.runtime.deinit();
            try std.testing.expect(try self.catalog.ensureNamespaceWithPolicy("docs", 100, .{
                .keep_latest_versions = 8,
                .max_pending_records = 1,
                .compaction_enabled = true,
                .compaction_trigger_version_count = 2,
            }));
            return self;
        }

        fn deinit(self: *State) void {
            if (self.runtime_live) self.runtime.deinit();
            self.query.deinit();
            self.catalog.deinit();
            self.lease_impl.deinit();
            self.catalog_store.deinit();
            self.progress.deinit();
            self.wal.deinit();
            self.manifests.deinit();
            self.artifacts.deinit();
            self.lease_faults.deinit();
            self.catalog_faults.deinit();
            self.progress_faults.deinit();
            self.wal_faults.deinit();
            self.manifest_faults.deinit();
            self.artifact_faults.deinit();
            self.memory.deinit();
            self.sim.deinit();
            self.alloc.destroy(self);
        }

        fn initRuntime(self: *State, owner_id: []const u8) !void {
            self.runtime = runtime_manager.ManagedRuntime.initWithIo(
                self.alloc,
                self.sim.io(),
                .{
                    .tick_interval_ms = 1,
                    .publish_enabled = true,
                    .compaction_enabled = true,
                    .prune_enabled = false,
                    .enrichment_enabled = false,
                },
                &self.catalog,
                build_mod.Pruner.init(
                    self.alloc,
                    &self.artifacts,
                    &self.manifests,
                    &self.progress,
                    &self.wal,
                ),
            );
            errdefer self.runtime.deinit();
            try self.runtime.configureWorkLease(
                self.lease_impl.provider(),
                owner_id,
                100,
            );
            self.runtime.setCompactor(build_mod.Compactor.init(
                self.alloc,
                &self.artifacts,
                &self.manifests,
                &self.progress,
            ));
            self.runtime_live = true;
        }

        fn restartRuntime(self: *State) !void {
            if (self.runtime_live) {
                self.runtime.deinit();
                self.runtime_live = false;
            }
            self.artifact_faults.resetClientAfterCrash();
            self.manifest_faults.resetClientAfterCrash();
            self.wal_faults.resetClientAfterCrash();
            self.progress_faults.resetClientAfterCrash();
            self.catalog_faults.resetClientAfterCrash();
            self.lease_faults.resetClientAfterCrash();
            try self.initRuntime("worker-recovery");
        }

        fn ingest(self: *State, doc_id: []const u8, body: []const u8, timestamp_ns: u64) !void {
            const mutations = [_]api_mod.DocumentMutation{.{
                .kind = .upsert,
                .doc_id = doc_id,
                .body = body,
            }};
            var result = try self.api.ingestBatch(.{
                .namespace = "docs",
                .timestamp_ns = timestamp_ns,
                .mutations = &mutations,
            });
            result.deinit(self.alloc);
        }

        fn runFirstPublication(self: *State) !void {
            switch (self.mode) {
                .clean, .compaction_crash => {
                    _ = try self.runtime.runOnce();
                },
                .duplicate_workers => {
                    var incumbent = (try build_mod.work_lease.acquireBootstrapHeld(
                        self.lease_impl.provider(),
                        self.sim.io(),
                        "docs",
                        "worker-incumbent",
                        100,
                    )).?;
                    // Cross the original expiry while the incumbent is in a
                    // CPU checkpoint. The attached heartbeat must extend the
                    // bootstrap claim before a duplicate worker can start.
                    try self.sim.jumpRealtime(60);
                    try incumbent.cancellation(maintenance_cancellation.Token{
                        .io = self.sim.io(),
                    }).check();
                    try self.sim.jumpRealtime(60);
                    const contender = try self.runtime.runOnce();
                    self.duplicate_blocked = contender.work_lease_conflicts == 1 and
                        (self.progress.getHead("docs") catch 0) == 0;
                    var published = try self.catalog.buildNamespace("docs");
                    published.deinit(self.alloc);
                    _ = try incumbent.release();
                },
                .lease_takeover, .legacy_head_rewrite => {
                    // First publication is protected by the absent-to-present
                    // HEAD CAS; bootstrap coordination never materializes HEAD.
                    _ = try self.runtime.runOnce();
                },
                .ambiguous_publish => {
                    self.progress_faults.commitNextPutThenFail(error.Timeout);
                    try self.expectInterrupted(error.Timeout);
                    try std.testing.expectEqual(@as(u64, 1), try self.progress.getHead("docs"));
                    try self.restartRuntime();
                    _ = try self.runtime.runOnce();
                },
                .cancellation => {
                    self.progress_faults.failNextPutBefore(error.Canceled);
                    try self.expectInterrupted(error.Canceled);
                    try std.testing.expectError(error.FileNotFound, self.progress.getHead("docs"));
                    try self.restartRuntime();
                    _ = try self.runtime.runOnce();
                },
                .retry => {
                    self.artifact_faults.failNextPutBefore(error.Timeout);
                    try self.expectInterrupted(error.Timeout);
                    try self.restartRuntime();
                    _ = try self.runtime.runOnce();
                },
                .crash_recovery => {
                    self.manifest_faults.commitNextPutThenFail(error.ConnectionResetByPeer);
                    try self.expectInterrupted(error.ConnectionResetByPeer);
                    try std.testing.expectError(error.FileNotFound, self.progress.getHead("docs"));
                    try self.restartRuntime();
                    _ = try self.runtime.runOnce();
                },
            }
            try std.testing.expectEqual(@as(u64, 1), try self.progress.getHead("docs"));
        }

        fn expectInterrupted(self: *State, expected: anyerror) !void {
            if (self.runtime.runOnce()) |_| return error.ExpectedWorkflowInterruption else |err| {
                if (err != expected) return err;
                self.first_attempt_interrupted = true;
            }
        }

        fn runSecondPublicationAndCompaction(self: *State) !void {
            try self.ingest("doc-b", "beta gamma", 200);
            if (self.mode == .lease_takeover) {
                var stale = (try build_mod.work_lease.acquireHeld(
                    self.lease_impl.provider(),
                    self.sim.io(),
                    "docs",
                    "worker-stale",
                    10,
                )).?;
                if (try build_mod.work_lease.acquireHeld(
                    self.lease_impl.provider(),
                    self.sim.io(),
                    "docs",
                    "worker-stale",
                    100,
                )) |unexpected| {
                    var owned = unexpected;
                    _ = try owned.release();
                    return error.ActiveSameOwnerReacquired;
                }
                var raw = self.memory.client();
                var before_takeover = try raw.getObject("workflow-progress", "tenant/docs/HEAD", .{});
                const stale_etag = try self.alloc.dupe(u8, before_takeover.metadata.etag.?);
                before_takeover.deinit(self.alloc);
                defer self.alloc.free(stale_etag);
                const stale_renewal = head_coordination.Record{
                    .head_version = 1,
                    .owner_id = "worker-stale",
                    .fencing_token = stale.acquisition.fencing_token,
                    .expires_at_unix_ns = 200,
                    .released = false,
                };
                const stale_payload = try head_coordination.payloadAlloc(self.alloc, stale_renewal);
                defer self.alloc.free(stale_payload);
                const stale_content_type = try head_coordination.contentTypeAlloc(self.alloc, stale_renewal);
                defer self.alloc.free(stale_content_type);
                try self.sim.jumpRealtime(10);
                var replacement = (try build_mod.work_lease.acquireHeld(
                    self.lease_impl.provider(),
                    self.sim.io(),
                    "docs",
                    "worker-replacement",
                    100,
                )).?;
                if (raw.putObject("workflow-progress", "tenant/docs/HEAD", stale_payload, .{
                    .content_type = stale_content_type,
                    .if_match_etag = stale_etag,
                })) |*result| {
                    var owned = result.*;
                    owned.deinit(self.alloc);
                    return error.StaleLeaseResurrected;
                } else |err| {
                    if (err != error.PreconditionFailed) return err;
                }
                if (self.catalog.buildNamespaceGuarded("docs", stale.guard())) |*result| {
                    var owned = result.*;
                    owned.deinit(self.alloc);
                    return error.StaleWorkerPublished;
                } else |err| {
                    if (err != error.WorkLeaseLost) return err;
                    self.stale_fenced = true;
                    self.first_attempt_interrupted = true;
                }
                _ = try replacement.release();
                _ = try self.runtime.runOnce();
            } else if (self.mode == .legacy_head_rewrite) {
                var stale = (try build_mod.work_lease.acquireHeld(
                    self.lease_impl.provider(),
                    self.sim.io(),
                    "docs",
                    "stable-owner",
                    100,
                )).?;
                var legacy_client = self.memory.client();
                var current = try legacy_client.getObject("workflow-progress", "tenant/docs/HEAD", .{});
                const current_etag = try self.alloc.dupe(u8, current.metadata.etag.?);
                current.deinit(self.alloc);
                defer self.alloc.free(current_etag);
                var legacy_write = try legacy_client.putObject("workflow-progress", "tenant/docs/HEAD", "1", .{
                    .content_type = "text/plain",
                    .if_match_etag = current_etag,
                });
                legacy_write.deinit(self.alloc);
                var replacement = (try build_mod.work_lease.acquireHeld(
                    self.lease_impl.provider(),
                    self.sim.io(),
                    "docs",
                    "stable-owner",
                    100,
                )).?;
                self.legacy_epoch_preserved = replacement.acquisition.fencing_token > stale.acquisition.fencing_token;
                if (self.catalog.buildNamespaceGuarded("docs", stale.guard())) |*result| {
                    var owned = result.*;
                    owned.deinit(self.alloc);
                    return error.StaleWorkerPublishedAfterLegacyRewrite;
                } else |err| {
                    if (err != error.WorkLeaseLost) return err;
                }
                _ = try replacement.release();
                _ = try self.runtime.runOnce();
            } else if (self.mode == .compaction_crash) {
                var builder_lease = (try build_mod.work_lease.acquireHeld(
                    self.lease_impl.provider(),
                    self.sim.io(),
                    "docs",
                    "worker-builder",
                    100,
                )).?;
                var build = try self.catalog.buildNamespaceGuarded("docs", builder_lease.guard());
                build.deinit(self.alloc);
                _ = try builder_lease.release();
                self.progress_faults.commitNextPutThenFail(error.Timeout);
                try self.expectInterrupted(error.Timeout);
                try std.testing.expectEqual(@as(u64, 3), try self.progress.getHead("docs"));
                try self.restartRuntime();
                _ = try self.runtime.runOnce();
            } else {
                const stats = try self.runtime.runOnce();
                self.compacted = stats.compacted_namespaces == 1;
            }
            self.final_head = try self.progress.getHead("docs");
            if (self.final_head == 3) self.compacted = true;
        }

        fn observeVisibility(self: *State) !void {
            var status = try self.catalog.buildStatus("docs");
            defer status.deinit(self.alloc);
            if (status.head_version != self.final_head or
                status.pending_records != 0 or
                status.publish_recommended)
            {
                return error.CatalogHeadNotVisible;
            }

            var session = try self.query.openHeadSession("docs");
            defer session.deinit();
            const artifact_index = session.findArtifactIndex(.document_segment) orelse
                return error.DocumentSegmentNotFound;
            const payload = try session.fetchArtifactAlloc(artifact_index);
            defer self.alloc.free(payload);
            const entries = try document_segment.decodeAlloc(self.alloc, payload);
            defer document_segment.freeEntries(self.alloc, entries);
            var mask: u8 = 0;
            for (entries) |entry| {
                if (std.mem.eql(u8, entry.doc_id, "doc-a")) mask |= 1;
                if (std.mem.eql(u8, entry.doc_id, "doc-b")) mask |= 2;
            }
            self.visible_document_mask = mask;
        }

        fn run(self: *State) !void {
            try self.ingest("doc-a", "alpha beta", 100);
            try self.runFirstPublication();
            try self.runSecondPublicationAndCompaction();
            try self.observeVisibility();
            try self.sim.ensureNoCapabilityViolation();
            self.recovered = true;
            self.complete = true;
        }
    };

    pub const World = struct { state: *State };

    pub fn init(allocator: std.mem.Allocator) !World {
        return .{ .state = try State.init(allocator) };
    }

    pub fn deinit(world: *World, _: std.mem.Allocator) void {
        world.state.deinit();
        world.* = undefined;
    }

    pub fn enumerate(
        world: *World,
        list: *vopr.transition.List,
        allocator: std.mem.Allocator,
    ) !void {
        if (world.state.complete) return;
        inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| {
            try list.append(allocator, .{
                .id = id,
                .name = mode_name,
                .kind = if (mode == .clean) .workload else .fault,
            });
        }
    }

    pub fn execute(
        world: *World,
        selected: vopr.transition.Transition,
        events: *vopr.event.Sink,
        allocator: std.mem.Allocator,
    ) !vopr.outcome.TransitionOutcome {
        var found = false;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| {
            if (selected.id == id) {
                world.state.mode = mode;
                found = true;
            }
        }
        if (!found) return error.InvalidServerlessWorkflowTransition;
        try world.state.run();
        try events.emitNamed(allocator, .domain, selected.name, world.state.final_head);
        return .applied();
    }

    pub fn observe(
        world: *World,
        builder: *vopr.observation.Builder,
        allocator: std.mem.Allocator,
    ) !void {
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(world.state.complete));
        try builder.addNamed(allocator, name ++ ".head", @intCast(world.state.final_head));
        try builder.addNamed(allocator, name ++ ".documents", world.state.visible_document_mask);
        try builder.addNamed(allocator, name ++ ".interrupted", @intFromBool(world.state.first_attempt_interrupted));
        try builder.addNamed(allocator, name ++ ".compacted", @intFromBool(world.state.compacted));
        try builder.addNamed(allocator, name ++ ".legacy-epoch-preserved", @intFromBool(world.state.legacy_epoch_preserved));
    }

    pub fn evaluate(
        world: *World,
        sink: *vopr.property.Sink,
        allocator: std.mem.Allocator,
    ) !void {
        const state = world.state;
        try sink.check(
            allocator,
            no_lost_documents_id,
            !state.complete or state.visible_document_mask == 3,
        );
        try sink.check(
            allocator,
            catalog_visible_id,
            !state.complete or state.final_head == 3,
        );
        try sink.check(
            allocator,
            stale_fenced_id,
            state.mode != .lease_takeover or state.stale_fenced,
        );
        try sink.check(
            allocator,
            legacy_epoch_id,
            state.mode != .legacy_head_rewrite or state.legacy_epoch_preserved,
        );
        try sink.check(
            allocator,
            duplicate_serialized_id,
            state.mode != .duplicate_workers or state.duplicate_blocked,
        );
        try sink.check(allocator, recovery_id, state.complete and state.recovered);
        try sink.check(allocator, compacted_id, state.complete and state.compacted);
    }

    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        const state = world.state;
        return state.sim.healthSnapshot(.{
            .progress_expected = true,
            .progress_units = state.final_head + @popCount(state.visible_document_mask),
            .recovery_expected = state.mode != .clean,
            .recovery_complete = state.complete and state.recovered,
            .consistency_valid = !state.complete or
                (state.visible_document_mask == 3 and state.final_head == 3 and state.compacted),
            .cleanup_complete = state.complete and !state.runtime_live,
        });
    }

    pub fn done(world: *World) bool {
        return world.state.complete;
    }
};

test "complete serverless workflow VOPR exact replays claim build compact publish and recovery" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids) |mode_id| {
        var scripted = vopr.choice.Scripted{ .selections = &.{mode_id} };
        var recorded = try vopr.runner.run(
            Scenario,
            std.testing.allocator,
            scripted.source(),
            .{
                .system = "antfly",
                .transition_budget = 1,
                .backend_ids = &backend_ids,
                .source_revision = "serverless-workflow-vopr-v3",
                .target = "native",
                .optimize = @tagName(@import("builtin").mode),
            },
        );
        defer recorded.deinit();
        try std.testing.expectEqual(@as(u64, 0), recorded.summary.?.property_failures);
        for (0..10) |_| {
            var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &recorded);
            replayed.deinit();
        }
    }
}
