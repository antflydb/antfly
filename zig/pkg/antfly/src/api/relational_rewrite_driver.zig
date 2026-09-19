// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Scheduling slices of the existing restore job, not a second job system.
//! Source receipts, target progress and final cuts are durable independently
//! of this scheduling cursor. Every step performs bounded owner work.
const std = @import("std");
const stages = @import("../metadata/restore_staging.zig");
const owners = @import("restore_owner_contract.zig");
const jobs = @import("restore_jobs.zig");
const operation = @import("operation.zig");
const wire = @import("../storage/db/online_merge_io_contract.zig");
const source = @import("../storage/db/online_source_contract.zig");
const rewrite = @import("../storage/db/relational_rewrite_contract.zig");
const artifact = @import("../storage/db/source_artifact_transfer.zig");

fn readSource(host: anytype, comptime T: type, alloc: std.mem.Allocator, table: []const u8, request: wire.Request, context: operation.RequestContext) !T {
    const encoded = try host.executeRewriteSource(alloc, table, request, context);
    return std.json.parseFromSliceLeaky(T, alloc, encoded, .{ .allocate = .alloc_always });
}

/// The caller owns an arena for this slice; descriptor/string lifetimes never
/// escape the metadata command or owner RPC that copies them.
fn prepareSource(host: anytype, alloc: std.mem.Allocator, job: *std.json.Parsed(stages.Job), context: operation.RequestContext) !void {
    for (job.value.plan.targets) |target| for (target.rewrite_sources) |scope| {
        const ready = for (target.source_artifacts) |item| {
            if (item.target_group_id == scope.fence.peer_group_id) break true;
        } else false;
        if (ready) continue;
        const status = try readSource(host, wire.SourceStatus, alloc, target.table.name, .{ .scope = scope, .operation = .{ .status = .donor } }, context);
        if (status.progress == null) {
            try host.submitRewriteSource(target.table.name, .{ .online_source = .{ .admit = .{ .scope = scope, .limit = @import("../storage/retained_effects.zig").default_limit } } }, context);
            return;
        }
        const progress = status.progress.?;
        if (progress.phase != .retaining) return error.RestoreStagingScopeChanged;
        if (progress.snapshot_phase != .published) {
            const certificate = try readSource(host, ?@import("../storage/source_snapshot.zig").Certificate, alloc, target.table.name, .{ .scope = scope, .operation = .publication }, context);
            if (certificate) |value| try host.submitRewriteSource(target.table.name, .{ .online_source = .{ .publish_certificate = .{ .scope = scope, .certificate = value } } }, context);
            return;
        }
        const descriptor = try readSource(host, artifact.Descriptor, alloc, target.table.name, .{ .scope = scope, .operation = .{ .artifact = .{ .describe = scope } } }, context);
        const digest = try descriptor.certificate.digest();
        if (!std.meta.eql(scope, descriptor.scope) or !std.mem.eql(u8, &digest, &progress.snapshot_certificate)) return error.RestoreStagingScopeChanged;
        const receipt: stages.SourceArtifact = .{
            .target_group_id = scope.fence.peer_group_id,
            .source_namespace = scope.fence.namespace,
            .format = .portable,
            .snapshot_path = "source.afb2",
            .artifact_size_bytes = descriptor.total_bytes,
            // Explicit source-copy mode binds the verified logical certificate;
            // ordinary repository artifacts continue using a byte SHA256.
            .artifact_sha256 = digest,
            .rewrite = .{ .program_digest = target.rewrite.?.program_digest, .retained_pin = scope.pin(), .snapshot_certificate = digest, .retained_epoch = scope.consumer_epoch, .retained_start = progress.start, .source_applied_index = progress.admitted_applied_index, .source_scope = scope },
        };
        try host.applyRewriteStagingCommand(job, .{ .id = job.value.plan.id, .expected_revision = job.value.revision, .action = .rewrite_source_ready, .source_artifact = receipt }, context);
        return;
    };
    var frozen = job.value.plan;
    frozen.preparing_sources = false;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly rewrite pinned cohort v1");
    hash.update(&try frozen.rewriteIntentDigest(alloc));
    for (frozen.targets) |target| for (target.source_artifacts) |item| hash.update(&try item.digest(alloc));
    hash.final(&frozen.cohort_digest);
    try host.applyRewriteStagingCommand(job, .{ .id = frozen.id, .expected_revision = job.value.revision, .action = .freeze_rewrite, .plan = frozen }, context);
}

fn checkpointAfter(progress: jobs.RewriteProgress, pending: bool, count: u32) !jobs.RewriteProgress {
    var next = progress;
    next.pending = next.pending or pending;
    next.owner += 1;
    if (next.owner == count) {
        next.owner = 0;
        next.round = try std.math.add(u64, next.round, 1);
        if (!next.pending) next.phase = @enumFromInt(@intFromEnum(next.phase) + 1);
        next.pending = false;
    }
    return next;
}

fn transferTail(host: anytype, alloc: std.mem.Allocator, target: stages.Target, scope: @import("../storage/db/restore_staging_contract.zig").Scope, status: owners.Response, source_status: wire.SourceStatus, context: operation.RequestContext) !bool {
    const progress = status.rewrite orelse return error.RestoreStagingScopeChanged;
    const source_scope = scope.rewrite.?.source_scope.?;
    const source_progress = source_status.progress orelse return error.RestoreSourceProofMissing;
    const through = if (source_progress.phase == .fenced) source_progress.through_sequence else source_status.retained_head;
    if (progress.sequence > through) return error.RestoreStagingScopeChanged;
    if (progress.sequence == through) {
        try acknowledgeAndReclaim(host, target.table.name, source_scope, source_progress.acknowledged, progress.sequence, context);
        return false;
    }
    const chunk = (try readSource(host, ?rewrite.TailChunk, alloc, target.table.name, .{ .scope = source_scope, .operation = .{ .rewrite_tail = .{ .after = progress.sequence, .offset = status.tail_next } } }, context)) orelse return error.RestoreValidationPending;
    const result = try host.executeRestoreOwner(alloc, target.table.name, scope.target_namespace.shard_id, .{ .scope = scope, .action = .import_page, .rewrite = target.rewrite, .rewrite_tail = chunk }, context);
    const applied = result.rewrite orelse return error.RestoreStagingScopeChanged;
    if (applied.sequence > progress.sequence) try acknowledgeAndReclaim(host, target.table.name, source_scope, source_progress.acknowledged, applied.sequence, context);
    return applied.sequence != through;
}

fn acknowledgeAndReclaim(host: anytype, table: []const u8, scope: source.Scope, previous: u64, next: u64, context: operation.RequestContext) !void {
    if (next > previous) try host.submitRewriteSource(table, .{ .online_source = .{ .acknowledge = .{ .scope = scope, .previous = previous, .next = next } } }, context);
    // ACK advances the safe floor, not physical byte accounting. GC is an
    // explicit bounded replicated step; retries also drain a lost GC reply.
    try host.submitRewriteSource(table, .{ .online_source = .{ .reclaim = .{ .scope = scope, .frame_limit = 128, .byte_limit = 16 * 1024 * 1024 } } }, context);
}

/// Snapshot and catchup remain writable. Only after an entire catchup pass
/// completes do we fence/drain the full cohort, then consume exact final cuts.
/// The ordinary shared validation/publication worker takes over afterward.
pub fn step(host: anytype, job: *std.json.Parsed(stages.Job), worker: *jobs.JobState, context: operation.RequestContext) !void {
    var arena = std.heap.ArenaAllocator.init(host.alloc);
    defer arena.deinit();
    const alloc = arena.allocator();
    if (job.value.state == .preparing_sources) return prepareSource(host, alloc, job, context);
    if (job.value.state != .importing) return error.InvalidRestoreStagingCommand;
    const progress = worker.rewrite_progress;
    var count: u32 = 0;
    for (job.value.plan.targets) |target| count += @intCast(target.ranges.len);
    if (progress.phase == .complete or progress.owner >= count) return error.InvalidRestoreProgress;
    var ordinal = progress.owner;
    const selected = for (job.value.plan.targets) |target| {
        if (ordinal < target.ranges.len) break .{ .target = target, .range = target.ranges[ordinal] };
        ordinal -= @intCast(target.ranges.len);
    } else return error.InvalidRestoreProgress;
    const target = selected.target;
    const range = selected.range;
    const scope = try stages.ownerScope(alloc, job.value.plan, job.value.plan_digest, target, range);
    const binding = scope.rewrite orelse return error.InvalidRestoreStagingCommand;
    const source_scope = binding.source_scope.?;
    const source_status = try readSource(host, wire.SourceStatus, alloc, target.table.name, .{ .scope = source_scope, .operation = .{ .status = .donor } }, context);
    if (!std.meta.eql(source_status.scope, source_scope) or source_status.progress == null or source_status.progress.?.phase == .released) return error.RestoreStagingScopeChanged;
    var pending = false;
    switch (progress.phase) {
        .snapshot => {
            _ = try host.executeRestoreOwner(alloc, target.table.name, range.group_id, .{ .scope = scope, .action = .begin }, context);
            const before = try host.executeRestoreOwner(alloc, target.table.name, range.group_id, .{ .scope = scope, .action = .status }, context);
            if (!(before.rewrite orelse return error.RestoreStagingScopeChanged).snapshot_complete) {
                const receipt = for (target.source_artifacts) |item| {
                    if (item.target_group_id == range.group_id) break item;
                } else return error.RestoreSourceProofMissing;
                const published = source_status.progress.?.published_certificate orelse return error.RestoreSourceProofMissing;
                const descriptor: artifact.Descriptor = .{ .scope = source_scope, .certificate = published, .total_bytes = receipt.artifact_size_bytes };
                var request: owners.Request = .{ .scope = scope, .action = .import_page, .rewrite = target.rewrite, .source = .{ .location = "", .artifact = receipt, .peer_descriptor = descriptor } };
                var response = try host.executeRestoreOwner(alloc, target.table.name, range.group_id, request, context);
                if (!response.rewrite.?.snapshot_complete and response.source_next_offset < descriptor.total_bytes) {
                    request.source_chunk = try readSource(host, artifact.ReadResponse, alloc, target.table.name, .{ .scope = source_scope, .operation = .{ .artifact = .{ .read = .{ .descriptor = descriptor, .offset = response.source_next_offset } } } }, context);
                    response = try host.executeRestoreOwner(alloc, target.table.name, range.group_id, request, context);
                }
                pending = !response.rewrite.?.snapshot_complete;
            }
        },
        .catchup => {
            const status = try host.executeRestoreOwner(alloc, target.table.name, range.group_id, .{ .scope = scope, .action = .status }, context);
            pending = try transferTail(host, alloc, target, scope, status, source_status, context);
        },
        .fencing => {
            if (source_status.fence == null) {
                try host.submitRewriteSource(target.table.name, .{ .relational_topology = .{ .fence = source_scope.fence, .action = .begin } }, context);
                pending = true;
            } else {
                if (!source_status.fence.?.eql(source_scope.fence)) return error.RestoreStagingScopeChanged;
                pending = !source_status.drained;
            }
        },
        .tail => {
            const source_progress = source_status.progress orelse return error.RestoreSourceProofMissing;
            if (source_status.fence == null or !source_status.fence.?.eql(source_scope.fence) or !source_status.drained) return error.RestoreStagingScopeChanged;
            if (source_progress.phase != .fenced) {
                try host.submitRewriteSource(target.table.name, .{ .online_source = .{ .final_fence = .{ .scope = source_scope, .expected_sequence = source_status.retained_head } } }, context);
                pending = true;
            } else {
                const status = try host.executeRestoreOwner(alloc, target.table.name, range.group_id, .{ .scope = scope, .action = .status }, context);
                pending = try transferTail(host, alloc, target, scope, status, source_status, context);
                if (!pending) {
                    const final_receipt = try rewrite.FinalReceipt.fromProgress(binding, source_progress);
                    const result = try host.executeRestoreOwner(alloc, target.table.name, range.group_id, .{ .scope = scope, .action = .import_page, .rewrite = target.rewrite, .rewrite_finish = final_receipt }, context);
                    if (result.phase != .imported) return error.RestoreStagingScopeChanged;
                    try host.applyRewriteStagingCommand(job, .{ .id = job.value.plan.id, .expected_revision = job.value.revision, .action = .imported, .receipt = .{ .group_id = range.group_id, .range_id = range.range_id, .plan_digest = job.value.plan_digest, .completion_digest = result.receipt } }, context);
                }
            }
        },
        .complete => unreachable,
    }
    if (job.value.state == .importing) {
        const next = try checkpointAfter(progress, pending, count);
        const saved = try host.restore_job_store.recordRewriteProgress(host.alloc, worker.job_id, worker.attempt_id, next);
        host.alloc.free(saved);
        // Publish to the current scheduling burst only after durable success.
        // A lost checkpoint reply leaves this cursor unchanged for replay.
        worker.rewrite_progress = next;
    }
}

/// An unfinished complete pass must yield to asynchronous owner work. Within
/// a pass, advance other owners without paying one job retry per owner RPC.
pub fn completedPendingPass(before: jobs.RewriteProgress, after: jobs.RewriteProgress) bool {
    return before.phase == after.phase and after.round > before.round;
}

test "rewrite shared job scheduler fences only after complete catchup pass" {
    var progress: jobs.RewriteProgress = .{ .phase = .catchup };
    progress = try checkpointAfter(progress, false, 2);
    try std.testing.expectEqual(.catchup, progress.phase);
    const before_pending = progress;
    progress = try checkpointAfter(progress, true, 2);
    try std.testing.expect(completedPendingPass(before_pending, progress));
    try std.testing.expectEqual(.catchup, progress.phase);
    try std.testing.expectEqual(@as(u64, 1), progress.round);
    progress = try checkpointAfter(progress, false, 2);
    const before_fencing = progress;
    progress = try checkpointAfter(progress, false, 2);
    try std.testing.expect(!completedPendingPass(before_fencing, progress));
    try std.testing.expectEqual(.fencing, progress.phase);
    try std.testing.expectEqual(@as(u32, 0), progress.owner);
}

test "rewrite shared job driver resumes lost scheduling receipts and fences whole cohort before final cuts" {
    const Fixture = struct {
        const Store = struct {
            progress: jobs.RewriteProgress = .{},
            lose_checkpoint: bool = false,
            pub fn recordRewriteProgress(self: *@This(), alloc: std.mem.Allocator, _: u64, _: u64, value: jobs.RewriteProgress) ![]u8 {
                if (self.lose_checkpoint) {
                    self.lose_checkpoint = false;
                    return error.RestoreStagingYield;
                }
                self.progress = value;
                return alloc.dupe(u8, "{}");
            }
        };
        alloc: std.mem.Allocator,
        restore_job_store: Store = .{},
        sources: [2]wire.SourceStatus,
        targets: [2]owners.Response = @splat(.{ .phase = .importing, .rows = 0, .receipt = @splat(2), .rewrite = .{ .sequence = 0 } }),
        imported: [2]bool = @splat(false),
        fences: usize = 0,
        final_cuts: usize = 0,
        pub fn executeRewriteSource(self: *@This(), alloc: std.mem.Allocator, _: []const u8, request: wire.Request, _: operation.RequestContext) ![]u8 {
            const ordinal: usize = @intCast(request.scope.fence.owner_group_id - 301);
            try request.validate();
            return switch (request.operation) {
                .status => std.json.Stringify.valueAlloc(alloc, self.sources[ordinal], .{}),
                .artifact => |value| switch (value) {
                    .read => |read| blk: {
                        try std.testing.expectEqual(@as(u64, 0), read.offset);
                        break :blk std.json.Stringify.valueAlloc(alloc, artifact.ReadResponse{ .offset = 0, .data_base64 = "eA==", .digest = @splat(1) }, .{});
                    },
                    else => error.TestUnexpectedResult,
                },
                .rewrite_tail => |page| blk: {
                    try std.testing.expectEqual(@as(u64, 0), page.after);
                    break :blk std.json.Stringify.valueAlloc(alloc, @as(?rewrite.TailChunk, .{ .pin = request.scope.pin(), .sequence = 1, .frame_digest = @splat(1), .total = 1, .offset = 0, .data = "x" }), .{});
                },
                else => error.TestUnexpectedResult,
            };
        }
        pub fn submitRewriteSource(self: *@This(), _: []const u8, request: @import("../storage/db/types.zig").BatchRequest, _: operation.RequestContext) !void {
            if (request.relational_topology) |topology| {
                try std.testing.expectEqual(.begin, topology.action);
                for (self.targets) |target| {
                    try std.testing.expect(target.rewrite.?.snapshot_complete);
                    try std.testing.expectEqual(@as(u64, 1), target.rewrite.?.sequence);
                }
                const ordinal: usize = @intCast(topology.fence.owner_group_id - 301);
                if (self.sources[ordinal].fence == null) self.fences += 1;
                self.sources[ordinal].fence = topology.fence;
                self.sources[ordinal].drained = true;
                return;
            }
            const command = request.online_source.?;
            const ordinal: usize = @intCast(command.scope().fence.owner_group_id - 301);
            const progress = &self.sources[ordinal].progress.?;
            switch (command) {
                .acknowledge => |ack| {
                    try std.testing.expectEqual(progress.acknowledged, ack.previous);
                    progress.acknowledged = ack.next;
                },
                .reclaim => {},
                .final_fence => |fence| {
                    // A final source cut cannot precede ANY other owner fence.
                    try std.testing.expectEqual(@as(usize, 2), self.fences);
                    progress.phase = .fenced;
                    progress.through_sequence = fence.expected_sequence;
                    progress.applied_index = 10;
                    progress.cut_digest = source.finalCutDigest(fence.scope, fence.expected_sequence, 10);
                    self.final_cuts += 1;
                },
                else => return error.TestUnexpectedResult,
            }
        }
        pub fn executeRestoreOwner(self: *@This(), _: std.mem.Allocator, _: []const u8, group: u64, request: owners.Request, _: operation.RequestContext) !owners.Response {
            try request.validate(group);
            const ordinal: usize = @intCast(group - 401);
            const target = &self.targets[ordinal];
            switch (request.action) {
                .begin, .status => {},
                .import_page => {
                    if (request.source_chunk != null) {
                        target.rewrite.?.snapshot_complete = true;
                    } else if (request.rewrite_tail != null) {
                        target.rewrite.?.sequence = request.rewrite_tail.?.sequence;
                    } else if (request.rewrite_finish) |final| {
                        try std.testing.expectEqual(@as(usize, 2), self.fences);
                        try std.testing.expectEqual(final.cut.sequence, target.rewrite.?.sequence);
                        target.phase = .imported;
                        target.rewrite.?.final_cut = final.cut;
                    }
                },
                else => return error.TestUnexpectedResult,
            }
            return target.*;
        }
        pub fn applyRewriteStagingCommand(self: *@This(), job: *std.json.Parsed(stages.Job), command: stages.Command, _: operation.RequestContext) !void {
            try std.testing.expectEqual(.imported, command.action);
            const ordinal: usize = @intCast(command.receipt.?.group_id - 401);
            self.imported[ordinal] = true;
            if (self.imported[0] and self.imported[1]) job.value.state = .validating;
        }
    };
    var fixture: Fixture = .{ .alloc = std.testing.allocator, .sources = undefined };
    var definitions: [2]stages.SourceArtifact = undefined;
    var targets: [2]stages.Target = undefined;
    var ranges: [2]@import("../metadata/table_manager.zig").RangeRecord = undefined;
    for (0..2) |ordinal| {
        const scope: source.Scope = .{ .fence = .{ .role = .rewrite_source, .transition_id = 1, .attempt = 1, .admission_epoch = 1, .owner_group_id = 301 + ordinal, .peer_group_id = 401 + ordinal, .namespace = .{ .table_id = 11 + ordinal, .shard_id = 301 + ordinal, .range_id = 301 + ordinal }, .catalog_digest = @splat(1) }, .receiver_namespace = .{ .table_id = 21 + ordinal, .shard_id = 401 + ordinal, .range_id = 401 + ordinal }, .consumer_epoch = 1, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } };
        const certificate: @import("../storage/source_snapshot.zig").Certificate = .{ .cut = .{ .namespace = scope.fence.namespace, .applied_index = 2, .retained_start = 0 }, .objects = 1, .content_bytes = 1, .schema_manifest_digest = @splat(1), .ordered_content_digest = @splat(2) };
        const digest = try certificate.digest();
        fixture.sources[ordinal] = .{ .scope = scope, .certificate = certificate, .progress = .{ .namespace = scope.namespace(), .consumer_epoch = 1, .pin = scope.pin(), .start = 0, .acknowledged = 0, .admitted_applied_index = 2, .snapshot_certificate = digest, .published_certificate = certificate, .snapshot_phase = .published }, .retained_head = 1, .fence = null, .next_epoch = 1, .drained = false, .row_derived_indexes = true };
        definitions[ordinal] = .{ .target_group_id = 401 + ordinal, .source_namespace = scope.fence.namespace, .format = .portable, .snapshot_path = "source.afb2", .artifact_size_bytes = 1, .artifact_sha256 = digest, .rewrite = .{ .program_digest = @splat(9), .retained_pin = scope.pin(), .snapshot_certificate = digest, .retained_epoch = 1, .retained_start = 0, .source_applied_index = 2, .source_scope = scope } };
        ranges[ordinal] = .{ .table_id = 21 + ordinal, .group_id = 401 + ordinal, .range_id = 401 + ordinal, .doc_identity_shard_id = 401 + ordinal, .doc_identity_range_id = 401 + ordinal, .start_key = "" };
        targets[ordinal] = .{ .source_table_id = 11 + ordinal, .table = .{ .table_id = 21 + ordinal, .name = if (ordinal == 0) "first" else "second", .schema_json = "{}" }, .ranges = ranges[ordinal..][0..1], .source_artifacts = definitions[ordinal..][0..1], .rewrite = .{ .preserve_document = true, .source_schemas = &.{"{}"}, .target_schema = "{}", .program_digest = @splat(9) } };
    }
    const initial = try std.json.Stringify.valueAlloc(fixture.alloc, stages.Job{ .plan = .{ .id = try stages.idForAttempt(1, 1), .cohort_digest = @splat(3), .targets = &targets }, .plan_digest = @splat(4) }, .{});
    defer fixture.alloc.free(initial);
    var job = try std.json.parseFromSlice(stages.Job, fixture.alloc, initial, .{ .allocate = .alloc_always });
    defer job.deinit();
    var worker = std.mem.zeroes(jobs.JobState);
    worker.job_id = 1;
    worker.attempt_id = 1;
    for (0..100) |iteration| {
        worker.rewrite_progress = fixture.restore_job_store.progress;
        fixture.restore_job_store.lose_checkpoint = iteration % 7 == 0;
        const before = worker.rewrite_progress;
        step(&fixture, &job, &worker, .{}) catch |err| {
            if (err != error.RestoreStagingYield) return err;
            try std.testing.expectEqualDeep(before, worker.rewrite_progress);
        };
        try std.testing.expectEqualDeep(fixture.restore_job_store.progress, worker.rewrite_progress);
        if (job.value.state == .validating) break;
    } else return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 2), fixture.final_cuts);
    for (fixture.targets) |target| try std.testing.expectEqual(.imported, target.phase);
}
