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

var owner_timing_gate: @import("bounded_diagnostic_gate.zig").Gate = .{};

fn readSource(host: anytype, comptime T: type, alloc: std.mem.Allocator, table: []const u8, request: wire.Request, context: operation.RequestContext) !T {
    const started = @import("antfly_platform").time.monotonicNs();
    defer {
        const elapsed = @import("antfly_platform").time.monotonicNs() -| started;
        if (elapsed >= 500 * std.time.ns_per_ms and owner_timing_gate.admit(@import("antfly_platform").time.monotonicNs()))
            std.log.info("rewrite source RPC operation={s} group={d} elapsed_ms={d}", .{ @tagName(request.operation), request.scope.fence.owner_group_id, elapsed / std.time.ns_per_ms });
    }
    const encoded = try host.executeRewriteSource(alloc, table, request, context);
    return std.json.parseFromSliceLeaky(T, alloc, encoded, .{ .allocate = .alloc_always });
}

/// The caller owns an arena for this slice; descriptor/string lifetimes never
/// escape the metadata command or owner RPC that copies them.
fn prepareSource(host: anytype, alloc: std.mem.Allocator, job: *std.json.Parsed(stages.Job), context: operation.RequestContext) !void {
    const Slot = struct {
        arena: std.heap.ArenaAllocator = .init(std.heap.page_allocator),
        target: stages.Target,
        scope: source.Scope,
        receipt: ?stages.SourceArtifact = null,
        failure: ?anyerror = null,
        fn run(slot: *@This(), h: @TypeOf(host), control: operation.RequestContext) void {
            slot.receipt = prepareOneSource(h, slot.arena.allocator(), slot.target, slot.scope, control) catch |err| {
                slot.failure = err;
                return;
            };
        }
    };
    var slots: [4]Slot = undefined;
    var count: usize = 0;
    const capability = context.fanout_io orelse context.deadline_io;
    for (job.value.plan.targets) |target| for (target.rewrite_sources) |scope| {
        const ready = for (target.source_artifacts) |item| {
            if (item.target_group_id == scope.fence.peer_group_id) break true;
        } else false;
        if (ready or count == (if (capability != null) @as(usize, 4) else 1)) continue;
        slots[count] = .{ .target = target, .scope = scope };
        count += 1;
    };
    defer for (slots[0..count]) |*slot| slot.arena.deinit();
    if (count != 0) {
        if (capability) |borrow| {
            var receiver = try borrow.receive();
            const io = receiver.io();
            var tasks: std.Io.Group = .init;
            for (slots[0..count]) |*slot| tasks.async(io, Slot.run, .{ slot, host, context });
            tasks.await(io) catch return error.Cancelled;
        } else Slot.run(&slots[0], host, context);
        try context.ensureActive();
        var ready_count: usize = 0;
        for (slots[0..count]) |slot| if (slot.receipt) |receipt| {
            try host.applyRewriteStagingCommand(job, .{ .id = job.value.plan.id, .expected_revision = job.value.revision, .action = .rewrite_source_ready, .source_artifact = receipt }, context);
            ready_count += 1;
        };
        for (slots[0..count]) |slot| if (slot.failure) |err| return err;
        if (ready_count == 0) return error.RestoreStagingWait;
        return;
    }
    var frozen = job.value.plan;
    frozen.preparing_sources = false;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly rewrite pinned cohort v1");
    hash.update(&try frozen.rewriteIntentDigest(alloc));
    for (frozen.targets) |target| for (target.source_artifacts) |item| hash.update(&try item.digest(alloc));
    hash.final(&frozen.cohort_digest);
    try host.applyRewriteStagingCommand(job, .{ .id = frozen.id, .expected_revision = job.value.revision, .action = .freeze_rewrite, .plan = frozen }, context);
}

fn prepareOneSource(host: anytype, alloc: std.mem.Allocator, target: stages.Target, scope: source.Scope, context: operation.RequestContext) !?stages.SourceArtifact {
    const status = try readSource(host, wire.SourceStatus, alloc, target.table.name, .{ .scope = scope, .operation = .{ .status = .donor } }, context);
    if (!std.meta.eql(status.scope, scope)) return error.RestoreStagingScopeChanged;
    if (status.progress == null) {
        try host.submitRewriteSource(target.table.name, .{ .online_source = .{ .admit = .{ .scope = scope, .limit = @import("../storage/retained_effects.zig").default_limit } } }, context);
        return null;
    }
    const progress = status.progress.?;
    if (progress.phase != .retaining) return error.RestoreStagingScopeChanged;
    if (progress.snapshot_phase != .published) {
        const certificate = try readSource(host, ?@import("../storage/source_snapshot.zig").Certificate, alloc, target.table.name, .{ .scope = scope, .operation = .publication }, context);
        if (certificate) |value| try host.submitRewriteSource(target.table.name, .{ .online_source = .{ .publish_certificate = .{ .scope = scope, .certificate = value } } }, context);
        return null;
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
    return receipt;
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
    if (job.value.state == .preparing_sources) {
        var arena = std.heap.ArenaAllocator.init(host.alloc);
        defer arena.deinit();
        return prepareSource(host, arena.allocator(), job, context);
    }
    if (job.value.state != .importing) return error.InvalidRestoreStagingCommand;
    const progress = worker.rewrite_progress;
    const started = @import("antfly_platform").time.monotonicNs();
    var rpc_ns: u64 = 0;
    var receipt_ns: u64 = 0;
    var checkpoint_ns: u64 = 0;
    defer {
        const elapsed = @import("antfly_platform").time.monotonicNs() -| started;
        if (elapsed >= 500 * std.time.ns_per_ms and owner_timing_gate.admit(@import("antfly_platform").time.monotonicNs()))
            std.log.info("rewrite owner wave phase={s} owner={d} elapsed_ms={d} rpc_ms={d} receipt_ms={d} cursor_ms={d}", .{ @tagName(progress.phase), progress.owner, elapsed / std.time.ns_per_ms, rpc_ns / std.time.ns_per_ms, receipt_ns / std.time.ns_per_ms, checkpoint_ns / std.time.ns_per_ms });
    }
    var count: u32 = 0;
    for (job.value.plan.targets) |target| count += @intCast(target.ranges.len);
    if (progress.phase == .complete or progress.owner >= count) return error.InvalidRestoreProgress;
    const width = 4;
    const Slot = struct {
        arena: std.heap.ArenaAllocator = .init(std.heap.page_allocator),
        result: ?OwnerResult = null,
        failure: ?anyerror = null,
        fn run(slot: *@This(), h: @TypeOf(host), plan: stages.Plan, digest: [32]u8, owner: u32, phase: @FieldType(jobs.RewriteProgress, "phase"), control: operation.RequestContext) void {
            var ordinal = owner;
            const selected = for (plan.targets) |target| {
                if (ordinal < target.ranges.len) break .{ .target = target, .range = target.ranges[ordinal] };
                ordinal -= @intCast(target.ranges.len);
            } else {
                slot.failure = error.InvalidRestoreProgress;
                return;
            };
            slot.result = advanceOwner(h, slot.arena.allocator(), plan, digest, selected.target, selected.range, phase, control) catch |err| {
                slot.failure = err;
                return;
            };
        }
    };
    var slots: [width]Slot = @splat(.{});
    defer for (&slots) |*slot| slot.arena.deinit();
    const borrow = context.fanout_io orelse context.deadline_io;
    const wave: u32 = @min(if (borrow != null) @as(u32, width) else 1, count - progress.owner);
    // Never cross a phase/pass boundary within a concurrent wave. All owner
    // receipts are joined before metadata or scheduling state is changed.
    if (borrow) |capability| {
        var receiver = try capability.receive();
        const io = receiver.io();
        var tasks: std.Io.Group = .init;
        for (slots[0..wave], 0..) |*slot, i| tasks.async(io, Slot.run, .{ slot, host, job.value.plan, job.value.plan_digest, progress.owner + @as(u32, @intCast(i)), progress.phase, context });
        tasks.await(io) catch return error.Cancelled;
    } else Slot.run(&slots[0], host, job.value.plan, job.value.plan_digest, progress.owner, progress.phase, context);
    rpc_ns = @import("antfly_platform").time.monotonicNs() -| started;
    try context.ensureActive();
    const receipts_started = @import("antfly_platform").time.monotonicNs();
    // A successful owner has durable evidence even when a sibling loses its
    // reply. Record those receipts first; replay the same fenced wave on error.
    for (slots[0..wave]) |slot| if (slot.result) |result| if (result.receipt) |receipt| {
        try host.applyRewriteStagingCommand(job, .{ .id = job.value.plan.id, .expected_revision = job.value.revision, .action = .imported, .receipt = receipt }, context);
    };
    receipt_ns = @import("antfly_platform").time.monotonicNs() -| receipts_started;
    for (slots[0..wave]) |slot| if (slot.failure) |err| return err;
    if (job.value.state == .importing) {
        var next = progress;
        for (slots[0..wave]) |slot| next = try checkpointAfter(next, slot.result.?.pending, count);
        const checkpoint_started = @import("antfly_platform").time.monotonicNs();
        defer checkpoint_ns = @import("antfly_platform").time.monotonicNs() -| checkpoint_started;
        const saved = try host.restore_job_store.recordRewriteProgress(host.alloc, worker.job_id, worker.attempt_id, next);
        host.alloc.free(saved);
        worker.rewrite_progress = next;
    }
}

const OwnerResult = struct { pending: bool, receipt: ?stages.OwnerReceipt = null };

fn advanceOwner(host: anytype, alloc: std.mem.Allocator, plan: stages.Plan, plan_digest: [32]u8, target_arg: stages.Target, range_arg: @import("../metadata/table_manager.zig").RangeRecord, phase: @FieldType(jobs.RewriteProgress, "phase"), context: operation.RequestContext) !OwnerResult {
    const target = target_arg;
    const range = range_arg;
    const scope = try stages.ownerScope(alloc, plan, plan_digest, target, range);
    const binding = scope.rewrite orelse return error.InvalidRestoreStagingCommand;
    const source_scope = binding.source_scope.?;
    const source_status = try readSource(host, wire.SourceStatus, alloc, target.table.name, .{ .scope = source_scope, .operation = .{ .status = .donor } }, context);
    if (!std.meta.eql(source_status.scope, source_scope) or source_status.progress == null or source_status.progress.?.phase == .released) return error.RestoreStagingScopeChanged;
    var pending = false;
    var receipt: ?stages.OwnerReceipt = null;
    switch (phase) {
        .snapshot => {
            // Import admits a reserved owner and returns authoritative progress
            // itself. A separate begin/status probe on every page duplicates
            // the owner and metadata ReadIndex barriers, including after replay.
            const source_receipt = for (target.source_artifacts) |item| {
                if (item.target_group_id == range.group_id) break item;
            } else return error.RestoreSourceProofMissing;
            const published = source_status.progress.?.published_certificate orelse return error.RestoreSourceProofMissing;
            const descriptor: artifact.Descriptor = .{ .scope = source_scope, .certificate = published, .total_bytes = source_receipt.artifact_size_bytes };
            var request: owners.Request = .{ .scope = scope, .action = .import_page, .rewrite = target.rewrite, .source = .{ .location = "", .artifact = source_receipt, .peer_descriptor = descriptor } };
            var response = try host.executeRestoreOwner(alloc, target.table.name, range.group_id, request, context);
            if (!response.rewrite.?.snapshot_complete and response.source_next_offset < descriptor.total_bytes) {
                request.source_chunk = try readSource(host, artifact.ReadResponse, alloc, target.table.name, .{ .scope = source_scope, .operation = .{ .artifact = .{ .read = .{ .descriptor = descriptor, .offset = response.source_next_offset } } } }, context);
                response = try host.executeRestoreOwner(alloc, target.table.name, range.group_id, request, context);
            }
            pending = !response.rewrite.?.snapshot_complete;
        },
        .catchup => {
            const status = try host.executeRestoreOwner(alloc, target.table.name, range.group_id, .{ .scope = scope, .action = .status }, context);
            pending = try transferTail(host, alloc, target, scope, status, source_status, context);
        },
        .fencing => {
            if (source_status.fence == null) {
                try host.submitRewriteSource(target.table.name, .{ .relational_topology = .{ .fence = source_scope.fence, .action = .begin, .generation_handoff = if (target.generation_handoffs.len != 0) .{ .plan_id = plan.id, .plan_digest = plan_digest } else null } }, context);
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
                    receipt = .{ .group_id = range.group_id, .range_id = range.range_id, .plan_digest = plan_digest, .completion_digest = result.receipt };
                }
            }
        },
        .complete => unreachable,
    }
    return .{ .pending = pending, .receipt = receipt };
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
    try testOwnerWaves(false);
}

test "rewrite shared job concurrent wave joins receipts and consolidates lost cursor checkpoints" {
    try testOwnerWaves(true);
}

fn testOwnerWaves(concurrent: bool) !void {
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
        fences: std.atomic.Value(usize) = .init(0),
        final_cuts: std.atomic.Value(usize) = .init(0),
        snapshot_started: std.atomic.Value(bool) = .init(false),
        snapshot_complete: bool = false,
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
                if (self.sources[ordinal].fence == null) _ = self.fences.fetchAdd(1, .monotonic);
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
                    try std.testing.expectEqual(@as(usize, 2), self.fences.load(.acquire));
                    progress.phase = .fenced;
                    progress.through_sequence = fence.expected_sequence;
                    progress.applied_index = 10;
                    progress.cut_digest = source.finalCutDigest(fence.scope, fence.expected_sequence, 10);
                    _ = self.final_cuts.fetchAdd(1, .monotonic);
                },
                else => return error.TestUnexpectedResult,
            }
        }
        pub fn executeRestoreOwner(self: *@This(), _: std.mem.Allocator, _: []const u8, group: u64, request: owners.Request, _: operation.RequestContext) !owners.Response {
            try request.validate(group);
            const ordinal: usize = @intCast(group - 401);
            const target = &self.targets[ordinal];
            switch (request.action) {
                .begin => return error.TestUnexpectedResult,
                .status => {
                    // Snapshot pages must use the durable progress returned
                    // by import, not issue a redundant status/ReadIndex RPC.
                    try std.testing.expect(self.snapshot_complete);
                },
                .import_page => {
                    if (request.source != null) self.snapshot_started.store(true, .release);
                    if (request.source_chunk != null) {
                        target.rewrite.?.snapshot_complete = true;
                    } else if (request.rewrite_tail != null) {
                        target.rewrite.?.sequence = request.rewrite_tail.?.sequence;
                    } else if (request.rewrite_finish) |final| {
                        try std.testing.expectEqual(@as(usize, 2), self.fences.load(.acquire));
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
    var threaded: std.Io.Threaded = .init(fixture.alloc, .{ .async_limit = .limited(4) });
    defer threaded.deinit();
    const io = threaded.io();
    const control: operation.RequestContext = if (concurrent) .{ .fanout_io = @import("../runtime_io_abi.zig").Borrow.init(&io) } else .{};
    for (0..100) |iteration| {
        worker.rewrite_progress = fixture.restore_job_store.progress;
        fixture.restore_job_store.lose_checkpoint = iteration % 7 == 0;
        const before = worker.rewrite_progress;
        fixture.snapshot_complete = before.phase != .snapshot;
        step(&fixture, &job, &worker, control) catch |err| {
            if (err != error.RestoreStagingYield) return err;
            try std.testing.expectEqualDeep(before, worker.rewrite_progress);
        };
        try std.testing.expectEqualDeep(fixture.restore_job_store.progress, worker.rewrite_progress);
        if (job.value.state == .validating) break;
    } else return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 2), fixture.final_cuts.load(.acquire));
    try std.testing.expect(fixture.snapshot_started.load(.acquire));
    for (fixture.targets) |target| try std.testing.expectEqual(.imported, target.phase);
}
