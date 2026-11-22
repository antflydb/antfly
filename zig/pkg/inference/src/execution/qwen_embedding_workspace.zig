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

//! Physical admission, receiving, and retirement for bounded Qwen embedding
//! workspaces. The caller supplies token packing and embedding publication.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const architecture = @import("architecture_session.zig");
const cleanup_mod = @import("cleanup_context.zig");
const session_mod = @import("../backends/session.zig");
const execution_control = @import("../execution_control.zig");
const memory_admission = @import("../runtime/tier/memory.zig");
const Waiter = @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter;
const roles = @import("../backends/metal_workspace_roles.zig");
const ownership = @import("../backends/metal_workspace_ownership.zig");
const Metal = @import("../ops/metal_compute.zig").MetalCompute;
const ops_mod = @import("../ops/ops.zig");
const decoder_gated_runtime = @import("../backends/decoder_gated_runtime.zig");
const embedding_trace = @import("../embedding_trace.zig");

pub const Options = struct {
    allocator: std.mem.Allocator,
    session: session_mod.Session,
    control: ?execution_control.InferenceExecutionControl,
    execution_lock: ?*std.atomic.Mutex,
    execution_gate_held: *bool,
    batch: usize,
    max_sequence: usize,
    pooling: enum { last, mean, unsupported },
    normalize: bool,
    measure_lock: bool,
};

const NumericalScope = struct {
    extra_gate: ?*std.atomic.Mutex,
    lane: ?execution_control.ExecutionLane.Lease,

    fn acquire(options: Options, selected: *Waiter, body: anytype) !NumericalScope {
        const session = options.session;
        try session.acquireSelectedCohort(options.control orelse .{}, selected);
        errdefer session.releaseSelectedCohortGate(selected);
        const gate = session.execution_gate orelse options.execution_lock;
        const extra_gate = if (options.execution_lock != gate) options.execution_lock else null;
        if (extra_gate) |extra| {
            if (options.control) |control| try control.lock(extra) else platform.sync.lockYielding(extra);
        }
        errdefer if (extra_gate) |extra| extra.unlock();
        const lane = try body.enterLane();
        options.execution_gate_held.* = true;
        return .{ .extra_gate = extra_gate, .lane = lane };
    }

    fn release(self: NumericalScope, options: Options, selected: *Waiter, body: anytype) void {
        options.execution_gate_held.* = false;
        body.leaveLane(self.lane);
        if (self.extra_gate) |extra| extra.unlock();
        options.session.releaseSelectedCohortGate(selected);
    }
};

pub fn hostDeclarationBytes(cold_capacity: usize, fresh_count: usize, receiving_count: usize, selected_metadata: usize, cpu_bytes: usize) !usize {
    const final_capacity = try std.math.add(usize, cold_capacity, fresh_count);
    const cold_arrays = try std.math.mul(usize, 2, try roles.Snapshot.requestedMetadataBytes(cold_capacity));
    const final_arrays = try roles.Snapshot.requestedMetadataBytes(final_capacity);
    const roots = try std.math.mul(usize, try std.math.add(usize, cold_capacity, final_capacity), @sizeOf(*ownership.Root));
    const bank_all = try ownership.UseBank.requestedMetadataBytes(receiving_count);
    // Receiving Fences are in the shared persistent metadata charge;
    // Entry arrays are synchronous and remain in the original permit.
    const bank_entries = bank_all - try std.math.mul(usize, receiving_count, @sizeOf(@import("../backends/metal_workspace_fence.zig").Fence));
    var bytes = try std.math.add(usize, try ownership.Phase.requestedPersistentMetadataBytes(fresh_count, receiving_count), bank_entries);
    inline for (.{ cold_arrays, final_arrays, roots, selected_metadata, cpu_bytes }) |part| bytes = try std.math.add(usize, bytes, part);
    return bytes;
}

fn admitWorkspace(session: session_mod.Session, request: session_mod.RunRequest, device_bytes: usize, host_bytes: usize, held_output: ?session_mod.HostOutputProducer.HeldAdmission, pressure: *?memory_admission.AdmissionPressure) !session_mod.RunPermit {
    var declarations: [2]@import("prepared_storage_admission.zig").Requirement = undefined;
    var count: usize = 0;
    if (device_bytes != 0) {
        declarations[count] = .{ .backend = .metal, .class = .workspace, .bytes = device_bytes };
        count += 1;
    }
    if (host_bytes != 0) {
        declarations[count] = .{ .backend = .native, .class = .host_workspace, .bytes = host_bytes };
        count += 1;
    }
    return session.admitDeclaredStorageWithHeldHostOutputWithPressure(request, declarations[0..count], held_output, pressure);
}

/// First-cold family publication. This is a pre-numerical transaction:
/// every supplier/metadata operation precedes the actual cohort/Model/lane,
/// then the original family preparer creates the SAME scratch/token banks
/// through their individually funded Root slots. Its published immutable
/// formats are inspected only after all numerical/provider owners leave.
/// A caller must rebuild its cold selected plan after this transaction.
fn prepareFamily(
    options: Options,
    stage: anytype,
    request: session_mod.RunRequest,
    cleanup: cleanup_mod.Context,
    pressure: *?memory_admission.AdmissionPressure,
    selected: *Waiter,
    held_output: ?session_mod.HostOutputProducer.HeldAdmission,
) anyerror!void {
    const session = options.session;
    if (session.run_admission == null) return error.MissingRunAdmission;
    const epoch = cleanup.boundary orelse return error.MissingCurrentCleanupEpoch;
    const cfg = architecture.getGptConfig(session) orelse return error.UnsupportedResidentTextEncoder;
    const process = platform.allocator.processAllocator(platform.allocator.concurrentFallback());
    // Constructor-only preparation retains the original complete request
    // floor and cannot call beginRequest or inspect unobserved credit.
    var constructor_permit = try session.admitWithPressure(request, pressure);
    defer constructor_permit.deinit();
    if (!try architecture.prepareQwenWorkspaceProviderOutsideExecution(session, &constructor_permit, options.control))
        return error.UnsupportedResidentTextEncoder;
    var described = (try architecture.describeQwenWorkspaceAuxiliaryOutsideExecution(session, 0, options.control)) orelse
        return error.UnsupportedResidentTextEncoder;
    defer described.deinitOutsideExecution();
    // Release before acquiring the SAME original floor plus disjoint
    // declarations; no double original peak and no supplier under a lane.
    constructor_permit.deinit();
    const old_count = (try described.census.physical.capturedCoverage()).witnessed_roots;
    const host_metadata = try hostDeclarationBytes(described.census.physical.entries.len, described.auxiliary.plan.fresh_slots, old_count, 0, 0);
    var permit = try admitWorkspace(session, request, described.auxiliary.plan.fresh_bytes, host_metadata, held_output, pressure);
    defer permit.deinit();
    var phase = try ownership.Phase.prepareFromPermitOutsideExecution(session, &permit, &described.census.target, described.auxiliary.plan.fresh_slots, described.auxiliary.plan.fresh_bytes, old_count, options.control);
    defer phase.finishOutsideExecution();
    const roots = try process.alloc(*ownership.Root, described.census.physical.entries.len);
    defer process.free(roots);
    const existing = try described.census.physical.copyRoots(roots);
    var bank = try ownership.UseBank.prepareExact(existing.len);
    defer bank.deinitOutsideExecution();
    try phase.fundReceivingBankOutsideExecution(&bank);
    try bank.bindOutsideExecution(existing, epoch);
    var validation = try roles.Snapshot.prepare(described.census.physical.entries.len);
    defer validation.deinitOutsideExecution();
    const final_capacity = try std.math.add(usize, described.census.physical.entries.len, described.auxiliary.plan.fresh_slots);
    var final = try roles.Snapshot.prepare(final_capacity);
    defer final.deinitOutsideExecution();
    const final_roots = try process.alloc(*ownership.Root, final_capacity);
    defer process.free(final_roots);
    // These final-address loans are synchronous only; no late node points
    // into the caller or this stack. Each bank node owns its own pure cell.
    var receiving: ownership.ReceiverUses = .{ .existing = &bank, .allocations = &phase };
    var current = cleanup;
    var context: @import("invocation_context.zig").Context = .{ .cleanup = current, .workspace_uses = &receiving };
    const numerical_scope = try NumericalScope.acquire(options, selected, stage);
    defer numerical_scope.release(options, selected, stage);
    var managed = try architecture.constructMetalWorkspaceReceiver(session, options.allocator, options.control);
    defer managed.deinitWithCleanup(current);
    const cb = &managed.backend;
    if (!try cb.bindCleanupEpoch(epoch)) return error.BackendOperationCleanupUnavailable;
    current.operation = cb.operationToken();
    context.cleanup = current;
    var binding = try Metal.beginPreparedAuxiliaryWorkspaceInvocation(cb, &context, &described.census.physical, &validation, &described.auxiliary);
    // Family preparation has the same checked scope lifecycle, with no
    // mathematical work or retry. Close before propagating its outcome.
    const body: anyerror!bool = body: {
        @import("../backends/compute_factory.zig").beginRequestWithCleanup(session, cb, current) catch |err| break :body err;
        const prepared = cb.decoderRuntimePrepareOrReuseTextPrefill(options.allocator, cfg, cfg.num_hidden_layers) catch |err| break :body err;
        if (!prepared.prepared) break :body false;
        // GGUF logical registration deliberately leaves quantized SDK kinds
        // lazy. Publish the real SAME representations during this funded
        // cold family scope, before the outside selected-plan capture.
        break :body Metal.prepareOwnedQwenWorkspaceCatalog(cb, cfg, cfg.num_hidden_layers, options.control) catch |err| break :body err;
    };
    const closed = binding.closeReported(&final, final_roots) catch platform.inference_process_supervisor.restartWorker();
    receiving.allocations = null;
    const prepared = try body; // real SDK errors retain their original cause
    try closed.check();
    if (!prepared) return error.PreparedWorkspaceFamilyUnavailable;
}

pub fn run(options: Options, stage: anytype, held_output: ?session_mod.HostOutputProducer.HeldAdmission, pressure: *?memory_admission.AdmissionPressure, admission_denied: *bool) anyerror![][]f32 {
    if (comptime !build_options.enable_metal) return error.UnsupportedResidentTextEncoder;
    const session = options.session;
    const cfg = architecture.getGptConfig(session) orelse return error.UnsupportedResidentTextEncoder;
    if (!architecture.acceptsMetalWorkspaceSession(session)) return error.UnsupportedWorkspaceTarget;
    const process = platform.allocator.processAllocator(platform.allocator.concurrentFallback());
    var cleanup_owner = try @import("cleanup_owner.zig").Owner.beginResident(session);
    defer cleanup_owner.finishAfterUnlock();
    const cleanup: cleanup_mod.Context = .{ .boundary = cleanup_owner.epoch() };
    const epoch = cleanup.boundary orelse return error.MissingCurrentCleanupEpoch;
    const sequence = if (options.batch == 1 and options.max_sequence == 1) 2 else options.max_sequence;
    const total = try std.math.mul(usize, options.batch, sequence);
    const width: usize = cfg.hidden_size;
    const original_workspace = if (session.run_admission) |admission|
        try admission.qwenTransientWorkspaceBytes(total)
    else
        try std.math.add(usize, 16 * 1024 * 1024, try std.math.mul(usize, total, try std.math.mul(usize, width, 16)));
    // Preserve the COMPLETE original opaque/private/static/mandatory/IO
    // floor. Exact fresh SDK declarations are additive; no old fungible
    // byte estimate or snapshot is spent as replacement credit.
    const request: session_mod.RunRequest = .{
        .batch = options.batch,
        .sequence = sequence,
        .input_bytes = try std.math.mul(usize, total, 12),
        .output_bytes = try std.math.mul(usize, options.batch, try std.math.mul(usize, width, @sizeOf(f32))),
        .workspace_bytes = original_workspace,
    };
    var waiter: Waiter = .{};
    const gate = session.execution_gate orelse options.execution_lock;
    const lock_started = if (options.measure_lock) embedding_trace.now() else 0;
    try session.selectReadyCohort(gate, options.control orelse .{}, &waiter);
    defer session.leaveSelectedCohort(&waiter);
    if (session.run_admission == null)
        return runUnadmitted(options, stage, sequence, request, cleanup, &waiter);
    var layers: [256]ops_mod.DecoderRuntimeLayerSpec = undefined;
    const actual_layers = try decoder_gated_runtime.fillDenseQwen3LayerSpecs(cfg, cfg.num_hidden_layers, &layers);
    const frame_request: ops_mod.DecoderRuntimePrefillFramePlanRequest = .{
        .contract = .qwen3_dense_text_prefill,
        .layer_count = actual_layers.len,
        .rows = total,
        .batch = options.batch,
        .seq_len = sequence,
        .hidden_size = cfg.hidden_size,
        .vocab_size = cfg.vocab_size,
        .num_attention_heads = cfg.num_attention_heads,
        .global_head_dim = 0,
        .ple_hidden_size = 0,
        .final_norm_slot = decoder_gated_runtime.finalNormSlot(cfg.num_hidden_layers),
        .final_lm_head_slot = 0,
        .include_tail = false,
        .layers = actual_layers,
    };
    const last = options.pooling == .last;
    if (!last and options.pooling != .mean) return error.UnsupportedWorkspaceTail;
    const normalize_in_frame = last and options.normalize and !Metal.workspaceFrameNormalizeDisabled();
    const tail_request: Metal.WorkspaceTailRequest = .{
        .batch = options.batch,
        .sequence = sequence,
        .width = cfg.hidden_size,
        .take_rows = last,
        .normalize_rows = normalize_in_frame,
        .pool_sequence = if (!last) sequence else if (options.normalize and !normalize_in_frame) 1 else null,
    };
    var family_prepared = false;
    var stale_attempts: usize = 0;
    while (true) {
        if (options.control) |control| try control.check();
        var cold = (architecture.describeQwenWorkspaceOutsideExecution(session, &frame_request, tail_request, options.control) catch |err| {
            if (err != error.QueueFull) return err;
            const control = options.control orelse return err;
            if (control.deadline_ns == null) return err;
            try control.check();
            std.Thread.yield() catch {};
            continue;
        }) orelse {
            if (family_prepared) return error.PreparedWorkspaceFamilyUnavailable;
            // This original SDK preparation is BEFORE any numerical work;
            // afterwards the actual selected formats are replanned cold.
            try prepareFamily(options, stage, request, cleanup, pressure, &waiter, held_output);
            family_prepared = true;
            continue;
        };
        defer cold.deinitOutsideExecution();
        const fresh_count = try std.math.add(usize, cold.reference.fresh_slots, try std.math.add(usize, try std.math.add(usize, cold.roles.fresh_slots, cold.auxiliary.plan.fresh_slots), try std.math.add(usize, cold.tail.allocations.fresh_slots, cold.tail.staging.fresh_slots)));
        const fresh_bytes = try std.math.add(usize, cold.reference.fresh_bytes, try std.math.add(usize, try std.math.add(usize, cold.roles.fresh_bytes, cold.auxiliary.plan.fresh_bytes), try std.math.add(usize, cold.tail.allocations.fresh_bytes, cold.tail.staging.fresh_bytes)));
        const old_count = (try cold.census.physical.capturedCoverage()).witnessed_roots;
        const host_metadata = try hostDeclarationBytes(cold.census.physical.entries.len, fresh_count, old_count, try cold.metadata.requestedMetadataBytes(), try std.math.add(usize, cold.tail.host_cpu_bytes, cold.reference.host_cpu_bytes));
        var permit = admitWorkspace(session, request, fresh_bytes, host_metadata, held_output, pressure) catch |err| {
            admission_denied.* = true;
            return err;
        };
        defer permit.deinit();
        var phase = try ownership.Phase.prepareFromPermitOutsideExecutionWithHostCpu(session, &permit, &cold.census.target, fresh_count, fresh_bytes, old_count, cold.reference.host_cpu_bytes, options.control);
        defer phase.finishOutsideExecution();
        const root_storage = try process.alloc(*ownership.Root, cold.census.physical.entries.len);
        defer process.free(root_storage);
        const old_roots = try cold.census.physical.copyRoots(root_storage);
        var bank = try ownership.UseBank.prepareExact(old_roots.len);
        defer bank.deinitOutsideExecution();
        try phase.fundReceivingBankOutsideExecution(&bank);
        try bank.bindOutsideExecution(old_roots, epoch);
        var validation = try roles.Snapshot.prepare(cold.census.physical.entries.len);
        defer validation.deinitOutsideExecution();
        const final_capacity = try std.math.add(usize, cold.census.physical.entries.len, fresh_count);
        var final = try roles.Snapshot.prepare(final_capacity);
        defer final.deinitOutsideExecution();
        const final_roots = try process.alloc(*ownership.Root, final_capacity);
        defer process.free(final_roots);
        var uses: ownership.ReceiverUses = .{ .existing = &bank, .allocations = &phase };
        var current = cleanup;
        var context: @import("invocation_context.zig").Context = .{ .cleanup = current, .workspace_uses = &uses };
        // All supplier and metadata work above runs with no actual Model,
        // provider, external gate or execution lane. Only this block owns
        // numerical state. A stale cold plan retries after ALL defers leave.
        const vectors = numerical: {
            const numerical_scope = try NumericalScope.acquire(options, &waiter, stage);
            defer numerical_scope.release(options, &waiter, stage);
            var managed = try architecture.constructMetalWorkspaceReceiver(session, options.allocator, options.control);
            defer managed.deinitWithCleanup(current);
            const cb = &managed.backend;
            if (!try cb.bindCleanupEpoch(epoch)) return error.BackendOperationCleanupUnavailable;
            current.operation = cb.operationToken();
            context.cleanup = current;
            if (!stage.valid()) return error.WorkspacePolicyChanged;
            var binding = Metal.beginPreparedWorkspaceInvocation(cb, &context, &cold.census.physical, &validation, &cold.roles, &cold.auxiliary, &cold.tail, &cold.reference, &cold.metadata, &frame_request, &final, final_roots) catch |err| switch (err) {
                error.WorkspaceRoleChanged, error.WorkspaceTailChanged, error.WorkspaceReferenceProgramChanged, error.WorkspaceTargetChanged, error.WorkspaceCensusChanged, error.WorkspaceRequestChanged => break :numerical null,
                else => return err,
            };
            // Capture the original body outcome before closing ONCE. Its
            // inner SDK/CT defers have completed; no body error can bypass
            // checked cleanup or authorize replay through an outer retry.
            const body: anyerror![][]f32 = body: {
                @import("../backends/compute_factory.zig").beginRequestWithCleanup(session, cb, current) catch |err| break :body err;
                stage.recordLockWait(lock_started);
                const produced = stage.produce(sequence, current, cb) catch |err| break :body err;
                break :body produced orelse error.PreparedWorkspaceKernelUnavailable;
            };
            const closed = binding.closeReported(&final, final_roots) catch platform.inference_process_supervisor.restartWorker();
            uses.allocations = null;
            const result = body catch |err| {
                // Optional callbacks report null for allocation decline;
                // use its certified cause, preserving real SDK/math errors.
                if (err == error.PreparedWorkspaceKernelUnavailable) try closed.check();
                return err;
            };
            errdefer freeEmbeddingSlices(options.allocator, result);
            try closed.check();
            break :numerical result;
        };
        if (vectors) |result| return result;
        stale_attempts += 1;
        if (stale_attempts == 8) return error.WorkspaceRoleChanged;
        // No numerical callback ran on this path. Loop teardown executes
        // outside every gate before rebuilding the immutable declarations.
    }
}

/// No admission means no new modern Root or quota credit. It still borrows
/// exact old-root cells under its genuine resident B, so an ordinary peer
/// cannot consume prior charged roots with an unbound cleanup context.
fn runUnadmitted(options: Options, stage: anytype, sequence: usize, request: session_mod.RunRequest, cleanup: cleanup_mod.Context, selected: *Waiter) anyerror![][]f32 {
    const process = platform.allocator.processAllocator(platform.allocator.concurrentFallback());
    const session = options.session;
    var constructor_permit = try session.admit(request);
    defer constructor_permit.deinit();
    _ = try architecture.prepareQwenWorkspaceProviderOutsideExecution(session, &constructor_permit, options.control);
    var census = (try architecture.snapshotQwenWorkspaceOutsideExecution(session, options.control)) orelse return error.WorkspaceTargetChanged;
    defer census.deinitOutsideExecution();
    const root_storage = try process.alloc(*ownership.Root, census.physical.entries.len);
    defer process.free(root_storage);
    const roots = try census.physical.copyRoots(root_storage);
    var bank = try ownership.UseBank.prepareExact(roots.len);
    defer bank.deinitOutsideExecution();
    try bank.bindOutsideExecution(roots, cleanup.boundary.?);
    var validation = try roles.Snapshot.prepare(census.physical.entries.len);
    defer validation.deinitOutsideExecution();
    var final = try roles.Snapshot.prepare(census.physical.entries.len);
    defer final.deinitOutsideExecution();
    var uses: ownership.ReceiverUses = .{ .existing = &bank };
    var current = cleanup;
    var context: @import("invocation_context.zig").Context = .{ .cleanup = current, .workspace_uses = &uses };
    const numerical_scope = try NumericalScope.acquire(options, selected, stage);
    defer numerical_scope.release(options, selected, stage);
    var managed = try architecture.constructMetalWorkspaceReceiver(session, options.allocator, options.control);
    defer managed.deinitWithCleanup(current);
    const cb = &managed.backend;
    if (!try cb.bindCleanupEpoch(cleanup.boundary.?)) return error.BackendOperationCleanupUnavailable;
    current.operation = cb.operationToken();
    context.cleanup = current;
    // Preserve the exact original Main expression, including unadmitted
    // maxInt when there is no lease. This raw byte guard is NOT Root credit.
    const original_workspace_bytes = if (constructor_permit.lease) |lease| lease.amounts.backend_scratch_bytes else std.math.maxInt(usize);
    var binding = try Metal.bindUnadmittedBoundedWorkspaceInvocation(cb, &context, @intFromPtr(census.target.target.metal.runtime), &census.physical, &validation, original_workspace_bytes);
    const body: anyerror![][]f32 = body: {
        @import("../backends/compute_factory.zig").beginRequestWithCleanup(session, cb, current) catch |err| break :body err;
        const result = stage.produce(sequence, current, cb) catch |err| break :body err;
        break :body result orelse error.UnsupportedResidentTextEncoder;
    };
    const closed = Metal.finishExistingWorkspaceInvocationReported(&binding, &final, root_storage) catch platform.inference_process_supervisor.restartWorker();
    const result = body catch |err| {
        if (err == error.UnsupportedResidentTextEncoder) try closed.check();
        return err;
    };
    errdefer freeEmbeddingSlices(options.allocator, result);
    try closed.check();
    return result;
}

fn freeEmbeddingSlices(allocator: std.mem.Allocator, embeddings: [][]f32) void {
    for (embeddings) |values| allocator.free(values);
    allocator.free(embeddings);
}
