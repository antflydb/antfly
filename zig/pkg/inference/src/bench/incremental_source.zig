// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Maintained encoder-to-incremental-decoder ownership workload. Each sample
//! encodes once outside receiver timing, then uses the public retained-source
//! Decoder through repeated real cross-attention consumers and final retirement.
const std = @import("std");
const inference = @import("inference_internal");
const stages = inference.execution.encoder_decoder_runtime;
const decoder_stage = inference.execution.decoder_stage;
const architecture = inference.execution.architecture_session;
const Session = inference.backends.Session;
const memory = inference.runtime.tier.memory;
const Control = inference.execution_control.InferenceExecutionControl;
const Root = if (@hasDecl(inference.execution, "owned_incremental_input")) inference.execution.owned_incremental_input else LegacyRoot;

// Identical harness source also builds against the checked synchronous transport
// predecessor. Public source/resource ownership remains identical there; only
// the newer explicit task cleanup capability is absent.
const LegacyRoot = struct {
    const Cleanup = union(enum) {
        unqualified,
        fn beginTaskWithoutEnclosingLanes(_: ?Control) !@This() {
            return .unqualified;
        }
        fn finish(self: *@This()) void {
            self.* = .unqualified;
        }
    };
    const OwnedIncrementalInput = struct {
        encoder: stages.EncoderOutputs,
        source: ?decoder_stage.RetainedEncoderSource,
        allocator: std.mem.Allocator,
        fn take(a: std.mem.Allocator, encoded: *stages.EncoderOutputs, destination: Session, _: *Cleanup) !@This() {
            const retained = try decoder_stage.retainIncrementalSource(a, encoded, destination);
            const result = @This(){ .encoder = encoded.*, .source = retained, .allocator = a };
            encoded.* = undefined;
            return result;
        }
        fn deinit(self: *@This()) void {
            if (self.source) |*source| source.deinit();
            self.encoder.deinit(self.allocator);
            self.* = undefined;
        }
    };
};

// The checked synchronous predecessor has no detached-clone owner field:
// that ownership mechanism cannot be present in its type. All physical-span,
// alias-chain, cache/workspace and actual legacy receiver checks still run.
// This adapter never treats absent detached fields as detached proof success.
fn hasForeignClone(comptime Tensor: type, tensor: *const Tensor) bool {
    if (comptime @hasField(Tensor, "foreign_clone_owner")) return tensor.foreign_clone_owner != null;
    return false;
}

// Benchmark-only profiler control. The borrowed library remains pinned by the
// complete OwnedIncrementalInput until every constructor scope has stopped.
// No numerical operation, model lane, provider lane or stream method is exposed.
const ConstructorTrace = struct {
    const ProfilerCall = *const fn () callconv(.c) c_int;
    context: *anyopaque,
    invoke_checked: *const fn (*anyopaque, ProfilerCall) anyerror!void,
    start_call: ProfilerCall,
    stop_call: ProfilerCall,
    active: bool = false,

    fn load(comptime Cuda: type, backend: *const inference.ops.ComputeBackend) !ConstructorTrace {
        if (comptime @hasDecl(Cuda, "foreignTensorCopyTarget")) {
            if (Cuda.foreignTensorCopyTarget(backend) == null) return error.CanonicalCudaSourceRequired;
            const compute: *Cuda.CudaCompute = @ptrCast(@alignCast(backend.ptr));
            const Driver = @TypeOf(compute.ctx.driver);
            const Adapter = struct {
                fn invoke(raw: *anyopaque, sdk: ProfilerCall) !void {
                    const driver: *Driver = @ptrCast(@alignCast(raw));
                    try driver.check(sdk());
                }
            };
            // Dynamic optional profiler symbols require no runtime dependency.
            const start_symbol = compute.ctx.driver.lib.lookup(ProfilerCall, "cuProfilerStart") orelse return error.CudaProfilerApiUnavailable;
            const stop_symbol = compute.ctx.driver.lib.lookup(ProfilerCall, "cuProfilerStop") orelse return error.CudaProfilerApiUnavailable;
            return .{ .context = &compute.ctx.driver, .invoke_checked = Adapter.invoke, .start_call = start_symbol, .stop_call = stop_symbol };
        } else return error.SourceTraceProofUnsupported;
    }
    fn start(self: *ConstructorTrace, control: Control) !void {
        if (self.active) return error.ConstructorTraceAlreadyActive;
        var supervised = try control.enterUninterruptible(.uninterruptible);
        defer supervised.deinit();
        try self.invoke_checked(self.context, self.start_call);
        self.active = true;
    }
    fn stop(self: *ConstructorTrace, control: Control) !void {
        if (!self.active) return;
        var supervised = try control.enterUninterruptible(.uninterruptible);
        defer supervised.deinit();
        try self.invoke_checked(self.context, self.stop_call);
        self.active = false;
    }
};

// Independent physical-root inspection: do not call the selector under test.
fn inspectRoot(comptime Cuda: type, source: anytype, bytes: usize) !*Cuda.CudaTensor {
    var root: *Cuda.CudaTensor = @ptrCast(@alignCast(source.value));
    for (0..64) |_| {
        if (!root.owned_by_tensor or root.resident_references.load(.acquire) != 1 or root.storage_retirement != null or root.resident_owner != null or hasForeignClone(Cuda.CudaTensor, root) or root.physical_lease != null or root.model_constant or root.model_constant_quota.hasLease() or root.immutable_cache_backing or root.strict_resident_training or root.quant_type != null or root.tc_quant != null or root.bf16_mirror.ptr != 0 or root.training_upload_host.ptr != null or root.training_upload_generation != null or root.resident_indices != null or root.physical_storage_base) return error.UnsupportedSourcePhysicalOwner;
        for (root.auxiliary_leases) |lease| if (lease != null) return error.UnsupportedSourcePhysicalOwner;
        if (root.buffer.len != bytes or root.elem_count != bytes / 4 or root.dtype != .f32) return error.NonFullSpanSource;
        if (root.owns_buffer) {
            if (root.resident_parent != null or root.buffer.borrowed or root.buffer.allocation_bytes != bytes) return error.NonFullSpanSource;
            const compute: *Cuda.CudaCompute = @ptrCast(@alignCast(source.backend.ptr));
            for (compute.temp_buffers.buffers.items) |cached| if (cached.ptr == root.buffer.ptr) return error.WorkspaceSource;
            for (compute.deferred_device_frees.items) |pending| if (pending.ptr == root.buffer.ptr) return error.WorkspaceSource;
            const end = try std.math.add(u64, root.buffer.ptr, @intCast(bytes));
            for (compute.temp_pinned_slots.items) |slot| if (slot.buffer.ptr != 0 and root.buffer.ptr < try std.math.add(u64, slot.buffer.ptr, @intCast(slot.buffer.len)) and slot.buffer.ptr < end) return error.WorkspaceSource;
            for (compute.debug_cuda_graph_slots) |slot| {
                const spans = .{ slot.input_storage, slot.aux_input_storage, slot.output_storage, slot.input, slot.aux_input, slot.output };
                inline for (spans) |span| if (span.ptr != 0 and root.buffer.ptr < try std.math.add(u64, span.ptr, @intCast(span.len)) and span.ptr < end) return error.WorkspaceSource;
            }
            return root;
        }
        const parent = root.resident_parent orelse return error.UnsupportedSourcePhysicalOwner;
        if (root.buffer.ptr != parent.buffer.ptr or root.buffer.len != parent.buffer.len or root.elem_count != parent.elem_count or root.dtype != parent.dtype) return error.NonFullSpanSource;
        root = parent;
    }
    return error.UnsupportedSourcePhysicalOwner;
}

// Same physical owner used by the maintained transcription task. Every timed
// overlap is required to encode ahead with both real frame handles present.
fn pipeline(native: *architecture.WhisperNativeDecoder, steps: usize, output: []f32) !void {
    const Pending = inference.execution.decoder_pending_step.PipelinedStage(architecture.WhisperNativeDecoder);
    const ops = inference.ops;
    const grammar: ops.WhisperGrammarState = .{ 1, @intCast(native.config.vocab_size), @intCast(native.config.vocab_size), 0, 0, 0, 0, 0 };
    var stage = Pending.begin(native, &grammar) orelse return error.PipelinedWhisperRequired;
    defer stage.deinit();
    const params = ops.WhisperLogitsParams{ .out_dim = @intCast(native.config.vocab_size), .suppress_count = 0, .ts_begin = @intCast(native.config.vocab_size), .text_allowed = 1, .ts_min = @intCast(native.config.vocab_size), .ts_max = @intCast(native.config.vocab_size), .eot = 50257, .probe_id = @intCast(native.config.vocab_size), .mode = ops.whisper_logits_mode_choose | ops.whisper_logits_mode_eot_allowed };
    if (!try stage.encodePrefix(&.{ 0, 1, 3 }, params, &.{})) return error.PipelinedWhisperRequired;
    try stage.submit();
    for (0..steps) |index| {
        const next = index + 1 < steps;
        if (next) {
            if (!try stage.encodeLookahead(params, &.{})) return error.PipelinedWhisperRequired;
            if (!native.cb.backend.decoderRuntimeHasSubmittedFrame() or !native.cb.backend.decoderRuntimeHasActiveFrame()) return error.ActualFrameOverlapRequired;
        }
        const stats = try stage.awaitStats();
        const token: i32 = @bitCast(stats[14]);
        if (token < 0 or @as(usize, @intCast(token)) >= native.config.vocab_size or !std.math.isFinite(stats[15])) return error.InvalidPipelinedOutput;
        output[index * 2] = @floatFromInt(token);
        output[index * 2 + 1] = stats[15];
        if (next) try stage.submitLookahead();
    }
}

fn transient(a: memory.AdmissionAmounts) memory.AdmissionAmounts {
    var result = a;
    // Model weights stay resident across requests; only request/physical KV and
    // scratch are part of this terminal receiver interval.
    result.host_weight_bytes = 0;
    result.backend_weight_bytes = 0;
    return result;
}
fn waitRetired(controller: *memory.AdmissionController, expected: memory.AdmissionAmounts, control: Control) !void {
    while (!std.meta.eql(transient(controller.snapshot()), transient(expected))) {
        try control.check();
        inference.platform.time.sleepNs(100 * std.time.ns_per_us);
    }
}
fn writeSamples(a: std.mem.Allocator, label: []const u8, values: []const u64) !void {
    const sorted = try a.dupe(u64, values);
    defer a.free(sorted);
    std.mem.sort(u64, sorted, {}, std.sort.asc(u64));
    std.debug.print("source_receiver_metric name={s} count={d} p50_ns={d} p95_ns={d} min_ns={d}\n", .{ label, sorted.len, sorted[sorted.len / 2], sorted[@min(sorted.len - 1, sorted.len * 95 / 100)], sorted[0] });
}

pub fn run(init: std.process.Init, source_model: anytype, a: std.mem.Allocator, options: anytype) !void {
    if (options.source_trace_constructor and (options.backend != .cuda or options.source_proof == .off or !options.diagnostics)) return error.ConstructorTraceRequiresCudaProof;
    if (options.source_proof == .off and options.diagnostics and !options.source_metadata_proof) return error.TimingRequiresDiagnosticsOff;
    if (options.source_metadata_proof and (options.backend != .metal or options.source_policy != .qualified or !options.diagnostics or options.source_proof != .off or options.source_trace_constructor)) return error.InvalidMetadataProof;
    if (options.source_proof != .off and !options.diagnostics) return error.ProofRequiresDiagnostics;
    if (options.source_proof == .detached and options.source_policy != .qualified) return error.InvalidProofPolicy;
    const Cuda = inference.native_compute.cuda;
    const encoder = source_model.session;
    const encoder_config = architecture.getWhisperConfig(encoder) orelse return error.WhisperEncoderRequired;
    const backend = encoder.backend();
    if (backend != .cuda and backend != .metal and backend != .native) return error.ResidentBackendRequired;
    if (backend != .cuda and options.source_proof != .off) return error.CudaSourceProofRequired;
    if (backend != .metal and options.source_pipelined_steps != 0) return error.MetalPipelinedWhisperRequired;
    var target_sessions = inference.backends.SessionManager.initWithIo(a, init.io);
    inference.native_backend_choice.configureSessionPreference(&target_sessions, options.backend);
    var target_manager = inference.server.model_manager.ModelManager.init(a, target_sessions);
    defer target_manager.deinit();
    target_manager.configureServingPolicy(.{ .allow_unknown = true });
    const target_path = if (options.decoder_model_dir.len != 0) options.decoder_model_dir else options.model_dir;
    const target_model = try target_manager.loadFromDir(target_path);
    const destination = target_model.session;
    const config = architecture.getWhisperConfig(destination) orelse return error.WhisperDecoderRequired;
    const receiver = architecture.IncrementalDecoderAdapter.qualify(encoder, destination) orelse return error.IncrementalReceiverRequired;
    if (receiver.backend != backend or receiver.same_model_owner) return error.IndependentReceiverRequired;
    if (encoder_config.d_model != config.d_model or options.seq_len > encoder_config.max_source_positions * 2) return error.InvalidGeometry;
    _ = encoder.configureExecutionDiagnostics(options.diagnostics);
    _ = destination.configureExecutionDiagnostics(options.diagnostics);
    const source_lifetime = encoder.model_lifetime orelse return error.SourceModelLifetimeRequired;
    const target_lifetime = destination.model_lifetime orelse return error.DestinationModelLifetimeRequired;
    const source_domain = if (source_lifetime.admission_domain) |pin| pin.retain() else return error.SourceAdmissionRequired;
    defer source_domain.release();
    const target_domain = if (target_lifetime.admission_domain) |pin| pin.retain() else return error.DestinationAdmissionRequired;
    defer target_domain.release();
    const watchdog = try inference.HardCancellationWatchdog.create(a);
    defer watchdog.destroy();
    try watchdog.start(init.io);
    const count = try std.math.mul(usize, encoder_config.num_mel_bins, options.seq_len);
    const features = try a.alloc(f32, count);
    defer a.free(features);
    for (features, 0..) |*value, index| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(index % 31)) - 15)) * 0.01;
    var input = try inference.backends.Tensor.initFloat32(a, "input_features", &.{ 1, @intCast(encoder_config.num_mel_bins), @intCast(options.seq_len) }, features);
    defer input.deinit();
    const attempt_count = try std.math.mul(usize, options.iterations, options.source_attempts);
    const constructor_samples = try a.alloc(u64, attempt_count);
    defer a.free(constructor_samples);
    const return_samples = try a.alloc(u64, attempt_count);
    defer a.free(return_samples);
    const retired_samples = try a.alloc(u64, attempt_count);
    defer a.free(retired_samples);
    const root_retired_samples = try a.alloc(u64, options.iterations);
    defer a.free(root_retired_samples);
    const lifecycle_samples = try a.alloc(u64, options.iterations);
    defer a.free(lifecycle_samples);
    const complete_task_samples = try a.alloc(u64, options.iterations);
    defer a.free(complete_task_samples);
    var reference: ?[]f32 = null;
    defer if (reference) |values| a.free(values);
    var source_shape: [3]i64 = undefined;
    var source_bytes: usize = 0;
    var checksum: f64 = 0;
    for (0..options.warmup + options.iterations) |iteration| {
        const control = Control{ .io = init.io, .deadline_ns = inference.platform.time.monotonicNs() + 120 * std.time.ns_per_s, .hard_cancellation = watchdog.boundary(), .native_call_policy = .require_supervision, .cancellation_grace_ns = std.time.ns_per_s };
        // Include real admission, encoder allocation/cache reuse, complete
        // receiver consumption, both root siblings and physical retirement.
        const complete_task_start = inference.platform.time.monotonicNs();
        const source_initial = source_domain.controller().snapshot();
        const target_task_initial = target_domain.controller().snapshot();
        var arena = std.heap.ArenaAllocator.init(a);
        var arena_open = true;
        defer if (arena_open) arena.deinit();
        const request_allocator = arena.allocator();
        // This benchmark owns its private model handles and holds no enclosing
        // provider/model lanes. The guard begins before actual encoder work.
        var cleanup: Root.Cleanup = if (options.source_policy == .qualified) try Root.Cleanup.beginTaskWithoutEnclosingLanes(control) else .unqualified;
        defer cleanup.finish();
        var permit = try encoder.admitInputs(&.{input}, request_allocator, control);
        defer permit.deinit();
        try stages.prepareEncoderPermit(encoder, destination, null, control, true, &permit);
        var encoded = try stages.encodeForIncrementalWithPermit(request_allocator, encoder, destination, &.{input}, null, control, &permit);
        // EncoderOutputs holds its complete resources. Release any residual
        // permit-only admission before receiver timing and terminal snapshots.
        permit.deinit();
        var encoded_open = true;
        defer if (encoded_open) encoded.deinit(request_allocator);
        source_shape = switch (encoded) {
            .resident => |resident| resident.shape,
            .host => return error.ResidentEncoderRequired,
        };
        if (source_shape[0] != 1 or source_shape[1] <= 0 or source_shape[2] != @as(i64, @intCast(config.d_model))) return error.InvalidEncoderShape;
        source_bytes = try std.math.mul(usize, try std.math.mul(usize, @intCast(source_shape[1]), @intCast(source_shape[2])), @sizeOf(f32));
        const mask = try request_allocator.alloc(i64, @intCast(source_shape[1]));
        @memset(mask, 1);
        const metadata_source_backend = if (options.source_metadata_proof) encoded.resident.values.backend else null;
        const metadata_source_usage = if (options.source_metadata_proof) source_domain.controller().snapshot() else null;
        if (options.source_metadata_proof and metadata_source_backend.?.operationToken() == null) return error.MetadataProofRequiresOwnedSourceOperation;
        var metadata_audit = inference.execution.transfer_audit.Audit{ .forbid_device_host_access = true };
        var metadata_audit_open = false;
        defer if (metadata_audit_open) inference.execution.transfer_audit.end();
        if (options.source_metadata_proof) {
            try inference.execution.transfer_audit.begin(&metadata_audit);
            metadata_audit_open = true;
        }
        var owned = try Root.OwnedIncrementalInput.take(request_allocator, &encoded, destination, &cleanup);
        if (metadata_audit_open) {
            inference.execution.transfer_audit.end();
            metadata_audit_open = false;
        }
        encoded_open = false;
        var owned_open = true;
        defer if (owned_open) owned.deinit();
        if (owned.source == null) return error.RetainedIncrementalSourceRequired;
        if (options.source_metadata_proof) {
            if (owned.source.?.backend == metadata_source_backend.? or owned.source.?.backend.operationToken() != null) return error.MetadataPromotionRequired;
            if (metadata_audit.device_copy_calls != 0 or metadata_audit.upload_calls != 0 or metadata_audit.download_calls != 0 or metadata_audit.host_accesses != 0) return error.MetadataPromotionTransferredPayload;
            if (!std.meta.eql(transient(metadata_source_usage.?), transient(source_domain.controller().snapshot()))) return error.MetadataPromotionChangedSourceQuota;
            // Actual source provider reacquisition is a diagnostic-only proof
            // that the original Engine released exclusion before target admit.
            // This extra construction is outside promotion's transfer audit.
            var source_reentry = try architecture.getComputeBackendWithControl(encoder, a, control);
            source_reentry.deinit();
        }
        // Inspection also runs in timing mode: unsupported layouts fail closed.
        const source_address: u64 = if (backend == .cuda) address: {
            if (comptime @hasDecl(Cuda, "CudaCompute")) break :address (try inspectRoot(Cuda, owned.source.?, source_bytes)).buffer.ptr;
            return error.CudaNotEnabled;
        } else 0;
        const source_usage = source_domain.controller().snapshot();
        const lifecycle_start = inference.platform.time.monotonicNs();
        for (0..options.source_attempts) |attempt| {
            const target_initial = target_domain.controller().snapshot();
            var attempt_arena = std.heap.ArenaAllocator.init(a);
            var attempt_arena_open = true;
            defer if (attempt_arena_open) attempt_arena.deinit();
            var audit = inference.execution.transfer_audit.Audit{ .forbid_device_host_access = true };
            // The source backend is created by a private canonical ArchSession;
            // no other caller can submit through this model during this sample.
            const source_backend = owned.source.?.backend;
            var constructor_trace: ?ConstructorTrace = if (options.source_trace_constructor) try ConstructorTrace.load(Cuda, source_backend) else null;
            // Stop on constructor error as well. A failed supervised cleanup
            // cannot leave an active process-wide proof range behind.
            defer if (constructor_trace) |*trace| trace.stop(control) catch inference.platform.inference_process_supervisor.restartWorker();
            var copy_before: usize = 0;
            var calls_before: usize = 0;
            var target_bytes_before: usize = 0;
            var target_calls_before: usize = 0;
            var clone_address: u64 = 0; // Scalar metadata only; never retain the header past teardown.
            if (options.source_proof != .off) {
                if (comptime @hasDecl(Cuda, "foreignTensorCopyTarget")) {
                    if (Cuda.foreignTensorCopyTarget(source_backend) == null) return error.CanonicalCudaSourceRequired;
                    const compute: *Cuda.CudaCompute = @ptrCast(@alignCast(source_backend.ptr));
                    copy_before = compute.stats.cross_backend_copy_bytes;
                    calls_before = compute.stats.cross_backend_copies;
                    var proof_backend = try architecture.getComputeBackend(destination, a);
                    defer proof_backend.deinit();
                    if (Cuda.foreignTensorCopyTarget(&proof_backend) == null) return error.CanonicalCudaDestinationRequired;
                    const target_compute: *Cuda.CudaCompute = @ptrCast(@alignCast(proof_backend.ptr));
                    target_bytes_before = target_compute.stats.cross_backend_copy_bytes;
                    target_calls_before = target_compute.stats.cross_backend_copies;
                } else return error.SourceCopyProofUnsupported;
                try inference.execution.transfer_audit.begin(&audit);
            }
            var audit_open = options.source_proof != .off;
            defer if (audit_open) inference.execution.transfer_audit.end();
            const start = inference.platform.time.monotonicNs();
            if (constructor_trace) |*trace| try trace.start(control);
            var decoder = try decoder_stage.Decoder.initWithSource(attempt_arena.allocator(), destination, &owned.encoder, @intCast(source_shape[1]), mask, config.vocab_size, control, .{ .allow_merged = false }, &owned.source.?);
            var decoder_open = true;
            defer if (decoder_open) decoder.deinit();
            if (constructor_trace) |*trace| {
                if (comptime @hasDecl(Cuda, "foreignTensorCopyTarget")) {
                    const ready = if (decoder.native) |*value| value else return error.NativeDecoderRequired;
                    // Exact canonical implementation certification precedes
                    // the diagnostic-only context cast and direct SDK fence.
                    if (Cuda.foreignTensorCopyTarget(&ready.cb.backend) == null) return error.CanonicalCudaDestinationRequired;
                    const compute: *Cuda.CudaCompute = @ptrCast(@alignCast(ready.cb.backend.ptr));
                    {
                        var supervised = try control.enterUninterruptible(.uninterruptible);
                        defer supervised.deinit();
                        // The target stream already waits for the imported
                        // clone-ready event. Complete that real copy inside the
                        // profiler range without any readback or global policy.
                        try compute.ctx.synchronize();
                    }
                } else return error.SourceTraceProofUnsupported;
                try trace.stop(control);
            }
            const constructed = inference.platform.time.monotonicNs();
            if (audit_open) {
                inference.execution.transfer_audit.end();
                audit_open = false;
            }
            const native = if (decoder.native) |*value| value else return error.NativeDecoderRequired;
            if (options.source_metadata_proof and !std.meta.eql(transient(source_usage), transient(source_domain.controller().snapshot()))) return error.MetadataPromotionChangedSourceQuota;
            if (options.source_proof != .off) {
                if (comptime @hasDecl(Cuda, "foreignTensorCopyTarget")) {
                    if (Cuda.foreignTensorCopyTarget(&native.cb.backend) == null) return error.CanonicalCudaDestinationRequired;
                    const compute: *Cuda.CudaCompute = @ptrCast(@alignCast(source_backend.ptr));
                    const target_compute: *Cuda.CudaCompute = @ptrCast(@alignCast(native.cb.backend.ptr));
                    if ((compute.stats.cross_backend_copy_bytes - copy_before) + (target_compute.stats.cross_backend_copy_bytes - target_bytes_before) != source_bytes or (compute.stats.cross_backend_copies - calls_before) + (target_compute.stats.cross_backend_copies - target_calls_before) != 1 or audit.download_calls != 0 or audit.host_accesses != 0) return error.SourceCopyProofFailed;
                    const tensor: *const Cuda.CudaTensor = @ptrCast(@alignCast(native.encoder_hidden));
                    const detached = hasForeignClone(Cuda.CudaTensor, tensor);
                    if ((options.source_proof == .detached) != detached) return error.SourceReceiverModeNotSelected;
                    if (tensor.buffer.ptr == source_address or tensor.buffer.len != source_bytes) return error.SourceClonePhysicalAlias;
                    clone_address = tensor.buffer.ptr;
                    if (comptime @hasField(Cuda.CudaTensor, "foreign_clone_owner")) {
                        if (tensor.foreign_clone_owner) |clone| {
                            const exact = memory.AdmissionAmounts{ .backend_scratch_bytes = source_bytes };
                            if (clone.source.original.ptr != source_address or clone.source.original.len != source_bytes or clone.buffer.ptr != tensor.buffer.ptr or clone.buffer.len != source_bytes or clone.buffer.allocation_bytes != source_bytes or !tensor.buffer.borrowed) return error.SourcePhysicalOwnerProofFailed;
                            if (!std.meta.eql(clone.charge.lease.amounts, exact) or !std.meta.eql(clone.source.charge.lease.amounts, exact)) return error.SourceCloneQuotaNotExact;
                            if (clone.charge.lease.controller != target_domain.controller() or clone.source.charge.lease.controller != source_domain.controller()) return error.SourceCloneQuotaWrongDomain;
                        }
                    }
                }
                if (!std.meta.eql(transient(source_usage), transient(source_domain.controller().snapshot()))) return error.SourceQuotaDuplicated;
            }
            // Required logits readback belongs to the consumer, after the
            // separate strict encoder-transfer audit has ended.
            var row = try decoder.step(&.{ 0, 1 }, null, null);
            var row_open = true;
            defer if (row_open) row.deinit();
            if (row.logits.len != config.vocab_size) return error.InvalidLogitsShape;
            for (row.logits) |value| if (!std.math.isFinite(value)) return error.NonFiniteOutput;
            const measured_output = if (options.source_pipelined_steps == 0) row.logits else collected: {
                const values = try a.alloc(f32, row.logits.len + options.source_pipelined_steps * 2);
                errdefer a.free(values);
                @memcpy(values[0..row.logits.len], row.logits);
                try pipeline(native, options.source_pipelined_steps, values[row.logits.len..]);
                break :collected values;
            };
            defer if (options.source_pipelined_steps != 0) a.free(measured_output);
            if (reference) |values| {
                if (!std.mem.eql(u8, std.mem.sliceAsBytes(values), std.mem.sliceAsBytes(measured_output))) return error.SourceReceiverParityFailed;
            } else reference = try a.dupe(f32, measured_output);
            row.deinit();
            row_open = false;
            decoder.deinit();
            decoder_open = false;
            attempt_arena.deinit();
            attempt_arena_open = false;
            const returned = inference.platform.time.monotonicNs();
            try waitRetired(target_domain.controller(), target_initial, control);
            if (options.source_metadata_proof and !std.meta.eql(transient(source_usage), transient(source_domain.controller().snapshot()))) return error.MetadataPromotionChangedSourceQuota;
            const retired = inference.platform.time.monotonicNs();
            if (iteration >= options.warmup) {
                const sample = (iteration - options.warmup) * options.source_attempts + attempt;
                constructor_samples[sample] = constructed - start;
                return_samples[sample] = returned - start;
                retired_samples[sample] = retired - start;
            }
            // Logging/export is after Decoder/model lanes and retirement scopes.
            if (options.source_proof != .off) std.debug.print("source_receiver_proof attempt={d} expected={s} bytes={d} source_address=0x{x} clone_address=0x{x} transport_bytes_exact=true transport_calls=1 sdk_d2d_calls=external_trace_required constructor_trace={s} trace_completion_fence={s} transfer_download_calls=0 source_quota_once=true detached_clone_quota_exact={s} physical_owner_independent=true attempt_terminal_quota=baseline\n", .{ attempt, @tagName(options.source_proof), source_bytes, source_address, clone_address, if (options.source_trace_constructor) "cudaProfilerApi" else "off", if (options.source_trace_constructor) "diagnostic_target_stream_sync" else "off", if (options.source_proof == .detached) "true" else "not_applicable" });
        }
        const root_start = inference.platform.time.monotonicNs();
        owned.deinit();
        owned_open = false;
        arena.deinit();
        arena_open = false;
        try waitRetired(source_domain.controller(), source_initial, control);
        try waitRetired(target_domain.controller(), target_task_initial, control);
        const complete = inference.platform.time.monotonicNs();
        if (iteration >= options.warmup) {
            const index = iteration - options.warmup;
            root_retired_samples[index] = complete - root_start;
            lifecycle_samples[index] = complete - lifecycle_start;
            complete_task_samples[index] = complete - complete_task_start;
        }
    }
    for (reference.?) |value| checksum += value;
    if (options.output) |path| try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = path, .data = std.mem.sliceAsBytes(reference.?) });
    std.debug.print("unified_source_receiver backend={s} fixture={s} policy={s} shape={any} bytes={d} attempts={d} iterations={d} width={d} encoder_layers={d} decoder_layers={d} encoder_heads={d} decoder_heads={d} mel_bins={d} input_frames={d} output_elements={d} checksum={d:.9} parity=exact encoder_timing=complete_task_lifecycle request_arenas=destroyed terminal_quota=baseline\n", .{ @tagName(backend), @tagName(options.source_fixture_label), @tagName(options.source_policy), source_shape, source_bytes, options.source_attempts, options.iterations, config.d_model, encoder_config.encoder_layers, config.decoder_layers, encoder_config.encoder_attention_heads, config.decoder_attention_heads, encoder_config.num_mel_bins, options.seq_len, reference.?.len, checksum });
    if (options.source_metadata_proof) {
        std.debug.print("source_metadata_proof backend=metal policy=qualified promotion=actual_foreign_source Engine_retirement=before_receiver_admission promotion_device_copies=0 promotion_uploads=0 promotion_downloads=0 promotion_host_accesses=0 source_quota=unchanged source_and_target_terminal_quota=baseline scope=promotion_only receiver_transport=separate\n", .{});
        return; // Diagnostic completion proof, never timing distributions.
    }
    if (options.source_proof != .off) return; // Proof runs never publish timing samples.
    try writeSamples(a, "constructor", constructor_samples);
    try writeSamples(a, "first_consumer_and_return", return_samples);
    try writeSamples(a, "attempt_retired", retired_samples);
    try writeSamples(a, "source_root_retired", root_retired_samples);
    try writeSamples(a, "complete_receiver_lifecycle", lifecycle_samples);
    try writeSamples(a, "complete_task_lifecycle", complete_task_samples);
}
