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

//! Reproducible pretokenized encoder acceptance workload for #1166.
//! Use identical source on the base revision and candidate revision.
const std = @import("std");
const inference = @import("inference_internal");
const Tensor = inference.backends.Tensor;
const backend_choice = inference.native_backend_choice;

const Primitive = enum { norm_backward };

const Options = struct {
    primitive: ?Primitive = null,
    rows: usize = 17,
    dim: usize = 768,
    model_dir: []const u8 = "",
    backend: backend_choice.Choice = .native,
    batch: usize = 1,
    seq_len: usize = 16,
    warmup: usize = 3,
    iterations: usize = 20,
    output: ?[]const u8 = null,
    decoder_steps: usize = 0,
    diagnostics: bool = false,
    soak_seconds: u64 = 0,
    soak_pause_ms: u64 = 1000,
    soak_allocator: enum { smp, c } = .smp,
};

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    var options = Options{};
    while (args.next()) |arg| {
        const value = args.next() orelse return error.MissingArgument;
        if (std.mem.eql(u8, arg, "--primitive")) options.primitive = std.meta.stringToEnum(Primitive, value) orelse return error.InvalidArgument else if (std.mem.eql(u8, arg, "--rows")) options.rows = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, arg, "--dim")) options.dim = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, arg, "--model-dir")) options.model_dir = value else if (std.mem.eql(u8, arg, "--backend")) options.backend = backend_choice.parse(value) orelse return error.InvalidBackend else if (std.mem.eql(u8, arg, "--batch")) options.batch = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, arg, "--seq-len")) options.seq_len = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, arg, "--warmup-iters")) options.warmup = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, arg, "--measure-iters")) options.iterations = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, arg, "--output")) options.output = value else if (std.mem.eql(u8, arg, "--diagnostics")) options.diagnostics = if (std.mem.eql(u8, value, "true")) true else if (std.mem.eql(u8, value, "false")) false else return error.InvalidArgument else if (std.mem.eql(u8, arg, "--decoder-steps")) options.decoder_steps = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, arg, "--soak-seconds")) options.soak_seconds = try std.fmt.parseInt(u64, value, 10) else if (std.mem.eql(u8, arg, "--soak-pause-ms")) options.soak_pause_ms = try std.fmt.parseInt(u64, value, 10) else if (std.mem.eql(u8, arg, "--soak-allocator")) options.soak_allocator = std.meta.stringToEnum(@TypeOf(options.soak_allocator), value) orelse return error.InvalidArgument else return error.InvalidArgument;
    }
    if (options.primitive) |primitive| {
        if (options.backend != .native or options.rows == 0 or options.dim == 0 or options.iterations == 0 or options.soak_seconds != 0) return error.InvalidGeometry;
        switch (primitive) {
            .norm_backward => try runNormBackward(init, options),
        }
        return;
    }
    if (options.model_dir.len == 0 or options.batch == 0 or options.seq_len < 4 or options.iterations == 0) return error.InvalidGeometry;
    // Keep the timing allocator unchanged; the C backing isolates long-run
    // library ownership from the SMP allocator's unreleased free-list slabs.
    var bounded = inference.runtime.bounded_allocator.BoundedAllocator{ .backing = if (options.soak_allocator == .c) std.heap.c_allocator else std.heap.smp_allocator, .limit = 8 * 1024 * 1024 * 1024 };
    const a = if (options.soak_seconds != 0) bounded.allocator() else std.heap.smp_allocator;
    defer if (options.soak_seconds != 0) {
        std.debug.print("soak_allocator_final live={d} peak={d} denied={}\n", .{ bounded.live, bounded.peak, bounded.denied });
        std.debug.assert(bounded.live == 0);
    };
    const setup_started = inference.platform.time.awakeNs(init.io);
    try backend_choice.validate(options.backend);
    var sessions = inference.backends.SessionManager.initWithIo(a, init.io);
    backend_choice.configureSessionPreference(&sessions, options.backend);
    var manager = inference.server.model_manager.ModelManager.init(a, sessions);
    defer manager.deinit();
    const model = try manager.loadFromDir(options.model_dir);
    const model_loaded = inference.platform.time.awakeNs(init.io);
    const session = model.session;
    if (options.decoder_steps != 0) {
        try runDecoder(init, model, a, options, &bounded);
        return;
    }
    if (@hasDecl(inference.backends.Session, "configureExecutionDiagnostics")) _ = session.configureExecutionDiagnostics(options.diagnostics);
    const count = try std.math.mul(usize, options.batch, options.seq_len);
    const ids = try a.alloc(i64, count);
    defer a.free(ids);
    const mask = try a.alloc(i64, count);
    defer a.free(mask);
    const positions = try a.alloc(i64, count);
    defer a.free(positions);
    const types = try a.alloc(i64, count);
    defer a.free(types);
    @memset(types, 0);
    for (ids, mask, positions, 0..) |*id, *valid, *position, index| {
        const column = index % options.seq_len;
        const padding = column >= options.seq_len - 2;
        id.* = if (padding) 0 else if (column == 0) 101 else if (column == options.seq_len - 3) 102 else @intCast(1000 + column % 1000);
        valid.* = if (padding) 0 else 1;
        position.* = @intCast(column);
    }
    var inputs: std.ArrayListUnmanaged(Tensor) = .empty;
    defer {
        for (inputs.items) |*input| input.deinit();
        inputs.deinit(a);
    }
    for (session.inputInfo()) |info| {
        var shape = [_]i64{ @intCast(options.batch), @intCast(options.seq_len), 0 };
        var rank: usize = 2;
        if (info.shape.len == 1) {
            shape[0] = @intCast(count);
            rank = 1;
        }
        const data: []const i64 = if (std.mem.indexOf(u8, info.name, "position") != null) positions else if (std.mem.indexOf(u8, info.name, "token_type") != null) types else if ((std.mem.indexOf(u8, info.name, "mask") != null or std.mem.indexOf(u8, info.name, "bias") != null)) mask else if (std.mem.indexOf(u8, info.name, "input_ids") != null or std.mem.eql(u8, info.name, "ids")) ids else return error.UnknownInput;
        if (info.dtype == .f32 and (std.mem.indexOf(u8, info.name, "bias") != null or info.shape.len == 3)) {
            const heads = model.manifest.num_attention_heads;
            const bias_count = try std.math.mul(usize, count, @as(usize, heads) * options.seq_len);
            const bias = try a.alloc(f32, bias_count);
            defer a.free(bias);
            for (bias, 0..) |*value, index| value.* = if (index % options.seq_len >= options.seq_len - 2) -10000 else 0;
            shape = .{ @intCast(options.batch * heads), @intCast(options.seq_len), @intCast(options.seq_len) };
            try inputs.append(a, try Tensor.initFloat32(a, info.name, &shape, bias));
        } else if (info.dtype == .i64) {
            try inputs.append(a, try Tensor.initInt64(a, info.name, shape[0..rank], data));
        } else return error.UnsupportedInputType;
    }
    const inputs_ready = inference.platform.time.awakeNs(init.io);
    var first_warmup_ns: u64 = 0;
    for (0..options.warmup) |warmup_index| {
        const outputs = try session.run(inputs.items, a);
        for (outputs) |*output| output.deinit();
        a.free(outputs);
        if (warmup_index == 0) first_warmup_ns = inference.platform.time.awakeNs(init.io) - inputs_ready;
    }
    std.debug.print("unified_setup load_ns={d} inputs_ns={d} first_warmup_ns={d}\n", .{ model_loaded - setup_started, inputs_ready - model_loaded, first_warmup_ns });
    if (options.soak_seconds != 0) {
        try runSoak(init, session, inputs.items, a, &bounded, options);
        return;
    }
    const before = providerSnapshot(session, a);
    const samples = try a.alloc(u64, options.iterations);
    defer a.free(samples);
    var checksum: f64 = 0;
    var output_count: usize = 0;
    for (samples, 0..) |*elapsed, iteration| {
        const start = inference.platform.time.awakeNs(init.io);
        const outputs = try session.run(inputs.items, a);
        elapsed.* = inference.platform.time.awakeNs(init.io) - start;
        defer {
            for (outputs) |*output| output.deinit();
            a.free(outputs);
        }
        checksum = 0;
        output_count = 0;
        for (outputs) |output| {
            for (output.asFloat32()) |value| {
                if (!std.math.isFinite(value)) return error.NonFiniteOutput;
                checksum += value;
                output_count += 1;
            }
        }
        if (iteration + 1 == samples.len) {
            if (options.output) |path| {
                if (outputs.len != 1) return error.MultipleOutputs;
                try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = path, .data = outputs[0].data });
            }
        }
    }
    std.mem.sort(u64, samples, {}, std.sort.asc(u64));
    const after = providerSnapshot(session, a);
    std.debug.print("unified_execution backend={s} batch={d} seq={d} iterations={d} output_elements={d} checksum={d:.9} p50_ns={d} p95_ns={d} min_ns={d}\n", .{ @tagName(session.backend()), options.batch, options.seq_len, options.iterations, output_count, checksum, samples[samples.len / 2], samples[@min(samples.len - 1, samples.len * 95 / 100)], samples[0] });
    std.debug.print("memory device_live={d} device_peak={d} runtime={d} host_mirror_live={d} host_mirror_peak={d} downloads={d} download_bytes={d} device_allocations={d}\n", .{ after.metal_tensor_device_owned_live_bytes, after.metal_tensor_device_owned_peak_live_bytes, after.metal_runtime_total_bytes, after.metal_tensor_host_mirror_live_bytes, after.metal_tensor_host_mirror_peak_live_bytes, after.metal_tensor_to_host_device_calls - before.metal_tensor_to_host_device_calls, after.metal_tensor_host_mirror_download_bytes - before.metal_tensor_host_mirror_download_bytes, after.metal_tensor_device_owned_buffers_created - before.metal_tensor_device_owned_buffers_created });
    std.debug.print("kernels qkv={d} qkv_fallback={d} ffn={d} ffn_fallback={d} compute_encoders={d} blit_encoders={d} commands={d}\n", .{ after.metal_runtime_dense_qkv_packed_calls - before.metal_runtime_dense_qkv_packed_calls, after.metal_runtime_dense_qkv_packed_fallbacks - before.metal_runtime_dense_qkv_packed_fallbacks, after.metal_runtime_deberta_ffn_fused_calls - before.metal_runtime_deberta_ffn_fused_calls, after.metal_runtime_deberta_ffn_fused_fallbacks - before.metal_runtime_deberta_ffn_fused_fallbacks, after.metal_runtime_last_frame_compute_encoder_count, after.metal_runtime_last_frame_blit_encoder_count, after.metal_runtime_last_frame_planned_command_op_count });
    if (@hasDecl(inference.backends.Session, "executionDiagnostics")) {
        if (session.executionDiagnostics()) |snapshot| {
            const stats = snapshot.execution;
            std.debug.print("execution_actual upload_calls={d} upload_bytes={d} download_calls={d} download_bytes={d} device_host_accesses={d} device_host_access_bytes={d}\n", .{ stats.actual_upload_calls, stats.actual_upload_bytes, stats.actual_download_calls, stats.actual_download_bytes, stats.device_host_accesses, stats.device_host_access_bytes });
            std.debug.print("execution assigned={d} storage_allocations={d} storage_bytes={d} storage_reuses={d} busy_fallbacks={d} transfers={d} planned_transfers={d} planned_transfer_bytes={d} interpreter_fallbacks={d} regions={d} commands={d}\n", .{ stats.assigned_kernel_dispatches, stats.physical_storage_allocations, stats.physical_storage_bytes, stats.physical_storage_reuses, stats.physical_frame_busy_fallbacks, stats.cross_device_transfers, stats.physical_transfer_dispatches, stats.physical_transfer_bytes, stats.interpreter_fallbacks, stats.runtime_region_plan_dispatches, stats.backend_command_dispatches });
        }
    }
}

fn providerSnapshot(session: inference.backends.Session, allocator: std.mem.Allocator) inference.ops.NativeQuantTimingStats {
    if (@hasDecl(inference.backends.Session, "executionDiagnostics")) {
        if (session.executionDiagnostics()) |snapshot| return snapshot.provider;
    }
    if (inference.backends.imported_onnx_session.sharedBackendContext(session)) |shared| {
        if (@hasDecl(@TypeOf(shared.*), "stableBackend")) return shared.stableBackend().debugTimingSnapshot().provider;
        // The baseline's context predates the public common backend contract.
        // Read its existing provider directly without owning/destroying it.
        switch (shared.ctx) {
            inline else => |context| {
                if (@typeInfo(@typeInfo(@TypeOf(context.compute)).pointer.child) != .@"opaque") {
                    const backend = context.compute.computeBackend();
                    return backend.debugTimingSnapshot().provider;
                }
            },
        }
        return .{};
    }
    // This handle owns the backend execution lane until deinit. Never retain
    // it across a measured session invocation.
    var backend = inference.architectures.session_factory.getComputeBackend(session, allocator) catch return .{};
    defer backend.deinit();
    return backend.debugTimingSnapshot().provider;
}

const ResidentRunResult = @typeInfo(@TypeOf(inference.backends.Session.runResident)).@"fn".return_type.?;

fn auditedResidentRun(session: inference.backends.Session, inputs: []const Tensor, allocator: std.mem.Allocator) ResidentRunResult {
    if (@hasDecl(inference, "execution")) {
        const audit_module = inference.execution.transfer_audit;
        var audit = audit_module.Audit{ .forbid_device_host_access = true };
        try audit_module.begin(&audit);
        defer audit_module.end();
        return session.runResident(inputs, allocator);
    }
    return session.runResident(inputs, allocator);
}

/// Persistent-session numerical and ownership soak. Samples stay bounded and
/// retained resident outputs deliberately exercise frame pressure and recovery.
fn runSoak(init: std.process.Init, session: inference.backends.Session, inputs: []const Tensor, a: std.mem.Allocator, bounded: *inference.runtime.bounded_allocator.BoundedAllocator, options: Options) !void {
    const alternate = try a.alloc(Tensor, inputs.len);
    var initialized: usize = 0;
    defer {
        for (alternate[0..initialized]) |*input| input.deinit();
        a.free(alternate);
    }
    var shifted_ids = false;
    for (inputs, alternate) |input, *target| {
        if (std.mem.indexOf(u8, input.name, "input_ids") != null or std.mem.eql(u8, input.name, "ids")) {
            if (input.dtype != .i64) return error.UnsupportedAlternateInput;
            const shifted = try a.dupe(i64, input.asInt64());
            defer a.free(shifted);
            for (shifted) |*value| if (value.* != 0) {
                value.* = try std.math.add(i64, value.*, 17);
            };
            target.* = try Tensor.initInt64(a, input.name, input.shape, shifted);
            shifted_ids = true;
        } else target.* = input.borrowedView(input.name);
        initialized += 1;
    }
    if (!shifted_ids) return error.SoakRequiresTokenIds;
    const first_reference = try session.run(inputs, a);
    defer {
        for (first_reference) |*output| output.deinit();
        a.free(first_reference);
    }
    const second_reference = try session.run(alternate, a);
    defer {
        for (second_reference) |*output| output.deinit();
        a.free(second_reference);
    }
    if (first_reference.len != second_reference.len) return error.SoakOutputCountChanged;
    var distinct = false;
    for (first_reference, second_reference) |first, second| if (!std.mem.eql(u8, first.data, second.data)) {
        distinct = true;
    };
    if (!distinct) return error.SoakInputsNotDistinct;
    const references = [2][]Tensor{ first_reference, second_reference };
    const input_sets = [2][]const Tensor{ inputs, alternate };
    const Resident = @typeInfo(@typeInfo(ResidentRunResult).error_union.payload).optional.child;
    const Held = struct { value: Resident, reference: usize };
    var held = std.ArrayListUnmanaged(Held).empty;
    defer {
        for (held.items) |*output| output.value.deinit();
        held.deinit(a);
    }
    const start = inference.platform.time.awakeNs(init.io);
    const duration = try std.math.mul(u64, options.soak_seconds, std.time.ns_per_s);
    var iterations: u64 = 0;
    var steady_host: usize = 0;
    var steady_device: u64 = 0;
    var steady_mirror: u64 = 0;
    while (inference.platform.time.awakeNs(init.io) - start < duration) {
        const reference_index: usize = @intCast(iterations % 2);
        const reference = references[reference_index];
        const request_inputs = input_sets[reference_index];
        {
            const outputs = try session.run(request_inputs, a);
            defer {
                for (outputs) |*output| output.deinit();
                a.free(outputs);
            }
            if (outputs.len != reference.len) return error.SoakOutputCountChanged;
            for (outputs, reference) |output, expected| {
                if (!std.mem.eql(u8, output.data, expected.data)) return error.SoakOutputChanged;
            }
        }
        // Retaining five results exceeds the four-frame plan pool. Both the
        // compatibility fallback and recovery must preserve published values.
        if (iterations % 8 < 5) {
            if (try auditedResidentRun(session, request_inputs, a)) |resident| {
                var owned = resident;
                errdefer owned.deinit();
                try held.append(a, .{ .value = resident, .reference = reference_index });
            }
        } else {
            for (held.items) |*retained| {
                const resident = &retained.value;
                const expected_outputs = references[retained.reference];
                if (resident.outputs.len != expected_outputs.len) return error.SoakOutputCountChanged;
                for (resident.outputs, expected_outputs) |value, expected| {
                    const data = try resident.backend.toFloat32(value, a);
                    defer a.free(data);
                    if (!std.mem.eql(f32, data, expected.asFloat32())) return error.SoakResidentOutputChanged;
                }
                resident.deinit();
            }
            held.clearRetainingCapacity();
        }
        iterations += 1;
        if (iterations % 8 == 0) {
            const provider = providerSnapshot(session, a);
            // Backend workers are quiescent at request completion. Lock the
            // accounting owner anyway so diagnostics never race a prefetcher.
            while (!bounded.mutex.tryLock()) std.atomic.spinLoopHint();
            const host_live = bounded.live;
            const host_peak = bounded.peak;
            bounded.mutex.unlock();
            if (iterations == 128) {
                steady_host = host_live;
                steady_device = provider.metal_tensor_device_owned_live_bytes;
                steady_mirror = provider.metal_tensor_host_mirror_live_bytes;
            }
            if (iterations > 128) {
                if (host_live > steady_host + 1024 * 1024) return error.SoakHostMemoryGrowth;
                if (provider.metal_tensor_device_owned_live_bytes > steady_device + 1024 * 1024) return error.SoakDeviceMemoryGrowth;
                if (provider.metal_tensor_host_mirror_live_bytes > steady_mirror + 1024 * 1024) return error.SoakHostMirrorGrowth;
            }
            if (iterations % 128 == 0) std.debug.print("soak_progress backend={s} iterations={d} elapsed_s={d} host_live={d} host_peak={d} host_mirror_live={d} device_live={d} device_peak={d}\n", .{ @tagName(session.backend()), iterations, (inference.platform.time.awakeNs(init.io) - start) / std.time.ns_per_s, host_live, host_peak, provider.metal_tensor_host_mirror_live_bytes, provider.metal_tensor_device_owned_live_bytes, provider.metal_tensor_device_owned_peak_live_bytes });
        }
        if (options.soak_pause_ms != 0) try init.io.sleep(.fromMilliseconds(@intCast(options.soak_pause_ms)), .awake);
    }
    std.debug.print("soak_complete backend={s} iterations={d} elapsed_s={d} strict_resident={} alternating_inputs=true\n", .{ @tagName(session.backend()), iterations, (inference.platform.time.awakeNs(init.io) - start) / std.time.ns_per_s, @hasDecl(inference, "execution") });
}

fn runDecoder(init: std.process.Init, model: *inference.server.model_manager.LoadedModel, a: std.mem.Allocator, options: Options, bounded: *inference.runtime.bounded_allocator.BoundedAllocator) !void {
    if (options.batch != 1) return error.UnsupportedDecoderBatch;
    var executor = (try model.wholeModelExecutor(a, null)) orelse return error.UnsupportedModelRuntime;
    defer executor.deinit();
    // The serving request owns the provider lane while its runtime borrows it.
    var request_backend = try inference.architectures.session_factory.getComputeBackend(model.session, a);
    defer request_backend.deinit();
    var runtime = try executor.createRuntime(a);
    var runtime_live = true;
    defer if (runtime_live) runtime.deinit();
    const config = inference.architectures.session_factory.getGptConfig(model.session) orelse return error.MissingDecoderConfig;
    const prompt = try a.alloc(i64, options.seq_len);
    defer a.free(prompt);
    for (prompt, 0..) |*token, index| token.* = @intCast(1 + index % @max(@as(usize, 1), config.vocab_size - 1));
    _ = try runtime.prepare(a, .{ .kv_tokens_hint = options.seq_len + options.decoder_steps });
    const samples = try a.alloc(u64, options.iterations);
    defer a.free(samples);
    var reference = std.ArrayListUnmanaged(f32).empty;
    defer reference.deinit(a);
    const soak_started = inference.platform.time.awakeNs(init.io);
    const soak_duration = try std.math.mul(u64, options.soak_seconds, std.time.ns_per_s);
    var steady_host: usize = 0;
    var steady_device: u64 = 0;
    var steady_mirror: u64 = 0;
    var iteration: usize = 0;
    while (iteration < options.warmup + options.iterations or
        (options.soak_seconds != 0 and inference.platform.time.awakeNs(init.io) - soak_started < soak_duration)) : (iteration += 1)
    {
        if (options.soak_seconds != 0 and iteration != 0 and iteration % 256 == 0) {
            runtime.deinit();
            runtime_live = false;
            runtime = try executor.createRuntime(a);
            runtime_live = true;
            _ = try runtime.prepare(a, .{ .kv_tokens_hint = options.seq_len + options.decoder_steps });
        }
        try runtime.reset();
        const start = inference.platform.time.awakeNs(init.io);
        var values = std.ArrayListUnmanaged(f32).empty;
        defer values.deinit(a);
        {
            var output = try runtime.prefill(a, .{ .input_ids = prompt, .seq_len = prompt.len, .query_seq_len = prompt.len, .attention_mode = .paged_prefill });
            defer output.deinit(a);
            const logits = try output.hostLogits(a);
            if (logits.len != config.vocab_size) return error.InvalidDecoderOutput;
            try values.appendSlice(a, logits);
        }
        for (0..options.decoder_steps) |step| {
            // Fixed tokens make full-logit comparisons meaningful across
            // backends even when two nearly tied argmax values differ.
            var output = try runtime.decode(a, .{ .token_id = @intCast(11 + step), .position = prompt.len + step });
            defer output.deinit(a);
            const logits = try output.hostLogits(a);
            if (logits.len != config.vocab_size) return error.InvalidDecoderOutput;
            try values.appendSlice(a, logits);
        }
        const elapsed = inference.platform.time.awakeNs(init.io) - start;
        for (values.items) |value| if (!std.math.isFinite(value)) return error.NonFiniteOutput;
        if (iteration == 0) try reference.appendSlice(a, values.items) else if (!std.mem.eql(f32, reference.items, values.items)) return error.DecoderResetOutputChanged;
        if (iteration >= options.warmup and iteration - options.warmup < samples.len) samples[iteration - options.warmup] = elapsed;
        if (options.soak_seconds != 0) {
            if ((iteration + 1) % 128 == 0) {
                while (!bounded.mutex.tryLock()) std.atomic.spinLoopHint();
                const host_live = bounded.live;
                const host_peak = bounded.peak;
                bounded.mutex.unlock();
                const provider = request_backend.debugTimingSnapshot().provider;
                if (iteration + 1 == 128) {
                    steady_host = host_live;
                    steady_device = provider.metal_tensor_device_owned_live_bytes;
                    steady_mirror = provider.metal_tensor_host_mirror_live_bytes;
                } else {
                    if (host_live > steady_host + 1024 * 1024) return error.SoakHostMemoryGrowth;
                    if (provider.metal_tensor_device_owned_live_bytes > steady_device + 1024 * 1024) return error.SoakDeviceMemoryGrowth;
                    if (provider.metal_tensor_host_mirror_live_bytes > steady_mirror + 1024 * 1024) return error.SoakHostMirrorGrowth;
                }
                std.debug.print("decoder_soak_progress backend={s} iterations={d} elapsed_s={d} host_live={d} host_peak={d} host_mirror_live={d} device_live={d} device_peak={d} runtime_evictions={d}\n", .{ @tagName(model.session.backend()), iteration + 1, (inference.platform.time.awakeNs(init.io) - soak_started) / std.time.ns_per_s, host_live, host_peak, provider.metal_tensor_host_mirror_live_bytes, provider.metal_tensor_device_owned_live_bytes, provider.metal_tensor_device_owned_peak_live_bytes, iteration / 256 });
            }
            if (options.soak_pause_ms != 0) try init.io.sleep(.fromMilliseconds(@intCast(options.soak_pause_ms)), .awake);
        }
    }
    if (options.soak_seconds != 0) std.debug.print("decoder_soak_complete backend={s} iterations={d} elapsed_s={d} runtime_evictions={d}\n", .{ @tagName(model.session.backend()), iteration, (inference.platform.time.awakeNs(init.io) - soak_started) / std.time.ns_per_s, (iteration - 1) / 256 });
    if (options.output) |path| try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = path, .data = std.mem.sliceAsBytes(reference.items) });
    // Chunked prefill must preserve the same state/positions as a whole prompt.
    try runtime.reset();
    const split = prompt.len / 2;
    {
        var first = try runtime.prefill(a, .{ .input_ids = prompt[0..split], .seq_len = split, .query_seq_len = split, .attention_mode = .paged_prefill });
        defer first.deinit(a);
        var second = try runtime.prefill(a, .{ .input_ids = prompt[split..], .seq_len = prompt.len, .query_seq_len = prompt.len - split, .attention_mode = .paged_prefill });
        defer second.deinit(a);
        const logits = try second.hostLogits(a);
        if (logits.len != config.vocab_size) return error.InvalidDecoderOutput;
        var max_delta: f32 = 0;
        var squared: f64 = 0;
        for (logits, reference.items[0..config.vocab_size]) |actual, expected| {
            const delta = @abs(actual - expected);
            max_delta = @max(max_delta, delta);
            squared += @as(f64, delta) * delta;
        }
        std.debug.print("decoder_chunked_prefill max_abs={d:.9} rms={d:.9}\n", .{ max_delta, @sqrt(squared / @as(f64, @floatFromInt(logits.len))) });
        if (max_delta > 1e-4) return error.ChunkedPrefillOutputChanged;
    }
    std.mem.sort(u64, samples, {}, std.sort.asc(u64));
    var checksum: f64 = 0;
    for (reference.items) |value| checksum += value;
    const stats = runtime.debugTimingStats();
    std.debug.print("unified_decoder backend={s} prompt={d} decode_steps={d} iterations={d} output_elements={d} checksum={d:.9} p50_ns={d} p95_ns={d} min_ns={d} reset_parity=exact chunked_prefill_tolerance=0.0001\n", .{ @tagName(model.session.backend()), prompt.len, options.decoder_steps, options.iterations, reference.items.len, checksum, samples[samples.len / 2], samples[@min(samples.len - 1, samples.len * 95 / 100)], samples[0] });
    std.debug.print("decoder_kernels prefill_calls={d} decode_greedy_calls={d} prepared_hits={d} runtime_prepared_hits={d}\n", .{ stats.prefill_calls, stats.decode_greedy_calls, stats.ensure_prepared_fast_hits, stats.runtime_prepare_fast_hits });
}

// Isolate tiny primitive timings from the CLI's other mode frames. Aligning
// the state also fixes the stack alignment inherited from that caller.
noinline fn runNormBackward(init: std.process.Init, options: Options) !void {
    const a = std.heap.smp_allocator;
    const native = inference.native_compute.native;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    defer store.deinitOwned();
    var compute: native.NativeCompute align(64) = native.NativeCompute.initWithIo(a, &store, null, init.io);
    defer compute.deinit();
    const backend = compute.computeBackend();
    const count = try std.math.mul(usize, options.rows, options.dim);
    const x = try a.alloc(f32, count);
    defer a.free(x);
    const dy = try a.alloc(f32, count);
    defer a.free(dy);
    const gamma = try a.alloc(f32, options.dim);
    defer a.free(gamma);
    const beta = try a.alloc(f32, options.dim);
    defer a.free(beta);
    @memset(beta, 0);
    for (x, dy, 0..) |*value, *gradient, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 37)) - 18)) * 0.125;
        gradient.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 23)) - 11)) * 0.0625;
    }
    for (gamma, 0..) |*value, i| value.* = 0.5 + @as(f32, @floatFromInt(i % 5)) * 0.125;
    const input = try backend.fromFloat32(x);
    defer backend.free(input);
    const upstream = try backend.fromFloat32(dy);
    defer backend.free(upstream);
    const weight = try backend.fromFloat32(gamma);
    defer backend.free(weight);
    const bias = try backend.fromFloat32(beta);
    defer backend.free(bias);
    const samples = try a.alloc(u64, options.iterations);
    defer a.free(samples);
    var checksum: f64 = 0;
    var elements: usize = 0;
    // Batch tiny invocations so timer granularity cannot dominate allocation
    // and arithmetic costs. Both revisions use this identical harness.
    const repeats: u64 = if (count <= 1024) 1024 else 1;
    for (0..options.warmup + options.iterations) |iteration| {
        const start = inference.platform.time.awakeNs(init.io);
        for (0..repeats - 1) |_| {
            const repeated = (try backend.layerNormBackward(input, weight, bias, upstream, options.dim, 1e-5)) orelse return error.UnsupportedOperation;
            backend.free(repeated);
        }
        const result = (try backend.layerNormBackward(input, weight, bias, upstream, options.dim, 1e-5)) orelse return error.UnsupportedOperation;
        const elapsed = (inference.platform.time.awakeNs(init.io) - start) / repeats;
        defer backend.free(result);
        if (iteration >= options.warmup) samples[iteration - options.warmup] = elapsed;
        if (iteration + 1 == options.warmup + options.iterations) {
            const values = try backend.toFloat32(result, a);
            defer a.free(values);
            elements = values.len;
            for (values) |value| {
                if (!std.math.isFinite(value)) return error.NonFiniteOutput;
                checksum += value;
            }
            if (options.output) |path| try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = path, .data = std.mem.sliceAsBytes(values) });
        }
    }
    std.mem.sort(u64, samples, {}, std.sort.asc(u64));
    std.debug.print("unified_primitive primitive=norm_backward backend=native rows={d} dim={d} iterations={d} output_elements={d} checksum={d:.9} p50_ns={d} p95_ns={d} min_ns={d}\n", .{ options.rows, options.dim, options.iterations, elements, checksum, samples[samples.len / 2], samples[@min(samples.len - 1, samples.len * 95 / 100)], samples[0] });
}
