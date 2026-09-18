// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Diagnostic worker over the production trainer. Snapshots are explicitly
//! outside measured steps; no alternative optimizer or training graph lives here.
const std = @import("std");
const inference = @import("inference_internal");
const native = inference.finetune.gliner_boundary_native_trainer;
const sources = inference.finetune.gliner_boundary_training_source;
const data = inference.finetune.gliner_boundary_dataset;
const job = inference.finetune.gliner_boundary_training_job;
const processor = inference.pipelines.gliner_boundary_processor;
const Allocator = std.mem.Allocator;
const scope = "gliner25_cuda_training_comparison_v1";
comptime {
    if (!@import("build_options").enable_cuda or @import("builtin").mode != .ReleaseFast)
        @compileError("CUDA training benchmark requires CUDA and ReleaseFast");
}
const Command = struct { request_id: u32, op: enum { validate, run, stop }, case_id: []const u8 = "" };
fn now() !u64 {
    var ts: std.posix.timespec = undefined;
    if (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts)) != .SUCCESS) return error.ClockUnavailable;
    return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec);
}
fn emit(a: Allocator, out: *std.Io.Writer, value: anytype) !void {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    if (bytes.len > 4 * 1024 * 1024) return error.ResponseLimitExceeded;
    try out.writeAll(bytes);
    try out.writeByte('\n');
    try out.flush();
}
fn sync(owner: *native.Trainer) !void {
    try inference.native_compute.cuda.gliner25_api.synchronizeAndDrainDeferredDeviceFrees(owner.backend.cuda_backend.?);
}
fn benchmarkName(name: []const u8) []const u8 {
    // Removing the published classifier's Dropout changes its Sequential
    // index in the native zero-dropout graph. Preserve canonical tensor names
    // in receipts and alias only this derived, in-memory parameter inventory.
    if (std.mem.eql(u8, name, "classifier.3.weight")) return "classifier.2.weight";
    if (std.mem.eql(u8, name, "classifier.3.bias")) return "classifier.2.bias";
    return name;
}
fn create(a: Allocator, source: *sources.Source, dataset: *const data.Dataset, config: job.Config) !*native.Trainer {
    var model_config = source.config;
    model_config.encoder.hidden_dropout_prob = 0;
    model_config.encoder.attention_probs_dropout_prob = 0;
    model_config.head.dropout = 0;
    // No cross-framework RNG equivalence is assumed. All golds are injected,
    // all negative queries admitted, and schema/data order is fixed.
    // Zero disables query subsampling. A ratio of one only retains as many
    // absent queries as positives and is insufficient for arbitrary datasets.
    model_config.head.negative_query_ratio = 0;
    const parameters = try a.dupe(@typeInfo(@TypeOf(source.parameters)).pointer.child, source.parameters);
    defer a.free(parameters);
    for (parameters) |*parameter| parameter.name = benchmarkName(parameter.name);
    return native.Trainer.init(a, &source.store, source.tokenizer(), source.identity, model_config, dataset, parameters, .{
        .run = config.run,
        .execution = .resident_cuda,
        .attention_profile = config.attention_profile,
        .activation_profile = config.activation_profile,
        .source_reserved_bytes = source.reservedBytes(),
        .processor = .{ .max_batch_items = config.run.batch_size, .max_text_words = config.tokenization.max_text_words, .max_sequence_tokens = config.tokenization.max_sequence_tokens, .max_queries = config.tokenization.max_queries },
        .gold_start = 1,
        .gold_end = 1,
        .weights = config.weights,
        .peft = config.peft,
        .limits = try job.trainerLimits(config),
    }, null);
}
const Slot = struct { name: []const u8, canonical_name: []const u8, shape: []const i32, elements: usize, offset: usize, present: bool, adam_step: u32, group: usize };
// Opt-in diagnostic commands sample the existing execution-control checkpoints.
// These are host phase intervals (GPU work may cross a phase boundary), and are
// deliberately absent from the paired throughput command path.
const PhaseProfile = struct {
    owner: *native.Trainer,
    phase: native.MemoryPhase,
    start_ns: u64,
    last_ns: u64,
    calls: usize = 0,
    decision_ns: [4]?u64 = @splat(null),
    phase_ns: [@typeInfo(native.MemoryPhase).@"enum".fields.len]u64 = @splat(0),

    fn sample(raw: ?*anyopaque) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.calls += 1;
        // next() and this callback run synchronously on the same worker.
        const phase = self.owner.memory_failures.phase;
        if (phase == self.phase and self.calls % 128 != 0) return;
        const timestamp = try now();
        self.phase_ns[@intFromEnum(self.phase)] += timestamp - self.last_ns;
        self.last_ns = timestamp;
        self.phase = phase;
    }
    fn decision(raw: *anyopaque, event: native.DecisionEvents.Event) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const index = @intFromEnum(std.meta.activeTag(event));
        if (self.decision_ns[index] == null) self.decision_ns[index] = (try now()) - self.start_ns;
    }
};
const TraceCapture = struct {
    const Tensor = struct { name: []const u8, kind: []const u8, elements: usize, offset: usize };
    const Norm = struct { rows: u32, dim: u32, eps: f32, offsets: [5]usize };
    budget: inference.runtime.bounded_allocator.BoundedAllocator,
    events: std.ArrayList([]const u8) = .empty,
    tensors: std.ArrayList(Tensor) = .empty,
    seen: std.AutoHashMapUnmanaged(u64, void) = .empty,
    file: ?std.Io.File = null,
    io: std.Io = undefined,
    owner: ?*native.Trainer = null,
    source: ?*sources.Source = null,
    written: usize = 0,
    records: bool = false,
    backward_file: ?std.Io.File = null,
    backward_written: usize = 0,
    norms: std.ArrayList(Norm) = .empty,
    fn init(a: Allocator, json_bytes: usize) @This() {
        return .{ .budget = .{ .backing = a, .limit = json_bytes } };
    }
    fn deinit(self: *@This()) void {
        const a = self.budget.allocator();
        for (self.events.items) |event| a.free(event);
        self.events.deinit(a);
        self.tensors.deinit(a);
        self.seen.deinit(a);
        self.norms.deinit(a);
    }
    fn record(self: *@This(), value: anytype) !void {
        const a = self.budget.allocator();
        const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
        errdefer a.free(bytes);
        try self.events.append(a, bytes);
    }
    fn inputs(raw: *anyopaque, value: native.DecisionEvents.BoundaryInputs) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.record(.{ .kind = "boundary_inputs", .value = value });
    }
    fn decision(raw: *anyopaque, event: native.DecisionEvents.Event) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        switch (event) {
            .boundary_loss => |value| try self.record(.{ .kind = "boundary_loss", .gradients = value.gradients, .hard_negative_mask = value.hard_negative_mask, .pair_query_mask = value.pair_query_mask }),
            .record => |value| if (self.records) {
                const t = value.target;
                const m = value.matches;
                // Serialize only borrowed semantic data, never allocator state.
                // The existing trace allocator and response caps bound this.
                try self.record(.{ .kind = "record_inputs", .group = value.group, .sample = value.sample, .schema_group = value.schema_group, .weight = value.loss_weight, .value = .{
                    .mode = t.mode,
                    .fields = t.fields,
                    .candidate_spans = t.candidate_spans,
                    .candidate_valid = t.candidate_valid,
                    .field_membership = t.field_membership,
                    .instance_mask = t.instance_mask,
                    .anchor_field = t.anchor_field,
                    .anchor_candidates = t.anchor_candidates,
                    .record_indices = t.record_indices,
                    .gold_indicator = t.gold_indicator,
                    .pairs = m.pairs,
                    .object_targets = m.object_targets,
                    .object_mask = m.object_mask,
                    .objects = value.logits.objects,
                    .assignments = value.logits.assignments,
                } });
            },
            else => {},
        }
    }
    fn retained(raw: *anyopaque, value: native.DecisionEvents.RetainedValues) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const a = self.budget.allocator();
        const file = self.file orelse return;
        for (0..value.graph.nodeCount()) |index| {
            const node = value.graph.node(@intCast(index));
            switch (node.op) {
                .fused_linear, .fused_layer_norm => {},
                else => continue,
            }
            const bias = value.graph.node(node.inputs[2]);
            if (bias.op != .parameter) continue;
            const name = value.graph.parameterName(bias);
            var canonical: ?[]const u8 = null;
            for (self.source.?.parameters) |parameter| if (std.mem.eql(u8, benchmarkName(parameter.name), name)) {
                canonical = parameter.canonical_name;
                break;
            };
            const full_name = canonical orelse continue;
            if (!std.mem.endsWith(u8, full_name, ".bias")) continue;
            const module = full_name[0 .. full_name.len - 5];
            for ([_]u32{ node.inputs[0], @intCast(index) }, 0..) |original_id, role| {
                const key = @as(u64, @intCast(index)) * 2 + role;
                if (self.seen.contains(key) or original_id >= value.id_map.len) continue;
                const mapped = value.id_map[original_id];
                const slot = std.mem.indexOfScalar(u32, value.ids, mapped) orelse continue;
                const tensor = value.values[slot] orelse continue;
                const shape = value.graph.node(original_id).output_shape;
                if (shape.dtype != .f32) continue;
                const elements = std.math.cast(usize, shape.numElements() orelse return error.InvalidTraceTensor) orelse return error.InvalidTraceTensor;
                if (elements > (128 * 1024 * 1024 -| self.written) / 4) return error.TraceLimitExceeded;
                const host = try a.alloc(f32, elements);
                defer a.free(host);
                try self.owner.?.cb.glinerBoundaryDownload(tensor, host);
                try file.writeStreamingAll(self.io, std.mem.sliceAsBytes(host));
                try self.tensors.append(a, .{ .name = module, .kind = if (role == 0) "input" else "output", .elements = elements, .offset = self.written });
                self.written += elements * 4;
                try self.seen.put(a, key, {});
            }
        }
    }
    fn normBackward(raw: *anyopaque, operands: [4]inference.ops.CT, output: inference.ops.CT, rows: u32, dim: u32, eps: f32) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const file = self.backward_file orelse return error.InvalidTraceTensor;
        if (self.norms.items.len >= 1024) return error.TraceLimitExceeded;
        const a = self.budget.allocator();
        const n = try std.math.mul(usize, rows, dim);
        const packed_count = try std.math.add(usize, n, try std.math.mul(usize, 2, dim));
        const counts = [_]usize{ n, dim, dim, n, packed_count };
        var norm_record = Norm{ .rows = rows, .dim = dim, .eps = eps, .offsets = undefined };
        for (operands ++ .{output}, counts, &norm_record.offsets) |tensor, elements, *offset| {
            if (elements > (128 * 1024 * 1024 -| self.backward_written) / 4) return error.TraceLimitExceeded;
            const host = try a.alloc(f32, elements);
            defer a.free(host);
            try self.owner.?.cb.glinerBoundaryDownload(tensor, host);
            offset.* = self.backward_written;
            try file.writeStreamingAll(self.io, std.mem.sliceAsBytes(host));
            self.backward_written += elements * 4;
        }
        try self.norms.append(a, norm_record);
    }
};
pub fn main(init: std.process.Init) !void {
    const a = std.heap.c_allocator;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    var config_path: ?[]const u8 = null;
    var snapshot_path: ?[]const u8 = null;
    var trace_json_bytes: usize = 2 * 1024 * 1024;
    var trace_path: ?[]const u8 = null;
    var backward_path: ?[]const u8 = null;
    var trace_records = false;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--trace-records")) {
            trace_records = true;
            continue;
        }
        const value = args.next() orelse return error.MissingArgument;
        if (std.mem.eql(u8, arg, "--config")) config_path = value else if (std.mem.eql(u8, arg, "--snapshot")) snapshot_path = value else if (std.mem.eql(u8, arg, "--trace-tensors")) trace_path = value else if (std.mem.eql(u8, arg, "--trace-backward")) backward_path = value else if (std.mem.eql(u8, arg, "--trace-json-bytes")) trace_json_bytes = try std.fmt.parseInt(usize, value, 10) else return error.UnknownArgument;
    }
    // Serialization growth can exceed the final JSON size at larger batches.
    // This explicit diagnostic allowance never changes the response cap,
    // training resource limits or any measured-step arithmetic.
    if (trace_json_bytes < 2 * 1024 * 1024 or trace_json_bytes > 8 * 1024 * 1024) return error.InvalidTraceBudget;
    const config_snapshot = try job.loadConfigSnapshot(a, init.io, config_path orelse return error.MissingArgument);
    var parsed = config_snapshot.parsed;
    defer parsed.deinit();
    const config = parsed.value;
    if (trace_path != null and config.activation_profile != .retained_v1) return error.UnsupportedTraceActivationProfile;
    if (config.execution != .resident_cuda or config.run.shuffle or config.run.batch_size > 8 or config.run.epochs > 2048 or config.run.scheduler != .constant or config.run.warmup_steps != 0 or config.gold_start != 1 or config.gold_end != 1) return error.UnsupportedBenchmarkConfig;
    const path = snapshot_path orelse return error.MissingArgument;
    if (!std.fs.path.isAbsolute(path)) return error.InvalidSnapshotPath;
    if (trace_path) |destination| if (!std.fs.path.isAbsolute(destination) or std.mem.eql(u8, path, destination)) return error.InvalidSnapshotPath;
    if (backward_path) |destination| {
        if (!std.fs.path.isAbsolute(destination) or std.mem.eql(u8, path, destination)) return error.InvalidSnapshotPath;
        if (trace_path) |forward_path| if (std.mem.eql(u8, forward_path, destination)) return error.InvalidSnapshotPath;
    }
    const source = try sources.Source.open(a, init.io, config.source_dir, .{ .limits = config.source_limits, .expected_identity = config.expected_source }, null);
    defer source.deinit();
    var dataset = try data.Dataset.open(a, config.train_file, .{ .limits = config.dataset_limits }, null, null);
    defer dataset.deinit();
    var owner = try create(a, source, &dataset, config);
    defer owner.deinit();
    try sync(owner);
    var output_buffer: [4096]u8 = undefined;
    var output = std.Io.File.stdout().writer(init.io, &output_buffer);
    const cuda = owner.backend.cuda_backend.?;
    try emit(a, &output.interface, .{ .event = "ready", .arm = "native", .scope = scope, .backend = "cuda", .device_name = cuda.ctx.info.nameSlice(), .dtype = "float32", .dropout = 0, .negative_query_sampling = false, .source = source.identity, .dataset_sha256 = std.fmt.bytesToHex(dataset.sha256, .lower), .config_sha256 = std.fmt.bytesToHex(config_snapshot.sha256, .lower), .mode = config.run.mode, .batch_size = config.run.batch_size, .accumulation = config.run.accumulation, .trainable_tensors = owner.optimizer.owner.regular_params.items.len, .cublas_version = cuda.training_blas.?.version, .training_fingerprint = std.fmt.bytesToHex(owner.fingerprint, .lower), .qualification = false });
    var input_buffer: [4096]u8 = undefined;
    var input = std.Io.File.stdin().readerStreaming(init.io, &input_buffer);
    var count: usize = 0;
    var previous: u32 = 0;
    while (try input.interface.takeDelimiter('\n')) |line| {
        if (line.len > 2048 or count >= 4096) return error.CommandLimitExceeded;
        count += 1;
        var command = try std.json.parseFromSlice(Command, a, line, .{});
        defer command.deinit();
        const c = command.value;
        if (c.request_id <= previous) return error.InvalidCommandIdentity;
        previous = c.request_id;
        if (c.op == .stop) {
            try sync(owner);
            try emit(a, &output.interface, .{ .event = "stopped", .arm = "native", .request_id = c.request_id });
            return;
        }
        if (std.mem.eql(u8, c.case_id, "reset") and c.op == .validate) {
            const replacement = try create(a, source, &dataset, config);
            owner.deinit();
            owner = replacement;
            try sync(owner);
            try emit(a, &output.interface, .{ .event = "result", .arm = "native", .request_id = c.request_id, .case_id = c.case_id, .duration_ns = 1 });
        } else if (std.mem.eql(u8, c.case_id, "snapshot") and c.op == .validate) {
            try owner.optimizer.ensureHostState(null);
            const file = try std.Io.Dir.cwd().createFile(init.io, path, .{});
            defer file.close(init.io);
            var slots = std.ArrayList(Slot).empty;
            defer slots.deinit(a);
            var offset: usize = 0;
            for (owner.optimizer.owner.regular_params.items, owner.optimizer.present, owner.optimizer.group_ids) |slot, present, group| {
                const state = owner.optimizer.owner.optimizer_state.param_states.get(slot.name).?;
                var canonical: ?[]const u8 = null;
                for (source.parameters) |parameter| if (std.mem.eql(u8, benchmarkName(parameter.name), slot.name)) {
                    canonical = parameter.canonical_name;
                    break;
                };
                try slots.append(a, .{ .name = slot.name, .canonical_name = canonical orelse slot.name, .shape = slot.dims, .elements = slot.weights.len, .offset = offset, .present = present, .adam_step = slot.adam_step_count, .group = group });
                for ([_][]const f32{ slot.weights, slot.grad_accum, state.m, state.v }) |values| {
                    const bytes = std.mem.sliceAsBytes(values);
                    try file.writeStreamingAll(init.io, bytes);
                    offset += bytes.len;
                }
            }
            try emit(a, &output.interface, .{ .event = "result", .arm = "native", .request_id = c.request_id, .case_id = c.case_id, .duration_ns = 1, .snapshot = path, .size_bytes = offset, .layout = "little_endian_f32_weight_gradient_m_v", .slots = slots.items, .identity = owner.optimizer.identity(), .accumulated_microbatches = owner.optimizer.owner.accum_count });
        } else if (std.mem.eql(u8, c.case_id, "inputs") and c.op == .validate) {
            const pos = try owner.position();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const scratch = arena.allocator();
            const samples = try scratch.alloc(data.Sample, pos.count);
            const items = try scratch.alloc(processor.Item, pos.count);
            var initialized: usize = 0;
            defer for (samples[0..initialized]) |*sample| sample.deinit();
            for (samples, items, 0..) |*sample, *item, i| {
                sample.* = try dataset.sample(@intCast(pos.offset + i), null, null);
                initialized += 1;
                item.* = .{ .text = sample.row.text, .schema = &sample.schema };
            }
            var prepared = try processor.prepare(a, source.tokenizer(), items, owner.options.processor);
            defer prepared.deinit();
            try emit(a, &output.interface, .{ .event = "result", .arm = "native", .request_id = c.request_id, .case_id = c.case_id, .duration_ns = 1, .input_ids = prepared.input_ids, .encoder_shape = .{ prepared.samples.len, prepared.sequence_length }, .word_width = prepared.word_width, .query_width = prepared.query_width });
        } else if (std.mem.eql(u8, c.case_id, "step") or std.mem.eql(u8, c.case_id, "profile_step") or std.mem.eql(u8, c.case_id, "trace_step")) {
            try sync(owner);
            const before = owner.cb.trainingRuntimeStats();
            const start = try now();
            var profile = PhaseProfile{ .owner = owner, .phase = owner.memory_failures.phase, .start_ns = start, .last_ns = start };
            const profiling = std.mem.eql(u8, c.case_id, "profile_step");
            const tracing = std.mem.eql(u8, c.case_id, "trace_step");
            if (tracing and c.op != .validate) return error.UnsupportedBenchmarkConfig;
            var trace = TraceCapture.init(a, trace_json_bytes);
            defer trace.deinit();
            trace.io = init.io;
            trace.owner = owner;
            trace.source = source;
            trace.records = trace_records;
            if (tracing) if (trace_path) |destination| {
                trace.file = try std.Io.Dir.cwd().createFile(init.io, destination, .{});
            };
            defer if (trace.file) |file| file.close(init.io);
            if (tracing) if (backward_path) |destination| {
                trace.backward_file = try std.Io.Dir.cwd().createFile(init.io, destination, .{});
            };
            defer if (trace.backward_file) |file| file.close(init.io);
            const previous_norm_observer = owner.backend.cuda_backend.?.layer_norm_backward_observer;
            if (trace.backward_file != null) owner.backend.cuda_backend.?.layer_norm_backward_observer = .{ .context = &trace, .observe = TraceCapture.normBackward };
            defer owner.backend.cuda_backend.?.layer_norm_backward_observer = previous_norm_observer;
            const previous_observer = owner.options.decision_observer;
            if (profiling) owner.options.decision_observer = .{ .context = &profile, .observe = PhaseProfile.decision };
            if (tracing) owner.options.decision_observer = .{ .context = &trace, .observe = TraceCapture.decision, .boundary_inputs = TraceCapture.inputs, .retained_values = if (trace.file != null) TraceCapture.retained else null };
            defer owner.options.decision_observer = previous_observer;
            const control: ?inference.InferenceExecutionControl = if (profiling) .{ .ptr = &profile, .check_fn = PhaseProfile.sample } else null;
            const report = (owner.next(control) catch |err| {
                // Preserve the admitted owner's failure context before teardown.
                // These diagnostics are emitted only on a failed command.
                try emit(a, &output.interface, .{
                    .event = "error",
                    .arm = "native",
                    .request_id = c.request_id,
                    .case_id = c.case_id,
                    .error_type = @errorName(err),
                    .message = @tagName(owner.memory_failures.phase),
                    .allocation_failure = owner.memory_failures.snapshot(),
                    .cuda_allocations = owner.backend.cuda_backend.?.ctx.device_allocations.snapshot(),
                });
                return err;
            }) orelse return error.TrainingRunComplete;
            try sync(owner);
            const finished = try now();
            const duration = finished - start;
            if (profiling) {
                profile.phase_ns[@intFromEnum(profile.phase)] += finished - profile.last_ns;
                try emit(a, &output.interface, .{ .event = "phase_profile", .request_id = c.request_id, .phase_names = std.meta.fieldNames(native.MemoryPhase), .phase_ns = profile.phase_ns, .decision_names = .{ "pool", "relations", "boundary_loss", "record" }, .decision_ns = profile.decision_ns, .control_checks = profile.calls, .matmul_plans = if (owner.backend.cuda_backend.?.cublaslt) |*blas| blas.tensor_core_plans.count() else 0, .timing = "diagnostic_host_intervals" });
            }
            const after = owner.cb.trainingRuntimeStats();
            if (after.to_float32_calls != before.to_float32_calls or after.download_alloc_calls != before.download_alloc_calls) return error.UnexpectedHostFallback;
            try emit(a, &output.interface, .{ .event = "result", .arm = "native", .request_id = c.request_id, .case_id = c.case_id, .duration_ns = duration, .report = report, .trace_events = trace.events.items, .module_trace = .{ .path = trace_path, .size_bytes = trace.written, .tensors = trace.tensors.items }, .backward_trace = .{ .path = backward_path, .size_bytes = trace.backward_written, .norms = trace.norms.items }, .cuda_allocations = owner.backend.cuda_backend.?.ctx.device_allocations.snapshot(), .cuda_transfers = .{ .h2d_bytes = after.h2d_bytes - before.h2d_bytes, .d2h_bytes = after.d2h_bytes - before.d2h_bytes, .kernel_launches = after.kernel_launches - before.kernel_launches, .host_fallback_calls = after.to_float32_calls - before.to_float32_calls } });
        } else return error.UnknownCommand;
    }
    return error.ProtocolEndedWithoutStop;
}
