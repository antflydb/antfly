// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Actual managed service qualification and admission measurement. No driver
//! reset, cache eviction, model precision override or synthetic model admission.
const std = @import("std");
const inference = @import("inference_internal");
const service = inference.server.memorilla_service;
const manager_mod = inference.server.model_manager;
const qualified = inference.models.memorilla_qualification;
const encoder = inference.pipelines.memorilla_encoder;
const decode = inference.pipelines.memorilla_generation;
const memory = inference.runtime.tier.memory;
const Control = inference.execution_control.InferenceExecutionControl;
const CB = inference.ops.ComputeBackend;
const CT = inference.ops.CT;
const safe = inference.models.safetensors;
const Tensor = inference.backends.Tensor;
const file = inference.util.c_file;
const A = std.mem.Allocator;

const Args = struct {
    mode: enum { prepare, verify } = .verify,
    encoder: []const u8 = "",
    decoder: []const u8 = "",
    bundle: []const u8 = "",
    features: []const u8 = "",
    fixture: []const u8 = "",
    query_output: []const u8 = "",
    query_reference: []const u8 = "",
    question: []const u8 = "What does Antfly preserve when a source changes?",
    budget_gib: usize = 96,
};

fn parse(args: []const []const u8) !Args {
    var out = Args{};
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (i + 1 >= args.len) return error.InvalidArguments;
        const key = args[i];
        i += 1;
        const value = args[i];
        if (std.mem.eql(u8, key, "--mode")) out.mode = std.meta.stringToEnum(@TypeOf(out.mode), value) orelse return error.InvalidArguments else if (std.mem.eql(u8, key, "--encoder")) out.encoder = value else if (std.mem.eql(u8, key, "--decoder")) out.decoder = value else if (std.mem.eql(u8, key, "--bundle")) out.bundle = value else if (std.mem.eql(u8, key, "--features")) out.features = value else if (std.mem.eql(u8, key, "--fixture")) out.fixture = value else if (std.mem.eql(u8, key, "--native-query-output")) out.query_output = value else if (std.mem.eql(u8, key, "--native-query")) out.query_reference = value else if (std.mem.eql(u8, key, "--question")) out.question = value else if (std.mem.eql(u8, key, "--budget-gib")) out.budget_gib = try std.fmt.parseInt(usize, value, 10) else return error.InvalidArguments;
    }
    if (out.encoder.len == 0 or out.decoder.len == 0 or out.bundle.len == 0 or out.features.len == 0 or out.budget_gib == 0 or out.budget_gib > 112 or
        (out.mode == .prepare and out.query_output.len == 0) or (out.mode == .verify and (out.fixture.len == 0 or out.query_reference.len == 0))) return error.InvalidArguments;
    return out;
}

fn emit(a: A, io: std.Io, value: anytype) !void {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    var buffer: [4096]u8 = undefined;
    var writer = std.Io.File.stdout().writerStreaming(io, &buffer);
    try writer.interface.writeAll(bytes);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

const ProcMemory = struct { rss_bytes: ?usize = null, peak_rss_bytes: ?usize = null };
fn processMemory(a: A, io: std.Io) ProcMemory {
    var status = std.Io.Dir.cwd().openFile(io, "/proc/self/status", .{}) catch return .{};
    defer status.close(io);
    var buffer: [4096]u8 = undefined;
    var reader = status.readerStreaming(io, &buffer);
    const bytes = reader.interface.allocRemaining(a, .limited(64 * 1024)) catch return .{};
    defer a.free(bytes);
    var result = ProcMemory{};
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |line| {
        var fields = std.mem.tokenizeAny(u8, line, " \t");
        const name = fields.next() orelse continue;
        if (!std.mem.eql(u8, name, "VmRSS:") and !std.mem.eql(u8, name, "VmHWM:")) continue;
        const amount = std.fmt.parseInt(usize, fields.next() orelse continue, 10) catch continue;
        if (std.mem.eql(u8, name, "VmRSS:")) result.rss_bytes = amount * 1024 else result.peak_rss_bytes = amount * 1024;
    }
    return result;
}

const Monitor = struct {
    manager: *manager_mod.ModelManager,
    io: std.Io,
    stop: std.atomic.Value(bool) = .init(false),
    host_peak: std.atomic.Value(usize) = .init(0),
    backend_peak: std.atomic.Value(usize) = .init(0),
    combined_peak: std.atomic.Value(usize) = .init(0),

    fn sample(self: *@This()) void {
        const current = self.manager.currentAdmissionAmounts();
        _ = self.host_peak.fetchMax(current.hostTotalBytes(), .monotonic);
        _ = self.backend_peak.fetchMax(current.backendTotalBytes(), .monotonic);
        _ = self.combined_peak.fetchMax(current.hostTotalBytes() + current.backendTotalBytes(), .monotonic);
    }
    fn run(self: *@This()) void {
        while (!self.stop.load(.acquire)) {
            self.sample();
            std.Io.sleep(self.io, .fromMilliseconds(10), .awake) catch return;
        }
    }
    fn reset(self: *@This()) void {
        self.host_peak.store(0, .monotonic);
        self.backend_peak.store(0, .monotonic);
        self.combined_peak.store(0, .monotonic);
        self.sample();
    }
    fn peaks(self: *@This()) struct { host: usize, backend: usize, combined: usize, sample_period_ms: usize = 10 } {
        self.sample();
        return .{ .host = self.host_peak.load(.monotonic), .backend = self.backend_peak.load(.monotonic), .combined = self.combined_peak.load(.monotonic) };
    }
};

const Comparison = struct { elements: usize, mismatches: usize = 0, max_absolute_error: f64 = 0, relative_l2: f64 = 0 };
fn compare(actual: []const f32, expected: []const f32, atol: f64, rtol: f64) !Comparison {
    if (actual.len != expected.len or actual.len == 0) return error.InvalidFixtureShape;
    var result = Comparison{ .elements = actual.len };
    var squared_error: f64 = 0;
    var squared_reference: f64 = 0;
    for (actual, expected) |x, y| {
        if (!std.math.isFinite(x) or !std.math.isFinite(y)) return error.NonFiniteComparison;
        const difference = @abs(@as(f64, x) - y);
        result.max_absolute_error = @max(result.max_absolute_error, difference);
        result.mismatches += @intFromBool(difference > atol + rtol * @abs(@as(f64, y)));
        squared_error += difference * difference;
        squared_reference += @as(f64, y) * y;
    }
    result.relative_l2 = if (squared_reference == 0) 0 else @sqrt(squared_error / squared_reference);
    return result;
}

const Oracle = struct {
    reader: safe.MMapReader,
    embeddings: Tensor,
    input_ids: Tensor,
    memory_tokens: Tensor,
    prefill: Tensor,
    decode_logits: Tensor,
    decode_ids: Tensor,
    tensor_sha256: [64]u8,

    fn open(a: A, path: []const u8) !Oracle {
        var reader = try safe.MMapReader.openFileAbsoluteLimited(a, path, 256 * 1024 * 1024, 2 * 1024 * 1024);
        errdefer reader.deinit();
        try safe.validateReader(a, &reader);
        const receipt_path = try std.fs.path.join(a, &.{ std.fs.path.dirname(path) orelse ".", "reference.json" });
        defer a.free(receipt_path);
        const bytes = try file.readFileMax(a, receipt_path, 4 * 1024 * 1024);
        defer a.free(bytes);
        const receipt = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
        defer receipt.deinit();
        const declared = receipt.value.object.get("files").?.object.get("reference.safetensors").?.object;
        const digest = inference.models.memorilla.digest(reader.file_bytes);
        if (!std.mem.eql(u8, &digest, declared.get("sha256").?.string) or declared.get("size_bytes").?.integer != reader.file_bytes.len)
            return error.FixtureChecksumMismatch;
        const result = Oracle{ .reader = reader, .embeddings = try reader.readTensor("input_embeddings"), .input_ids = try reader.readTensor("input_ids"), .memory_tokens = try reader.readTensor("memory_tokens"), .prefill = try reader.readTensor("prefill_logits"), .decode_logits = try reader.readTensor("decode_logits"), .decode_ids = try reader.readTensor("decode_input_ids"), .tensor_sha256 = digest };
        if (result.embeddings.dtype != .f32 or result.input_ids.dtype != .i64 or result.memory_tokens.dtype != .f32 or
            result.prefill.dtype != .f32 or result.decode_logits.dtype != .f32 or result.decode_ids.dtype != .i64 or
            result.embeddings.shape.len != 2 or result.embeddings.shape[1] != 4096 or
            result.memory_tokens.shape.len != 2 or result.memory_tokens.shape[0] != 16 or result.memory_tokens.shape[1] != 4096 or
            result.prefill.elementCount() != 151936 or result.decode_logits.elementCount() != result.decode_ids.elementCount() * 151936)
            return error.InvalidFixtureShape;
        return result;
    }
    fn deinit(self: *@This()) void {
        self.embeddings.deinit();
        self.input_ids.deinit();
        self.memory_tokens.deinit();
        self.prefill.deinit();
        self.decode_logits.deinit();
        self.decode_ids.deinit();
        self.reader.deinit();
    }
};

const Context = struct {
    a: A,
    io: std.Io,
    label: []const u8 = "",
    oracle: ?*Oracle = null,
    config: ?inference.models.gpt.Config = null,
    validate: bool = true,
    cancel_after_prefill: bool = false,
    cancelled: bool = false,
    query_output: ?[]const u8 = null,
    query_reference: ?*const std.json.Value = null,
    embeddings: ?Comparison = null,
    memory_tokens: ?Comparison = null,
    input_ids_match: bool = false,
    memory_start: usize = 0,
    logits: [8]?Comparison = @splat(null),
    logit_digests: [8]?[64]u8 = @splat(null),
    embedding_digest: ?[64]u8 = null,

    fn check(raw: ?*anyopaque) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (self.cancelled) return error.Cancelled;
    }
    fn stage(raw: ?*anyopaque, stage_id: service.Stage, model_manager: *manager_mod.ModelManager) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        const admitted = model_manager.currentAdmissionAmounts();
        const decoder_cached = !std.mem.eql(u8, self.label, "prepare") and !std.mem.eql(u8, self.label, "cold_a");
        const expected_weights: ?usize = switch (stage_id) {
            .encoder_loaded, .question_encoded => inference.models.memorilla_runtime.parameter_bytes + qualified.encoder_weight_bytes + (if (decoder_cached) qualified.decoder_weight_bytes else 0),
            .decoder_loaded, .reader_loaded, .decoded => inference.models.memorilla_runtime.parameter_bytes + qualified.encoder_weight_bytes + qualified.decoder_weight_bytes,
            .prepared => null,
        };
        if (expected_weights) |expected| if (admitted.backend_weight_bytes != expected) return error.MemorillaModelWeightAdmissionMissing;
        try emit(self.a, self.io, .{ .event = "stage", .run = self.label, .stage = @tagName(stage_id), .admitted = admitted, .expected_backend_weight_bytes = expected_weights, .process_memory = processMemory(self.a, self.io) });
    }
    fn question(raw: ?*anyopaque, features: *const encoder.Features) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (self.query_reference) |expected| {
            if (expected.* != .object) return error.InvalidNativeQueryReference;
            const object = expected.object;
            const text = object.get("question_text") orelse return error.InvalidNativeQueryReference;
            const half = object.get("stored_fp16") orelse return error.InvalidNativeQueryReference;
            const bf16 = object.get("memory_bf16_f32") orelse return error.InvalidNativeQueryReference;
            if (text != .string or !std.mem.eql(u8, text.string, features.question_text) or half != .array or bf16 != .array or
                half.array.items.len != qualified.width or bf16.array.items.len != qualified.width) return error.InvalidNativeQueryReference;
            for (half.array.items, bf16.array.items, features.question_fp16, features.question) |h, b, actual_h, actual_b| {
                const expected_h: f32 = switch (h) {
                    .float => |v| @floatCast(v),
                    .integer => |v| @floatFromInt(v),
                    else => return error.InvalidNativeQueryReference,
                };
                const expected_b: f32 = switch (b) {
                    .float => |v| @floatCast(v),
                    .integer => |v| @floatFromInt(v),
                    else => return error.InvalidNativeQueryReference,
                };
                if (@as(u32, @bitCast(expected_h)) != @as(u32, @bitCast(@as(f32, @floatCast(actual_h)))) or
                    @as(u32, @bitCast(expected_b)) != @as(u32, @bitCast(actual_b))) return error.NativeQueryFeatureBitsChanged;
            }
            try emit(self.a, self.io, .{ .event = "native_query", .run = self.label, .fp16_bits_match = true, .bf16_bits_match = true, .elements = qualified.width });
        }
        if (self.query_output) |path| {
            const stored = try self.a.alloc(f32, features.question_fp16.len);
            defer self.a.free(stored);
            for (features.question_fp16, stored) |value, *out| out.* = @floatCast(value);
            const publication = std.fmt.bytesToHex(features.producer.publication, .lower);
            const json = try std.json.Stringify.valueAlloc(self.a, .{
                .schema = "antfly.memorilla.native-query.v1",
                .question_text = features.question_text,
                .stored_fp16 = stored,
                .memory_bf16_f32 = features.question,
                .producer = .{
                    .profile = @tagName(features.producer.profile),
                    .publication = publication[0..],
                    .identity = features.producer.identity[0..],
                    .source_lock_sha256 = features.producer.source_lock_sha256[0..],
                    .recipe_sha256 = features.producer.recipe_sha256[0..],
                },
            }, .{});
            defer self.a.free(json);
            var output = try std.Io.Dir.cwd().createFile(self.io, path, .{ .exclusive = true });
            defer output.close(self.io);
            try output.writeStreamingAll(self.io, json);
            return error.MemorillaPrepareComplete;
        }
    }
    fn preparedText(raw: ?*anyopaque, text: *const decode.PreparedText) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.memory_start = text.memory_start;
        if (self.validate) if (self.oracle) |oracle| {
            if (!std.mem.eql(i64, text.input_ids, oracle.input_ids.asInt64())) return error.ComposedPromptTokenMismatch;
            self.input_ids_match = true;
        };
        try emit(self.a, self.io, .{ .event = "framing", .run = self.label, .input_ids = text.input_ids, .memory_start = text.memory_start, .memory_rows = text.recipe.memory_rows, .oracle_ids_match = self.input_ids_match });
        if (self.config) |config| {
            const plan = try decode.ResourcePlan.create(config, .cuda, text.input_ids.len, text.rendered.len, text.recipe.memory_rows, 128, .{ .max_new_tokens = 128 });
            try emit(self.a, self.io, .{ .event = "max_generation_resource_contract", .run = self.label, .max_new_tokens = 128, .plan = plan, .scope = "resource-plan-only; numerical-decode-gate-remains-three-steps" });
        }
    }
    fn inputEmbeddings(raw: *anyopaque, cb: *const CB, tensor: CT) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const values = try cb.toFloat32(tensor, self.a);
        defer self.a.free(values);
        self.embedding_digest = inference.models.memorilla.digest(std.mem.sliceAsBytes(values));
        if (self.validate) if (self.oracle) |oracle| {
            self.embeddings = try compare(values, oracle.embeddings.asFloat32(), 5e-5, 1e-5);
            const memory_values = oracle.memory_tokens.asFloat32();
            if (self.memory_start * 4096 + memory_values.len > values.len) return error.InvalidFixtureShape;
            self.memory_tokens = try compare(values[self.memory_start * 4096 ..][0..memory_values.len], memory_values, 5e-5, 1e-5);
        };
    }
    fn logit(raw: *anyopaque, step: usize, values: []const f32) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (step >= self.logits.len) return error.InvalidTraceStep;
        self.logit_digests[step] = inference.models.memorilla.digest(std.mem.sliceAsBytes(values));
        if (self.validate) if (self.oracle) |oracle| {
            const expected = if (step == 0) oracle.prefill.asFloat32() else oracle.decode_logits.asFloat32()[(step - 1) * values.len ..][0..values.len];
            self.logits[step] = try compare(values, expected, 0.001, 0.001);
        };
        if (step == 0 and self.cancel_after_prefill) self.cancelled = true;
    }
    fn physical(self: *@This(), role: []const u8, value: decode.PhysicalResources) anyerror!void {
        const planned = try std.math.add(usize, value.baseline, value.held_transient_bytes);
        // The warm A after B uses a smaller ceiling than the preceding
        // 128-token request. A lifetime high-water cannot describe its local
        // peak; its enforced allocation ceiling still bounds every allocation.
        const peak_comparable = std.mem.eql(u8, role, "reader") or !std.mem.eql(u8, self.label, "warm_a_after_b");
        const within_plan = value.limit <= planned and value.live <= value.limit and (!peak_comparable or value.lifetime_peak <= value.limit);
        try emit(self.a, self.io, .{ .event = "physical_cuda", .run = self.label, .role = role, .resources = value, .within_admitted_plan = within_plan, .lifetime_peak_comparable_to_current_plan = peak_comparable, .scope = "owned-DeviceBuffers; vendor-private allocations require deployment headroom" });
        if (!within_plan) return error.MemorillaPhysicalAdmissionExceeded;
    }
    fn decoderPhysical(raw: *anyopaque, value: decode.PhysicalResources) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.physical("decoder", value);
    }
    fn readerPhysical(raw: ?*anyopaque, value: decode.PhysicalResources) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try self.physical("reader", value);
    }
    fn passed(self: *const @This(), steps: usize) bool {
        if (!self.input_ids_match or self.embeddings == null or self.embeddings.?.mismatches != 0 or
            self.memory_tokens == null or self.memory_tokens.?.mismatches != 0 or self.memory_tokens.?.relative_l2 > 5e-6) return false;
        for (self.logits[0 .. steps + 1]) |item| if (item == null or item.?.mismatches != 0) return false;
        return true;
    }
};

pub fn main(init: std.process.Init) !void {
    const a = std.heap.c_allocator;
    const args = try init.minimal.args.toSlice(a);
    defer a.free(args);
    const opts = try parse(args);
    const cap = try std.math.mul(usize, opts.budget_gib, 1024 * 1024 * 1024);
    const limits = memory.Limits{ .host_limit_bytes = cap, .backend_limit_bytes = cap, .combined_limit_bytes = cap, .kv_limit_bytes = 2 * 1024 * 1024 * 1024, .scratch_limit_bytes = 12 * 1024 * 1024 * 1024 };
    var source = try safe.MMapReader.openFileAbsoluteLimited(a, opts.features, 64 * 1024 * 1024, 1024 * 1024);
    defer source.deinit();
    var normalized = try source.readTensor("reference.normalized");
    defer normalized.deinit();
    if (normalized.dtype != .f32 or normalized.shape.len != 2 or normalized.shape[0] < 3 or normalized.shape[1] != qualified.width) return error.InvalidFixtureShape;
    const documents = [_][]const f32{ normalized.asFloat32()[qualified.width .. 2 * qualified.width], normalized.asFloat32()[2 * qualified.width .. 3 * qualified.width] };
    var max_documents: [qualified.max_documents][]const f32 = undefined;
    // A different collection as well as a larger geometry: repeated first
    // document, rather than preserving A's two-document proportions.
    for (&max_documents) |*row| row.* = documents[0];
    const identity = qualified.executionIdentity(.qwen3_encoder_f32_v1);
    const decoder_identity = qualified.executionIdentity(.qwen3_decoder_f32_kv_f32_v1);
    const source_digest = inference.models.memorilla.digest(source.file_bytes);
    const selection = inference.model_selection;
    const encoder_path = try std.Io.Dir.cwd().realPathFileAlloc(init.io, opts.encoder, a);
    const selected_encoder = try selection.fromDirectory(a, init.io, encoder_path);
    defer a.free(selected_encoder.path);
    const decoder_path = try std.Io.Dir.cwd().realPathFileAlloc(init.io, opts.decoder, a);
    const selected_decoder = try selection.fromDirectory(a, init.io, decoder_path);
    defer a.free(selected_decoder.path);
    const paths = service.Paths{ .encoder = selected_encoder, .decoder = selected_decoder };
    const config_path = try std.fs.path.join(a, &.{ selected_decoder.path, "config.json" });
    defer a.free(config_path);
    const config_bytes = try file.readFileMax(a, config_path, 4096);
    defer a.free(config_bytes);
    const config = try inference.models.gpt.parseConfig(a, config_bytes);
    const profile = service.Profile{ .name = "managed-stage3-f32", .encoder = selected_encoder.path, .decoder = selected_decoder.path, .memory_module_path = opts.bundle, .max_new_tokens = 128 };
    var sessions = inference.backends.SessionManager.initWithIo(a, init.io);
    sessions.preferred_backends = &.{.cuda};
    sessions.required_backend = .cuda;
    sessions.native_call_policy = .in_process;
    var models = manager_mod.ModelManager.init(a, sessions);
    defer models.deinit();
    models.configureServingPolicy(.{});
    models.configureAdmissionLimits(limits);
    models.configureProcessMemoryLimit(cap, .explicit);
    models.configureModelCache(600_000, 4);
    try models.ensureResourceOwnerReady();
    var monitor = Monitor{ .manager = &models, .io = init.io };
    const thread = try std.Thread.spawn(.{}, Monitor.run, .{&monitor});
    defer {
        monitor.stop.store(true, .release);
        thread.join();
    }
    var oracle: ?Oracle = if (opts.mode == .verify) try Oracle.open(a, opts.fixture) else null;
    defer if (oracle) |*value| value.deinit();
    const steps: usize = if (oracle) |*value| value.decode_ids.asInt64().len else 3;
    if (steps == 0 or steps > 7) return error.InvalidFixtureShape;
    const query_bytes = if (opts.mode == .verify) try file.readFileMax(a, opts.query_reference, 256 * 1024) else null;
    defer if (query_bytes) |bytes| a.free(bytes);
    const query_reference = if (query_bytes) |bytes| try std.json.parseFromSlice(std.json.Value, a, bytes, .{}) else null;
    defer if (query_reference) |value| value.deinit();
    const query_value = if (query_reference) |*value| &value.value else null;
    var context = Context{ .a = a, .io = init.io, .label = if (opts.mode == .prepare) "prepare" else "cold_a", .oracle = if (oracle) |*value| value else null, .config = config, .query_reference = query_value, .query_output = if (opts.mode == .prepare) opts.query_output else null };
    const control = Control{ .io = init.io, .native_call_policy = .in_process, .ptr = &context, .check_fn = Context.check };
    const options = service.Options{ .context = &context, .question_features = Context.question, .prepared_text = Context.preparedText, .reader_physical_resources = Context.readerPhysical, .stage = Context.stage, .trace = .{ .context = &context, .input_embeddings = Context.inputEmbeddings, .logits = Context.logit, .physical_resources = Context.decoderPhysical }, .teacher_forced_ids = if (oracle) |*value| value.decode_ids.asInt64() else null, .ignore_eos = true };
    try emit(a, init.io, .{ .event = "ready", .scope = "actual-managed-memorilla-service", .mode = @tagName(opts.mode), .operator_cap_bytes = cap, .documents = documents.len, .supplied_document_origin = "independent-CPU-F32-encoder-reference-normalized-rows-1-and-2", .document_tensor_sha256 = source_digest[0..], .process_memory = processMemory(a, init.io) });
    if (opts.mode == .prepare) {
        var unexpected = service.generateWithOptions(a, init.io, &models, profile, paths, opts.question, &documents, &identity, steps, limits, control, options) catch |err| {
            if (err != error.MemorillaPrepareComplete) return err;
            try emit(a, init.io, .{ .event = "prepared", .native_query_output = opts.query_output, .full_pipeline_executed = false, .admission_peak = monitor.peaks(), .retained = models.currentAdmissionAmounts(), .process_memory = processMemory(a, init.io) });
            return;
        };
        unexpected.deinit();
        return error.MissingPrepareBoundary;
    }
    var cold = try service.generateWithOptions(a, init.io, &models, profile, paths, opts.question, &documents, &identity, steps, limits, control, options);
    const cold_pass = context.passed(steps);
    const baseline_digests = context.logit_digests;
    try emit(a, init.io, .{ .event = "case", .run = context.label, .passed = cold_pass, .input_embeddings = context.embeddings, .memory_tokens = context.memory_tokens, .logits = context.logits, .admission_peak = monitor.peaks(), .process_memory = processMemory(a, init.io), .question_tokens = cold.question_tokens });
    cold.deinit();
    const retained = models.currentAdmissionAmounts();
    monitor.reset();
    context = .{ .a = a, .io = init.io, .label = "cancel_after_prefill", .oracle = &oracle.?, .query_reference = query_value, .cancel_after_prefill = true };
    var cancelled = false;
    if (service.generateWithOptions(a, init.io, &models, profile, paths, opts.question, &documents, &identity, steps, limits, control, options)) |value| {
        var unexpected = value;
        unexpected.deinit();
    } else |err| {
        if (err != error.Cancelled) return err;
        cancelled = true;
    }
    const cancellation_refunded = std.meta.eql(retained, models.currentAdmissionAmounts());
    try emit(a, init.io, .{ .event = "case", .run = context.label, .cancelled = cancelled, .admission_refunded = cancellation_refunded, .admission_peak = monitor.peaks() });
    var passed = cold_pass and cancelled and cancellation_refunded;
    for ([_][]const u8{ "warm_a_after_cancel", "warm_b_64_documents_128_tokens", "warm_a_after_b" }) |label| {
        monitor.reset();
        const is_b = std.mem.eql(u8, label, "warm_b_64_documents_128_tokens");
        context = .{ .a = a, .io = init.io, .label = label, .oracle = &oracle.?, .query_reference = query_value, .validate = !is_b, .config = if (is_b) config else null };
        const input_rows: []const []const f32 = if (is_b) &max_documents else &documents;
        var run_options = options;
        if (is_b) {
            run_options.teacher_forced_ids = null;
            run_options.trace = .{ .context = &context, .input_embeddings = Context.inputEmbeddings, .physical_resources = Context.decoderPhysical };
        }
        const request_steps: usize = if (is_b) 128 else steps;
        var result = try service.generateWithOptions(a, init.io, &models, profile, paths, opts.question, input_rows, &identity, request_steps, limits, control, run_options);
        const numerical_pass = if (is_b) true else context.passed(steps);
        const isolated = if (is_b) true else std.meta.eql(baseline_digests, context.logit_digests);
        const generated_tokens = if (result.generation.token_ids) |ids| ids.len else 0;
        const generation_bound_exercised = !is_b or generated_tokens == request_steps;
        result.deinit();
        const refunded = std.meta.eql(retained, models.currentAdmissionAmounts());
        passed = passed and numerical_pass and isolated and refunded and generation_bound_exercised;
        try emit(a, init.io, .{ .event = "case", .run = label, .documents = input_rows.len, .max_new_tokens = request_steps, .generated_tokens = generated_tokens, .generation_bound_exercised = generation_bound_exercised, .passed = numerical_pass and isolated and refunded and generation_bound_exercised, .compared_to_oracle = !is_b, .bitwise_a_isolated = isolated, .admission_refunded = refunded, .input_embeddings = context.embeddings, .memory_tokens = context.memory_tokens, .logits = context.logits, .admission_peak = monitor.peaks(), .process_memory = processMemory(a, init.io) });
    }
    try emit(a, init.io, .{ .event = "complete", .passed = passed, .scope = "actual-managed-service-conditional-native-query-oracle", .oracle_tensor_sha256 = oracle.?.tensor_sha256[0..], .retained = models.currentAdmissionAmounts(), .encoder_execution_identity = identity[0..], .decoder_execution_identity = decoder_identity[0..], .frozen_logit_atol = 0.001, .frozen_logit_rtol = 0.001, .frozen_embedding_atol = 5e-5, .frozen_embedding_rtol = 1e-5 });
    if (!passed) return error.ComposedMemorillaQualificationFailed;
}
