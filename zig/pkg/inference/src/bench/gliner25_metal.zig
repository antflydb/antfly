// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Supervised GLiNER2.5 Metal comparison worker. The model session, tokenizer
//! and cached provider survive requests; each request owns its managed compute
//! backend. The optimized policy uses the production session's immutable FP32
//! owner; the reference policy retains request-owned weights. Timing includes the full
//! decode and cleanup. Wire I/O, observations and returned-output destruction
//! are outside timing. This is a direct-core benchmark, not serving evidence.
const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const inference = @import("inference_internal");
const factory = inference.architectures.session_factory;
const model = inference.models.gliner_boundary;
const bundle = inference.models.gliner_boundary_bundle;
const processor = inference.pipelines.gliner_boundary_processor;
const schema_mod = inference.pipelines.extraction_schema;
const device = inference.architectures.gliner_boundary_request_device;
const Control = inference.InferenceExecutionControl;
const Watchdog = inference.HardCancellationWatchdog;
const Allocator = std.mem.Allocator;
const Session = inference.backends.Session;
const MemoryStats = inference.metal_tensor.MemoryStats;
const admission_memory = inference.runtime.tier.memory;
const arm = "antfly_metal";
const optimized_scope = "gliner25_direct_core_metal_comparison_fp32_v2";
const scaling_scope = "gliner25_fp32_scaling_inputs_v1";
const timing_boundary = "schema_parse_compile+processor+encoder+heads+decode+temporary_cleanup";
const source_commit = "3c913c7369301133d3b7699252074c4303ada50e";
const startup_timeout_ms = 120000;
const teardown_timeout_ms = 30000;

comptime {
    if (builtin.mode != .ReleaseFast) @compileError("GLiNER2.5 benchmark requires ReleaseFast for the complete dependency graph");
    if (!build_options.enable_metal or build_options.enable_cuda or build_options.enable_onnx or build_options.enable_pjrt)
        @compileError("GLiNER2.5 Metal benchmark requires -Dmetal=true -Dcuda=false -Donnx=false -Dpjrt=false");
}

const Pin = struct { sha256: []const u8, size_bytes: usize };
const Files = struct {
    @"config.json": Pin,
    @"encoder_config/config.json": Pin,
    @"model.safetensors": Pin,
    @"tokenizer.json": Pin,
    @"tokenizer_config.json": Pin,
};
const Case = struct {
    id: []const u8,
    text: []const u8 = "",
    schema: std.json.Value,
    items: []const struct { id: []const u8, text: []const u8 } = &.{},
    expected_encoded_lengths: []const usize = &.{},
    encoded_width: usize = 0,
};
const Fixture = struct {
    format_version: u32,
    scope: ?[]const u8 = null,
    source_commit: []const u8,
    model: []const u8,
    model_id: []const u8,
    revision: []const u8,
    model_files: Files,
    requests_sha256: ?[]const u8 = null,
    cases: []const Case,
};
const Options = struct {
    model_dir: []const u8 = "",
    cases_path: []const u8 = "",
    threads: usize = 1,
    timeout_ms: u64 = 30000,
    max_commands: usize = 2048,
    execution_policy: device.ExecutionPolicy = .reference_v1,
    workload: enum { fixture, scaling } = .fixture,
    diagnostics: bool = false,
};
const Command = struct { request_id: u32, op: enum { validate, run, stop }, case_id: []const u8 = "" };
const NativeEnvironment = struct {
    TERMITE_METAL_DISABLE_DOT_GENERAL_2D_MPS: ?[]const u8 = null,
    TERMITE_METAL_FORCE_DEBERTA_SCALAR: ?[]const u8 = null,
    TERMITE_METAL_FORCE_DEBERTA_TG: ?[]const u8 = null,
    TERMITE_METAL_DISABLE_DEBERTA_MPS_ATTENTION: ?[]const u8 = null,
    TERMITE_METAL_DEBERTA_MPS_ATTENTION_MAX_MB: ?[]const u8 = null,
    TERMITE_METAL_DISABLE_GLINER_DEBERTA_DIRECT_FFN: ?[]const u8 = null,
};

const Extraction = struct {
    result: device.Result,
    input_ids: ?[]i64 = null,
    attention_mask: ?[]i64 = null,
    input_shape: [2]usize,
    runtime_stats: RuntimeStats,

    fn deinit(self: *Extraction, a: Allocator) void {
        self.result.deinit();
        if (self.input_ids) |ids| a.free(ids);
        if (self.attention_mask) |mask| a.free(mask);
    }
};

const RuntimeStats = struct {
    actual_weight_upload_bytes: u64 = 0,
    scope_submissions: u64 = 0,
    scope_wait_nanos: u64 = 0,
    scope_gpu_nanos: u64 = 0,
    peak_pending_device_bytes: usize = 0,
    pending_device_bytes: usize = 0,
    workspace_live_bytes: usize = 0,
    workspace_products: u64 = 0,
    workspace_product_bytes: u64 = 0,
    workspace_oversized_products: u64 = 0,
    workspace_drains: u64 = 0,
    workspace_pending_bytes: usize = 0,
    workspace_peak_pending_bytes: usize = 0,
    phase_timings_ns: ?PhaseTimings = null,
};

const PhaseTimings = struct {
    schema: u64 = 0,
    processor: u64 = 0,
    backend_setup: u64 = 0,
    device: u64 = 0,
    backend_cleanup: u64 = 0,
};

fn diagnosticNow(enabled: bool) !u64 {
    return if (enabled) try nowNs() else 0;
}

fn nowNs() !u64 {
    var ts: std.posix.timespec = undefined;
    if (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts)) != .SUCCESS) return error.MonotonicClockUnavailable;
    return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec);
}

fn controlFor(watchdog: *Watchdog, timeout_ms: u64) !Control {
    return .{
        .deadline_ns = try std.math.add(u64, try nowNs(), try std.math.mul(u64, timeout_ms, std.time.ns_per_ms)),
        .hard_cancellation = watchdog.boundary(),
    };
}

fn hash(bytes: []const u8) [64]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return std.fmt.bytesToHex(digest, .lower);
}

fn validDigest(value: []const u8) bool {
    if (value.len != 64) return false;
    for (value) |c| if (!((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'))) return false;
    return true;
}

fn filePin(comptime name: []const u8, pin: Pin) bundle.FilePin {
    return .{ .path = name, .sha256 = pin.sha256, .size_bytes = pin.size_bytes };
}

fn verifyFiles(a: Allocator, directory: []const u8, files: Files, control: Control) !void {
    inline for (std.meta.fields(Files)) |field| {
        const pin = @field(files, field.name);
        if (!validDigest(pin.sha256) or pin.size_bytes == 0 or pin.size_bytes > try bundle.fileLimit(field.name)) return error.InvalidBenchmarkArtifactPin;
        try control.check();
        const path = try std.fs.path.join(a, &.{ directory, field.name });
        defer a.free(path);
        // Read-only mappings avoid creating a second full checkpoint copy.
        var mapping = try inference.util.c_file.MmapRegion.init(a, path);
        defer mapping.deinit();
        try bundle.verifyBytes(filePin(field.name, pin), mapping.data, control);
    }
}

fn verifyIdentity(identity: bundle.Identity, fixture: Fixture) !void {
    if (identity.precision != .fp32 or !std.mem.eql(u8, @tagName(identity.backbone), fixture.model)) return error.BenchmarkArtifactIdentityMismatch;
    try identity.weight.verify(filePin("model.safetensors", fixture.model_files.@"model.safetensors"));
    inline for (bundle.sidecar_names, 0..) |name, index| try identity.sidecars[index].verify(filePin(name, @field(fixture.model_files, name)));
}

fn checkBackend(cb: *const inference.ops.ComputeBackend) !void {
    if (cb.kind() != .metal or !cb.decoderRuntimeReady() or cb.vtable.glinerBoundaryDevice == null or cb.vtable.glinerBoundaryDownload == null)
        return error.BenchmarkMetalBackendUnavailable;
    if (cb.decoderRuntimeHasActiveFrame()) return error.BenchmarkExternalMetalFrame;
    try cb.checkExecutionControl();
}

fn extract(a: Allocator, session: Session, controller: *admission_memory.AdmissionController, config: *const model.Config, tokenizer: inference.tokenizer.Tokenizer, case: *const Case, schema_json: []const u8, capture_tokens: bool, options: Options, control: Control) !Extraction {
    try control.check();
    var phase = PhaseTimings{};
    const schema_start = try diagnosticNow(options.diagnostics);
    var schema = try schema_mod.compile(a, schema_json, .{});
    defer schema.deinit();
    const processor_start = try diagnosticNow(options.diagnostics);
    phase.schema = processor_start - schema_start;
    const scaling = options.workload == .scaling;
    const batch = if (scaling) case.items.len else 1;
    var items: [8]processor.Item = undefined;
    var schemas: [8]*const schema_mod.CompiledSchema = undefined;
    for (0..batch) |index| {
        items[index] = .{ .text = if (scaling) case.items[index].text else case.text, .schema = &schema };
        schemas[index] = &schema;
    }
    var prepared = try processor.prepare(a, tokenizer, items[0..batch], .{
        .max_batch_items = if (scaling) 8 else 1,
        .max_text_words = if (scaling) 512 else 128,
        .max_sequence_tokens = 512,
        .max_queries = 64,
        .control = control,
    });
    defer prepared.deinit();
    if (scaling) {
        if (prepared.sequence_length != case.encoded_width or prepared.samples.len != case.expected_encoded_lengths.len)
            return error.BenchmarkScalingGeometryMismatch;
        for (prepared.samples, case.expected_encoded_lengths) |sample, expected| if (sample.input_ids.len != expected)
            return error.BenchmarkScalingGeometryMismatch;
    }
    const tokens = if (capture_tokens) try a.dupe(i64, prepared.input_ids) else null;
    errdefer if (tokens) |ids| a.free(ids);
    const attention_mask = if (capture_tokens and scaling) try a.dupe(i64, prepared.attention_mask) else null;
    errdefer if (attention_mask) |mask| a.free(mask);
    const backend_start = try diagnosticNow(options.diagnostics);
    phase.processor = backend_start - processor_start;
    var limits = device.Limits{};
    const workspace_plan: ?factory.GlinerBoundaryWorkspacePlan = if (options.execution_policy == .optimized_v2)
        try factory.planGlinerBoundaryWorkspace(session, batch, prepared.sequence_length)
    else
        null;
    var workspace_permit: ?admission_memory.AdmissionLease = null;
    defer if (workspace_permit) |*permit| permit.release();
    if (workspace_plan) |plan| {
        limits.max_encoder_device_bytes = try plan.requestEncoderLimit(limits.max_encoder_device_bytes);
        if (plan.replacement_amounts) |amounts| workspace_permit = try controller.tryAcquire(.gpu, .{
            .backend_limit_bytes = (device.Limits{}).max_combined_device_bytes,
            .scratch_limit_bytes = (device.Limits{}).max_combined_device_bytes,
        }, amounts, false);
    }
    var managed = if (workspace_plan) |plan|
        try factory.getManagedGlinerBoundaryComputeBackend(session, a, null, control, plan, if (workspace_permit) |*permit| permit else null)
    else
        try factory.getManagedComputeBackend(session, a, null, control);
    var managed_open = true;
    defer if (managed_open) managed.deinit();
    try checkBackend(&managed.backend);
    const device_start = try diagnosticNow(options.diagnostics);
    phase.backend_setup = device_start - backend_start;
    var result = try device.run(&managed.backend, a, config, &prepared, schemas[0..batch], .{
        .precision = .fp32,
        .execution_policy = options.execution_policy,
        .limits = limits,
        .pipeline = .{ .offset_unit = .unicode_codepoints, .control = control },
    });
    errdefer result.deinit();
    try checkBackend(&managed.backend);
    try control.check();
    // run() already released encoder/head owners. The managed backend,
    // prepared batch and schema also unwind before the caller stops its clock.
    const measured = try managed.backend.glinerBoundaryScope(&.snapshot);
    if (measured.active or measured.pending_device_bytes != 0 or measured.workspace_pending_bytes != 0) return error.BenchmarkIncompleteMetalScope;
    const cleanup_start = try diagnosticNow(options.diagnostics);
    phase.device = cleanup_start - device_start;
    managed.deinit();
    managed_open = false;
    phase.backend_cleanup = (try diagnosticNow(options.diagnostics)) - cleanup_start;
    return .{ .result = result, .input_ids = tokens, .attention_mask = attention_mask, .input_shape = .{ batch, prepared.sequence_length }, .runtime_stats = .{
        .actual_weight_upload_bytes = measured.actual_weight_upload_bytes,
        .scope_submissions = measured.submissions,
        .scope_wait_nanos = measured.wait_nanos,
        .scope_gpu_nanos = measured.gpu_nanos,
        .peak_pending_device_bytes = measured.peak_pending_device_bytes,
        .pending_device_bytes = measured.pending_device_bytes,
        .workspace_products = measured.workspace_products,
        .workspace_product_bytes = measured.workspace_product_bytes,
        .workspace_oversized_products = measured.workspace_oversized_products,
        .workspace_drains = measured.workspace_drains,
        .workspace_pending_bytes = measured.workspace_pending_bytes,
        .workspace_peak_pending_bytes = measured.workspace_peak_pending_bytes,
        .phase_timings_ns = if (options.diagnostics) phase else null,
    } };
}

fn memoryDelta(before: u64, after: u64) !u64 {
    return std.math.sub(u64, after, before) catch error.BenchmarkMemoryCounterRegression;
}

fn transientBytes(snapshot: MemoryStats, owners: factory.GlinerBoundaryResidentStats) !u64 {
    return std.math.sub(u64, snapshot.device_owned_live_bytes, try std.math.add(u64, owners.model_live_bytes, owners.workspace_live_bytes)) catch error.BenchmarkMemoryOwnerMismatch;
}

fn verifyRequestCleanup(before: MemoryStats, after: MemoryStats, before_owners: factory.GlinerBoundaryResidentStats, after_owners: factory.GlinerBoundaryResidentStats) !void {
    const growth = try memoryDelta(before_owners.workspace_live_bytes, after_owners.workspace_live_bytes);
    if (before_owners.model_live_bytes != after_owners.model_live_bytes or
        try transientBytes(before, before_owners) != 0 or try transientBytes(after, after_owners) != 0 or
        before.host_mirror_live_bytes != 0 or after.host_mirror_live_bytes != 0 or
        (growth == 0 and try memoryDelta(before.device_owned_buffers_created, after.device_owned_buffers_created) != try memoryDelta(before.device_owned_buffers_released, after.device_owned_buffers_released)) or
        try memoryDelta(before.device_owned_bytes_created, after.device_owned_bytes_created) != try std.math.add(u64, growth, try memoryDelta(before.device_owned_bytes_released, after.device_owned_bytes_released)) or
        try memoryDelta(before.host_mirror_allocations, after.host_mirror_allocations) != try memoryDelta(before.host_mirror_frees, after.host_mirror_frees))
        return error.BenchmarkRequestDeviceLeak;
}

fn emit(a: Allocator, writer: *std.Io.Writer, value: anytype) !void {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    if (bytes.len > 4 * 1024 * 1024) return error.BenchmarkResponseLimitExceeded;
    try writer.writeAll(bytes);
    try writer.writeByte('\n');
    try writer.flush();
}

fn parseArgs(init: std.process.Init) !Options {
    var options = Options{};
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    while (args.next()) |arg| {
        const next = args.next() orelse return error.MissingBenchmarkArgument;
        if (std.mem.eql(u8, arg, "--model-dir")) {
            options.model_dir = next;
        } else if (std.mem.eql(u8, arg, "--cases")) {
            options.cases_path = next;
        } else if (std.mem.eql(u8, arg, "--threads")) {
            options.threads = try std.fmt.parseInt(usize, next, 10);
        } else if (std.mem.eql(u8, arg, "--timeout-ms")) {
            options.timeout_ms = try std.fmt.parseInt(u64, next, 10);
        } else if (std.mem.eql(u8, arg, "--max-commands")) {
            options.max_commands = try std.fmt.parseInt(usize, next, 10);
        } else if (std.mem.eql(u8, arg, "--execution-policy")) {
            options.execution_policy = std.meta.stringToEnum(device.ExecutionPolicy, next) orelse return error.InvalidBenchmarkExecutionPolicy;
        } else if (std.mem.eql(u8, arg, "--workload")) {
            options.workload = std.meta.stringToEnum(@TypeOf(options.workload), next) orelse return error.InvalidBenchmarkWorkload;
        } else if (std.mem.eql(u8, arg, "--diagnostics")) {
            if (std.mem.eql(u8, next, "true")) options.diagnostics = true else if (std.mem.eql(u8, next, "false")) options.diagnostics = false else return error.InvalidBenchmarkDiagnostics;
        } else return error.UnknownBenchmarkArgument;
    }
    if (options.model_dir.len == 0 or options.cases_path.len == 0 or options.threads != 1 or options.timeout_ms == 0 or options.timeout_ms > 60000 or options.max_commands == 0 or options.max_commands > 4096) return error.InvalidBenchmarkOptions;
    return options;
}

fn verifyEnvironment() !void {
    inline for (.{ "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "BLIS_NUM_THREADS" }) |name| {
        const value = std.c.getenv(name) orelse return error.MissingBenchmarkThreadControl;
        if (!std.mem.eql(u8, std.mem.span(value), "1")) return error.InvalidBenchmarkThreadControl;
    }
    // Some runtime switches are enabled by presence, including empty/"0".
    // A production-default receipt therefore requires them to be absent.
    inline for (std.meta.fields(NativeEnvironment)) |field| if (std.c.getenv(field.name) != null) return error.NonDefaultBenchmarkMetalEnvironment;
}

fn closeSession(session: Session, watchdog: *Watchdog) void {
    const control = controlFor(watchdog, teardown_timeout_ms) catch inference.platform.process.exitImmediately(1);
    var guard = control.enterUninterruptible(.process_required) catch inference.platform.process.exitImmediately(1);
    defer guard.deinit();
    session.close();
}

pub fn main(init: std.process.Init) !void {
    const a = std.heap.c_allocator;
    const options = try parseArgs(init);
    var workspace_admission = admission_memory.AdmissionController{};
    defer workspace_admission.deinit();
    try verifyEnvironment();
    const bytes = try inference.util.c_file.readFileMax(a, options.cases_path, 2 * 1024 * 1024);
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(Fixture, a, bytes, .{ .ignore_unknown_fields = true, .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    const fixture = parsed.value;
    const scaling = options.workload == .scaling;
    if (fixture.format_version != @as(u32, if (scaling) 1 else 2) or fixture.cases.len == 0 or fixture.cases.len > @as(usize, if (scaling) 128 else 32) or !std.mem.eql(u8, fixture.source_commit, source_commit)) return error.UnpinnedBenchmarkFixture;
    if (fixture.requests_sha256) |digest| if (!validDigest(digest)) return error.UnpinnedBenchmarkFixture;
    if (scaling and (fixture.scope == null or !std.mem.eql(u8, fixture.scope.?, scaling_scope) or fixture.requests_sha256 == null)) return error.UnpinnedBenchmarkScalingManifest;
    const schemas = try a.alloc([]const u8, fixture.cases.len);
    defer a.free(schemas);
    var compiled_count: usize = 0;
    defer for (schemas[0..compiled_count]) |schema| a.free(schema);
    for (fixture.cases, 0..) |case, i| {
        if (case.text.len > 65536 or case.id.len == 0 or case.id.len > 64 or case.schema != .object) return error.InvalidBenchmarkCase;
        if (scaling) {
            if (case.text.len != 0 or case.items.len == 0 or case.items.len > 8 or case.expected_encoded_lengths.len != case.items.len or case.encoded_width == 0 or case.encoded_width > 512) return error.InvalidBenchmarkScalingCase;
            var maximum: usize = 0;
            for (case.items, case.expected_encoded_lengths, 0..) |item, length, index| {
                if (item.id.len == 0 or item.id.len > 64 or item.text.len > 65536 or length == 0 or length > case.encoded_width) return error.InvalidBenchmarkScalingCase;
                for (case.items[0..index]) |prior| if (std.mem.eql(u8, prior.id, item.id)) return error.DuplicateBenchmarkScalingItem;
                maximum = @max(maximum, length);
            }
            if (maximum != case.encoded_width) return error.InvalidBenchmarkScalingCase;
        } else if (case.items.len != 0 or case.expected_encoded_lengths.len != 0 or case.encoded_width != 0) return error.InvalidBenchmarkCase;
        for (fixture.cases[0..i]) |prior| if (std.mem.eql(u8, prior.id, case.id)) return error.DuplicateBenchmarkCase;
        schemas[i] = try std.json.Stringify.valueAlloc(a, case.schema, .{});
        compiled_count += 1;
    }
    const watchdog = try Watchdog.create(a);
    defer watchdog.destroy();
    try watchdog.start(init.io);
    const startup = try controlFor(watchdog, startup_timeout_ms);
    try verifyFiles(a, options.model_dir, fixture.model_files, startup);
    const process_memory_baseline = inference.metal_tensor.memoryStatsSnapshot();
    if (process_memory_baseline.device_owned_live_bytes != 0 or process_memory_baseline.host_mirror_live_bytes != 0) return error.BenchmarkUnexpectedStartupOwner;
    const session = blk: {
        var guard = try startup.enterUninterruptible(.process_required);
        defer guard.deinit();
        break :blk try factory.createMetalSession(a, options.model_dir);
    };
    var session_open = true;
    defer if (session_open) closeSession(session, watchdog);
    if (session.backend() != .metal) return error.BenchmarkMetalBackendUnavailable;
    const identity = try factory.getGlinerBoundaryIdentity(session);
    try verifyIdentity(identity, fixture);
    const config = try factory.getGlinerBoundaryConfig(session);
    if (options.execution_policy == .optimized_v2) {
        try factory.prepareGlinerBoundaryResident(session, startup);
        if (!factory.isGlinerBoundaryResidentReady(session)) return error.BenchmarkResidentPreparationIncomplete;
    }
    const tok_path = try std.fs.path.join(a, &.{ options.model_dir, "tokenizer.json" });
    defer a.free(tok_path);
    const tok_bytes = try inference.util.c_file.readFileMax(a, tok_path, 32 * 1024 * 1024);
    defer a.free(tok_bytes);
    try bundle.verifyBytes(filePin("tokenizer.json", fixture.model_files.@"tokenizer.json"), tok_bytes, startup);
    const tokenizer = try inference.hf_tokenizer.HfTokenizer.loadFromBytesWithOptions(a, tok_bytes, .{ .strict_unigram_normalizer = true });
    defer tokenizer.tokenizer().deinitTokenizer();
    // First-touch provider construction belongs to model startup. No model
    // operation or weight upload is performed by this capability check.
    {
        var managed = try factory.getManagedComputeBackend(session, a, null, startup);
        defer managed.deinit();
        try checkBackend(&managed.backend);
    }
    var device_name: [4096]u8 = undefined;
    const device_name_len = inference.metal_runtime.termite_metal_copy_device_name(null, 0);
    if (device_name_len == 0 or device_name_len > device_name.len or inference.metal_runtime.termite_metal_copy_device_name(device_name[0..].ptr, device_name.len) != device_name_len)
        return error.BenchmarkMetalDeviceIdentityUnavailable;
    const cases_digest = hash(bytes);
    const identity_digest = std.fmt.bytesToHex(identity.fingerprint(), .lower);
    const ready_owners = try factory.glinerBoundaryResidentStats(session);
    const ready_memory = inference.metal_tensor.memoryStatsSnapshot();
    if (try transientBytes(ready_memory, ready_owners) != 0) return error.BenchmarkUnexpectedStartupOwner;
    var stdout_buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    try emit(a, &stdout.interface, .{
        .event = "ready",
        .arm = arm,
        .scope = if (scaling) scaling_scope else optimized_scope,
        .workload = @tagName(options.workload),
        .diagnostics_enabled = options.diagnostics,
        .runtime_contract_version = 2,
        .execution_policy = @tagName(options.execution_policy),
        .runtime_policy = .{
            .weight_residency = if (options.execution_policy == .optimized_v2) "model" else "request",
            .relative_projection_residency = if (options.execution_policy == .optimized_v2) "model" else "request",
            .submission = if (options.execution_policy == .optimized_v2) "request_owned" else "synchronous_unframed",
            .required_outputs = if (options.execution_policy == .optimized_v2) "schema_required" else "all_enabled",
        },
        .model_live_bytes = ready_owners.model_live_bytes,
        .workspace_live_bytes = ready_owners.workspace_live_bytes,
        .workspace_capacity_bytes = if (options.execution_policy == .optimized_v2) (device.Limits{}).max_encoder_device_bytes else 0,
        .workspace_capacity_semantics = "hard_admission_upper_bound",
        .workspace_generation = ready_owners.workspace_generation,
        .timing_boundary = timing_boundary,
        .model = fixture.model,
        .model_id = fixture.model_id,
        .revision = fixture.revision,
        .model_files = fixture.model_files,
        .cases_sha256 = cases_digest[0..],
        .requests_sha256 = fixture.requests_sha256,
        .build_mode = @tagName(builtin.mode),
        .zig_version = builtin.zig_version_string,
        .threads = options.threads,
        .scheduler = "serial_requests",
        .system_blas = build_options.enable_system_blas,
        .backend = "metal",
        .device = "metal",
        .device_name = device_name[0..device_name_len],
        .dtype = "float32",
        .qualification = false,
        .artifact_identity = .{ .fingerprint_sha256 = identity_digest[0..], .backbone = @tagName(identity.backbone), .precision = @tagName(identity.precision) },
        .runtime_ready = true,
        .external_frame = false,
        .math_policy = "production_default",
        .math = .{ .gemm = "mps_matrix_or_metal_default_dispatch", .per_request_gemm_path_certified = false, .compiler_fast_math = "runtime_default_unobserved" },
        .residency = .{ .model_session = "persistent", .tokenizer = "persistent", .metal_provider = "session_cached", .compute_backend = "per_request", .device_weights = if (options.execution_policy == .optimized_v2) "model_owned" else "request_owned", .command_buffers = if (options.execution_policy == .optimized_v2) "request_owned" else "synchronous_unframed" },
        .native_environment = NativeEnvironment{},
        .host_fallback_allowed = false,
        .memory_stats_scope = "cumulative_metal_tensor_owners_not_process_rss_or_runtime_cache",
        .owned_memory = ready_memory,
        .startup_timeout_ms = startup_timeout_ms,
        .request_timeout_ms = options.timeout_ms,
        .teardown_timeout_ms = teardown_timeout_ms,
    });
    var stdin_buffer: [4096]u8 = undefined;
    var stdin = std.Io.File.stdin().readerStreaming(init.io, &stdin_buffer);
    var count: usize = 0;
    var last_request_id: ?u32 = null;
    while (try stdin.interface.takeDelimiter('\n')) |line| {
        if (line.len > 2048 or count >= options.max_commands) return error.BenchmarkCommandLimitExceeded;
        count += 1;
        const command = try std.json.parseFromSlice(Command, a, line, .{ .duplicate_field_behavior = .@"error" });
        defer command.deinit();
        const cmd = command.value;
        if (last_request_id) |previous| if (cmd.request_id <= previous) return error.BenchmarkRequestIdOutOfOrder;
        last_request_id = cmd.request_id;
        if (cmd.op == .stop) {
            try verifyFiles(a, options.model_dir, fixture.model_files, try controlFor(watchdog, startup_timeout_ms));
            const final_bytes = try inference.util.c_file.readFileMax(a, options.cases_path, 2 * 1024 * 1024);
            defer a.free(final_bytes);
            if (bytes.len != final_bytes.len or !std.mem.eql(u8, &cases_digest, &hash(final_bytes))) return error.BenchmarkFixtureIdentityMismatch;
            closeSession(session, watchdog);
            session_open = false;
            const stopped_memory = inference.metal_tensor.memoryStatsSnapshot();
            if (stopped_memory.device_owned_live_bytes != process_memory_baseline.device_owned_live_bytes or stopped_memory.host_mirror_live_bytes != process_memory_baseline.host_mirror_live_bytes) return error.BenchmarkSessionDeviceLeak;
            const stopped_admission = workspace_admission.snapshot();
            if (stopped_admission.backendTotalBytes() != 0 or stopped_admission.hostTotalBytes() != 0) return error.BenchmarkWorkspaceAdmissionLeak;
            try emit(a, &stdout.interface, .{ .event = "stopped", .arm = arm, .request_id = cmd.request_id, .runtime_contract_version = 2, .execution_policy = @tagName(options.execution_policy), .owned_memory = stopped_memory, .model_live_bytes = 0, .workspace_live_bytes = 0, .transient_live_bytes = 0, .pending_device_bytes = 0 });
            return;
        }
        var case_index: ?usize = null;
        for (fixture.cases, 0..) |case, i| if (std.mem.eql(u8, cmd.case_id, case.id)) {
            case_index = i;
            break;
        };
        const i = case_index orelse return error.UnknownBenchmarkCase;
        const control = try controlFor(watchdog, options.timeout_ms);
        // Observations must not introduce dispatches, synchronization, resets,
        // allocations or a second managed lifetime inside the measured work.
        const memory_before = inference.metal_tensor.memoryStatsSnapshot();
        const owners_before = try factory.glinerBoundaryResidentStats(session);
        const started = try nowNs();
        var result = try extract(a, session, &workspace_admission, &config, tokenizer.tokenizer(), &fixture.cases[i], schemas[i], cmd.op == .validate, options, control);
        defer result.deinit(a);
        const elapsed = (try nowNs()) - started;
        const memory_after = inference.metal_tensor.memoryStatsSnapshot();
        const owners_after = try factory.glinerBoundaryResidentStats(session);
        try verifyRequestCleanup(memory_before, memory_after, owners_before, owners_after);
        result.runtime_stats.workspace_live_bytes = owners_after.workspace_live_bytes;
        const stats = result.result.stats;
        const gpu_work_submitted = stats.encoder.device_dispatches != 0 or if (stats.head) |head| head.device_dispatches != 0 else false;
        if (elapsed == 0 or result.result.outputs.samples.len != @as(usize, if (scaling) fixture.cases[i].items.len else 1) or !gpu_work_submitted) return error.InvalidBenchmarkResult;
        const admitted = result.result.admission;
        try emit(a, &stdout.interface, .{
            .event = "result",
            .arm = arm,
            .runtime_contract_version = 2,
            .execution_policy = @tagName(options.execution_policy),
            .request_id = cmd.request_id,
            .case_id = cmd.case_id,
            .duration_ns = elapsed,
            .input_ids = result.input_ids,
            .input_shape = result.input_shape,
            .attention_mask = result.attention_mask,
            .output = result.result.outputs.samples[0],
            .outputs = if (scaling) result.result.outputs.samples else null,
            .gpu_work_submitted = gpu_work_submitted,
            .external_frame = false,
            .request_stats = stats,
            .device_admission = .{ .encoder_budget_bytes = admitted.encoder_budget_bytes, .head_budget_bytes = admitted.head_budget_bytes, .combined_device_upper_bound_bytes = admitted.combined_device_upper_bound_bytes, .minimum_result_download_bytes = admitted.minimum_result_download_bytes },
            .runtime_stats = result.runtime_stats,
            .owned_memory = .{
                .before = memory_before,
                .after = memory_after,
                .model = .{ .before_live_bytes = owners_before.model_live_bytes, .after_live_bytes = owners_after.model_live_bytes },
                .workspace = .{ .before_live_bytes = owners_before.workspace_live_bytes, .after_live_bytes = owners_after.workspace_live_bytes, .generation = owners_after.workspace_generation },
                .transient = .{ .before_live_bytes = try transientBytes(memory_before, owners_before), .after_live_bytes = try transientBytes(memory_after, owners_after), .pending_after_bytes = result.runtime_stats.pending_device_bytes },
            },
            .host_fallback_evidence = .{
                .strict_device_dispatch = true,
                .host_mirror_allocations_delta = try memoryDelta(memory_before.host_mirror_allocations, memory_after.host_mirror_allocations),
                .host_mirror_download_bytes_delta = try memoryDelta(memory_before.host_mirror_download_bytes, memory_after.host_mirror_download_bytes),
                .to_host_device_calls_delta = try memoryDelta(memory_before.to_host_device_calls, memory_after.to_host_device_calls),
            },
        });
    }
    return error.BenchmarkProtocolEndedWithoutStop;
}
