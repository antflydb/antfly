// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Disposable, bounded CPU/Metal execution of an exactly pinned trained FP32
//! artifact. The independent driver owns a private six-file copy and compares
//! every result. No expected values, PEFT runtime, or pretrained outputs enter
//! this worker, and this worker does not issue a qualification receipt.
const std = @import("std");
const inference = @import("inference_internal");
const contract = @import("gliner25_trained_contract.zig");
const factory = inference.architectures.session_factory;
const pipeline = inference.pipelines.gliner_boundary_pipeline;
const processor = inference.pipelines.gliner_boundary_processor;
const engine = inference.architectures.gliner_boundary_engine;
const device = inference.architectures.gliner_boundary_request_device;
const bundle = inference.models.gliner_boundary_bundle;
const memory = inference.runtime.tier.memory;
const Allocator = std.mem.Allocator;
const Control = inference.InferenceExecutionControl;
const mib = 1024 * 1024;

const Arguments = struct {
    directory: []const u8,
    fixture_path: []const u8,
    inputs_path: []const u8,
    backend: contract.Backend,
};
fn parseArgs(args: []const []const u8) !Arguments {
    var directory: ?[]const u8 = null;
    var fixture: ?[]const u8 = null;
    var inputs: ?[]const u8 = null;
    var backend: ?contract.Backend = null;
    if (args.len != 8) return error.InvalidTrainedExecutionArguments;
    var i: usize = 0;
    while (i < args.len) : (i += 2) {
        const name = args[i];
        const value = args[i + 1];
        if (std.mem.eql(u8, name, "--model-dir") and directory == null) directory = value else if (std.mem.eql(u8, name, "--fixture") and fixture == null) fixture = value else if (std.mem.eql(u8, name, "--inputs") and inputs == null) inputs = value else if (std.mem.eql(u8, name, "--backend") and backend == null) {
            backend = std.meta.stringToEnum(contract.Backend, value) orelse return error.InvalidTrainedExecutionArguments;
        } else return error.InvalidTrainedExecutionArguments;
    }
    const result = Arguments{ .directory = directory orelse return error.InvalidTrainedExecutionArguments, .fixture_path = fixture orelse return error.InvalidTrainedExecutionArguments, .inputs_path = inputs orelse return error.InvalidTrainedExecutionArguments, .backend = backend orelse return error.InvalidTrainedExecutionArguments };
    for ([_][]const u8{ result.directory, result.fixture_path, result.inputs_path }) |path|
        if (!std.fs.path.isAbsolute(path) or path.len >= std.fs.max_path_bytes or std.mem.indexOfScalar(u8, path, 0) != null)
            return error.InvalidTrainedExecutionArguments;
    return result;
}

pub fn main(init: std.process.Init) !void {
    var iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = iter.next();
    var values: [8][]const u8 = undefined;
    var count: usize = 0;
    while (iter.next()) |arg| {
        if (count == values.len) return error.InvalidTrainedExecutionArguments;
        values[count] = arg;
        count += 1;
    }
    if (count == 1 and std.mem.eql(u8, values[0], "--help")) {
        try std.Io.File.stdout().writeStreamingAll(init.io, "Usage: antfly-inference-gliner25-trained-check --model-dir PRIVATE_COPY --fixture ENVELOPE_JSON --inputs PINNED_INPUTS_JSON --backend native|metal\n");
        return;
    }
    const args = try parseArgs(values[0..count]);
    try run(init.gpa, init.io, args);
}

const Output = struct {
    io: std.Io,
    limits: contract.Limits,
    total_bytes: usize = 0,
    fn emit(self: *Output, a: Allocator, value: anytype) !void {
        const buffer = try a.alloc(u8, self.limits.event_bytes);
        defer a.free(buffer);
        try self.emitInBuffer(buffer, value);
    }
    fn emitInBuffer(self: *Output, buffer: []u8, value: anytype) !void {
        var writer = std.Io.Writer.fixed(buffer);
        std.json.Stringify.value(value, .{}, &writer) catch return error.TrainedExecutionLimitExceeded;
        const bytes = writer.buffered();
        const count = std.math.add(usize, bytes.len, 1) catch return error.TrainedExecutionLimitExceeded;
        if (count > self.limits.event_bytes or count > self.limits.total_output_bytes -| self.total_bytes)
            return error.TrainedExecutionLimitExceeded;
        try std.Io.File.stdout().writeStreamingAll(self.io, bytes);
        try std.Io.File.stdout().writeStreamingAll(self.io, "\n");
        self.total_bytes += count;
    }
};

fn deadline(start: u64, milliseconds: u64) !u64 {
    return std.math.add(u64, start, try std.math.mul(u64, milliseconds, std.time.ns_per_ms));
}

// The Python owner keeps this inherited pipe open without sending commands.
// The monitor samples only this live descriptor; it never signals a saved PID.
// A killed parent closes the pipe, so driver-loss shutdown does not depend on
// the parent's ability to run a finally block or on a cooperative kernel.
const Lifeline = struct {
    fd: std.posix.fd_t,
    fn check(raw: ?*anyopaque) !void {
        const self: *const Lifeline = @ptrCast(@alignCast(raw.?));
        var descriptors = [_]std.posix.pollfd{.{ .fd = self.fd, .events = std.posix.POLL.IN, .revents = 0 }};
        if (try std.posix.poll(&descriptors, 0) != 0) return error.TrainedExecutionLifelineClosed;
    }
};
fn run(a: Allocator, io: std.Io, args: Arguments) !void {
    const started = inference.platform.time.monotonicNs();
    const initial = Control{ .io = io, .deadline_ns = try deadline(started, 180000) };
    var verified = try contract.Verified.open(a, io, args.directory, args.fixture_path, args.inputs_path, args.backend, initial);
    defer verified.deinit();
    const envelope = verified.envelope.value;
    const amounts = try contract.admission(envelope);
    const expires = try deadline(started, envelope.limits.total_timeout_ms);
    const startup_expires = @min(expires, try deadline(started, envelope.limits.startup_timeout_ms));
    const setup = verified.owner.allocator();
    const watchdog = try inference.HardCancellationWatchdog.create(setup);
    defer watchdog.destroy();
    try watchdog.start(io);
    if ((try std.Io.File.stdin().stat(io)).kind != .named_pipe) return error.InvalidTrainedExecutionLifeline;
    var lifeline = Lifeline{ .fd = std.Io.File.stdin().handle };
    const overall = Control{ .io = io, .deadline_ns = expires, .hard_cancellation = watchdog.boundary(), .ptr = &lifeline, .check_fn = Lifeline.check };
    var startup = overall;
    startup.deadline_ns = startup_expires;
    var overall_guard = try overall.enterUninterruptible(.process_required);
    defer overall_guard.deinit();
    var admission = memory.AdmissionController{};
    defer admission.deinit();
    var lease = try admission.tryAcquire(if (args.backend == .metal) .gpu else .cpu, .{
        .host_limit_bytes = amounts.host_total_bytes,
        .backend_limit_bytes = amounts.backend_total_bytes,
        .combined_limit_bytes = envelope.limits.combined_bytes,
    }, .{ .host_weight_bytes = amounts.mapped_weight_bytes, .host_scratch_bytes = amounts.host_total_bytes - amounts.mapped_weight_bytes, .backend_scratch_bytes = amounts.backend_total_bytes }, true);
    defer lease.release();
    {
        var guard = try startup.enterUninterruptible(.process_required);
        defer guard.deinit();
        try contract.verifyArtifacts(setup, io, args.directory, envelope, startup);
    }
    var output = Output{ .io = io, .limits = envelope.limits };
    const loader = try contract.Owner.create(a, envelope.limits.loader_host_bytes);
    defer loader.destroy();
    execute(a, io, args, &verified, amounts, &admission, loader, startup, overall, &output) catch |err| return loader.mapError(err);
    // execute's session, tokenizer, model caches and per-request owners have
    // all been released before a successful terminal event can be published.
    if (loader.budget.live != 0) return error.TrainedExecutionOwnerLeak;
    try contract.verifyArtifacts(setup, io, args.directory, envelope, overall);
    try contract.verifyInputs(setup, io, args.fixture_path, args.inputs_path, verified.fixture_digest, overall);
    try overall.check();
    try output.emit(setup, .{ .event = "complete", .cases = @as(usize, 10), .errors = @as(usize, 0), .qualification = false, .fixture_digest = verified.fixture_digest, .inputs_digest = contract.input_pin, .merge_receipt = envelope.merge_receipt, .identity = envelope.merge.merged, .loader_host_peak_bytes = loader.budget.peak + @sizeOf(contract.Owner), .output_bytes_before_complete = output.total_bytes });
}

fn execute(a: Allocator, io: std.Io, args: Arguments, verified: *const contract.Verified, amounts: contract.Admission, admission: *memory.AdmissionController, loader: *contract.Owner, startup: Control, overall: Control, output: *Output) !void {
    const envelope = verified.envelope.value;
    const model_allocator = loader.allocator();
    var startup_guard = try startup.enterUninterruptible(.process_required);
    defer startup_guard.deinit();
    const session = if (args.backend == .metal) try factory.createMetalSession(model_allocator, args.directory) else try factory.createNativeSession(model_allocator, args.directory);
    defer session.close();
    const identity = try factory.getGlinerBoundaryIdentity(session);
    if (!contract.equalIdentity(identity, envelope.merge.merged)) return error.TrainedExecutionArtifactMismatch;
    // All lazy FP32 host tensors borrow the separately charged aligned mmap.
    // Shared cache bookkeeping therefore receives its resident source credit;
    // owned per-request F32 cache copies remain in the request allocator.
    try factory.configureSharedCacheAdmissionForSession(session, model_allocator, admission, if (args.backend == .metal) .gpu else .cpu, .{
        .host_limit_bytes = amounts.host_total_bytes,
        .backend_limit_bytes = amounts.backend_total_bytes,
        .combined_limit_bytes = envelope.limits.combined_bytes,
    }, .{ .host_weight_bytes = amounts.mapped_weight_bytes });
    const config = try factory.getGlinerBoundaryConfig(session);
    const tokenizer_path = try std.fs.path.join(model_allocator, &.{ args.directory, "tokenizer.json" });
    defer model_allocator.free(tokenizer_path);
    const tokenizer_bytes = try inference.file_snapshot.read(model_allocator, io, .cwd(), tokenizer_path, 32 * mib, startup);
    defer model_allocator.free(tokenizer_bytes);
    if (!std.meta.eql(bundle.Digest.of(tokenizer_bytes), identity.sidecars[2])) return error.TrainedExecutionArtifactMismatch;
    const tokenizer = try inference.hf_tokenizer.HfTokenizer.loadFromBytesWithOptions(model_allocator, tokenizer_bytes, .{ .strict_unigram_normalizer = true });
    defer tokenizer.tokenizer().deinitTokenizer();
    try startup.check();
    startup_guard.deinit();
    try output.emit(verified.owner.allocator(), .{ .event = "ready", .scope = contract.scope, .version = @as(u32, 1), .qualification = false, .backend = args.backend, .math_policy = contract.math_policy, .weight_precision = identity.precision, .activation_precision = "f32", .accumulation_precision = "f32", .head_precision = "f32", .build_mode = @tagName(@import("builtin").mode), .zig_version = @import("builtin").zig_version_string, .fixture_digest = verified.fixture_digest, .inputs_digest = contract.input_pin, .oracle_report = envelope.oracle_report, .merge_receipt = envelope.merge_receipt, .identity = identity, .limits = envelope.limits, .admission = amounts });
    for (verified.inputs.value.cases) |case| {
        try overall.check();
        const request_owner = try contract.Owner.create(a, amounts.request_host_bytes);
        defer request_owner.destroy();
        var request_control = overall;
        request_control.deadline_ns = @min(overall.deadline_ns.?, try deadline(inference.platform.time.monotonicNs(), envelope.limits.request_timeout_ms));
        executeCase(request_owner, session, &config, tokenizer.tokenizer(), envelope, amounts, case, request_control, output) catch |err|
            return request_owner.mapError(err);
    }
}

fn encoderLimits() engine.Limits {
    return .{ .max_batch = 1, .max_sequence_tokens = 512, .max_batch_tokens = 512, .max_text_words = 128, .max_queries = 64, .max_classification_labels = 512, .max_groups = 64, .max_relations = 64 };
}
fn executeCase(owner: *contract.Owner, session: anytype, config: anytype, tokenizer: anytype, envelope: contract.Envelope, amounts: contract.Admission, case: contract.Case, control: Control, output: *Output) !void {
    const a = owner.allocator();
    const request_digest = try contract.canonicalRequestDigest(a, case);
    const schema_bytes = try std.json.Stringify.valueAlloc(a, case.schema, .{});
    defer a.free(schema_bytes);
    var schema = try inference.pipelines.extraction_schema.compile(a, schema_bytes, .{});
    defer schema.deinit();
    var prepared = try processor.prepare(a, tokenizer, &.{.{ .text = case.text, .schema = &schema }}, .{
        .max_batch_items = 1,
        .max_text_bytes = mib,
        .max_text_words = 128,
        .max_total_words = 128,
        .max_sequence_tokens = 512,
        .max_batch_tokens = 512,
        .max_queries = 64,
        .max_classification_labels = 512,
        .word_splitter = .whitespace,
        .control = control,
    });
    defer prepared.deinit();
    // Weight reservations are logical aliases of admitted resident/mapped
    // storage; allocator accounting separately bounds actual host ownership.
    var run_budget = memory.RunBudget.init(.{ .host_limit_bytes = try std.math.add(usize, amounts.mapped_weight_bytes, amounts.request_host_bytes), .backend_limit_bytes = amounts.device_context_bytes, .combined_limit_bytes = envelope.limits.combined_bytes });
    var native_guard = if (envelope.backend == .native) try control.enterUninterruptible(.process_required) else inference.execution_control.UninterruptibleGuard{};
    defer native_guard.deinit();
    var managed = try factory.getManagedComputeBackend(session, a, &run_budget, control);
    defer managed.deinit();
    const options = pipeline.Options{
        .threshold = 0.5,
        .overlap = .flat,
        .best_effort = false,
        .offset_unit = .unicode_codepoints,
        .control = control,
        .max_output_values = 2048,
        .max_output_string_bytes = mib,
        .head_limits = .{ .max_batch = 1, .max_text_words = 128, .max_queries = 64 },
    };
    var metal_stats: ?@FieldType(device.Result, "stats") = null;
    var result = if (envelope.backend == .metal) blk: {
        const output_device = try device.run(&managed.backend, a, config, &prepared, &.{&schema}, .{
            .precision = .fp32,
            .pipeline = options,
            .limits = .{
                .encoder = encoderLimits(),
                .max_encoder_device_bytes = envelope.limits.encoder_device_bytes,
                .max_combined_device_bytes = amounts.device_context_bytes,
                .head = .{ .max_batch = 1, .max_text_words = 128, .max_queries = 64, .max_device_bytes = envelope.limits.head_device_bytes, .max_proposal_download_bytes = envelope.limits.proposal_download_bytes, .max_result_download_bytes = envelope.limits.result_download_bytes },
                .scorer = .{ .max_result_download_bytes = envelope.limits.result_download_bytes },
            },
        });
        metal_stats = output_device.stats;
        break :blk output_device.outputs;
    } else blk: {
        var encoded = try engine.encodeNative(&managed.backend, a, config, &prepared, .{ .limits = encoderLimits(), .control = control });
        defer encoded.deinit();
        break :blk try pipeline.runNative(&managed.backend, a, config, &prepared, &.{&schema}, .{
            .text_states = encoded.text_states,
            .query_states = encoded.query_states,
            .classification_states = encoded.classification_states,
            .text_lengths = encoded.text_lengths,
        }, options);
    };
    defer result.deinit();
    if (result.samples.len != 1) return error.InvalidExtractionOutput;
    try control.check();
    const event_buffer = try a.alloc(u8, envelope.limits.event_bytes);
    defer a.free(event_buffer);
    try output.emitInBuffer(event_buffer, .{ .event = "result", .case_id = case.id, .canonical_request_sha256 = request_digest.sha256[0..], .input_ids = prepared.input_ids, .output = result.samples[0], .request_host_peak_bytes = owner.budget.peak + @sizeOf(contract.Owner), .metal = metal_stats });
}

test "trained execution arguments require explicit backend and absolute carriers" {
    const valid = [_][]const u8{ "--model-dir", "/models/private", "--fixture", "/inputs/envelope", "--inputs", "/inputs/cases", "--backend", "native" };
    try std.testing.expectEqual(contract.Backend.native, (try parseArgs(&valid)).backend);
    try std.testing.expectError(error.InvalidTrainedExecutionArguments, parseArgs(valid[0..6]));
    var changed = valid;
    changed[7] = "cuda";
    try std.testing.expectError(error.InvalidTrainedExecutionArguments, parseArgs(&changed));
    changed = valid;
    changed[1] = "relative";
    try std.testing.expectError(error.InvalidTrainedExecutionArguments, parseArgs(&changed));
    changed = valid;
    changed[4] = "--fixture";
    try std.testing.expectError(error.InvalidTrainedExecutionArguments, parseArgs(&changed));
}

test "trained execution lifeline observes closure without signalling another process" {
    if (@import("builtin").os.tag != .macos and @import("builtin").os.tag != .linux) return error.SkipZigTest;
    const C = struct {
        extern "c" fn pipe(*[2]std.posix.fd_t) c_int;
    };
    var descriptors: [2]std.posix.fd_t = undefined;
    if (C.pipe(&descriptors) != 0) return error.Unexpected;
    const reader = std.Io.File{ .handle = descriptors[0], .flags = .{ .nonblocking = false } };
    const writer = std.Io.File{ .handle = descriptors[1], .flags = .{ .nonblocking = false } };
    defer reader.close(std.testing.io);
    var writer_open = true;
    defer if (writer_open) writer.close(std.testing.io);
    var lifeline = Lifeline{ .fd = descriptors[0] };
    try Lifeline.check(&lifeline);
    writer.close(std.testing.io);
    writer_open = false;
    try std.testing.expectError(error.TrainedExecutionLifelineClosed, Lifeline.check(&lifeline));
}

test {
    _ = contract;
}
