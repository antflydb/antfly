// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const internal = @import("inference_internal");
const job = internal.finetune.gliner_boundary_training_job;
const process = internal.platform.one_shot_process;

pub fn main(init: std.process.Init) !void {
    return mainWithOriginal(init, init.minimal.args);
}

/// Finetune dispatch preserves the invocation before it replaces argv[0].
/// Runtime ABI callers additionally carry the real executable's bounded argv.
pub fn mainWithOriginal(init: std.process.Init, original: std.process.Args) !void {
    if (try commandWithOriginal(init, original)) |outcome| process.finishParent(outcome);
}

/// All command-owned argument/configuration allocations drain before the
/// completed parent exits. The executable and runtime ABI may have C exit
/// handlers or unrelated IO teardown, so do not return into either after a
/// supervised job. Help and pre-supervision setup errors retain normal return.
fn commandWithOriginal(init: std.process.Init, original: std.process.Args) !?process.Outcome {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const config_path = args.next() orelse return usage();
    if (std.mem.eql(u8, config_path, "--help") or std.mem.eql(u8, config_path, "-h")) {
        help();
        return null;
    }
    var tail = std.ArrayListUnmanaged([]const u8).empty;
    defer tail.deinit(init.gpa);
    try tail.append(init.gpa, config_path);
    var stop: ?u64 = null;
    var grace: ?u32 = null;
    while (args.next()) |arg| {
        const value = args.next() orelse return usage();
        if (std.mem.eql(u8, arg, "--stop-after-microbatches") and stop == null) {
            stop = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--shutdown-grace-seconds") and grace == null) {
            grace = try std.fmt.parseUnsigned(u32, value, 10);
            if (grace.? == 0 or grace.? > 300) return usage();
        } else return usage();
        try tail.appendSlice(init.gpa, &.{ arg, value });
    }
    const grace_seconds = grace orelse 30;
    if (try process.workerRequested(init.environ_map)) {
        // Start the executor-independent monitor before rereading the config
        // or initializing any model/provider. It survives Job's full teardown.
        const worker = try process.Worker.create(init.gpa, init.io, init.environ_map);
        workerMain(init, config_path, stop, grace_seconds, worker) catch |err| {
            std.debug.print("GLiNER2.5 training worker failed: {s}\n", .{@errorName(err)});
            worker.finish(if (err == error.Timeout) process.timeout_exit_code else 1);
        };
        worker.finish(0);
    }
    var invocation = try process.originalArguments(init.gpa, original, init.environ_map, tail.items);
    defer invocation.deinit();
    var config = try job.loadConfigSnapshot(init.gpa, init.io, config_path);
    defer config.deinit();
    return process.runParent(init, invocation.values, .{
        .timeout_ns = try std.math.mul(u64, config.parsed.value.timeout_seconds, std.time.ns_per_s),
        .shutdown_grace_ns = @as(u64, grace_seconds) * std.time.ns_per_s,
        .fingerprint = configFingerprint(config, stop, grace_seconds),
    }) catch |err| {
        // runParent has already killed/reaped any child it owned. Supervisor
        // failures need the same cleanup-before-immediate-exit boundary as
        // completed outcomes; pre-supervision argument/config errors above
        // remain ordinary CLI errors.
        std.debug.print("GLiNER2.5 training supervisor failed: {s}\n", .{@errorName(err)});
        return process.Outcome{ .term = .{ .exited = 1 } };
    };
}

fn configFingerprint(config: job.ConfigSnapshot, stop: ?u64, grace_seconds: u32) [32]u8 {
    return contractFingerprint(config.size_bytes, config.sha256, stop, grace_seconds);
}

fn contractFingerprint(size_bytes: u64, sha256: [32]u8, stop: ?u64, grace_seconds: u32) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.training-process.config.v1\x00");
    var number: [8]u8 = undefined;
    std.mem.writeInt(u64, &number, size_bytes, .little);
    hash.update(&number);
    hash.update(&sha256);
    hash.update(&.{@intFromBool(stop != null)});
    std.mem.writeInt(u64, &number, stop orelse 0, .little);
    hash.update(&number);
    std.mem.writeInt(u64, &number, grace_seconds, .little);
    hash.update(&number);
    return hash.finalResult();
}

fn workerMain(init: std.process.Init, config_path: []const u8, stop: ?u64, grace_seconds: u32, worker: *process.Worker) !void {
    const watchdog = try internal.HardCancellationWatchdog.create(init.gpa);
    defer watchdog.destroy();
    try watchdog.start(init.io);
    var guard = try (internal.InferenceExecutionControl{ .io = init.io, .ptr = worker, .check_fn = process.Worker.checkHard, .hard_cancellation = watchdog.boundary() }).enterUninterruptible(.process_required);
    defer guard.deinit();
    var config = try job.loadConfigSnapshot(init.gpa, init.io, config_path);
    defer config.deinit();
    try worker.verifyFingerprint(configFingerprint(config, stop, grace_seconds));
    var admission = internal.runtime.tier.memory.AdmissionController{};
    defer admission.deinit();
    var progress = Progress{ .io = init.io };
    const result = try job.execute(init.gpa, init.io, config.parsed.value, &admission, .{
        .stop_after_microbatches = stop,
        .pause_context = worker,
        .pause_requested = process.Worker.pauseRequested,
        .report_context = &progress,
        .report_fn = Progress.report,
    }, .{ .io = init.io, .deadline_ns = worker.softDeadline(), .hard_cancellation = watchdog.boundary() });
    try writeEvent(init.io, std.Io.File.stdout(), .{ .event = "result", .result = result, .output_dir = config.parsed.value.output_dir });
}

fn writeEvent(io: std.Io, file: std.Io.File, value: anytype) !void {
    var buffer: [8192]u8 = undefined;
    var writer = file.writerStreaming(io, &buffer);
    try std.json.Stringify.value(value, .{}, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

const Progress = struct {
    io: std.Io,
    fn report(raw: ?*anyopaque, value: internal.finetune.gliner_boundary_native_trainer.Report) !void {
        const self: *Progress = @ptrCast(@alignCast(raw.?));
        try writeEvent(self.io, std.Io.File.stdout(), .{ .event = "step", .report = value });
    }
};

fn help() void {
    std.debug.print(
        \\usage: antfly-inference finetune train gliner25 <job.json> [--stop-after-microbatches N] [--shutdown-grace-seconds N]
        \\The version-1 job specifies absolute source/data/output paths and FP32 training settings.
        \\execution defaults to native; resident_metal requires a Metal-enabled build and admitted device budgets.
        \\The output directory must be new. To resume, set resume_from to latest.safetensors and choose a new output directory.
        \\A cooperative stop preserves unfinished accumulation; a completed run exports a portable model or PEFT adapter.
        \\SIGINT/SIGTERM requests a checkpoint at the next safe boundary; a second signal forces termination.
        \\Shutdown grace defaults to 30 seconds (maximum 300). Job timeout is bounded to seven days.
        \\Timeout exits 124; signal interruption exits 128 plus the signal number.
        \\One disposable worker owns each invocation; failures never restart automatically.
        \\
    , .{});
}
fn usage() error{InvalidArguments} {
    help();
    return error.InvalidArguments;
}

test "GLiNER25 training CLI redirected streaming events preserve all JSON lines" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const file = try temporary.dir.createFile(io, "events.jsonl", .{ .read = true });
    defer file.close(io);
    try writeEvent(io, file, .{ .event = "step", .microbatch = 1 });
    try writeEvent(io, file, .{ .event = "step", .microbatch = 2 });
    try writeEvent(io, file, .{ .event = "result", .status = "paused" });
    try file.sync(io);
    const bytes = try temporary.dir.readFileAlloc(io, "events.jsonl", a, .limited(8192));
    defer a.free(bytes);
    try std.testing.expectEqualStrings("{\"event\":\"step\",\"microbatch\":1}\n{\"event\":\"step\",\"microbatch\":2}\n{\"event\":\"result\",\"status\":\"paused\"}\n", bytes);
}

test "GLiNER25 training CLI config fingerprint binds exact bytes pause and grace" {
    const base = contractFingerprint(42, .{3} ** 32, null, 30);
    const variants = [_][32]u8{
        contractFingerprint(43, .{3} ** 32, null, 30),
        contractFingerprint(42, .{4} ** 32, null, 30),
        contractFingerprint(42, .{3} ** 32, 0, 30),
        contractFingerprint(42, .{3} ** 32, 1, 30),
        contractFingerprint(42, .{3} ** 32, null, 31),
    };
    for (variants, 0..) |variant, index| {
        try std.testing.expect(!std.mem.eql(u8, &base, &variant));
        for (variants[index + 1 ..]) |other| try std.testing.expect(!std.mem.eql(u8, &variant, &other));
    }
    try std.testing.expectEqual(base, contractFingerprint(42, .{3} ** 32, null, 30));
}
