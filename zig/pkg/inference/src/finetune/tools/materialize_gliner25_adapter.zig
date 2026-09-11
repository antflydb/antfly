// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const internal = @import("inference_internal");
const job = internal.finetune.gliner_boundary_merge_job;
const process = internal.platform.one_shot_process;

pub fn main(init: std.process.Init) !void {
    return mainWithOriginal(init, init.minimal.args);
}
pub fn mainWithOriginal(init: std.process.Init, original: std.process.Args) !void {
    if (try command(init, original)) |outcome| process.finishParent(outcome);
}

const Options = struct { config_path: []const u8, grace_seconds: u32 = 30 };
fn parseOptions(tail: []const []const u8) !Options {
    if (tail.len != 1 and tail.len != 3) return error.InvalidArguments;
    if (tail[0].len == 0 or std.mem.startsWith(u8, tail[0], "--")) return error.InvalidArguments;
    var options = Options{ .config_path = tail[0] };
    if (tail.len == 3) {
        if (!std.mem.eql(u8, tail[1], "--shutdown-grace-seconds")) return error.InvalidArguments;
        options.grace_seconds = std.fmt.parseUnsigned(u32, tail[2], 10) catch return error.InvalidArguments;
        if (options.grace_seconds == 0 or options.grace_seconds > 300) return error.InvalidArguments;
    }
    return options;
}

fn command(init: std.process.Init, original: std.process.Args) !?process.Outcome {
    var arguments = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer arguments.deinit();
    _ = arguments.next();
    var tail = std.ArrayListUnmanaged([]const u8).empty;
    defer tail.deinit(init.gpa);
    while (arguments.next()) |argument| {
        if (tail.items.len == 3) return usage();
        try tail.append(init.gpa, argument);
    }
    if (tail.items.len == 1 and (std.mem.eql(u8, tail.items[0], "--help") or std.mem.eql(u8, tail.items[0], "-h"))) {
        help();
        return null;
    }
    const options = parseOptions(tail.items) catch return usage();
    if (try process.workerRequested(init.environ_map)) {
        const worker = try process.Worker.create(init.gpa, init.io, init.environ_map);
        workerMain(init, options, worker) catch |err| {
            std.debug.print("GLiNER2.5 merge worker failed: {s}\n", .{@errorName(err)});
            worker.finish(switch (err) {
                error.Timeout => process.timeout_exit_code,
                error.Cancelled => 130,
                else => 1,
            });
        };
        worker.finish(0);
    }
    var snapshot = try job.loadConfigSnapshot(init.gpa, init.io, options.config_path);
    defer snapshot.deinit();
    var invocation = process.originalArguments(snapshot.allocator(), original, init.environ_map, tail.items) catch |err| return snapshot.mapAllocationError(err);
    defer invocation.deinit();
    return process.runParent(init, invocation.values, .{
        .timeout_ns = @as(u64, snapshot.parsed.value.timeout_seconds) * std.time.ns_per_s,
        .shutdown_grace_ns = @as(u64, options.grace_seconds) * std.time.ns_per_s,
        .fingerprint = fingerprint(snapshot.digest, options.grace_seconds),
    }) catch |err| {
        std.debug.print("GLiNER2.5 merge supervisor failed: {s}\n", .{@errorName(err)});
        return process.Outcome{ .term = .{ .exited = 1 } };
    };
}

fn fingerprint(digest: internal.models.gliner_boundary_bundle.Digest, grace_seconds: u32) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.merge-process.config.v1\x00");
    var number: [8]u8 = undefined;
    std.mem.writeInt(u64, &number, digest.size_bytes, .little);
    hash.update(&number);
    hash.update(&digest.sha256);
    std.mem.writeInt(u64, &number, grace_seconds, .little);
    hash.update(&number);
    return hash.finalResult();
}

fn checkMerge(raw: ?*anyopaque) !void {
    try process.Worker.checkHard(raw);
    if (process.Worker.pauseRequested(raw)) return error.Cancelled;
}

fn workerMain(init: std.process.Init, options: Options, worker: *process.Worker) !void {
    // Keep the hard monitor alive through all cleanup. The guard checks the
    // hard deadline only; cooperative cancellation is allowed to clean private
    // staging before the independent process deadline terminates the worker.
    const watchdog = try internal.HardCancellationWatchdog.create(init.gpa);
    defer watchdog.destroy();
    try watchdog.start(init.io);
    var guard = try (internal.InferenceExecutionControl{ .io = init.io, .ptr = worker, .check_fn = process.Worker.checkHard, .hard_cancellation = watchdog.boundary() }).enterUninterruptible(.process_required);
    defer guard.deinit();
    var snapshot = try job.loadConfigSnapshot(init.gpa, init.io, options.config_path);
    defer snapshot.deinit();
    try worker.verifyFingerprint(fingerprint(snapshot.digest, options.grace_seconds));
    var admission = internal.runtime.tier.memory.AdmissionController{};
    defer admission.deinit();
    const result = try job.execute(init.gpa, init.io, &snapshot, &admission, .{
        .io = init.io,
        .ptr = worker,
        .check_fn = checkMerge,
        .deadline_ns = worker.softDeadline(),
        .hard_cancellation = watchdog.boundary(),
    });
    var buffer: [8192]u8 = undefined;
    var output = std.Io.File.stdout().writerStreaming(init.io, &buffer);
    try std.json.Stringify.value(.{ .event = "result", .result = result, .output_dir = snapshot.parsed.value.output_dir }, .{}, &output.interface);
    try output.interface.writeByte('\n');
    try output.interface.flush();
}

fn help() void {
    std.debug.print(
        \\usage: antfly-inference finetune adapter materialize gliner25 <job.json> [--shutdown-grace-seconds N]
        \\The version-1 job requires exact original FP32 source and adapter file identities.
        \\It streams a new FP32 model directory, preserving every untouched tensor, bias and sidecar.
        \\Source, adapter import and scratch reservations remain subject to process admission.
        \\The output directory must be new. Failed or cancelled work removes private staging.
        \\A complete directory published before a final sync failure is retained and reported explicitly.
        \\SIGINT/SIGTERM cancels the merge; a second signal or shutdown deadline forces termination.
        \\Grace defaults to 30 seconds (maximum 300), timeout to 30 minutes (maximum 24 hours).
        \\Timeout exits 124; parent signal interruption exits 128 plus the signal number.
        \\No automatic retry, overwrite, quality approval or quantization is performed.
        \\
    , .{});
}
fn usage() error{InvalidArguments} {
    help();
    return error.InvalidArguments;
}

test "boundary merge CLI binds exact config digest grace and rejects ambiguous options" {
    try std.testing.expectEqual(@as(u32, 30), (try parseOptions(&.{"/tmp/job.json"})).grace_seconds);
    try std.testing.expectEqual(@as(u32, 300), (try parseOptions(&.{ "/tmp/job.json", "--shutdown-grace-seconds", "300" })).grace_seconds);
    for ([_][]const []const u8{ &.{}, &.{ "/tmp/job", "--stop-after-microbatches", "1" }, &.{ "/tmp/job", "--shutdown-grace-seconds", "0" }, &.{ "/tmp/job", "--shutdown-grace-seconds", "301" }, &.{ "/tmp/job", "--shutdown-grace-seconds", "-1" } }) |tail|
        try std.testing.expectError(error.InvalidArguments, parseOptions(tail));
    const digest = internal.models.gliner_boundary_bundle.Digest.of("exact raw merge config");
    var changed = digest;
    changed.sha256[0] ^= 1;
    try std.testing.expect(!std.meta.eql(fingerprint(digest, 30), fingerprint(changed, 30)));
    changed = digest;
    changed.size_bytes += 1;
    try std.testing.expect(!std.meta.eql(fingerprint(digest, 30), fingerprint(changed, 30)));
    try std.testing.expect(!std.meta.eql(fingerprint(digest, 30), fingerprint(digest, 31)));
}
