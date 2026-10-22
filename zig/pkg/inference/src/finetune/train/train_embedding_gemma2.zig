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

const std = @import("std");
const internal = @import("inference_internal");
const runner = internal.finetune.embedding_gemma2_runner;
const process = internal.platform.one_shot_process;

pub fn main(init: std.process.Init) !void {
    return mainWithOriginal(init, init.minimal.args);
}
pub fn mainWithOriginal(init: std.process.Init, original: std.process.Args) !void {
    if (try command(init, original)) |outcome| process.finishParent(outcome);
}

fn command(init: std.process.Init, original: std.process.Args) !?process.Outcome {
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
    var options = runner.Options{};
    var grace: u32 = 30;
    while (args.next()) |arg| {
        try tail.append(init.gpa, arg);
        if (std.mem.eql(u8, arg, "--dry-run")) {
            options.dry_run = true;
            continue;
        }
        const value = args.next() orelse return usage();
        try tail.append(init.gpa, value);
        if (std.mem.eql(u8, arg, "--resume-from") and options.resume_from == null) options.resume_from = value else if (std.mem.eql(u8, arg, "--stop-after-steps") and options.stop_after_steps == null) options.stop_after_steps = try std.fmt.parseUnsigned(u64, value, 10) else if (std.mem.eql(u8, arg, "--shutdown-grace-seconds")) {
            grace = try std.fmt.parseUnsigned(u32, value, 10);
            if (grace == 0 or grace > 300) return usage();
        } else return usage();
    }
    const raw = try runner.readFile(init.gpa, init.io, config_path, 64 * 1024);
    defer init.gpa.free(raw);
    const config = try runner.loadConfig(init.gpa, init.io, config_path);
    defer config.deinit();
    if (options.dry_run) {
        _ = try runner.run(init.gpa, init.io, config.value, options, null);
        return null;
    }
    if (!@import("build_options").enable_cuda) return error.CudaTrainingBuildRequired;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(raw);
    for (tail.items) |arg| {
        hash.update("\x00");
        hash.update(arg);
    }
    const fingerprint = hash.finalResult();
    if (try process.workerRequested(init.environ_map)) {
        const worker = try process.Worker.create(init.gpa, init.io, init.environ_map);
        workerMain(init, config.value, options, fingerprint, worker) catch |err| {
            std.debug.print("EmbeddingGemma 2 training failed: {s}\n", .{@errorName(err)});
            worker.finish(if (err == error.Timeout) process.timeout_exit_code else 1);
        };
        worker.finish(0);
    }
    var invocation = try process.originalArguments(init.gpa, original, init.environ_map, tail.items);
    defer invocation.deinit();
    return process.runParent(init, invocation.values, .{ .timeout_ns = @as(u64, config.value.timeout_seconds) * std.time.ns_per_s, .shutdown_grace_ns = @as(u64, grace) * std.time.ns_per_s, .fingerprint = fingerprint }) catch |err| {
        std.debug.print("EmbeddingGemma 2 training supervisor failed: {s}\n", .{@errorName(err)});
        return process.Outcome{ .term = .{ .exited = 1 } };
    };
}

fn workerMain(init: std.process.Init, config: internal.finetune.embedding_gemma2_job.Config, options: runner.Options, fingerprint: [32]u8, worker: *process.Worker) !void {
    const watchdog = try internal.HardCancellationWatchdog.create(init.gpa);
    defer watchdog.destroy();
    try watchdog.start(init.io);
    var guard = try (internal.InferenceExecutionControl{ .io = init.io, .ptr = worker, .check_fn = process.Worker.checkHard, .hard_cancellation = watchdog.boundary() }).enterUninterruptible(.uninterruptible);
    defer guard.deinit();
    try worker.verifyFingerprint(fingerprint);
    var active = options;
    active.pause_context = worker;
    active.pause_requested = process.Worker.pauseRequested;
    const result = try runner.run(init.gpa, init.io, config, active, .{ .io = init.io, .deadline_ns = worker.softDeadline(), .hard_cancellation = watchdog.boundary() });
    try runner.event(init.io, .{ .event = "result", .result = result, .output_dir = config.output_dir });
}

fn help() void {
    std.debug.print(
        \\usage: train-embeddinggemma2 <job.json> [--dry-run] [--resume-from checkpoint_dir]
        \\       [--stop-after-steps N] [--shutdown-grace-seconds 30]
        \\Native resident CUDA LoRA; absolute model/prepared/output paths; output must be new.
        \\SIGINT/SIGTERM checkpoints at a completed logical batch; a second signal forces termination.
        \\Every checkpoint contains FP32 adapter masters, Adam moments, and exact dataset/sampler identity.
        \\Export any durable checkpoint with export-embeddinggemma2-lora.
        \\Training quality and speed require separate qualification; job completion does not certify them.
        \\
    , .{});
}
fn usage() error{InvalidArguments} {
    help();
    return error.InvalidArguments;
}
