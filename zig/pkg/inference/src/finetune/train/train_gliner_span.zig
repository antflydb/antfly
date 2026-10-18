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
const job = internal.finetune.gliner_span_job;
const process = internal.platform.one_shot_process;
pub fn main(init: std.process.Init) !void {
    return mainWithOriginal(init, init.minimal.args);
}
pub fn mainWithOriginal(init: std.process.Init, original: std.process.Args) !void {
    if (try command(job, init, original)) |outcome| process.finishParent(outcome);
}
pub fn evalMain(init: std.process.Init) !void {
    return evalMainWithOriginal(init, init.minimal.args);
}
pub fn evalMainWithOriginal(init: std.process.Init, original: std.process.Args) !void {
    if (try command(internal.finetune.gliner_span_eval, init, original)) |outcome| process.finishParent(outcome);
}
fn command(comptime task: type, init: std.process.Init, original: std.process.Args) !?process.Outcome {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const name = args.next() orelse return error.MissingArgument;
    if (std.mem.eql(u8, name, "--help")) {
        if (task == job) std.debug.print("usage: antfly inference finetune train gliner25-span <job.json>\nFrozen-encoder ModernBERT supervised classifier/count/span projection training.\nUses Antenna's version-1 training rows and shared transactional optimizer.\n", .{}) else std.debug.print("usage: antfly inference finetune eval gliner25-span <evaluation.json>\nOffline exact decoded entity/relation evaluation; no production capability promotion.\n", .{});
        return null;
    }
    if (args.next() != null) return error.InvalidArguments;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, name, init.gpa, .limited(1024 * 1024));
    defer init.gpa.free(bytes);
    const parsed = try std.json.parseFromSlice(task.Config, init.gpa, bytes, .{});
    defer parsed.deinit();
    var fingerprint: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &fingerprint, .{});
    if (try process.workerRequested(init.environ_map)) {
        const worker = try process.Worker.create(init.gpa, init.io, init.environ_map);
        worker.verifyFingerprint(fingerprint) catch |err| {
            std.debug.print("GLiNER span config changed: {s}\n", .{@errorName(err)});
            worker.finish(1);
        };
        const watchdog = try internal.HardCancellationWatchdog.create(init.gpa);
        defer watchdog.destroy();
        try watchdog.start(init.io);
        const control = internal.InferenceExecutionControl{ .io = init.io, .ptr = worker, .check_fn = process.Worker.checkHard, .deadline_ns = worker.softDeadline(), .hard_cancellation = watchdog.boundary() };
        var guard = try control.enterUninterruptible(.uninterruptible);
        defer guard.deinit();
        task.execute(init.gpa, init.io, parsed.value, control) catch |err| {
            std.debug.print("GLiNER span training failed: {s}\n", .{@errorName(err)});
            worker.finish(if (err == error.Timeout) process.timeout_exit_code else 1);
        };
        worker.finish(0);
    }
    if (parsed.value.timeout_seconds == 0 or parsed.value.timeout_seconds > 7 * 24 * 60 * 60) return error.InvalidSpanTrainingConfig;
    var invocation = try process.originalArguments(init.gpa, original, init.environ_map, &.{name});
    defer invocation.deinit();
    return try process.runParent(init, invocation.values, .{
        .timeout_ns = @as(u64, parsed.value.timeout_seconds) * std.time.ns_per_s,
        .shutdown_grace_ns = 30 * std.time.ns_per_s,
        .fingerprint = fingerprint,
    });
}
