// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One disposable command worker with exclusive output/resource ownership.
//! Server restart policy remains in inference_process_supervisor.zig. Commands
//! never restart automatically: only their last durable checkpoint survives a
//! hard failure. The parent is the sole process waiter and signal sender.
const std = @import("std");
const builtin = @import("builtin");
const time = @import("time.zig");
const exitImmediately = @import("process.zig").exitImmediately;

pub const original_argv_env = "ANTFLY_TRAINING_ORIGINAL_ARGV_V1";
pub const worker_env = "ANTFLY_TRAINING_COMMAND_WORKER_V1";
pub const contract_env = "ANTFLY_TRAINING_COMMAND_CONTRACT_V1";
pub const fatal_exit_code: u8 = 86;
pub const timeout_exit_code: u8 = 124;
pub const max_arguments: usize = 128;
pub const max_argument_bytes: usize = 64 * 1024;

fn validateArguments(arguments: []const []const u8) !void {
    if (arguments.len == 0 or arguments.len > max_arguments) return error.TrainingInvocationLimitExceeded;
    var bytes: usize = 0;
    for (arguments) |argument| {
        if (std.mem.indexOfScalar(u8, argument, 0) != null) return error.InvalidTrainingInvocation;
        bytes = std.math.add(usize, bytes, argument.len + 1) catch return error.TrainingInvocationLimitExceeded;
        if (bytes > max_argument_bytes) return error.TrainingInvocationLimitExceeded;
    }
    if (arguments[0].len == 0) return error.InvalidTrainingInvocation;
}

/// Called only at an executable boundary, before a runtime replaces argv[0]
/// or removes its role prefix. Caller frees the returned bounded JSON bytes.
pub fn encodeOriginalArguments(a: std.mem.Allocator, args: std.process.Args) ![]u8 {
    var iterator = try std.process.Args.Iterator.initAllocator(args, a);
    defer iterator.deinit();
    var arguments = std.ArrayListUnmanaged([]const u8).empty;
    defer arguments.deinit(a);
    var bytes: usize = 0;
    while (iterator.next()) |argument| {
        if (arguments.items.len >= max_arguments) return error.TrainingInvocationLimitExceeded;
        bytes = std.math.add(usize, bytes, argument.len + 1) catch return error.TrainingInvocationLimitExceeded;
        if (bytes > max_argument_bytes) return error.TrainingInvocationLimitExceeded;
        try arguments.append(a, argument);
    }
    try validateArguments(arguments.items);
    const encoded = try std.json.Stringify.valueAlloc(a, arguments.items, .{});
    errdefer a.free(encoded);
    if (encoded.len > max_argument_bytes) return error.TrainingInvocationLimitExceeded;
    return encoded;
}

/// Match only supervised GLiNER2.5 training and adapter-materialization routes.
/// Keep the existing name for runtime ABI callers; unrelated invocations never
/// receive an original-argv carrier.
pub fn isTrainingInvocation(args: std.process.Args) bool {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return false;
    var iterator = std.process.Args.Iterator.init(args);
    _ = iterator.next() orelse return false;
    var argument = iterator.next() orelse return false;
    if (std.mem.eql(u8, argument, "inference")) argument = iterator.next() orelse return false;
    if (!std.mem.eql(u8, argument, "finetune")) return false;
    const domain = iterator.next() orelse return false;
    if (std.mem.eql(u8, domain, "adapter")) {
        if (!std.mem.eql(u8, iterator.next() orelse return false, "materialize")) return false;
        return std.mem.eql(u8, iterator.next() orelse return false, "gliner25");
    }
    if (!std.mem.eql(u8, domain, "train")) return false;
    argument = iterator.next() orelse return false;
    if (std.mem.eql(u8, argument, "run")) argument = iterator.next() orelse return false;
    return std.mem.eql(u8, argument, "gliner25");
}

pub const Arguments = struct {
    allocator: std.mem.Allocator,
    values: [][]u8,
    pub fn deinit(self: *Arguments) void {
        for (self.values) |value| self.allocator.free(value);
        self.allocator.free(self.values);
    }
};

fn copyArguments(a: std.mem.Allocator, values: []const []const u8) !Arguments {
    try validateArguments(values);
    const result = try a.alloc([]u8, values.len);
    errdefer a.free(result);
    var initialized: usize = 0;
    errdefer for (result[0..initialized]) |value| a.free(value);
    for (values, result) |value, *out| {
        out.* = try a.dupe(u8, value);
        initialized += 1;
    }
    return .{ .allocator = a, .values = result };
}

fn validateCarrier(bytes: []const u8) !void {
    if (bytes.len == 0 or bytes.len > max_argument_bytes) return error.TrainingInvocationLimitExceeded;
    var quoted = false;
    var escaped = false;
    var strings: usize = 0;
    var depth: usize = 0;
    for (bytes) |byte| {
        if (quoted) {
            if (escaped) escaped = false else if (byte == '\\') escaped = true else if (byte == '"') quoted = false;
        } else switch (byte) {
            '"' => {
                quoted = true;
                strings += 1;
                if (strings > max_arguments) return error.TrainingInvocationLimitExceeded;
            },
            '[' => {
                if (depth != 0) return error.InvalidTrainingInvocation;
                depth = 1;
            },
            ']' => {
                if (depth != 1) return error.InvalidTrainingInvocation;
                depth = 0;
            },
            '{', '}' => return error.InvalidTrainingInvocation,
            else => {},
        }
    }
}

pub fn matchesTrainingTail(arguments: []const []const u8, tail: []const []const u8) bool {
    return invocationKind(arguments, tail) != null;
}

const InvocationKind = enum { standalone, training, merge };
fn invocationKind(arguments: []const []const u8, tail: []const []const u8) ?InvocationKind {
    if (tail.len == 0 or arguments.len <= tail.len) return null;
    const prefix = arguments.len - tail.len;
    for (arguments[prefix..], tail) |actual, expected| if (!std.mem.eql(u8, actual, expected)) return null;
    if (prefix == 1) return .standalone;
    var index: usize = 1;
    if (std.mem.eql(u8, arguments[index], "inference")) index += 1;
    if (index + 3 > prefix or !std.mem.eql(u8, arguments[index], "finetune")) return null;
    if (std.mem.eql(u8, arguments[index + 1], "adapter")) {
        if (index + 4 != prefix or !std.mem.eql(u8, arguments[index + 2], "materialize") or !std.mem.eql(u8, arguments[index + 3], "gliner25")) return null;
        return .merge;
    }
    if (!std.mem.eql(u8, arguments[index + 1], "train")) return null;
    index += 2;
    if (std.mem.eql(u8, arguments[index], "run")) index += 1;
    return if (index + 1 == prefix and std.mem.eql(u8, arguments[index], "gliner25")) .training else null;
}

/// Recover the real executable invocation, and prove its complete command
/// tail matches the current adapted parser input. A runtime's synthetic argv
/// cannot be guessed from an executable basename. The outer executable
/// overwrites the carrier before entering the runtime ABI.
pub fn originalArguments(a: std.mem.Allocator, args: std.process.Args, environment: *const std.process.Environ.Map, tail: []const []const u8) !Arguments {
    var iterator = try std.process.Args.Iterator.initAllocator(args, a);
    defer iterator.deinit();
    const first = iterator.next() orelse return error.InvalidTrainingInvocation;
    if (std.mem.eql(u8, first, "antfly-runtime")) {
        const bytes = environment.get(original_argv_env) orelse return error.MissingOriginalTrainingInvocation;
        try validateCarrier(bytes);
        const parsed = try std.json.parseFromSlice([]const []const u8, a, bytes, .{ .allocate = .alloc_always });
        defer parsed.deinit();
        try validateArguments(parsed.value);
        if (std.mem.eql(u8, parsed.value[0], "antfly-runtime") or !matchesTrainingTail(parsed.value, tail)) return error.InvalidTrainingInvocation;
        var current: [max_arguments][]const u8 = undefined;
        current[0] = first;
        var count: usize = 1;
        while (iterator.next()) |argument| {
            if (count == current.len) return error.TrainingInvocationLimitExceeded;
            current[count] = argument;
            count += 1;
        }
        // A valid suffix alone must not redirect a merge worker into a
        // training command (or vice versa) across the runtime ABI carrier.
        if (invocationKind(current[0..count], tail) == null or invocationKind(current[0..count], tail) != invocationKind(parsed.value, tail)) return error.InvalidTrainingInvocation;
        return copyArguments(a, parsed.value);
    }
    var values = std.ArrayListUnmanaged([]const u8).empty;
    defer values.deinit(a);
    try values.append(a, first);
    while (iterator.next()) |value| {
        if (values.items.len >= max_arguments) return error.TrainingInvocationLimitExceeded;
        try values.append(a, value);
    }
    if (!matchesTrainingTail(values.items, tail)) return error.InvalidTrainingInvocation;
    return copyArguments(a, values.items);
}

pub const Options = struct {
    timeout_ns: u64,
    shutdown_grace_ns: u64 = 30 * std.time.ns_per_s,
    fingerprint: [32]u8,
};
const Contract = struct {
    version: u32 = 1,
    soft_deadline_ns: u64,
    hard_deadline_ns: u64,
    shutdown_grace_ns: u64,
    fingerprint: [32]u8,
};
pub const Outcome = struct {
    term: std.process.Child.Term,
    interrupted: ?std.posix.SIG = null,
    timed_out: bool = false,
    forced: bool = false,
    pub fn exitCode(self: Outcome) u8 {
        if (self.interrupted) |signal| return @intCast(@min(255, 128 + @as(u32, @intFromEnum(signal))));
        if (self.timed_out) return timeout_exit_code;
        return switch (self.term) {
            .exited => |code| code,
            .signal, .stopped => |signal| @intCast(@min(255, 128 + @as(u32, @intFromEnum(signal)))),
            .unknown => 1,
        };
    }
};

/// Executable-only completion, after runParent reaps its child and the
/// caller drains command-owned allocations. A completed one-shot command
/// must not enter unmonitored C exit handlers or outer runtime IO teardown.
/// Library callers can instead retain runParent's ordinary returning API.
pub fn finishParent(outcome: Outcome) noreturn {
    exitImmediately(outcome.exitCode());
}

fn supported() bool {
    return builtin.os.tag == .macos or builtin.os.tag == .linux;
}
fn makeContract(options: Options, now: u64) !Contract {
    if (options.timeout_ns == 0 or options.timeout_ns > 7 * 24 * 60 * 60 * std.time.ns_per_s or
        options.shutdown_grace_ns == 0 or options.shutdown_grace_ns > 300 * std.time.ns_per_s)
        return error.InvalidTrainingWorkerLimits;
    const soft = std.math.add(u64, now, options.timeout_ns) catch return error.InvalidTrainingWorkerLimits;
    return .{ .soft_deadline_ns = soft, .hard_deadline_ns = std.math.add(u64, soft, options.shutdown_grace_ns) catch return error.InvalidTrainingWorkerLimits, .shutdown_grace_ns = options.shutdown_grace_ns, .fingerprint = options.fingerprint };
}

fn completedOutcome(term: std.process.Child.Term, interrupted: ?std.posix.SIG, contract: Contract, now: u64) Outcome {
    // A cooperative worker reports the reserved timeout status. A watchdog
    // may terminate it at the soft deadline while it is inside a declared
    // uninterruptible call; that same deadline has the same public status.
    const timeout = switch (term) {
        .exited => |code| code == timeout_exit_code or (code == fatal_exit_code and now >= contract.soft_deadline_ns),
        else => false,
    };
    return .{ .term = term, .interrupted = interrupted, .timed_out = timeout };
}

pub fn workerRequested(environment: *const std.process.Environ.Map) !bool {
    const marker = environment.get(worker_env);
    const contract = environment.get(contract_env);
    if (marker == null and contract == null) return false;
    if (marker == null or contract == null or !std.mem.eql(u8, marker.?, "1")) return error.InvalidTrainingWorkerEnvironment;
    return true;
}

var signals_in_use: std.atomic.Value(bool) = .init(false);
var signal_count: std.atomic.Value(u32) = .init(0);
var first_signal: std.atomic.Value(u32) = .init(0);
fn onSignal(signal: std.posix.SIG) callconv(.c) void {
    // No allocation, logging, IO, checkpointing, or teardown in signal context.
    _ = first_signal.cmpxchgStrong(0, @intFromEnum(signal), .release, .monotonic);
    if (signal_count.load(.monotonic) < 2) _ = signal_count.fetchAdd(1, .release);
}
const Signals = struct {
    old_int: std.posix.Sigaction,
    old_term: std.posix.Sigaction,
    old_pipe: std.posix.Sigaction,
    fn install() !Signals {
        if (comptime !supported()) return error.UnsupportedTrainingWorkerPlatform;
        if (signals_in_use.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return error.TrainingSupervisorAlreadyActive;
        signal_count.store(0, .release);
        first_signal.store(0, .release);
        const action = std.posix.Sigaction{ .handler = .{ .handler = onSignal }, .mask = std.posix.sigemptyset(), .flags = 0 };
        const ignored = std.posix.Sigaction{ .handler = .{ .handler = std.posix.SIG.IGN }, .mask = std.posix.sigemptyset(), .flags = 0 };
        var self: Signals = undefined;
        std.posix.sigaction(.INT, &action, &self.old_int);
        std.posix.sigaction(.TERM, &action, &self.old_term);
        std.posix.sigaction(.PIPE, &ignored, &self.old_pipe);
        return self;
    }
    fn deinit(self: *Signals) void {
        std.posix.sigaction(.INT, &self.old_int, null);
        std.posix.sigaction(.TERM, &self.old_term, null);
        std.posix.sigaction(.PIPE, &self.old_pipe, null);
        signals_in_use.store(false, .release);
    }
};

test "one-shot training routes preserve public invocation boundaries" {
    if (comptime !supported()) return error.SkipZigTest;
    const Case = struct { argv: []const [*:0]const u8, expected: bool };
    const cases = [_]Case{
        .{ .argv = &.{ "antfly", "inference", "finetune", "train", "gliner25", "/tmp/job.json" }, .expected = true },
        .{ .argv = &.{ "antfly-inference", "finetune", "train", "run", "gliner25", "/tmp/job.json" }, .expected = true },
        .{ .argv = &.{ "antfly", "finetune", "train", "gliner25", "--help" }, .expected = true },
        .{ .argv = &.{ "antfly", "inference", "finetune", "adapter", "materialize", "gliner25", "/tmp/job.json" }, .expected = true },
        .{ .argv = &.{ "antfly-inference", "finetune", "adapter", "materialize", "gliner25", "--help" }, .expected = true },
        .{ .argv = &.{ "antfly-inference", "finetune", "adapter", "inspect", "gliner25", "--help" }, .expected = false },
        .{ .argv = &.{ "train-gliner25", "/tmp/job.json" }, .expected = false },
        .{ .argv = &.{ "antfly", "inference", "run" }, .expected = false },
        .{ .argv = &.{ "antfly", "inference", "finetune", "train", "gliner2" }, .expected = false },
        .{ .argv = &.{ "antfly", "inference", "finetune", "train", "run" }, .expected = false },
        .{ .argv = &.{ "antfly", "inference", "finetune", "eval", "gliner25" }, .expected = false },
        .{ .argv = &.{ "antfly", "--help" }, .expected = false },
    };
    for (cases) |case| try std.testing.expectEqual(case.expected, isTrainingInvocation(.{ .vector = case.argv }));
}

fn exerciseOriginalArguments(a: std.mem.Allocator) !void {
    if (comptime !supported()) return error.SkipZigTest;
    const original = std.process.Args{ .vector = &.{ "/tmp/antfly executable", "inference", "finetune", "train", "run", "gliner25", "/tmp/α job \"one\".json", "--stop-after-microbatches", "2" } };
    const synthetic = std.process.Args{ .vector = &.{ "antfly-runtime", "finetune", "train", "run", "gliner25", "/tmp/α job \"one\".json", "--stop-after-microbatches", "2" } };
    const tail = [_][]const u8{ "/tmp/α job \"one\".json", "--stop-after-microbatches", "2" };
    const encoded = try encodeOriginalArguments(a, original);
    defer a.free(encoded);
    var environment = std.process.Environ.Map.init(a);
    defer environment.deinit();
    try environment.put(original_argv_env, encoded);
    var recovered = try originalArguments(a, synthetic, &environment, &tail);
    defer recovered.deinit();
    try std.testing.expectEqual(original.vector.len, recovered.values.len);
    for (original.vector, recovered.values) |expected, actual| try std.testing.expectEqualStrings(std.mem.span(expected), actual);
    var direct = try originalArguments(a, original, &environment, &tail);
    defer direct.deinit();
    try std.testing.expectEqualStrings(recovered.values[0], direct.values[0]);
}

test "one-shot original argv roundtrip owns every allocation" {
    try exerciseOriginalArguments(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseOriginalArguments, .{});
}

fn exerciseMergeArguments(a: std.mem.Allocator) !void {
    if (comptime !supported()) return error.SkipZigTest;
    const original = std.process.Args{ .vector = &.{ "/tmp/antfly executable", "inference", "finetune", "adapter", "materialize", "gliner25", "/tmp/α merge.json", "--shutdown-grace-seconds", "7" } };
    const synthetic = std.process.Args{ .vector = &.{ "antfly-runtime", "finetune", "adapter", "materialize", "gliner25", "/tmp/α merge.json", "--shutdown-grace-seconds", "7" } };
    const tail = [_][]const u8{ "/tmp/α merge.json", "--shutdown-grace-seconds", "7" };
    const encoded = try encodeOriginalArguments(a, original);
    defer a.free(encoded);
    var environment = std.process.Environ.Map.init(a);
    defer environment.deinit();
    try environment.put(original_argv_env, encoded);
    var recovered = try originalArguments(a, synthetic, &environment, &tail);
    defer recovered.deinit();
    for (original.vector, recovered.values) |expected, actual| try std.testing.expectEqualStrings(std.mem.span(expected), actual);
    const training = std.process.Args{ .vector = &.{ "antfly-runtime", "finetune", "train", "gliner25", "/tmp/α merge.json", "--shutdown-grace-seconds", "7" } };
    if (originalArguments(a, training, &environment, &tail)) |accepted| {
        var owned = accepted;
        owned.deinit();
        return error.TestUnexpectedResult;
    } else |err| switch (err) {
        error.InvalidTrainingInvocation => {},
        else => return err,
    }
}

test "one-shot merge original argv binds command kind and cleans allocation failures" {
    try exerciseMergeArguments(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseMergeArguments, .{});
}

test "one-shot carrier rejects missing tampered nested and excessive arguments" {
    if (comptime !supported()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var environment = std.process.Environ.Map.init(a);
    defer environment.deinit();
    const synthetic = std.process.Args{ .vector = &.{ "antfly-runtime", "finetune", "train", "gliner25", "job.json" } };
    try std.testing.expectError(error.MissingOriginalTrainingInvocation, originalArguments(a, synthetic, &environment, &.{"job.json"}));
    try environment.put(original_argv_env, "[\"antfly\",\"inference\",\"finetune\",\"train\",\"gliner25\",\"different.json\"]");
    try std.testing.expectError(error.InvalidTrainingInvocation, originalArguments(a, synthetic, &environment, &.{"job.json"}));
    try environment.put(original_argv_env, "[\"antfly-runtime\",\"job.json\"]");
    try std.testing.expectError(error.InvalidTrainingInvocation, originalArguments(a, synthetic, &environment, &.{"job.json"}));
    try environment.put(original_argv_env, "[[[[]]]]");
    try std.testing.expectError(error.InvalidTrainingInvocation, originalArguments(a, synthetic, &environment, &.{"job.json"}));
    try std.testing.expectError(error.InvalidTrainingInvocation, validateArguments(&.{ "antfly", "bad\x00tail" }));
    try std.testing.expectError(error.TrainingInvocationLimitExceeded, validateArguments(&([_]([]const u8){"x"} ** (max_arguments + 1))));
    const excessive = try a.alloc(u8, max_argument_bytes + 1);
    defer a.free(excessive);
    @memset(excessive, 'x');
    try std.testing.expectError(error.TrainingInvocationLimitExceeded, validateCarrier(excessive));
    try std.testing.expectError(error.TrainingInvocationLimitExceeded, validateArguments(&.{ "antfly", excessive }));
    try std.testing.expect(matchesTrainingTail(&.{ "train-gliner25", "job.json" }, &.{"job.json"}));
    try std.testing.expect(!matchesTrainingTail(&.{ "antfly", "finetune", "train", "run", "job.json" }, &.{"job.json"}));
    try std.testing.expect(!matchesTrainingTail(&.{ "antfly", "inference", "run", "job.json" }, &.{"job.json"}));
    // Direct executable arguments never use a stale inherited carrier.
    var direct = try originalArguments(a, .{ .vector = &.{ "train-gliner25", "job.json" } }, &environment, &.{"job.json"});
    defer direct.deinit();
}

test "one-shot worker markers are paired and strict" {
    var environment = std.process.Environ.Map.init(std.testing.allocator);
    defer environment.deinit();
    try std.testing.expect(!(try workerRequested(&environment)));
    try environment.put(worker_env, "1");
    try std.testing.expectError(error.InvalidTrainingWorkerEnvironment, workerRequested(&environment));
    try environment.put(contract_env, "{}");
    try std.testing.expect(try workerRequested(&environment));
    try environment.put(worker_env, "yes");
    try std.testing.expectError(error.InvalidTrainingWorkerEnvironment, workerRequested(&environment));
    _ = environment.swapRemove(worker_env);
    try std.testing.expectError(error.InvalidTrainingWorkerEnvironment, workerRequested(&environment));
}

test "one-shot deadlines distinguish completion timeout fatal exit and signals" {
    const options = Options{ .timeout_ns = 100, .shutdown_grace_ns = 20, .fingerprint = .{3} ** 32 };
    const contract = try makeContract(options, 1000);
    try std.testing.expectEqual(@as(u64, 1100), contract.soft_deadline_ns);
    try std.testing.expectEqual(@as(u64, 1120), contract.hard_deadline_ns);
    try std.testing.expectError(error.InvalidTrainingWorkerLimits, makeContract(.{ .timeout_ns = 0, .fingerprint = .{0} ** 32 }, 0));
    try std.testing.expectError(error.InvalidTrainingWorkerLimits, makeContract(.{ .timeout_ns = 1, .shutdown_grace_ns = 301 * std.time.ns_per_s, .fingerprint = .{0} ** 32 }, 0));
    try std.testing.expectError(error.InvalidTrainingWorkerLimits, makeContract(options, std.math.maxInt(u64)));
    try std.testing.expectEqual(@as(u8, 0), completedOutcome(.{ .exited = 0 }, null, contract, 1101).exitCode());
    try std.testing.expectEqual(@as(u8, 0), completedOutcome(.{ .exited = 0 }, null, contract, 1121).exitCode());
    try std.testing.expectEqual(@as(u8, 7), completedOutcome(.{ .exited = 7 }, null, contract, 1101).exitCode());
    try std.testing.expectEqual(fatal_exit_code, completedOutcome(.{ .exited = fatal_exit_code }, null, contract, 1099).exitCode());
    try std.testing.expectEqual(timeout_exit_code, completedOutcome(.{ .exited = fatal_exit_code }, null, contract, 1100).exitCode());
    try std.testing.expectEqual(timeout_exit_code, completedOutcome(.{ .exited = timeout_exit_code }, null, contract, 1100).exitCode());
    try std.testing.expectEqual(@as(u8, 130), completedOutcome(.{ .exited = timeout_exit_code }, .INT, contract, 1100).exitCode());
    try std.testing.expectEqual(@as(u8, 143), (Outcome{ .term = .{ .signal = .KILL }, .interrupted = .TERM, .timed_out = true, .forced = true }).exitCode());
}

test "one-shot worker identity and pause do not cancel the soft boundary" {
    const now = time.monotonicNs();
    const contract = try makeContract(.{ .timeout_ns = std.time.ns_per_s, .shutdown_grace_ns = std.time.ns_per_s, .fingerprint = .{9} ** 32 }, now);
    var worker = Worker{ .allocator = std.testing.allocator, .contract = contract, .signals = undefined, .hard_deadline = .init(contract.hard_deadline_ns) };
    try worker.verifyFingerprint(.{9} ** 32);
    try std.testing.expectError(error.TrainingWorkerConfigurationChanged, worker.verifyFingerprint(.{8} ** 32));
    try std.testing.expect(!Worker.pauseRequested(&worker));
    worker.pause();
    try std.testing.expect(Worker.pauseRequested(&worker));
    try std.testing.expectEqual(contract.soft_deadline_ns, worker.softDeadline());
    try std.testing.expect(worker.hard_deadline.load(.acquire) <= contract.hard_deadline_ns);
    try Worker.checkHard(&worker);
    worker.hard_deadline.store(0, .release);
    try std.testing.expectError(error.Timeout, Worker.checkHard(&worker));
}

fn statusToTerm(status: u32) std.process.Child.Term {
    return if (std.posix.W.IFEXITED(status))
        .{ .exited = std.posix.W.EXITSTATUS(status) }
    else if (std.posix.W.IFSIGNALED(status))
        .{ .signal = std.posix.W.TERMSIG(status) }
    else if (std.posix.W.IFSTOPPED(status))
        .{ .stopped = std.posix.W.STOPSIG(status) }
    else
        .{ .unknown = status };
}

fn pollChild(child: *std.process.Child) !?std.process.Child.Term {
    // The parent is the only waiter. A nonblocking wait and any subsequent
    // kill occur on this same thread, so a monitor can never signal a reaped,
    // reused PID. std.Child.kill still owns error/cancellation cleanup.
    var status: if (builtin.link_libc) c_int else u32 = undefined;
    const result = std.posix.system.waitpid(child.id.?, &status, std.posix.W.NOHANG);
    switch (std.posix.errno(result)) {
        .SUCCESS => {
            if (result == 0) return null;
            child.id = null;
            return statusToTerm(@bitCast(status));
        },
        .INTR => return null,
        .CHILD => {
            // Another owner has reaped it; never signal a potentially reused
            // PID while reporting that violated ownership contract.
            child.id = null;
            return error.TrainingWorkerWaitFailed;
        },
        else => return error.TrainingWorkerWaitFailed,
    }
}

fn forceReap(child: *std.process.Child, io: std.Io) void {
    const pid = child.id orelse return;
    // Zig 0.16 Child.kill sends SIGTERM on POSIX. A graceful-signal handler
    // deliberately keeps training alive, so issue SIGKILL before using its
    // uncancelable wait/handle cleanup. This parent is still the sole waiter.
    while (true) switch (std.posix.errno(std.posix.system.kill(pid, .KILL))) {
        .INTR => continue,
        else => break,
    };
    child.kill(io);
}

/// One child, no restart. The stdin pipe remains an owner-loss lifeline just
/// as in the inference supervisor. A single 'P' byte requests a soft pause.
/// POSIX polling keeps the parent independent of an IO executor while it
/// handles signals/deadlines; only it may reap or forcibly terminate the child.
pub fn runParent(init: std.process.Init, arguments: []const []const u8, options: Options) !Outcome {
    if (comptime !supported()) return error.UnsupportedTrainingWorkerPlatform;
    if (try workerRequested(init.environ_map)) return error.NestedTrainingWorker;
    try validateArguments(arguments);
    const contract = try makeContract(options, time.monotonicNs());
    var signals = try Signals.install();
    defer signals.deinit();
    const executable = try std.process.executablePathAlloc(init.io, init.gpa);
    defer init.gpa.free(executable);
    const argv = try init.gpa.dupe([]const u8, arguments);
    defer init.gpa.free(argv);
    argv[0] = executable;
    var environment = try init.environ_map.clone(init.gpa);
    defer environment.deinit();
    // The server supervisor's stdin reader must never compete with this
    // disposable command worker's private lifeline.
    _ = environment.swapRemove("ANTFLY_INFERENCE_SUPERVISED_WORKER");
    _ = environment.swapRemove("ANTFLY_INFERENCE_SUPERVISOR_LIFELINE");
    try environment.put(worker_env, "1");
    const encoded = try std.json.Stringify.valueAlloc(init.gpa, contract, .{});
    defer init.gpa.free(encoded);
    try environment.put(contract_env, encoded);
    var child = try std.process.spawn(init.io, .{ .argv = argv, .environ_map = &environment, .stdin = .pipe, .stdout = .inherit, .stderr = .inherit, .pgid = 0 });
    defer forceReap(&child, init.io);
    // Detach the lifeline handle: childWait/kill must not race its closure.
    const lifeline = child.stdin.?;
    child.stdin = null;
    defer lifeline.close(init.io);
    var interrupted: ?std.posix.SIG = null;
    var signal_deadline: ?u64 = null;
    while (true) {
        try init.io.checkCancel();
        const received = signal_count.load(.acquire);
        if (received != 0 and interrupted == null) {
            interrupted = @enumFromInt(first_signal.load(.acquire));
            signal_deadline = time.monotonicNs() +| contract.shutdown_grace_ns;
            // Only one byte is written, so a worker stalled before its reader
            // starts cannot fill the pipe. A dead child's EPIPE is harmless.
            const byte = [_]u8{'P'};
            while (true) switch (std.posix.errno(std.posix.system.write(lifeline.handle, &byte, 1))) {
                .INTR => continue,
                else => break,
            };
        }
        if (try pollChild(&child)) |term| return completedOutcome(term, interrupted, contract, time.monotonicNs());
        const now = time.monotonicNs();
        const expired = now >= contract.hard_deadline_ns;
        const forced = received > 1 or (if (signal_deadline) |deadline| now >= deadline else false);
        if (expired or forced) {
            forceReap(&child, init.io);
            return .{ .term = .{ .signal = .KILL }, .interrupted = interrupted, .timed_out = expired, .forced = true };
        }
        time.sleepNs(10 * std.time.ns_per_ms);
    }
}

/// Process-lifetime monitor. It uses a dedicated OS thread and a nonblocking
/// pipe poll so it survives native driver stalls and IO/runtime teardown.
/// Parent loss, malformed control data, and hard expiry never run destructors
/// over buffers that might still be retained by the driver.
pub const Worker = struct {
    allocator: std.mem.Allocator,
    contract: Contract,
    signals: Signals,
    thread: ?std.Thread = null,
    stopping: std.atomic.Value(bool) = .init(false),
    paused: std.atomic.Value(bool) = .init(false),
    hard_deadline: std.atomic.Value(u64),

    pub fn create(a: std.mem.Allocator, io: std.Io, environment: *const std.process.Environ.Map) !*Worker {
        if (comptime !supported()) return error.UnsupportedTrainingWorkerPlatform;
        if (!(try workerRequested(environment))) return error.InvalidTrainingWorkerEnvironment;
        const encoded = environment.get(contract_env).?;
        if (encoded.len > 2048) return error.InvalidTrainingWorkerEnvironment;
        const parsed = try std.json.parseFromSlice(Contract, a, encoded, .{});
        defer parsed.deinit();
        const contract = parsed.value;
        if (contract.version != 1 or contract.shutdown_grace_ns == 0 or contract.shutdown_grace_ns > 300 * std.time.ns_per_s or
            contract.hard_deadline_ns <= contract.soft_deadline_ns or contract.hard_deadline_ns - contract.soft_deadline_ns != contract.shutdown_grace_ns or
            contract.hard_deadline_ns -| time.monotonicNs() > (7 * 24 * 60 * 60 + 300) * std.time.ns_per_s)
            return error.InvalidTrainingWorkerEnvironment;
        if ((try std.Io.File.stdin().stat(io)).kind != .named_pipe) return error.InvalidTrainingWorkerLifeline;
        const self = try a.create(Worker);
        errdefer a.destroy(self);
        self.* = .{ .allocator = a, .contract = contract, .signals = try Signals.install(), .hard_deadline = .init(contract.hard_deadline_ns) };
        errdefer self.signals.deinit();
        self.thread = try std.Thread.spawn(.{}, monitor, .{self});
        return self;
    }

    pub fn verifyFingerprint(self: *const Worker, expected: [32]u8) !void {
        if (!std.mem.eql(u8, &self.contract.fingerprint, &expected)) return error.TrainingWorkerConfigurationChanged;
    }
    pub fn softDeadline(self: *const Worker) u64 {
        return self.contract.soft_deadline_ns;
    }
    pub fn pauseRequested(raw: ?*const anyopaque) bool {
        const self: *const Worker = @ptrCast(@alignCast(raw orelse return false));
        return self.paused.load(.acquire);
    }
    pub fn checkHard(raw: ?*anyopaque) !void {
        const self: *const Worker = @ptrCast(@alignCast(raw orelse return error.InvalidTrainingWorkerEnvironment));
        if (time.monotonicNs() >= self.hard_deadline.load(.acquire)) return error.Timeout;
    }

    /// Test/embedding teardown, after all controlled resources are destroyed.
    /// CLI workers use finish so the monitor remains live through process exit.
    pub fn destroy(self: *Worker) void {
        self.stopping.store(true, .release);
        if (self.thread) |thread| thread.join();
        self.signals.deinit();
        self.allocator.destroy(self);
    }
    pub fn finish(_: *Worker, code: u8) noreturn {
        exitImmediately(code);
    }
    fn pause(self: *Worker) void {
        if (self.paused.cmpxchgStrong(false, true, .acq_rel, .acquire) == null) {
            const deadline = @min(self.contract.hard_deadline_ns, time.monotonicNs() +| self.contract.shutdown_grace_ns);
            self.hard_deadline.store(deadline, .release);
        }
    }
    fn monitor(self: *Worker) void {
        while (!self.stopping.load(.acquire)) {
            const received = signal_count.load(.acquire);
            if (received != 0) self.pause();
            if (received > 1 or time.monotonicNs() >= self.hard_deadline.load(.acquire)) exitImmediately(fatal_exit_code);
            var descriptors = [_]std.posix.pollfd{.{ .fd = std.Io.File.stdin().handle, .events = std.posix.POLL.IN, .revents = 0 }};
            const ready = std.posix.poll(&descriptors, 10) catch exitImmediately(1);
            if (ready == 0) continue;
            var byte: [1]u8 = undefined;
            const count = std.posix.system.read(descriptors[0].fd, &byte, 1);
            switch (std.posix.errno(count)) {
                .SUCCESS => {
                    if (count == 0) exitImmediately(1);
                    if (byte[0] != 'P') exitImmediately(1);
                    self.pause();
                },
                .INTR => continue,
                else => exitImmediately(1),
            }
        }
    }
};
