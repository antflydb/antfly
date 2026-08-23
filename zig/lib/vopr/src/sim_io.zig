// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Fail-closed deterministic `std.Io` boundary for VOPR.
//!
//! This is the capability shell underneath the staged SimIo implementation.
//! It intentionally advertises only behavior implemented here. Unsupported
//! capabilities are rejected at construction, and the vtable is cloned from
//! `std.Io.failing` rather than from a host backend. Selected operations whose
//! standard failing implementation would be infallible, a no-op, or
//! unreachable are replaced with deterministic handlers that latch a harness
//! violation instead.

const builtin = @import("builtin");
const std = @import("std");
const event = @import("event.zig");
const ids = @import("id.zig");
const runtime_mod = @import("runtime.zig");
const task_mod = @import("sim_io_task.zig");
const transition = @import("transition.zig");

pub const model_version: u32 = 1;

pub const Capability = enum(u6) {
    eager_async,
    clock_read,
    deterministic_entropy,
    task_scheduling,
    synchronization,
    sleep,
    files,
    sockets,
    processes,
    resources,
    instrumentation,
};

pub const CapabilitySet = struct {
    bits: u64 = 0,

    pub fn of(capabilities: []const Capability) CapabilitySet {
        var result: CapabilitySet = .{};
        for (capabilities) |capability| result.insert(capability);
        return result;
    }

    pub fn insert(self: *CapabilitySet, capability: Capability) void {
        self.bits |= bit(capability);
    }

    pub fn contains(self: CapabilitySet, capability: Capability) bool {
        return self.bits & bit(capability) != 0;
    }

    pub fn containsAll(self: CapabilitySet, required: CapabilitySet) bool {
        return self.bits & required.bits == required.bits;
    }

    pub fn missing(self: CapabilitySet, required: CapabilitySet) CapabilitySet {
        return .{ .bits = required.bits & ~self.bits };
    }

    fn bit(capability: Capability) u64 {
        return @as(u64, 1) << @intFromEnum(capability);
    }
};

/// Capabilities whose semantics are implemented by this file. Adding a bit is
/// an API promise and therefore requires conformance tests on both SimIo and
/// `std.Io.Threaded`.
pub const supported_capabilities = CapabilitySet.of(&.{
    .eager_async,
    .clock_read,
    .deterministic_entropy,
    .task_scheduling,
    .synchronization,
    .sleep,
});

pub const Metadata = struct {
    zig_version: []const u8,
    capability_digest: u64,
    virtual_os_model_version: u32,
    instrumentation_map_digest: u64,
};

pub fn metadata() Metadata {
    return .{
        .zig_version = builtin.zig_version_string,
        .capability_digest = capabilityDigest(supported_capabilities),
        .virtual_os_model_version = model_version,
        .instrumentation_map_digest = 0,
    };
}

pub fn capabilityDigest(capabilities: CapabilitySet) u64 {
    return ids.derive(
        "sim-io-capabilities-v1",
        ids.digest(builtin.zig_version_string),
        capabilities.bits,
    );
}

/// Canonical backend identities to place in `trace.Config.backend_ids` for
/// every SimIo-backed scenario. This uses the existing trace-v1 extension
/// surface, so toolchain and virtual-OS compatibility are replay inputs without
/// coupling the stable trace schema to Zig's evolving public vtable layout.
pub fn artifactBackendIds() [4]ids.StableId {
    var result = [4]ids.StableId{
        ids.stable("sim-io-zig", builtin.zig_version_string),
        capabilityDigest(supported_capabilities),
        ids.derive("sim-io-model", ids.stable("sim-io", "virtual-os"), model_version),
        ids.derive("sim-io-instrumentation", ids.stable("sim-io", "safepoints"), 0),
    };
    ids.sort(&result);
    return result;
}

pub fn validateArtifactBackendIds(actual: []const ids.StableId) !void {
    const required = artifactBackendIds();
    var required_index: usize = 0;
    for (actual) |candidate| {
        if (required_index == required.len) break;
        if (candidate == required[required_index]) required_index += 1;
    }
    if (required_index != required.len) return error.IncompatibleSimIoBackend;
}

pub const ViolationOperation = enum {
    crash_handler,
    future_await,
    future_cancel,
    group_await,
    group_cancel,
    futex_wait,
    futex_wait_uncancelable,
    futex_wake,
    operation_batch,
    directory_access,
    directory_close,
    directory_read,
    directory_set_timestamps,
    file_close,
    file_is_tty,
    file_enable_ansi,
    file_supports_ansi,
    file_set_timestamps,
    file_unlock,
    memory_map_destroy,
    memory_map_set_length,
    memory_map_read,
    memory_map_write,
    stderr_lock,
    stderr_unlock,
    child_wait,
    child_kill,
    sleep,
    network_send,
    network_close,
    network_interface_name,
};

pub const CapabilityViolation = struct {
    operation: ViolationOperation,
    sequence: u64,
};

pub const Config = struct {
    required: CapabilitySet = .{},
    seed: u64 = 0,
    monotonic_ns: i64 = 0,
    realtime_ns: i64 = 0,
    process_cpu_ns: i64 = 0,
    thread_cpu_ns: i64 = 0,
    task_allocator: std.mem.Allocator = std.heap.page_allocator,
    tasks: task_mod.Config = .{},
};

pub const InitError = error{
    UnsupportedSimIoCapabilities,
    SimIoStackTooSmall,
    InvalidSimIoTaskLimit,
};

pub const SimIo = struct {
    prng: std.Random.DefaultPrng,
    monotonic_ns: i96,
    realtime_ns: i96,
    process_cpu_ns: i96,
    thread_cpu_ns: i96,
    cancel_requested: bool = false,
    cancel_protection: std.Io.CancelProtection = .unblocked,
    random_calls: u64 = 0,
    violation_count: u64 = 0,
    first_violation: ?CapabilityViolation = null,
    tasks: task_mod.Kernel,

    pub fn init(config: Config) InitError!SimIo {
        if (!supported_capabilities.containsAll(config.required)) {
            return error.UnsupportedSimIoCapabilities;
        }
        return .{
            .prng = std.Random.DefaultPrng.init(config.seed),
            .monotonic_ns = config.monotonic_ns,
            .realtime_ns = config.realtime_ns,
            .process_cpu_ns = config.process_cpu_ns,
            .thread_cpu_ns = config.thread_cpu_ns,
            .tasks = try task_mod.Kernel.init(config.task_allocator, config.tasks),
        };
    }

    pub fn deinit(self: *SimIo) void {
        self.tasks.deinit();
        self.* = undefined;
    }

    pub fn io(self: *SimIo) std.Io {
        return .{ .userdata = self, .vtable = &vtable };
    }

    pub fn scheduler(self: *SimIo) runtime_mod.SchedulerPort {
        return .{ .ptr = self, .vtable = &scheduler_vtable };
    }

    pub fn require(required: CapabilitySet) InitError!void {
        if (!supported_capabilities.containsAll(required)) {
            return error.UnsupportedSimIoCapabilities;
        }
    }

    pub fn missingCapabilities(required: CapabilitySet) CapabilitySet {
        return supported_capabilities.missing(required);
    }

    pub fn firstCapabilityViolation(self: *const SimIo) ?CapabilityViolation {
        return self.first_violation;
    }

    pub fn clearCapabilityViolation(self: *SimIo) void {
        self.violation_count = 0;
        self.first_violation = null;
    }

    pub fn ensureNoCapabilityViolation(self: *const SimIo) !void {
        if (self.first_violation != null) return error.SimIoCapabilityViolation;
    }

    /// Advance the monotonic clock and ordinary realtime together. Explicit
    /// realtime jumps are modeled separately by `jumpRealtime`.
    pub fn advance(self: *SimIo, delta_ns: u64) !void {
        const delta: i96 = @intCast(delta_ns);
        self.monotonic_ns = std.math.add(i96, self.monotonic_ns, delta) catch
            return error.SimIoClockOverflow;
        self.realtime_ns = std.math.add(i96, self.realtime_ns, delta) catch
            return error.SimIoClockOverflow;
        self.tasks.wakeDue(self.monotonic_ns, self.realtime_ns);
    }

    pub fn jumpRealtime(self: *SimIo, delta_ns: i64) !void {
        self.realtime_ns = std.math.add(i96, self.realtime_ns, delta_ns) catch
            return error.SimIoClockOverflow;
        self.tasks.wakeDue(self.monotonic_ns, self.realtime_ns);
    }

    pub fn setCpuTime(self: *SimIo, process_ns: i64, thread_ns: i64) void {
        self.process_cpu_ns = process_ns;
        self.thread_cpu_ns = thread_ns;
    }

    fn latch(self: *SimIo, operation: ViolationOperation) void {
        self.violation_count +|= 1;
        if (self.first_violation == null) {
            self.first_violation = .{
                .operation = operation,
                .sequence = self.violation_count,
            };
        }
    }

    fn timeAdvanceId(delta_ns: u64) ids.StableId {
        return ids.derive("sim-io.time-advance", ids.stable("sim-io", "global-time"), delta_ns);
    }
};

fn state(userdata: ?*anyopaque) *SimIo {
    return @ptrCast(@alignCast(userdata orelse unreachable));
}

fn crashHandler(userdata: ?*anyopaque) void {
    state(userdata).latch(.crash_handler);
}

fn asyncTask(userdata: ?*anyopaque, result: []u8, result_alignment: std.mem.Alignment, context: []const u8, context_alignment: std.mem.Alignment, start: *const fn (*const anyopaque, *anyopaque) void) ?*std.Io.AnyFuture {
    return state(userdata).tasks.async(result, result_alignment, context, context_alignment, start);
}

fn concurrentTask(userdata: ?*anyopaque, result_len: usize, result_alignment: std.mem.Alignment, context: []const u8, context_alignment: std.mem.Alignment, start: *const fn (*const anyopaque, *anyopaque) void) std.Io.ConcurrentError!*std.Io.AnyFuture {
    return state(userdata).tasks.concurrent(result_len, result_alignment, context, context_alignment, start);
}

fn futureAwait(userdata: ?*anyopaque, future: *std.Io.AnyFuture, result: []u8, result_alignment: std.mem.Alignment) void {
    const self = state(userdata);
    self.tasks.await(future, result, result_alignment) catch {
        self.latch(.future_await);
        @memset(result, 0);
    };
}

fn futureCancel(userdata: ?*anyopaque, future: *std.Io.AnyFuture, result: []u8, result_alignment: std.mem.Alignment) void {
    const self = state(userdata);
    self.tasks.cancel(future, result, result_alignment) catch {
        self.latch(.future_cancel);
        @memset(result, 0);
    };
}

fn groupAsync(userdata: ?*anyopaque, group: *std.Io.Group, context: []const u8, context_alignment: std.mem.Alignment, start: *const fn (*const anyopaque) void) void {
    state(userdata).tasks.groupAsync(group, context, context_alignment, start);
}

fn groupConcurrent(userdata: ?*anyopaque, group: *std.Io.Group, context: []const u8, context_alignment: std.mem.Alignment, start: *const fn (*const anyopaque) void) std.Io.ConcurrentError!void {
    return state(userdata).tasks.groupConcurrent(group, context, context_alignment, start);
}

fn groupAwait(userdata: ?*anyopaque, group: *std.Io.Group, token: *anyopaque) std.Io.Cancelable!void {
    const self = state(userdata);
    return self.tasks.groupAwait(group, token) catch |err| switch (err) {
        error.Canceled => error.Canceled,
        else => {
            self.latch(.group_await);
            return error.Canceled;
        },
    };
}

fn groupCancel(userdata: ?*anyopaque, group: *std.Io.Group, token: *anyopaque) void {
    const self = state(userdata);
    self.tasks.groupCancel(group, token) catch {
        self.latch(.group_cancel);
        group.token.store(null, .release);
    };
}

fn recancel(userdata: ?*anyopaque) void {
    const self = state(userdata);
    self.tasks.recancel() catch self.latch(.future_cancel);
}

fn swapCancelProtection(userdata: ?*anyopaque, new: std.Io.CancelProtection) std.Io.CancelProtection {
    const self = state(userdata);
    return self.tasks.swapCancelProtection(new) catch {
        self.latch(.future_cancel);
        return .unblocked;
    };
}

fn checkCancel(userdata: ?*anyopaque) std.Io.Cancelable!void {
    return state(userdata).tasks.checkCancel();
}

fn futexWait(userdata: ?*anyopaque, ptr: *const u32, expected: u32, timeout: std.Io.Timeout) std.Io.Cancelable!void {
    const self = state(userdata);
    const sleep_info = timeoutSleep(self, timeout) catch {
        self.latch(.futex_wait);
        return error.Canceled;
    };
    return self.tasks.futexWait(ptr, expected, sleep_info, false) catch |err| switch (err) {
        error.Canceled => error.Canceled,
        else => {
            self.latch(.futex_wait);
            return error.Canceled;
        },
    };
}

fn futexWaitUncancelable(userdata: ?*anyopaque, ptr: *const u32, expected: u32) void {
    const self = state(userdata);
    self.tasks.futexWait(ptr, expected, null, true) catch self.latch(.futex_wait_uncancelable);
}

fn futexWake(userdata: ?*anyopaque, ptr: *const u32, max_waiters: u32) void {
    const self = state(userdata);
    self.tasks.futexWake(ptr, max_waiters) catch self.latch(.futex_wake);
}

fn operate(userdata: ?*anyopaque, _: std.Io.Operation) std.Io.Cancelable!std.Io.Operation.Result {
    state(userdata).latch(.operation_batch);
    return error.Canceled;
}

fn batchAwaitAsync(userdata: ?*anyopaque, _: *std.Io.Batch) std.Io.Cancelable!void {
    state(userdata).latch(.operation_batch);
    return error.Canceled;
}

fn batchAwaitConcurrent(userdata: ?*anyopaque, _: *std.Io.Batch, _: std.Io.Timeout) std.Io.Batch.AwaitConcurrentError!void {
    state(userdata).latch(.operation_batch);
    return error.ConcurrencyUnavailable;
}

fn batchCancel(userdata: ?*anyopaque, _: *std.Io.Batch) void {
    state(userdata).latch(.operation_batch);
}

fn dirAccess(userdata: ?*anyopaque, _: std.Io.Dir, _: []const u8, _: std.Io.Dir.AccessOptions) std.Io.Dir.AccessError!void {
    state(userdata).latch(.directory_access);
    return error.FileNotFound;
}

fn dirClose(userdata: ?*anyopaque, _: []const std.Io.Dir) void {
    state(userdata).latch(.directory_close);
}

fn dirRead(userdata: ?*anyopaque, _: *std.Io.Dir.Reader, _: []std.Io.Dir.Entry) std.Io.Dir.Reader.Error!usize {
    state(userdata).latch(.directory_read);
    return error.AccessDenied;
}

fn dirSetTimestamps(userdata: ?*anyopaque, _: std.Io.Dir, _: []const u8, _: std.Io.Dir.SetTimestampsOptions) std.Io.Dir.SetTimestampsError!void {
    state(userdata).latch(.directory_set_timestamps);
    return error.AccessDenied;
}

fn fileClose(userdata: ?*anyopaque, _: []const std.Io.File) void {
    state(userdata).latch(.file_close);
}

fn fileIsTty(userdata: ?*anyopaque, _: std.Io.File) std.Io.Cancelable!bool {
    state(userdata).latch(.file_is_tty);
    return error.Canceled;
}

fn fileEnableAnsi(userdata: ?*anyopaque, _: std.Io.File) std.Io.File.EnableAnsiEscapeCodesError!void {
    state(userdata).latch(.file_enable_ansi);
    return error.NotTerminalDevice;
}

fn fileSupportsAnsi(userdata: ?*anyopaque, _: std.Io.File) std.Io.Cancelable!bool {
    state(userdata).latch(.file_supports_ansi);
    return error.Canceled;
}

fn fileSetTimestamps(userdata: ?*anyopaque, _: std.Io.File, _: std.Io.File.SetTimestampsOptions) std.Io.File.SetTimestampsError!void {
    state(userdata).latch(.file_set_timestamps);
    return error.AccessDenied;
}

fn fileUnlock(userdata: ?*anyopaque, _: std.Io.File) void {
    state(userdata).latch(.file_unlock);
}

fn memoryMapDestroy(userdata: ?*anyopaque, _: *std.Io.File.MemoryMap) void {
    state(userdata).latch(.memory_map_destroy);
}

fn memoryMapSetLength(userdata: ?*anyopaque, _: *std.Io.File.MemoryMap, _: usize) std.Io.File.MemoryMap.SetLengthError!void {
    state(userdata).latch(.memory_map_set_length);
    return error.AccessDenied;
}

fn memoryMapRead(userdata: ?*anyopaque, _: *std.Io.File.MemoryMap) std.Io.File.ReadPositionalError!void {
    state(userdata).latch(.memory_map_read);
    return error.AccessDenied;
}

fn memoryMapWrite(userdata: ?*anyopaque, _: *std.Io.File.MemoryMap) std.Io.File.WritePositionalError!void {
    state(userdata).latch(.memory_map_write);
    return error.AccessDenied;
}

fn lockStderr(userdata: ?*anyopaque, _: ?std.Io.Terminal.Mode) std.Io.Cancelable!std.Io.LockedStderr {
    state(userdata).latch(.stderr_lock);
    return error.Canceled;
}

fn tryLockStderr(userdata: ?*anyopaque, _: ?std.Io.Terminal.Mode) std.Io.Cancelable!?std.Io.LockedStderr {
    state(userdata).latch(.stderr_lock);
    return null;
}

fn unlockStderr(userdata: ?*anyopaque) void {
    state(userdata).latch(.stderr_unlock);
}

fn childWait(userdata: ?*anyopaque, _: *std.process.Child) std.process.Child.WaitError!std.process.Child.Term {
    state(userdata).latch(.child_wait);
    return error.AccessDenied;
}

fn childKill(userdata: ?*anyopaque, child: *std.process.Child) void {
    state(userdata).latch(.child_kill);
    child.id = null;
}

fn now(userdata: ?*anyopaque, clock: std.Io.Clock) std.Io.Timestamp {
    const self = state(userdata);
    return .fromNanoseconds(switch (clock) {
        .real => self.realtime_ns,
        .awake, .boot => self.monotonic_ns,
        .cpu_process => self.process_cpu_ns,
        .cpu_thread => self.thread_cpu_ns,
    });
}

fn clockResolution(_: ?*anyopaque, _: std.Io.Clock) std.Io.Clock.ResolutionError!std.Io.Duration {
    return .fromNanoseconds(1);
}

fn sleep(userdata: ?*anyopaque, timeout: std.Io.Timeout) std.Io.Cancelable!void {
    const self = state(userdata);
    const sleep_info = timeoutSleep(self, timeout) catch {
        self.latch(.sleep);
        return error.Canceled;
    };
    if (sleep_info) |value| {
        if (value.deadline_ns <= clockValue(self, value.clock)) return self.tasks.checkCancel();
    }
    return self.tasks.sleepCurrent(sleep_info) catch |err| switch (err) {
        error.Canceled => error.Canceled,
        else => {
            self.latch(.sleep);
            return error.Canceled;
        },
    };
}

fn timeoutSleep(self: *SimIo, timeout: std.Io.Timeout) !?task_mod.Sleep {
    return switch (timeout) {
        .none => null,
        .duration => |duration| blk: {
            if (duration.clock == .cpu_process or duration.clock == .cpu_thread)
                return error.UnsupportedSimIoCpuSleep;
            const deadline = std.math.add(i96, clockValue(self, duration.clock), duration.raw.toNanoseconds()) catch
                return error.SimIoClockOverflow;
            break :blk .{ .clock = duration.clock, .deadline_ns = deadline };
        },
        .deadline => |deadline| blk: {
            if (deadline.clock == .cpu_process or deadline.clock == .cpu_thread)
                return error.UnsupportedSimIoCpuSleep;
            break :blk .{ .clock = deadline.clock, .deadline_ns = deadline.raw.toNanoseconds() };
        },
    };
}

fn clockValue(self: *const SimIo, clock: std.Io.Clock) i96 {
    return switch (clock) {
        .real => self.realtime_ns,
        .awake, .boot => self.monotonic_ns,
        .cpu_process => self.process_cpu_ns,
        .cpu_thread => self.thread_cpu_ns,
    };
}

fn random(userdata: ?*anyopaque, buffer: []u8) void {
    const self = state(userdata);
    self.random_calls +|= 1;
    self.prng.random().bytes(buffer);
}

fn randomSecure(userdata: ?*anyopaque, buffer: []u8) std.Io.RandomSecureError!void {
    random(userdata, buffer);
}

fn netSend(userdata: ?*anyopaque, _: std.Io.net.Socket.Handle, _: []std.Io.net.OutgoingMessage, _: std.Io.net.SendFlags) struct { ?std.Io.net.Socket.SendError, usize } {
    state(userdata).latch(.network_send);
    return .{ error.NetworkDown, 0 };
}

fn netClose(userdata: ?*anyopaque, _: []const std.Io.net.Socket.Handle) void {
    state(userdata).latch(.network_close);
}

fn netInterfaceName(userdata: ?*anyopaque, _: std.Io.net.Interface) std.Io.net.Interface.NameError!std.Io.net.Interface.Name {
    state(userdata).latch(.network_interface_name);
    return error.InterfaceNotFound;
}

fn enumerateReady(ptr: *anyopaque, list: *transition.List, allocator: std.mem.Allocator) !void {
    const self: *SimIo = @ptrCast(@alignCast(ptr));
    try self.tasks.enumerateReady(list, allocator);
    if (self.tasks.nextWakeDelta(self.monotonic_ns, self.realtime_ns)) |delta_ns| {
        if (delta_ns == 0) return error.SimIoDueTimerNotWoken;
        try list.append(allocator, .{
            .id = SimIo.timeAdvanceId(delta_ns),
            .name = "sim-io.time_advance",
            .kind = .scheduler,
            .resource_id = ids.stable("sim-io", "global-time"),
            .parameter = @intCast(@min(delta_ns, @as(u64, std.math.maxInt(i64)))),
        });
    }
}

fn executeReady(ptr: *anyopaque, transition_id: ids.StableId, sink: *event.Sink, allocator: std.mem.Allocator) !void {
    const self: *SimIo = @ptrCast(@alignCast(ptr));
    if (try self.tasks.executeReady(transition_id)) |execution| {
        try self.ensureNoCapabilityViolation();
        try sink.emit(allocator, .{
            .id = ids.stable("event", switch (execution.kind) {
                .task => if (execution.completed) "sim-io.task_completed" else "sim-io.task_parked",
                .futex_wake => "sim-io.futex_waiter_selected",
            }),
            .name = switch (execution.kind) {
                .task => if (execution.completed) "sim-io.task_completed" else "sim-io.task_parked",
                .futex_wake => "sim-io.futex_waiter_selected",
            },
            .kind = .state_change,
            .actor_id = execution.task_id,
            .resource_id = execution.task_id,
        });
        return;
    }

    const delta_ns = self.tasks.nextWakeDelta(self.monotonic_ns, self.realtime_ns) orelse
        return error.UnknownSimIoTransition;
    if (delta_ns == 0 or transition_id != SimIo.timeAdvanceId(delta_ns))
        return error.UnknownSimIoTransition;
    try self.advance(delta_ns);
    try sink.emit(allocator, .{
        .id = ids.stable("event", "sim-io.time_advanced"),
        .name = "sim-io.time_advanced",
        .kind = .state_change,
        .resource_id = ids.stable("sim-io", "global-time"),
        .payload_digest = delta_ns,
    });
}

fn quiescent(ptr: *anyopaque) bool {
    const self: *SimIo = @ptrCast(@alignCast(ptr));
    return self.tasks.isQuiescent();
}

const scheduler_vtable: runtime_mod.SchedulerPort.VTable = .{
    .enumerate_ready = enumerateReady,
    .execute_ready = executeReady,
    .quiescent = quiescent,
};

const vtable: std.Io.VTable = blk: {
    // This copy is the fail-closed proof boundary. New Zig vtable fields make
    // this declaration keep compiling only because `std.Io.failing` supplies a
    // deterministic non-host implementation; the tests below compare every
    // entry against Threaded and pin the compiler version in metadata.
    var result = std.Io.failing.vtable.*;
    result.crashHandler = crashHandler;
    result.async = asyncTask;
    result.concurrent = concurrentTask;
    result.await = futureAwait;
    result.cancel = futureCancel;
    result.groupAsync = groupAsync;
    result.groupConcurrent = groupConcurrent;
    result.groupAwait = groupAwait;
    result.groupCancel = groupCancel;
    result.recancel = recancel;
    result.swapCancelProtection = swapCancelProtection;
    result.checkCancel = checkCancel;
    result.futexWait = futexWait;
    result.futexWaitUncancelable = futexWaitUncancelable;
    result.futexWake = futexWake;
    result.operate = operate;
    result.batchAwaitAsync = batchAwaitAsync;
    result.batchAwaitConcurrent = batchAwaitConcurrent;
    result.batchCancel = batchCancel;
    result.dirAccess = dirAccess;
    result.dirClose = dirClose;
    result.dirRead = dirRead;
    result.dirSetTimestamps = dirSetTimestamps;
    result.fileClose = fileClose;
    result.fileIsTty = fileIsTty;
    result.fileEnableAnsiEscapeCodes = fileEnableAnsi;
    result.fileSupportsAnsiEscapeCodes = fileSupportsAnsi;
    result.fileSetTimestamps = fileSetTimestamps;
    result.fileUnlock = fileUnlock;
    result.fileMemoryMapDestroy = memoryMapDestroy;
    result.fileMemoryMapSetLength = memoryMapSetLength;
    result.fileMemoryMapRead = memoryMapRead;
    result.fileMemoryMapWrite = memoryMapWrite;
    result.lockStderr = lockStderr;
    result.tryLockStderr = tryLockStderr;
    result.unlockStderr = unlockStderr;
    result.childWait = childWait;
    result.childKill = childKill;
    result.now = now;
    result.clockResolution = clockResolution;
    result.sleep = sleep;
    result.random = random;
    result.randomSecure = randomSecure;
    result.netSend = netSend;
    result.netClose = netClose;
    result.netInterfaceName = netInterfaceName;
    break :blk result;
};

test "SimIo capability construction fails before exposing unsupported services" {
    try SimIo.require(.of(&.{ .clock_read, .deterministic_entropy }));
    try std.testing.expectError(
        error.UnsupportedSimIoCapabilities,
        SimIo.init(.{ .required = .of(&.{.files}) }),
    );
    try std.testing.expectEqual(
        CapabilitySet.of(&.{ .files, .sockets }).bits,
        SimIo.missingCapabilities(.of(&.{ .clock_read, .files, .sockets })).bits,
    );
}

test "SimIo clocks and entropy are deterministic and host independent" {
    var left = try SimIo.init(.{
        .required = .of(&.{ .clock_read, .deterministic_entropy }),
        .seed = 0x51_4d_49_4f,
        .monotonic_ns = 100,
        .realtime_ns = 1_000,
    });
    defer left.deinit();
    var right = try SimIo.init(.{
        .required = .of(&.{ .clock_read, .deterministic_entropy }),
        .seed = 0x51_4d_49_4f,
        .monotonic_ns = 100,
        .realtime_ns = 1_000,
    });
    defer right.deinit();
    const left_io = left.io();
    const right_io = right.io();

    try std.testing.expectEqual(@as(i96, 100), std.Io.Clock.awake.now(left_io).toNanoseconds());
    try std.testing.expectEqual(@as(i96, 1_000), std.Io.Clock.real.now(left_io).toNanoseconds());
    try left.advance(25);
    try left.jumpRealtime(-10);
    try std.testing.expectEqual(@as(i96, 125), std.Io.Clock.boot.now(left_io).toNanoseconds());
    try std.testing.expectEqual(@as(i96, 1_015), std.Io.Clock.real.now(left_io).toNanoseconds());
    try std.testing.expectEqual(@as(i96, 1), (try std.Io.Clock.real.resolution(left_io)).toNanoseconds());

    var left_bytes: [64]u8 = undefined;
    var right_bytes: [64]u8 = undefined;
    left_io.random(&left_bytes);
    right_io.random(&right_bytes);
    try std.testing.expectEqualSlices(u8, &left_bytes, &right_bytes);
    try std.testing.expectEqual(@as(u64, 1), left.random_calls);
    try left.ensureNoCapabilityViolation();
}

test "SimIo unsupported operations latch a deterministic harness violation" {
    var sim = try SimIo.init(.{});
    defer sim.deinit();
    const io = sim.io();
    try std.testing.expectError(
        error.FileNotFound,
        std.Io.Dir.cwd().access(io, "must-not-touch-host", .{}),
    );
    try std.testing.expectEqual(ViolationOperation.directory_access, sim.firstCapabilityViolation().?.operation);
    try std.testing.expectError(error.SimIoCapabilityViolation, sim.ensureNoCapabilityViolation());

    sim.clearCapabilityViolation();
    io.vtable.netClose(io.userdata, &.{});
    try std.testing.expectEqual(ViolationOperation.network_close, sim.firstCapabilityViolation().?.operation);
}

test "SimIo vtable contains no std.Io.Threaded handlers" {
    var sim = try SimIo.init(.{});
    defer sim.deinit();
    const io = sim.io();
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const host_io = threaded.io();

    inline for (@typeInfo(std.Io.VTable).@"struct".fields) |field| {
        try std.testing.expect(
            @intFromPtr(@field(io.vtable, field.name)) !=
                @intFromPtr(@field(host_io.vtable, field.name)),
        );
    }
    const info = metadata();
    try std.testing.expectEqualStrings(builtin.zig_version_string, info.zig_version);
    try std.testing.expect(info.capability_digest != 0);
}

test "SimIo replay backend identities are canonical and compatibility checked" {
    const backend_ids = artifactBackendIds();
    try ids.validateCanonical(&backend_ids);
    try validateArtifactBackendIds(&backend_ids);
    try std.testing.expectError(
        error.IncompatibleSimIoBackend,
        validateArtifactBackendIds(backend_ids[1..]),
    );
}

test "SimIo scheduler controls nested futures and virtual sleep" {
    const Shared = struct {
        io: std.Io,
        order: [5]u8 = [_]u8{0} ** 5,
        len: usize = 0,

        fn push(self: *@This(), value: u8) void {
            self.order[self.len] = value;
            self.len += 1;
        }

        fn child(self: *@This()) u32 {
            self.push(2);
            std.Io.sleep(self.io, .fromNanoseconds(5), .awake) catch unreachable;
            self.push(4);
            return 42;
        }

        fn parent(self: *@This()) void {
            self.push(1);
            var future = self.io.async(child, .{self});
            self.push(3);
            std.debug.assert(future.await(self.io) == 42);
            self.push(5);
        }
    };

    var sim = try SimIo.init(.{
        .required = .of(&.{ .task_scheduling, .sleep, .clock_read }),
    });
    defer sim.deinit();
    var shared = Shared{ .io = sim.io() };
    _ = shared.io.async(Shared.parent, .{&shared});

    const scheduler = sim.scheduler();
    var enabled: transition.List = .{};
    defer enabled.deinit(std.testing.allocator);
    var sink: event.Sink = .{};
    defer sink.deinit(std.testing.allocator);
    while (!scheduler.quiescent()) {
        enabled.items.clearRetainingCapacity();
        try scheduler.enumerateReady(&enabled, std.testing.allocator);
        try enabled.canonicalize();
        try std.testing.expect(enabled.items.items.len != 0);
        try scheduler.executeReady(enabled.items.items[0].id, &sink, std.testing.allocator);
    }

    try std.testing.expectEqualSlices(u8, &.{ 1, 3, 2, 4, 5 }, shared.order[0..shared.len]);
    try std.testing.expectEqual(@as(i96, 5), std.Io.Clock.awake.now(shared.io).toNanoseconds());
    try sim.ensureNoCapabilityViolation();
}

test "SimIo groups and futex wake ordering remain scheduler visible" {
    const Shared = struct {
        io: std.Io,
        futex: u32 align(@alignOf(u32)) = 0,
        completed: u32 = 0,

        fn waiter(self: *@This()) std.Io.Cancelable!void {
            try std.Io.futexWait(self.io, u32, &self.futex, 0);
            self.completed += 1;
        }

        fn parent(self: *@This()) void {
            var group: std.Io.Group = .init;
            group.async(self.io, waiter, .{self});
            group.async(self.io, waiter, .{self});
            std.Io.futexWake(self.io, u32, &self.futex, 1);
            std.Io.futexWake(self.io, u32, &self.futex, 1);
            group.await(self.io) catch unreachable;
        }
    };

    var sim = try SimIo.init(.{
        .required = .of(&.{ .task_scheduling, .synchronization }),
    });
    defer sim.deinit();
    var shared = Shared{ .io = sim.io() };
    _ = shared.io.async(Shared.parent, .{&shared});

    const scheduler = sim.scheduler();
    var enabled: transition.List = .{};
    defer enabled.deinit(std.testing.allocator);
    var sink: event.Sink = .{};
    defer sink.deinit(std.testing.allocator);
    var saw_choice = false;
    while (!scheduler.quiescent()) {
        enabled.items.clearRetainingCapacity();
        try scheduler.enumerateReady(&enabled, std.testing.allocator);
        try enabled.canonicalize();
        if (enabled.items.items.len > 1) saw_choice = true;
        try std.testing.expect(enabled.items.items.len != 0);
        try scheduler.executeReady(enabled.items.items[0].id, &sink, std.testing.allocator);
    }
    try std.testing.expect(saw_choice);
    try std.testing.expectEqual(@as(u32, 2), shared.completed);
    try sim.ensureNoCapabilityViolation();
}

test "SimIo future cancellation is delivered at a task cancellation point" {
    const Shared = struct {
        io: std.Io,
        canceled: bool = false,
        parent_done: bool = false,

        fn child(self: *@This()) std.Io.Cancelable!u8 {
            const forever: std.Io.Timeout = .none;
            try forever.sleep(self.io);
            return 1;
        }

        fn parent(self: *@This()) void {
            var future = self.io.async(child, .{self});
            _ = future.cancel(self.io) catch |err| switch (err) {
                error.Canceled => self.canceled = true,
            };
            self.parent_done = true;
        }
    };

    var sim = try SimIo.init(.{
        .required = .of(&.{ .task_scheduling, .sleep }),
    });
    defer sim.deinit();
    var shared = Shared{ .io = sim.io() };
    _ = shared.io.async(Shared.parent, .{&shared});

    const scheduler = sim.scheduler();
    var enabled: transition.List = .{};
    defer enabled.deinit(std.testing.allocator);
    var sink: event.Sink = .{};
    defer sink.deinit(std.testing.allocator);
    while (!scheduler.quiescent()) {
        enabled.items.clearRetainingCapacity();
        try scheduler.enumerateReady(&enabled, std.testing.allocator);
        try enabled.canonicalize();
        try std.testing.expect(enabled.items.items.len != 0);
        try scheduler.executeReady(enabled.items.items[0].id, &sink, std.testing.allocator);
    }
    try std.testing.expect(shared.canceled);
    try std.testing.expect(shared.parent_done);
    try sim.ensureNoCapabilityViolation();
}

test "SimIo runs std.Io mutex queue and select on the futex kernel" {
    const Selected = union(enum) { fast: u8, slow: u8 };
    const Shared = struct {
        io: std.Io,
        mutex: std.Io.Mutex = .init,
        protected_value: u32 = 0,
        selected_sum: u32 = 0,
        done: bool = false,

        fn worker(self: *@This()) std.Io.Cancelable!void {
            try self.mutex.lock(self.io);
            defer self.mutex.unlock(self.io);
            try std.Io.sleep(self.io, .fromNanoseconds(1), .awake);
            self.protected_value += 1;
        }

        fn selected(self: *@This(), delay_ns: i64, value: u8) u8 {
            std.Io.sleep(self.io, .fromNanoseconds(delay_ns), .awake) catch unreachable;
            return value;
        }

        fn parent(self: *@This()) void {
            var group: std.Io.Group = .init;
            group.async(self.io, worker, .{self});
            group.async(self.io, worker, .{self});
            group.await(self.io) catch unreachable;

            var buffer: [2]Selected = undefined;
            var selection = std.Io.Select(Selected).init(self.io, &buffer);
            selection.async(.slow, selected, .{ self, 5, 20 });
            selection.async(.fast, selected, .{ self, 2, 10 });
            const first = selection.await() catch unreachable;
            const second = selection.await() catch unreachable;
            self.selected_sum = switch (first) {
                inline else => |value| value,
            } + switch (second) {
                inline else => |value| value,
            };
            selection.cancelDiscard();
            self.done = true;
        }
    };

    var sim = try SimIo.init(.{
        .required = .of(&.{ .task_scheduling, .synchronization, .sleep }),
    });
    defer sim.deinit();
    var shared = Shared{ .io = sim.io() };
    _ = shared.io.async(Shared.parent, .{&shared});

    const scheduler = sim.scheduler();
    var enabled: transition.List = .{};
    defer enabled.deinit(std.testing.allocator);
    var sink: event.Sink = .{};
    defer sink.deinit(std.testing.allocator);
    while (!scheduler.quiescent()) {
        enabled.items.clearRetainingCapacity();
        try scheduler.enumerateReady(&enabled, std.testing.allocator);
        try enabled.canonicalize();
        try std.testing.expect(enabled.items.items.len != 0);
        try scheduler.executeReady(enabled.items.items[0].id, &sink, std.testing.allocator);
    }
    try std.testing.expect(shared.done);
    try std.testing.expectEqual(@as(u32, 2), shared.protected_value);
    try std.testing.expectEqual(@as(u32, 30), shared.selected_sum);
    try sim.ensureNoCapabilityViolation();
}
