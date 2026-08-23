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
const file_mod = @import("sim_io_file.zig");
const net_mod = @import("sim_io_net.zig");
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
    .files,
    .sockets,
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
    directory_symbolic_link,
    directory_hard_link,
    directory_set_timestamps,
    file_close,
    file_is_tty,
    file_enable_ansi,
    file_supports_ansi,
    file_set_timestamps,
    file_write_file,
    file_hard_link,
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
    file_allocator: std.mem.Allocator = std.heap.page_allocator,
    files: file_mod.Config = .{},
    net_allocator: std.mem.Allocator = std.heap.page_allocator,
    network: net_mod.Config = .{},
};

pub const InitError = error{
    OutOfMemory,
    UnsupportedSimIoCapabilities,
    SimIoStackTooSmall,
    InvalidSimIoTaskLimit,
    InvalidSimIoFileHandleLimit,
    InvalidSimIoSocketLimit,
    InvalidSimIoStreamCapacity,
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
    files: file_mod.FileSystem,
    network: net_mod.Network,

    pub fn init(config: Config) InitError!SimIo {
        if (!supported_capabilities.containsAll(config.required)) {
            return error.UnsupportedSimIoCapabilities;
        }
        var files = file_mod.FileSystem.init(config.file_allocator, config.files) catch |err| switch (err) {
            error.InvalidSimIoFileHandleLimit => return error.InvalidSimIoFileHandleLimit,
            else => return error.OutOfMemory,
        };
        errdefer files.deinit();
        var network = net_mod.Network.init(config.net_allocator, config.network) catch |err| switch (err) {
            error.InvalidSimIoSocketLimit => return error.InvalidSimIoSocketLimit,
            error.InvalidSimIoStreamCapacity => return error.InvalidSimIoStreamCapacity,
        };
        errdefer network.deinit();
        return .{
            .prng = std.Random.DefaultPrng.init(config.seed),
            .monotonic_ns = config.monotonic_ns,
            .realtime_ns = config.realtime_ns,
            .process_cpu_ns = config.process_cpu_ns,
            .thread_cpu_ns = config.thread_cpu_ns,
            .tasks = try task_mod.Kernel.init(config.task_allocator, config.tasks),
            .files = files,
            .network = network,
        };
    }

    pub fn deinit(self: *SimIo) void {
        self.tasks.deinit();
        self.files.deinit();
        self.network.deinit();
        self.* = undefined;
    }

    pub fn io(self: *SimIo) std.Io {
        self.bindNetwork();
        return .{ .userdata = self, .vtable = &vtable };
    }

    pub fn scheduler(self: *SimIo) runtime_mod.SchedulerPort {
        self.bindNetwork();
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

    fn bindNetwork(self: *SimIo) void {
        self.network.bindWaitPort(.{ .ptr = &self.tasks, .vtable = &network_wait_vtable });
    }
};

fn networkWait(ptr: *anyopaque, resource_id: ids.StableId) !void {
    const kernel: *task_mod.Kernel = @ptrCast(@alignCast(ptr));
    return kernel.waitExternal(resource_id);
}

fn networkWake(ptr: *anyopaque, resource_id: ids.StableId, max_waiters: u32) !void {
    const kernel: *task_mod.Kernel = @ptrCast(@alignCast(ptr));
    return kernel.wakeExternal(resource_id, max_waiters);
}

const network_wait_vtable: net_mod.WaitPort.VTable = .{
    .wait = networkWait,
    .wake = networkWake,
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

fn operate(userdata: ?*anyopaque, operation: std.Io.Operation) std.Io.Cancelable!std.Io.Operation.Result {
    const self = state(userdata);
    return switch (operation) {
        .file_read_streaming => |request| .{
            .file_read_streaming = self.files.readStreaming(request.file, request.data),
        },
        .file_write_streaming => |request| .{
            .file_write_streaming = self.files.writeStreaming(
                request.file,
                request.header,
                request.data,
                request.splat,
                self.realtime_ns,
            ),
        },
        .net_receive => {
            self.latch(.operation_batch);
            return .{ .net_receive = .{ error.NetworkDown, 0 } };
        },
        .device_io_control => {
            self.latch(.operation_batch);
            return error.Canceled;
        },
    };
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

fn dirCreateDir(userdata: ?*anyopaque, dir: std.Io.Dir, sub_path: []const u8, permissions: std.Io.Dir.Permissions) std.Io.Dir.CreateDirError!void {
    const self = state(userdata);
    return self.files.createDir(dir, sub_path, permissions, self.realtime_ns);
}

fn dirCreateDirPath(userdata: ?*anyopaque, dir: std.Io.Dir, sub_path: []const u8, permissions: std.Io.Dir.Permissions) std.Io.Dir.CreateDirPathError!std.Io.Dir.CreatePathStatus {
    const self = state(userdata);
    return self.files.createDirPath(dir, sub_path, permissions, self.realtime_ns);
}

fn dirCreateDirPathOpen(userdata: ?*anyopaque, dir: std.Io.Dir, sub_path: []const u8, permissions: std.Io.Dir.Permissions, options: std.Io.Dir.OpenOptions) std.Io.Dir.CreateDirPathOpenError!std.Io.Dir {
    const self = state(userdata);
    _ = self.files.createDirPath(dir, sub_path, permissions, self.realtime_ns) catch |err|
        return @errorCast(err);
    return self.files.openDir(dir, sub_path, options) catch |err| return @errorCast(err);
}

fn dirOpenDir(userdata: ?*anyopaque, dir: std.Io.Dir, sub_path: []const u8, options: std.Io.Dir.OpenOptions) std.Io.Dir.OpenError!std.Io.Dir {
    return state(userdata).files.openDir(dir, sub_path, options);
}

fn dirStat(userdata: ?*anyopaque, dir: std.Io.Dir) std.Io.Dir.StatError!std.Io.Dir.Stat {
    return state(userdata).files.statDir(dir);
}

fn dirStatFile(userdata: ?*anyopaque, dir: std.Io.Dir, sub_path: []const u8, _: std.Io.Dir.StatFileOptions) std.Io.Dir.StatFileError!std.Io.File.Stat {
    return state(userdata).files.statFileAt(dir, sub_path);
}

fn dirAccess(userdata: ?*anyopaque, dir: std.Io.Dir, sub_path: []const u8, _: std.Io.Dir.AccessOptions) std.Io.Dir.AccessError!void {
    return state(userdata).files.access(dir, sub_path);
}

fn dirCreateFile(userdata: ?*anyopaque, dir: std.Io.Dir, sub_path: []const u8, options: std.Io.Dir.CreateFileOptions) std.Io.File.OpenError!std.Io.File {
    const self = state(userdata);
    return self.files.createFile(dir, sub_path, options, self.realtime_ns);
}

fn dirOpenFile(userdata: ?*anyopaque, dir: std.Io.Dir, sub_path: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
    return state(userdata).files.openFile(dir, sub_path, options);
}

fn dirClose(userdata: ?*anyopaque, dirs: []const std.Io.Dir) void {
    const self = state(userdata);
    if (!self.files.closeDirs(dirs)) self.latch(.directory_close);
}

fn dirCreateFileAtomic(userdata: ?*anyopaque, dir: std.Io.Dir, sub_path: []const u8, options: std.Io.Dir.CreateFileAtomicOptions) std.Io.Dir.CreateFileAtomicError!std.Io.File.Atomic {
    const self = state(userdata);
    return self.files.createFileAtomic(dir, sub_path, options, self.realtime_ns);
}

fn dirRead(userdata: ?*anyopaque, reader: *std.Io.Dir.Reader, entries: []std.Io.Dir.Entry) std.Io.Dir.Reader.Error!usize {
    return state(userdata).files.readDir(reader, entries);
}

fn dirRealPath(userdata: ?*anyopaque, dir: std.Io.Dir, out: []u8) std.Io.Dir.RealPathError!usize {
    return state(userdata).files.realPathDir(dir, out);
}

fn dirRealPathFile(userdata: ?*anyopaque, dir: std.Io.Dir, sub_path: []const u8, out: []u8) std.Io.Dir.RealPathFileError!usize {
    return state(userdata).files.realPathAt(dir, sub_path, out);
}

fn dirDeleteFile(userdata: ?*anyopaque, dir: std.Io.Dir, sub_path: []const u8) std.Io.Dir.DeleteFileError!void {
    return state(userdata).files.deleteFile(dir, sub_path);
}

fn dirDeleteDir(userdata: ?*anyopaque, dir: std.Io.Dir, sub_path: []const u8) std.Io.Dir.DeleteDirError!void {
    return state(userdata).files.deleteDir(dir, sub_path);
}

fn dirRename(userdata: ?*anyopaque, old_dir: std.Io.Dir, old_sub_path: []const u8, new_dir: std.Io.Dir, new_sub_path: []const u8) std.Io.Dir.RenameError!void {
    return state(userdata).files.rename(old_dir, old_sub_path, new_dir, new_sub_path, false) catch |err|
        return @errorCast(err);
}

fn dirRenamePreserve(userdata: ?*anyopaque, old_dir: std.Io.Dir, old_sub_path: []const u8, new_dir: std.Io.Dir, new_sub_path: []const u8) std.Io.Dir.RenamePreserveError!void {
    return state(userdata).files.rename(old_dir, old_sub_path, new_dir, new_sub_path, true) catch |err|
        return @errorCast(err);
}

fn dirSymLink(userdata: ?*anyopaque, _: std.Io.Dir, _: []const u8, _: []const u8, _: std.Io.Dir.SymLinkFlags) std.Io.Dir.SymLinkError!void {
    state(userdata).latch(.directory_symbolic_link);
    return error.AccessDenied;
}

fn dirReadLink(userdata: ?*anyopaque, _: std.Io.Dir, _: []const u8, _: []u8) std.Io.Dir.ReadLinkError!usize {
    state(userdata).latch(.directory_symbolic_link);
    return error.AccessDenied;
}

fn dirSetOwner(userdata: ?*anyopaque, dir: std.Io.Dir, _: ?std.Io.File.Uid, _: ?std.Io.File.Gid) std.Io.Dir.SetOwnerError!void {
    _ = state(userdata).files.statDir(dir) catch return error.AccessDenied;
}

fn dirSetFileOwner(userdata: ?*anyopaque, dir: std.Io.Dir, sub_path: []const u8, _: ?std.Io.File.Uid, _: ?std.Io.File.Gid, _: std.Io.Dir.SetFileOwnerOptions) std.Io.Dir.SetFileOwnerError!void {
    _ = state(userdata).files.statFileAt(dir, sub_path) catch |err| return switch (err) {
        error.FileNotFound => error.FileNotFound,
        error.NameTooLong => error.NameTooLong,
        else => error.AccessDenied,
    };
}

fn dirSetPermissions(userdata: ?*anyopaque, dir: std.Io.Dir, permissions: std.Io.Dir.Permissions) std.Io.Dir.SetPermissionsError!void {
    const self = state(userdata);
    return self.files.setDirPermissions(dir, permissions, self.realtime_ns);
}

fn dirSetFilePermissions(userdata: ?*anyopaque, dir: std.Io.Dir, sub_path: []const u8, permissions: std.Io.File.Permissions, _: std.Io.Dir.SetFilePermissionsOptions) std.Io.Dir.SetFilePermissionsError!void {
    const self = state(userdata);
    return self.files.setPermissionsAt(dir, sub_path, permissions, self.realtime_ns);
}

fn dirSetTimestamps(userdata: ?*anyopaque, dir: std.Io.Dir, sub_path: []const u8, options: std.Io.Dir.SetTimestampsOptions) std.Io.Dir.SetTimestampsError!void {
    const self = state(userdata);
    return self.files.setTimestampsAt(dir, sub_path, options, self.realtime_ns);
}

fn dirHardLink(userdata: ?*anyopaque, _: std.Io.Dir, _: []const u8, _: std.Io.Dir, _: []const u8, _: std.Io.Dir.HardLinkOptions) std.Io.Dir.HardLinkError!void {
    state(userdata).latch(.directory_hard_link);
    return error.OperationUnsupported;
}

fn fileStat(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.StatError!std.Io.File.Stat {
    return state(userdata).files.statFile(file);
}

fn fileLength(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.LengthError!u64 {
    return state(userdata).files.fileLength(file);
}

fn fileClose(userdata: ?*anyopaque, files: []const std.Io.File) void {
    const self = state(userdata);
    if (!self.files.closeFiles(files)) self.latch(.file_close);
}

fn fileWritePositional(userdata: ?*anyopaque, file: std.Io.File, header: []const u8, data: []const []const u8, splat: usize, offset: u64) std.Io.File.WritePositionalError!usize {
    const self = state(userdata);
    return self.files.writePositional(file, header, data, splat, offset, self.realtime_ns);
}

fn fileWriteFileStreaming(userdata: ?*anyopaque, _: std.Io.File, _: []const u8, _: *std.Io.File.Reader, _: std.Io.Limit) std.Io.File.Writer.WriteFileError!usize {
    state(userdata).latch(.file_write_file);
    return error.Unimplemented;
}

fn fileWriteFilePositional(userdata: ?*anyopaque, _: std.Io.File, _: []const u8, _: *std.Io.File.Reader, _: std.Io.Limit, _: u64) std.Io.File.WriteFilePositionalError!usize {
    state(userdata).latch(.file_write_file);
    return error.Unimplemented;
}

fn fileReadPositional(userdata: ?*anyopaque, file: std.Io.File, data: []const []u8, offset: u64) std.Io.File.ReadPositionalError!usize {
    return state(userdata).files.readPositional(file, data, offset);
}

fn fileSeekBy(userdata: ?*anyopaque, file: std.Io.File, relative_offset: i64) std.Io.File.SeekError!void {
    return state(userdata).files.seekBy(file, relative_offset);
}

fn fileSeekTo(userdata: ?*anyopaque, file: std.Io.File, absolute_offset: u64) std.Io.File.SeekError!void {
    return state(userdata).files.seekTo(file, absolute_offset);
}

fn fileSync(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.SyncError!void {
    return state(userdata).files.syncFile(file);
}

fn fileSetLength(userdata: ?*anyopaque, file: std.Io.File, length: u64) std.Io.File.SetLengthError!void {
    const self = state(userdata);
    return self.files.setLength(file, length, self.realtime_ns);
}

fn fileIsTty(_: ?*anyopaque, _: std.Io.File) std.Io.Cancelable!bool {
    return false;
}

fn fileEnableAnsi(_: ?*anyopaque, _: std.Io.File) std.Io.File.EnableAnsiEscapeCodesError!void {
    return error.NotTerminalDevice;
}

fn fileSupportsAnsi(_: ?*anyopaque, _: std.Io.File) std.Io.Cancelable!bool {
    return false;
}

fn fileSetOwner(userdata: ?*anyopaque, file: std.Io.File, _: ?std.Io.File.Uid, _: ?std.Io.File.Gid) std.Io.File.SetOwnerError!void {
    _ = state(userdata).files.statFile(file) catch return error.AccessDenied;
}

fn fileSetPermissions(userdata: ?*anyopaque, file: std.Io.File, permissions: std.Io.File.Permissions) std.Io.File.SetPermissionsError!void {
    const self = state(userdata);
    return self.files.setFilePermissions(file, permissions, self.realtime_ns);
}

fn fileSetTimestamps(userdata: ?*anyopaque, file: std.Io.File, options: std.Io.File.SetTimestampsOptions) std.Io.File.SetTimestampsError!void {
    const self = state(userdata);
    return self.files.setFileTimestamps(file, options, self.realtime_ns);
}

fn fileLock(userdata: ?*anyopaque, file: std.Io.File, lock: std.Io.File.Lock) std.Io.File.LockError!void {
    if (!try state(userdata).files.tryLock(file, lock)) return error.FileLocksUnsupported;
}

fn fileTryLock(userdata: ?*anyopaque, file: std.Io.File, lock: std.Io.File.Lock) std.Io.File.LockError!bool {
    return state(userdata).files.tryLock(file, lock);
}

fn fileUnlock(userdata: ?*anyopaque, file: std.Io.File) void {
    const self = state(userdata);
    if (!self.files.unlock(file)) self.latch(.file_unlock);
}

fn fileDowngradeLock(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.DowngradeLockError!void {
    return state(userdata).files.downgradeLock(file);
}

fn fileRealPath(userdata: ?*anyopaque, file: std.Io.File, out: []u8) std.Io.File.RealPathError!usize {
    return state(userdata).files.realPathFile(file, out);
}

fn fileHardLink(userdata: ?*anyopaque, _: std.Io.File, _: std.Io.Dir, _: []const u8, _: std.Io.File.HardLinkOptions) std.Io.File.HardLinkError!void {
    state(userdata).latch(.file_hard_link);
    return error.OperationUnsupported;
}

fn memoryMapCreate(userdata: ?*anyopaque, file: std.Io.File, options: std.Io.File.MemoryMap.CreateOptions) std.Io.File.MemoryMap.CreateError!std.Io.File.MemoryMap {
    return state(userdata).files.createMemoryMap(file, options);
}

fn memoryMapDestroy(userdata: ?*anyopaque, map: *std.Io.File.MemoryMap) void {
    state(userdata).files.destroyMemoryMap(map);
}

fn memoryMapSetLength(userdata: ?*anyopaque, map: *std.Io.File.MemoryMap, len: usize) std.Io.File.MemoryMap.SetLengthError!void {
    return state(userdata).files.setMemoryMapLength(map, len);
}

fn memoryMapRead(userdata: ?*anyopaque, map: *std.Io.File.MemoryMap) std.Io.File.ReadPositionalError!void {
    return state(userdata).files.readMemoryMap(map);
}

fn memoryMapWrite(userdata: ?*anyopaque, map: *std.Io.File.MemoryMap) std.Io.File.WritePositionalError!void {
    const self = state(userdata);
    return self.files.writeMemoryMap(map, self.realtime_ns);
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

fn netListenIp(userdata: ?*anyopaque, address: *const std.Io.net.IpAddress, options: std.Io.net.IpAddress.ListenOptions) std.Io.net.IpAddress.ListenError!std.Io.net.Socket {
    return state(userdata).network.listenIp(address, options);
}

fn netAccept(userdata: ?*anyopaque, server: std.Io.net.Socket.Handle, _: std.Io.net.Server.AcceptOptions) std.Io.net.Server.AcceptError!std.Io.net.Socket {
    return state(userdata).network.accept(server);
}

fn netBindIp(userdata: ?*anyopaque, _: *const std.Io.net.IpAddress, _: std.Io.net.IpAddress.BindOptions) std.Io.net.IpAddress.BindError!std.Io.net.Socket {
    state(userdata).latch(.network_send);
    return error.OptionUnsupported;
}

fn netConnectIp(userdata: ?*anyopaque, address: *const std.Io.net.IpAddress, options: std.Io.net.IpAddress.ConnectOptions) std.Io.net.IpAddress.ConnectError!std.Io.net.Socket {
    return state(userdata).network.connectIp(address, options);
}

fn netListenUnix(userdata: ?*anyopaque, address: *const std.Io.net.UnixAddress, options: std.Io.net.UnixAddress.ListenOptions) std.Io.net.UnixAddress.ListenError!std.Io.net.Socket.Handle {
    return state(userdata).network.listenUnix(address, options);
}

fn netConnectUnix(userdata: ?*anyopaque, address: *const std.Io.net.UnixAddress) std.Io.net.UnixAddress.ConnectError!std.Io.net.Socket.Handle {
    return state(userdata).network.connectUnix(address);
}

fn netSocketCreatePair(userdata: ?*anyopaque, options: std.Io.net.Socket.CreatePairOptions) std.Io.net.Socket.CreatePairError![2]std.Io.net.Socket {
    return state(userdata).network.createPair(options);
}

fn netSend(userdata: ?*anyopaque, _: std.Io.net.Socket.Handle, _: []std.Io.net.OutgoingMessage, _: std.Io.net.SendFlags) struct { ?std.Io.net.Socket.SendError, usize } {
    state(userdata).latch(.network_send);
    return .{ error.NetworkDown, 0 };
}

fn netRead(userdata: ?*anyopaque, source: std.Io.net.Socket.Handle, data: [][]u8) std.Io.net.Stream.Reader.Error!usize {
    return state(userdata).network.read(source, data);
}

fn netWrite(userdata: ?*anyopaque, destination: std.Io.net.Socket.Handle, header: []const u8, data: []const []const u8, splat: usize) std.Io.net.Stream.Writer.Error!usize {
    return state(userdata).network.write(destination, header, data, splat);
}

fn netWriteFile(userdata: ?*anyopaque, _: std.Io.net.Socket.Handle, _: []const u8, _: *std.Io.File.Reader, _: std.Io.Limit) std.Io.net.Stream.Writer.WriteFileError!usize {
    state(userdata).latch(.network_send);
    return error.NetworkDown;
}

fn netClose(userdata: ?*anyopaque, handles: []const std.Io.net.Socket.Handle) void {
    const self = state(userdata);
    if (!self.network.close(handles)) self.latch(.network_close);
}

fn netShutdown(userdata: ?*anyopaque, handle: std.Io.net.Socket.Handle, how: std.Io.net.ShutdownHow) std.Io.net.ShutdownError!void {
    return state(userdata).network.shutdown(handle, how);
}

fn netInterfaceNameResolve(_: ?*anyopaque, name: *const std.Io.net.Interface.Name) std.Io.net.Interface.Name.ResolveError!std.Io.net.Interface {
    if (std.mem.eql(u8, name.toSlice(), "lo")) return .{ .index = 1 };
    return error.InterfaceNotFound;
}

fn netInterfaceName(_: ?*anyopaque, interface: std.Io.net.Interface) std.Io.net.Interface.NameError!std.Io.net.Interface.Name {
    if (interface.index != 1) return error.InterfaceNotFound;
    return std.Io.net.Interface.Name.fromSlice("lo") catch return error.NameTooLong;
}

fn netLookup(userdata: ?*anyopaque, host_name: std.Io.net.HostName, resolved: *std.Io.Queue(std.Io.net.HostName.LookupResult), options: std.Io.net.HostName.LookupOptions) std.Io.net.HostName.LookupError!void {
    const self = state(userdata);
    const io = self.io();
    defer resolved.close(io);
    const address: std.Io.net.IpAddress = blk: {
        if (std.Io.net.IpAddress.parseIp4(host_name.bytes, options.port)) |parsed| {
            if (options.family == .ip6) return error.UnknownHostName;
            break :blk parsed;
        } else |_| {}
        if (std.Io.net.IpAddress.parseIp6(host_name.bytes, options.port)) |parsed| {
            if (options.family == .ip4) return error.UnknownHostName;
            break :blk parsed;
        } else |_| {}
        if (!std.ascii.eqlIgnoreCase(host_name.bytes, "localhost") and
            !std.ascii.eqlIgnoreCase(host_name.bytes, "localhost.")) return error.UnknownHostName;
        break :blk if (options.family == .ip6)
            .{ .ip6 = .loopback(options.port) }
        else
            .{ .ip4 = .loopback(options.port) };
    };
    resolved.putOne(io, .{ .address = address }) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        error.Closed => unreachable,
    };
    if (options.canonical_name_buffer) |buffer| {
        const name = if (std.ascii.endsWithIgnoreCase(host_name.bytes, ".")) host_name.bytes[0 .. host_name.bytes.len - 1] else host_name.bytes;
        @memcpy(buffer[0..name.len], name);
        resolved.putOne(io, .{ .canonical_name = std.Io.net.HostName.init(buffer[0..name.len]) catch unreachable }) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.Closed => unreachable,
        };
    }
}

fn enumerateReady(ptr: *anyopaque, list: *transition.List, allocator: std.mem.Allocator) !void {
    const self: *SimIo = @ptrCast(@alignCast(ptr));
    try self.tasks.enumerateReady(list, allocator);
    try self.network.enumerateReady(list, allocator);
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
                .external_wake => "sim-io.external_waiter_selected",
            }),
            .name = switch (execution.kind) {
                .task => if (execution.completed) "sim-io.task_completed" else "sim-io.task_parked",
                .futex_wake => "sim-io.futex_waiter_selected",
                .external_wake => "sim-io.external_waiter_selected",
            },
            .kind = .state_change,
            .actor_id = execution.task_id,
            .resource_id = execution.task_id,
        });
        return;
    }

    if (try self.network.executeReady(transition_id, sink, allocator)) {
        try self.ensureNoCapabilityViolation();
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
    return self.tasks.isQuiescent() and self.network.isQuiescent();
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
    result.dirCreateDir = dirCreateDir;
    result.dirCreateDirPath = dirCreateDirPath;
    result.dirCreateDirPathOpen = dirCreateDirPathOpen;
    result.dirOpenDir = dirOpenDir;
    result.dirStat = dirStat;
    result.dirStatFile = dirStatFile;
    result.dirAccess = dirAccess;
    result.dirCreateFile = dirCreateFile;
    result.dirCreateFileAtomic = dirCreateFileAtomic;
    result.dirOpenFile = dirOpenFile;
    result.dirClose = dirClose;
    result.dirRead = dirRead;
    result.dirRealPath = dirRealPath;
    result.dirRealPathFile = dirRealPathFile;
    result.dirDeleteFile = dirDeleteFile;
    result.dirDeleteDir = dirDeleteDir;
    result.dirRename = dirRename;
    result.dirRenamePreserve = dirRenamePreserve;
    result.dirSymLink = dirSymLink;
    result.dirReadLink = dirReadLink;
    result.dirSetOwner = dirSetOwner;
    result.dirSetFileOwner = dirSetFileOwner;
    result.dirSetPermissions = dirSetPermissions;
    result.dirSetFilePermissions = dirSetFilePermissions;
    result.dirSetTimestamps = dirSetTimestamps;
    result.dirHardLink = dirHardLink;
    result.fileStat = fileStat;
    result.fileLength = fileLength;
    result.fileClose = fileClose;
    result.fileWritePositional = fileWritePositional;
    result.fileWriteFileStreaming = fileWriteFileStreaming;
    result.fileWriteFilePositional = fileWriteFilePositional;
    result.fileReadPositional = fileReadPositional;
    result.fileSeekBy = fileSeekBy;
    result.fileSeekTo = fileSeekTo;
    result.fileSync = fileSync;
    result.fileIsTty = fileIsTty;
    result.fileEnableAnsiEscapeCodes = fileEnableAnsi;
    result.fileSupportsAnsiEscapeCodes = fileSupportsAnsi;
    result.fileSetLength = fileSetLength;
    result.fileSetOwner = fileSetOwner;
    result.fileSetPermissions = fileSetPermissions;
    result.fileSetTimestamps = fileSetTimestamps;
    result.fileLock = fileLock;
    result.fileTryLock = fileTryLock;
    result.fileUnlock = fileUnlock;
    result.fileDowngradeLock = fileDowngradeLock;
    result.fileRealPath = fileRealPath;
    result.fileHardLink = fileHardLink;
    result.fileMemoryMapCreate = memoryMapCreate;
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
    result.netListenIp = netListenIp;
    result.netAccept = netAccept;
    result.netBindIp = netBindIp;
    result.netConnectIp = netConnectIp;
    result.netListenUnix = netListenUnix;
    result.netConnectUnix = netConnectUnix;
    result.netSocketCreatePair = netSocketCreatePair;
    result.netSend = netSend;
    result.netRead = netRead;
    result.netWrite = netWrite;
    result.netWriteFile = netWriteFile;
    result.netClose = netClose;
    result.netShutdown = netShutdown;
    result.netInterfaceNameResolve = netInterfaceNameResolve;
    result.netInterfaceName = netInterfaceName;
    result.netLookup = netLookup;
    break :blk result;
};

test "SimIo capability construction fails before exposing unsupported services" {
    try SimIo.require(.of(&.{ .clock_read, .deterministic_entropy, .files, .sockets }));
    try std.testing.expectError(
        error.UnsupportedSimIoCapabilities,
        SimIo.init(.{ .required = .of(&.{.processes}) }),
    );
    try std.testing.expectEqual(
        CapabilitySet.of(&.{.processes}).bits,
        SimIo.missingCapabilities(.of(&.{ .clock_read, .files, .sockets, .processes })).bits,
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
    try sim.ensureNoCapabilityViolation();
    try std.testing.expectError(
        error.AccessDenied,
        std.Io.Dir.cwd().symLink(io, "target", "unsupported", .{}),
    );
    try std.testing.expectEqual(ViolationOperation.directory_symbolic_link, sim.firstCapabilityViolation().?.operation);
    try std.testing.expectError(error.SimIoCapabilityViolation, sim.ensureNoCapabilityViolation());

    sim.clearCapabilityViolation();
    _ = io.vtable.netSend(io.userdata, 0, &.{}, .{});
    try std.testing.expectEqual(ViolationOperation.network_send, sim.firstCapabilityViolation().?.operation);
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
            std.Io.sleep(self.io, .fromNanoseconds(1), .awake) catch unreachable;
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
        var selected = enabled.items.items[0];
        for (enabled.items.items) |candidate| {
            if (!std.mem.eql(u8, candidate.name, "sim-io.time_advance")) {
                selected = candidate;
                break;
            }
        }
        try scheduler.executeReady(selected.id, &sink, std.testing.allocator);
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

test "SimIo core modeled files execute through std.Io and survive only durable syncs" {
    var sim = try SimIo.init(.{});
    defer sim.deinit();
    const io = sim.io();

    try std.testing.expectEqual(
        std.Io.Dir.CreatePathStatus.created,
        try std.Io.Dir.cwd().createDirPathStatus(io, "state", .default_dir),
    );
    var file = try std.Io.Dir.cwd().createFile(io, "state/wal", .{ .read = true });
    try file.writeStreamingAll(io, "old");
    try file.sync(io);
    file.close(io);
    try sim.files.syncNamespace();

    file = try std.Io.Dir.cwd().openFile(io, "state/wal", .{ .mode = .read_write });
    try file.writePositionalAll(io, "new", 0);
    sim.files.faults.drop_next_sync = true;
    try file.sync(io);
    try sim.files.crash();

    file = try std.Io.Dir.cwd().openFile(io, "state/wal", .{});
    defer file.close(io);
    var bytes: [3]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 3), try file.readPositionalAll(io, &bytes, 0));
    try std.testing.expectEqualStrings("old", &bytes);
    try std.testing.expectEqual(@as(u64, 3), try file.length(io));
    try sim.ensureNoCapabilityViolation();
}

test "SimIo modeled file surface supports deterministic iteration atomic replace locks and mappings" {
    var sim = try SimIo.init(.{ .required = .of(&.{.files}) });
    defer sim.deinit();
    const io = sim.io();

    try std.Io.Dir.cwd().createDirPath(io, "tree/nested");
    var alpha = try std.Io.Dir.cwd().createFile(io, "tree/alpha", .{ .read = true });
    try alpha.writeStreamingAll(io, "alpha");
    alpha.close(io);
    var beta = try std.Io.Dir.cwd().createFile(io, "tree/beta", .{ .read = true });
    beta.close(io);

    var tree = try std.Io.Dir.cwd().openDir(io, "tree", .{ .iterate = true });
    var iterator = tree.iterate();
    try std.testing.expectEqualStrings("alpha", (try iterator.next(io)).?.name);
    try std.testing.expectEqualStrings("beta", (try iterator.next(io)).?.name);
    try std.testing.expectEqualStrings("nested", (try iterator.next(io)).?.name);
    try std.testing.expect((try iterator.next(io)) == null);
    tree.close(io);

    try std.Io.Dir.cwd().rename("tree", std.Io.Dir.cwd(), "moved", io);
    alpha = try std.Io.Dir.cwd().openFile(io, "moved/alpha", .{ .mode = .read_write });
    defer alpha.close(io);
    var path_buffer: [std.Io.Dir.max_path_bytes]u8 = undefined;
    const path_len = try alpha.realPath(io, &path_buffer);
    try std.testing.expectEqualStrings("/moved/alpha", path_buffer[0..path_len]);
    try alpha.setPermissions(io, .default_file);
    try alpha.setTimestamps(io, .{ .modify_timestamp = .now });

    try std.testing.expect(try alpha.tryLock(io, .exclusive));
    alpha.downgradeLock(io) catch unreachable;
    alpha.unlock(io);

    var map = try alpha.createMemoryMap(io, .{ .len = 5 });
    defer map.destroy(io);
    @memcpy(map.memory, "ALPHA");
    try map.write(io);
    try map.setLength(io, 6);
    map.memory[5] = '!';
    try map.write(io);
    var mapped_bytes: [6]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 6), try alpha.readPositionalAll(io, &mapped_bytes, 0));
    try std.testing.expectEqualStrings("ALPHA!", &mapped_bytes);

    var atomic = try std.Io.Dir.cwd().createFileAtomic(io, "moved/published", .{});
    defer atomic.deinit(io);
    try atomic.file.writeStreamingAll(io, "ready");
    try atomic.link(io);
    var published = try std.Io.Dir.cwd().openFile(io, "moved/published", .{});
    defer published.close(io);
    var published_bytes: [5]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 5), try published.readPositionalAll(io, &published_bytes, 0));
    try std.testing.expectEqualStrings("ready", &published_bytes);
    try sim.ensureNoCapabilityViolation();
}

test "SimIo stream sockets expose packet delivery and waiter wake choices" {
    const Shared = struct {
        io: std.Io,
        server: *std.Io.net.Server,
        client: std.Io.net.Stream,
        server_received: [4]u8 = undefined,
        client_received: [4]u8 = undefined,
        server_done: bool = false,
        client_done: bool = false,

        fn serve(self: *@This()) void {
            const stream = self.server.accept(self.io) catch unreachable;
            defer stream.close(self.io);
            var read_buffer: [16]u8 = undefined;
            var reader = stream.reader(self.io, &read_buffer);
            reader.interface.readSliceAll(&self.server_received) catch unreachable;
            var write_buffer: [16]u8 = undefined;
            var writer = stream.writer(self.io, &write_buffer);
            writer.interface.writeAll("pong") catch unreachable;
            writer.interface.flush() catch unreachable;
            stream.shutdown(self.io, .send) catch unreachable;
            self.server_done = true;
        }

        fn request(self: *@This()) void {
            defer self.client.close(self.io);
            var write_buffer: [16]u8 = undefined;
            var writer = self.client.writer(self.io, &write_buffer);
            writer.interface.writeAll("ping") catch unreachable;
            writer.interface.flush() catch unreachable;
            var read_buffer: [16]u8 = undefined;
            var reader = self.client.reader(self.io, &read_buffer);
            reader.interface.readSliceAll(&self.client_received) catch unreachable;
            self.client_done = true;
        }
    };

    var sim = try SimIo.init(.{ .required = .of(&.{.sockets}) });
    defer sim.deinit();
    const io = sim.io();
    const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(31_337) };
    var server = try address.listen(io, .{});
    defer server.deinit(io);
    const client = try address.connect(io, .{ .mode = .stream });
    var shared: Shared = .{ .io = io, .server = &server, .client = client };
    _ = io.async(Shared.serve, .{&shared});
    _ = io.async(Shared.request, .{&shared});

    const scheduler = sim.scheduler();
    var enabled: transition.List = .{};
    defer enabled.deinit(std.testing.allocator);
    var sink: event.Sink = .{};
    defer sink.deinit(std.testing.allocator);
    var saw_packet_delivery = false;
    var saw_external_wake = false;
    while (!scheduler.quiescent()) {
        enabled.items.clearRetainingCapacity();
        try scheduler.enumerateReady(&enabled, std.testing.allocator);
        try enabled.canonicalize();
        try std.testing.expect(enabled.items.items.len != 0);
        const selected = enabled.items.items[0];
        saw_packet_delivery = saw_packet_delivery or std.mem.eql(u8, selected.name, "sim-io.packet_deliver");
        saw_external_wake = saw_external_wake or std.mem.eql(u8, selected.name, "sim-io.external_wake");
        try scheduler.executeReady(selected.id, &sink, std.testing.allocator);
    }
    try std.testing.expect(shared.server_done and shared.client_done);
    try std.testing.expectEqualStrings("ping", &shared.server_received);
    try std.testing.expectEqualStrings("pong", &shared.client_received);
    try std.testing.expect(saw_packet_delivery);
    try std.testing.expect(saw_external_wake);
    try sim.ensureNoCapabilityViolation();
}
