// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Registered in-process programs for VoprIo.
//!
//! This is deliberately not an executable emulator. Only names explicitly
//! registered by the harness can spawn. Each child is a VoprIo fiber and may
//! cooperate with pause and deterministic CPU quotas through `Context`.

const std = @import("std");
const ids = @import("id.zig");
const vopr_io_net = @import("vopr_io_net.zig");

pub const Start = *const fn (*Context, []const []const u8) u8;

pub const Registration = struct {
    name: []const u8,
    userdata: ?*anyopaque = null,
    start: Start,
};

pub const Config = struct {
    registrations: []const Registration = &.{},
    max_processes: usize = 256,
    cpu_budget_per_process: u64 = std.math.maxInt(u64),
};

pub const Context = struct {
    io: std.Io,
    userdata: ?*anyopaque,
    table: *Table,
    process: *Process,

    pub fn id(self: *const Context) u32 {
        return self.process.pid;
    }

    pub fn checkpoint(self: *Context, work_units: u64) std.Io.Cancelable!void {
        self.process.cpu_used = std.math.add(u64, self.process.cpu_used, work_units) catch
            return error.Canceled;
        if (self.process.cpu_used > self.process.cpu_budget) {
            self.process.resource_exhausted = true;
            return error.Canceled;
        }
        while (self.process.paused and !self.process.kill_requested) {
            self.table.wait_port.?.wait(self.process.resumeResource()) catch return error.Canceled;
        }
        try std.Io.checkCancel(self.io);
    }
};

const Process = struct {
    pid: u32,
    logical_id: ids.StableId,
    registration: Registration,
    argv_storage: []u8,
    argv: [][]const u8,
    future: std.Io.Future(u8) = undefined,
    future_initialized: bool = false,
    term: ?std.process.Child.Term = null,
    paused: bool = false,
    kill_requested: bool = false,
    resource_exhausted: bool = false,
    cpu_used: u64 = 0,
    cpu_budget: u64,

    fn resumeResource(self: *const Process) ids.StableId {
        return ids.derive("sim-io.process-resume", self.logical_id, 0);
    }
};

pub const Table = struct {
    allocator: std.mem.Allocator,
    config: Config,
    wait_port: ?vopr_io_net.WaitPort = null,
    next_pid: u32 = 10_000,
    next_sequence: u64 = 1,
    processes: std.AutoHashMapUnmanaged(u32, *Process) = .empty,
    process_order: std.ArrayListUnmanaged(*Process) = .empty,

    pub fn init(allocator: std.mem.Allocator, config: Config) !Table {
        if (config.max_processes == 0) return error.InvalidVoprIoProcessLimit;
        for (config.registrations, 0..) |registration, index| {
            if (registration.name.len == 0) return error.InvalidVoprIoProgramName;
            for (config.registrations[0..index]) |earlier| {
                if (std.mem.eql(u8, earlier.name, registration.name)) return error.DuplicateVoprIoProgram;
            }
        }
        return .{ .allocator = allocator, .config = config };
    }

    pub fn deinit(self: *Table) void {
        self.processes.deinit(self.allocator);
        for (self.process_order.items) |process| self.destroyProcess(process);
        self.process_order.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn bindWaitPort(self: *Table, wait_port: vopr_io_net.WaitPort) void {
        self.wait_port = wait_port;
    }

    pub fn spawn(self: *Table, io: std.Io, options: std.process.SpawnOptions) std.process.SpawnError!std.process.Child {
        if (options.argv.len == 0) return error.FileNotFound;
        if (options.stdin == .pipe or options.stdout == .pipe or options.stderr == .pipe)
            return error.OperationUnsupported;
        const registration = self.findRegistration(options.argv[0]) orelse return error.FileNotFound;
        if (self.processes.count() >= self.config.max_processes) return error.ResourceLimitReached;

        const process = self.allocator.create(Process) catch return error.OutOfMemory;
        errdefer self.allocator.destroy(process);
        const copied = copyArgv(self.allocator, options.argv) catch return error.OutOfMemory;
        errdefer {
            self.allocator.free(copied.argv);
            self.allocator.free(copied.storage);
        }
        const pid = self.allocatePid() catch return error.ResourceLimitReached;
        const sequence = self.next_sequence;
        self.next_sequence +|= 1;
        process.* = .{
            .pid = pid,
            .logical_id = ids.derive("sim-io.process", process_table_id, sequence),
            .registration = registration,
            .argv_storage = copied.storage,
            .argv = copied.argv,
            .paused = options.start_suspended,
            .cpu_budget = self.config.cpu_budget_per_process,
        };
        self.process_order.append(self.allocator, process) catch return error.OutOfMemory;
        errdefer _ = self.process_order.pop();
        self.processes.put(self.allocator, pid, process) catch return error.OutOfMemory;
        errdefer _ = self.processes.remove(pid);
        process.future = io.async(run, .{ self, process, io });
        process.future_initialized = true;
        return childForPid(pid, options.request_resource_usage_statistics);
    }

    pub fn wait(self: *Table, io: std.Io, child: *std.process.Child) std.process.Child.WaitError!std.process.Child.Term {
        const pid = pidFromChild(child) orelse return error.AccessDenied;
        const process = self.processes.get(pid) orelse return error.AccessDenied;
        if (process.term == null) {
            const exit_code = process.future.await(io);
            process.future_initialized = false;
            process.term = if (process.kill_requested)
                .{ .signal = .KILL }
            else if (process.resource_exhausted)
                .{ .signal = .XCPU }
            else
                .{ .exited = exit_code };
        }
        child.id = null;
        child.stdin = null;
        child.stdout = null;
        child.stderr = null;
        return process.term.?;
    }

    pub fn kill(self: *Table, io: std.Io, child: *std.process.Child) bool {
        const pid = pidFromChild(child) orelse return false;
        const process = self.processes.get(pid) orelse return false;
        process.kill_requested = true;
        process.paused = false;
        self.wait_port.?.wake(process.resumeResource(), 1) catch {};
        if (process.term == null and process.future_initialized) {
            _ = process.future.cancel(io);
            process.future_initialized = false;
            process.term = .{ .signal = .KILL };
        }
        child.id = null;
        child.stdin = null;
        child.stdout = null;
        child.stderr = null;
        return true;
    }

    pub fn pauseProcess(self: *Table, pid: u32) bool {
        const process = self.processes.get(pid) orelse return false;
        if (process.term != null) return false;
        process.paused = true;
        return true;
    }

    pub fn resumeProcess(self: *Table, pid: u32) bool {
        const process = self.processes.get(pid) orelse return false;
        if (process.term != null) return false;
        process.paused = false;
        self.wait_port.?.wake(process.resumeResource(), 1) catch {};
        return true;
    }

    pub fn setCpuBudget(self: *Table, pid: u32, budget: u64) bool {
        const process = self.processes.get(pid) orelse return false;
        process.cpu_budget = budget;
        return true;
    }

    pub fn childPid(child: *const std.process.Child) ?u32 {
        return pidFromChild(child);
    }

    fn run(self: *Table, process: *Process, io: std.Io) u8 {
        var context: Context = .{
            .io = io,
            .userdata = process.registration.userdata,
            .table = self,
            .process = process,
        };
        context.checkpoint(0) catch return 128;
        return process.registration.start(&context, process.argv);
    }

    fn findRegistration(self: *const Table, name: []const u8) ?Registration {
        for (self.config.registrations) |registration| {
            if (std.mem.eql(u8, registration.name, name)) return registration;
        }
        return null;
    }

    fn allocatePid(self: *Table) !u32 {
        var attempts: usize = 0;
        while (attempts < self.config.max_processes + 1) : (attempts += 1) {
            const candidate = self.next_pid;
            self.next_pid +%= 1;
            if (!self.processes.contains(candidate)) return candidate;
        }
        return error.ResourceLimitReached;
    }

    fn destroyProcess(self: *Table, process: *Process) void {
        self.allocator.free(process.argv);
        self.allocator.free(process.argv_storage);
        self.allocator.destroy(process);
    }

    const process_table_id = ids.stable("sim-io", "process-table-v1");
};

fn copyArgv(allocator: std.mem.Allocator, argv: []const []const u8) !struct { storage: []u8, argv: [][]const u8 } {
    var total: usize = 0;
    for (argv) |arg| total = try std.math.add(usize, total, arg.len);
    const storage = try allocator.alloc(u8, total);
    errdefer allocator.free(storage);
    const result = try allocator.alloc([]const u8, argv.len);
    errdefer allocator.free(result);
    var offset: usize = 0;
    for (argv, 0..) |arg, index| {
        @memcpy(storage[offset..][0..arg.len], arg);
        result[index] = storage[offset..][0..arg.len];
        offset += arg.len;
    }
    return .{ .storage = storage, .argv = result };
}

fn childForPid(pid: u32, request_resource_usage_statistics: bool) std.process.Child {
    return .{
        .id = @intCast(pid),
        .thread_handle = {},
        .stdin = null,
        .stdout = null,
        .stderr = null,
        .request_resource_usage_statistics = request_resource_usage_statistics,
    };
}

fn pidFromChild(child: *const std.process.Child) ?u32 {
    return if (child.id) |value| @intCast(value) else null;
}
