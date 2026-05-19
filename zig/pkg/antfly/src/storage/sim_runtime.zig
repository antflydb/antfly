// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const builtin = @import("builtin");
const lsm_storage = @import("lsm_backend/storage_io.zig");

const Allocator = std.mem.Allocator;

const SpinMutex = struct {
    inner: std.atomic.Mutex = .unlocked,

    fn lock(self: *SpinMutex) void {
        while (!self.inner.tryLock()) {
            std.atomic.spinLoopHint();
        }
    }

    fn unlock(self: *SpinMutex) void {
        self.inner.unlock();
    }
};

pub const Clock = struct {
    ctx: ?*anyopaque = null,
    now_ns_fn: *const fn (?*anyopaque) u64,
    sleep_ns_fn: *const fn (?*anyopaque, u64) void,

    pub fn nowNs(self: Clock) u64 {
        return self.now_ns_fn(self.ctx);
    }

    pub fn sleepNs(self: Clock, ns: u64) void {
        self.sleep_ns_fn(self.ctx, ns);
    }
};

pub const real_clock: Clock = .{
    .now_ns_fn = realNowNs,
    .sleep_ns_fn = realSleepNs,
};

pub const CompletionScheduler = struct {
    ctx: ?*anyopaque = null,
    wait_ns_fn: *const fn (?*anyopaque, u64) anyerror!void,

    pub fn waitNs(self: CompletionScheduler, ns: u64) !void {
        try self.wait_ns_fn(self.ctx, ns);
    }
};

pub const real_completion_scheduler: CompletionScheduler = .{
    .wait_ns_fn = realCompletionWaitNs,
};

pub const Runtime = struct {
    const Event = struct {
        due_ns: u64,
        sequence: u64,
        ctx: *anyopaque,
        callback: *const fn (*anyopaque) anyerror!void,
    };

    alloc: Allocator,
    now_ns: u64 = 0,
    next_sequence: u64 = 0,
    events: std.ArrayListUnmanaged(Event) = .empty,

    pub fn init(alloc: Allocator) Runtime {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Runtime) void {
        self.events.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn clock(self: *Runtime) Clock {
        return .{
            .ctx = self,
            .now_ns_fn = runtimeNowNs,
            .sleep_ns_fn = runtimeSleepNs,
        };
    }

    pub fn completionScheduler(self: *Runtime) CompletionScheduler {
        return .{
            .ctx = self,
            .wait_ns_fn = runtimeCompletionWaitNs,
        };
    }

    pub fn schedule(
        self: *Runtime,
        delay_ns: u64,
        ctx: *anyopaque,
        callback: *const fn (*anyopaque) anyerror!void,
    ) !void {
        try self.events.append(self.alloc, .{
            .due_ns = self.now_ns +| delay_ns,
            .sequence = self.next_sequence,
            .ctx = ctx,
            .callback = callback,
        });
        self.next_sequence +|= 1;
    }

    pub fn advanceNs(self: *Runtime, delta_ns: u64) !void {
        self.now_ns +|= delta_ns;
        try self.runDue();
    }

    pub fn runUntilIdle(self: *Runtime) !void {
        while (self.events.items.len > 0) {
            const next_index = self.nextEventIndex(null) orelse return;
            const due_ns = self.events.items[next_index].due_ns;
            if (due_ns > self.now_ns) self.now_ns = due_ns;
            try self.runDue();
        }
    }

    fn runDue(self: *Runtime) !void {
        while (self.nextEventIndex(self.now_ns)) |index| {
            const event = self.events.orderedRemove(index);
            try event.callback(event.ctx);
        }
    }

    fn runUntilCompletion(self: *Runtime, completion: *Completion) !void {
        while (!completion.done) {
            const next_index = self.nextEventIndex(null) orelse return error.MissingScheduledCompletion;
            const due_ns = self.events.items[next_index].due_ns;
            if (due_ns > self.now_ns) self.now_ns = due_ns;
            try self.runDue();
        }
    }

    fn nextEventIndex(self: *Runtime, max_due_ns: ?u64) ?usize {
        var best_index: ?usize = null;
        for (self.events.items, 0..) |event, index| {
            if (max_due_ns) |max_due| {
                if (event.due_ns > max_due) continue;
            }
            const best = best_index orelse {
                best_index = index;
                continue;
            };
            const best_event = self.events.items[best];
            if (event.due_ns < best_event.due_ns or
                (event.due_ns == best_event.due_ns and event.sequence < best_event.sequence))
            {
                best_index = index;
            }
        }
        return best_index;
    }
};

const Completion = struct {
    done: bool = false,

    fn mark(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.done = true;
    }
};

pub const DeviceList = struct {
    names: []const []const u8,

    pub fn deinit(self: *DeviceList, alloc: Allocator) void {
        for (self.names) |name| alloc.free(name);
        alloc.free(self.names);
        self.* = undefined;
    }
};

pub const Device = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        read_alloc: *const fn (ptr: *anyopaque, alloc: Allocator, path: []const u8, offset: usize, len: usize) anyerror![]u8,
        write: *const fn (ptr: *anyopaque, path: []const u8, offset: usize, bytes: []const u8) anyerror!void,
        sync: *const fn (ptr: *anyopaque, path: []const u8) anyerror!void,
        truncate: *const fn (ptr: *anyopaque, path: []const u8, len: usize) anyerror!void,
        rename: *const fn (ptr: *anyopaque, old_path: []const u8, new_path: []const u8) anyerror!void,
        remove: *const fn (ptr: *anyopaque, path: []const u8) anyerror!void,
        list_alloc: *const fn (ptr: *anyopaque, alloc: Allocator, prefix: []const u8) anyerror!DeviceList,
        crash: *const fn (ptr: *anyopaque) anyerror!void,
    };

    pub fn readAlloc(self: Device, alloc: Allocator, path: []const u8, offset: usize, len: usize) ![]u8 {
        return try self.vtable.read_alloc(self.ptr, alloc, path, offset, len);
    }

    pub fn write(self: Device, path: []const u8, offset: usize, bytes: []const u8) !void {
        try self.vtable.write(self.ptr, path, offset, bytes);
    }

    pub fn sync(self: Device, path: []const u8) !void {
        try self.vtable.sync(self.ptr, path);
    }

    pub fn truncate(self: Device, path: []const u8, len: usize) !void {
        try self.vtable.truncate(self.ptr, path, len);
    }

    pub fn rename(self: Device, old_path: []const u8, new_path: []const u8) !void {
        try self.vtable.rename(self.ptr, old_path, new_path);
    }

    pub fn remove(self: Device, path: []const u8) !void {
        try self.vtable.remove(self.ptr, path);
    }

    pub fn listAlloc(self: Device, alloc: Allocator, prefix: []const u8) !DeviceList {
        return try self.vtable.list_alloc(self.ptr, alloc, prefix);
    }

    pub fn crash(self: Device) !void {
        try self.vtable.crash(self.ptr);
    }
};

pub const ModeledDevice = struct {
    const FileState = struct {
        volatile_bytes: []u8 = &.{},
        durable_bytes: []u8 = &.{},

        fn deinit(self: *FileState, alloc: Allocator) void {
            if (self.volatile_bytes.len > 0) alloc.free(self.volatile_bytes);
            if (self.durable_bytes.len > 0) alloc.free(self.durable_bytes);
            self.* = undefined;
        }
    };

    alloc: Allocator,
    mutex: SpinMutex = .{},
    files: std.StringHashMapUnmanaged(FileState) = .empty,
    tick: u64 = 1,
    fail_next_write: bool = false,
    fail_next_sync: bool = false,
    drop_next_sync: bool = false,
    fail_next_write_path_contains: ?[]u8 = null,
    fail_next_sync_path_contains: ?[]u8 = null,
    fail_next_delete_path_contains: ?[]u8 = null,

    pub fn init(alloc: Allocator) ModeledDevice {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *ModeledDevice) void {
        if (self.fail_next_write_path_contains) |needle| self.alloc.free(needle);
        if (self.fail_next_sync_path_contains) |needle| self.alloc.free(needle);
        if (self.fail_next_delete_path_contains) |needle| self.alloc.free(needle);
        var it = self.files.iterator();
        while (it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.alloc);
        }
        self.files.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn device(self: *ModeledDevice) Device {
        return .{
            .ptr = self,
            .vtable = &.{
                .read_alloc = readAlloc,
                .write = write,
                .sync = sync,
                .truncate = truncate,
                .rename = rename,
                .remove = remove,
                .list_alloc = listAlloc,
                .crash = crash,
            },
        };
    }

    pub fn storage(self: *ModeledDevice) lsm_storage.Storage {
        return .{
            .ptr = self,
            .vtable = &modeled_storage_vtable,
        };
    }

    pub fn injectWriteFailure(self: *ModeledDevice) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.fail_next_write = true;
    }

    pub fn injectWriteFailureForPathContains(self: *ModeledDevice, needle: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.replaceFaultNeedle(&self.fail_next_write_path_contains, needle);
    }

    pub fn injectSyncFailure(self: *ModeledDevice) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.fail_next_sync = true;
    }

    pub fn injectSyncFailureForPathContains(self: *ModeledDevice, needle: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.replaceFaultNeedle(&self.fail_next_sync_path_contains, needle);
    }

    pub fn injectDeleteFailureForPathContains(self: *ModeledDevice, needle: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.replaceFaultNeedle(&self.fail_next_delete_path_contains, needle);
    }

    pub fn dropNextSync(self: *ModeledDevice) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.drop_next_sync = true;
    }

    pub fn fileSize(self: *ModeledDevice, path: []const u8) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        const file = self.files.get(path) orelse return error.FileNotFound;
        return file.volatile_bytes.len;
    }

    fn readAlloc(ptr: *anyopaque, alloc: Allocator, path: []const u8, offset: usize, len: usize) ![]u8 {
        const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
        self.mutex.lock();
        defer self.mutex.unlock();
        const file = self.files.get(path) orelse return error.FileNotFound;
        if (offset > file.volatile_bytes.len) return error.EndOfFile;
        const end = @min(file.volatile_bytes.len, offset + len);
        return try alloc.dupe(u8, file.volatile_bytes[offset..end]);
    }

    fn write(ptr: *anyopaque, path: []const u8, offset: usize, bytes: []const u8) !void {
        const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.fail_next_write or self.consumeFaultNeedle(&self.fail_next_write_path_contains, path)) {
            self.fail_next_write = false;
            return error.InjectedWriteFault;
        }
        const file = try self.ensureFile(path);
        const end = offset + bytes.len;
        try resizeBuffer(self.alloc, &file.volatile_bytes, end);
        @memcpy(file.volatile_bytes[offset..end], bytes);
    }

    fn sync(ptr: *anyopaque, path: []const u8) !void {
        const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.fail_next_sync or self.consumeFaultNeedle(&self.fail_next_sync_path_contains, path)) {
            self.fail_next_sync = false;
            return error.InjectedSyncFault;
        }
        const file = self.files.getPtr(path) orelse return error.FileNotFound;
        if (self.drop_next_sync) {
            self.drop_next_sync = false;
            return;
        }
        const durable = try self.alloc.dupe(u8, file.volatile_bytes);
        if (file.durable_bytes.len > 0) self.alloc.free(file.durable_bytes);
        file.durable_bytes = durable;
    }

    fn truncate(ptr: *anyopaque, path: []const u8, len: usize) !void {
        const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
        self.mutex.lock();
        defer self.mutex.unlock();
        const file = try self.ensureFile(path);
        try resizeBuffer(self.alloc, &file.volatile_bytes, len);
    }

    fn rename(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
        const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
        self.mutex.lock();
        defer self.mutex.unlock();
        const removed = self.files.fetchRemove(old_path) orelse return error.FileNotFound;
        self.alloc.free(removed.key);
        if (self.files.fetchRemove(new_path)) |existing| {
            self.alloc.free(existing.key);
            var existing_file = existing.value;
            existing_file.deinit(self.alloc);
        }
        const owned_new_path = try self.alloc.dupe(u8, new_path);
        errdefer self.alloc.free(owned_new_path);
        try self.files.put(self.alloc, owned_new_path, removed.value);
    }

    fn remove(ptr: *anyopaque, path: []const u8) !void {
        const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.consumeFaultNeedle(&self.fail_next_delete_path_contains, path)) return error.InjectedDeleteFault;
        const removed = self.files.fetchRemove(path) orelse return;
        self.alloc.free(removed.key);
        var file = removed.value;
        file.deinit(self.alloc);
    }

    fn listAlloc(ptr: *anyopaque, alloc: Allocator, prefix: []const u8) !DeviceList {
        const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
        self.mutex.lock();
        defer self.mutex.unlock();
        var names = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (names.items) |name| alloc.free(name);
            names.deinit(alloc);
        }

        var it = self.files.keyIterator();
        while (it.next()) |path| {
            if (!std.mem.startsWith(u8, path.*, prefix)) continue;
            try names.append(alloc, try alloc.dupe(u8, path.*));
        }
        return .{ .names = try names.toOwnedSlice(alloc) };
    }

    fn crash(ptr: *anyopaque) !void {
        const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
        self.mutex.lock();
        defer self.mutex.unlock();
        var it = self.files.valueIterator();
        while (it.next()) |file| {
            const volatile_bytes = try self.alloc.dupe(u8, file.durable_bytes);
            if (file.volatile_bytes.len > 0) self.alloc.free(file.volatile_bytes);
            file.volatile_bytes = volatile_bytes;
        }
    }

    fn ensureFile(self: *ModeledDevice, path: []const u8) !*FileState {
        const owned_path = try self.alloc.dupe(u8, path);
        errdefer self.alloc.free(owned_path);
        const gop = try self.files.getOrPut(self.alloc, owned_path);
        if (gop.found_existing) {
            self.alloc.free(owned_path);
        } else {
            gop.key_ptr.* = owned_path;
            gop.value_ptr.* = .{};
        }
        return gop.value_ptr;
    }

    fn replaceFaultNeedle(self: *ModeledDevice, slot: *?[]u8, needle: []const u8) !void {
        if (slot.*) |old| self.alloc.free(old);
        slot.* = try self.alloc.dupe(u8, needle);
    }

    fn consumeFaultNeedle(self: *ModeledDevice, slot: *?[]u8, path: []const u8) bool {
        const needle = slot.* orelse return false;
        if (std.mem.indexOf(u8, path, needle) == null) return false;
        self.alloc.free(needle);
        slot.* = null;
        return true;
    }
};

const modeled_storage_vtable: lsm_storage.Storage.VTable = .{
    .create_dir_path = modeledCreateDirPath,
    .read_file_alloc = modeledReadFileAlloc,
    .read_file_range_alloc = modeledReadFileRangeAlloc,
    .file_size = modeledFileSize,
    .read_file_trailer_alloc = modeledReadFileTrailerAlloc,
    .write_file_absolute = modeledWriteFileAbsolute,
    .rename_absolute = modeledRenameAbsolute,
    .delete_file_absolute = modeledDeleteFileAbsolute,
    .delete_tree = modeledDeleteTree,
    .now_ns = modeledNowNs,
};

fn modeledCreateDirPath(_: *anyopaque, _: []const u8) !void {}

fn modeledReadFileAlloc(ptr: *anyopaque, alloc: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    self.mutex.lock();
    defer self.mutex.unlock();
    const file = self.files.get(path) orelse return error.FileNotFound;
    if (file.volatile_bytes.len > max_bytes) return error.FileTooBig;
    return try alloc.dupe(u8, file.volatile_bytes);
}

fn modeledReadFileRangeAlloc(ptr: *anyopaque, alloc: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    self.mutex.lock();
    defer self.mutex.unlock();
    const file = self.files.get(path) orelse return error.FileNotFound;
    const start: usize = @intCast(offset);
    if (start > file.volatile_bytes.len or file.volatile_bytes.len - start < len) return error.EndOfStream;
    return try alloc.dupe(u8, file.volatile_bytes[start .. start + len]);
}

fn modeledFileSize(ptr: *anyopaque, path: []const u8) !u64 {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    return @intCast(try self.fileSize(path));
}

fn modeledReadFileTrailerAlloc(ptr: *anyopaque, alloc: Allocator, path: []const u8, len: usize) ![]u8 {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    self.mutex.lock();
    defer self.mutex.unlock();
    const file = self.files.get(path) orelse return error.FileNotFound;
    if (file.volatile_bytes.len < len) return error.EndOfStream;
    return try alloc.dupe(u8, file.volatile_bytes[file.volatile_bytes.len - len ..]);
}

fn modeledWriteFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    const device = self.device();
    try device.truncate(path, 0);
    try device.write(path, 0, contents);
    try device.sync(path);
}

fn modeledRenameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    try self.device().rename(old_path, new_path);
}

fn modeledDeleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    self.mutex.lock();
    defer self.mutex.unlock();
    if (self.consumeFaultNeedle(&self.fail_next_delete_path_contains, path)) return error.InjectedDeleteFault;
    const removed = self.files.fetchRemove(path) orelse return error.FileNotFound;
    self.alloc.free(removed.key);
    var file = removed.value;
    file.deinit(self.alloc);
}

fn modeledDeleteTree(ptr: *anyopaque, path: []const u8) !void {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    self.mutex.lock();
    defer self.mutex.unlock();
    var doomed = std.ArrayListUnmanaged([]const u8).empty;
    defer doomed.deinit(self.alloc);

    var it = self.files.keyIterator();
    while (it.next()) |file_path| {
        if (!pathContains(path, file_path.*)) continue;
        try doomed.append(self.alloc, file_path.*);
    }

    for (doomed.items) |file_path| {
        const removed = self.files.fetchRemove(file_path) orelse continue;
        self.alloc.free(removed.key);
        var file = removed.value;
        file.deinit(self.alloc);
    }
}

fn modeledNowNs(ptr: *anyopaque) u64 {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    self.mutex.lock();
    defer self.mutex.unlock();
    const current = self.tick;
    self.tick += 1;
    return current;
}

fn pathContains(prefix: []const u8, path: []const u8) bool {
    if (!std.mem.startsWith(u8, path, prefix)) return false;
    if (path.len == prefix.len) return true;
    return path[prefix.len] == '/';
}

fn resizeBuffer(alloc: Allocator, buffer: *[]u8, new_len: usize) !void {
    if (buffer.*.len == new_len) return;
    const resized = try alloc.alloc(u8, new_len);
    const copy_len = @min(buffer.*.len, new_len);
    if (copy_len > 0) @memcpy(resized[0..copy_len], buffer.*[0..copy_len]);
    if (new_len > copy_len) @memset(resized[copy_len..], 0);
    if (buffer.*.len > 0) alloc.free(buffer.*);
    buffer.* = resized;
}

fn realNowNs(_: ?*anyopaque) u64 {
    if (comptime builtin.os.tag == .freestanding) return 0;

    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => {},
        else => unreachable,
    }
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

fn realSleepNs(_: ?*anyopaque, ns: u64) void {
    if (ns == 0) return;
    if (comptime builtin.os.tag == .freestanding) return;

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Clock.Duration.sleep(.{
        .clock = .awake,
        .raw = .fromNanoseconds(@intCast(ns)),
    }, io_impl.io()) catch {};
}

fn realCompletionWaitNs(_: ?*anyopaque, ns: u64) !void {
    realSleepNs(null, ns);
}

fn runtimeNowNs(ctx: ?*anyopaque) u64 {
    const runtime: *Runtime = @ptrCast(@alignCast(ctx.?));
    return runtime.now_ns;
}

fn runtimeSleepNs(ctx: ?*anyopaque, ns: u64) void {
    const runtime: *Runtime = @ptrCast(@alignCast(ctx.?));
    runtime.advanceNs(ns) catch @panic("storage sim scheduled event failed");
}

fn runtimeCompletionWaitNs(ctx: ?*anyopaque, ns: u64) !void {
    const runtime: *Runtime = @ptrCast(@alignCast(ctx.?));
    var completion = Completion{};
    try runtime.schedule(ns, &completion, Completion.mark);
    try runtime.runUntilCompletion(&completion);
}

test "storage sim runtime advances virtual time and scheduled events" {
    const Counter = struct {
        value: u32 = 0,

        fn inc(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.value += 1;
        }
    };

    var runtime = Runtime.init(std.testing.allocator);
    defer runtime.deinit();
    var counter = Counter{};
    try runtime.schedule(20, &counter, Counter.inc);
    try runtime.schedule(10, &counter, Counter.inc);

    try std.testing.expectEqual(@as(u64, 0), runtime.clock().nowNs());
    runtime.clock().sleepNs(10);
    try std.testing.expectEqual(@as(u32, 1), counter.value);
    runtime.clock().sleepNs(10);
    try std.testing.expectEqual(@as(u32, 2), counter.value);
}

test "storage sim completion scheduler advances to scheduled completion" {
    var runtime = Runtime.init(std.testing.allocator);
    defer runtime.deinit();

    var counter = struct {
        value: u32 = 0,

        fn inc(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.value += 1;
        }
    }{};

    try runtime.schedule(5, &counter, @TypeOf(counter).inc);
    try runtime.completionScheduler().waitNs(10);
    try std.testing.expectEqual(@as(u64, 10), runtime.clock().nowNs());
    try std.testing.expectEqual(@as(u32, 1), counter.value);
}

test "modeled device preserves only synced bytes across crash" {
    var device_model = ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();
    const device = device_model.device();

    try device.write("wal", 0, "abc");
    try device.sync("wal");
    try device.write("wal", 3, "dirty");
    try device.crash();

    const bytes = try device.readAlloc(std.testing.allocator, "wal", 0, 16);
    defer std.testing.allocator.free(bytes);
    try std.testing.expectEqualStrings("abc", bytes);
}

test "modeled device exposes lsm storage view" {
    var device_model = ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();
    const storage = device_model.storage();

    try storage.createDirPath("/root");
    try storage.writeFileAbsolute("/root/a", "abc");

    const read = try storage.readFileAlloc(std.testing.allocator, "/root/a", 16);
    defer std.testing.allocator.free(read);
    try std.testing.expectEqualStrings("abc", read);
    try std.testing.expectEqual(@as(u64, 3), try storage.fileSize("/root/a"));

    try device_model.device().write("/root/a", 3, "dirty");
    try device_model.device().crash();

    const after_crash = try storage.readFileAlloc(std.testing.allocator, "/root/a", 16);
    defer std.testing.allocator.free(after_crash);
    try std.testing.expectEqualStrings("abc", after_crash);
}
