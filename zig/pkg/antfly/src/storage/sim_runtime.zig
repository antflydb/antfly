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
const lsm_wal = @import("lsm_backend/wal.zig");

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
    const DurableBytes = struct {
        refs: usize = 1,
        bytes: []u8,

        fn create(alloc: Allocator, bytes: []const u8) !?*DurableBytes {
            if (bytes.len == 0) return null;
            const snapshot = try alloc.create(DurableBytes);
            errdefer alloc.destroy(snapshot);
            snapshot.* = .{ .bytes = try alloc.dupe(u8, bytes) };
            return snapshot;
        }

        fn retain(self: *DurableBytes) *DurableBytes {
            std.debug.assert(self.refs < std.math.maxInt(usize));
            self.refs += 1;
            return self;
        }

        fn release(self: *DurableBytes, alloc: Allocator) void {
            std.debug.assert(self.refs > 0);
            self.refs -= 1;
            if (self.refs != 0) return;
            alloc.free(self.bytes);
            alloc.destroy(self);
        }
    };

    const FileState = struct {
        volatile_bytes: []u8 = &.{},
        durable_bytes: ?*DurableBytes = null,

        fn deinit(self: *FileState, alloc: Allocator) void {
            if (self.volatile_bytes.len > 0) alloc.free(self.volatile_bytes);
            if (self.durable_bytes) |snapshot| snapshot.release(alloc);
            self.* = undefined;
        }
    };

    const DirtyDirectory = struct {
        paths: std.StringHashMapUnmanaged(void) = .empty,

        fn deinit(self: *DirtyDirectory, alloc: Allocator) void {
            var it = self.paths.keyIterator();
            while (it.next()) |path| alloc.free(path.*);
            self.paths.deinit(alloc);
            self.* = undefined;
        }
    };

    alloc: Allocator,
    mutex: SpinMutex = .{},
    files: std.StringHashMapUnmanaged(FileState) = .empty,
    durable_files: std.StringHashMapUnmanaged(?*DurableBytes) = .empty,
    directories: std.StringHashMapUnmanaged(void) = .empty,
    durable_directories: std.StringHashMapUnmanaged(void) = .empty,
    dirty_directories: std.StringHashMapUnmanaged(DirtyDirectory) = .empty,
    tick: u64 = 1,
    fail_next_read: bool = false,
    fail_next_write: bool = false,
    fail_next_sync: bool = false,
    fail_next_truncate: bool = false,
    fail_next_rename: bool = false,
    fail_next_delete: bool = false,
    drop_next_sync: bool = false,
    partial_next_write_bytes: ?usize = null,
    partial_write_faults_consumed: u64 = 0,
    dropped_syncs_consumed: u64 = 0,
    device_full_faults_consumed: u64 = 0,
    fail_next_read_path_contains: ?[]u8 = null,
    fail_next_write_path_contains: ?[]u8 = null,
    fail_next_sync_path_contains: ?[]u8 = null,
    fail_next_truncate_path_contains: ?[]u8 = null,
    fail_next_rename_path_contains: ?[]u8 = null,
    fail_next_delete_path_contains: ?[]u8 = null,
    capacity_bytes: ?usize = null,

    pub fn init(alloc: Allocator) ModeledDevice {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *ModeledDevice) void {
        inline for (&.{
            &self.fail_next_read_path_contains,
            &self.fail_next_write_path_contains,
            &self.fail_next_sync_path_contains,
            &self.fail_next_truncate_path_contains,
            &self.fail_next_rename_path_contains,
            &self.fail_next_delete_path_contains,
        }) |slot| if (slot.*) |needle| self.alloc.free(needle);
        var it = self.files.iterator();
        while (it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.alloc);
        }
        self.files.deinit(self.alloc);
        var durable_it = self.durable_files.iterator();
        while (durable_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            if (entry.value_ptr.*) |snapshot| snapshot.release(self.alloc);
        }
        self.durable_files.deinit(self.alloc);
        var dir_it = self.directories.keyIterator();
        while (dir_it.next()) |path| self.alloc.free(path.*);
        self.directories.deinit(self.alloc);
        var durable_dir_it = self.durable_directories.keyIterator();
        while (durable_dir_it.next()) |path| self.alloc.free(path.*);
        self.durable_directories.deinit(self.alloc);
        var dirty_it = self.dirty_directories.iterator();
        while (dirty_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.alloc);
        }
        self.dirty_directories.deinit(self.alloc);
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

    pub fn injectReadFailure(self: *ModeledDevice) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.fail_next_read = true;
    }

    pub fn injectReadFailureForPathContains(self: *ModeledDevice, needle: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.replaceFaultNeedle(&self.fail_next_read_path_contains, needle);
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

    pub fn injectTruncateFailure(self: *ModeledDevice) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.fail_next_truncate = true;
    }

    pub fn injectTruncateFailureForPathContains(self: *ModeledDevice, needle: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.replaceFaultNeedle(&self.fail_next_truncate_path_contains, needle);
    }

    pub fn injectRenameFailure(self: *ModeledDevice) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.fail_next_rename = true;
    }

    pub fn injectRenameFailureForPathContains(self: *ModeledDevice, needle: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.replaceFaultNeedle(&self.fail_next_rename_path_contains, needle);
    }

    pub fn injectDeleteFailure(self: *ModeledDevice) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.fail_next_delete = true;
    }

    /// The next write persists at most `bytes_written` volatile bytes and then
    /// reports an error. A later sync may make that torn write durable; a crash
    /// before sync discards it with the rest of volatile state.
    pub fn injectPartialWrite(self: *ModeledDevice, bytes_written: usize) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.partial_next_write_bytes = bytes_written;
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

    pub fn partialWriteFaultsConsumed(self: *ModeledDevice) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.partial_write_faults_consumed;
    }

    pub fn droppedSyncsConsumed(self: *ModeledDevice) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.dropped_syncs_consumed;
    }

    pub fn setCapacityBytes(self: *ModeledDevice, capacity: ?usize) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.capacity_bytes = capacity;
    }

    pub fn usedBytes(self: *ModeledDevice) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.usedBytesLocked();
    }

    pub fn deviceFullFaultsConsumed(self: *ModeledDevice) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.device_full_faults_consumed;
    }

    pub fn corruptVolatileByte(self: *ModeledDevice, path: []const u8, offset: usize, xor_mask: u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (xor_mask == 0) return error.InvalidCorruptionMask;
        const file = self.files.getPtr(path) orelse return error.FileNotFound;
        if (offset >= file.volatile_bytes.len) return error.CorruptionOffsetOutOfBounds;
        file.volatile_bytes[offset] ^= xor_mask;
    }

    pub fn corruptDurableByte(self: *ModeledDevice, path: []const u8, offset: usize, xor_mask: u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (xor_mask == 0) return error.InvalidCorruptionMask;
        const file = self.files.getPtr(path) orelse return error.FileNotFound;
        const durable = file.durable_bytes orelse return error.FileNotDurable;
        if (offset >= durable.bytes.len) return error.CorruptionOffsetOutOfBounds;
        durable.bytes[offset] ^= xor_mask;
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
        if (self.consumeReadFaultLocked(path)) return error.InjectedReadFault;
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
        const end = offset + bytes.len;
        const old_len = if (self.files.get(path)) |file| file.volatile_bytes.len else 0;
        try self.ensureCapacityLocked(path, @max(old_len, end));
        const file = try self.ensureFile(path);
        if (self.takePartialWriteLimit()) |limit| {
            const written = @min(limit, bytes.len);
            const partial_end = offset + written;
            try resizeBuffer(self.alloc, &file.volatile_bytes, @max(file.volatile_bytes.len, partial_end));
            @memcpy(file.volatile_bytes[offset..partial_end], bytes[0..written]);
            return error.InjectedPartialWriteFault;
        }
        try resizeBuffer(self.alloc, &file.volatile_bytes, @max(file.volatile_bytes.len, end));
        @memcpy(file.volatile_bytes[offset..end], bytes);
    }

    fn sync(ptr: *anyopaque, path: []const u8) !void {
        const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.syncContentsLocked(path);
        try self.syncDirectoryLocked(parentPath(path));
    }

    fn syncContentsLocked(self: *ModeledDevice, path: []const u8) !void {
        if (self.fail_next_sync or self.consumeFaultNeedle(&self.fail_next_sync_path_contains, path)) {
            self.fail_next_sync = false;
            return error.InjectedSyncFault;
        }
        const file = self.files.getPtr(path) orelse return error.FileNotFound;
        if (self.drop_next_sync) {
            self.drop_next_sync = false;
            self.dropped_syncs_consumed +|= 1;
            return;
        }
        const durable = try DurableBytes.create(self.alloc, file.volatile_bytes);
        if (file.durable_bytes) |old| old.release(self.alloc);
        file.durable_bytes = durable;
        if (self.durable_files.getPtr(path)) |durable_file| {
            const published = if (durable) |snapshot| snapshot.retain() else null;
            if (durable_file.*) |old| old.release(self.alloc);
            durable_file.* = published;
        }
    }

    fn syncDirectoryLocked(self: *ModeledDevice, parent: []const u8) !void {
        const dirty = self.dirty_directories.getPtr(parent) orelse return;
        var dirty_it = dirty.paths.keyIterator();
        while (dirty_it.next()) |path| {
            if (self.files.get(path.*)) |file| {
                try self.publishDurableFileLocked(path.*, file.durable_bytes);
            } else if (self.directories.contains(path.*)) {
                try self.publishDurableDirectoryLocked(path.*);
            } else {
                self.removeDurableFileLocked(path.*);
                try self.removeDurableDirectoryTreeLocked(path.*);
            }
        }

        const removed = self.dirty_directories.fetchRemove(parent).?;
        self.alloc.free(removed.key);
        var removed_dirty = removed.value;
        removed_dirty.deinit(self.alloc);
    }

    fn publishDurableFileLocked(self: *ModeledDevice, path: []const u8, snapshot: ?*DurableBytes) !void {
        const owned_path = try self.alloc.dupe(u8, path);
        errdefer self.alloc.free(owned_path);
        const gop = try self.durable_files.getOrPut(self.alloc, owned_path);
        if (gop.found_existing) {
            self.alloc.free(owned_path);
            if (gop.value_ptr.*) |old| old.release(self.alloc);
        } else {
            gop.key_ptr.* = owned_path;
        }
        gop.value_ptr.* = if (snapshot) |bytes| bytes.retain() else null;
    }

    fn removeDurableFileLocked(self: *ModeledDevice, path: []const u8) void {
        const removed = self.durable_files.fetchRemove(path) orelse return;
        self.alloc.free(removed.key);
        if (removed.value) |snapshot| snapshot.release(self.alloc);
    }

    fn removeDurableDirectoryTreeLocked(self: *ModeledDevice, path: []const u8) !void {
        var doomed = std.ArrayListUnmanaged([]const u8).empty;
        defer doomed.deinit(self.alloc);
        var file_it = self.durable_files.keyIterator();
        while (file_it.next()) |file_path| {
            if (pathContains(path, file_path.*)) try doomed.append(self.alloc, file_path.*);
        }
        for (doomed.items) |file_path| self.removeDurableFileLocked(file_path);

        doomed.clearRetainingCapacity();
        var dir_it = self.durable_directories.keyIterator();
        while (dir_it.next()) |dir_path| {
            if (pathContains(path, dir_path.*)) try doomed.append(self.alloc, dir_path.*);
        }
        for (doomed.items) |dir_path| {
            const removed = self.durable_directories.fetchRemove(dir_path) orelse continue;
            self.alloc.free(removed.key);
        }
    }

    fn publishDurableDirectoryLocked(self: *ModeledDevice, path: []const u8) !void {
        if (self.durable_directories.contains(path)) return;
        const owned_path = try self.alloc.dupe(u8, path);
        errdefer self.alloc.free(owned_path);
        try self.durable_directories.put(self.alloc, owned_path, {});
    }

    fn ensureDirectoryLocked(self: *ModeledDevice, path: []const u8) !void {
        if (path.len == 0 or std.mem.eql(u8, path, "/") or std.mem.eql(u8, path, ".")) return;
        if (self.directories.contains(path)) return;
        const parent = parentPath(path);
        const top_level = std.mem.eql(u8, parent, "/") or std.mem.eql(u8, parent, ".");
        if (!top_level) try self.markNamespaceDirtyLocked(path);
        const owned_path = try self.alloc.dupe(u8, path);
        errdefer self.alloc.free(owned_path);
        if (top_level and !self.durable_directories.contains(path)) {
            const owned_durable_path = try self.alloc.dupe(u8, path);
            errdefer self.alloc.free(owned_durable_path);
            try self.directories.ensureUnusedCapacity(self.alloc, 1);
            try self.durable_directories.ensureUnusedCapacity(self.alloc, 1);
            self.directories.putAssumeCapacityNoClobber(owned_path, {});
            self.durable_directories.putAssumeCapacityNoClobber(owned_durable_path, {});
            return;
        }
        try self.directories.putNoClobber(self.alloc, owned_path, {});
        // A top-level test/provisioned root is outside the modeled database
        // lifecycle. Nested directories must cross an explicit parent sync.
        if (top_level) std.debug.assert(self.durable_directories.contains(path));
    }

    fn truncate(ptr: *anyopaque, path: []const u8, len: usize) !void {
        const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.fail_next_truncate or self.consumeFaultNeedle(&self.fail_next_truncate_path_contains, path)) {
            self.fail_next_truncate = false;
            return error.InjectedTruncateFault;
        }
        try self.ensureCapacityLocked(path, len);
        const file = try self.ensureFile(path);
        try resizeBuffer(self.alloc, &file.volatile_bytes, len);
    }

    fn rename(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
        const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.fail_next_rename or
            self.consumeFaultNeedle(&self.fail_next_rename_path_contains, old_path) or
            self.consumeFaultNeedle(&self.fail_next_rename_path_contains, new_path))
        {
            self.fail_next_rename = false;
            return error.InjectedRenameFault;
        }
        if (!self.files.contains(old_path)) return error.FileNotFound;
        try self.markNamespaceDirtyLocked(old_path);
        try self.markNamespaceDirtyLocked(new_path);
        const owned_new_path = try self.alloc.dupe(u8, new_path);
        errdefer self.alloc.free(owned_new_path);
        try self.files.ensureUnusedCapacity(self.alloc, 1);
        const removed = self.files.fetchRemove(old_path) orelse return error.FileNotFound;
        self.alloc.free(removed.key);
        if (self.files.fetchRemove(new_path)) |existing| {
            self.alloc.free(existing.key);
            var existing_file = existing.value;
            existing_file.deinit(self.alloc);
        }
        self.files.putAssumeCapacityNoClobber(owned_new_path, removed.value);
    }

    fn remove(ptr: *anyopaque, path: []const u8) !void {
        const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.fail_next_delete or self.consumeFaultNeedle(&self.fail_next_delete_path_contains, path)) {
            self.fail_next_delete = false;
            return error.InjectedDeleteFault;
        }
        if (!self.files.contains(path)) return;
        try self.markNamespaceDirtyLocked(path);
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

        var restored: std.StringHashMapUnmanaged(FileState) = .empty;
        errdefer {
            var restored_it = restored.iterator();
            while (restored_it.next()) |entry| {
                self.alloc.free(entry.key_ptr.*);
                entry.value_ptr.deinit(self.alloc);
            }
            restored.deinit(self.alloc);
        }
        var durable_it = self.durable_files.iterator();
        while (durable_it.next()) |entry| {
            if (!self.durableAncestorsPresentLocked(entry.key_ptr.*)) continue;
            try cloneDurableFileInto(self.alloc, &restored, entry.key_ptr.*, entry.value_ptr.*);
        }

        var restored_directories: std.StringHashMapUnmanaged(void) = .empty;
        errdefer {
            var restored_dir_it = restored_directories.keyIterator();
            while (restored_dir_it.next()) |path| self.alloc.free(path.*);
            restored_directories.deinit(self.alloc);
        }
        var durable_dir_it = self.durable_directories.keyIterator();
        while (durable_dir_it.next()) |path| {
            if (!self.durableAncestorsPresentLocked(path.*)) continue;
            const owned = try self.alloc.dupe(u8, path.*);
            errdefer self.alloc.free(owned);
            try restored_directories.put(self.alloc, owned, {});
        }

        var volatile_it = self.files.iterator();
        while (volatile_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.alloc);
        }
        self.files.deinit(self.alloc);
        self.files = restored;
        var volatile_dir_it = self.directories.keyIterator();
        while (volatile_dir_it.next()) |path| self.alloc.free(path.*);
        self.directories.deinit(self.alloc);
        self.directories = restored_directories;
        self.clearDirtyDirectoriesLocked();
    }

    fn ensureFile(self: *ModeledDevice, path: []const u8) !*FileState {
        if (self.files.getPtr(path)) |file| return file;
        try self.markNamespaceDirtyLocked(path);
        const owned_path = try self.alloc.dupe(u8, path);
        errdefer self.alloc.free(owned_path);
        try self.files.putNoClobber(self.alloc, owned_path, .{});
        return self.files.getPtr(path).?;
    }

    fn markNamespaceDirtyLocked(self: *ModeledDevice, path: []const u8) !void {
        const parent = parentPath(path);
        if (self.dirty_directories.getPtr(parent)) |dirty| {
            if (dirty.paths.contains(path)) return;
            const owned_path = try self.alloc.dupe(u8, path);
            dirty.paths.putNoClobber(self.alloc, owned_path, {}) catch |err| {
                self.alloc.free(owned_path);
                return err;
            };
            return;
        }

        var dirty: DirtyDirectory = .{};
        errdefer dirty.deinit(self.alloc);
        const owned_path = try self.alloc.dupe(u8, path);
        dirty.paths.putNoClobber(self.alloc, owned_path, {}) catch |err| {
            self.alloc.free(owned_path);
            return err;
        };
        const owned_parent = try self.alloc.dupe(u8, parent);
        self.dirty_directories.putNoClobber(self.alloc, owned_parent, dirty) catch |err| {
            self.alloc.free(owned_parent);
            return err;
        };
    }

    fn durableAncestorsPresentLocked(self: *const ModeledDevice, path: []const u8) bool {
        var parent = parentPath(path);
        while (!std.mem.eql(u8, parent, "/") and !std.mem.eql(u8, parent, ".")) {
            if (!self.durable_directories.contains(parent)) return false;
            const next = parentPath(parent);
            if (std.mem.eql(u8, next, parent)) break;
            parent = next;
        }
        return true;
    }

    fn clearDirtyDirectoriesLocked(self: *ModeledDevice) void {
        var dirty_it = self.dirty_directories.iterator();
        while (dirty_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.alloc);
        }
        self.dirty_directories.clearRetainingCapacity();
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

    fn takePartialWriteLimit(self: *ModeledDevice) ?usize {
        const limit = self.partial_next_write_bytes;
        self.partial_next_write_bytes = null;
        if (limit != null) self.partial_write_faults_consumed +|= 1;
        return limit;
    }

    fn consumeReadFaultLocked(self: *ModeledDevice, path: []const u8) bool {
        if (!self.fail_next_read and !self.consumeFaultNeedle(&self.fail_next_read_path_contains, path)) return false;
        self.fail_next_read = false;
        return true;
    }

    fn usedBytesLocked(self: *const ModeledDevice) usize {
        var total: usize = 0;
        var it = self.files.valueIterator();
        while (it.next()) |file| total +|= file.volatile_bytes.len;
        return total;
    }

    fn ensureCapacityLocked(self: *ModeledDevice, path: []const u8, new_len: usize) !void {
        const capacity = self.capacity_bytes orelse return;
        const old_len = if (self.files.get(path)) |file| file.volatile_bytes.len else 0;
        const used = self.usedBytesLocked();
        const proposed = used -| old_len +| new_len;
        if (proposed <= capacity) return;
        self.device_full_faults_consumed +|= 1;
        return error.InjectedDeviceFull;
    }
};

const modeled_storage_vtable: lsm_storage.Storage.VTable = .{
    .create_dir_path = modeledCreateDirPath,
    .read_file_alloc = modeledReadFileAlloc,
    .read_file_range_alloc = modeledReadFileRangeAlloc,
    .file_size = modeledFileSize,
    .read_file_trailer_alloc = modeledReadFileTrailerAlloc,
    .write_file_absolute = modeledWriteFileAbsolute,
    .append_file_absolute = modeledAppendFileAbsolute,
    .sync_contents_absolute = modeledSyncFileContentsAbsolute,
    .sync_parent_absolute = modeledSyncParentAbsolute,
    .rename_absolute = modeledRenameAbsolute,
    .delete_file_absolute = modeledDeleteFileAbsolute,
    .delete_tree = modeledDeleteTree,
    .now_ns = modeledNowNs,
    .rename_is_atomic = true,
};

fn modeledCreateDirPath(ptr: *anyopaque, path: []const u8) !void {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    self.mutex.lock();
    defer self.mutex.unlock();
    if (path.len == 0) return;

    var i: usize = if (std.fs.path.isAbsolute(path)) 1 else 0;
    while (i < path.len) : (i += 1) {
        if (path[i] != '/') continue;
        if (i > 0) try self.ensureDirectoryLocked(path[0..i]);
    }
    var end = path.len;
    while (end > 1 and path[end - 1] == '/') end -= 1;
    try self.ensureDirectoryLocked(path[0..end]);
}

fn modeledReadFileAlloc(ptr: *anyopaque, alloc: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    self.mutex.lock();
    defer self.mutex.unlock();
    if (self.consumeReadFaultLocked(path)) return error.InjectedReadFault;
    const file = self.files.get(path) orelse return error.FileNotFound;
    if (file.volatile_bytes.len > max_bytes) return error.FileTooBig;
    return try alloc.dupe(u8, file.volatile_bytes);
}

fn modeledReadFileRangeAlloc(ptr: *anyopaque, alloc: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    self.mutex.lock();
    defer self.mutex.unlock();
    if (self.consumeReadFaultLocked(path)) return error.InjectedReadFault;
    const file = self.files.get(path) orelse return error.FileNotFound;
    const start: usize = @intCast(offset);
    if (start > file.volatile_bytes.len or file.volatile_bytes.len - start < len) return error.EndOfStream;
    return try alloc.dupe(u8, file.volatile_bytes[start .. start + len]);
}

fn modeledFileSize(ptr: *anyopaque, path: []const u8) !u64 {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    return @intCast(try self.fileSize(path));
}

fn modeledReadFileTrailerAlloc(ptr: *anyopaque, alloc: Allocator, path: []const u8, len: usize) !lsm_storage.FileTrailer {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    self.mutex.lock();
    defer self.mutex.unlock();
    if (self.consumeReadFaultLocked(path)) return error.InjectedReadFault;
    const file = self.files.get(path) orelse return error.FileNotFound;
    if (file.volatile_bytes.len < len) return error.EndOfStream;
    return .{
        .bytes = try alloc.dupe(u8, file.volatile_bytes[file.volatile_bytes.len - len ..]),
        .file_size = file.volatile_bytes.len,
    };
}

fn modeledWriteFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    self.mutex.lock();
    defer self.mutex.unlock();
    if (self.fail_next_write or self.consumeFaultNeedle(&self.fail_next_write_path_contains, path)) {
        self.fail_next_write = false;
        return error.InjectedWriteFault;
    }
    try self.ensureCapacityLocked(path, contents.len);
    const file = try self.ensureFile(path);
    if (self.takePartialWriteLimit()) |limit| {
        const written = @min(limit, contents.len);
        try resizeBuffer(self.alloc, &file.volatile_bytes, written);
        @memcpy(file.volatile_bytes[0..written], contents[0..written]);
        return error.InjectedPartialWriteFault;
    }
    try resizeBuffer(self.alloc, &file.volatile_bytes, contents.len);
    @memcpy(file.volatile_bytes, contents);
}

fn modeledAppendFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8, should_sync: bool) !void {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    self.mutex.lock();
    defer self.mutex.unlock();
    if (self.fail_next_write or self.consumeFaultNeedle(&self.fail_next_write_path_contains, path)) {
        self.fail_next_write = false;
        return error.InjectedWriteFault;
    }
    const old_len = if (self.files.get(path)) |file| file.volatile_bytes.len else 0;
    try self.ensureCapacityLocked(path, old_len + contents.len);
    const file = try self.ensureFile(path);
    if (self.takePartialWriteLimit()) |limit| {
        const written = @min(limit, contents.len);
        try resizeBuffer(self.alloc, &file.volatile_bytes, old_len + written);
        @memcpy(file.volatile_bytes[old_len..], contents[0..written]);
        return error.InjectedPartialWriteFault;
    }
    try resizeBuffer(self.alloc, &file.volatile_bytes, old_len + contents.len);
    @memcpy(file.volatile_bytes[old_len..], contents);
    if (should_sync) try self.syncContentsLocked(path);
}

fn modeledSyncFileContentsAbsolute(ptr: *anyopaque, path: []const u8) !void {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    self.mutex.lock();
    defer self.mutex.unlock();
    try self.syncContentsLocked(path);
}

fn modeledSyncParentAbsolute(ptr: *anyopaque, path: []const u8) !void {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    self.mutex.lock();
    defer self.mutex.unlock();
    if (self.fail_next_sync or self.consumeFaultNeedle(&self.fail_next_sync_path_contains, path)) {
        self.fail_next_sync = false;
        return error.InjectedSyncFault;
    }
    try self.syncDirectoryLocked(parentPath(path));
}

fn modeledRenameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    try self.device().rename(old_path, new_path);
}

fn modeledDeleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    self.mutex.lock();
    defer self.mutex.unlock();
    if (self.fail_next_delete or self.consumeFaultNeedle(&self.fail_next_delete_path_contains, path)) {
        self.fail_next_delete = false;
        return error.InjectedDeleteFault;
    }
    if (!self.files.contains(path)) return error.FileNotFound;
    try self.markNamespaceDirtyLocked(path);
    const removed = self.files.fetchRemove(path) orelse return error.FileNotFound;
    self.alloc.free(removed.key);
    var file = removed.value;
    file.deinit(self.alloc);
}

fn modeledDeleteTree(ptr: *anyopaque, path: []const u8) !void {
    const self: *ModeledDevice = @ptrCast(@alignCast(ptr));
    self.mutex.lock();
    defer self.mutex.unlock();
    if (self.fail_next_delete or self.consumeFaultNeedle(&self.fail_next_delete_path_contains, path)) {
        self.fail_next_delete = false;
        return error.InjectedDeleteFault;
    }
    var doomed = std.ArrayListUnmanaged([]const u8).empty;
    defer doomed.deinit(self.alloc);

    // Directory-tree durability is represented by the root namespace entry.
    // Journaling every descendant would require syncing every removed child
    // directory and turns a tree deletion into quadratic simulator work.
    try self.markNamespaceDirtyLocked(path);
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

    doomed.clearRetainingCapacity();
    var dir_it = self.directories.keyIterator();
    while (dir_it.next()) |dir_path| {
        if (!pathContains(path, dir_path.*)) continue;
        try doomed.append(self.alloc, dir_path.*);
    }
    for (doomed.items) |dir_path| {
        const removed = self.directories.fetchRemove(dir_path) orelse continue;
        self.alloc.free(removed.key);
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
    if (std.mem.eql(u8, prefix, "/")) return true;
    return path[prefix.len] == '/';
}

fn parentPath(path: []const u8) []const u8 {
    return std.fs.path.dirname(path) orelse if (std.fs.path.isAbsolute(path)) "/" else ".";
}

fn cloneDurableFileInto(
    alloc: Allocator,
    files: *std.StringHashMapUnmanaged(ModeledDevice.FileState),
    path: []const u8,
    snapshot: ?*ModeledDevice.DurableBytes,
) !void {
    const bytes = if (snapshot) |durable| durable.bytes else &.{};
    const owned_path = try alloc.dupe(u8, path);
    errdefer alloc.free(owned_path);
    const volatile_bytes = try alloc.dupe(u8, bytes);
    errdefer alloc.free(volatile_bytes);
    const retained = if (snapshot) |durable| durable.retain() else null;
    errdefer if (retained) |durable| durable.release(alloc);
    try files.putNoClobber(alloc, owned_path, .{
        .volatile_bytes = volatile_bytes,
        .durable_bytes = retained,
    });
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

test "modeled device exposes torn writes and acknowledged dropped syncs" {
    var device_model = ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();
    const device = device_model.device();

    try device.write("wal", 0, "abc");
    try device.sync("wal");
    device_model.injectPartialWrite(2);
    try std.testing.expectError(error.InjectedPartialWriteFault, device.write("wal", 3, "WXYZ"));
    try device.sync("wal");
    try device.crash();
    const torn = try device.readAlloc(std.testing.allocator, "wal", 0, 16);
    defer std.testing.allocator.free(torn);
    try std.testing.expectEqualStrings("abcWX", torn);

    try device.write("wal", 5, "dirty");
    device_model.dropNextSync();
    try device.sync("wal");
    try device.crash();
    const dropped = try device.readAlloc(std.testing.allocator, "wal", 0, 16);
    defer std.testing.allocator.free(dropped);
    try std.testing.expectEqualStrings("abcWX", dropped);
}

test "modeled device exposes typed operation capacity and corruption faults" {
    var device_model = ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();
    const device = device_model.device();

    try device_model.storage().createDirPath("/root");
    try device.write("/root/value", 0, "abcdef");
    try device.sync("/root/value");

    device_model.injectReadFailure();
    try std.testing.expectError(error.InjectedReadFault, device.readAlloc(std.testing.allocator, "/root/value", 0, 6));
    device_model.injectTruncateFailure();
    try std.testing.expectError(error.InjectedTruncateFault, device.truncate("/root/value", 2));
    device_model.injectRenameFailure();
    try std.testing.expectError(error.InjectedRenameFault, device.rename("/root/value", "/root/renamed"));
    device_model.injectDeleteFailure();
    try std.testing.expectError(error.InjectedDeleteFault, device.remove("/root/value"));

    try std.testing.expectEqual(@as(usize, 6), device_model.usedBytes());
    device_model.setCapacityBytes(6);
    try std.testing.expectError(error.InjectedDeviceFull, device.write("/root/value", 6, "g"));
    try std.testing.expectEqual(@as(u64, 1), device_model.deviceFullFaultsConsumed());
    try device.write("/root/value", 1, "B");
    device_model.setCapacityBytes(null);

    try device_model.corruptVolatileByte("/root/value", 2, 0x20);
    const volatile_corrupt = try device.readAlloc(std.testing.allocator, "/root/value", 0, 6);
    defer std.testing.allocator.free(volatile_corrupt);
    try std.testing.expectEqualStrings("aBCdef", volatile_corrupt);
    try device.crash();
    const restored = try device.readAlloc(std.testing.allocator, "/root/value", 0, 6);
    defer std.testing.allocator.free(restored);
    try std.testing.expectEqualStrings("abcdef", restored);

    try device_model.corruptDurableByte("/root/value", 0, 0x20);
    try device.crash();
    const durable_corrupt = try device.readAlloc(std.testing.allocator, "/root/value", 0, 6);
    defer std.testing.allocator.free(durable_corrupt);
    try std.testing.expectEqualStrings("Abcdef", durable_corrupt);
}

test "modeled storage consumes path-targeted read rename and delete faults" {
    var device_model = ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();
    const storage = device_model.storage();

    try storage.createDirPath("/root");
    try storage.writeFileAbsolute("/root/a", "abc");
    try device_model.injectReadFailureForPathContains("/root/a");
    try std.testing.expectError(error.InjectedReadFault, storage.readFileAlloc(std.testing.allocator, "/root/a", 16));
    try device_model.injectRenameFailureForPathContains("/root/a");
    try std.testing.expectError(error.InjectedRenameFault, storage.renameAbsolute("/root/a", "/root/b"));
    try device_model.injectDeleteFailureForPathContains("/root");
    try std.testing.expectError(error.InjectedDeleteFault, storage.deleteTree("/root"));
}

test "modeled storage requires a directory sync for a newly created file" {
    var device_model = ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();
    const storage = device_model.storage();

    try storage.createDirPath("/wal");
    try storage.syncParentAbsolute("/wal");
    try storage.appendFileAbsolute(std.testing.allocator, "/wal/1.log", "first", true);
    try device_model.device().crash();
    try std.testing.expectError(
        error.FileNotFound,
        storage.readFileAlloc(std.testing.allocator, "/wal/1.log", 16),
    );

    try storage.appendFileAbsolute(std.testing.allocator, "/wal/1.log", "first", true);
    device_model.injectSyncFailure();
    try std.testing.expectError(
        error.InjectedSyncFault,
        storage.syncParentAbsolute("/wal/1.log"),
    );
    try device_model.device().crash();
    try std.testing.expectError(
        error.FileNotFound,
        storage.readFileAlloc(std.testing.allocator, "/wal/1.log", 16),
    );

    try storage.appendFileAbsolute(std.testing.allocator, "/wal/1.log", "first", true);
    try storage.syncParentAbsolute("/wal/1.log");
    try device_model.device().crash();
    const bytes = try storage.readFileAlloc(std.testing.allocator, "/wal/1.log", 16);
    defer std.testing.allocator.free(bytes);
    try std.testing.expectEqualStrings("first", bytes);
}

test "modeled storage drops files below an unsynced nested directory" {
    var device_model = ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();
    const storage = device_model.storage();

    try storage.createDirPath("/root/wal/replay");
    try storage.syncParentAbsolute("/root/wal");
    // replay itself has not been published in /root/wal.
    try storage.appendFileAbsolute(std.testing.allocator, "/root/wal/replay/1.log", "row", true);
    try storage.syncParentAbsolute("/root/wal/replay/1.log");
    try device_model.device().crash();
    try std.testing.expectError(
        error.FileNotFound,
        storage.readFileAlloc(std.testing.allocator, "/root/wal/replay/1.log", 16),
    );
}

test "modeled replay WAL survives initial publication and segment rotation" {
    var device_model = ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();
    const storage = device_model.storage();
    const root = "/replay-crash";
    try storage.createDirPath(root);

    _ = try lsm_wal.appendReplay(storage, std.testing.allocator, root, 1, "one", true, .{ .segment_bytes = 28 });
    _ = try lsm_wal.appendReplay(storage, std.testing.allocator, root, 2, "two", true, .{ .segment_bytes = 28 });
    try device_model.device().crash();

    const entries = try lsm_wal.iterateReplayFrom(storage, std.testing.allocator, root, 1);
    defer {
        for (entries) |*entry| entry.deinit(std.testing.allocator);
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqualStrings("one", entries[0].payload);
    try std.testing.expectEqualStrings("two", entries[1].payload);
}

test "modeled storage rolls back an unsynced rename namespace" {
    var device_model = ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();
    const storage = device_model.storage();

    try storage.createDirPath("/root");
    try storage.writeFileAbsolute("/root/old", "stable");
    try storage.syncFileContentsAbsolute("/root/old");
    try storage.syncParentAbsolute("/root/old");
    try storage.renameAbsolute("/root/old", "/root/new");
    try device_model.device().crash();

    const old = try storage.readFileAlloc(std.testing.allocator, "/root/old", 16);
    defer std.testing.allocator.free(old);
    try std.testing.expectEqualStrings("stable", old);
    try std.testing.expectError(
        error.FileNotFound,
        storage.readFileAlloc(std.testing.allocator, "/root/new", 16),
    );
}

test "modeled storage requires a parent sync for durable deletion" {
    var device_model = ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();
    const storage = device_model.storage();

    try storage.createDirPath("/root");
    try storage.writeFileAbsolute("/root/stable", "value");
    try storage.syncFileContentsAbsolute("/root/stable");
    try storage.syncParentAbsolute("/root/stable");

    try storage.deleteFileAbsolute("/root/stable");
    try device_model.device().crash();
    const resurrected = try storage.readFileAlloc(std.testing.allocator, "/root/stable", 16);
    defer std.testing.allocator.free(resurrected);
    try std.testing.expectEqualStrings("value", resurrected);

    try storage.deleteFileAbsolute("/root/stable");
    try storage.syncParentAbsolute("/root/stable");
    try device_model.device().crash();
    try std.testing.expectError(
        error.FileNotFound,
        storage.readFileAlloc(std.testing.allocator, "/root/stable", 16),
    );
}

test "modeled storage publishes tree deletion at the removed root" {
    var device_model = ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();
    const storage = device_model.storage();

    try storage.createDirPath("/root/tree/nested");
    try storage.syncParentAbsolute("/root/tree");
    try storage.syncParentAbsolute("/root/tree/nested");
    try storage.writeFileAbsolute("/root/tree/nested/value", "durable");
    try storage.syncFileContentsAbsolute("/root/tree/nested/value");
    try storage.syncParentAbsolute("/root/tree/nested/value");

    try storage.deleteTree("/root/tree");
    try storage.syncParentAbsolute("/root/tree");
    try device_model.device().crash();
    try std.testing.expectError(
        error.FileNotFound,
        storage.readFileAlloc(std.testing.allocator, "/root/tree/nested/value", 16),
    );
}

test "modeled device exposes lsm storage view" {
    var device_model = ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();
    const storage = device_model.storage();

    try storage.createDirPath("/root");
    try storage.writeFileAbsolute("/root/a", "abc");
    try storage.syncFileContentsAbsolute("/root/a");
    try storage.syncParentAbsolute("/root/a");

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
