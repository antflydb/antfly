// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2

const std = @import("std");
const builtin = @import("builtin");
const platform = @import("antfly_platform");
const fs_paths = @import("../../common/fs_paths.zig");

const Allocator = std.mem.Allocator;

const ActiveTransition = struct {
    path: []u8,
    path_key: []u8,
    id: u64,
};

pub const Manager = struct {
    allocator: Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    next_id: u64 = 1,
    active: std.ArrayListUnmanaged(ActiveTransition) = .empty,

    pub fn init(allocator: Allocator) Manager {
        return .{ .allocator = allocator };
    }

    pub fn beginExclusive(self: *Manager, path: []const u8) !ExclusiveTransition {
        const path_key = try std.fs.path.resolve(self.allocator, &.{path});
        errdefer self.allocator.free(path_key);
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);

        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();

        for (self.active.items) |entry| {
            if (std.mem.eql(u8, entry.path_key, path_key)) return error.GenerationTransitionActive;
        }

        const id = self.next_id;
        self.next_id +%= 1;
        if (self.next_id == 0) self.next_id = 1;
        try self.active.append(self.allocator, .{ .path = owned_path, .path_key = path_key, .id = id });
        return .{
            .manager = self,
            .alloc = self.allocator,
            .path = owned_path,
            .id = id,
        };
    }

    fn finishExclusive(self: *Manager, path: []const u8, id: u64) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();

        for (self.active.items, 0..) |entry, index| {
            if (entry.id != id or !std.mem.eql(u8, entry.path, path)) continue;
            const removed = self.active.swapRemove(index);
            self.allocator.free(removed.path);
            self.allocator.free(removed.path_key);
            return;
        }
        unreachable;
    }

    fn validateExclusive(self: *Manager, id: u64) !void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        for (self.active.items) |entry| {
            if (entry.id == id) return;
        }
        return error.InvalidGenerationTransition;
    }

    pub fn deinit(self: *Manager) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        std.debug.assert(self.active.items.len == 0);
        self.active.deinit(self.allocator);
        self.active = .empty;
    }
};

pub const ExclusiveTransition = struct {
    manager: *Manager,
    alloc: Allocator,
    path: []const u8,
    id: u64,
    active: bool = true,

    pub fn validate(self: *const ExclusiveTransition, path: []const u8) !void {
        if (!self.active or !std.mem.eql(u8, self.path, path)) return error.InvalidGenerationTransition;
    }

    pub fn deinit(self: *ExclusiveTransition) void {
        if (!self.active) return;
        self.manager.finishExclusive(self.path, self.id);
        self.active = false;
    }

    pub fn beginStaging(self: *ExclusiveTransition) !StagedGeneration {
        try self.validate(self.path);
        const live_path = try self.alloc.dupe(u8, self.path);
        errdefer self.alloc.free(live_path);
        const nonce = platform.time.monotonicNs();
        const staging_path = try std.fmt.allocPrint(self.alloc, "{s}.restore-stage-{x}-{x}", .{ self.path, self.id, nonce });
        errdefer self.alloc.free(staging_path);
        const retired_path = try std.fmt.allocPrint(self.alloc, "{s}.restore-retired-{x}-{x}", .{ self.path, self.id, nonce });
        errdefer self.alloc.free(retired_path);

        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        const io = io_impl.io();
        if (pathExists(io, staging_path) or pathExists(io, retired_path)) return error.GenerationStagingCollision;
        try fs_paths.createDirPathPortable(io, staging_path);
        return .{
            .alloc = self.alloc,
            .manager = self.manager,
            .transition_id = self.id,
            .live_path = live_path,
            .staging_path = staging_path,
            .retired_path = retired_path,
        };
    }
};

pub const StagedGeneration = struct {
    alloc: Allocator,
    manager: *Manager,
    transition_id: u64,
    live_path: []u8,
    staging_path: []u8,
    retired_path: []u8,
    published: bool = false,
    closed: bool = false,

    pub fn path(self: *const StagedGeneration) []const u8 {
        return self.staging_path;
    }

    pub fn validatePath(self: *const StagedGeneration, path_value: []const u8) !void {
        if (self.closed or !std.mem.eql(u8, self.staging_path, path_value)) return error.InvalidGenerationTransition;
    }

    pub fn validateLivePath(self: *const StagedGeneration, path_value: []const u8) !void {
        if (self.closed or !std.mem.eql(u8, self.live_path, path_value)) return error.InvalidGenerationTransition;
    }

    pub fn publish(self: *StagedGeneration) !void {
        if (self.closed or self.published) return error.InvalidGenerationTransition;
        try self.manager.validateExclusive(self.transition_id);

        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        const io = io_impl.io();
        if (!pathExists(io, self.staging_path)) return error.GenerationStagingMissing;
        try syncTreePortable(self.alloc, io, self.staging_path);
        const parent = std.fs.path.dirname(self.live_path) orelse if (std.fs.path.isAbsolute(self.live_path)) "/" else ".";
        try fs_paths.syncDirPortable(io, parent);

        const had_live_generation = pathExists(io, self.live_path);
        if (had_live_generation and try exchangeDirectoriesAtomic(self.alloc, self.live_path, self.staging_path)) {
            // The namespace mutation has completed. Record that before any operation
            // that can fail so deinit never mistakes the retired root for staging.
            self.published = true;
            try fs_paths.syncDirPortable(io, parent);
            std.Io.Dir.cwd().deleteTree(io, self.staging_path) catch |err| {
                std.log.warn("retired exchanged generation cleanup deferred path={s} err={s}", .{ self.staging_path, @errorName(err) });
            };
            fs_paths.syncDirPortable(io, parent) catch |err| {
                std.log.warn("retired exchanged generation parent sync deferred path={s} err={s}", .{ parent, @errorName(err) });
            };
            return;
        }
        if (had_live_generation) {
            try std.Io.Dir.rename(std.Io.Dir.cwd(), self.live_path, std.Io.Dir.cwd(), self.retired_path, io);
        }
        std.Io.Dir.rename(std.Io.Dir.cwd(), self.staging_path, std.Io.Dir.cwd(), self.live_path, io) catch |publish_err| {
            if (had_live_generation) {
                std.Io.Dir.rename(std.Io.Dir.cwd(), self.retired_path, std.Io.Dir.cwd(), self.live_path, io) catch |rollback_err| {
                    std.log.err("generation publication rollback failed live={s} retired={s} err={s}", .{
                        self.live_path,
                        self.retired_path,
                        @errorName(rollback_err),
                    });
                    return error.GenerationPublicationRollbackFailed;
                };
            }
            return publish_err;
        };

        self.published = true;
        try fs_paths.syncDirPortable(io, parent);
        if (had_live_generation) {
            std.Io.Dir.cwd().deleteTree(io, self.retired_path) catch |err| {
                std.log.warn("retired generation cleanup deferred path={s} err={s}", .{ self.retired_path, @errorName(err) });
            };
            fs_paths.syncDirPortable(io, parent) catch |err| {
                std.log.warn("retired generation parent sync deferred path={s} err={s}", .{ parent, @errorName(err) });
            };
        }
    }

    pub fn deinit(self: *StagedGeneration) void {
        if (self.closed) return;
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        if (self.published) {
            // A post-rename durability error can return before publish() reaches
            // its normal cleanup. Neither name can refer to the new live root.
            std.Io.Dir.cwd().deleteTree(io_impl.io(), self.staging_path) catch {};
            std.Io.Dir.cwd().deleteTree(io_impl.io(), self.retired_path) catch {};
        } else {
            std.Io.Dir.cwd().deleteTree(io_impl.io(), self.staging_path) catch {};
        }
        self.alloc.free(self.staging_path);
        self.alloc.free(self.retired_path);
        self.alloc.free(self.live_path);
        self.closed = true;
    }
};

fn pathExists(io: std.Io, path: []const u8) bool {
    _ = std.Io.Dir.cwd().statFile(io, path, .{}) catch return false;
    return true;
}

fn syncTreePortable(alloc: Allocator, io: std.Io, root: []const u8) !void {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return;

    var dir = if (std.fs.path.isAbsolute(root))
        try std.Io.Dir.openDirAbsolute(io, root, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true });
    defer dir.close(io);

    var iterator = dir.iterate();
    while (try iterator.next(io)) |entry| {
        const child = try std.fs.path.join(alloc, &.{ root, entry.name });
        defer alloc.free(child);
        switch (entry.kind) {
            .directory => try syncTreePortable(alloc, io, child),
            .file => {
                const file = if (std.fs.path.isAbsolute(child))
                    try std.Io.Dir.openFileAbsolute(io, child, .{})
                else
                    try std.Io.Dir.cwd().openFile(io, child, .{});
                defer file.close(io);
                try file.sync(io);
            },
            else => {},
        }
    }
    try fs_paths.syncDirPortable(io, root);
}

fn exchangeDirectoriesAtomic(alloc: Allocator, left: []const u8, right: []const u8) !bool {
    if (comptime builtin.os.tag == .macos and builtin.link_libc) {
        const Darwin = struct {
            extern "c" fn renameatx_np(
                old_dir_fd: c_int,
                old_path: [*:0]const u8,
                new_dir_fd: c_int,
                new_path: [*:0]const u8,
                flags: c_uint,
            ) c_int;
        };
        const left_z = try alloc.dupeZ(u8, left);
        defer alloc.free(left_z);
        const right_z = try alloc.dupeZ(u8, right);
        defer alloc.free(right_z);
        const rename_swap: c_uint = 0x0000_0002;
        return Darwin.renameatx_np(std.posix.AT.FDCWD, left_z.ptr, std.posix.AT.FDCWD, right_z.ptr, rename_swap) == 0;
    }
    if (comptime builtin.os.tag == .linux) {
        const linux = std.os.linux;
        const left_z = try alloc.dupeZ(u8, left);
        defer alloc.free(left_z);
        const right_z = try alloc.dupeZ(u8, right);
        defer alloc.free(right_z);
        return linux.errno(linux.renameat2(
            linux.AT.FDCWD,
            left_z.ptr,
            linux.AT.FDCWD,
            right_z.ptr,
            .{ .EXCHANGE = true },
        )) == .SUCCESS;
    }
    return false;
}

const process_manager_allocator = if (builtin.link_libc) std.heap.c_allocator else std.heap.page_allocator;
var process_manager: Manager = Manager.init(process_manager_allocator);

pub fn beginProcessExclusive(path: []const u8) !ExclusiveTransition {
    return try process_manager.beginExclusive(path);
}

test "generation lifecycle serializes the same root and validates capability target" {
    const alloc = std.testing.allocator;
    var manager = Manager.init(alloc);
    defer manager.deinit();

    var first = try manager.beginExclusive("/tmp/table-a");
    defer first.deinit();
    try first.validate("/tmp/table-a");
    try std.testing.expectError(error.InvalidGenerationTransition, first.validate("/tmp/table-b"));
    try std.testing.expectError(error.GenerationTransitionActive, manager.beginExclusive("/tmp/table-a"));
    try std.testing.expectError(error.GenerationTransitionActive, manager.beginExclusive("/tmp/../tmp/table-a"));

    var other = try manager.beginExclusive("/tmp/table-b");
    other.deinit();
}

test "staged generation cannot publish after its exclusive capability is released" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/released", .{tmp.sub_path});
    defer alloc.free(live_path);
    var manager = Manager.init(alloc);
    defer manager.deinit();
    var transition = try manager.beginExclusive(live_path);
    var staged = try transition.beginStaging();
    defer staged.deinit();

    transition.deinit();
    try std.testing.expectError(error.InvalidGenerationTransition, staged.publish());
}

test "staged generation preserves live root until publication" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/live", .{tmp.sub_path});
    defer alloc.free(live_path);
    const live_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{live_path});
    defer alloc.free(live_value_path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    try fs_paths.createDirPathPortable(io, live_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = live_value_path, .data = "old" });

    var manager = Manager.init(alloc);
    defer manager.deinit();
    var transition = try manager.beginExclusive(live_path);
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();

    const staged_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{staged.path()});
    defer alloc.free(staged_value_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = staged_value_path, .data = "new" });

    const before = try std.Io.Dir.cwd().readFileAlloc(io, live_value_path, alloc, .limited(16));
    defer alloc.free(before);
    try std.testing.expectEqualStrings("old", before);

    try staged.publish();
    const after = try std.Io.Dir.cwd().readFileAlloc(io, live_value_path, alloc, .limited(16));
    defer alloc.free(after);
    try std.testing.expectEqualStrings("new", after);
}
