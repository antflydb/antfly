// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Deterministic in-memory filesystem for VoprIo.
//!
//! Handles are virtual integers and never reach the host. Namespace durability
//! and file-data durability are separate: file sync persists contents, while
//! syncNamespace persists create/delete/rename publication. crash closes every
//! handle and reconstructs volatile state from those durable images.

const std = @import("std");

pub const Config = struct {
    max_open_handles: usize = 1024,
    capacity_bytes: u64 = std.math.maxInt(u64),
};

pub const Faults = struct {
    fail_next_read: bool = false,
    fail_next_write: bool = false,
    fail_next_sync: bool = false,
    fail_next_rename: bool = false,
    drop_next_sync: bool = false,
    torn_next_sync_limit: ?usize = null,
    partial_write_limit: ?usize = null,
    corrupt_next_read: ?CorruptRange = null,
};

pub const CorruptRange = struct {
    offset: u64,
    length: u64,
    xor_mask: u8,
};

const PersistentCorruption = struct {
    inode: u64,
    range: CorruptRange,
};

const Node = struct {
    inode: u64,
    kind: std.Io.File.Kind,
    path: []u8,
    durable_path: []u8,
    exists: bool,
    durable_exists: bool,
    data: std.ArrayListUnmanaged(u8) = .empty,
    durable_data: std.ArrayListUnmanaged(u8) = .empty,
    permissions: std.Io.File.Permissions,
    durable_permissions: std.Io.File.Permissions,
    atime_ns: i96 = 0,
    mtime_ns: i96 = 0,
    ctime_ns: i96 = 0,
    durable_atime_ns: i96 = 0,
    durable_mtime_ns: i96 = 0,
    durable_ctime_ns: i96 = 0,
    lock: std.Io.File.Lock = .none,
    shared_lock_count: usize = 0,
};

const OpenHandle = struct {
    node: *Node,
    cursor: u64 = 0,
    readable: bool,
    writable: bool,
    directory: bool,
    owned_lock: std.Io.File.Lock = .none,
};

pub const FileSystem = struct {
    allocator: std.mem.Allocator,
    config: Config,
    next_inode: u64 = 2,
    next_handle: std.Io.File.Handle = 0x4000_0000,
    next_atomic: u64 = 0xa710_0000_0000_0000,
    nodes: std.ArrayListUnmanaged(*Node) = .empty,
    paths: std.StringHashMapUnmanaged(*Node) = .empty,
    handles: std.AutoHashMapUnmanaged(std.Io.File.Handle, OpenHandle) = .empty,
    persistent_corruptions: std.ArrayListUnmanaged(PersistentCorruption) = .empty,
    faults: Faults = .{},

    pub fn init(allocator: std.mem.Allocator, config: Config) !FileSystem {
        if (config.max_open_handles == 0) return error.InvalidVoprIoFileHandleLimit;
        var self: FileSystem = .{ .allocator = allocator, .config = config };
        errdefer self.deinit();
        const root = try self.createNode("/", .directory, .default_dir, 0);
        root.durable_exists = true;
        try root.durable_data.resize(allocator, 0);
        return self;
    }

    pub fn deinit(self: *FileSystem) void {
        self.handles.deinit(self.allocator);
        self.persistent_corruptions.deinit(self.allocator);
        self.paths.deinit(self.allocator);
        for (self.nodes.items) |node| {
            self.allocator.free(node.path);
            self.allocator.free(node.durable_path);
            node.data.deinit(self.allocator);
            node.durable_data.deinit(self.allocator);
            self.allocator.destroy(node);
        }
        self.nodes.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn createDir(self: *FileSystem, dir: std.Io.Dir, sub_path: []const u8, permissions: std.Io.Dir.Permissions, now_ns: i96) std.Io.Dir.CreateDirError!void {
        const path = self.resolvePath(dir, sub_path, false) catch |err| return mapCreateDirError(err);
        defer self.allocator.free(path);
        if (self.paths.contains(path)) return error.PathAlreadyExists;
        self.requireParentDirectory(path) catch |err| return mapCreateDirError(err);
        _ = self.createNode(path, .directory, permissions, now_ns) catch return error.SystemResources;
    }

    pub fn createDirPath(self: *FileSystem, dir: std.Io.Dir, sub_path: []const u8, permissions: std.Io.Dir.Permissions, now_ns: i96) std.Io.Dir.CreateDirPathError!std.Io.Dir.CreatePathStatus {
        const path = self.resolvePath(dir, sub_path, false) catch |err| return mapCreateDirPathError(err);
        defer self.allocator.free(path);
        if (self.paths.get(path)) |node| {
            if (node.kind != .directory) return error.NotDir;
            return .existed;
        }
        self.ensureDirectoryPath(path, permissions, now_ns) catch |err| return mapCreateDirPathError(err);
        return .created;
    }

    pub fn openDir(self: *FileSystem, dir: std.Io.Dir, sub_path: []const u8, _: std.Io.Dir.OpenOptions) std.Io.Dir.OpenError!std.Io.Dir {
        const path = self.resolvePath(dir, sub_path, false) catch |err| return mapOpenDirPathError(err);
        defer self.allocator.free(path);
        const node = self.paths.get(path) orelse return error.FileNotFound;
        if (node.kind != .directory) return error.NotDir;
        const handle = self.openHandle(node, true, false, true) catch |err| switch (err) {
            error.ProcessFdQuotaExceeded => return error.ProcessFdQuotaExceeded,
            else => return error.SystemResources,
        };
        return .{ .handle = handle };
    }

    pub fn statDir(self: *FileSystem, dir: std.Io.Dir) std.Io.Dir.StatError!std.Io.Dir.Stat {
        const node = self.nodeForDir(dir) catch return error.AccessDenied;
        return nodeStat(node);
    }

    pub fn statFileAt(self: *FileSystem, dir: std.Io.Dir, sub_path: []const u8) std.Io.Dir.StatFileError!std.Io.File.Stat {
        const path = self.resolvePath(dir, sub_path, false) catch |err| return mapOpenFilePathError(err);
        defer self.allocator.free(path);
        const node = self.paths.get(path) orelse return error.FileNotFound;
        return nodeStat(node);
    }

    pub fn access(self: *FileSystem, dir: std.Io.Dir, sub_path: []const u8) std.Io.Dir.AccessError!void {
        const path = self.resolvePath(dir, sub_path, false) catch |err| return mapAccessPathError(err);
        defer self.allocator.free(path);
        if (!self.paths.contains(path)) return error.FileNotFound;
    }

    pub fn createFile(self: *FileSystem, dir: std.Io.Dir, sub_path: []const u8, options: std.Io.Dir.CreateFileOptions, now_ns: i96) std.Io.File.OpenError!std.Io.File {
        const path = self.resolvePath(dir, sub_path, options.resolve_beneath) catch |err| return mapOpenFilePathError(err);
        defer self.allocator.free(path);
        var node = self.paths.get(path);
        if (node) |existing| {
            if (options.exclusive) return error.PathAlreadyExists;
            if (existing.kind == .directory) return error.IsDir;
            if (options.truncate) {
                existing.data.clearRetainingCapacity();
                existing.mtime_ns = now_ns;
                existing.ctime_ns = now_ns;
            }
        } else {
            self.requireParentDirectory(path) catch |err| return mapOpenFilePathError(err);
            node = self.createNode(path, .file, options.permissions, now_ns) catch
                return error.SystemResources;
        }
        const handle = self.openHandle(node.?, options.read, true, false) catch |err| switch (err) {
            error.ProcessFdQuotaExceeded => return error.ProcessFdQuotaExceeded,
            else => return error.SystemResources,
        };
        const file: std.Io.File = .{ .handle = handle, .flags = .{ .nonblocking = false } };
        errdefer _ = self.closeFiles(&.{file});
        if (options.lock != .none and !(self.tryLock(file, options.lock) catch return error.FileLocksUnsupported))
            return error.WouldBlock;
        return file;
    }

    pub fn createFileAtomic(self: *FileSystem, dir: std.Io.Dir, sub_path: []const u8, options: std.Io.Dir.CreateFileAtomicOptions, now_ns: i96) std.Io.Dir.CreateFileAtomicError!std.Io.File.Atomic {
        const target = self.resolvePath(dir, sub_path, false) catch |err|
            return mapAtomicPathError(err);
        defer self.allocator.free(target);
        const parent_path = std.fs.path.dirname(target) orelse "/";
        if (options.make_path) {
            _ = self.createDirPath(.cwd(), parent_path, .default_dir, now_ns) catch |err|
                return @errorCast(err);
        }
        const atomic_dir = self.openDir(.cwd(), parent_path, .{}) catch |err|
            return @errorCast(err);
        errdefer _ = self.closeDirs(&.{atomic_dir});

        const basename_hex = self.next_atomic;
        self.next_atomic +%= 1;
        const temp_name = std.fmt.hex(basename_hex);
        const file = self.createFile(atomic_dir, &temp_name, .{
            .exclusive = true,
            .permissions = options.permissions,
        }, now_ns) catch |err| return @errorCast(err);
        return .{
            .file = file,
            .file_basename_hex = basename_hex,
            .file_open = true,
            .file_exists = true,
            .dir = atomic_dir,
            .close_dir_on_deinit = true,
            .dest_sub_path = std.fs.path.basename(sub_path),
        };
    }

    pub fn openFile(self: *FileSystem, dir: std.Io.Dir, sub_path: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
        const path = self.resolvePath(dir, sub_path, options.resolve_beneath) catch |err| return mapOpenFilePathError(err);
        defer self.allocator.free(path);
        const node = self.paths.get(path) orelse return error.FileNotFound;
        if (node.kind == .directory and (!options.allow_directory or options.isWrite())) return error.IsDir;
        const handle = self.openHandle(node, options.isRead(), options.isWrite(), node.kind == .directory) catch |err| switch (err) {
            error.ProcessFdQuotaExceeded => return error.ProcessFdQuotaExceeded,
            else => return error.SystemResources,
        };
        const file: std.Io.File = .{ .handle = handle, .flags = .{ .nonblocking = false } };
        errdefer _ = self.closeFiles(&.{file});
        if (options.lock != .none and !(self.tryLock(file, options.lock) catch return error.FileLocksUnsupported))
            return error.WouldBlock;
        return file;
    }

    pub fn closeDirs(self: *FileSystem, dirs: []const std.Io.Dir) bool {
        var valid = true;
        for (dirs) |dir| {
            if (dir.handle == std.Io.Dir.cwd().handle or !self.handles.remove(dir.handle)) valid = false;
        }
        return valid;
    }

    pub fn closeFiles(self: *FileSystem, files: []const std.Io.File) bool {
        var valid = true;
        for (files) |file| {
            if (self.handles.getPtr(file.handle)) |handle| {
                _ = releaseHandleLock(handle);
                _ = self.handles.remove(file.handle);
            } else valid = false;
        }
        return valid;
    }

    pub fn readDir(self: *FileSystem, reader: *std.Io.Dir.Reader, entries: []std.Io.Dir.Entry) std.Io.Dir.Reader.Error!usize {
        const directory = self.nodeForDir(reader.dir) catch return error.AccessDenied;
        var children: std.ArrayListUnmanaged(*Node) = .empty;
        defer children.deinit(self.allocator);
        for (self.nodes.items) |node| {
            if (!node.exists or std.mem.eql(u8, node.path, directory.path)) continue;
            const parent = std.fs.path.dirname(node.path) orelse "/";
            if (std.mem.eql(u8, parent, directory.path))
                children.append(self.allocator, node) catch return error.SystemResources;
        }
        std.mem.sort(*Node, children.items, {}, lessThanPath);
        if (reader.state == .reset) {
            reader.index = 0;
            reader.end = 0;
            reader.state = .reading;
        }
        if (reader.index >= children.items.len) {
            reader.state = .finished;
            reader.end = 0;
            return 0;
        }

        var count: usize = 0;
        var name_end: usize = 0;
        while (reader.index < children.items.len and count < entries.len) {
            const node = children.items[reader.index];
            const name = std.fs.path.basename(node.path);
            if (name.len > reader.buffer.len - name_end) break;
            @memcpy(reader.buffer[name_end..][0..name.len], name);
            entries[count] = .{
                .name = reader.buffer[name_end..][0..name.len],
                .kind = node.kind,
                .inode = @intCast(node.inode),
            };
            name_end += name.len;
            count += 1;
            reader.index += 1;
        }
        reader.end = name_end;
        if (reader.index == children.items.len) reader.state = .finished;
        return count;
    }

    pub fn realPathDir(self: *FileSystem, dir: std.Io.Dir, out: []u8) std.Io.Dir.RealPathError!usize {
        const node = self.nodeForDir(dir) catch return error.FileNotFound;
        return copyPath(node.path, out) catch |err| return @errorCast(err);
    }

    pub fn realPathAt(self: *FileSystem, dir: std.Io.Dir, sub_path: []const u8, out: []u8) std.Io.Dir.RealPathFileError!usize {
        const path = self.resolvePath(dir, sub_path, false) catch |err| return switch (err) {
            error.FileNotFound => error.FileNotFound,
            error.NotDir => error.AccessDenied,
            error.AccessDenied => error.AccessDenied,
            error.NameTooLong => error.NameTooLong,
            else => error.AccessDenied,
        };
        defer self.allocator.free(path);
        if (!self.paths.contains(path)) return error.FileNotFound;
        return copyPath(path, out) catch |err| return @errorCast(err);
    }

    pub fn realPathFile(self: *FileSystem, file: std.Io.File, out: []u8) std.Io.File.RealPathError!usize {
        const handle = self.handles.get(file.handle) orelse return error.FileNotFound;
        if (!handle.node.exists) return error.FileNotFound;
        return copyPath(handle.node.path, out) catch |err| return @errorCast(err);
    }

    pub fn setDirPermissions(self: *FileSystem, dir: std.Io.Dir, permissions: std.Io.File.Permissions, now_ns: i96) std.Io.Dir.SetPermissionsError!void {
        const node = self.nodeForDir(dir) catch return error.AccessDenied;
        setPermissions(node, permissions, now_ns);
    }

    pub fn setPermissionsAt(self: *FileSystem, dir: std.Io.Dir, sub_path: []const u8, permissions: std.Io.File.Permissions, now_ns: i96) std.Io.Dir.SetFilePermissionsError!void {
        const node = self.nodeAt(dir, sub_path) catch |err| return switch (err) {
            error.FileNotFound, error.NotDir => error.AccessDenied,
            error.AccessDenied => error.AccessDenied,
            error.NameTooLong => error.NameTooLong,
            else => error.SystemResources,
        };
        setPermissions(node, permissions, now_ns);
    }

    pub fn setFilePermissions(self: *FileSystem, file: std.Io.File, permissions: std.Io.File.Permissions, now_ns: i96) std.Io.File.SetPermissionsError!void {
        const handle = self.handles.get(file.handle) orelse return error.AccessDenied;
        setPermissions(handle.node, permissions, now_ns);
    }

    pub fn setTimestampsAt(self: *FileSystem, dir: std.Io.Dir, sub_path: []const u8, options: std.Io.Dir.SetTimestampsOptions, now_ns: i96) std.Io.Dir.SetTimestampsError!void {
        const node = self.nodeAt(dir, sub_path) catch |err| return switch (err) {
            error.FileNotFound, error.NotDir => error.AccessDenied,
            error.AccessDenied => error.AccessDenied,
            error.NameTooLong => error.NameTooLong,
            else => error.AccessDenied,
        };
        applyTimestamp(&node.atime_ns, options.access_timestamp, now_ns);
        applyTimestamp(&node.mtime_ns, options.modify_timestamp, now_ns);
        node.ctime_ns = now_ns;
    }

    pub fn setFileTimestamps(self: *FileSystem, file: std.Io.File, options: std.Io.File.SetTimestampsOptions, now_ns: i96) std.Io.File.SetTimestampsError!void {
        const handle = self.handles.get(file.handle) orelse return error.AccessDenied;
        applyTimestamp(&handle.node.atime_ns, options.access_timestamp, now_ns);
        applyTimestamp(&handle.node.mtime_ns, options.modify_timestamp, now_ns);
        handle.node.ctime_ns = now_ns;
    }

    pub fn deleteFile(self: *FileSystem, dir: std.Io.Dir, sub_path: []const u8) std.Io.Dir.DeleteFileError!void {
        const path = self.resolvePath(dir, sub_path, false) catch |err| return mapDeletePathError(err);
        defer self.allocator.free(path);
        const node = self.paths.get(path) orelse return error.FileNotFound;
        if (node.kind == .directory) return error.IsDir;
        _ = self.paths.remove(node.path);
        node.exists = false;
    }

    pub fn deleteDir(self: *FileSystem, dir: std.Io.Dir, sub_path: []const u8) std.Io.Dir.DeleteDirError!void {
        const path = self.resolvePath(dir, sub_path, false) catch |err| return mapDeleteDirPathError(err);
        defer self.allocator.free(path);
        if (std.mem.eql(u8, path, "/")) return error.AccessDenied;
        const node = self.paths.get(path) orelse return error.FileNotFound;
        if (node.kind != .directory) return error.NotDir;
        if (self.hasChildren(path)) return error.DirNotEmpty;
        _ = self.paths.remove(node.path);
        node.exists = false;
    }

    pub fn rename(self: *FileSystem, old_dir: std.Io.Dir, old_sub_path: []const u8, new_dir: std.Io.Dir, new_sub_path: []const u8, preserve: bool) anyerror!void {
        if (self.faults.fail_next_rename) {
            self.faults.fail_next_rename = false;
            return error.HardwareFailure;
        }
        const old_path = self.resolvePath(old_dir, old_sub_path, false) catch |err| return mapRenamePathError(err);
        defer self.allocator.free(old_path);
        const new_path = self.resolvePath(new_dir, new_sub_path, false) catch |err| return mapRenamePathError(err);
        defer self.allocator.free(new_path);
        const node = self.paths.get(old_path) orelse return error.FileNotFound;
        if (std.mem.eql(u8, old_path, "/")) return error.AccessDenied;
        if (node.kind == .directory and pathWithin(old_path, new_path)) return error.AccessDenied;
        self.requireParentDirectory(new_path) catch |err| return mapRenamePathError(err);
        if (self.paths.get(new_path)) |replaced| {
            if (preserve) return error.PathAlreadyExists;
            if (node.kind == .directory and replaced.kind != .directory) return error.NotDir;
            if (node.kind != .directory and replaced.kind == .directory) return error.IsDir;
            if (replaced.kind == .directory and self.hasChildren(new_path)) return error.PathAlreadyExists;
            _ = self.paths.remove(replaced.path);
            replaced.exists = false;
        }

        const RenameItem = struct { node: *Node, replacement: []u8 };
        var changed: std.ArrayListUnmanaged(RenameItem) = .empty;
        defer changed.deinit(self.allocator);
        errdefer for (changed.items) |item| self.allocator.free(item.replacement);
        for (self.nodes.items) |candidate| {
            if (!candidate.exists or !pathWithin(old_path, candidate.path)) continue;
            const suffix = candidate.path[old_path.len..];
            const replacement = std.mem.concat(self.allocator, u8, &.{ new_path, suffix }) catch
                return error.SystemResources;
            changed.append(self.allocator, .{ .node = candidate, .replacement = replacement }) catch {
                self.allocator.free(replacement);
                return error.SystemResources;
            };
        }
        for (changed.items) |item| _ = self.paths.remove(item.node.path);
        for (changed.items) |item| {
            self.allocator.free(item.node.path);
            item.node.path = item.replacement;
            self.paths.put(self.allocator, item.node.path, item.node) catch unreachable;
        }
    }

    pub fn statFile(self: *FileSystem, file: std.Io.File) std.Io.File.StatError!std.Io.File.Stat {
        const handle = self.handles.get(file.handle) orelse return error.AccessDenied;
        return nodeStat(handle.node);
    }

    pub fn fileLength(self: *FileSystem, file: std.Io.File) std.Io.File.LengthError!u64 {
        const handle = self.handles.get(file.handle) orelse return error.AccessDenied;
        if (handle.directory) return error.AccessDenied;
        return handle.node.data.items.len;
    }

    pub fn readPositional(self: *FileSystem, file: std.Io.File, buffers: []const []u8, offset: u64) std.Io.File.ReadPositionalError!usize {
        if (self.faults.fail_next_read) {
            self.faults.fail_next_read = false;
            return error.InputOutput;
        }
        const handle = self.handles.get(file.handle) orelse return error.AccessDenied;
        if (!handle.readable) return error.NotOpenForReading;
        if (handle.directory) return error.IsDir;
        const start = std.math.cast(usize, offset) orelse return 0;
        if (start >= handle.node.data.items.len) return 0;
        const corruption = self.faults.corrupt_next_read;
        self.faults.corrupt_next_read = null;
        var source_index = start;
        var total: usize = 0;
        for (buffers) |buffer| {
            const count = @min(buffer.len, handle.node.data.items.len - source_index);
            @memcpy(buffer[0..count], handle.node.data.items[source_index..][0..count]);
            if (corruption) |range| {
                applyCorruption(buffer[0..count], source_index, range);
            }
            for (self.persistent_corruptions.items) |entry| {
                if (entry.inode == handle.node.inode)
                    applyCorruption(buffer[0..count], source_index, entry.range);
            }
            total += count;
            source_index += count;
            if (count != buffer.len or source_index == handle.node.data.items.len) break;
        }
        return total;
    }

    pub fn writePositional(self: *FileSystem, file: std.Io.File, header: []const u8, buffers: []const []const u8, splat: usize, offset: u64, now_ns: i96) std.Io.File.WritePositionalError!usize {
        if (self.faults.fail_next_write) {
            self.faults.fail_next_write = false;
            return error.InputOutput;
        }
        const handle = self.handles.get(file.handle) orelse return error.AccessDenied;
        if (!handle.writable) return error.NotOpenForWriting;
        if (handle.directory) return error.AccessDenied;

        var requested = header.len;
        for (0..splat) |_| {
            for (buffers) |buffer| requested = std.math.add(usize, requested, buffer.len) catch
                return error.FileTooBig;
        }
        var allowed = requested;
        if (self.faults.partial_write_limit) |limit| {
            allowed = @min(allowed, limit);
            self.faults.partial_write_limit = null;
        }
        const start = std.math.cast(usize, offset) orelse return error.FileTooBig;
        const end = std.math.add(usize, start, allowed) catch return error.FileTooBig;
        const existing_total = self.totalBytes();
        const growth = end -| handle.node.data.items.len;
        if (existing_total > self.config.capacity_bytes or growth > self.config.capacity_bytes - existing_total)
            return error.NoSpaceLeft;
        if (end > handle.node.data.items.len) {
            const old_len = handle.node.data.items.len;
            handle.node.data.resize(self.allocator, end) catch return error.SystemResources;
            @memset(handle.node.data.items[old_len..], 0);
        }

        var written: usize = 0;
        written += copyLimited(handle.node.data.items[start + written ..], header, allowed - written);
        outer: for (0..splat) |_| {
            for (buffers) |buffer| {
                written += copyLimited(handle.node.data.items[start + written ..], buffer, allowed - written);
                if (written == allowed) break :outer;
            }
        }
        handle.node.mtime_ns = now_ns;
        handle.node.ctime_ns = now_ns;
        return written;
    }

    pub fn readStreaming(self: *FileSystem, file: std.Io.File, buffers: []const []u8) std.Io.Operation.FileReadStreaming.Result {
        const handle = self.handles.getPtr(file.handle) orelse return error.AccessDenied;
        const count = self.readPositional(file, buffers, handle.cursor) catch |err| return switch (err) {
            error.Canceled, error.Unseekable => error.AccessDenied,
            else => @errorCast(err),
        };
        handle.cursor += count;
        return count;
    }

    pub fn writeStreaming(self: *FileSystem, file: std.Io.File, header: []const u8, buffers: []const []const u8, splat: usize, now_ns: i96) std.Io.Operation.FileWriteStreaming.Result {
        const handle = self.handles.getPtr(file.handle) orelse return error.AccessDenied;
        const count = self.writePositional(file, header, buffers, splat, handle.cursor, now_ns) catch |err| return switch (err) {
            error.Canceled, error.Unseekable => error.AccessDenied,
            else => @errorCast(err),
        };
        handle.cursor += count;
        return count;
    }

    pub fn seekTo(self: *FileSystem, file: std.Io.File, offset: u64) std.Io.File.SeekError!void {
        const handle = self.handles.getPtr(file.handle) orelse return error.AccessDenied;
        if (handle.directory) return error.Unseekable;
        handle.cursor = offset;
    }

    pub fn seekBy(self: *FileSystem, file: std.Io.File, relative: i64) std.Io.File.SeekError!void {
        const handle = self.handles.getPtr(file.handle) orelse return error.AccessDenied;
        if (handle.directory) return error.Unseekable;
        if (relative < 0) {
            const magnitude: u64 = @intCast(-relative);
            if (magnitude > handle.cursor) return error.Unseekable;
            handle.cursor -= magnitude;
        } else {
            handle.cursor = std.math.add(u64, handle.cursor, @intCast(relative)) catch
                return error.Unseekable;
        }
    }

    pub fn setLength(self: *FileSystem, file: std.Io.File, length: u64, now_ns: i96) std.Io.File.SetLengthError!void {
        const handle = self.handles.get(file.handle) orelse return error.AccessDenied;
        if (!handle.writable) return error.AccessDenied;
        if (handle.directory) return error.AccessDenied;
        const new_len = std.math.cast(usize, length) orelse return error.FileTooBig;
        const old_len = handle.node.data.items.len;
        if (new_len > old_len) {
            const existing_total = self.totalBytes();
            const growth = new_len - old_len;
            if (existing_total > self.config.capacity_bytes or growth > self.config.capacity_bytes - existing_total)
                return error.FileTooBig;
        }
        handle.node.data.resize(self.allocator, new_len) catch return error.FileTooBig;
        if (new_len > old_len) @memset(handle.node.data.items[old_len..], 0);
        handle.node.mtime_ns = now_ns;
        handle.node.ctime_ns = now_ns;
    }

    pub fn tryLock(self: *FileSystem, file: std.Io.File, lock: std.Io.File.Lock) std.Io.File.LockError!bool {
        const handle = self.handles.getPtr(file.handle) orelse return error.FileLocksUnsupported;
        if (lock == .none) {
            _ = releaseHandleLock(handle);
            return true;
        }
        if (handle.owned_lock == lock) return true;
        if (handle.owned_lock == .exclusive and lock == .shared) {
            handle.node.lock = .shared;
            handle.node.shared_lock_count = 1;
            handle.owned_lock = .shared;
            return true;
        }
        if (handle.owned_lock == .shared and lock == .exclusive) {
            if (handle.node.lock != .shared or handle.node.shared_lock_count != 1) return false;
            handle.node.shared_lock_count = 0;
            handle.node.lock = .exclusive;
            handle.owned_lock = .exclusive;
            return true;
        }
        std.debug.assert(handle.owned_lock == .none);
        switch (lock) {
            .none => unreachable,
            .shared => {
                if (handle.node.lock == .exclusive) return false;
                handle.node.lock = .shared;
                handle.node.shared_lock_count += 1;
                handle.owned_lock = .shared;
                return true;
            },
            .exclusive => {
                if (handle.node.lock != .none) return false;
                handle.node.lock = .exclusive;
                handle.owned_lock = .exclusive;
                return true;
            },
        }
    }

    pub fn unlock(self: *FileSystem, file: std.Io.File) bool {
        const handle = self.handles.getPtr(file.handle) orelse return false;
        return releaseHandleLock(handle);
    }

    pub fn downgradeLock(self: *FileSystem, file: std.Io.File) std.Io.File.DowngradeLockError!void {
        const handle = self.handles.getPtr(file.handle) orelse return error.Unexpected;
        if (handle.owned_lock != .exclusive or handle.node.lock != .exclusive) return error.Unexpected;
        handle.node.lock = .shared;
        handle.node.shared_lock_count = 1;
        handle.owned_lock = .shared;
    }

    fn releaseHandleLock(handle: *OpenHandle) bool {
        switch (handle.owned_lock) {
            .none => return false,
            .exclusive => handle.node.lock = .none,
            .shared => {
                std.debug.assert(handle.node.shared_lock_count > 0);
                handle.node.shared_lock_count -= 1;
                if (handle.node.shared_lock_count == 0) handle.node.lock = .none;
            },
        }
        handle.owned_lock = .none;
        return true;
    }

    pub fn createMemoryMap(self: *FileSystem, file: std.Io.File, options: std.Io.File.MemoryMap.CreateOptions) std.Io.File.MemoryMap.CreateError!std.Io.File.MemoryMap {
        const handle = self.handles.get(file.handle) orelse return error.AccessDenied;
        if (handle.directory or (options.protection.read and !handle.readable) or
            (options.protection.write and !handle.writable) or options.protection.execute)
            return error.AccessDenied;
        const memory = try self.allocator.alignedAlloc(u8, .fromByteUnits(std.heap.page_size_min), options.len);
        errdefer self.allocator.free(memory);
        if (options.undefined_contents) {
            @memset(memory, undefined);
        } else {
            @memset(memory, 0);
            _ = try self.readPositional(file, &.{memory}, options.offset);
        }
        return .{
            .file = file,
            .offset = options.offset,
            .memory = memory,
            .section = null,
        };
    }

    pub fn destroyMemoryMap(self: *FileSystem, map: *std.Io.File.MemoryMap) void {
        self.allocator.free(map.memory);
        map.* = undefined;
    }

    pub fn setMemoryMapLength(self: *FileSystem, map: *std.Io.File.MemoryMap, new_len: usize) std.Io.File.MemoryMap.SetLengthError!void {
        const replacement = try self.allocator.alignedAlloc(u8, .fromByteUnits(std.heap.page_size_min), new_len);
        const preserved = @min(map.memory.len, replacement.len);
        @memcpy(replacement[0..preserved], map.memory[0..preserved]);
        if (replacement.len > preserved) @memset(replacement[preserved..], 0);
        self.allocator.free(map.memory);
        map.memory = replacement;
    }

    pub fn readMemoryMap(self: *FileSystem, map: *std.Io.File.MemoryMap) std.Io.File.ReadPositionalError!void {
        @memset(map.memory, 0);
        _ = try self.readPositional(map.file, &.{map.memory}, map.offset);
    }

    pub fn writeMemoryMap(self: *FileSystem, map: *std.Io.File.MemoryMap, now_ns: i96) std.Io.File.WritePositionalError!void {
        var written: usize = 0;
        while (written < map.memory.len) {
            const count = try self.writePositional(map.file, &.{}, &.{map.memory[written..]}, 1, map.offset + written, now_ns);
            if (count == 0) return error.NoSpaceLeft;
            written += count;
        }
    }

    pub fn syncFile(self: *FileSystem, file: std.Io.File) std.Io.File.SyncError!void {
        if (self.faults.fail_next_sync) {
            self.faults.fail_next_sync = false;
            return error.InputOutput;
        }
        const handle = self.handles.get(file.handle) orelse return error.AccessDenied;
        if (self.faults.drop_next_sync) {
            self.faults.drop_next_sync = false;
            return;
        }
        if (handle.directory) {
            // A directory fsync is the durable namespace-publication boundary.
            // The model currently persists the bounded virtual namespace as a
            // unit, which is conservative and matches the existing explicit
            // `syncNamespace` crash contract.
            self.syncNamespace() catch return error.AccessDenied;
            return;
        }
        if (self.faults.torn_next_sync_limit) |limit| {
            self.faults.torn_next_sync_limit = null;
            const persisted_len = @min(limit, handle.node.data.items.len);
            if (handle.node.durable_data.items.len < persisted_len) {
                handle.node.durable_data.resize(self.allocator, persisted_len) catch
                    return error.AccessDenied;
            }
            @memcpy(handle.node.durable_data.items[0..persisted_len], handle.node.data.items[0..persisted_len]);
            persistMetadata(handle.node);
            return;
        }
        handle.node.durable_data.resize(self.allocator, handle.node.data.items.len) catch
            return error.AccessDenied;
        @memcpy(handle.node.durable_data.items, handle.node.data.items);
        persistMetadata(handle.node);
    }

    /// Installs a media-corruption range keyed by durable inode identity. The
    /// overlay affects every later read and survives virtual crashes until it
    /// is explicitly cleared, modeling a bad sector rather than a one-shot
    /// transport bit flip.
    pub fn addPersistentCorruption(self: *FileSystem, file: std.Io.File, range: CorruptRange) !void {
        if (range.length == 0 or range.xor_mask == 0) return error.InvalidFileCorruptionRange;
        const handle = self.handles.get(file.handle) orelse return error.InvalidVoprIoFile;
        try self.persistent_corruptions.append(self.allocator, .{
            .inode = handle.node.inode,
            .range = range,
        });
    }

    pub fn clearPersistentCorruption(self: *FileSystem, file: std.Io.File) !void {
        const handle = self.handles.get(file.handle) orelse return error.InvalidVoprIoFile;
        var index: usize = 0;
        while (index < self.persistent_corruptions.items.len) {
            if (self.persistent_corruptions.items[index].inode != handle.node.inode) {
                index += 1;
                continue;
            }
            _ = self.persistent_corruptions.swapRemove(index);
        }
    }

    pub fn syncNamespace(self: *FileSystem) !void {
        for (self.nodes.items) |node| {
            node.durable_exists = node.exists;
            if (!node.exists) continue;
            const durable_path = try self.allocator.dupe(u8, node.path);
            self.allocator.free(node.durable_path);
            node.durable_path = durable_path;
            if (node.kind == .directory) persistMetadata(node);
        }
    }

    pub fn crash(self: *FileSystem) !void {
        self.handles.clearRetainingCapacity();
        self.paths.clearRetainingCapacity();
        for (self.nodes.items) |node| {
            node.exists = node.durable_exists;
            node.lock = .none;
            node.shared_lock_count = 0;
            node.data.clearRetainingCapacity();
            if (!node.durable_exists) continue;
            try node.data.appendSlice(self.allocator, node.durable_data.items);
            const path = try self.allocator.dupe(u8, node.durable_path);
            self.allocator.free(node.path);
            node.path = path;
            node.permissions = node.durable_permissions;
            node.atime_ns = node.durable_atime_ns;
            node.mtime_ns = node.durable_mtime_ns;
            node.ctime_ns = node.durable_ctime_ns;
            try self.paths.put(self.allocator, node.path, node);
        }
    }

    pub fn totalBytes(self: *const FileSystem) u64 {
        var total: u64 = 0;
        for (self.nodes.items) |node| {
            if (node.exists and node.kind == .file) total +|= node.data.items.len;
        }
        return total;
    }

    pub fn openHandleCount(self: *const FileSystem) usize {
        return self.handles.count();
    }

    pub fn capacityBytes(self: *const FileSystem) u64 {
        return self.config.capacity_bytes;
    }

    pub fn availableBytes(self: *const FileSystem) u64 {
        return self.config.capacity_bytes -| self.totalBytes();
    }

    fn createNode(self: *FileSystem, path: []const u8, kind: std.Io.File.Kind, permissions: std.Io.File.Permissions, now_ns: i96) !*Node {
        const node = try self.allocator.create(Node);
        errdefer self.allocator.destroy(node);
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);
        const durable_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(durable_path);
        node.* = .{
            .inode = self.next_inode,
            .kind = kind,
            .path = owned_path,
            .durable_path = durable_path,
            .exists = true,
            .durable_exists = false,
            .permissions = permissions,
            .durable_permissions = permissions,
            .atime_ns = now_ns,
            .mtime_ns = now_ns,
            .ctime_ns = now_ns,
            .durable_atime_ns = now_ns,
            .durable_mtime_ns = now_ns,
            .durable_ctime_ns = now_ns,
        };
        self.next_inode += 1;
        try self.nodes.append(self.allocator, node);
        errdefer _ = self.nodes.pop();
        try self.paths.put(self.allocator, node.path, node);
        return node;
    }

    fn openHandle(self: *FileSystem, node: *Node, readable: bool, writable: bool, directory: bool) !std.Io.File.Handle {
        if (self.handles.count() >= self.config.max_open_handles) return error.ProcessFdQuotaExceeded;
        if (self.next_handle == std.math.maxInt(std.Io.File.Handle)) return error.ProcessFdQuotaExceeded;
        const handle = self.next_handle;
        self.next_handle += 1;
        try self.handles.put(self.allocator, handle, .{
            .node = node,
            .readable = readable,
            .writable = writable,
            .directory = directory,
        });
        return handle;
    }

    fn nodeForDir(self: *FileSystem, dir: std.Io.Dir) !*Node {
        if (dir.handle == std.Io.Dir.cwd().handle) return self.paths.get("/").?;
        const handle = self.handles.get(dir.handle) orelse return error.FileNotFound;
        if (!handle.directory or handle.node.kind != .directory) return error.NotDir;
        return handle.node;
    }

    fn nodeAt(self: *FileSystem, dir: std.Io.Dir, sub_path: []const u8) !*Node {
        const path = try self.resolvePath(dir, sub_path, false);
        defer self.allocator.free(path);
        return self.paths.get(path) orelse error.FileNotFound;
    }

    fn resolvePath(self: *FileSystem, dir: std.Io.Dir, sub_path: []const u8, beneath: bool) ![]u8 {
        if (sub_path.len == 0) return error.FileNotFound;
        const base = (try self.nodeForDir(dir)).path;
        const resolved = std.fs.path.resolve(
            self.allocator,
            if (std.fs.path.isAbsolute(sub_path)) &.{sub_path} else &.{ base, sub_path },
        ) catch return error.SystemResources;
        errdefer self.allocator.free(resolved);
        if (resolved.len > std.Io.Dir.max_path_bytes) return error.NameTooLong;
        if (beneath and !pathWithin(base, resolved)) return error.AccessDenied;
        return resolved;
    }

    fn ensureDirectoryPath(self: *FileSystem, path: []const u8, permissions: std.Io.File.Permissions, now_ns: i96) !void {
        if (std.mem.eql(u8, path, "/")) return;
        const parent = std.fs.path.dirname(path) orelse "/";
        if (self.paths.get(parent)) |node| {
            if (node.kind != .directory) return error.NotDir;
        } else {
            try self.ensureDirectoryPath(parent, permissions, now_ns);
        }
        if (!self.paths.contains(path)) _ = try self.createNode(path, .directory, permissions, now_ns);
    }

    fn requireParentDirectory(self: *FileSystem, path: []const u8) !void {
        const parent = std.fs.path.dirname(path) orelse "/";
        const node = self.paths.get(parent) orelse return error.FileNotFound;
        if (node.kind != .directory) return error.NotDir;
    }

    fn hasChildren(self: *const FileSystem, path: []const u8) bool {
        for (self.nodes.items) |node| {
            if (!node.exists or std.mem.eql(u8, node.path, path)) continue;
            if (pathWithin(path, node.path)) return true;
        }
        return false;
    }
};

fn nodeStat(node: *const Node) std.Io.File.Stat {
    return .{
        .inode = @intCast(node.inode),
        .nlink = 1,
        .size = node.data.items.len,
        .permissions = node.permissions,
        .kind = node.kind,
        .atime = .fromNanoseconds(node.atime_ns),
        .mtime = .fromNanoseconds(node.mtime_ns),
        .ctime = .fromNanoseconds(node.ctime_ns),
        .block_size = 1,
    };
}

fn lessThanPath(_: void, left: *Node, right: *Node) bool {
    return std.mem.order(u8, left.path, right.path) == .lt;
}

fn copyPath(path: []const u8, out: []u8) error{NameTooLong}!usize {
    if (path.len > out.len) return error.NameTooLong;
    @memcpy(out[0..path.len], path);
    return path.len;
}

fn setPermissions(node: *Node, permissions: std.Io.File.Permissions, now_ns: i96) void {
    node.permissions = permissions;
    node.ctime_ns = now_ns;
}

fn applyTimestamp(destination: *i96, value: std.Io.File.SetTimestamp, now_ns: i96) void {
    switch (value) {
        .unchanged => {},
        .now => destination.* = now_ns,
        .new => |timestamp| destination.* = timestamp.toNanoseconds(),
    }
}

fn persistMetadata(node: *Node) void {
    node.durable_permissions = node.permissions;
    node.durable_atime_ns = node.atime_ns;
    node.durable_mtime_ns = node.mtime_ns;
    node.durable_ctime_ns = node.ctime_ns;
}

fn pathWithin(base: []const u8, candidate: []const u8) bool {
    if (std.mem.eql(u8, base, "/")) return std.fs.path.isAbsolute(candidate);
    if (!std.mem.startsWith(u8, candidate, base)) return false;
    return candidate.len == base.len or candidate[base.len] == std.fs.path.sep;
}

fn copyLimited(destination: []u8, source: []const u8, limit: usize) usize {
    const count = @min(@min(destination.len, source.len), limit);
    @memcpy(destination[0..count], source[0..count]);
    return count;
}

fn mapOpenFilePathError(err: anyerror) std.Io.File.OpenError {
    return switch (err) {
        error.FileNotFound => error.FileNotFound,
        error.NotDir => error.NotDir,
        error.AccessDenied => error.AccessDenied,
        error.NameTooLong => error.NameTooLong,
        else => error.SystemResources,
    };
}

fn mapAtomicPathError(err: anyerror) std.Io.Dir.CreateFileAtomicError {
    return switch (err) {
        error.FileNotFound => error.FileNotFound,
        error.NotDir => error.NotDir,
        error.AccessDenied => error.AccessDenied,
        error.NameTooLong => error.NameTooLong,
        else => error.SystemResources,
    };
}

fn mapCreateDirError(err: anyerror) std.Io.Dir.CreateDirError {
    return switch (err) {
        error.FileNotFound => error.FileNotFound,
        error.NotDir => error.NotDir,
        error.AccessDenied => error.AccessDenied,
        error.NameTooLong => error.NameTooLong,
        else => error.SystemResources,
    };
}

fn mapOpenDirPathError(err: anyerror) std.Io.Dir.OpenError {
    return switch (err) {
        error.FileNotFound => error.FileNotFound,
        error.NotDir => error.NotDir,
        error.AccessDenied => error.AccessDenied,
        error.NameTooLong => error.NameTooLong,
        else => error.SystemResources,
    };
}

fn mapCreateDirPathError(err: anyerror) std.Io.Dir.CreateDirPathError {
    return switch (err) {
        error.FileNotFound => error.FileNotFound,
        error.NotDir => error.NotDir,
        error.AccessDenied => error.AccessDenied,
        error.NameTooLong => error.NameTooLong,
        else => error.SystemResources,
    };
}

fn mapAccessPathError(err: anyerror) std.Io.Dir.AccessError {
    return switch (err) {
        error.FileNotFound => error.FileNotFound,
        error.NotDir => error.FileNotFound,
        error.AccessDenied => error.AccessDenied,
        error.NameTooLong => error.NameTooLong,
        else => error.SystemResources,
    };
}

fn mapDeletePathError(err: anyerror) std.Io.Dir.DeleteFileError {
    return switch (err) {
        error.FileNotFound => error.FileNotFound,
        error.NotDir => error.NotDir,
        error.AccessDenied => error.AccessDenied,
        error.NameTooLong => error.NameTooLong,
        else => error.SystemResources,
    };
}

fn mapDeleteDirPathError(err: anyerror) std.Io.Dir.DeleteDirError {
    return switch (err) {
        error.FileNotFound => error.FileNotFound,
        error.NotDir => error.NotDir,
        error.AccessDenied => error.AccessDenied,
        error.NameTooLong => error.NameTooLong,
        else => error.SystemResources,
    };
}

fn mapRenamePathError(err: anyerror) std.Io.Dir.RenameError {
    return switch (err) {
        error.FileNotFound => error.FileNotFound,
        error.NotDir => error.NotDir,
        error.AccessDenied => error.AccessDenied,
        error.NameTooLong => error.NameTooLong,
        else => error.SystemResources,
    };
}

fn applyCorruption(bytes: []u8, absolute_start: usize, range: CorruptRange) void {
    const corrupt_start = std.math.cast(usize, range.offset) orelse std.math.maxInt(usize);
    const corrupt_end = std.math.add(
        usize,
        corrupt_start,
        std.math.cast(usize, range.length) orelse std.math.maxInt(usize),
    ) catch std.math.maxInt(usize);
    for (bytes, absolute_start..) |*byte, absolute_index| {
        if (absolute_index >= corrupt_start and absolute_index < corrupt_end)
            byte.* ^= range.xor_mask;
    }
}

test "virtual filesystem separates file and namespace durability" {
    var fs = try FileSystem.init(std.testing.allocator, .{});
    defer fs.deinit();
    _ = try fs.createDirPath(.cwd(), "db", .default_dir, 1);
    var file = try fs.createFile(.cwd(), "db/wal", .{ .read = true }, 2);
    try std.testing.expectEqual(@as(usize, 3), try fs.writePositional(file, &.{}, &.{"one"}, 1, 0, 3));
    try fs.syncFile(file);
    try fs.syncNamespace();
    try std.testing.expect(fs.closeFiles(&.{file}));

    file = try fs.openFile(.cwd(), "db/wal", .{ .mode = .read_write });
    try std.testing.expectEqual(@as(usize, 3), try fs.writePositional(file, &.{}, &.{"two"}, 1, 0, 4));
    fs.faults.drop_next_sync = true;
    try fs.syncFile(file);
    try fs.crash();

    file = try fs.openFile(.cwd(), "db/wal", .{});
    var bytes: [3]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 3), try fs.readPositional(file, &.{&bytes}, 0));
    try std.testing.expectEqualStrings("one", &bytes);
}

test "virtual filesystem enforces descriptor capacity partial writes and crash publication" {
    var fs = try FileSystem.init(std.testing.allocator, .{
        .max_open_handles = 1,
        .capacity_bytes = 4,
    });
    defer fs.deinit();
    const file = try fs.createFile(.cwd(), "ephemeral", .{ .read = true }, 1);
    try std.testing.expectError(
        error.ProcessFdQuotaExceeded,
        fs.openFile(.cwd(), "ephemeral", .{}),
    );
    fs.faults.partial_write_limit = 2;
    try std.testing.expectEqual(@as(usize, 2), try fs.writePositional(file, &.{}, &.{"abcd"}, 1, 0, 2));
    try std.testing.expectError(error.NoSpaceLeft, fs.writePositional(file, &.{}, &.{"xyz"}, 1, 3, 2));
    try std.testing.expect(fs.closeFiles(&.{file}));
    try fs.crash();
    try std.testing.expectError(error.FileNotFound, fs.openFile(.cwd(), "ephemeral", .{}));
}

test "virtual filesystem applies precise one-shot read range corruption" {
    var fs = try FileSystem.init(std.testing.allocator, .{});
    defer fs.deinit();
    const file = try fs.createFile(.cwd(), "range", .{ .read = true }, 1);
    try std.testing.expectEqual(@as(usize, 6), try fs.writePositional(file, &.{}, &.{"abcdef"}, 1, 0, 2));
    fs.faults.corrupt_next_read = .{ .offset = 2, .length = 2, .xor_mask = 0x20 };
    var bytes: [6]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 6), try fs.readPositional(file, &.{&bytes}, 0));
    try std.testing.expectEqualStrings("abCDef", &bytes);
    try std.testing.expectEqual(@as(usize, 6), try fs.readPositional(file, &.{&bytes}, 0));
    try std.testing.expectEqualStrings("abcdef", &bytes);
}

test "virtual filesystem persists torn sync prefixes and durable sector corruption" {
    var fs = try FileSystem.init(std.testing.allocator, .{});
    defer fs.deinit();
    var file = try fs.createFile(.cwd(), "durable-corruption", .{ .read = true }, 1);
    try std.testing.expectEqual(@as(usize, 6), try fs.writePositional(file, &.{}, &.{"abcdef"}, 1, 0, 2));
    try fs.syncFile(file);
    try fs.syncNamespace();

    try std.testing.expectEqual(@as(usize, 6), try fs.writePositional(file, &.{}, &.{"UVWXYZ"}, 1, 0, 3));
    fs.faults.torn_next_sync_limit = 2;
    try fs.syncFile(file);
    try fs.crash();

    file = try fs.openFile(.cwd(), "durable-corruption", .{});
    var bytes: [6]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 6), try fs.readPositional(file, &.{&bytes}, 0));
    try std.testing.expectEqualStrings("UVcdef", &bytes);

    try fs.addPersistentCorruption(file, .{ .offset = 2, .length = 2, .xor_mask = 0x20 });
    try std.testing.expectEqual(@as(usize, 6), try fs.readPositional(file, &.{&bytes}, 0));
    try std.testing.expectEqualStrings("UVCDef", &bytes);
    try fs.crash();
    file = try fs.openFile(.cwd(), "durable-corruption", .{});
    try std.testing.expectEqual(@as(usize, 6), try fs.readPositional(file, &.{&bytes}, 0));
    try std.testing.expectEqualStrings("UVCDef", &bytes);
    try fs.clearPersistentCorruption(file);
    try std.testing.expectEqual(@as(usize, 6), try fs.readPositional(file, &.{&bytes}, 0));
    try std.testing.expectEqualStrings("UVcdef", &bytes);
}
