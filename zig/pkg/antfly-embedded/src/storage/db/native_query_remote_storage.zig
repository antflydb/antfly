// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Read-only storage over an authenticated retained generation. Metadata and
//! immutable pages share one authority; fetching a page never recaptures rows.
const std = @import("std");
const storage = @import("../lsm_backend/storage_io.zig");
const source = @import("../../segment_source.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const Control = @import("native_query_cut.zig").Control;
const A = std.mem.Allocator;
pub const Chunk = struct { artifact_id: []const u8, byte_len: u64, checksum: []const u8 };
pub const File = struct { path: []const u8, size: u64, chunks: []const Chunk };
pub const Fetch = struct {
    ptr: *anyopaque,
    read: *const fn (*anyopaque, A, Chunk, Cancellation) anyerror![]u8,
};
pub const Reader = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    root: []const u8,
    files: []const File,
    fetch: Fetch,
    control: Control,
    io: std.Io,
    refs: std.atomic.Value(usize) = .init(1),
    mutex: std.Io.Mutex = .init,
    cache: [32]?Cached = @splat(null),
    clock: u64 = 0,
    const Cached = struct { id: []const u8, bytes: []u8, used: u64 };

    /// Manifest ownership is copied; caller buffers may be released after open.
    pub fn create(a: A, io: std.Io, root: []const u8, files: []const File, fetch: Fetch, control: Control) !*Reader {
        const self = try a.create(Reader);
        errdefer a.destroy(self);
        self.* = .{ .a = a, .arena = .init(a), .root = undefined, .files = undefined, .fetch = fetch, .control = control, .io = io };
        errdefer self.arena.deinit();
        const scratch = self.arena.allocator();
        self.root = try scratch.dupe(u8, std.mem.trimEnd(u8, root, "/"));
        const owned = try scratch.alloc(File, files.len);
        for (owned, files) |*out, manifest_file| {
            if (manifest_file.path.len == 0 or std.fs.path.isAbsolute(manifest_file.path) or std.mem.indexOf(u8, manifest_file.path, "..") != null or std.mem.indexOfAny(u8, manifest_file.path, "\\\x00") != null) return error.CatalogGenerationChanged;
            out.* = .{ .path = try scratch.dupe(u8, manifest_file.path), .size = manifest_file.size, .chunks = try scratch.alloc(Chunk, manifest_file.chunks.len) };
            var size: u64 = 0;
            for (@constCast(out.chunks), manifest_file.chunks) |*chunk, input| {
                if (input.byte_len == 0 or input.byte_len > 4 * 1024 * 1024) return error.CatalogGenerationChanged;
                size = try std.math.add(u64, size, input.byte_len);
                chunk.* = .{ .artifact_id = try scratch.dupe(u8, input.artifact_id), .byte_len = input.byte_len, .checksum = try scratch.dupe(u8, input.checksum) };
            }
            if (size != manifest_file.size) return error.CatalogGenerationChanged;
        }
        self.files = owned;
        return self;
    }
    fn retain(self: *Reader) void {
        _ = self.refs.fetchAdd(1, .monotonic);
    }
    pub fn release(self: *Reader) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        for (self.cache) |entry| if (entry) |value| self.a.free(value.bytes);
        self.arena.deinit();
        self.a.destroy(self);
    }
    pub fn lease(self: *Reader) storage.Storage.Lease {
        return .{ .view = self.view(), .release = releaseOpaque };
    }
    fn releaseOpaque(raw: *anyopaque) void {
        cast(raw).release();
    }
    fn cast(raw: *anyopaque) *Reader {
        return @ptrCast(@alignCast(raw));
    }
    fn relative(self: *Reader, path: []const u8) ![]const u8 {
        if (!std.mem.startsWith(u8, path, self.root) or path.len <= self.root.len or path[self.root.len] != '/') return error.FileNotFound;
        return path[self.root.len + 1 ..];
    }
    fn file(self: *Reader, path: []const u8) !*const File {
        const relative_path = try self.relative(path);
        for (self.files) |*entry| if (std.mem.eql(u8, relative_path, entry.path)) return entry;
        return error.FileNotFound;
    }
    fn chunkSlice(self: *Reader, ref: Chunk, offset: usize, out: []u8) !void {
        try self.control.token().check();
        self.mutex.lockUncancelable(self.io);
        self.clock +%= 1;
        for (&self.cache) |*entry| if (entry.*) |*cached| {
            if (std.mem.eql(u8, cached.id, ref.artifact_id)) {
                if (cached.bytes.len != ref.byte_len) {
                    self.mutex.unlock(self.io);
                    return error.CatalogGenerationChanged;
                }
                cached.used = self.clock;
                @memcpy(out, cached.bytes[offset..][0..out.len]);
                self.mutex.unlock(self.io);
                return;
            }
        };
        self.mutex.unlock(self.io);
        const bytes = try self.fetch.read(self.fetch.ptr, self.a, ref, self.control.token());
        var adopted = false;
        defer if (!adopted) self.a.free(bytes);
        if (bytes.len != ref.byte_len) return error.CatalogGenerationChanged;
        @memcpy(out, bytes[offset..][0..out.len]);
        try self.control.token().check();
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        // Concurrent misses may fetch twice but never cache duplicate entries.
        for (self.cache) |entry| if (entry) |cached| if (std.mem.eql(u8, cached.id, ref.artifact_id)) return;
        var victim: usize = 0;
        for (self.cache, 0..) |entry, index| {
            if (entry == null) {
                victim = index;
                break;
            }
            if (entry.?.used < self.cache[victim].?.used) victim = index;
        }
        if (self.cache[victim]) |cached| self.a.free(cached.bytes);
        self.clock +%= 1;
        self.cache[victim] = .{ .id = ref.artifact_id, .bytes = bytes, .used = self.clock };
        adopted = true;
    }
    fn readInto(self: *Reader, entry: *const File, offset: u64, out: []u8) !void {
        if (offset > entry.size or out.len > entry.size - offset) return error.EndOfStream;
        var position: u64 = 0;
        var copied: usize = 0;
        for (entry.chunks) |ref| {
            const end = position + ref.byte_len;
            const wanted = offset + copied;
            if (wanted < end and copied < out.len) {
                const within: usize = @intCast(wanted - position);
                const length = @min(out.len - copied, @as(usize, @intCast(ref.byte_len)) - within);
                try self.chunkSlice(ref, within, out[copied..][0..length]);
                copied += length;
            }
            position = end;
            if (copied == out.len) return;
        }
        if (copied != out.len) return error.EndOfStream;
    }
    fn readRange(raw: *anyopaque, a: A, path: []const u8, offset: u64, length: usize) ![]u8 {
        const self = cast(raw);
        const entry = try self.file(path);
        const bytes = try a.alloc(u8, length);
        errdefer a.free(bytes);
        try self.readInto(entry, offset, bytes);
        return bytes;
    }
    fn readFile(raw: *anyopaque, a: A, path: []const u8, maximum: usize) ![]u8 {
        const length = (try cast(raw).file(path)).size;
        if (length > maximum) return error.FileTooBig;
        return readRange(raw, a, path, 0, @intCast(length));
    }
    fn fileSize(raw: *anyopaque, path: []const u8) !u64 {
        return (try cast(raw).file(path)).size;
    }
    fn readOnly(_: *anyopaque, _: []const u8) !void {
        return error.ReadOnly;
    }
    fn writeOnly(_: *anyopaque, _: []const u8, _: []const u8) !void {
        return error.ReadOnly;
    }
    fn now(_: *anyopaque) u64 {
        return @import("antfly_platform").time.realtimeNs();
    }
    fn acquire(raw: *anyopaque) !storage.Storage.Lease {
        const self = cast(raw);
        self.retain();
        return self.lease();
    }
    fn list(raw: *anyopaque, a: A, path: []const u8) ![][]u8 {
        const self = cast(raw);
        const directory = if (std.mem.eql(u8, path, self.root)) "" else try self.relative(path);
        var names: std.ArrayList([]u8) = .empty;
        errdefer {
            for (names.items) |name| a.free(name);
            names.deinit(a);
        }
        for (self.files) |entry| {
            const parent = std.fs.path.dirname(entry.path) orelse "";
            if (std.mem.eql(u8, directory, parent)) try names.append(a, try a.dupe(u8, std.fs.path.basename(entry.path)));
        }
        return names.toOwnedSlice(a);
    }
    const PageSource = struct {
        a: A,
        owner: *Reader,
        entry: *const File,
        fn read(raw: *anyopaque, offset: u64, bytes: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.owner.readInto(self.entry, offset, bytes);
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.owner.release();
            self.a.destroy(self);
        }
    };
    fn openSource(raw: *anyopaque, a: A, path: []const u8) !source.Source {
        const self = cast(raw);
        const entry = try self.file(path);
        const page = try a.create(PageSource);
        page.* = .{ .a = a, .owner = self, .entry = entry };
        self.retain();
        return .{ .ranges = .{ .ptr = page, .length = entry.size, .read_into = PageSource.read, .close = PageSource.close } };
    }
    pub fn view(self: *Reader) storage.Storage {
        return .{ .ptr = self, .vtable = &.{ .acquire_lease = acquire, .create_dir_path = readOnly, .read_file_alloc = readFile, .read_file_range_alloc = readRange, .file_size = fileSize, .write_file_absolute = writeOnly, .rename_absolute = writeOnly, .delete_file_absolute = readOnly, .delete_tree = readOnly, .now_ns = now, .list_file_names_alloc = list, .open_immutable_source = openSource, .open_leased_immutable_source = openSource } };
    }
};

test "external lake remote retained storage fetches required chunks and pins source ownership" {
    const Fixture = struct {
        reads: usize = 0,
        fn fetch(raw: *anyopaque, a: A, ref: Chunk, cancellation: Cancellation) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try cancellation.check();
            self.reads += 1;
            return a.dupe(u8, ref.artifact_id);
        }
    };
    const a = std.testing.allocator;
    var fixture: Fixture = .{};
    const reader = try Reader.create(a, std.testing.io, "/logical/cut", &.{.{ .path = "runs/1.sst", .size = 8, .chunks = &.{ .{ .artifact_id = "abcd", .byte_len = 4, .checksum = "verified-by-provider" }, .{ .artifact_id = "efgh", .byte_len = 4, .checksum = "verified-by-provider" } } }}, .{ .ptr = &fixture, .read = Fixture.fetch }, .{ .parent = .none, .deadline_ns = std.math.maxInt(u64) });
    var lease = reader.lease();
    var paged = try lease.view.openLeasedImmutableSource(a, "/logical/cut/runs/1.sst");
    defer paged.close();
    const first = try lease.view.readFileRangeAlloc(a, "/logical/cut/runs/1.sst", 1, 2);
    defer a.free(first);
    try std.testing.expectEqualStrings("bc", first);
    try std.testing.expectEqual(@as(usize, 1), fixture.reads);
    try std.testing.expectError(error.ReadOnly, lease.view.deleteTree("/logical/cut"));
    try std.testing.expectError(error.FileNotFound, lease.view.fileSize("/logical/cut-other/runs/1.sst"));
    lease.deinit();
    var crossing: [4]u8 = undefined;
    try paged.readInto(2, &crossing);
    try std.testing.expectEqualStrings("cdef", &crossing);
    try std.testing.expectEqual(@as(usize, 2), fixture.reads);
    try paged.readInto(2, &crossing);
    try std.testing.expectEqual(@as(usize, 2), fixture.reads);
}

test "external lake remote retained storage rejects cancellation and malformed extent sizes" {
    const Fixture = struct {
        fn fetch(_: *anyopaque, _: A, _: Chunk, _: Cancellation) anyerror![]u8 {
            return error.TestUnexpectedResult;
        }
    };
    var marker: u8 = 0;
    const files = [_]File{.{ .path = "1.sst", .size = 2, .chunks = &.{.{ .artifact_id = "one", .byte_len = 3, .checksum = "digest" }} }};
    try std.testing.expectError(error.CatalogGenerationChanged, Reader.create(std.testing.allocator, std.testing.io, "/cut", &files, .{ .ptr = &marker, .read = Fixture.fetch }, .{ .parent = .none, .deadline_ns = std.math.maxInt(u64) }));
    const reader = try Reader.create(std.testing.allocator, std.testing.io, "/cut", &.{.{ .path = "1.sst", .size = 3, .chunks = files[0].chunks }}, .{ .ptr = &marker, .read = Fixture.fetch }, .{ .parent = .none, .deadline_ns = 0 });
    defer reader.release();
    try std.testing.expectError(error.DeadlineExceeded, reader.view().readFileRangeAlloc(std.testing.allocator, "/cut/1.sst", 0, 1));
}

test "external lake remote retained storage releases partial manifest ownership" {
    const Fixture = struct {
        fn fetch(_: *anyopaque, _: A, _: Chunk, _: Cancellation) anyerror![]u8 {
            return error.TestUnexpectedResult;
        }
        fn check(a: A) !void {
            var marker: u8 = 0;
            const chunk: Chunk = .{ .artifact_id = "immutable", .byte_len = 3, .checksum = "digest" };
            const reader = try Reader.create(a, std.testing.io, "/cut", &.{
                .{ .path = "runs/1.sst", .size = 3, .chunks = &.{chunk} },
                .{ .path = "runs/2.sst", .size = 3, .chunks = &.{chunk} },
            }, .{ .ptr = &marker, .read = fetch }, .{ .parent = .none, .deadline_ns = std.math.maxInt(u64) });
            defer reader.release();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
}
