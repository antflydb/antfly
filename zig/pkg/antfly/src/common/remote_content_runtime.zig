// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const scraping = @import("antfly_scraping");
const config_mod = @import("config.zig");
const secrets = @import("secrets.zig");
const platform_sync = @import("antfly_platform").sync;

const FileMetadata = struct {
    inode: std.Io.File.INode,
    size: u64,
    mtime_ns: i128,

    fn eql(self: FileMetadata, other: FileMetadata) bool {
        return self.inode == other.inode and self.size == other.size and self.mtime_ns == other.mtime_ns;
    }
};

const PublishedSnapshot = struct {
    alloc: std.mem.Allocator,
    refs: std.atomic.Value(usize) = .init(1),
    config: config_mod.Config,
    empty_remote_content: scraping.RemoteContentConfig = .{},
    generation: u64,
    hash: [std.crypto.hash.sha2.Sha256.digest_length]u8,

    fn remoteContent(self: *const PublishedSnapshot) *const scraping.RemoteContentConfig {
        if (self.config.remote_content) |*remote_content| return remote_content;
        return &self.empty_remote_content;
    }

    fn retain(self: *PublishedSnapshot) void {
        _ = self.refs.fetchAdd(1, .acq_rel);
    }

    fn release(self: *PublishedSnapshot) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        self.config.deinit();
        self.alloc.destroy(self);
    }
};

pub const Health = struct {
    generation: u64,
    hash: [std.crypto.hash.sha2.Sha256.digest_length]u8,
    last_reload_failed: bool,
    stale_snapshot: bool,
    reload_successes: u64,
    reload_failures: u64,
};

/// Owns and atomically publishes complete, validated config.json snapshots.
/// Existing API/storage objects keep only the stable scraping facade returned
/// by `attach`; each actual remote fetch acquires a ref-counted snapshot.
pub const Runtime = struct {
    alloc: std.mem.Allocator,
    path: []u8,
    secret_store: ?*secrets.FileStore,
    expected_deployment: ?config_mod.DeploymentMode,
    mutex: std.atomic.Mutex = .unlocked,
    current: *PublishedSnapshot,
    observed_metadata: FileMetadata,
    observed_hash: [std.crypto.hash.sha2.Sha256.digest_length]u8,
    last_reload_failed: bool = false,
    reload_successes: u64 = 1,
    reload_failures: u64 = 0,

    pub fn init(
        alloc: std.mem.Allocator,
        path: []const u8,
        secret_store: ?*secrets.FileStore,
        expected_deployment: ?config_mod.DeploymentMode,
    ) !Runtime {
        const owned_path = try alloc.dupe(u8, path);
        errdefer alloc.free(owned_path);
        var image = try readFileImage(alloc, path);
        defer image.deinit(alloc);
        var config = try config_mod.Config.parseFromSliceWithSecretsForDeployment(alloc, image.raw, secret_store, expected_deployment);
        errdefer config.deinit();
        const snapshot = try alloc.create(PublishedSnapshot);
        snapshot.* = .{
            .alloc = alloc,
            .config = config,
            .generation = 1,
            .hash = image.hash,
        };
        return .{
            .alloc = alloc,
            .path = owned_path,
            .secret_store = secret_store,
            .expected_deployment = expected_deployment,
            .current = snapshot,
            .observed_metadata = image.metadata,
            .observed_hash = image.hash,
        };
    }

    pub fn deinit(self: *Runtime) void {
        self.current.release();
        self.alloc.free(self.path);
        self.* = undefined;
    }

    pub fn attach(self: *Runtime, facade: *scraping.RemoteContentConfig) void {
        facade.runtime = .{
            .context = self,
            .acquire_fn = acquireAdapter,
            .health_fn = healthAdapter,
        };
    }

    pub fn refreshIfChanged(self: *Runtime) bool {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();

        var image = readFileImage(self.alloc, self.path) catch |err| {
            self.markFailedLocked(err);
            return false;
        };
        defer image.deinit(self.alloc);
        if (self.observed_metadata.eql(image.metadata) and std.mem.eql(u8, &self.observed_hash, &image.hash)) return false;

        var next_config = config_mod.Config.parseFromSliceWithSecretsForDeployment(
            self.alloc,
            image.raw,
            self.secret_store,
            self.expected_deployment,
        ) catch |err| {
            self.observed_metadata = image.metadata;
            self.observed_hash = image.hash;
            self.markFailedLocked(err);
            return false;
        };
        errdefer next_config.deinit();
        const next = self.alloc.create(PublishedSnapshot) catch |err| {
            next_config.deinit();
            self.markFailedLocked(err);
            return false;
        };
        next.* = .{
            .alloc = self.alloc,
            .config = next_config,
            .generation = self.current.generation +% 1,
            .hash = image.hash,
        };

        const previous = self.current;
        self.current = next;
        self.observed_metadata = image.metadata;
        self.observed_hash = image.hash;
        self.last_reload_failed = false;
        self.reload_successes +%= 1;
        previous.release();
        return true;
    }

    pub fn health(self: *Runtime) Health {
        _ = self.refreshIfChanged();
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        return .{
            .generation = self.current.generation,
            .hash = self.current.hash,
            .last_reload_failed = self.last_reload_failed,
            .stale_snapshot = self.last_reload_failed,
            .reload_successes = self.reload_successes,
            .reload_failures = self.reload_failures,
        };
    }

    fn acquire(self: *Runtime) scraping.RemoteContentConfig.Snapshot {
        _ = self.refreshIfChanged();
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.current.retain();
        return .{
            .config = self.current.remoteContent(),
            .context = self.current,
            .release_fn = releaseAdapter,
        };
    }

    fn markFailedLocked(self: *Runtime, err: anyerror) void {
        if (!self.last_reload_failed) {
            self.reload_failures +%= 1;
            std.log.warn("config reload failed; keeping last known good remote-content snapshot path={s} err={}", .{ self.path, err });
        }
        self.last_reload_failed = true;
    }

    fn acquireAdapter(context: *anyopaque) ?scraping.RemoteContentConfig.Snapshot {
        const self: *Runtime = @ptrCast(@alignCast(context));
        return self.acquire();
    }

    fn healthAdapter(context: *anyopaque) scraping.RemoteContentConfig.RuntimeHealth {
        const self: *Runtime = @ptrCast(@alignCast(context));
        const value = self.health();
        return .{
            .generation = value.generation,
            .hash = value.hash,
            .last_reload_failed = value.last_reload_failed,
            .stale_snapshot = value.stale_snapshot,
            .reload_successes = value.reload_successes,
            .reload_failures = value.reload_failures,
        };
    }

    fn releaseAdapter(context: *anyopaque) void {
        const snapshot: *PublishedSnapshot = @ptrCast(@alignCast(context));
        snapshot.release();
    }
};

const FileImage = struct {
    raw: []u8,
    metadata: FileMetadata,
    hash: [std.crypto.hash.sha2.Sha256.digest_length]u8,

    fn deinit(self: *FileImage, alloc: std.mem.Allocator) void {
        alloc.free(self.raw);
        self.* = undefined;
    }
};

fn readFileImage(alloc: std.mem.Allocator, path: []const u8) !FileImage {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    var read_buffer: [4096]u8 = undefined;
    var reader = file.reader(io, &read_buffer);
    const raw = try reader.interface.allocRemaining(alloc, .limited(16 * 1024 * 1024));
    errdefer alloc.free(raw);

    var hash: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &hash, .{});
    return .{
        .raw = raw,
        .metadata = .{
            .inode = stat.inode,
            .size = stat.size,
            .mtime_ns = stat.mtime.toNanoseconds(),
        },
        .hash = hash,
    };
}

fn writeTestConfigAtomically(path: []const u8, contents: []const u8) !void {
    const alloc = std.testing.allocator;
    const next_path = try std.fmt.allocPrint(alloc, "{s}.next", .{path});
    defer alloc.free(next_path);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    {
        var file = try std.Io.Dir.cwd().createFile(io, next_path, .{ .truncate = true });
        defer file.close(io);
        var buffer: [4096]u8 = undefined;
        var writer = file.writer(io, &buffer);
        try writer.interface.writeAll(contents);
        try writer.end();
    }
    try std.Io.Dir.rename(std.Io.Dir.cwd(), next_path, std.Io.Dir.cwd(), path, io);
}

fn writeTestConfigInPlacePreservingMtime(path: []const u8, contents: []const u8, mtime: std.Io.Timestamp) !void {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    {
        var file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true });
        defer file.close(io);
        var buffer: [4096]u8 = undefined;
        var writer = file.writer(io, &buffer);
        try writer.interface.writeAll(contents);
        try writer.end();
    }
    try std.Io.Dir.cwd().setTimestamps(io, path, .{ .modify_timestamp = .{ .new = mtime } });
}

test "remote content runtime publishes validated snapshots and retains readers" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/config.json", .{tmp.sub_path});
    defer alloc.free(path);

    try writeTestConfigAtomically(path,
        \\{"remote_content":{"default_s3":"primary","s3":{"primary":{"access_key_id":"access","secret_access_key":"secret"}}}}
    );
    var runtime = try Runtime.init(alloc, path, null, null);
    defer runtime.deinit();
    var facade = scraping.RemoteContentConfig{};
    runtime.attach(&facade);

    var held = facade.acquire();
    defer held.deinit();
    try std.testing.expectEqualStrings("primary", held.config.default_s3.?);
    const initial_health = runtime.health();
    const initial_metadata = runtime.observed_metadata;

    const replacement =
        \\{"remote_content":{"default_s3":"archive","s3":{"archive":{"access_key_id":"access","secret_access_key":"secret"}}}}
    ;
    try std.testing.expectEqual(initial_metadata.size, @as(u64, @intCast(replacement.len)));
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const initial_stat = try std.Io.Dir.cwd().statFile(io_impl.io(), path, .{});
    try writeTestConfigInPlacePreservingMtime(path, replacement, initial_stat.mtime);
    var replacement_image = try readFileImage(alloc, path);
    defer replacement_image.deinit(alloc);
    try std.testing.expect(initial_metadata.eql(replacement_image.metadata));
    try std.testing.expect(!std.mem.eql(u8, &initial_health.hash, &replacement_image.hash));

    var current = facade.acquire();
    defer current.deinit();
    try std.testing.expectEqualStrings("archive", current.config.default_s3.?);
    try std.testing.expectEqualStrings("primary", held.config.default_s3.?);
    try std.testing.expectEqual(initial_health.generation + 1, runtime.health().generation);

    const ConcurrentReader = struct {
        facade: *scraping.RemoteContentConfig,
        stop: std.atomic.Value(bool) = .init(false),
        reads: std.atomic.Value(usize) = .init(0),
        invalid: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            while (!self.stop.load(.acquire)) {
                var snapshot = self.facade.acquire();
                const name = snapshot.config.default_s3 orelse "";
                if (!std.mem.eql(u8, name, "primary") and !std.mem.eql(u8, name, "archive")) {
                    self.invalid.store(true, .release);
                }
                snapshot.deinit();
                _ = self.reads.fetchAdd(1, .release);
            }
        }
    };
    var concurrent_reader = ConcurrentReader{ .facade = &facade };
    const first_thread = try std.Thread.spawn(.{}, ConcurrentReader.run, .{&concurrent_reader});
    const second_thread = try std.Thread.spawn(.{}, ConcurrentReader.run, .{&concurrent_reader});
    while (concurrent_reader.reads.load(.acquire) < 20) std.Thread.yield() catch {};
    try writeTestConfigAtomically(path,
        \\{"remote_content":{"default_s3":"primary","s3":{"primary":{"access_key_id":"access","secret_access_key":"secret"}}}}
    );
    var published_again = facade.acquire();
    published_again.deinit();
    while (concurrent_reader.reads.load(.acquire) < 40) std.Thread.yield() catch {};
    concurrent_reader.stop.store(true, .release);
    first_thread.join();
    second_thread.join();
    try std.testing.expect(!concurrent_reader.invalid.load(.acquire));

    try writeTestConfigAtomically(path, "{not-json");
    var after_malformed = facade.acquire();
    defer after_malformed.deinit();
    try std.testing.expectEqualStrings("primary", after_malformed.config.default_s3.?);
    const stale_health = runtime.health();
    try std.testing.expect(stale_health.last_reload_failed);
    try std.testing.expect(stale_health.stale_snapshot);
    try std.testing.expectEqual(initial_health.generation + 2, stale_health.generation);
}
