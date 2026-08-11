// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const scraping = @import("antfly_scraping");
const config_mod = @import("config.zig");
const secrets = @import("secrets.zig");
const platform_sync = @import("antfly_platform").sync;
const platform_time = @import("antfly_platform").time;

const request_refresh_interval_ns: u64 = std.time.ns_per_s;
const health_refresh_interval_ns: u64 = 500 * std.time.ns_per_ms;

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
    /// Protects only publication state. File I/O and candidate parsing never
    /// run while this mutex is held, so existing readers remain wait-free with
    /// respect to a slow or malformed replacement.
    publish_mutex: std.atomic.Mutex = .unlocked,
    /// Serializes reload attempts. Request-path refreshes use tryLock and never
    /// wait behind another reload.
    refresh_mutex: std.atomic.Mutex = .unlocked,
    next_request_refresh_ns: std.atomic.Value(u64) = .init(0),
    next_health_refresh_ns: std.atomic.Value(u64) = .init(0),
    current: *PublishedSnapshot,
    observed_metadata: FileMetadata,
    observed_hash: [std.crypto.hash.sha2.Sha256.digest_length]u8,
    observed_candidate_valid: bool = true,
    startup_static_hash: [std.crypto.hash.sha2.Sha256.digest_length]u8,
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
        try validateRemoteContentConfig(&config);
        const startup_static_hash = try staticConfigHash(alloc, image.raw);
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
            .startup_static_hash = startup_static_hash,
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
        platform_sync.lockYielding(&self.refresh_mutex);
        defer self.refresh_mutex.unlock();
        return self.refreshIfChangedOwned(true);
    }

    fn refreshIfChangedForHealth(self: *Runtime) void {
        const now_ns = platform_time.monotonicNs();
        const scheduled_ns = self.next_health_refresh_ns.load(.acquire);
        if (scheduled_ns != 0 and now_ns < scheduled_ns) return;
        if (self.next_health_refresh_ns.cmpxchgStrong(
            scheduled_ns,
            now_ns +| health_refresh_interval_ns,
            .acq_rel,
            .acquire,
        ) != null) return;
        if (!self.refresh_mutex.tryLock()) return;
        defer self.refresh_mutex.unlock();
        _ = self.refreshIfChangedOwned(true);
    }

    /// Opportunistically checks for a replacement at a bounded rate. Only the
    /// request that wins both the interval CAS and refresh try-lock performs
    /// I/O; all other requests immediately acquire the current snapshot.
    fn refreshIfChangedForRequest(self: *Runtime) void {
        const now_ns = platform_time.monotonicNs();
        const scheduled_ns = self.next_request_refresh_ns.load(.acquire);
        if (scheduled_ns != 0 and now_ns < scheduled_ns) return;
        if (self.next_request_refresh_ns.cmpxchgStrong(
            scheduled_ns,
            now_ns +| request_refresh_interval_ns,
            .acq_rel,
            .acquire,
        ) != null) return;
        if (!self.refresh_mutex.tryLock()) return;
        defer self.refresh_mutex.unlock();
        _ = self.refreshIfChangedOwned(false);
    }

    fn refreshIfChangedOwned(self: *Runtime, force_content_check: bool) bool {
        if (!force_content_check) {
            const metadata = statFileMetadata(self.alloc, self.path) catch |err| {
                platform_sync.lockYielding(&self.publish_mutex);
                defer self.publish_mutex.unlock();
                self.markFailedLocked(err);
                return false;
            };
            platform_sync.lockYielding(&self.publish_mutex);
            const identity_unchanged = self.observed_metadata.eql(metadata);
            self.publish_mutex.unlock();
            if (identity_unchanged) return false;
        }

        var image = readFileImage(self.alloc, self.path) catch |err| {
            platform_sync.lockYielding(&self.publish_mutex);
            defer self.publish_mutex.unlock();
            self.markFailedLocked(err);
            return false;
        };
        defer image.deinit(self.alloc);
        platform_sync.lockYielding(&self.publish_mutex);
        const unchanged = self.observed_metadata.eql(image.metadata) and std.mem.eql(u8, &self.observed_hash, &image.hash);
        self.publish_mutex.unlock();
        if (unchanged) {
            platform_sync.lockYielding(&self.publish_mutex);
            defer self.publish_mutex.unlock();
            if (self.observed_candidate_valid) self.last_reload_failed = false;
            return false;
        }

        var next_config = config_mod.Config.parseFromSliceWithSecretsForDeployment(
            self.alloc,
            image.raw,
            self.secret_store,
            self.expected_deployment,
        ) catch |err| {
            platform_sync.lockYielding(&self.publish_mutex);
            defer self.publish_mutex.unlock();
            self.observed_metadata = image.metadata;
            self.observed_hash = image.hash;
            self.observed_candidate_valid = false;
            self.markFailedLocked(err);
            return false;
        };
        validateRemoteContentConfig(&next_config) catch |err| {
            next_config.deinit();
            platform_sync.lockYielding(&self.publish_mutex);
            defer self.publish_mutex.unlock();
            self.observed_metadata = image.metadata;
            self.observed_hash = image.hash;
            self.observed_candidate_valid = false;
            self.markFailedLocked(err);
            return false;
        };
        const next_static_hash = staticConfigHash(self.alloc, image.raw) catch |err| {
            next_config.deinit();
            platform_sync.lockYielding(&self.publish_mutex);
            defer self.publish_mutex.unlock();
            self.observed_metadata = image.metadata;
            self.observed_hash = image.hash;
            self.observed_candidate_valid = false;
            self.markFailedLocked(err);
            return false;
        };
        if (!std.mem.eql(u8, &self.startup_static_hash, &next_static_hash)) {
            next_config.deinit();
            platform_sync.lockYielding(&self.publish_mutex);
            defer self.publish_mutex.unlock();
            self.observed_metadata = image.metadata;
            self.observed_hash = image.hash;
            self.observed_candidate_valid = false;
            self.markFailedLocked(error.RestartRequiredConfigChange);
            return false;
        }
        const next = self.alloc.create(PublishedSnapshot) catch |err| {
            next_config.deinit();
            platform_sync.lockYielding(&self.publish_mutex);
            defer self.publish_mutex.unlock();
            self.markFailedLocked(err);
            return false;
        };

        platform_sync.lockYielding(&self.publish_mutex);
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
        self.observed_candidate_valid = true;
        self.last_reload_failed = false;
        self.reload_successes +%= 1;
        self.publish_mutex.unlock();
        previous.release();
        return true;
    }

    pub fn health(self: *Runtime) Health {
        self.refreshIfChangedForHealth();
        platform_sync.lockYielding(&self.publish_mutex);
        defer self.publish_mutex.unlock();
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
        self.refreshIfChangedForRequest();
        platform_sync.lockYielding(&self.publish_mutex);
        defer self.publish_mutex.unlock();
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

/// Validate the routing invariants that the generated OpenAPI structs cannot
/// express. A syntactically valid but incomplete replacement must never evict
/// a working credential snapshot.
fn validateRemoteContentConfig(config: *const config_mod.Config) !void {
    const cfg = if (config.remote_content) |*remote_content| remote_content else return;
    if (cfg.security) |security| validateContentSecurityConfig(security) catch
        return error.InvalidRemoteContentConfig;

    if (cfg.default_s3) |name| {
        if (name.len == 0 or cfg.getS3(name) == null) return error.InvalidRemoteContentConfig;
    }

    var s3_it = cfg.s3.iterator();
    while (s3_it.next()) |entry| {
        if (entry.key_ptr.*.len == 0) return error.InvalidRemoteContentConfig;
        const credential = entry.value_ptr;
        if (credential.security) |security| validateContentSecurityConfig(security) catch
            return error.InvalidRemoteContentConfig;
        if (credential.access_key_id) |value| if (value.len == 0) return error.InvalidRemoteContentConfig;
        if (credential.secret_access_key) |value| if (value.len == 0) return error.InvalidRemoteContentConfig;
        if (credential.buckets) |patterns| {
            for (patterns) |pattern| {
                if (pattern.len == 0) return error.InvalidRemoteContentConfig;
                if (std.mem.indexOfScalar(u8, pattern, '*')) |first_star| {
                    if (std.mem.indexOfScalarPos(u8, pattern, first_star + 1, '*') != null) {
                        return error.InvalidRemoteContentConfig;
                    }
                }
            }
        }
    }

    var http_it = cfg.http.iterator();
    while (http_it.next()) |entry| {
        if (entry.key_ptr.*.len == 0) return error.InvalidRemoteContentConfig;
        const credential = entry.value_ptr;
        if (credential.security) |security| validateContentSecurityConfig(security) catch
            return error.InvalidRemoteContentConfig;
        if (credential.base_url) |base_url| {
            const parsed = std.Uri.parse(base_url) catch return error.InvalidRemoteContentConfig;
            if ((!std.ascii.eqlIgnoreCase(parsed.scheme, "http") and
                !std.ascii.eqlIgnoreCase(parsed.scheme, "https")) or parsed.host == null)
            {
                return error.InvalidRemoteContentConfig;
            }
            if (parsed.user != null or parsed.password != null or parsed.query != null or parsed.fragment != null)
                return error.InvalidRemoteContentConfig;
        }
        var header_it = credential.headers.iterator();
        while (header_it.next()) |header| {
            if (!validHttpHeaderName(header.key_ptr.*) or
                std.mem.indexOfAny(u8, header.value_ptr.*, "\r\n") != null)
            {
                return error.InvalidRemoteContentConfig;
            }
        }
    }
}

fn validateContentSecurityConfig(security: scraping.ContentSecurityConfig) !void {
    try scraping.validateNat64Prefixes(security);
}

fn validHttpHeaderName(name: []const u8) bool {
    if (name.len == 0) return false;
    for (name) |ch| switch (ch) {
        'a'...'z', 'A'...'Z', '0'...'9', '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~' => {},
        else => return false,
    };
    return true;
}

/// Fingerprint the startup-only portion of config.json. A process may publish
/// a new full-file hash only when every field outside `remote_content` is
/// semantically unchanged; otherwise the operator's pod-template hash remains
/// the acknowledgement mechanism and the current process reports stale.
fn staticConfigHash(alloc: std.mem.Allocator, raw: []const u8) ![std.crypto.hash.sha2.Sha256.digest_length]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    switch (parsed.value) {
        .object => |*root| _ = root.orderedRemove("remote_content"),
        else => return error.InvalidConfig,
    }
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(parsed.value, .{})});
    defer alloc.free(encoded);
    var hash: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(encoded, &hash, .{});
    return hash;
}

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

fn statFileMetadata(alloc: std.mem.Allocator, path: []const u8) !FileMetadata {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    return .{
        .inode = stat.inode,
        .size = stat.size,
        .mtime_ns = stat.mtime.toNanoseconds(),
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
    try std.testing.expect(runtime.refreshIfChanged());

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
    try std.testing.expect(runtime.refreshIfChanged());
    var published_again = facade.acquire();
    published_again.deinit();
    while (concurrent_reader.reads.load(.acquire) < 40) std.Thread.yield() catch {};
    concurrent_reader.stop.store(true, .release);
    first_thread.join();
    second_thread.join();
    try std.testing.expect(!concurrent_reader.invalid.load(.acquire));

    try writeTestConfigAtomically(path, "{not-json");
    try std.testing.expect(!runtime.refreshIfChanged());
    var after_malformed = facade.acquire();
    defer after_malformed.deinit();
    try std.testing.expectEqualStrings("primary", after_malformed.config.default_s3.?);
    const stale_health = runtime.health();
    try std.testing.expect(stale_health.last_reload_failed);
    try std.testing.expect(stale_health.stale_snapshot);
    try std.testing.expectEqual(initial_health.generation + 2, stale_health.generation);
}

test "remote content runtime rejects incomplete and startup-only replacements" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/config.json", .{tmp.sub_path});
    defer alloc.free(path);

    try writeTestConfigAtomically(path,
        \\{"health_port":8081,"remote_content":{"default_s3":"primary","s3":{"primary":{"access_key_id":"access","secret_access_key":"secret"}}}}
    );
    var runtime = try Runtime.init(alloc, path, null, null);
    defer runtime.deinit();
    var facade = scraping.RemoteContentConfig{};
    runtime.attach(&facade);
    const initial = runtime.health();

    // A startup-only change must not advance the full-file acknowledgement
    // hash because this runtime publishes only the remote-content projection.
    try writeTestConfigAtomically(path,
        \\{"health_port":8082,"remote_content":{"default_s3":"archive","s3":{"archive":{"access_key_id":"access","secret_access_key":"secret"}}}}
    );
    try std.testing.expect(!runtime.refreshIfChanged());
    var after_static_change = facade.acquire();
    defer after_static_change.deinit();
    try std.testing.expectEqualStrings("primary", after_static_change.config.default_s3.?);
    const static_failure = runtime.health();
    try std.testing.expectEqual(initial.generation, static_failure.generation);
    try std.testing.expectEqualSlices(u8, &initial.hash, &static_failure.hash);
    try std.testing.expect(static_failure.stale_snapshot);

    // Structurally incomplete routing is also rejected instead of silently
    // dropping credentials from live requests.
    try writeTestConfigAtomically(path,
        \\{"health_port":8081,"remote_content":{"default_s3":"missing","s3":{}}}
    );
    try std.testing.expect(!runtime.refreshIfChanged());
    var after_incomplete = facade.acquire();
    defer after_incomplete.deinit();
    try std.testing.expectEqualStrings("primary", after_incomplete.config.default_s3.?);
    try std.testing.expect(runtime.health().stale_snapshot);

    // Translation prefixes are security policy, so an invalid or ambiguous
    // prefix must fail closed without replacing the last working snapshot.
    try writeTestConfigAtomically(path,
        \\{"health_port":8081,"remote_content":{"security":{"nat64_prefixes":["2001:470::/72"]},"default_s3":"archive","s3":{"archive":{}}}}
    );
    try std.testing.expect(!runtime.refreshIfChanged());
    var after_invalid_nat64 = facade.acquire();
    defer after_invalid_nat64.deinit();
    try std.testing.expectEqualStrings("primary", after_invalid_nat64.config.default_s3.?);
    try std.testing.expect(runtime.health().stale_snapshot);

    // Once a complete remote-content-only candidate arrives, publication and
    // the exact full-file hash advance together. Credential fields are
    // optional because request-time resolution can use the standard AWS
    // environment fallback.
    try writeTestConfigAtomically(path,
        \\{"health_port":8081,"remote_content":{"default_s3":"archive","s3":{"archive":{}}}}
    );
    try std.testing.expect(runtime.refreshIfChanged());
    var recovered = facade.acquire();
    defer recovered.deinit();
    try std.testing.expectEqualStrings("archive", recovered.config.default_s3.?);
    const recovered_health = runtime.health();
    try std.testing.expectEqual(initial.generation + 1, recovered_health.generation);
    try std.testing.expect(!recovered_health.stale_snapshot);
}
