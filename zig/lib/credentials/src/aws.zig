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

//! Native AWS credential discovery and ref-counted cache, independent of model providers.
const std = @import("std");
const builtin = @import("builtin");
const cloud_credentials = @import("cloud.zig");
const httpx = @import("httpx");
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const credential_source_identity = @import("root.zig");
const HeaderPair = [2][]const u8;
const imds_default_endpoint = "http://169.254.169.254";
const ecs_credentials_endpoint = "http://169.254.170.2";

pub const Credentials = struct {
    access_key_id: []const u8,
    secret_access_key: []const u8,
    session_token: ?[]const u8 = null,
    expires_at_unix: ?u64 = null,

    pub fn deinit(self: *Credentials, alloc: std.mem.Allocator) void {
        alloc.free(self.access_key_id);
        alloc.free(self.secret_access_key);
        if (self.session_token) |value| alloc.free(value);
        self.* = undefined;
    }

    fn clone(self: Credentials, alloc: std.mem.Allocator) !Credentials {
        return try dupCredentials(alloc, self.access_key_id, self.secret_access_key, self.session_token, self.expires_at_unix);
    }

    fn isFresh(self: Credentials, now_unix: u64) bool {
        const refresh_skew_seconds: u64 = 300;
        return if (self.expires_at_unix) |expires|
            expires > now_unix + refresh_skew_seconds
        else
            true;
    }

    fn isUnexpired(self: Credentials, now_unix: u64) bool {
        return if (self.expires_at_unix) |expires| expires > now_unix else true;
    }
};

pub const ProfileCredentialSource = struct {
    name: []const u8,
    shared_credentials_file: ?[]const u8 = null,
};

pub const WebIdentityCredentialSource = struct {
    role_arn: []const u8,
    token_file: []const u8,
    session_name: []const u8 = "antfly",
    sts_endpoint: ?[]const u8 = null,
};

pub const CredentialSource = union(enum) {
    default,
    profile: ProfileCredentialSource,
    web_identity: WebIdentityCredentialSource,
};

/// One absolute control context shared by credential discovery and the model
/// invocation. Computing a residual timeout for every network hop prevents a
/// credential refresh from restarting the caller's timeout budget.
pub const RequestContext = struct {
    deadline_ns: ?i96 = null,
    cancellation: ?CancellationToken = null,

    pub fn init(io: std.Io, timeout_ms: ?u64, cancellation: ?CancellationToken) RequestContext {
        return initAt(std.Io.Timestamp.now(io, .awake), timeout_ms, cancellation);
    }

    pub fn initAt(now: std.Io.Timestamp, timeout_ms: ?u64, cancellation: ?CancellationToken) RequestContext {
        const deadline_ns: ?i96 = if (timeout_ms) |timeout|
            if (timeout == 0)
                null
            else
                now.toNanoseconds() +| (std.math.mul(i96, @intCast(timeout), std.time.ns_per_ms) catch std.math.maxInt(i96))
        else
            null;
        return .{ .deadline_ns = deadline_ns, .cancellation = cancellation };
    }

    pub fn isBounded(self: RequestContext) bool {
        return self.deadline_ns != null or self.cancellation != null;
    }

    pub fn check(self: RequestContext, io: std.Io) !void {
        if (self.cancellation) |token| if (token.isCancelled()) return error.Cancelled;
        if (self.deadline_ns) |deadline| {
            if (std.Io.Timestamp.now(io, .awake).toNanoseconds() >= deadline) return error.Timeout;
        }
    }

    pub fn remainingTimeoutMs(self: RequestContext, io: std.Io) !?u64 {
        return self.remainingTimeoutMsAt(std.Io.Timestamp.now(io, .awake));
    }

    pub fn remainingTimeoutMsAt(self: RequestContext, now: std.Io.Timestamp) !?u64 {
        if (self.cancellation) |token| if (token.isCancelled()) return error.Cancelled;
        const deadline = self.deadline_ns orelse return null;
        const remaining_ns = deadline - now.toNanoseconds();
        if (remaining_ns <= 0) return error.Timeout;
        return @intCast(@max(
            @as(i96, 1),
            @divTrunc(remaining_ns +| std.time.ns_per_ms - 1, std.time.ns_per_ms),
        ));
    }

    pub fn httpCancellation(self: RequestContext) ?httpx.CancellationToken {
        const token = self.cancellation orelse return null;
        const ptr = token.ptr orelse return null;
        const callback = token.is_cancelled_fn orelse return null;
        return httpx.CancellationToken.fromCallback(ptr, callback);
    }

    fn waitForRefresh(self: RequestContext, io: std.Io) !void {
        try self.check(io);
        const remaining_ms = try self.remainingTimeoutMs(io);
        const poll_ms: u64 = @min(remaining_ms orelse 5, 5);
        io.sleep(.fromMilliseconds(@intCast(@max(poll_ms, 1))), .awake) catch |err| switch (err) {
            error.Canceled => return error.Cancelled,
        };
        try self.check(io);
    }
};

pub const CredentialCache = struct {
    const Snapshot = struct {
        alloc: std.mem.Allocator,
        refs: std.atomic.Value(usize) = .init(1), // one cache-owned reference
        source_key: [std.crypto.hash.sha2.Sha256.digest_length]u8,
        credentials: Credentials,

        fn retain(self: *Snapshot) void {
            _ = self.refs.fetchAdd(1, .monotonic);
        }

        fn release(self: *Snapshot) void {
            if (self.refs.fetchSub(1, .acq_rel) != 1) return;
            var credentials = self.credentials;
            credentials.deinit(self.alloc);
            const alloc = self.alloc;
            alloc.destroy(self);
        }
    };

    pub const Lease = struct {
        snapshot: *Snapshot,

        pub fn credentials(self: Lease) Credentials {
            return self.snapshot.credentials;
        }

        pub fn release(self: *Lease) void {
            self.snapshot.release();
            self.* = undefined;
        }

        pub fn releaseOpaque(ptr: *anyopaque) void {
            const snapshot: *Snapshot = @ptrCast(@alignCast(ptr));
            snapshot.release();
        }

        pub fn releaseContext(self: Lease) *anyopaque {
            return self.snapshot;
        }
    };

    io_init_mutex: std.atomic.Mutex = .unlocked,
    closed: std.atomic.Value(bool) = .init(false),
    io: ?std.Io = null,
    mutex: std.Io.Mutex = .init,
    refreshed: std.Io.Condition = .init,
    refreshing: bool = false,
    closing: bool = false,
    active_calls: usize = 0,
    cached: ?*Snapshot = null,

    pub fn deinit(self: *CredentialCache, alloc: std.mem.Allocator) void {
        _ = alloc;
        self.closed.store(true, .release);
        while (!self.io_init_mutex.tryLock()) std.atomic.spinLoopHint();
        const maybe_io = self.io;
        self.io_init_mutex.unlock();
        const io = maybe_io orelse return;
        self.mutex.lockUncancelable(io);
        self.closing = true;
        self.refreshed.broadcast(io);
        while (self.refreshing or self.active_calls != 0) self.refreshed.waitUncancelable(io, &self.mutex);
        const cached = self.cached;
        self.cached = null;
        self.mutex.unlock(io);
        if (cached) |snapshot| snapshot.release();
    }

    pub fn get(self: *CredentialCache, alloc: std.mem.Allocator, http: *httpx.Client, region: []const u8) !Credentials {
        return try self.getWithContext(alloc, std.heap.smp_allocator, http, region, .{});
    }

    pub fn getForSource(self: *CredentialCache, alloc: std.mem.Allocator, http: *httpx.Client, region: []const u8, source: CredentialSource) !Credentials {
        return try self.getForSourceWithContext(alloc, std.heap.smp_allocator, http, region, source, .{});
    }

    fn getWithContext(self: *CredentialCache, result_alloc: std.mem.Allocator, cache_alloc: std.mem.Allocator, http: *httpx.Client, region: []const u8, context: RequestContext) !Credentials {
        var lease = try self.getLeaseForSourceWithContext(cache_alloc, http, region, .default, context);
        defer lease.release();
        return try lease.credentials().clone(result_alloc);
    }

    pub fn getForSourceWithContext(self: *CredentialCache, result_alloc: std.mem.Allocator, cache_alloc: std.mem.Allocator, http: *httpx.Client, region: []const u8, source: CredentialSource, context: RequestContext) !Credentials {
        var lease = try self.getLeaseForSourceWithContext(cache_alloc, http, region, source, context);
        defer lease.release();
        return try lease.credentials().clone(result_alloc);
    }

    /// Returns a ref-counted immutable credential snapshot. Storage clients use
    /// this path so cached requests avoid serialized key copies and heap churn;
    /// the lease keeps credentials alive across signing even when a concurrent
    /// refresh replaces the cache entry. The allocator argument remains for API
    /// compatibility; cache-owned snapshots always use the process thread-safe
    /// allocator because they outlive requests and cross worker threads.
    pub fn getLeaseForSource(self: *CredentialCache, _: std.mem.Allocator, http: *httpx.Client, region: []const u8, source: CredentialSource) !Lease {
        return try self.getLeaseForSourceWithContext(std.heap.smp_allocator, http, region, source, .{});
    }

    fn getLeaseForSourceWithContext(self: *CredentialCache, alloc: std.mem.Allocator, http: *httpx.Client, region: []const u8, source: CredentialSource, context: RequestContext) !Lease {
        return try self.getLeaseForSourceWithContextAndIo(alloc, http, null, region, source, context);
    }

    pub fn getLeaseForSourceWithIo(
        self: *CredentialCache,
        _: std.mem.Allocator,
        http: *httpx.Client,
        filesystem_io: ?std.Io,
        region: []const u8,
        source: CredentialSource,
    ) !Lease {
        return try self.getLeaseForSourceWithContextAndIo(
            std.heap.smp_allocator,
            http,
            filesystem_io,
            region,
            source,
            .{},
        );
    }

    fn getLeaseForSourceWithContextAndIo(
        self: *CredentialCache,
        alloc: std.mem.Allocator,
        http: *httpx.Client,
        filesystem_io: ?std.Io,
        region: []const u8,
        source: CredentialSource,
        context: RequestContext,
    ) !Lease {
        const source_key = credentialSourceKey(region, source);
        const io = try self.bindIo(http.io);
        try context.check(io);
        self.mutex.lockUncancelable(io);
        if (self.closing) {
            self.mutex.unlock(io);
            return error.CredentialCacheClosed;
        }
        self.active_calls += 1;
        self.mutex.unlock(io);
        defer self.finishCall(io);
        while (true) {
            const now = try currentUnixSeconds();
            self.mutex.lockUncancelable(io);
            if (self.closing) {
                self.mutex.unlock(io);
                return error.CredentialCacheClosed;
            }
            if (self.cached) |snapshot| {
                if (std.mem.eql(u8, &snapshot.source_key, &source_key) and snapshot.credentials.isFresh(now)) {
                    snapshot.retain();
                    self.mutex.unlock(io);
                    return .{ .snapshot = snapshot };
                }
            }

            if (self.refreshing) {
                // During proactive refresh, continue serving the still-valid
                // snapshot. Only callers with expired credentials block.
                if (self.cached) |snapshot| {
                    if (std.mem.eql(u8, &snapshot.source_key, &source_key) and snapshot.credentials.isUnexpired(now)) {
                        snapshot.retain();
                        self.mutex.unlock(io);
                        return .{ .snapshot = snapshot };
                    }
                }
                if (context.isBounded()) {
                    self.mutex.unlock(io);
                    try context.waitForRefresh(io);
                } else {
                    self.refreshed.waitUncancelable(io, &self.mutex);
                    self.mutex.unlock(io);
                }
                continue;
            }
            self.refreshing = true;
            self.mutex.unlock(io);

            var fresh = resolveCredentialsUncachedWithContextAndIo(alloc, http, filesystem_io, region, source, context) catch |err| {
                const request_aborted = err == error.Timeout or err == error.Cancelled or err == error.Canceled;
                if (request_aborted) {
                    self.finishFailedRefresh(io);
                    return err;
                }
                const fallback_now = currentUnixSeconds() catch |clock_err| {
                    self.finishFailedRefresh(io);
                    return clock_err;
                };
                self.mutex.lockUncancelable(io);
                self.refreshing = false;
                const closing = self.closing;
                const fallback = if (!closing and self.cached != null) blk: {
                    const snapshot = self.cached.?;
                    if (!std.mem.eql(u8, &snapshot.source_key, &source_key) or !snapshot.credentials.isUnexpired(fallback_now)) break :blk null;
                    snapshot.retain();
                    break :blk snapshot;
                } else null;
                self.refreshed.broadcast(io);
                self.mutex.unlock(io);
                if (closing) return error.CredentialCacheClosed;
                if (fallback) |snapshot| return .{ .snapshot = snapshot };
                return err;
            };
            errdefer fresh.deinit(alloc);
            const snapshot = alloc.create(Snapshot) catch |err| {
                self.finishFailedRefresh(io);
                return err;
            };
            snapshot.* = .{ .alloc = alloc, .source_key = source_key, .credentials = fresh };
            fresh = undefined;

            self.mutex.lockUncancelable(io);
            if (self.closing) {
                self.refreshing = false;
                self.refreshed.broadcast(io);
                self.mutex.unlock(io);
                snapshot.release();
                return error.CredentialCacheClosed;
            }
            const old = self.cached;
            self.cached = snapshot;
            snapshot.retain(); // caller lease
            self.refreshing = false;
            self.refreshed.broadcast(io);
            self.mutex.unlock(io);
            if (old) |previous| previous.release();
            return .{ .snapshot = snapshot };
        }
    }

    fn finishFailedRefresh(self: *CredentialCache, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        self.refreshing = false;
        self.refreshed.broadcast(io);
        self.mutex.unlock(io);
    }

    fn finishCall(self: *CredentialCache, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        std.debug.assert(self.active_calls > 0);
        self.active_calls -= 1;
        self.refreshed.broadcast(io);
        self.mutex.unlock(io);
    }

    fn bindIo(self: *CredentialCache, io: std.Io) !std.Io {
        if (self.closed.load(.acquire)) return error.CredentialCacheClosed;
        while (!self.io_init_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.io_init_mutex.unlock();
        if (self.closed.load(.acquire)) return error.CredentialCacheClosed;
        if (self.io == null) self.io = io;
        return self.io.?;
    }
};

fn credentialSourceIdentity(source: CredentialSource) credential_source_identity.CredentialSourceIdentity {
    const Identity = credential_source_identity.CredentialSourceIdentity;
    return switch (source) {
        .default => Identity.awsDefaultChain(),
        .profile => |profile| Identity.awsProfile(profile.name, profile.shared_credentials_file),
        .web_identity => |identity| Identity.awsWebIdentity(
            identity.role_arn,
            identity.token_file,
            identity.session_name,
            identity.sts_endpoint,
        ),
    };
}

fn credentialSourceKey(
    region: []const u8,
    source: CredentialSource,
) [std.crypto.hash.sha2.Sha256.digest_length]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var encoded_region_len = std.mem.nativeToLittle(u64, @intCast(region.len));
    hasher.update(std.mem.asBytes(&encoded_region_len));
    hasher.update(region);
    credentialSourceIdentity(source).updateHash(&hasher);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn credentialHttpRequest(
    http: *httpx.Client,
    context: RequestContext,
    method: httpx.Method,
    url: []const u8,
    options_in: httpx.RequestOptions,
) !httpx.Response {
    try context.check(http.io);
    var options = options_in;
    options.timeout_ms = try context.remainingTimeoutMs(http.io);
    options.cookies_enabled = false;
    options.cancellation = context.httpCancellation();
    return http.request(method, url, options) catch |err| switch (err) {
        error.Timeout, error.Cancelled, error.Canceled => err,
        else => error.MissingAwsCredentials,
    };
}

pub fn resolveCredentialsUncached(alloc: std.mem.Allocator, http: *httpx.Client, region: []const u8, source: CredentialSource, context: RequestContext) !Credentials {
    return try resolveCredentialsUncachedWithContextAndIo(alloc, http, null, region, source, context);
}

fn resolveCredentialsUncachedWithContextAndIo(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    filesystem_io: ?std.Io,
    region: []const u8,
    source: CredentialSource,
    context: RequestContext,
) !Credentials {
    try context.check(http.io);
    return switch (source) {
        .profile => |profile| try credentialsFromProfile(alloc, http, filesystem_io, profile, context),
        .web_identity => |identity| try credentialsFromWebIdentity(alloc, http, filesystem_io, region, identity, context),
        .default => blk: {
            if (getEnvOwned(alloc, "AWS_ACCESS_KEY_ID")) |access| {
                errdefer alloc.free(access);
                const secret = getEnvOwned(alloc, "AWS_SECRET_ACCESS_KEY") orelse return error.MissingSecretAccessKey;
                errdefer alloc.free(secret);
                const token = getEnvOwned(alloc, "AWS_SESSION_TOKEN");
                break :blk .{ .access_key_id = access, .secret_access_key = secret, .session_token = token };
            }
            if (credentialsFromWebIdentityFromEnv(alloc, http, filesystem_io, region, context)) |creds| break :blk creds else |err| switch (err) {
                error.Timeout, error.Cancelled, error.Canceled => return err,
                else => {},
            }
            const profile = getEnvOwned(alloc, "AWS_PROFILE") orelse try alloc.dupe(u8, "default");
            defer alloc.free(profile);
            // A configured browser or role identity is authoritative. Never fall through
            // to instance metadata or a stale static key when its login expires.
            if (builtin.os.tag != .freestanding) {
                const resolved = try cloud_credentials.resolveAwsProfile(alloc, filesystem_io orelse http.io, profile);
                defer alloc.free(resolved.login_profile);
                if (resolved.requires_export)
                    break :blk try credentialsFromExportedProfile(alloc, http, filesystem_io, profile, context);
            }
            if (credentialsFromSharedFiles(alloc, filesystem_io, profile, null)) |creds| break :blk creds else |_| {}
            try context.check(http.io);
            if (credentialsFromEcsMetadata(alloc, http, filesystem_io, context)) |creds| break :blk creds else |err| switch (err) {
                error.Timeout, error.Cancelled, error.Canceled => return err,
                else => {},
            }
            if (credentialsFromInstanceMetadata(alloc, http, context)) |creds| break :blk creds else |err| switch (err) {
                error.Timeout, error.Cancelled, error.Canceled => return err,
                else => {},
            }
            return error.MissingAwsCredentials;
        },
    };
}

fn credentialsFromProfile(alloc: std.mem.Allocator, http: *httpx.Client, filesystem_io: ?std.Io, profile: ProfileCredentialSource, context: RequestContext) !Credentials {
    if (builtin.os.tag != .freestanding and profile.shared_credentials_file == null) {
        const resolved = try cloud_credentials.resolveAwsProfile(alloc, filesystem_io orelse http.io, profile.name);
        defer alloc.free(resolved.login_profile);
        if (resolved.requires_export)
            return credentialsFromExportedProfile(alloc, http, filesystem_io, profile.name, context);
    }
    return credentialsFromSharedFiles(alloc, filesystem_io, profile.name, profile.shared_credentials_file);
}

fn credentialsFromExportedProfile(alloc: std.mem.Allocator, http: *httpx.Client, filesystem_io: ?std.Io, profile: []const u8, context: RequestContext) !Credentials {
    try context.check(http.io);
    const timeout_ms = @min(try context.remainingTimeoutMs(http.io) orelse 30_000, 30_000);
    var exported = try cloud_credentials.exportAwsCredentials(alloc, filesystem_io orelse http.io, profile, timeout_ms, context.cancellation);
    defer exported.deinit();
    try context.check(http.io);
    return credentialsFromTemporaryExport(alloc, exported.value, try currentUnixSeconds());
}

fn credentialsFromTemporaryExport(alloc: std.mem.Allocator, value: cloud_credentials.AwsExport, now: u64) !Credentials {
    const expiration = try cloud_credentials.validateTemporaryAwsExport(value, now);
    return dupCredentials(alloc, value.AccessKeyId, value.SecretAccessKey, value.SessionToken.?, expiration);
}

pub fn testBrowserProfileCredentialExpiration() !void {
    const a = std.testing.allocator;
    var value: cloud_credentials.AwsExport = .{ .Version = 1, .AccessKeyId = "access", .SecretAccessKey = "secret", .SessionToken = "session", .Expiration = "2030-01-02T03:04:05+00:00" };
    const expiration = try cloud_credentials.parseExpiration(value.Expiration.?);
    var creds = try credentialsFromTemporaryExport(a, value, expiration - 600);
    defer creds.deinit(a);
    try std.testing.expectEqual(expiration, creds.expires_at_unix.?);
    try std.testing.expect(creds.isFresh(expiration - 600));
    try std.testing.expect(!creds.isFresh(expiration - 100));
    try std.testing.expect(creds.isUnexpired(expiration - 100));
    try std.testing.expectError(error.AwsLoginRequired, credentialsFromTemporaryExport(a, value, expiration));
    value.Expiration = null;
    try std.testing.expectError(error.InvalidAwsCredentialExport, credentialsFromTemporaryExport(a, value, 0));
}

fn credentialsFromSharedFiles(alloc: std.mem.Allocator, filesystem_io: ?std.Io, profile: []const u8, explicit_path: ?[]const u8) !Credentials {
    const path = if (explicit_path) |value| try alloc.dupe(u8, value) else getEnvOwned(alloc, "AWS_SHARED_CREDENTIALS_FILE") orelse blk: {
        const home = getEnvOwned(alloc, "HOME") orelse return error.MissingAwsCredentials;
        defer alloc.free(home);
        break :blk try std.fmt.allocPrint(alloc, "{s}/.aws/credentials", .{home});
    };
    defer alloc.free(path);
    var io_impl: ?std.Io.Threaded = if (filesystem_io == null) std.Io.Threaded.init(alloc, .{}) else null;
    defer if (io_impl) |*owned| owned.deinit();
    const io = filesystem_io orelse io_impl.?.io();
    const data = std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(1 << 20)) catch return error.MissingAwsCredentials;
    defer alloc.free(data);
    return try parseProfileCredentials(alloc, data, profile);
}

fn parseProfileCredentials(alloc: std.mem.Allocator, data: []const u8, profile: []const u8) !Credentials {
    var in_profile = false;
    var access: ?[]u8 = null;
    var secret: ?[]u8 = null;
    var token: ?[]u8 = null;
    errdefer {
        if (access) |v| alloc.free(v);
        if (secret) |v| alloc.free(v);
        if (token) |v| alloc.free(v);
    }
    var lines = std.mem.splitScalar(u8, data, '\n');
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r\n");
        if (line.len == 0 or line[0] == '#') continue;
        if (line[0] == '[' and line[line.len - 1] == ']') {
            var section = std.mem.trim(u8, line[1 .. line.len - 1], " \t");
            if (std.mem.startsWith(u8, section, "profile ")) section = std.mem.trim(u8, section["profile ".len..], " \t");
            in_profile = std.mem.eql(u8, section, profile);
            continue;
        }
        if (!in_profile) continue;
        const eq = std.mem.indexOfScalar(u8, line, '=') orelse continue;
        const key = std.mem.trim(u8, line[0..eq], " \t");
        const value = std.mem.trim(u8, line[eq + 1 ..], " \t");
        if (std.mem.eql(u8, key, "aws_access_key_id")) {
            if (access != null) return error.DuplicateCredentialKey;
            access = try alloc.dupe(u8, value);
        }
        if (std.mem.eql(u8, key, "aws_secret_access_key")) {
            if (secret != null) return error.DuplicateCredentialKey;
            secret = try alloc.dupe(u8, value);
        }
        if (std.mem.eql(u8, key, "aws_session_token")) {
            if (token != null) return error.DuplicateCredentialKey;
            token = try alloc.dupe(u8, value);
        }
    }
    return .{
        .access_key_id = access orelse return error.MissingAccessKeyId,
        .secret_access_key = secret orelse return error.MissingSecretAccessKey,
        .session_token = token,
    };
}

fn credentialsFromWebIdentityFromEnv(alloc: std.mem.Allocator, http: *httpx.Client, filesystem_io: ?std.Io, region: []const u8, context: RequestContext) !Credentials {
    const role_arn = getEnvOwned(alloc, "AWS_ROLE_ARN") orelse return error.MissingAwsCredentials;
    defer alloc.free(role_arn);
    const token_file = getEnvOwned(alloc, "AWS_WEB_IDENTITY_TOKEN_FILE") orelse return error.MissingAwsCredentials;
    defer alloc.free(token_file);
    const session_name = getEnvOwned(alloc, "AWS_ROLE_SESSION_NAME") orelse try alloc.dupe(u8, "antfly-bedrock");
    defer alloc.free(session_name);
    const sts_endpoint = getEnvOwned(alloc, "AWS_STS_ENDPOINT");
    defer if (sts_endpoint) |value| alloc.free(value);
    return try credentialsFromWebIdentity(alloc, http, filesystem_io, region, .{
        .role_arn = role_arn,
        .token_file = token_file,
        .session_name = session_name,
        .sts_endpoint = sts_endpoint,
    }, context);
}

fn credentialsFromWebIdentity(alloc: std.mem.Allocator, http: *httpx.Client, filesystem_io: ?std.Io, region: []const u8, identity: WebIdentityCredentialSource, context: RequestContext) !Credentials {
    try context.check(http.io);
    const token = try webIdentityTokenFileAlloc(alloc, filesystem_io, identity.token_file);
    defer alloc.free(token);

    const sts_endpoint = if (identity.sts_endpoint) |endpoint| try alloc.dupe(u8, endpoint) else try std.fmt.allocPrint(alloc, "https://sts.{s}.amazonaws.com", .{region});
    defer alloc.free(sts_endpoint);
    const encoded_role = try percentEncodeAlloc(alloc, identity.role_arn);
    defer alloc.free(encoded_role);
    const encoded_session = try percentEncodeAlloc(alloc, identity.session_name);
    defer alloc.free(encoded_session);
    const encoded_token = try percentEncodeAlloc(alloc, std.mem.trim(u8, token, " \t\r\n"));
    defer alloc.free(encoded_token);
    const body = try std.fmt.allocPrint(alloc, "Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn={s}&RoleSessionName={s}&WebIdentityToken={s}", .{
        encoded_role,
        encoded_session,
        encoded_token,
    });
    defer alloc.free(body);

    const headers = [_]HeaderPair{.{ "content-type", "application/x-www-form-urlencoded" }};
    var resp = try credentialHttpRequest(http, context, .POST, sts_endpoint, .{ .headers = &headers, .body = body });
    defer resp.deinit();
    if (!resp.ok()) return error.MissingAwsCredentials;
    const response_body = resp.body orelse return error.MissingAwsCredentials;
    return try parseStsCredentials(alloc, response_body);
}

fn webIdentityTokenFileAlloc(
    alloc: std.mem.Allocator,
    filesystem_io: ?std.Io,
    path: []const u8,
) ![]u8 {
    var io_impl: ?std.Io.Threaded = if (filesystem_io == null) std.Io.Threaded.init(alloc, .{}) else null;
    defer if (io_impl) |*owned| owned.deinit();
    const io = filesystem_io orelse io_impl.?.io();
    return std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(1 << 20)) catch
        return error.MissingAwsCredentials;
}

fn credentialsFromEcsMetadata(alloc: std.mem.Allocator, http: *httpx.Client, filesystem_io: ?std.Io, context: RequestContext) !Credentials {
    const full_uri = getEnvOwned(alloc, "AWS_CONTAINER_CREDENTIALS_FULL_URI");
    const relative_uri = if (full_uri == null) getEnvOwned(alloc, "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") else null;
    defer if (full_uri) |value| alloc.free(value);
    defer if (relative_uri) |value| alloc.free(value);

    const url = if (full_uri) |value|
        try alloc.dupe(u8, value)
    else if (relative_uri) |value|
        try std.fmt.allocPrint(alloc, "{s}{s}", .{ ecs_credentials_endpoint, value })
    else
        return error.MissingAwsCredentials;
    defer alloc.free(url);

    const auth_token = containerAuthorizationToken(alloc, filesystem_io);
    defer if (auth_token) |value| alloc.free(value);
    const headers = if (auth_token) |token| &[_]HeaderPair{.{ "authorization", token }} else &[_]HeaderPair{};
    var resp = try credentialHttpRequest(http, context, .GET, url, .{ .headers = headers });
    defer resp.deinit();
    if (!resp.ok()) return error.MissingAwsCredentials;
    const body = resp.body orelse return error.MissingAwsCredentials;
    return try parseMetadataCredentials(alloc, body);
}

fn credentialsFromInstanceMetadata(alloc: std.mem.Allocator, http: *httpx.Client, context: RequestContext) !Credentials {
    try context.check(http.io);
    if (getEnvOwned(alloc, "AWS_EC2_METADATA_DISABLED")) |disabled| {
        defer alloc.free(disabled);
        if (std.ascii.eqlIgnoreCase(disabled, "true")) return error.MissingAwsCredentials;
    }
    const endpoint = getEnvOwned(alloc, "AWS_EC2_METADATA_SERVICE_ENDPOINT") orelse try alloc.dupe(u8, imds_default_endpoint);
    defer alloc.free(endpoint);
    const base = try endpointBaseAlloc(alloc, endpoint);
    defer alloc.free(base);

    const token = imdsToken(alloc, http, base, context) catch |err| switch (err) {
        error.Timeout, error.Cancelled, error.Canceled => return err,
        else => null,
    };
    defer if (token) |value| alloc.free(value);
    if (token == null) {
        if (getEnvOwned(alloc, "AWS_EC2_METADATA_V1_DISABLED")) |disabled| {
            defer alloc.free(disabled);
            if (std.ascii.eqlIgnoreCase(disabled, "true")) return error.MissingAwsCredentials;
        }
    }
    const headers = if (token) |value| &[_]HeaderPair{.{ "x-aws-ec2-metadata-token", value }} else &[_]HeaderPair{};

    const role_url = try std.fmt.allocPrint(alloc, "{s}/latest/meta-data/iam/security-credentials/", .{base});
    defer alloc.free(role_url);
    var role_resp = try credentialHttpRequest(http, context, .GET, role_url, .{ .headers = headers });
    defer role_resp.deinit();
    if (!role_resp.ok()) return error.MissingAwsCredentials;
    const role_body = role_resp.body orelse return error.MissingAwsCredentials;
    const role_name = std.mem.trim(u8, role_body, " \t\r\n");
    if (role_name.len == 0) return error.MissingAwsCredentials;

    const encoded_role = try percentEncodePathSegmentAlloc(alloc, role_name);
    defer alloc.free(encoded_role);
    const creds_url = try std.fmt.allocPrint(alloc, "{s}/latest/meta-data/iam/security-credentials/{s}", .{ base, encoded_role });
    defer alloc.free(creds_url);
    var creds_resp = try credentialHttpRequest(http, context, .GET, creds_url, .{ .headers = headers });
    defer creds_resp.deinit();
    if (!creds_resp.ok()) return error.MissingAwsCredentials;
    const body = creds_resp.body orelse return error.MissingAwsCredentials;
    return try parseMetadataCredentials(alloc, body);
}

fn imdsToken(alloc: std.mem.Allocator, http: *httpx.Client, endpoint: []const u8, context: RequestContext) ![]u8 {
    const url = try std.fmt.allocPrint(alloc, "{s}/latest/api/token", .{endpoint});
    defer alloc.free(url);
    const headers = [_]HeaderPair{.{ "x-aws-ec2-metadata-token-ttl-seconds", "21600" }};
    var resp = try credentialHttpRequest(http, context, .PUT, url, .{ .headers = &headers, .body = "" });
    defer resp.deinit();
    if (!resp.ok()) return error.MissingAwsCredentials;
    const body = resp.body orelse return error.MissingAwsCredentials;
    return try alloc.dupe(u8, std.mem.trim(u8, body, " \t\r\n"));
}

fn containerAuthorizationToken(alloc: std.mem.Allocator, filesystem_io: ?std.Io) ?[]u8 {
    if (getEnvOwned(alloc, "AWS_CONTAINER_AUTHORIZATION_TOKEN")) |token| return token;
    const token_file = getEnvOwned(alloc, "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") orelse return null;
    defer alloc.free(token_file);
    return containerAuthorizationTokenFileAlloc(alloc, filesystem_io, token_file) catch null;
}

fn containerAuthorizationTokenFileAlloc(alloc: std.mem.Allocator, filesystem_io: ?std.Io, token_file: []const u8) ![]u8 {
    var io_impl: ?std.Io.Threaded = if (filesystem_io == null) std.Io.Threaded.init(alloc, .{}) else null;
    defer if (io_impl) |*owned| owned.deinit();
    const io = filesystem_io orelse io_impl.?.io();
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, token_file, alloc, .limited(1 << 20));
    const trimmed = std.mem.trim(u8, raw, " \t\r\n");
    if (trimmed.len == raw.len) return raw;
    const out = alloc.dupe(u8, trimmed) catch |err| {
        alloc.free(raw);
        return err;
    };
    alloc.free(raw);
    return out;
}

fn parseMetadataCredentials(alloc: std.mem.Allocator, body: []const u8) !Credentials {
    const MetadataCredentials = struct {
        AccessKeyId: []const u8,
        SecretAccessKey: []const u8,
        Token: ?[]const u8 = null,
        Expiration: ?[]const u8 = null,
    };
    var parsed = try std.json.parseFromSlice(MetadataCredentials, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const expires_at = if (parsed.value.Expiration) |value| parseAwsIso8601(value) catch null else null;
    return try dupCredentials(alloc, parsed.value.AccessKeyId, parsed.value.SecretAccessKey, parsed.value.Token, expires_at);
}

fn parseStsCredentials(alloc: std.mem.Allocator, body: []const u8) !Credentials {
    const access = extractXmlTag(body, "AccessKeyId") orelse return error.MissingAccessKeyId;
    const secret = extractXmlTag(body, "SecretAccessKey") orelse return error.MissingSecretAccessKey;
    const token = extractXmlTag(body, "SessionToken");
    const expires_at = if (extractXmlTag(body, "Expiration")) |value| parseAwsIso8601(value) catch null else null;
    return try dupCredentials(alloc, access, secret, token, expires_at);
}

fn dupCredentials(alloc: std.mem.Allocator, access: []const u8, secret: []const u8, token: ?[]const u8, expires_at_unix: ?u64) !Credentials {
    const access_copy = try alloc.dupe(u8, access);
    errdefer alloc.free(access_copy);
    const secret_copy = try alloc.dupe(u8, secret);
    errdefer alloc.free(secret_copy);
    const token_copy = if (token) |value| try alloc.dupe(u8, value) else null;
    errdefer if (token_copy) |value| alloc.free(value);
    return .{ .access_key_id = access_copy, .secret_access_key = secret_copy, .session_token = token_copy, .expires_at_unix = expires_at_unix };
}

fn extractXmlTag(body: []const u8, tag: []const u8) ?[]const u8 {
    var open_buf: [64]u8 = undefined;
    var close_buf: [67]u8 = undefined;
    if (tag.len + 2 > open_buf.len or tag.len + 3 > close_buf.len) return null;
    const open = std.fmt.bufPrint(open_buf[0..], "<{s}>", .{tag}) catch return null;
    const close = std.fmt.bufPrint(close_buf[0..], "</{s}>", .{tag}) catch return null;
    const start = std.mem.indexOf(u8, body, open) orelse return null;
    const value_start = start + open.len;
    const end_rel = std.mem.indexOf(u8, body[value_start..], close) orelse return null;
    return body[value_start .. value_start + end_rel];
}

pub fn percentEncodeAlloc(alloc: std.mem.Allocator, raw: []const u8) ![]u8 {
    return percentEncodeWithSlash(alloc, raw, false);
}

pub fn percentEncodePathSegmentAlloc(alloc: std.mem.Allocator, raw: []const u8) ![]u8 {
    return percentEncodeWithSlash(alloc, raw, true);
}

fn percentEncodeWithSlash(alloc: std.mem.Allocator, raw: []const u8, keep_slash: bool) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (raw) |byte| {
        const unreserved =
            (byte >= 'A' and byte <= 'Z') or
            (byte >= 'a' and byte <= 'z') or
            (byte >= '0' and byte <= '9') or
            byte == '-' or byte == '_' or byte == '.' or byte == '~' or
            (keep_slash and byte == '/');
        if (unreserved) {
            try out.append(alloc, byte);
        } else {
            try out.append(alloc, '%');
            try out.append(alloc, std.fmt.digitToChar(byte >> 4, .upper));
            try out.append(alloc, std.fmt.digitToChar(byte & 0x0f, .upper));
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn parseAwsIso8601(value: []const u8) !u64 {
    if (value.len < "2006-01-02T15:04:05Z".len) return error.InvalidTimestamp;
    if (value[4] != '-' or value[7] != '-' or value[10] != 'T' or value[13] != ':' or value[16] != ':') return error.InvalidTimestamp;
    const year = try std.fmt.parseInt(i64, value[0..4], 10);
    const month = try std.fmt.parseUnsigned(u8, value[5..7], 10);
    const day = try std.fmt.parseUnsigned(u8, value[8..10], 10);
    const hour = try std.fmt.parseUnsigned(u8, value[11..13], 10);
    const minute = try std.fmt.parseUnsigned(u8, value[14..16], 10);
    const second = try std.fmt.parseUnsigned(u8, value[17..19], 10);
    if (month < 1 or month > 12 or day < 1 or day > 31 or hour > 23 or minute > 59 or second > 60) return error.InvalidTimestamp;
    if (value[19] != 'Z') {
        if (value[19] != '.') return error.InvalidTimestamp;
        if (std.mem.indexOfScalar(u8, value[20..], 'Z') == null) return error.InvalidTimestamp;
    }
    const days = daysFromCivil(year, month, day);
    if (days < 0) return error.InvalidTimestamp;
    return @as(u64, @intCast(days)) * std.time.s_per_day + @as(u64, hour) * std.time.s_per_hour + @as(u64, minute) * std.time.s_per_min + @as(u64, second);
}

fn daysFromCivil(year_in: i64, month_in: u8, day_in: u8) i64 {
    var year = year_in;
    const month: i64 = month_in;
    const day: i64 = day_in;
    if (month <= 2) year -= 1;
    const era = @divFloor(year, 400);
    const yoe = year - era * 400;
    const shifted_month = if (month > 2) month - 3 else month + 9;
    const doy = @divFloor(153 * shifted_month + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

pub fn currentUnixSeconds() !u64 {
    return unixSecondsFromTimestamp(std.Io.Timestamp.now(std.Io.Threaded.global_single_threaded.io(), .real));
}

pub fn unixSecondsFromTimestamp(timestamp: std.Io.Timestamp) !u64 {
    const nanoseconds = timestamp.toNanoseconds();
    if (nanoseconds < 0) return error.InvalidSystemTime;
    return @intCast(@divTrunc(nanoseconds, std.time.ns_per_s));
}

fn getEnvOwned(alloc: std.mem.Allocator, name: [:0]const u8) ?[]u8 {
    if (!builtin.link_libc) return null;
    const value = std.c.getenv(name) orelse return null;
    const text = std.mem.span(value);
    if (text.len == 0) return null;
    return alloc.dupe(u8, text) catch null;
}

pub fn testSharedCredentialsProfileParser() !void {
    const alloc = std.testing.allocator;
    var creds = try parseProfileCredentials(alloc,
        \\[default]
        \\aws_access_key_id = AKIADEFAULT
        \\aws_secret_access_key = defaultsecret
        \\[prod]
        \\aws_access_key_id = AKIAPROD
        \\aws_secret_access_key = prodsecret
        \\aws_session_token = token
    , "prod");
    defer creds.deinit(alloc);
    try std.testing.expectEqualStrings("AKIAPROD", creds.access_key_id);
    try std.testing.expectEqualStrings("prodsecret", creds.secret_access_key);
    try std.testing.expectEqualStrings("token", creds.session_token.?);
}

pub fn testMetadataCredentialParsers() !void {
    const alloc = std.testing.allocator;
    var ecs = try parseMetadataCredentials(alloc,
        \\{
        \\  "AccessKeyId": "AKIAECS",
        \\  "SecretAccessKey": "ecssecret",
        \\  "Token": "ecstoken",
        \\  "Expiration": "2026-01-02T03:04:05Z"
        \\}
    );
    defer ecs.deinit(alloc);
    try std.testing.expectEqualStrings("AKIAECS", ecs.access_key_id);
    try std.testing.expectEqualStrings("ecssecret", ecs.secret_access_key);
    try std.testing.expectEqualStrings("ecstoken", ecs.session_token.?);
    try std.testing.expectEqual(try parseAwsIso8601("2026-01-02T03:04:05Z"), ecs.expires_at_unix.?);

    var sts = try parseStsCredentials(alloc,
        \\<AssumeRoleWithWebIdentityResponse>
        \\  <AssumeRoleWithWebIdentityResult>
        \\    <Credentials>
        \\      <AccessKeyId>AKIASTS</AccessKeyId>
        \\      <SecretAccessKey>stssecret</SecretAccessKey>
        \\      <SessionToken>ststoken</SessionToken>
        \\      <Expiration>2026-01-02T03:04:05.000Z</Expiration>
        \\    </Credentials>
        \\  </AssumeRoleWithWebIdentityResult>
        \\</AssumeRoleWithWebIdentityResponse>
    );
    defer sts.deinit(alloc);
    try std.testing.expectEqualStrings("AKIASTS", sts.access_key_id);
    try std.testing.expectEqualStrings("stssecret", sts.secret_access_key);
    try std.testing.expectEqualStrings("ststoken", sts.session_token.?);
    try std.testing.expectEqual(try parseAwsIso8601("2026-01-02T03:04:05Z"), sts.expires_at_unix.?);
}

pub fn testCredentialUrlEncoding() !void {
    const alloc = std.testing.allocator;
    const query = try percentEncodeAlloc(alloc, "arn:aws:iam::123456789012:role/antfly bedrock");
    defer alloc.free(query);
    try std.testing.expectEqualStrings("arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fantfly%20bedrock", query);

    const path = try percentEncodePathSegmentAlloc(alloc, "role/name with space");
    defer alloc.free(path);
    try std.testing.expectEqualStrings("role/name%20with%20space", path);
}

pub fn testCredentialFilesUseSuppliedFilesystemAuthority() !void {
    const alloc = std.testing.allocator;
    var filesystem_impl = std.Io.Threaded.init(alloc, .{});
    defer filesystem_impl.deinit();
    const filesystem_io = filesystem_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const credentials_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/credentials", .{tmp.sub_path});
    defer alloc.free(credentials_path);
    try std.Io.Dir.cwd().writeFile(filesystem_io, .{
        .sub_path = credentials_path,
        .data = "[archive]\naws_access_key_id = AKIAPROFILE\naws_secret_access_key = profile-secret\naws_session_token = profile-token\n",
    });
    var credentials = try credentialsFromSharedFiles(alloc, filesystem_io, "archive", credentials_path);
    defer credentials.deinit(alloc);
    try std.testing.expectEqualStrings("AKIAPROFILE", credentials.access_key_id);
    try std.testing.expectEqualStrings("profile-secret", credentials.secret_access_key);
    try std.testing.expectEqualStrings("profile-token", credentials.session_token.?);

    const token_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/web-identity-token", .{tmp.sub_path});
    defer alloc.free(token_path);
    try std.Io.Dir.cwd().writeFile(filesystem_io, .{
        .sub_path = token_path,
        .data = "signed-web-identity-token\n",
    });
    const token = try webIdentityTokenFileAlloc(alloc, filesystem_io, token_path);
    defer alloc.free(token);
    try std.testing.expectEqualStrings("signed-web-identity-token\n", token);

    const container_token_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/container-auth-token", .{tmp.sub_path});
    defer alloc.free(container_token_path);
    try std.Io.Dir.cwd().writeFile(filesystem_io, .{
        .sub_path = container_token_path,
        .data = " container-auth-token \n",
    });
    const container_token = try containerAuthorizationTokenFileAlloc(alloc, filesystem_io, container_token_path);
    defer alloc.free(container_token);
    try std.testing.expectEqualStrings("container-auth-token", container_token);
}

pub fn testCredentialSourceKeysAreStructured() !void {
    const profile_a = CredentialSource{ .profile = .{
        .name = "ab",
        .shared_credentials_file = "c",
    } };
    const profile_b = CredentialSource{ .profile = .{
        .name = "a",
        .shared_credentials_file = "bc",
    } };
    const profile_omitted = CredentialSource{ .profile = .{ .name = "default" } };
    const profile_empty = CredentialSource{ .profile = .{
        .name = "default",
        .shared_credentials_file = "",
    } };
    const web_identity_a = CredentialSource{ .web_identity = .{
        .role_arn = "ab",
        .token_file = "c",
        .session_name = "d",
    } };
    const web_identity_b = CredentialSource{ .web_identity = .{
        .role_arn = "a",
        .token_file = "bc",
        .session_name = "d",
    } };

    try std.testing.expect(!std.mem.eql(
        u8,
        &credentialSourceKey("us-east-1", profile_a),
        &credentialSourceKey("us-east-1", profile_b),
    ));
    try std.testing.expect(!std.mem.eql(
        u8,
        &credentialSourceKey("us-east-1", profile_omitted),
        &credentialSourceKey("us-east-1", profile_empty),
    ));
    try std.testing.expect(!std.mem.eql(
        u8,
        &credentialSourceKey("us-east-1", web_identity_a),
        &credentialSourceKey("us-east-1", web_identity_b),
    ));
    try std.testing.expect(!std.mem.eql(
        u8,
        &credentialSourceKey("us-east-1", .default),
        &credentialSourceKey("us-west-2", .default),
    ));
}

test "shared credentials profile parser" {
    try testSharedCredentialsProfileParser();
}

test "shared credentials profile parser rejects duplicate keys" {
    try std.testing.expectError(error.DuplicateCredentialKey, parseProfileCredentials(
        std.testing.allocator,
        "[default]\naws_access_key_id = first\naws_access_key_id = second\naws_secret_access_key = secret\n",
        "default",
    ));
}

test "metadata credential parsers" {
    try testMetadataCredentialParsers();
}

test "credential url encoding" {
    try testCredentialUrlEncoding();
}

test "credential source keys are structured" {
    try testCredentialSourceKeysAreStructured();
}

test "credential cache shutdown waits for an in-flight refresh" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var cache = CredentialCache{};
    _ = try cache.bindIo(io);
    cache.mutex.lockUncancelable(io);
    cache.refreshing = true;
    cache.mutex.unlock(io);

    const Worker = struct {
        fn run(target: *CredentialCache, worker_io: std.Io) void {
            while (true) {
                target.mutex.lockUncancelable(worker_io);
                const closing = target.closing;
                target.mutex.unlock(worker_io);
                if (closing) break;
                std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
            }
            target.finishFailedRefresh(worker_io);
        }
    };
    var thread = try std.testing.io.concurrent(Worker.run, .{ &cache, io });
    cache.deinit(std.testing.allocator);
    thread.await(std.testing.io);
    try std.testing.expect(cache.closing);
    try std.testing.expect(!cache.refreshing);
    try std.testing.expect(cache.cached == null);
    try std.testing.expectError(error.CredentialCacheClosed, cache.bindIo(io));
}

test "AWS credential files use supplied filesystem authority" {
    try testCredentialFilesUseSuppliedFilesystemAuthority();
}

test "AWS credential leases survive source replacement and cache shutdown" {
    const alloc = std.testing.allocator;
    var threaded = std.Io.Threaded.init(alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/profiles", .{tmp.sub_path});
    defer alloc.free(path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = "[first]\naws_access_key_id = first-key\naws_secret_access_key = first-secret\n[second]\naws_access_key_id = second-key\naws_secret_access_key = second-secret\n" });
    var http = httpx.Client.init(alloc, io);
    defer http.deinit();
    var cache = CredentialCache{};
    var closed = false;
    defer if (!closed) cache.deinit(alloc);
    var first = try cache.getLeaseForSourceWithIo(alloc, &http, io, "us-east-1", .{ .profile = .{ .name = "first", .shared_credentials_file = path } });
    defer first.release();
    var second = try cache.getLeaseForSourceWithIo(alloc, &http, io, "us-east-1", .{ .profile = .{ .name = "second", .shared_credentials_file = path } });
    defer second.release();
    cache.deinit(alloc);
    closed = true;
    try std.testing.expectEqualStrings("first-key", first.credentials().access_key_id);
    try std.testing.expectEqualStrings("second-key", second.credentials().access_key_id);
    try std.testing.expectError(error.CredentialCacheClosed, cache.getLeaseForSourceWithIo(alloc, &http, io, "us-east-1", .default));
}

pub fn endpointBaseAlloc(alloc: std.mem.Allocator, endpoint: []const u8) ![]u8 {
    var end = endpoint.len;
    while (end > 0 and endpoint[end - 1] == '/') end -= 1;
    if (end == 0) return error.InvalidEndpoint;
    return try alloc.dupe(u8, endpoint[0..end]);
}
