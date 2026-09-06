// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Process-local outbound provider quotas. Identity is independent of policy;
//! named consumers cannot multiply capacity by choosing different limits.
const std = @import("std");
const httpx = @import("httpx");
const credentials = @import("credential_source_identity.zig");
const sync = @import("antfly_platform").sync;

pub const Operation = enum { embedding, generation, reranking };
pub const Provider = enum { openai, ollama, antfly, gemini, vertex, cohere, bedrock };
pub const EndpointIdentity = struct {
    provider: Provider,
    endpoint: []const u8,
    model: []const u8,
    region: []const u8 = "",
    project: []const u8 = "",
    location: []const u8 = "",
    credentials: credentials.CredentialSourceIdentity,

    pub fn updateHash(self: EndpointIdentity, hasher: *std.crypto.hash.sha2.Sha256) void {
        inline for (.{ @tagName(self.provider), std.mem.trimEnd(u8, self.endpoint, "/"), self.model, self.region, self.project, self.location }) |value|
            credentials.updateField(hasher, value);
        self.credentials.updateHash(hasher);
    }

    pub fn eql(self: EndpointIdentity, other: EndpointIdentity) bool {
        return self.provider == other.provider and self.credentials.eql(other.credentials) and
            std.mem.eql(u8, std.mem.trimEnd(u8, self.endpoint, "/"), std.mem.trimEnd(u8, other.endpoint, "/")) and
            std.mem.eql(u8, self.model, other.model) and std.mem.eql(u8, self.region, other.region) and
            std.mem.eql(u8, self.project, other.project) and std.mem.eql(u8, self.location, other.location);
    }
};

pub const QuotaIdentity = struct {
    endpoint: EndpointIdentity,
    operation: Operation,

    pub fn digest(self: QuotaIdentity) [32]u8 {
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        credentials.updateField(&hasher, "antfly-provider-quota-v1");
        self.endpoint.updateHash(&hasher);
        credentials.updateField(&hasher, @tagName(self.operation));
        var result: [32]u8 = undefined;
        hasher.final(&result);
        return result;
    }
};

pub const Policy = struct {
    pub const Pacing = enum { token_bucket, completion };
    pacing: Pacing = .token_bucket,
    requests_per_minute: u32 = 0,
    burst: u32 = 1,
    tokens_per_minute: u64 = 0,
    max_concurrency: u32 = 0,

    pub fn enabled(self: Policy) bool {
        return self.requests_per_minute != 0 or self.tokens_per_minute != 0 or self.max_concurrency != 0;
    }

    pub fn fromConfig(maybe: anytype) !Policy {
        const config = maybe orelse return .{};
        const policy = Policy{
            .pacing = if (config.pacing) |value| std.meta.stringToEnum(Pacing, @tagName(value)).? else .token_bucket,
            .requests_per_minute = try positive(u32, config.requests_per_minute, 0),
            .burst = try positive(u32, config.burst, 1),
            .tokens_per_minute = try positive(u64, config.tokens_per_minute, 0),
            .max_concurrency = try positive(u32, config.max_concurrency, 0),
        };
        try policy.validate();
        return policy;
    }

    fn validate(self: Policy) !void {
        if (self.burst == 0) return error.InvalidRateLimitPolicy;
        if (self.pacing == .completion and (self.requests_per_minute == 0 or self.burst != 1))
            return error.InvalidRateLimitPolicy;
    }

    fn intervalNs(self: Policy) u64 {
        std.debug.assert(self.requests_per_minute > 0);
        return std.math.divCeil(u64, 60 * std.time.ns_per_s, self.requests_per_minute) catch unreachable;
    }

    fn positive(comptime T: type, value: anytype, default: T) !T {
        const raw = value orelse return default;
        const result = std.math.cast(T, raw) orelse return error.InvalidRateLimitPolicy;
        if (result == 0) return error.InvalidRateLimitPolicy;
        return result;
    }
};

pub fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    if (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts)) != .SUCCESS) return 0;
    return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec);
}

pub const Limiter = struct {
    mutex: std.atomic.Mutex = .unlocked,
    policy: Policy,
    requests: f64,
    tokens: f64,
    last_ns: u64,
    cooldown_ns: u64 = 0,
    next_start_ns: u64 = 0,
    in_flight: u32 = 0,

    fn init(policy: Policy) Limiter {
        return .{ .policy = policy, .requests = @floatFromInt(policy.burst), .tokens = @floatFromInt(policy.tokens_per_minute), .last_ns = nowNs() };
    }

    pub fn observer(self: *Limiter, output_tokens: u64) httpx.AttemptObserver {
        return .{ .ptr = self, .before = before, .after = after, .output_tokens = output_tokens };
    }

    fn before(ptr: *anyopaque, context: httpx.AttemptObserver.Context) !void {
        const self: *Limiter = @ptrCast(@alignCast(ptr));
        // Conservative text accounting: serialized UTF-8 bytes plus the output
        // cap. No refund on failure; retries consume independent reservations.
        const cost = @as(u64, @intCast(context.body_bytes)) +| context.output_tokens;
        if (self.policy.tokens_per_minute != 0 and cost > self.policy.tokens_per_minute)
            return error.ProviderTokenBudgetExceeded;
        while (true) {
            if (context.is_cancelled(context.cancellation_ptr)) return error.Cancelled;
            if (context.deadline_ms) |deadline| {
                if (std.Io.Clock.awake.now(context.io).toMilliseconds() >= deadline) return error.Timeout;
            }
            sync.lockYielding(&self.mutex);
            const wait_ns = self.tryReserve(nowNs(), cost);
            self.mutex.unlock();
            if (wait_ns == 0) return;
            // Keep both caller cancellation and the transport watchdog live.
            try context.io.sleep(.fromNanoseconds(@intCast(@min(wait_ns, 5 * std.time.ns_per_ms))), .awake);
        }
    }

    fn refill(self: *Limiter, now: u64) void {
        const elapsed: f64 = @floatFromInt(now -| self.last_ns);
        self.last_ns = now;
        const minute: f64 = 60 * std.time.ns_per_s;
        self.requests = @min(@as(f64, @floatFromInt(self.policy.burst)), self.requests + elapsed * @as(f64, @floatFromInt(self.policy.requests_per_minute)) / minute);
        self.tokens = @min(@as(f64, @floatFromInt(self.policy.tokens_per_minute)), self.tokens + elapsed * @as(f64, @floatFromInt(self.policy.tokens_per_minute)) / minute);
    }

    fn tryReserve(self: *Limiter, now: u64, cost: u64) u64 {
        self.refill(now);
        const minute: f64 = 60 * std.time.ns_per_s;
        if (now < self.cooldown_ns) return self.cooldown_ns - now;
        // An admission timestamp is not a send timestamp: connection setup,
        // scheduler stalls and streaming can all delay the admitted request.
        // Completion pacing never banks reservations during that uncertainty.
        if (self.policy.pacing == .completion and self.in_flight != 0)
            return std.time.ns_per_ms;
        if (now < self.next_start_ns) return self.next_start_ns - now;
        if (self.policy.max_concurrency != 0 and self.in_flight >= self.policy.max_concurrency)
            return std.time.ns_per_ms;
        if (self.policy.requests_per_minute != 0 and self.requests < 1)
            return @max(1, @as(u64, @intFromFloat(@ceil((1 - self.requests) * minute / @as(f64, @floatFromInt(self.policy.requests_per_minute))))));
        if (self.policy.tokens_per_minute != 0 and self.tokens < @as(f64, @floatFromInt(cost)))
            return @max(1, @as(u64, @intFromFloat(@ceil((@as(f64, @floatFromInt(cost)) - self.tokens) * minute / @as(f64, @floatFromInt(self.policy.tokens_per_minute))))));
        if (self.policy.requests_per_minute != 0) self.requests -= 1;
        if (self.policy.tokens_per_minute != 0) self.tokens -= @floatFromInt(cost);
        self.in_flight += 1;
        return 0;
    }

    fn after(ptr: *anyopaque, response: ?*const httpx.Response) void {
        const self: *Limiter = @ptrCast(@alignCast(ptr));
        sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.finishAt(response, nowNs(), wallSeconds());
    }

    fn finishAt(self: *Limiter, response: ?*const httpx.Response, now: u64, wall_seconds: u64) void {
        std.debug.assert(self.in_flight > 0);
        self.in_flight -= 1;
        if (self.policy.pacing == .completion)
            self.next_start_ns = @max(self.next_start_ns, now +| self.policy.intervalNs());
        if (response) |res| {
            if (res.status.code == 429) {
                const seconds = retryAfterSeconds(res.header("Retry-After"), wall_seconds);
                self.cooldown_ns = @max(self.cooldown_ns, now +| seconds *| std.time.ns_per_s);
            }
        }
    }
};

pub const Registry = struct {
    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,
    const Entry = struct { key: [32]u8, limiter: Limiter, refs: usize = 1, released_ns: u64 = 0 };
    const idle_ttl = 5 * 60 * std.time.ns_per_s;

    pub fn init(allocator: std.mem.Allocator) Registry {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Registry) void {
        for (self.entries.items) |entry| {
            std.debug.assert(entry.refs == 0);
            self.allocator.destroy(entry);
        }
        self.entries.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn acquire(self: *Registry, identity: QuotaIdentity, policy: Policy) !Handle {
        try policy.validate();
        sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const key = identity.digest();
        const now = nowNs();
        var i: usize = 0;
        while (i < self.entries.items.len) {
            const entry = self.entries.items[i];
            if (entry.refs == 0) entry.limiter.refill(now);
            // TTL alone cannot retire debt: a large burst at a low RPM can
            // take longer than the idle TTL to replenish.
            if (entry.refs == 0 and now -| entry.released_ns >= idle_ttl and now >= entry.limiter.cooldown_ns and now >= entry.limiter.next_start_ns and
                (entry.limiter.policy.requests_per_minute == 0 or entry.limiter.requests >= @as(f64, @floatFromInt(entry.limiter.policy.burst))) and
                entry.limiter.tokens >= @as(f64, @floatFromInt(entry.limiter.policy.tokens_per_minute)))
            {
                self.allocator.destroy(entry);
                _ = self.entries.swapRemove(i);
                continue;
            }
            if (std.mem.eql(u8, &key, &entry.key)) {
                if (!std.meta.eql(entry.limiter.policy, policy)) {
                    if (entry.refs != 0) return error.ConflictingRateLimitPolicy;
                    // A replacement configuration may change policy only after
                    // the previous owner drains. Preserve debt and cooldown.
                    entry.limiter.policy = policy;
                    entry.limiter.requests = @min(entry.limiter.requests, @as(f64, @floatFromInt(policy.burst)));
                    entry.limiter.tokens = @min(entry.limiter.tokens, @as(f64, @floatFromInt(policy.tokens_per_minute)));
                }
                entry.refs += 1;
                return .{ .registry = self, .entry = entry };
            }
            i += 1;
        }
        if (self.entries.items.len >= 4096) return error.ProviderQuotaRegistryFull;
        const entry = try self.allocator.create(Entry);
        errdefer self.allocator.destroy(entry);
        entry.* = .{ .key = key, .limiter = Limiter.init(policy) };
        try self.entries.append(self.allocator, entry);
        return .{ .registry = self, .entry = entry };
    }
};

pub const Handle = struct {
    registry: *Registry,
    entry: *Registry.Entry,
    pub fn limiter(self: Handle) *Limiter {
        return &self.entry.limiter;
    }
    pub fn release(self: *Handle) void {
        const registry = self.registry;
        sync.lockYielding(&registry.mutex);
        defer registry.mutex.unlock();
        std.debug.assert(self.entry.refs > 0);
        self.entry.refs -= 1;
        if (self.entry.refs == 0) self.entry.released_ns = nowNs();
        self.* = undefined;
    }
};

/// Default process registry; services can inject a registry with an explicit
/// lifetime. This does not coordinate quotas across Antfly processes.
pub var process_registry = Registry.init(std.heap.page_allocator);

fn wallSeconds() u64 {
    var ts: std.posix.timespec = undefined;
    if (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts)) != .SUCCESS) return 0;
    return @intCast(@max(0, ts.sec));
}

fn retryAfterSeconds(raw: ?[]const u8, now_seconds: u64) u64 {
    const value = std.mem.trim(u8, raw orelse return 1, " \t");
    if (std.fmt.parseUnsigned(u64, value, 10)) |seconds| return seconds else |_| {}
    if (value.len != 29 or !std.mem.endsWith(u8, value, " GMT")) return 1;
    const year = std.fmt.parseUnsigned(u16, value[12..16], 10) catch return 1;
    const day = std.fmt.parseUnsigned(u8, value[5..7], 10) catch return 1;
    const hour = std.fmt.parseUnsigned(u8, value[17..19], 10) catch return 1;
    const minute = std.fmt.parseUnsigned(u8, value[20..22], 10) catch return 1;
    const second = std.fmt.parseUnsigned(u8, value[23..25], 10) catch return 1;
    if (year < 1970 or hour > 23 or minute > 59 or second > 59) return 1;
    const months = [_][]const u8{ "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
    const month: usize = for (months, 0..) |name, i| {
        if (std.mem.eql(u8, name, value[8..11])) break i;
    } else return 1;
    var days: u64 = 0;
    var y: u16 = 1970;
    while (y < year) : (y += 1) days += if (leapYear(y)) @as(u64, 366) else 365;
    const lengths = [_]u8{ 31, if (leapYear(year)) 29 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (day == 0 or day > lengths[month]) return 1;
    for (lengths[0..month]) |length| days += length;
    const target = (days + day - 1) * 86400 + @as(u64, hour) * 3600 + @as(u64, minute) * 60 + second;
    return target -| now_seconds;
}

fn leapYear(year: u16) bool {
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0);
}

pub fn testCancellationAndDeadline() !void {
    var limiter = Limiter.init(.{ .requests_per_minute = 60 });
    const hook = limiter.observer(0);
    var cancelled = false;
    const Probe = struct {
        fn isCancelled(ptr: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(ptr))).*;
        }
    };
    var context: httpx.AttemptObserver.Context = .{
        .io = std.testing.io,
        .deadline_ms = null,
        .cancellation_ptr = &cancelled,
        .is_cancelled = Probe.isCancelled,
        .body_bytes = 10,
        .output_tokens = 0,
    };
    try hook.before(hook.ptr, context);
    hook.after(hook.ptr, null);
    context.deadline_ms = std.Io.Clock.awake.now(context.io).toMilliseconds() - 1;
    try std.testing.expectError(error.Timeout, hook.before(hook.ptr, context));
    context.deadline_ms = null;
    cancelled = true;
    try std.testing.expectError(error.Cancelled, hook.before(hook.ptr, context));
    try std.testing.expectEqual(@as(u32, 0), limiter.in_flight);
}

pub fn testProviderQuotas() !void {
    var registry = Registry.init(std.testing.allocator);
    defer registry.deinit();
    const identity = QuotaIdentity{ .operation = .embedding, .endpoint = .{
        .provider = .bedrock,
        .endpoint = "https://proxy.example",
        .model = "model",
        .region = "us-east-1",
        .credentials = .awsDefaultChain(),
    } };
    const policy = Policy{ .requests_per_minute = 60, .max_concurrency = 1 };
    var first = try registry.acquire(identity, policy);
    defer first.release();
    var same = try registry.acquire(identity, policy);
    defer same.release();
    try std.testing.expect(first.limiter() == same.limiter());
    try std.testing.expectError(error.ConflictingRateLimitPolicy, registry.acquire(identity, .{ .requests_per_minute = 120 }));
    var regional = identity;
    regional.endpoint.region = "us-west-2";
    var west = try registry.acquire(regional, policy);
    defer west.release();
    try std.testing.expect(first.limiter() != west.limiter());
    var generation = identity;
    generation.operation = .generation;
    var generator = try registry.acquire(generation, policy);
    defer generator.release();
    try std.testing.expect(first.limiter() != generator.limiter());
    var vertex = identity;
    vertex.endpoint.provider = .vertex;
    vertex.endpoint.region = "";
    vertex.endpoint.project = "project";
    vertex.endpoint.location = "us-central1";
    const central = vertex.digest();
    vertex.endpoint.location = "europe-west4";
    try std.testing.expect(!std.mem.eql(u8, &central, &vertex.digest()));

    var limiter = Limiter.init(.{ .requests_per_minute = 60, .burst = 2, .tokens_per_minute = 120, .max_concurrency = 1 });
    const now = limiter.last_ns;
    try std.testing.expectEqual(@as(u64, 0), limiter.tryReserve(now, 100));
    try std.testing.expect(limiter.tryReserve(now, 1) != 0);
    Limiter.after(&limiter, null);
    try std.testing.expect(limiter.tryReserve(now, 21) != 0);
    try std.testing.expectEqual(@as(u64, 0), limiter.tryReserve(now + std.time.ns_per_s, 21));
    var response = httpx.Response.init(std.testing.allocator, 429);
    defer response.deinit();
    try response.headers.set("Retry-After", "3");
    Limiter.after(&limiter, &response);
    try std.testing.expect(limiter.tryReserve(nowNs(), 1) != 0);
    try std.testing.expectEqual(@as(u64, 3), retryAfterSeconds("Thu, 01 Jan 1970 00:00:03 GMT", 0));
    try std.testing.expectEqual(@as(u64, 0), retryAfterSeconds("Thu, 01 Jan 1970 00:00:03 GMT", 5));
    try std.testing.expectEqual(@as(u64, 1), retryAfterSeconds("garbage", 0));
    try testCancellationAndDeadline();
    // A long-lived burst debt must survive idle TTL and policy replacement.
    var slow = identity;
    slow.endpoint.model = "slow";
    var debt = try registry.acquire(slow, .{ .requests_per_minute = 1, .burst = 1000 });
    const debt_limiter = debt.limiter();
    debt_limiter.requests = 0;
    debt.release();
    const debt_entry = registry.entries.items[registry.entries.items.len - 1];
    debt_entry.released_ns = nowNs() -| Registry.idle_ttl;
    var retained = try registry.acquire(slow, .{ .requests_per_minute = 1, .burst = 1000 });
    try std.testing.expect(retained.limiter() == debt_limiter);
    try std.testing.expect(retained.limiter().requests < 1);
    retained.release();
    var replaced = try registry.acquire(slow, .{ .requests_per_minute = 2, .burst = 1000 });
    defer replaced.release();
    try std.testing.expect(replaced.limiter().requests < 1);
}

test "provider quotas share identity and enforce policy without leaking permits" {
    try testProviderQuotas();
}

test "provider quotas completion pacing anchors to finish without banking delayed dispatches" {
    const ms = std.time.ns_per_ms;
    var limiter = Limiter.init(.{ .pacing = .completion, .requests_per_minute = 6000 });
    const start = limiter.last_ns;
    try std.testing.expectEqual(@as(u64, 0), limiter.tryReserve(start, 0));
    // DNS/TLS, scheduler stalls or streaming can outlast many RPM intervals.
    // None of them may grant another attempt while this one is outstanding.
    const finish = start + 1000 * ms;
    try std.testing.expect(limiter.tryReserve(finish, 0) != 0);
    try std.testing.expectEqual(@as(u32, 1), limiter.in_flight);
    limiter.finishAt(null, finish, 0);
    try std.testing.expectEqual(@as(u64, 10 * ms), limiter.tryReserve(finish, 0));
    try std.testing.expectEqual(@as(u64, 1), limiter.tryReserve(finish + 10 * ms - 1, 0));
    try std.testing.expectEqual(@as(u64, 0), limiter.tryReserve(finish + 10 * ms, 0));
    var response = httpx.Response.init(std.testing.allocator, 429);
    defer response.deinit();
    try response.headers.set("Retry-After", "2");
    limiter.finishAt(&response, finish + 11 * ms, 0);
    try std.testing.expectEqual(@as(u64, 2000 * ms), limiter.tryReserve(finish + 11 * ms, 0));
    try std.testing.expectEqual(@as(u32, 0), limiter.in_flight);

    // Token-bucket mode intentionally permits overlap and has no hidden
    // completion delay or fixed 50 ms safety margin.
    var bucket = Limiter.init(.{ .requests_per_minute = 6000 });
    const base = bucket.last_ns;
    try std.testing.expectEqual(@as(u64, 0), bucket.tryReserve(base, 0));
    try std.testing.expectEqual(@as(u64, 0), bucket.tryReserve(base + 10 * ms, 0));
    try std.testing.expectEqual(@as(u32, 2), bucket.in_flight);
    bucket.finishAt(null, base + 11 * ms, 0);
    bucket.finishAt(null, base + 12 * ms, 0);
    try std.testing.expectEqual(@as(u64, 0), bucket.next_start_ns);
}

test "provider quotas validate pacing and preserve completion debt across policy replacement" {
    var registry = Registry.init(std.testing.allocator);
    defer registry.deinit();
    const identity = QuotaIdentity{ .operation = .generation, .endpoint = .{
        .provider = .openai,
        .endpoint = "https://example.test",
        .model = "test",
        .credentials = .none(),
    } };
    try std.testing.expectError(error.InvalidRateLimitPolicy, registry.acquire(identity, .{ .pacing = .completion }));
    try std.testing.expectError(error.InvalidRateLimitPolicy, registry.acquire(identity, .{ .pacing = .completion, .requests_per_minute = 60, .burst = 2 }));
    var first = try registry.acquire(identity, .{ .pacing = .completion, .requests_per_minute = 60 });
    const limiter = first.limiter();
    try std.testing.expectEqual(@as(u64, 0), limiter.tryReserve(nowNs(), 0));
    Limiter.after(limiter, null);
    const not_before = limiter.next_start_ns;
    try std.testing.expectError(error.ConflictingRateLimitPolicy, registry.acquire(identity, .{ .requests_per_minute = 60 }));
    first.release();
    var replaced = try registry.acquire(identity, .{ .requests_per_minute = 6000 });
    defer replaced.release();
    try std.testing.expect(replaced.limiter() == limiter);
    try std.testing.expectEqual(not_before, limiter.next_start_ns);
    try std.testing.expect(limiter.tryReserve(not_before - 1, 0) != 0);
    try std.testing.expectEqual(@as(u32, 0), limiter.in_flight);
}

test "provider quotas completion wait honors deadlines and cancellation without stealing a permit" {
    const Probe = struct {
        checks: usize = 0,
        cancel: bool = false,
        fn cancelled(ptr: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ptr)));
            self.checks += 1;
            return self.cancel and self.checks >= 2;
        }
    };
    var limiter = Limiter.init(.{ .pacing = .completion, .requests_per_minute = 6000 });
    try std.testing.expectEqual(@as(u64, 0), limiter.tryReserve(nowNs(), 0));
    const hook = limiter.observer(0);
    var probe = Probe{};
    var context = httpx.AttemptObserver.Context{
        .io = std.testing.io,
        .deadline_ms = std.Io.Clock.awake.now(std.testing.io).toMilliseconds() + 5,
        .cancellation_ptr = &probe,
        .is_cancelled = Probe.cancelled,
        .body_bytes = 0,
        .output_tokens = 0,
    };
    try std.testing.expectError(error.Timeout, hook.before(hook.ptr, context));
    try std.testing.expectEqual(@as(u32, 1), limiter.in_flight);
    context.deadline_ms = null;
    probe = .{ .cancel = true };
    try std.testing.expectError(error.Cancelled, hook.before(hook.ptr, context));
    try std.testing.expectEqual(@as(u32, 1), limiter.in_flight);
    hook.after(hook.ptr, null);
    try std.testing.expectEqual(@as(u32, 0), limiter.in_flight);
}

test "provider quotas observe every retry and retain the permit through streamed writes" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const State = struct {
        starts: usize = 0,
        finishes: usize = 0,
        active: usize = 0,
        writes: usize = 0,
        failure: ?anyerror = null,
        reject: bool = false,
        fail_write: bool = false,
        fn begin(ptr: *anyopaque, _: httpx.AttemptObserver.Context) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.reject) return error.TestAdmissionRejected;
            try std.testing.expectEqual(@as(usize, 0), self.active);
            self.starts += 1;
            self.active += 1;
        }
        fn end(ptr: *anyopaque, _: ?*const httpx.Response) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            std.debug.assert(self.active == 1);
            self.active -= 1;
            self.finishes += 1;
        }
        pub fn writeAll(self: *@This(), bytes: []const u8) !void {
            try std.testing.expectEqual(@as(usize, 1), self.active);
            if (self.fail_write) return error.TestWriteFailed;
            self.writes += bytes.len;
        }
        fn run(client: *httpx.Client, state: *@This()) !void {
            runInner(client, state) catch |err| {
                state.failure = err;
            };
        }
        fn runInner(client: *httpx.Client, state: *@This()) !void {
            const observer = httpx.AttemptObserver{ .ptr = state, .before = begin, .after = end };
            var retry = try client.get("/retry", .{ .attempt_observer = observer });
            defer retry.deinit();
            try std.testing.expectEqual(@as(u16, 503), retry.status.code);
            var stream = try client.getToWriter("/stream", .{ .attempt_observer = observer }, state, null, null);
            defer stream.deinit();
            client.config.retry_policy.max_retries = 0;
            state.fail_write = true;
            try std.testing.expectError(error.TestWriteFailed, client.getToWriter("/stream", .{ .attempt_observer = observer }, state, null, null));
            state.reject = true;
            try std.testing.expectError(error.TestAdmissionRejected, client.get("/stream", .{ .attempt_observer = observer }));
        }
    };
    var server = try httpx.TestServer.start(allocator, io, &.{
        .{ .path = "/retry", .respond = .{ .status = 503, .body = "retry" } },
        .{ .path = "/stream", .respond = .{ .body = "streamed" } },
    });
    defer server.deinit();
    var client = server.client();
    defer client.deinit();
    client.config.retry_policy.max_retries = 1;
    client.config.retry_policy.initial_delay_ms = 0;
    var state: State = .{};
    var group = std.Io.Group.init;
    defer group.cancel(io);
    try group.concurrent(io, State.run, .{ &client, &state });
    try server.handleOne();
    try server.handleOne();
    try server.handleOne();
    try server.handleOne();
    try group.await(io);
    if (state.failure) |err| return err;
    try std.testing.expectEqual(@as(usize, 4), state.starts);
    try std.testing.expectEqual(state.starts, state.finishes);
    try std.testing.expectEqual(@as(usize, 0), state.active);
    try std.testing.expectEqual(@as(usize, 8), state.writes);
}
