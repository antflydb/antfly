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

//! Dedicated health/metrics HTTP server, served on a separate port from the
//! main API. Exposes Kubernetes liveness/readiness probes and Prometheus
//! metrics in the standard text exposition format.
//!
//! The server is built on top of `httpx.Server` and takes two optional
//! pluggable interfaces via vtables:
//!   * `ReadinessChecker` — called by `/readyz` to decide 200 vs 503.
//!   * `MetricsWriter`    — called by `/metrics` to write Prometheus text.
//!
//! Callers typically wire this up once per binary, pointing at their
//! server-specific metrics sources (raft metrics, serverless metrics, etc).

const std = @import("std");
const httpx = @import("httpx");
const platform_sync = @import("antfly_platform").sync;
const Io = std.Io;
const platform_time = @import("antfly_platform").time;
const prometheus = @import("prometheus.zig");
const runtime_lifecycle = @import("runtime_lifecycle.zig");
const metrics_cache_ttl_ms: u64 = 5 * std.time.ms_per_s;
const graceful_shutdown_timeout_ms: u64 = 5_000;
pub const max_connections: u32 = 16;

pub const ReadinessChecker = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        check: *const fn (ptr: *anyopaque) bool,
    };

    pub fn check(self: ReadinessChecker) bool {
        return self.vtable.check(self.ptr);
    }
};

pub const MetricsWriter = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        write_metrics: *const fn (ptr: *anyopaque, writer: *std.Io.Writer) anyerror!void,
    };

    pub fn writeMetrics(self: MetricsWriter, writer: *std.Io.Writer) !void {
        return self.vtable.write_metrics(self.ptr, writer);
    }
};

pub const Config = struct {
    bind_host: []const u8 = "0.0.0.0",
    bind_port: u16,
    http_runtime: ?*httpx.HttpRuntime = null,
};

pub const HealthServer = struct {
    alloc: std.mem.Allocator,
    io: Io,
    ready: ?ReadinessChecker,
    metrics: ?MetricsWriter,
    server: httpx.Server,
    listener_task: httpx.ListenerTask,
    metrics_cache_mutex: std.atomic.Mutex = .unlocked,
    metrics_cache_body: ?[]u8 = null,
    metrics_cache_built_at_ms: u64 = 0,
    metrics_cache_refreshing: bool = false,
    metrics_refresh_future: ?Io.Future(void) = null,
    metrics_refresh_stop: std.atomic.Value(bool) = .init(false),

    pub fn init(
        alloc: std.mem.Allocator,
        io: Io,
        cfg: Config,
        ready: ?ReadinessChecker,
        metrics: ?MetricsWriter,
    ) !*HealthServer {
        const self = try alloc.create(HealthServer);
        errdefer alloc.destroy(self);

        self.* = .{
            .alloc = alloc,
            .io = io,
            .ready = ready,
            .metrics = metrics,
            .server = httpx.Server.initWithConfig(alloc, io, .{
                .host = cfg.bind_host,
                .port = cfg.bind_port,
                .reuse_address = true,
                .max_connections = max_connections,
                .http_runtime = cfg.http_runtime,
                .h1_disconnect_cancellation = .disabled,
            }),
            .listener_task = undefined,
        };
        errdefer self.server.deinit();
        self.listener_task = httpx.ListenerTask.init(&self.server);
        try self.server.get("/healthz", httpx.Handler.bind(self, healthz));
        try self.server.get("/readyz", httpx.Handler.bind(self, readyz));
        try self.server.get("/metrics", httpx.Handler.bind(self, metricsHandler));
        if (metrics != null) {
            self.refreshMetricsCacheSync() catch {};
        }
        return self;
    }

    pub fn deinit(self: *HealthServer) void {
        self.deinitWithDeadline(runtime_lifecycle.ShutdownDeadline.afterMilliseconds(graceful_shutdown_timeout_ms));
    }

    pub fn deinitWithDeadline(self: *HealthServer, deadline: runtime_lifecycle.ShutdownDeadline) void {
        self.stopWithDeadline(deadline);
        lockAtomic(&self.metrics_cache_mutex);
        const cached_body = self.metrics_cache_body;
        self.metrics_cache_body = null;
        self.metrics_cache_refreshing = false;
        self.metrics_cache_mutex.unlock();
        if (cached_body) |body| std.heap.page_allocator.free(body);
        self.server.deinit();
        self.alloc.destroy(self);
    }

    pub fn start(self: *HealthServer) !void {
        try self.listener_task.start();
        errdefer {
            self.listener_task.requestStop();
            self.listener_task.join() catch {};
        }
        try self.startMetricsRefreshThread();
    }

    pub fn stop(self: *HealthServer) void {
        self.stopWithDeadline(runtime_lifecycle.ShutdownDeadline.afterMilliseconds(graceful_shutdown_timeout_ms));
    }

    pub fn stopWithDeadline(self: *HealthServer, deadline: runtime_lifecycle.ShutdownDeadline) void {
        self.stopMetricsRefreshThread();
        self.listener_task.shutdown(deadline.remainingMilliseconds());
        self.listener_task.join() catch |err| {
            std.log.err("health listener failed during shutdown err={s}", .{@errorName(err)});
        };
    }

    pub fn baseUri(self: *HealthServer, alloc: std.mem.Allocator) ![]u8 {
        const address = self.server.boundAddress() orelse return error.ListenerNotStarted;
        return try std.fmt.allocPrint(alloc, "http://{f}", .{address});
    }

    pub fn runtimeFailure(self: *const HealthServer) ?anyerror {
        return self.listener_task.runtimeFailure();
    }

    /// Conditional init + start. Returns null when `port` is unset and
    /// propagates startup errors when a port was explicitly configured, so callers
    /// can write `const hs = try HealthServer.startIfConfigured(...); defer
    /// if (hs) |h| h.deinit();` without scattering if-blocks through each
    /// runtime. Prints the bound URI prefixed with `label` on success.
    pub fn startIfConfigured(
        alloc: std.mem.Allocator,
        io: Io,
        label: []const u8,
        port: ?u16,
        ready: ?ReadinessChecker,
        metrics: ?MetricsWriter,
    ) !?*HealthServer {
        return try startIfConfiguredOnHost(alloc, io, label, null, port, ready, metrics);
    }

    pub fn startIfConfiguredOnHost(
        alloc: std.mem.Allocator,
        io: Io,
        label: []const u8,
        bind_host: ?[]const u8,
        port: ?u16,
        ready: ?ReadinessChecker,
        metrics: ?MetricsWriter,
    ) !?*HealthServer {
        return try startIfConfiguredOnHostWithRuntime(alloc, io, label, bind_host, port, ready, metrics, null);
    }

    pub fn startIfConfiguredOnHostWithRuntime(
        alloc: std.mem.Allocator,
        io: Io,
        label: []const u8,
        bind_host: ?[]const u8,
        port: ?u16,
        ready: ?ReadinessChecker,
        metrics: ?MetricsWriter,
        http_runtime: ?*httpx.HttpRuntime,
    ) !?*HealthServer {
        const p = port orelse return null;
        const hs = try HealthServer.init(alloc, io, .{
            .bind_host = bind_host orelse "0.0.0.0",
            .bind_port = p,
            .http_runtime = http_runtime,
        }, ready, metrics);
        errdefer hs.deinit();
        try hs.start();
        const uri = try hs.baseUri(alloc);
        defer alloc.free(uri);
        std.debug.print("{s} health api listening on {s}\n", .{ label, uri });
        return hs;
    }

    fn healthz(_: *HealthServer, ctx: *httpx.Context) anyerror!httpx.Response {
        return try ctx.json(.{ .status = "ok" });
    }

    fn readyz(self: *HealthServer, ctx: *httpx.Context) anyerror!httpx.Response {
        const is_ready = if (self.ready) |ready| ready.check() else true;
        if (is_ready) return try ctx.json(.{ .status = "ready" });
        return try ctx.status(503).json(.{ .status = "not_ready" });
    }

    fn metricsHandler(self: *HealthServer, ctx: *httpx.Context) anyerror!httpx.Response {
        return try self.metricsResponseCached(ctx);
    }

    fn metricsResponseCached(self: *HealthServer, ctx: *httpx.Context) !httpx.Response {
        lockAtomic(&self.metrics_cache_mutex);
        const cached = self.metrics_cache_body;
        const body_copy = if (cached) |body|
            ctx.allocator.dupe(u8, body) catch |err| {
                self.metrics_cache_mutex.unlock();
                return err;
            }
        else
            null;
        self.metrics_cache_mutex.unlock();

        if (body_copy) |body| {
            defer ctx.allocator.free(body);
            var response = try ctx.text(body);
            try response.headers.set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            return response;
        }

        return try ctx.status(503).text("metrics unavailable");
    }

    fn startMetricsRefreshThread(self: *HealthServer) !void {
        if (self.metrics == null or self.metrics_refresh_future != null) return;
        self.metrics_refresh_stop.store(false, .release);
        self.metrics_refresh_future = try self.io.concurrent(metricsRefreshTask, .{self});
    }

    fn stopMetricsRefreshThread(self: *HealthServer) void {
        self.metrics_refresh_stop.store(true, .release);
        if (self.metrics_refresh_future) |*future| {
            _ = future.await(self.io);
            self.metrics_refresh_future = null;
        }
    }

    fn refreshMetricsCacheThread(self: *HealthServer) void {
        self.refreshMetricsCacheSync() catch {
            lockAtomic(&self.metrics_cache_mutex);
            self.metrics_cache_refreshing = false;
            self.metrics_cache_mutex.unlock();
        };
    }

    fn metricsRefreshTask(self: *HealthServer) void {
        while (!self.metrics_refresh_stop.load(.acquire)) {
            sleepRefreshInterval(self.io, &self.metrics_refresh_stop);
            if (self.metrics_refresh_stop.load(.acquire)) return;
            self.refreshMetricsCacheThread();
        }
    }

    fn refreshMetricsCacheSync(self: *HealthServer) !void {
        const body = try buildMetricsBody(std.heap.page_allocator, self.metrics);
        errdefer std.heap.page_allocator.free(body);

        const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
        lockAtomic(&self.metrics_cache_mutex);
        const old = self.metrics_cache_body;
        self.metrics_cache_body = body;
        self.metrics_cache_built_at_ms = now_ms;
        self.metrics_cache_refreshing = false;
        self.metrics_cache_mutex.unlock();
        if (old) |prev| std.heap.page_allocator.free(prev);
    }

    fn executeForTest(self: *HealthServer, method: httpx.Method, uri: []const u8) !httpx.Response {
        var request = try httpx.Request.init(self.alloc, method, uri);
        defer request.deinit();
        var ctx = httpx.Context.init(self.alloc, self.io, &request);
        defer ctx.deinit();
        var params: [16]httpx.RouteParam = undefined;
        const matched = self.server.router.find(method, uri, &params) orelse
            return try ctx.status(404).text("not found");
        ctx.params = matched.params;
        return try matched.handler.invoke(&ctx);
    }
};

fn sleepRefreshInterval(io: Io, stop: *std.atomic.Value(bool)) void {
    const slice_ms: u64 = 100;
    var slept_ms: u64 = 0;
    while (slept_ms < metrics_cache_ttl_ms and !stop.load(.acquire)) : (slept_ms += slice_ms) {
        io.sleep(Io.Duration.fromMilliseconds(@intCast(slice_ms)), .awake) catch return;
    }
}

pub const PromLabel = prometheus.PromLabel;
pub const appendPromMetric = prometheus.appendPromMetric;
pub const appendPromMetricLabeled = prometheus.appendPromMetricLabeled;
pub const appendPromMetricHeader = prometheus.appendPromMetricHeader;
pub const appendPromSample = prometheus.appendPromSample;
pub const appendPromSampleLabeled = prometheus.appendPromSampleLabeled;

fn buildMetricsBody(alloc: std.mem.Allocator, metrics: ?MetricsWriter) ![]u8 {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    defer writer.deinit();

    if (metrics) |m| {
        try m.writeMetrics(&writer.writer);
    }

    return try alloc.dupe(u8, writer.writer.buffered());
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

const FakeReady = struct {
    ready: bool,

    fn iface(self: *FakeReady) ReadinessChecker {
        return .{
            .ptr = self,
            .vtable = &.{ .check = check },
        };
    }

    fn check(ptr: *anyopaque) bool {
        const self: *FakeReady = @ptrCast(@alignCast(ptr));
        return self.ready;
    }
};

const FakeMetrics = struct {
    call_count: usize = 0,

    fn iface(self: *FakeMetrics) MetricsWriter {
        return .{
            .ptr = self,
            .vtable = &.{ .write_metrics = writeMetrics },
        };
    }

    fn writeMetrics(ptr: *anyopaque, writer: *std.Io.Writer) anyerror!void {
        const self: *FakeMetrics = @ptrCast(@alignCast(ptr));
        self.call_count += 1;
        try appendPromMetric(writer, "antfly_test_metric_total", "counter", "Test metric", 42);
    }
};

test "health server healthz returns ok" {
    const alloc = testing.allocator;
    const hs = try HealthServer.init(alloc, std.testing.io, .{ .bind_port = 0 }, null, null);
    defer hs.deinit();

    var resp = try hs.executeForTest(.GET, "/healthz");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status.code);
    try testing.expect(std.mem.indexOf(u8, resp.body.?, "ok") != null);
}

test "health server readyz reports 200 when ready" {
    const alloc = testing.allocator;
    var fake = FakeReady{ .ready = true };
    const hs = try HealthServer.init(alloc, std.testing.io, .{ .bind_port = 0 }, fake.iface(), null);
    defer hs.deinit();

    var resp = try hs.executeForTest(.GET, "/readyz");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status.code);
    try testing.expect(std.mem.indexOf(u8, resp.body.?, "ready") != null);
}

test "health server readyz reports 503 when not ready" {
    const alloc = testing.allocator;
    var fake = FakeReady{ .ready = false };
    const hs = try HealthServer.init(alloc, std.testing.io, .{ .bind_port = 0 }, fake.iface(), null);
    defer hs.deinit();

    var resp = try hs.executeForTest(.GET, "/readyz");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 503), resp.status.code);
    try testing.expect(std.mem.indexOf(u8, resp.body.?, "not_ready") != null);
}

test "health server metrics returns prometheus text" {
    const alloc = testing.allocator;
    var fake = FakeMetrics{};
    const hs = try HealthServer.init(alloc, std.testing.io, .{ .bind_port = 0 }, null, fake.iface());
    defer hs.deinit();

    var resp = try hs.executeForTest(.GET, "/metrics");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status.code);
    try testing.expect(std.mem.indexOf(u8, resp.headers.get("Content-Type").?, "text/plain") != null);
    try testing.expect(std.mem.indexOf(u8, resp.body.?, "# HELP antfly_test_metric_total") != null);
    try testing.expect(std.mem.indexOf(u8, resp.body.?, "antfly_test_metric_total 42") != null);
    try testing.expectEqual(@as(usize, 1), fake.call_count);
}

test "health server metrics serves cached payload within ttl" {
    const alloc = testing.allocator;
    var fake = FakeMetrics{};
    const hs = try HealthServer.init(alloc, std.testing.io, .{ .bind_port = 0 }, null, fake.iface());
    defer hs.deinit();

    var resp_a = try hs.executeForTest(.GET, "/metrics");
    defer resp_a.deinit();
    var resp_b = try hs.executeForTest(.GET, "/metrics");
    defer resp_b.deinit();

    try testing.expectEqualStrings(resp_a.body.?, resp_b.body.?);
    try testing.expectEqual(@as(usize, 1), fake.call_count);
}

test "health server metrics request path does not refresh stale cache" {
    const alloc = testing.allocator;
    var fake = FakeMetrics{};
    const hs = try HealthServer.init(alloc, std.testing.io, .{ .bind_port = 0 }, null, fake.iface());
    defer hs.deinit();

    lockAtomic(&hs.metrics_cache_mutex);
    hs.metrics_cache_built_at_ms = 0;
    hs.metrics_cache_mutex.unlock();

    var resp = try hs.executeForTest(.GET, "/metrics");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status.code);
    try testing.expectEqual(@as(usize, 1), fake.call_count);
}

test "health server startIfConfiguredOnHost uses provided bind host" {
    const alloc = testing.allocator;
    const hs = (try HealthServer.startIfConfiguredOnHost(alloc, std.testing.io, "test", "127.0.0.1", 0, null, null)).?;
    defer hs.deinit();

    const uri = try hs.baseUri(alloc);
    defer alloc.free(uri);
    try testing.expect(std.mem.startsWith(u8, uri, "http://127.0.0.1:"));
}

test "health server startIfConfiguredOnHost propagates configured bind failures" {
    try testing.expectError(
        error.ParseFailed,
        HealthServer.startIfConfiguredOnHost(testing.allocator, std.testing.io, "test", "not-an-ip", 4200, null, null),
    );
}

test "health server unknown path returns 404" {
    const alloc = testing.allocator;
    const hs = try HealthServer.init(alloc, std.testing.io, .{ .bind_port = 0 }, null, null);
    defer hs.deinit();

    var resp = try hs.executeForTest(.GET, "/nope");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 404), resp.status.code);
}

test "health server appendPromMetric formats correctly" {
    var buf: [256]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buf);
    try appendPromMetric(&writer, "my_metric", "gauge", "Help text", 7);
    const expected =
        "# HELP my_metric Help text\n" ++
        "# TYPE my_metric gauge\n" ++
        "my_metric 7\n";
    try testing.expectEqualStrings(expected, writer.buffered());
}

test "health server appendPromMetricLabeled formats and escapes labels" {
    var buf: [512]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buf);
    try appendPromMetricLabeled(
        &writer,
        "my_metric_total",
        "counter",
        "Help text",
        &.{
            .{ .name = "kind", .value = "run_table_index" },
            .{ .name = "path", .value = "quote\"slash\\line\n" },
        },
        9,
    );
    const expected =
        "# HELP my_metric_total Help text\n" ++
        "# TYPE my_metric_total counter\n" ++
        "my_metric_total{kind=\"run_table_index\",path=\"quote\\\"slash\\\\line\\n\"} 9\n";
    try testing.expectEqualStrings(expected, writer.buffered());
}
