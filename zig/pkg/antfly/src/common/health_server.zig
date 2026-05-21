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
//! The server is built on top of `StdHttpListener` and takes two optional
//! pluggable interfaces via vtables:
//!   * `ReadinessChecker` — called by `/readyz` to decide 200 vs 503.
//!   * `MetricsWriter`    — called by `/metrics` to write Prometheus text.
//!
//! Callers typically wire this up once per binary, pointing at their
//! server-specific metrics sources (raft metrics, serverless metrics, etc).

const std = @import("std");
const Io = std.Io;
const http_common = @import("http/http_common.zig");
const std_http_listener = @import("http/std_http_listener.zig");
const platform_time = @import("../platform/time.zig");

const StdHttpListener = std_http_listener.StdHttpListener;
const StdHttpListenerConfig = std_http_listener.StdHttpListenerConfig;
const HttpRequest = http_common.HttpRequest;
const HttpResponse = http_common.HttpResponse;
const RequestExecutor = http_common.RequestExecutor;
const metrics_cache_ttl_ms: u64 = 5 * std.time.ms_per_s;

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
};

pub const HealthServer = struct {
    alloc: std.mem.Allocator,
    ready: ?ReadinessChecker,
    metrics: ?MetricsWriter,
    listener: StdHttpListener,
    metrics_cache_mutex: std.atomic.Mutex = .unlocked,
    metrics_cache_body: ?[]u8 = null,
    metrics_cache_built_at_ms: u64 = 0,
    metrics_cache_refreshing: bool = false,
    metrics_refresh_io: ?Io.Threaded = null,
    metrics_refresh_future: ?Io.Future(void) = null,
    metrics_refresh_stop: std.atomic.Value(bool) = .init(false),

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: Config,
        ready: ?ReadinessChecker,
        metrics: ?MetricsWriter,
    ) !*HealthServer {
        const self = try alloc.create(HealthServer);
        errdefer alloc.destroy(self);

        self.* = .{
            .alloc = alloc,
            .ready = ready,
            .metrics = metrics,
            .listener = undefined,
        };

        self.listener = StdHttpListener.init(alloc, .{
            .bind_host = cfg.bind_host,
            .bind_port = cfg.bind_port,
            .reuse_address = true,
            .thread_stack_size = 256 * 1024,
            .serve_in_connection_threads = true,
            .connection_thread_stack_size = 256 * 1024,
            .max_connection_threads = 16,
        }, self.executor());
        if (metrics != null) {
            self.refreshMetricsCacheSync() catch {};
        }
        return self;
    }

    pub fn deinit(self: *HealthServer) void {
        self.stop();
        lockAtomic(&self.metrics_cache_mutex);
        const cached_body = self.metrics_cache_body;
        self.metrics_cache_body = null;
        self.metrics_cache_refreshing = false;
        self.metrics_cache_mutex.unlock();
        if (cached_body) |body| std.heap.page_allocator.free(body);
        self.listener.deinit();
        self.alloc.destroy(self);
    }

    pub fn start(self: *HealthServer) !void {
        try self.listener.start();
        errdefer self.listener.stop();
        try self.startMetricsRefreshThread();
    }

    pub fn stop(self: *HealthServer) void {
        self.stopMetricsRefreshThread();
        self.listener.stop();
    }

    pub fn baseUri(self: *const HealthServer, alloc: std.mem.Allocator) ![]u8 {
        return try self.listener.baseUri(alloc);
    }

    /// Conditional init + start. Returns null when `port` is unset so callers
    /// can write `const hs = try HealthServer.startIfConfigured(...); defer
    /// if (hs) |h| h.deinit();` without scattering if-blocks through each
    /// runtime. Prints the bound URI prefixed with `label` on success.
    pub fn startIfConfigured(
        alloc: std.mem.Allocator,
        label: []const u8,
        port: ?u16,
        ready: ?ReadinessChecker,
        metrics: ?MetricsWriter,
    ) !?*HealthServer {
        return try startIfConfiguredOnHost(alloc, label, null, port, ready, metrics);
    }

    pub fn startIfConfiguredOnHost(
        alloc: std.mem.Allocator,
        label: []const u8,
        bind_host: ?[]const u8,
        port: ?u16,
        ready: ?ReadinessChecker,
        metrics: ?MetricsWriter,
    ) !?*HealthServer {
        const p = port orelse return null;
        const hs = try HealthServer.init(alloc, .{
            .bind_host = bind_host orelse "0.0.0.0",
            .bind_port = p,
        }, ready, metrics);
        errdefer hs.deinit();
        try hs.start();
        const uri = try hs.baseUri(alloc);
        defer alloc.free(uri);
        std.debug.print("{s} health api listening on {s}\n", .{ label, uri });
        return hs;
    }

    pub fn executor(self: *HealthServer) RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{ .execute = execute },
        };
    }

    fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: HttpRequest) anyerror!HttpResponse {
        const self: *HealthServer = @ptrCast(@alignCast(ptr));
        const path = pathOnly(req.uri);

        if (req.method == .GET and std.mem.eql(u8, path, "/healthz")) {
            return try jsonResponse(alloc, 200, "{\"status\":\"ok\"}");
        }

        if (req.method == .GET and std.mem.eql(u8, path, "/readyz")) {
            const is_ready = if (self.ready) |r| r.check() else true;
            if (is_ready) {
                return try jsonResponse(alloc, 200, "{\"status\":\"ready\"}");
            }
            return try jsonResponse(alloc, 503, "{\"status\":\"not_ready\"}");
        }

        if (req.method == .GET and std.mem.eql(u8, path, "/metrics")) {
            return try self.metricsResponseCached(alloc);
        }

        return try textResponse(alloc, 404, "not found");
    }

    fn metricsResponseCached(self: *HealthServer, alloc: std.mem.Allocator) !HttpResponse {
        lockAtomic(&self.metrics_cache_mutex);
        const cached = self.metrics_cache_body;
        const body_copy = if (cached) |body|
            alloc.dupe(u8, body) catch |err| {
                self.metrics_cache_mutex.unlock();
                return err;
            }
        else
            null;
        self.metrics_cache_mutex.unlock();

        if (body_copy) |body| {
            errdefer alloc.free(body);
            const content_type = try alloc.dupe(u8, "text/plain; version=0.0.4; charset=utf-8");
            return .{
                .status = 200,
                .content_type = content_type,
                .body = body,
            };
        }

        return try textResponse(alloc, 503, "metrics unavailable");
    }

    fn startMetricsRefreshThread(self: *HealthServer) !void {
        if (self.metrics == null or self.metrics_refresh_future != null) return;
        self.metrics_refresh_stop.store(false, .release);
        if (self.metrics_refresh_io == null) {
            self.metrics_refresh_io = Io.Threaded.init(self.alloc, .{ .stack_size = 256 * 1024 });
        }
        const io = self.metrics_refresh_io.?.io();
        self.metrics_refresh_future = try io.concurrent(metricsRefreshTask, .{self});
    }

    fn stopMetricsRefreshThread(self: *HealthServer) void {
        self.metrics_refresh_stop.store(true, .release);
        if (self.metrics_refresh_future) |*future| {
            if (self.metrics_refresh_io) |*io_impl| {
                _ = future.await(io_impl.io());
            }
            self.metrics_refresh_future = null;
        }
        if (self.metrics_refresh_io) |*io_impl| {
            io_impl.deinit();
            self.metrics_refresh_io = null;
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
        const io = if (self.metrics_refresh_io) |*io_impl| io_impl.io() else return;
        while (!self.metrics_refresh_stop.load(.acquire)) {
            sleepRefreshInterval(io, &self.metrics_refresh_stop);
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
};

fn sleepRefreshInterval(io: Io, stop: *std.atomic.Value(bool)) void {
    const slice_ms: u64 = 100;
    var slept_ms: u64 = 0;
    while (slept_ms < metrics_cache_ttl_ms and !stop.load(.acquire)) : (slept_ms += slice_ms) {
        io.sleep(Io.Duration.fromMilliseconds(@intCast(slice_ms)), .awake) catch return;
    }
}

/// Writes a Prometheus text-format metric (HELP, TYPE, value) for a single
/// scalar counter/gauge. Matches the format used by termite-zig.
pub fn appendPromMetric(
    writer: *std.Io.Writer,
    name: []const u8,
    metric_type: []const u8,
    help: []const u8,
    value: u64,
) !void {
    try appendPromMetricHeader(writer, name, metric_type, help);
    try appendPromSample(writer, name, value);
}

pub const PromLabel = struct {
    name: []const u8,
    value: []const u8,
};

pub fn appendPromMetricLabeled(
    writer: *std.Io.Writer,
    name: []const u8,
    metric_type: []const u8,
    help: []const u8,
    labels: []const PromLabel,
    value: u64,
) !void {
    try appendPromMetricHeader(writer, name, metric_type, help);
    try appendPromSampleLabeled(writer, name, labels, value);
}

pub fn appendPromMetricHeader(
    writer: *std.Io.Writer,
    name: []const u8,
    metric_type: []const u8,
    help: []const u8,
) !void {
    try writer.print("# HELP {s} {s}\n# TYPE {s} {s}\n", .{ name, help, name, metric_type });
}

pub fn appendPromSample(writer: *std.Io.Writer, name: []const u8, value: u64) !void {
    try writer.print("{s} {d}\n", .{ name, value });
}

pub fn appendPromSampleLabeled(
    writer: *std.Io.Writer,
    name: []const u8,
    labels: []const PromLabel,
    value: u64,
) !void {
    try writer.print("{s}", .{name});
    try appendPromLabels(writer, labels);
    try writer.print(" {d}\n", .{value});
}

fn appendPromLabels(writer: *std.Io.Writer, labels: []const PromLabel) !void {
    if (labels.len == 0) return;
    try writer.print("{{", .{});
    for (labels, 0..) |label, i| {
        if (i > 0) try writer.print(",", .{});
        try writer.print("{s}=\"", .{label.name});
        try appendPromLabelValue(writer, label.value);
        try writer.print("\"", .{});
    }
    try writer.print("}}", .{});
}

fn appendPromLabelValue(writer: *std.Io.Writer, value: []const u8) !void {
    for (value) |c| {
        switch (c) {
            '\\' => try writer.print("\\\\", .{}),
            '"' => try writer.print("\\\"", .{}),
            '\n' => try writer.print("\\n", .{}),
            else => try writer.print("{c}", .{c}),
        }
    }
}

fn pathOnly(uri: []const u8) []const u8 {
    const query_index = std.mem.indexOfScalar(u8, uri, '?') orelse return uri;
    return uri[0..query_index];
}

fn jsonResponse(alloc: std.mem.Allocator, status: u16, body: []const u8) !HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "application/json"),
        .body = try alloc.dupe(u8, body),
    };
}

fn textResponse(alloc: std.mem.Allocator, status: u16, body: []const u8) !HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "text/plain; charset=utf-8"),
        .body = try alloc.dupe(u8, body),
    };
}

fn buildMetricsBody(alloc: std.mem.Allocator, metrics: ?MetricsWriter) ![]u8 {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    defer writer.deinit();

    if (metrics) |m| {
        try m.writeMetrics(&writer.writer);
    }

    return try alloc.dupe(u8, writer.writer.buffered());
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
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
    const hs = try HealthServer.init(alloc, .{ .bind_port = 0 }, null, null);
    defer hs.deinit();

    var resp = try hs.executor().execute(alloc, .{ .method = .GET, .uri = "/healthz" });
    defer resp.deinit(alloc);

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(std.mem.indexOf(u8, resp.body, "ok") != null);
}

test "health server readyz reports 200 when ready" {
    const alloc = testing.allocator;
    var fake = FakeReady{ .ready = true };
    const hs = try HealthServer.init(alloc, .{ .bind_port = 0 }, fake.iface(), null);
    defer hs.deinit();

    var resp = try hs.executor().execute(alloc, .{ .method = .GET, .uri = "/readyz" });
    defer resp.deinit(alloc);

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(std.mem.indexOf(u8, resp.body, "ready") != null);
}

test "health server readyz reports 503 when not ready" {
    const alloc = testing.allocator;
    var fake = FakeReady{ .ready = false };
    const hs = try HealthServer.init(alloc, .{ .bind_port = 0 }, fake.iface(), null);
    defer hs.deinit();

    var resp = try hs.executor().execute(alloc, .{ .method = .GET, .uri = "/readyz" });
    defer resp.deinit(alloc);

    try testing.expectEqual(@as(u16, 503), resp.status);
    try testing.expect(std.mem.indexOf(u8, resp.body, "not_ready") != null);
}

test "health server metrics returns prometheus text" {
    const alloc = testing.allocator;
    var fake = FakeMetrics{};
    const hs = try HealthServer.init(alloc, .{ .bind_port = 0 }, null, fake.iface());
    defer hs.deinit();

    var resp = try hs.executor().execute(alloc, .{ .method = .GET, .uri = "/metrics" });
    defer resp.deinit(alloc);

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(resp.content_type != null);
    try testing.expect(std.mem.indexOf(u8, resp.content_type.?, "text/plain") != null);
    try testing.expect(std.mem.indexOf(u8, resp.body, "# HELP antfly_test_metric_total") != null);
    try testing.expect(std.mem.indexOf(u8, resp.body, "antfly_test_metric_total 42") != null);
    try testing.expectEqual(@as(usize, 1), fake.call_count);
}

test "health server metrics serves cached payload within ttl" {
    const alloc = testing.allocator;
    var fake = FakeMetrics{};
    const hs = try HealthServer.init(alloc, .{ .bind_port = 0 }, null, fake.iface());
    defer hs.deinit();

    var resp_a = try hs.executor().execute(alloc, .{ .method = .GET, .uri = "/metrics" });
    defer resp_a.deinit(alloc);
    var resp_b = try hs.executor().execute(alloc, .{ .method = .GET, .uri = "/metrics" });
    defer resp_b.deinit(alloc);

    try testing.expectEqualStrings(resp_a.body, resp_b.body);
    try testing.expectEqual(@as(usize, 1), fake.call_count);
}

test "health server metrics request path does not refresh stale cache" {
    const alloc = testing.allocator;
    var fake = FakeMetrics{};
    const hs = try HealthServer.init(alloc, .{ .bind_port = 0 }, null, fake.iface());
    defer hs.deinit();

    lockAtomic(&hs.metrics_cache_mutex);
    hs.metrics_cache_built_at_ms = 0;
    hs.metrics_cache_mutex.unlock();

    var resp = try hs.executor().execute(alloc, .{ .method = .GET, .uri = "/metrics" });
    defer resp.deinit(alloc);

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expectEqual(@as(usize, 1), fake.call_count);
}

test "health server startIfConfiguredOnHost uses provided bind host" {
    const alloc = testing.allocator;
    const hs = (try HealthServer.startIfConfiguredOnHost(alloc, "test", "127.0.0.1", 0, null, null)).?;
    defer hs.deinit();

    try testing.expectEqualStrings("127.0.0.1", hs.listener.cfg.bind_host);
}

test "health server unknown path returns 404" {
    const alloc = testing.allocator;
    const hs = try HealthServer.init(alloc, .{ .bind_port = 0 }, null, null);
    defer hs.deinit();

    var resp = try hs.executor().execute(alloc, .{ .method = .GET, .uri = "/nope" });
    defer resp.deinit(alloc);

    try testing.expectEqual(@as(u16, 404), resp.status);
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
