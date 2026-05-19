//! httpx.zig Concurrency Benchmarks
//!
//! Integration benchmarks comparing sequential vs Io-fiber concurrent
//! request execution against a local echo server.
//!
//! Run with: zig build bench-concurrency

const std = @import("std");
const httpx = @import("httpx");
const builtin = @import("builtin");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const Thread = std.Thread;

const SERVER_HOST = "127.0.0.1";
const SERVER_PORT: u16 = 18_080;
const ROUNDS = 5;
const WARMUP = 20;

// ---------------------------------------------------------------------------
// Platform timer (ns resolution)
// ---------------------------------------------------------------------------

fn nowNs() u64 {
    return std.c.mach_absolute_time();
}

fn sleepMs(ms: u64) void {
    const ts = std.c.timespec{
        .sec = @intCast(ms / 1000),
        .nsec = @intCast((ms % 1000) * std.time.ns_per_ms),
    };
    _ = std.c.nanosleep(&ts, null);
}

// ---------------------------------------------------------------------------
// Echo server (runs on a background OS thread)
// ---------------------------------------------------------------------------

fn echoHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return ctx.json(.{ .ok = true });
}

fn serverThread(server: *httpx.Server) void {
    server.listen() catch |err| {
        std.debug.print("Server error: {}\n", .{err});
    };
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000_000.0;
}

fn printRow(batch: usize, seq_ns: u64, thread_ns: u64, fiber_ns: u64) void {
    const seq_ms = nsToMs(seq_ns);
    const thread_ms = nsToMs(thread_ns);
    const fiber_ms = nsToMs(fiber_ns);
    const thread_speedup = if (thread_ms > 0.001) seq_ms / thread_ms else 0.0;
    const fiber_speedup = if (fiber_ms > 0.001) seq_ms / fiber_ms else 0.0;

    std.debug.print("  {d: <6} {d: >10.2}ms  {d: >10.2}ms {d: >6.2}x  {d: >10.2}ms {d: >6.2}x\n", .{
        batch,
        seq_ms,
        thread_ms,
        thread_speedup,
        fiber_ms,
        fiber_speedup,
    });
}

// ---------------------------------------------------------------------------
// Benchmark routines
// ---------------------------------------------------------------------------

fn benchSequential(client: *httpx.Client, url: []const u8, n: usize) u64 {
    const start = nowNs();
    for (0..n) |_| {
        var resp = client.get(url, .{}) catch continue;
        resp.deinit();
    }
    return nowNs() - start;
}

fn benchAll(allocator: Allocator, client: *httpx.Client, specs: []const httpx.RequestSpec) u64 {
    const start = nowNs();
    const results = httpx.concurrency.all(allocator, client, specs) catch return nowNs() - start;
    for (results) |*r| r.deinit();
    allocator.free(results);
    return nowNs() - start;
}

/// OS-thread-based all() for comparison with fiber version.
fn benchAllThreads(allocator: Allocator, client: *httpx.Client, specs: []const httpx.RequestSpec) u64 {
    const WorkerCtx = struct {
        client: *httpx.Client,
        spec: httpx.RequestSpec,
        out: *httpx.RequestResult,

        fn run(self: *@This()) void {
            const result = self.client.request(self.spec.method, self.spec.url, .{
                .body = self.spec.body,
                .headers = self.spec.headers,
            });
            self.out.* = if (result) |response| .{ .success = response } else |err| .{ .err = err };
        }
    };

    const start = nowNs();

    const results = allocator.alloc(httpx.RequestResult, specs.len) catch return 0;
    defer {
        for (results) |*r| r.deinit();
        allocator.free(results);
    }

    const threads = allocator.alloc(Thread, specs.len) catch return 0;
    defer allocator.free(threads);
    const ctxs = allocator.alloc(WorkerCtx, specs.len) catch return 0;
    defer allocator.free(ctxs);

    var spawned: usize = 0;
    for (specs, 0..) |spec, i| {
        ctxs[i] = .{ .client = client, .spec = spec, .out = &results[i] };
        threads[i] = Thread.spawn(.{}, WorkerCtx.run, .{&ctxs[i]}) catch {
            // Fallback: run remaining sequentially
            for (specs[i..], i..) |s, j| {
                const r = client.request(s.method, s.url, .{ .body = s.body, .headers = s.headers });
                results[j] = if (r) |resp| .{ .success = resp } else |err| .{ .err = err };
            }
            break;
        };
        spawned += 1;
    }
    for (threads[0..spawned]) |t| t.join();

    return nowNs() - start;
}

fn benchAny(allocator: Allocator, client: *httpx.Client, specs: []const httpx.RequestSpec) u64 {
    const start = nowNs();
    const maybe_resp = httpx.concurrency.any(allocator, client, specs) catch null;
    if (maybe_resp) |resp| {
        var r = resp;
        r.deinit();
    }
    return nowNs() - start;
}

fn benchRace(allocator: Allocator, client: *httpx.Client, specs: []const httpx.RequestSpec) u64 {
    const start = nowNs();
    var result = httpx.concurrency.race(allocator, client, specs) catch return nowNs() - start;
    result.deinit();
    return nowNs() - start;
}

/// Returns the best (minimum) time across ROUNDS invocations.
fn bestOf(
    comptime func: fn (Allocator, *httpx.Client, []const httpx.RequestSpec) u64,
    allocator: Allocator,
    client: *httpx.Client,
    specs: []const httpx.RequestSpec,
) u64 {
    var best: u64 = std.math.maxInt(u64);
    for (0..ROUNDS) |_| {
        best = @min(best, func(allocator, client, specs));
    }
    return best;
}

fn bestOfSeq(client: *httpx.Client, url: []const u8, n: usize) u64 {
    var best: u64 = std.math.maxInt(u64);
    for (0..ROUNDS) |_| {
        best = @min(best, benchSequential(client, url, n));
    }
    return best;
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var io_backend = Io.Threaded.init(allocator, .{});
    const io = io_backend.io();

    // -- Header --
    std.debug.print("=== httpx.zig Concurrency Benchmarks ===\n\n", .{});
    std.debug.print("Host: {s}-{s} ({s})\n", .{
        @tagName(builtin.cpu.arch),
        @tagName(builtin.os.tag),
        @tagName(builtin.mode),
    });
    std.debug.print("Server: {s}:{d}   Rounds: {d}   Warmup: {d}\n\n", .{
        SERVER_HOST, SERVER_PORT, ROUNDS, WARMUP,
    });

    // -- Start echo server on background OS thread --
    var server = httpx.Server.initWithConfig(allocator, io, .{
        .host = SERVER_HOST,
        .port = SERVER_PORT,
    });
    defer server.deinit();
    try server.get("/echo", echoHandler);

    const srv_thread = try Thread.spawn(.{}, serverThread, .{&server});
    sleepMs(500);
    defer {
        server.stop();
        srv_thread.join();
    }

    // -- Client --
    var client = httpx.Client.init(allocator, io);
    defer client.deinit();

    const url = try std.fmt.allocPrint(allocator, "http://{s}:{d}/echo", .{ SERVER_HOST, SERVER_PORT });

    // -- Warmup --
    std.debug.print("Warming up ({d} sequential requests)...", .{WARMUP});
    for (0..WARMUP) |_| {
        var resp = client.get(url, .{}) catch |err| {
            std.debug.print("\nWarmup failed: {}. Is port {d} available?\n", .{ err, SERVER_PORT });
            return err;
        };
        resp.deinit();
    }
    std.debug.print(" done.\n\n", .{});

    // -- pool.all(): sequential vs threads vs fibers --
    {
        std.debug.print("pool.all() — sequential vs OS threads vs Io fibers (best of {d} rounds):\n", .{ROUNDS});
        std.debug.print("  {s: <6} {s: >12}  {s: >12} {s: >7}  {s: >12} {s: >7}\n", .{ "batch", "sequential", "threads", "speedup", "fibers", "speedup" });
        std.debug.print("  {s:-<68}\n", .{""});

        const batch_sizes = [_]usize{ 1, 5, 10, 25, 50 };
        for (batch_sizes) |n| {
            const specs = try allocator.alloc(httpx.RequestSpec, n);
            for (specs) |*s| s.* = .{ .url = url };

            const seq_ns = bestOfSeq(&client, url, n);
            const thread_ns = bestOf(benchAllThreads, allocator, &client, specs);
            const fiber_ns = bestOf(benchAll, allocator, &client, specs);

            printRow(n, seq_ns, thread_ns, fiber_ns);
        }
    }

    // -- pool.any() --
    {
        std.debug.print("\npool.any() — first successful from N concurrent requests:\n", .{});
        std.debug.print("  {s: <6} {s: >12}\n", .{ "batch", "best" });
        std.debug.print("  {s:-<20}\n", .{""});

        const any_sizes = [_]usize{ 5, 10, 25 };
        for (any_sizes) |n| {
            const specs = try allocator.alloc(httpx.RequestSpec, n);
            for (specs) |*s| s.* = .{ .url = url };

            const ns = bestOf(benchAny, allocator, &client, specs);
            std.debug.print("  {d: <6} {d: >10.2}ms\n", .{ n, nsToMs(ns) });
        }
    }

    // -- pool.race() --
    {
        std.debug.print("\npool.race() — first to complete from N concurrent requests:\n", .{});
        std.debug.print("  {s: <6} {s: >12}\n", .{ "batch", "best" });
        std.debug.print("  {s:-<20}\n", .{""});

        const race_sizes = [_]usize{ 5, 10, 25 };
        for (race_sizes) |n| {
            const specs = try allocator.alloc(httpx.RequestSpec, n);
            for (specs) |*s| s.* = .{ .url = url };

            const ns = bestOf(benchRace, allocator, &client, specs);
            std.debug.print("  {d: <6} {d: >10.2}ms\n", .{ n, nsToMs(ns) });
        }
    }

    std.debug.print("\n=== Benchmark Complete ===\n", .{});
}
