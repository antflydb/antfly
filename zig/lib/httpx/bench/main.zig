//! httpx.zig Benchmarks
//!
//! Performance benchmarks for core httpx.zig operations.

const std = @import("std");
const httpx = @import("httpx");

const BenchConfig = struct {
    iterations: usize,
    warmup_iterations: usize,
    rounds: usize,
};

fn runBenchmark(name: []const u8, cfg: BenchConfig, func: *const fn () void) void {
    for (0..cfg.warmup_iterations) |_| {
        func();
    }

    var min_ns: u64 = std.math.maxInt(u64);
    var max_ns: u64 = 0;
    var total_ns: u128 = 0;

    for (0..cfg.rounds) |_| {
        const start = std.c.mach_absolute_time();
        for (0..cfg.iterations) |_| {
            func();
        }
        const end = std.c.mach_absolute_time();

        const elapsed_ns = end - start;
        min_ns = @min(min_ns, elapsed_ns);
        max_ns = @max(max_ns, elapsed_ns);
        total_ns += elapsed_ns;
    }

    const avg_ns = @as(u64, @intCast(total_ns / cfg.rounds));
    const min_ns_per_op = @as(f64, @floatFromInt(min_ns)) / @as(f64, @floatFromInt(cfg.iterations));
    const avg_ns_per_op = @as(f64, @floatFromInt(avg_ns)) / @as(f64, @floatFromInt(cfg.iterations));
    const max_ns_per_op = @as(f64, @floatFromInt(max_ns)) / @as(f64, @floatFromInt(cfg.iterations));

    const ops_per_sec = if (avg_ns_per_op > 0.0)
        @as(u64, @intFromFloat(1_000_000_000.0 / avg_ns_per_op))
    else
        0;

    std.debug.print("  {s: <22} rounds={d} iters={d} min={d:.2}ns/op avg={d:.2}ns/op max={d:.2}ns/op throughput={d} ops/sec\n", .{
        name,
        cfg.rounds,
        cfg.iterations,
        min_ns_per_op,
        avg_ns_per_op,
        max_ns_per_op,
        ops_per_sec,
    });
}

var bench_allocator: std.mem.Allocator = undefined;

fn benchHeadersParse() void {
    var headers = httpx.Headers.init(bench_allocator);
    defer headers.deinit();

    headers.append("Content-Type", "application/json") catch {};
    headers.append("Authorization", "Bearer token") catch {};
    headers.append("Accept", "application/json") catch {};
    headers.append("User-Agent", "benchmark") catch {};

    _ = headers.get("Content-Type");
    _ = headers.get("Authorization");
}

fn benchUriParse() void {
    _ = httpx.Uri.parse("https://api.example.com:8080/users/123?page=1&limit=10#section") catch {};
}

fn benchStatusLookup() void {
    _ = httpx.status.reasonPhrase(200);
    _ = httpx.status.reasonPhrase(404);
    _ = httpx.status.reasonPhrase(500);
}

fn benchBase64Encode() void {
    const data = "Hello, World! This is a test string for base64 encoding.";
    const encoded = httpx.Base64.encode(bench_allocator, data) catch return;
    bench_allocator.free(encoded);
}

fn benchBase64Decode() void {
    const encoded = "SGVsbG8sIFdvcmxkISBUaGlzIGlzIGEgdGVzdCBzdHJpbmcgZm9yIGJhc2U2NCBlbmNvZGluZy4=";
    const decoded = httpx.Base64.decode(bench_allocator, encoded) catch return;
    bench_allocator.free(decoded);
}

fn benchJsonBuilder() void {
    var builder = httpx.json.JsonBuilder.init(bench_allocator);
    defer builder.deinit();

    builder.beginObject() catch {};
    builder.key("name") catch {};
    builder.string("John") catch {};
    builder.key("age") catch {};
    builder.number(30) catch {};
    builder.key("active") catch {};
    builder.boolean(true) catch {};
    builder.endObject() catch {};
}

fn benchMethodLookup() void {
    _ = httpx.Method.fromString("GET");
    _ = httpx.Method.fromString("POST");
    _ = httpx.Method.fromString("DELETE");
}

fn benchRequestBuild() void {
    var request = httpx.Request.init(bench_allocator, .GET, "https://api.example.com/users") catch return;
    defer request.deinit();

    request.headers.set("Accept", "application/json") catch {};
    request.addQueryParam("page", "1") catch {};
}

fn benchResponseBuilders() void {
    var text_resp = httpx.Response.fromText(bench_allocator, 200, "ok") catch return;
    defer text_resp.deinit();

    var json_resp = httpx.Response.fromJson(bench_allocator, 200, .{ .ok = true, .source = "bench" }) catch return;
    defer json_resp.deinit();
}

fn benchHttp2FrameHeader() void {
    const header = httpx.Http2FrameHeader{
        .length = 1024,
        .frame_type = .data,
        .flags = 0x01,
        .stream_id = 1,
    };
    const serialized = header.serialize();
    _ = httpx.Http2FrameHeader.parse(serialized);
}

fn benchVarIntEncoding() void {
    var buf: [8]u8 = undefined;
    _ = httpx.http.encodeVarInt(494878333, &buf) catch 0;
}

pub fn main() !void {
    bench_allocator = std.heap.page_allocator;

    std.debug.print("=== httpx.zig Benchmarks ===\n\n", .{});
    std.debug.print("Host: {s}-{s} ({s})\n\n", .{
        @tagName(@import("builtin").cpu.arch),
        @tagName(@import("builtin").os.tag),
        @tagName(@import("builtin").mode),
    });

    const core_cfg = BenchConfig{ .iterations = 200_000, .warmup_iterations = 5_000, .rounds = 5 };
    const heavy_cfg = BenchConfig{ .iterations = 100_000, .warmup_iterations = 2_000, .rounds = 5 };
    const parser_cfg = BenchConfig{ .iterations = 1_000_000, .warmup_iterations = 20_000, .rounds = 5 };

    std.debug.print("Core Operations:\n", .{});
    runBenchmark("headers_parse", core_cfg, benchHeadersParse);
    runBenchmark("uri_parse", core_cfg, benchUriParse);
    runBenchmark("status_lookup", parser_cfg, benchStatusLookup);
    runBenchmark("method_lookup", parser_cfg, benchMethodLookup);

    std.debug.print("\nEncoding:\n", .{});
    runBenchmark("base64_encode", heavy_cfg, benchBase64Encode);
    runBenchmark("base64_decode", heavy_cfg, benchBase64Decode);
    runBenchmark("json_builder", heavy_cfg, benchJsonBuilder);

    std.debug.print("\nRequest Building:\n", .{});
    runBenchmark("request_build", heavy_cfg, benchRequestBuild);
    runBenchmark("response_builders", heavy_cfg, benchResponseBuilders);

    std.debug.print("\nHTTP/2 & HTTP/3:\n", .{});
    runBenchmark("h2_frame_header", parser_cfg, benchHttp2FrameHeader);
    runBenchmark("h3_varint_encode", BenchConfig{ .iterations = 5_000_000, .warmup_iterations = 50_000, .rounds = 5 }, benchVarIntEncoding);

    std.debug.print("\n=== Benchmark Complete ===\n", .{});
}
