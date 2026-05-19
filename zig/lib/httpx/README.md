# httpx.zig

A high-performance HTTP client and server library for Zig 0.16+, with HTTP/1.1, HTTP/2, TLS, connection pooling, and pattern-based routing. Pure Zig, no external dependencies.

> Originally forked from [muhammad-fiaz/httpx.zig](https://github.com/muhammad-fiaz/httpx.zig). This fork has diverged significantly with HTTP/2 production hardening, HPACK optimizations, expanded test coverage (398 tests), security hardening, and Zig 0.16 `std.Io` fiber-based concurrency support.

## Installation

Add to your `build.zig.zon`:

```zig
.dependencies = .{
    .httpx = .{
        .url = "git+https://github.com/antflydb/httpx.zig.git",
        .hash = "...", // zig fetch --save will fill this
    },
},
```

Or fetch directly:

```bash
zig fetch --save git+https://github.com/antflydb/httpx.zig.git
```

Then in your `build.zig`:

```zig
const httpx_dep = b.dependency("httpx", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("httpx", httpx_dep.module("httpx"));
```

## Quick Start

### Client

```zig
const std = @import("std");
const httpx = @import("httpx");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var client = httpx.Client.init(allocator);
    defer client.deinit();

    // GET
    var response = try client.get("https://httpbin.org/get", .{});
    defer response.deinit();

    if (response.ok()) {
        std.debug.print("Response: {s}\n", .{response.text() orelse ""});
    }

    // POST with JSON
    var post_response = try client.post("https://httpbin.org/post", .{
        .json = "{\"name\": \"John\"}",
    });
    defer post_response.deinit();
}
```

### Server

```zig
const std = @import("std");
const httpx = @import("httpx");

fn hello(ctx: *httpx.Context) anyerror!httpx.Response {
    return ctx.json(.{ .message = "Hello, World!" });
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var server = httpx.Server.init(allocator);
    defer server.deinit();

    try server.use(httpx.logger());
    try server.use(httpx.cors(.{}));

    try server.get("/", hello);
    try server.listen();
}
```

## Features

| Feature | Description |
|---------|-------------|
| **HTTP/1.0 and HTTP/1.1** | Full client and server support with chunked transfer encoding |
| **HTTP/2** | HPACK header compression (RFC 7541), stream multiplexing, flow control |
| **HTTP/3** | QPACK header compression (RFC 9204), QUIC framing (RFC 9000) |
| **TLS 1.3** | Secure connections via Zig's `std.crypto.tls` |
| **Connection Pooling** | Automatic TCP connection reuse with keep-alive and health checking |
| **Pattern Routing** | Dynamic path parameters (`:id`), route groups, wildcard routes |
| **Middleware** | CORS, logging, security headers (Helmet), request ID, custom middleware |
| **Concurrency** | Parallel request patterns: `race`, `all`, `any` via `std.Io` fibers |
| **Streaming** | Chunked responses, Server-Sent Events (SSE), trailers |
| **Static Files** | File serving with path traversal protection |
| **Cookies** | First-class cookie helpers for client and server |
| **JSON and HTML** | Built-in response helpers for JSON serialization and HTML |
| **Interceptors** | Global hooks to modify requests and responses |
| **Retries** | Configurable retry policies with exponential backoff |
| **Zero Dependencies** | Pure Zig — no C libraries, no system dependencies |

## Examples

The `examples/` directory has runnable demos:

- **Client**: `simple_get.zig`, `post_json.zig`, `custom_headers.zig`, `connection_pool.zig`
- **JSON Parse**: `simple_get_deserialize.zig`
- **Cookies**: `cookies_demo.zig`
- **Concurrency**: `concurrent_requests.zig`
- **Streaming**: `streaming.zig`
- **Server**: `simple_server.zig`, `router_example.zig`, `middleware_example.zig`
- **Static Files**: `static_files.zig`
- **Website**: `multi_page_website.zig`
- **Protocol**: `http2_example.zig`, `http3_example.zig`

```bash
zig build run-simple_get
```

## Testing

```bash
zig build test --summary all
```

398 tests covering protocol parsing, HPACK compression, HTTP/2 framing, headers, URI parsing, routing, middleware, encoding, security (path traversal, injection prevention), and more.

## Requirements

| Requirement | Version |
|-------------|---------|
| **Zig** | 0.16.0+ |
| **OS** | Linux, macOS, Windows, FreeBSD |

## License

MIT License — see [LICENSE](LICENSE) for details.
