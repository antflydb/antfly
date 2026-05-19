//! Concurrent Request Patterns for httpx.zig
//!
//! Provides parallel request execution patterns:
//!
//! - `all`: Execute all requests, wait for all to complete
//! - `any`: Execute all requests, return first successful
//! - `race`: Execute all requests, return first to complete
//! - Batch request building

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const Client = @import("../client/client.zig").Client;
const Response = @import("../core/response.zig").Response;
const types = @import("../core/types.zig");

/// Request specification for batch operations.
pub const RequestSpec = struct {
    method: types.Method = .GET,
    url: []const u8,
    body: ?[]const u8 = null,
    headers: ?[]const [2][]const u8 = null,
};

/// Result of a parallel request.
pub const RequestResult = union(enum) {
    success: Response,
    err: anyerror,

    pub fn isSuccess(self: RequestResult) bool {
        return self == .success;
    }

    pub fn getResponse(self: *RequestResult) ?*Response {
        switch (self) {
            .success => |*r| return r,
            .err => return null,
        }
    }

    pub fn deinit(self: *RequestResult) void {
        switch (self.*) {
            .success => |*r| r.deinit(),
            .err => {},
        }
    }
};

/// Batch request builder for parallel execution.
pub const BatchBuilder = struct {
    allocator: Allocator,
    requests: std.ArrayListUnmanaged(RequestSpec) = .empty,

    const Self = @This();

    /// Creates a new batch builder.
    pub fn init(allocator: Allocator) Self {
        return .{ .allocator = allocator };
    }

    /// Releases builder resources.
    pub fn deinit(self: *Self) void {
        self.requests.deinit(self.allocator);
    }

    /// Adds a GET request to the batch.
    pub fn get(self: *Self, url: []const u8) !*Self {
        try self.requests.append(self.allocator, .{ .method = .GET, .url = url });
        return self;
    }

    /// Adds a POST request to the batch.
    pub fn post(self: *Self, url: []const u8, body: ?[]const u8) !*Self {
        try self.requests.append(self.allocator, .{ .method = .POST, .url = url, .body = body });
        return self;
    }

    /// Adds a PUT request to the batch.
    pub fn put(self: *Self, url: []const u8, body: ?[]const u8) !*Self {
        try self.requests.append(self.allocator, .{ .method = .PUT, .url = url, .body = body });
        return self;
    }

    /// Adds a DELETE request to the batch.
    pub fn delete(self: *Self, url: []const u8) !*Self {
        try self.requests.append(self.allocator, .{ .method = .DELETE, .url = url });
        return self;
    }

    /// Adds a custom request to the batch.
    pub fn add(self: *Self, spec: RequestSpec) !*Self {
        try self.requests.append(self.allocator, spec);
        return self;
    }

    /// Returns the number of requests in the batch.
    pub fn count(self: *const Self) usize {
        return self.requests.items.len;
    }

    /// Clears all requests from the batch.
    pub fn clear(self: *Self) void {
        self.requests.clearRetainingCapacity();
    }
};

// ---------------------------------------------------------------------------
// Shared dispatch helper
// ---------------------------------------------------------------------------

/// Dispatches all specs as fibers, falling back to sequential execution if
/// the Io backend does not support concurrency. Used by all/any/race.
fn dispatchAll(
    client: *Client,
    specs: []const RequestSpec,
    results: []RequestResult,
    comptime FiberFn: type,
    fiber_fn: FiberFn,
    extra_args: anytype,
) void {
    var group = Io.Group.init;
    var used_fibers = false;

    for (specs, 0..) |spec, i| {
        group.concurrent(client.io, fiber_fn, .{ client, spec, &results[i] } ++ extra_args) catch {
            // Concurrency unavailable — run remaining sequentially.
            runSequential(client, specs[i..], results[i..]);
            if (used_fibers) group.await(client.io) catch {};
            return;
        };
        used_fibers = true;
    }

    if (used_fibers) group.await(client.io) catch {};
}

fn runSequential(client: *Client, specs: []const RequestSpec, results: []RequestResult) void {
    for (specs, 0..) |spec, i| {
        results[i] = executeSpec(client, spec);
    }
}

// ---------------------------------------------------------------------------
// Fiber entry points
// ---------------------------------------------------------------------------

fn executeSpecFiber(client: *Client, spec: RequestSpec, out: *RequestResult) Io.Cancelable!void {
    out.* = executeSpec(client, spec);
}

fn raceSpecFiber(client: *Client, spec: RequestSpec, out: *RequestResult, winner_idx: *std.atomic.Value(usize), idx: usize) Io.Cancelable!void {
    out.* = executeSpec(client, spec);
    _ = winner_idx.cmpxchgStrong(std.math.maxInt(usize), idx, .acq_rel, .acquire);
}

fn executeSpec(client: *Client, spec: RequestSpec) RequestResult {
    const result = client.request(spec.method, spec.url, .{
        .body = spec.body,
        .headers = spec.headers,
    });

    if (result) |response| {
        return .{ .success = response };
    } else |err| {
        return .{ .err = err };
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Executes all requests concurrently using Io fibers and waits for
/// all to complete. Falls back to sequential execution if the Io
/// backend does not support concurrency.
pub fn all(allocator: Allocator, client: *Client, specs: []const RequestSpec) ![]RequestResult {
    const results = try allocator.alloc(RequestResult, specs.len);
    errdefer allocator.free(results);
    if (specs.len == 0) return results;

    // Initialize so partial fiber failures don't leave uninitialized entries.
    for (results) |*r| r.* = .{ .err = error.Pending };

    dispatchAll(client, specs, results, @TypeOf(executeSpecFiber), executeSpecFiber, .{});
    return results;
}


/// Executes all requests concurrently and returns the first successful response.
/// Uses Io fibers when available, falls back to sequential execution.
pub fn any(allocator: Allocator, client: *Client, specs: []const RequestSpec) !?Response {
    if (specs.len == 0) return null;

    const results = try allocator.alloc(RequestResult, specs.len);
    defer {
        for (results) |*r| r.deinit();
        allocator.free(results);
    }
    for (results) |*r| r.* = .{ .err = error.Pending };

    dispatchAll(client, specs, results, @TypeOf(executeSpecFiber), executeSpecFiber, .{});

    // Return first successful response, transferring ownership to caller.
    for (results) |*r| {
        switch (r.*) {
            .success => |resp| {
                if (resp.status.isSuccess()) {
                    r.* = .{ .err = error.OwnershipTransferred };
                    return resp;
                }
            },
            .err => {},
        }
    }
    return null;
}

/// Executes all requests concurrently and returns the first to complete.
/// Uses Io fibers when available, falls back to sequential execution.
/// The winner is determined by an atomic compare-and-swap on a shared index.
pub fn race(allocator: Allocator, client: *Client, specs: []const RequestSpec) !RequestResult {
    if (specs.len == 0) return .{ .err = error.NoRequests };

    const no_winner = std.math.maxInt(usize);

    var results = try allocator.alloc(RequestResult, specs.len);
    defer {
        for (results) |*r| r.deinit();
        allocator.free(results);
    }
    for (results) |*r| r.* = .{ .err = error.Pending };

    var winner_idx = std.atomic.Value(usize).init(no_winner);

    // race needs a different fiber that also writes to winner_idx.
    var group = Io.Group.init;
    var used_fibers = false;

    for (specs, 0..) |spec, i| {
        group.concurrent(client.io, raceSpecFiber, .{ client, spec, &results[i], &winner_idx, i }) catch {
            // Concurrency unavailable — run remaining sequentially.
            for (specs[i..], i..) |s, j| {
                results[j] = executeSpec(client, s);
                _ = winner_idx.cmpxchgStrong(no_winner, j, .acq_rel, .acquire);
            }
            if (used_fibers) group.await(client.io) catch {};
            break;
        };
        used_fibers = true;
    }

    if (used_fibers) group.await(client.io) catch {};

    const win = winner_idx.load(.acquire);
    if (win == no_winner) return .{ .err = error.NoRequests };

    // Transfer ownership of the winner to the caller.
    const result = results[win];
    results[win] = .{ .err = error.OwnershipTransferred };
    return result;
}

test "BatchBuilder" {
    const allocator = std.testing.allocator;
    var builder = BatchBuilder.init(allocator);
    defer builder.deinit();

    _ = try builder.get("https://api.example.com/users");
    _ = try builder.post("https://api.example.com/users", "{\"name\":\"test\"}");

    try std.testing.expectEqual(@as(usize, 2), builder.count());
}

test "BatchBuilder clear" {
    const allocator = std.testing.allocator;
    var builder = BatchBuilder.init(allocator);
    defer builder.deinit();

    _ = try builder.get("https://example.com");
    try std.testing.expectEqual(@as(usize, 1), builder.count());

    builder.clear();
    try std.testing.expectEqual(@as(usize, 0), builder.count());
}

test "RequestResult" {
    var success_result = RequestResult{ .err = error.OutOfMemory };
    try std.testing.expect(!success_result.isSuccess());

    success_result.deinit();
}

test "RequestSpec" {
    const spec = RequestSpec{
        .method = .POST,
        .url = "https://api.example.com",
        .body = "{\"key\":\"value\"}",
    };

    try std.testing.expectEqual(types.Method.POST, spec.method);
    try std.testing.expect(spec.body != null);
}


test "any empty" {
    const allocator = std.testing.allocator;
    var client = Client.init(allocator, std.testing.io);
    defer client.deinit();

    const result = try any(allocator, &client, &.{});
    try std.testing.expect(result == null);
}

test "race empty" {
    const allocator = std.testing.allocator;
    var client = Client.init(allocator, std.testing.io);
    defer client.deinit();

    const result = try race(allocator, &client, &.{});
    try std.testing.expect(result == .err);
}
