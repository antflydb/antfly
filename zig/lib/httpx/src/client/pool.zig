//! HTTP Connection Pool for httpx.zig
//!
//! Provides connection pooling for HTTP clients:
//!
//! - Reusable TCP and TLS connections with keep-alive
//! - Per-host connection limits
//! - Automatic connection health checking
//! - Idle connection timeout and cleanup
//!
//! Both `ConnectionPool` (plain TCP) and `TlsPool` (TCP + TLS) are
//! instantiations of `GenericPool`, which handles all shared pool logic.
//! Connections are indexed by host:port in a HashMap for O(1) lookup.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const socket_mod = @import("../net/socket.zig");
const Socket = socket_mod.Socket;
const Address = socket_mod.Address;
const resolveAddress = socket_mod.resolveAddress;
const TlsConfig = @import("../tls/tls.zig").TlsConfig;
const TlsSession = @import("../tls/tls.zig").TlsSession;
const milliTimestamp = @import("../util/common.zig").milliTimestamp;

pub const PoolError = error{
    PoolExhausted,
    PoolExhaustedForHost,
};

// ---------------------------------------------------------------------------
// Connection entry types
// ---------------------------------------------------------------------------

/// Shared methods for pooled connection types.
fn ConnectionMixin(comptime Self: type) type {
    return struct {
        pub fn acquire(self: *Self, now: i64) void {
            self.in_use = true;
            self.last_used = now;
        }

        pub fn release(self: *Self, now: i64) void {
            self.in_use = false;
            self.last_used = now;
            self.requests_made += 1;
        }

        pub fn markBroken(self: *Self) void {
            self.broken = true;
        }

        pub fn baseIsHealthy(self: *const Self, max_idle_ms: i64, now: i64) bool {
            if (self.in_use) return false;
            if (self.broken) return false;
            return (now - self.last_used) < max_idle_ms;
        }

        pub fn baseShouldEvict(self: *const Self, idle_timeout_ms: i64, max_requests: u32, now: i64) bool {
            if (self.in_use) return false;
            if (self.broken) return true;
            if (self.requests_made >= max_requests) return true;
            return (now - self.last_used) >= idle_timeout_ms;
        }
    };
}

/// Pooled TCP connection.
pub const Connection = struct {
    socket: Socket,
    host: []const u8,
    port: u16,
    in_use: bool = false,
    broken: bool = false,
    created_at: i64,
    last_used: i64,
    requests_made: u32 = 0,

    const mixin = ConnectionMixin(Connection);
    pub const acquire = mixin.acquire;
    pub const release = mixin.release;
    pub const markBroken = mixin.markBroken;

    pub fn isHealthy(self: *const Connection, max_idle_ms: i64, now: i64) bool {
        return mixin.baseIsHealthy(self, max_idle_ms, now);
    }

    pub fn shouldEvict(self: *const Connection, idle_timeout_ms: i64, max_requests: u32, now: i64) bool {
        return mixin.baseShouldEvict(self, idle_timeout_ms, max_requests, now);
    }

    pub fn close(self: *Connection) void {
        self.socket.close();
    }

    /// Creates a new TCP connection (DNS resolve + TCP connect).
    pub fn createNew(allocator: Allocator, io: Io, host: []const u8, port: u16, _: void) !*Connection {
        const host_owned = try allocator.dupe(u8, host);
        errdefer allocator.free(host_owned);

        const addr = try resolveAddress(io, host, port);
        var socket = try Socket.connect(addr, io);
        errdefer socket.close();
        socket.setKeepAlive(true) catch {};

        const now = milliTimestamp(io);
        const conn = try allocator.create(Connection);
        conn.* = .{
            .socket = socket,
            .host = host_owned,
            .port = port,
            .in_use = true,
            .created_at = now,
            .last_used = now,
        };
        return conn;
    }
};

/// Context passed to `TlsConnection.createNew` for TLS-specific configuration.
pub const TlsPoolContext = struct {
    verify_ssl: bool,
};

/// Pooled TLS connection bundle.
///
/// Heap-allocated for pointer stability: `TlsSession` stores internal pointers
/// to its `SocketIoReader`/`SocketIoWriter`, which in turn reference the `Socket`
/// via `@fieldParentPtr`.  Moving the struct in memory would invalidate those
/// pointers, so every entry lives at a stable heap address.
pub const TlsConnection = struct {
    socket: Socket,
    session: TlsSession,
    host: []const u8,
    port: u16,
    in_use: bool = false,
    broken: bool = false,
    created_at: i64,
    last_used: i64,
    requests_made: u32 = 0,

    const mixin = ConnectionMixin(TlsConnection);
    pub const acquire = mixin.acquire;
    pub const release = mixin.release;
    pub const markBroken = mixin.markBroken;

    pub fn isHealthy(self: *const TlsConnection, max_idle_ms: i64, now: i64) bool {
        if (!self.session.connected) return false;
        return mixin.baseIsHealthy(self, max_idle_ms, now);
    }

    pub fn shouldEvict(self: *const TlsConnection, idle_timeout_ms: i64, max_requests: u32, now: i64) bool {
        if (!self.session.connected) return true;
        return mixin.baseShouldEvict(self, idle_timeout_ms, max_requests, now);
    }

    pub fn close(self: *TlsConnection) void {
        self.session.deinit();
        self.socket.close();
    }

    /// Creates a new TLS connection (DNS resolve + TCP connect + TLS handshake).
    pub fn createNew(allocator: Allocator, io: Io, host: []const u8, port: u16, ctx: TlsPoolContext) !*TlsConnection {
        const host_owned = try allocator.dupe(u8, host);
        errdefer allocator.free(host_owned);

        const entry = try allocator.create(TlsConnection);
        errdefer allocator.destroy(entry);

        // Resolve and connect the TCP socket.
        const addr = try resolveAddress(io, host, port);
        entry.socket = try Socket.connect(addr, io);
        errdefer entry.socket.close();

        // Initialize the TLS session.  The session stores internal pointers to
        // entry.socket (via SocketIoReader/Writer), so the entry must already
        // live at its final heap address — which it does.
        const tls_cfg = if (ctx.verify_ssl) TlsConfig.init(allocator) else TlsConfig.insecure(allocator);
        entry.session = TlsSession.init(tls_cfg, io);
        errdefer entry.session.deinit();

        entry.session.attachSocket(&entry.socket);
        try entry.session.handshake(host);

        const now = milliTimestamp(io);
        entry.host = host_owned;
        entry.port = port;
        entry.in_use = true;
        entry.broken = false;
        entry.created_at = now;
        entry.last_used = now;
        entry.requests_made = 0;

        return entry;
    }
};

// ---------------------------------------------------------------------------
// Pool configuration and stats
// ---------------------------------------------------------------------------

/// Connection pool configuration.
pub const PoolConfig = struct {
    max_connections: u32 = 20,
    max_per_host: u32 = 5,
    idle_timeout_ms: i64 = 60_000,
    max_requests_per_connection: u32 = 1000,
    health_check_interval_ms: i64 = 30_000,
};

/// Snapshot statistics for a connection pool.
pub const PoolStats = struct {
    total: usize,
    active: usize,
    idle: usize,
};

// ---------------------------------------------------------------------------
// Generic connection pool
// ---------------------------------------------------------------------------

/// Generic connection pool parameterized on entry type and creation context.
///
/// `Entry` must provide:
///   - Fields: `host: []const u8`, `port: u16`, `in_use: bool`, `requests_made: u32`
///   - Methods: `acquire(now)`, `release(now)`, `isHealthy(ms, now)`,
///     `shouldEvict(ms, max, now)`, `close()`, `markBroken()`
///   - Class method: `createNew(allocator, io, host, port, context) !*Entry`
///
/// `Context` is an arbitrary type forwarded to `Entry.createNew`.
/// Use `void` when no extra context is needed (e.g. plain TCP).
///
/// Connections are indexed by `"host:port"` in a `StringHashMap` for O(1)
/// per-host lookup, matching Go's `net/http.Transport.idleConn` approach.
pub fn GenericPool(comptime Entry: type, comptime Context: type) type {
    return struct {
        allocator: Allocator,
        io: Io,
        config: PoolConfig,
        context: Context,
        /// Connections indexed by "host:port" key.
        host_map: std.StringHashMapUnmanaged(std.ArrayListUnmanaged(*Entry)) = .{},
        total_count: usize = 0,
        active_count: usize = 0,
        mutex: Io.Mutex = Io.Mutex.init,
        last_cleanup: i64 = 0,

        const Self = @This();

        /// Creates a new pool with default configuration.
        pub fn init(allocator: Allocator, io: Io, context: Context) Self {
            return initWithConfig(allocator, io, .{}, context);
        }

        /// Creates a pool with custom configuration.
        pub fn initWithConfig(allocator: Allocator, io: Io, config: PoolConfig, context: Context) Self {
            return .{
                .allocator = allocator,
                .io = io,
                .config = config,
                .context = context,
            };
        }

        /// Releases all pool resources.
        pub fn deinit(self: *Self) void {
            var it = self.host_map.iterator();
            while (it.next()) |map_entry| {
                for (map_entry.value_ptr.items) |entry| {
                    entry.close();
                    self.allocator.free(entry.host);
                    self.allocator.destroy(entry);
                }
                map_entry.value_ptr.deinit(self.allocator);
                self.allocator.free(map_entry.key_ptr.*);
            }
            self.host_map.deinit(self.allocator);
        }

        /// Format a "host:port" lookup key into a stack buffer.
        fn formatHostKey(buf: []u8, host: []const u8, port: u16) ?[]const u8 {
            return std.fmt.bufPrint(buf, "{s}:{d}", .{ host, port }) catch null;
        }

        /// Allocate a heap-owned "host:port" key for map insertion.
        fn allocHostKey(allocator: Allocator, host: []const u8, port: u16) ![]u8 {
            return std.fmt.allocPrint(allocator, "{s}:{d}", .{ host, port });
        }

        /// Gets an idle connection for `(host, port)`, or creates a new one.
        ///
        /// The returned pointer is heap-allocated and marked in-use.
        /// Callers must call `releaseConnection` or `evictConnection` when done.
        pub fn getConnection(self: *Self, host: []const u8, port: u16) !*Entry {
            var key_buf: [280]u8 = undefined;
            const key = formatHostKey(&key_buf, host, port) orelse return error.InvalidUri;

            {
                self.mutex.lockUncancelable(self.io);
                defer self.mutex.unlock(self.io);

                // Periodic cleanup: evict stale/broken connections.
                const now = milliTimestamp(self.io);
                if (now - self.last_cleanup > self.config.health_check_interval_ms) {
                    self.cleanupLocked(now);
                    self.last_cleanup = now;
                }

                if (self.host_map.getPtr(key)) |list| {
                    var i: usize = 0;
                    while (i < list.items.len) {
                        const entry = list.items[i];
                        if (entry.isHealthy(self.config.idle_timeout_ms, now) and
                            entry.requests_made < self.config.max_requests_per_connection)
                        {
                            entry.acquire(now);
                            self.active_count += 1;
                            return entry;
                        }
                        // Evict stale/broken connections inline so they don't
                        // count against max_per_host.
                        if (entry.shouldEvict(self.config.idle_timeout_ms, self.config.max_requests_per_connection, now)) {
                            entry.close();
                            self.allocator.free(entry.host);
                            self.allocator.destroy(entry);
                            _ = list.swapRemove(i);
                            self.total_count -|= 1;
                        } else {
                            i += 1;
                        }
                    }

                    if (list.items.len >= self.config.max_per_host) return PoolError.PoolExhaustedForHost;
                }

                if (self.total_count >= self.config.max_connections) return PoolError.PoolExhausted;
            }

            // Create outside the mutex — DNS, TCP connect (and TLS handshake) may block.
            return self.createConnection(host, port);
        }

        /// Creates a new connection via `Entry.createNew`, then inserts it
        /// into the pool after re-checking limits under the mutex.
        fn createConnection(self: *Self, host: []const u8, port: u16) !*Entry {
            const entry = try Entry.createNew(self.allocator, self.io, host, port, self.context);
            errdefer {
                entry.close();
                self.allocator.free(entry.host);
                self.allocator.destroy(entry);
            }

            var key_buf: [280]u8 = undefined;
            const lookup_key = formatHostKey(&key_buf, host, port) orelse unreachable;

            // Insert into the pool under the mutex.
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

            // Re-check limits — another fiber may have raced and filled the
            // pool while we were connecting.
            if (self.total_count >= self.config.max_connections) {
                return PoolError.PoolExhausted;
            }

            if (self.host_map.getPtr(lookup_key)) |list| {
                if (list.items.len >= self.config.max_per_host) {
                    return PoolError.PoolExhaustedForHost;
                }
                try list.append(self.allocator, entry);
            } else {
                const owned_key = try allocHostKey(self.allocator, host, port);
                var new_list = std.ArrayListUnmanaged(*Entry).empty;
                new_list.append(self.allocator, entry) catch |err| {
                    self.allocator.free(owned_key);
                    return err;
                };
                self.host_map.put(self.allocator, owned_key, new_list) catch |err| {
                    new_list.deinit(self.allocator);
                    self.allocator.free(owned_key);
                    return err;
                };
            }

            self.total_count += 1;
            self.active_count += 1;
            return entry;
        }

        /// Releases a connection back to the pool for reuse.
        pub fn releaseConnection(self: *Self, entry: *Entry) void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            const now = milliTimestamp(self.io);
            entry.release(now);
            self.active_count -|= 1;
            // Opportunistic cleanup on release.
            if (now - self.last_cleanup > self.config.health_check_interval_ms) {
                self.cleanupLocked(now);
                self.last_cleanup = now;
            }
        }

        /// Removes a connection from the pool and frees all its resources.
        /// Use this when a network error occurs instead of releasing.
        pub fn evictConnection(self: *Self, entry: *Entry) void {
            var key_buf: [280]u8 = undefined;
            const key = formatHostKey(&key_buf, entry.host, entry.port) orelse {
                // Fallback: can't format key, just free the entry.
                if (entry.in_use) self.active_count -|= 1;
                self.total_count -|= 1;
                entry.close();
                self.allocator.free(entry.host);
                self.allocator.destroy(entry);
                return;
            };

            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

            if (self.host_map.getPtr(key)) |list| {
                for (list.items, 0..) |c, i| {
                    if (c == entry) {
                        _ = list.swapRemove(i);
                        break;
                    }
                }
                // Remove empty bucket eagerly to avoid stale keys.
                if (list.items.len == 0) {
                    if (self.host_map.fetchRemove(key)) |removed| {
                        self.allocator.free(removed.key);
                        var empty_list = removed.value;
                        empty_list.deinit(self.allocator);
                    }
                }
            }

            if (entry.in_use) self.active_count -|= 1;
            self.total_count -|= 1;

            entry.close();
            self.allocator.free(entry.host);
            self.allocator.destroy(entry);
        }

        /// Removes idle/broken connections that should be evicted.
        pub fn cleanup(self: *Self) void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            self.cleanupLocked(milliTimestamp(self.io));
        }

        fn cleanupLocked(self: *Self, now: i64) void {
            var map_it = self.host_map.iterator();
            while (map_it.next()) |map_entry| {
                const list = map_entry.value_ptr;
                var i: usize = 0;
                while (i < list.items.len) {
                    const entry = list.items[i];
                    if (entry.shouldEvict(self.config.idle_timeout_ms, self.config.max_requests_per_connection, now)) {
                        entry.close();
                        self.allocator.free(entry.host);
                        self.allocator.destroy(entry);
                        _ = list.swapRemove(i);
                        self.total_count -|= 1;
                    } else {
                        i += 1;
                    }
                }
            }
            // Remove empty buckets to prevent unbounded map growth from
            // connections to many distinct hosts over the pool's lifetime.
            // Collect keys into a stack buffer to avoid heap allocation under
            // the mutex. If there are more empty buckets than the buffer can
            // hold, the remaining ones will be cleaned up on the next cycle.
            var empty_buf: [32][]const u8 = undefined;
            var empty_count: usize = 0;
            {
                var remove_it = self.host_map.iterator();
                while (remove_it.next()) |entry| {
                    if (entry.value_ptr.items.len == 0) {
                        if (empty_count < empty_buf.len) {
                            empty_buf[empty_count] = entry.key_ptr.*;
                            empty_count += 1;
                        }
                    }
                }
            }
            for (empty_buf[0..empty_count]) |key| {
                if (self.host_map.fetchRemove(key)) |removed| {
                    var list = removed.value;
                    list.deinit(self.allocator);
                    self.allocator.free(removed.key);
                }
            }
        }

        /// Returns the number of in-use connections.
        pub fn activeCount(self: *Self) usize {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            return self.active_count;
        }

        /// Returns the total number of connections (active + idle).
        pub fn totalCount(self: *Self) usize {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            return self.total_count;
        }

        /// Returns the number of idle connections.
        pub fn idleCount(self: *Self) usize {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            return self.total_count - self.active_count;
        }

        /// Returns the number of connections for a specific host/port pair.
        pub fn hostConnectionCount(self: *Self, host: []const u8, port: u16) usize {
            var key_buf: [280]u8 = undefined;
            const key = formatHostKey(&key_buf, host, port) orelse return 0;

            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            const list = self.host_map.get(key) orelse return 0;
            return list.items.len;
        }

        /// Returns a snapshot of total/active/idle pool counts.
        pub fn stats(self: *Self) PoolStats {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            return .{
                .total = self.total_count,
                .active = self.active_count,
                .idle = self.total_count - self.active_count,
            };
        }
    };
}

// ---------------------------------------------------------------------------
// Public type aliases
// ---------------------------------------------------------------------------

/// HTTP connection pool (plain TCP).
pub const ConnectionPool = GenericPool(Connection, void);

/// HTTPS connection pool (TCP + TLS).
pub const TlsPool = GenericPool(TlsConnection, TlsPoolContext);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "ConnectionPool initialization" {
    const allocator = std.testing.allocator;
    var pool_inst = ConnectionPool.init(allocator, std.testing.io, {});
    defer pool_inst.deinit();

    try std.testing.expectEqual(@as(usize, 0), pool_inst.totalCount());
}

test "pool resolveAddress falls back to hostname lookup" {
    const addr = try resolveAddress(std.testing.io, "localhost", 8080);
    switch (addr) {
        .ip4 => |ip4| try std.testing.expectEqual(@as(u16, 8080), ip4.port),
        .ip6 => |ip6| try std.testing.expectEqual(@as(u16, 8080), ip6.port),
    }
}

test "ConnectionPool config" {
    const allocator = std.testing.allocator;
    var pool_inst = ConnectionPool.initWithConfig(allocator, std.testing.io, .{
        .max_connections = 50,
        .max_per_host = 10,
    }, {});
    defer pool_inst.deinit();

    try std.testing.expectEqual(@as(u32, 50), pool_inst.config.max_connections);
    try std.testing.expectEqual(@as(u32, 10), pool_inst.config.max_per_host);
}

test "Connection health check" {
    const io = std.testing.io;
    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try @import("../net/socket.zig").TcpListener.init(listen_addr, io);
    defer listener.deinit();
    const bound_addr = listener.getLocalAddress();

    const now = milliTimestamp(io);
    var conn = Connection{
        .socket = try Socket.connect(bound_addr, io),
        .host = "localhost",
        .port = 8080,
        .created_at = now,
        .last_used = now,
    };
    defer conn.socket.close();

    try std.testing.expect(conn.isHealthy(60_000, now));

    conn.in_use = true;
    try std.testing.expect(!conn.isHealthy(60_000, now));
}

test "ConnectionPool stats helpers" {
    const allocator = std.testing.allocator;
    var pool_inst = ConnectionPool.init(allocator, std.testing.io, {});
    defer pool_inst.deinit();

    const s = pool_inst.stats();
    try std.testing.expectEqual(@as(usize, 0), s.total);
    try std.testing.expectEqual(@as(usize, 0), s.active);
    try std.testing.expectEqual(@as(usize, 0), s.idle);
    try std.testing.expectEqual(@as(usize, 0), pool_inst.hostConnectionCount("example.com", 443));
}

test "ConnectionPool mutex prevents data races" {
    const allocator = std.testing.allocator;
    var pool_inst = ConnectionPool.init(allocator, std.testing.io, {});
    defer pool_inst.deinit();

    // Verify that mutex-protected methods can be called from the same thread
    // without deadlocking (basic smoke test for the mutex wiring).
    pool_inst.cleanup();
    const s = pool_inst.stats();
    try std.testing.expectEqual(@as(usize, 0), s.total);
}

test "TlsConnection health check basics" {
    const now = milliTimestamp(std.testing.io);
    // We cannot construct a real TlsConnection without a network peer, so
    // test the health-check logic by inspecting flag combinations on a
    // zero-initialized stub (socket/session fields are not touched by the
    // health predicates).
    var entry: TlsConnection = undefined;
    entry.in_use = false;
    entry.broken = false;
    entry.session.connected = true;
    entry.last_used = now;
    entry.requests_made = 0;
    entry.created_at = now;

    // Healthy idle connection.
    try std.testing.expect(entry.isHealthy(60_000, now));

    // In-use connection is not considered healthy (for pool reuse).
    entry.in_use = true;
    try std.testing.expect(!entry.isHealthy(60_000, now));
    entry.in_use = false;

    // Broken connection.
    entry.broken = true;
    try std.testing.expect(!entry.isHealthy(60_000, now));
    entry.broken = false;

    // Disconnected TLS session.
    entry.session.connected = false;
    try std.testing.expect(!entry.isHealthy(60_000, now));
}

test "TlsPool initialization and stats" {
    const allocator = std.testing.allocator;
    var tls_pool = TlsPool.initWithConfig(allocator, std.testing.io, .{}, .{ .verify_ssl = true });
    defer tls_pool.deinit();

    const s = tls_pool.stats();
    try std.testing.expectEqual(@as(usize, 0), s.total);
    try std.testing.expectEqual(@as(usize, 0), s.active);
    try std.testing.expectEqual(@as(usize, 0), s.idle);
    try std.testing.expectEqual(@as(usize, 0), tls_pool.hostConnectionCount("example.com", 443));
}

test "TlsPool cleanup on empty pool" {
    const allocator = std.testing.allocator;
    var tls_pool = TlsPool.initWithConfig(allocator, std.testing.io, .{}, .{ .verify_ssl = true });
    defer tls_pool.deinit();

    // Should not panic on an empty pool.
    tls_pool.cleanup();
    try std.testing.expectEqual(@as(usize, 0), tls_pool.totalCount());
}
