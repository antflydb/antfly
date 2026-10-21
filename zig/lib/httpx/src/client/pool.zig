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

/// One physical socket envelope across TCP, TLS and H2, including connections
/// currently opening. The stable client owner outlives every entry.
pub const ConnectionSlots = struct {
    limit: usize,
    used: std.atomic.Value(usize) = .init(0),
    pub fn acquire(self: *ConnectionSlots) bool {
        var count = self.used.load(.acquire);
        while (count < self.limit) {
            if (self.used.cmpxchgWeak(count, count + 1, .acq_rel, .acquire)) |next| count = next else return true;
        }
        return false;
    }
    pub fn release(self: *ConnectionSlots) void {
        std.debug.assert(self.used.fetchSub(1, .acq_rel) > 0);
    }
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
            self.requests_made +|= 1;
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

    /// Rejected publication must not send a TLS alert during retirement.
    pub fn reject(self: *Connection) void {
        self.socket.abortRequest();
        self.socket.shutdown();
    }

    pub fn close(self: *Connection) void {
        self.socket.close();
    }

    /// Creates a new TCP connection (DNS resolve + TCP connect).
    pub fn createNew(allocator: Allocator, io: Io, host: []const u8, port: u16, _: void) !*Connection {
        return createNewControlled(allocator, io, host, port, {}, null);
    }

    pub fn createNewControlled(allocator: Allocator, io: Io, host: []const u8, port: u16, _: void, control: ?socket_mod.WriteControl) !*Connection {
        const host_owned = try allocator.dupe(u8, host);
        errdefer allocator.free(host_owned);

        var rejected = false;
        var socket = (try Socket.connectHostResolvedControlled(host, port, io, control orelse .{}, &rejected)).socket;
        errdefer socket.close();
        socket.setNoDelay(true) catch {};
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
    ca_file: ?[]const u8 = null,
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
        if (self.in_use) return false;
        if (!self.session.connected) return true;
        return mixin.baseShouldEvict(self, idle_timeout_ms, max_requests, now);
    }

    /// Rejected publication must not send a TLS alert during retirement.
    pub fn reject(self: *TlsConnection) void {
        self.socket.abortRequest();
        self.socket.shutdown();
    }

    pub fn close(self: *TlsConnection) void {
        self.session.deinit();
        self.socket.close();
    }

    /// Creates a new TLS connection (DNS resolve + TCP connect + TLS handshake).
    pub fn createNew(allocator: Allocator, io: Io, host: []const u8, port: u16, ctx: TlsPoolContext) !*TlsConnection {
        return createNewControlled(allocator, io, host, port, ctx, null);
    }

    pub fn createNewControlled(allocator: Allocator, io: Io, host: []const u8, port: u16, ctx: TlsPoolContext, control: ?socket_mod.WriteControl) !*TlsConnection {
        const host_owned = try allocator.dupe(u8, host);
        errdefer allocator.free(host_owned);

        const entry = try allocator.create(TlsConnection);
        errdefer allocator.destroy(entry);

        // Resolve and connect the TCP socket.
        var rejected = false;
        entry.socket = (try Socket.connectHostResolvedControlled(host, port, io, control orelse .{}, &rejected)).socket;
        errdefer entry.socket.close();
        entry.socket.setNoDelay(true) catch {};

        // Initialize the TLS session.  The session stores internal pointers to
        // entry.socket (via SocketIoReader/Writer), so the entry must already
        // live at its final heap address — which it does.
        var tls_cfg = if (ctx.verify_ssl) TlsConfig.init(allocator) else TlsConfig.insecure(allocator);
        tls_cfg.ca_file = ctx.ca_file;
        entry.session = TlsSession.init(tls_cfg, io);
        errdefer entry.session.deinit();

        entry.session.attachSocket(&entry.socket);
        {
            // The entry is still exclusive and unpublished. Only physical
            // TLS transport waits loan execution; cryptography stays runnable.
            // Clear borrowed callbacks before the connection can enter a pool.
            var transport_scope = try entry.session.controlTransport(control);
            defer transport_scope.deinit();
            entry.session.handshake(host) catch |err| {
                try entry.session.checkTransportError();
                return err;
            };
            if (control) |original| original.check() catch |err| {
                // A completed handshake is still unpublished. Prevent TLS
                // teardown from sending close_notify after authority expires.
                entry.socket.abortRequest();
                entry.socket.shutdown();
                return err;
            };
        }

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
    connecting: usize = 0,
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
        connecting: std.StringHashMapUnmanaged(usize) = .{},
        connecting_count: usize = 0,
        slots: ?*ConnectionSlots = null,
        total_count: usize = 0,
        active_count: usize = 0,
        mutex: Io.Mutex = Io.Mutex.init,
        last_cleanup: i64 = 0,
        changed: Io.Condition = .init,
        changed_generation: u64 = 0,
        waiters: usize = 0,
        closed: bool = false,
        /// Release/eviction wakes request admission without increasing limits.
        availability_epoch: std.atomic.Value(u32) = .init(0),

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
                    self.destroyEntry(entry);
                }
                map_entry.value_ptr.deinit(self.allocator);
                self.allocator.free(map_entry.key_ptr.*);
            }
            self.host_map.deinit(self.allocator);
            std.debug.assert(self.connecting_count == 0 and self.waiters == 0);
            self.connecting.deinit(self.allocator);
        }

        /// Format a "host:port" lookup key into a stack buffer.
        fn formatHostKey(buf: []u8, host: []const u8, port: u16) ?[]const u8 {
            return std.fmt.bufPrint(buf, "{s}:{d}", .{ host, port }) catch null;
        }

        /// Allocate a heap-owned "host:port" key for map insertion.
        fn allocHostKey(allocator: Allocator, host: []const u8, port: u16) ![]u8 {
            return std.fmt.allocPrint(allocator, "{s}:{d}", .{ host, port });
        }

        pub fn bindSlots(self: *Self, slots: *ConnectionSlots) void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            if (self.slots) |bound| {
                std.debug.assert(bound == slots);
            } else {
                std.debug.assert(self.total_count == 0 and self.connecting_count == 0);
                self.slots = slots;
            }
        }

        fn destroyEntry(self: *Self, entry: *Entry) void {
            entry.close();
            self.allocator.free(entry.host);
            self.allocator.destroy(entry);
            if (self.slots) |slots| slots.release();
        }

        /// Retire unused sockets to make room for another protocol/endpoint.
        pub fn retireIdle(self: *Self) void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            var it = self.host_map.iterator();
            while (it.next()) |bucket| {
                var i: usize = 0;
                while (i < bucket.value_ptr.items.len) {
                    const entry = bucket.value_ptr.items[i];
                    if (!entry.in_use) {
                        self.destroyEntry(entry);
                        _ = bucket.value_ptr.swapRemove(i);
                        self.total_count -= 1;
                    } else i += 1;
                }
            }
            self.cleanupLocked(milliTimestamp(self.io));
            self.changed_generation +%= 1;
            self.changed.broadcast(self.io);
        }

        pub fn close(self: *Self) void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            self.closed = true;
            self.changed_generation +%= 1;
            self.changed.broadcast(self.io);
        }

        pub fn getConnectionWait(self: *Self, host: []const u8, port: u16, options: @import("admission.zig").Options, max_waiters: u16, max_wait_ms: u32) !*Entry {
            const now = milliTimestamp(self.io);
            const deadline = @min(options.deadline_ms orelse std.math.maxInt(i64), now +| max_wait_ms);
            var queued = false;
            defer if (queued) {
                self.mutex.lockUncancelable(self.io);
                self.waiters -= 1;
                self.mutex.unlock(self.io);
            };
            var setup_control = socket_mod.DeadlineControl{ .io = self.io, .deadline_ms = options.deadline_ms, .original = options.execution_wait orelse .{} };
            while (true) {
                try options.check();
                if (options.isCancelled()) return error.Cancelled;
                const current = milliTimestamp(self.io);
                if (options.deadline_ms) |original| if (current >= original) return error.Timeout;
                if (queued and current >= deadline) return error.Timeout;
                const connection = self.getConnectionControlled(host, port, setup_control.view()) catch |err| switch (err) {
                    error.PoolExhausted, error.PoolExhaustedForHost => {
                        self.mutex.lockUncancelable(self.io);
                        defer self.mutex.unlock(self.io);
                        if (self.closed) return error.ClientClosed;
                        if (!queued) {
                            if (self.waiters >= max_waiters or max_wait_ms == 0) return err;
                            self.waiters += 1;
                            queued = true;
                        }
                        const wake_ns = @as(i128, @min(deadline, current +| 25)) * std.time.ns_per_ms;
                        try options.waitCapacity(self.io, &self.mutex, &self.changed, &self.changed_generation, wake_ns);
                        continue;
                    },
                    else => return err,
                };
                errdefer {
                    if (@hasDecl(Entry, "reject")) connection.reject();
                    self.evictConnection(connection);
                }
                try setup_control.view().check();
                if (options.isCancelled()) return error.Cancelled;
                return connection;
            }
        }

        /// Gets an idle connection for `(host, port)`, or creates a new one.
        ///
        /// The returned pointer is heap-allocated and marked in-use.
        /// Callers must call `releaseConnection` or `evictConnection` when done.
        pub fn getConnection(self: *Self, host: []const u8, port: u16) !*Entry {
            return self.getConnectionControlled(host, port, null);
        }

        fn getConnectionControlled(self: *Self, host: []const u8, port: u16, control: ?socket_mod.WriteControl) !*Entry {
            var key_buf: [280]u8 = undefined;
            const key = formatHostKey(&key_buf, host, port) orelse return error.InvalidUri;

            {
                self.mutex.lockUncancelable(self.io);
                defer self.mutex.unlock(self.io);

                if (self.closed) return error.ClientClosed;
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
                            self.destroyEntry(entry);
                            _ = list.swapRemove(i);
                            self.total_count -|= 1;
                        } else {
                            i += 1;
                        }
                    }

                    if (list.items.len + (self.connecting.get(key) orelse 0) >= self.config.max_per_host) return PoolError.PoolExhaustedForHost;
                }

                if ((self.connecting.get(key) orelse 0) >= self.config.max_per_host) return PoolError.PoolExhaustedForHost;
                if (self.total_count + self.connecting_count >= self.config.max_connections) {
                    var idle_it = self.host_map.iterator();
                    outer: while (idle_it.next()) |bucket| {
                        for (bucket.value_ptr.items, 0..) |entry, i| {
                            if (!entry.in_use) {
                                self.destroyEntry(entry);
                                _ = bucket.value_ptr.swapRemove(i);
                                self.total_count -= 1;
                                break :outer;
                            }
                        }
                    }
                }
                if (self.total_count + self.connecting_count >= self.config.max_connections) return PoolError.PoolExhausted;
                const reservation = try self.connecting.getOrPut(self.allocator, key);
                if (!reservation.found_existing) {
                    reservation.key_ptr.* = self.allocator.dupe(u8, key) catch |err| {
                        _ = self.connecting.remove(key);
                        return err;
                    };
                    reservation.value_ptr.* = 0;
                }
                if (self.slots) |slots| if (!slots.acquire()) {
                    if (reservation.value_ptr.* == 0) {
                        const removed = self.connecting.fetchRemove(key).?;
                        self.allocator.free(removed.key);
                    }
                    return PoolError.PoolExhausted;
                };
                reservation.value_ptr.* += 1;
                self.connecting_count += 1;
            }

            // Reserve BEFORE DNS/connect so concurrent creators cannot exceed socket limits.
            defer {
                self.mutex.lockUncancelable(self.io);
                defer self.mutex.unlock(self.io);
                self.connecting_count -= 1;
                self.changed_generation +%= 1;
                self.changed.broadcast(self.io);
                const count = self.connecting.getPtr(key).?;
                count.* -= 1;
                if (count.* == 0) {
                    const removed = self.connecting.fetchRemove(key).?;
                    self.allocator.free(removed.key);
                }
            }
            return self.createConnection(host, port, control) catch |err| {
                if (self.slots) |slots| slots.release();
                return err;
            };
        }

        /// Creates a new connection via `Entry.createNew`, then inserts it
        /// into the pool after re-checking limits under the mutex.
        fn createConnection(self: *Self, host: []const u8, port: u16, control: ?socket_mod.WriteControl) !*Entry {
            const entry = if (@hasDecl(Entry, "createNewControlled"))
                try Entry.createNewControlled(self.allocator, self.io, host, port, self.context, control)
            else
                try Entry.createNew(self.allocator, self.io, host, port, self.context);
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
            defer self.notifyAvailability();
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            const now = milliTimestamp(self.io);
            entry.release(now);
            self.changed_generation +%= 1;
            self.changed.broadcast(self.io);
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
            defer self.notifyAvailability();
            var key_buf: [280]u8 = undefined;
            const key = formatHostKey(&key_buf, entry.host, entry.port) orelse {
                // Fallback: can't format key, just free the entry.
                if (entry.in_use) self.active_count -|= 1;
                self.total_count -|= 1;
                self.destroyEntry(entry);
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
            self.changed_generation +%= 1;
            self.changed.broadcast(self.io);

            self.destroyEntry(entry);
        }

        /// Removes idle/broken connections that should be evicted.
        pub fn cleanup(self: *Self) void {
            defer self.notifyAvailability();
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            self.cleanupLocked(milliTimestamp(self.io));
        }

        fn notifyAvailability(self: *Self) void {
            _ = self.availability_epoch.fetchAdd(1, .release);
            Io.futexWake(self.io, u32, &self.availability_epoch.raw, std.math.maxInt(u32));
        }

        fn cleanupLocked(self: *Self, now: i64) void {
            var map_it = self.host_map.iterator();
            while (map_it.next()) |map_entry| {
                const list = map_entry.value_ptr;
                var i: usize = 0;
                while (i < list.items.len) {
                    const entry = list.items[i];
                    if (entry.shouldEvict(self.config.idle_timeout_ms, self.config.max_requests_per_connection, now)) {
                        self.destroyEntry(entry);
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
                .connecting = self.connecting_count,
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

const PoolTestContext = struct {
    io: Io,
    entered: Io.Event = .unset,
    resume_event: Io.Event = .unset,
    block: bool = false,
    fail: bool = false,
    calls: std.atomic.Value(usize) = .init(0),
    cancel_after_create: ?*std.atomic.Value(bool) = null,
};
const PoolTestEntry = struct {
    last_used: i64 = 0,
    host: []const u8,
    port: u16,
    in_use: bool = true,
    requests_made: u32 = 0,
    const mixin = ConnectionMixin(@This());
    pub const acquire = mixin.acquire;
    pub fn release(self: *@This(), _: i64) void {
        self.in_use = false;
        self.requests_made += 1;
    }
    pub fn isHealthy(self: *const @This(), _: i64, _: i64) bool {
        return !self.in_use;
    }
    pub fn shouldEvict(_: *const @This(), _: i64, _: u32, _: i64) bool {
        return false;
    }
    pub fn close(_: *@This()) void {}
    pub fn createNew(alloc: Allocator, _: Io, host: []const u8, port: u16, context: *PoolTestContext) !*@This() {
        _ = context.calls.fetchAdd(1, .acq_rel);
        context.entered.set(context.io);
        if (context.block) try context.resume_event.waitTimeout(context.io, .{ .duration = .{ .clock = .awake, .raw = .fromSeconds(3) } });
        if (context.fail) return error.TestConnectFailed;
        const owned_host = try alloc.dupe(u8, host);
        errdefer alloc.free(owned_host);
        const entry = try alloc.create(@This());
        entry.* = .{ .host = owned_host, .port = port };
        if (context.cancel_after_create) |signal| signal.store(true, .release);
        return entry;
    }
};
const TestPool = GenericPool(PoolTestEntry, *PoolTestContext);

test "outbound pool reserves pending connections before connect and reuses release" {
    const io = std.testing.io;
    var context: PoolTestContext = .{ .io = io, .block = true };
    var slots: ConnectionSlots = .{ .limit = 1 };
    var pool_inst = TestPool.initWithConfig(std.testing.allocator, io, .{ .max_connections = 1, .max_per_host = 1 }, &context);
    defer pool_inst.deinit();
    pool_inst.bindSlots(&slots);
    const Task = struct {
        fn run(pool: *TestPool) !*PoolTestEntry {
            return pool.getConnection("a", 80);
        }
    };
    var running = try io.concurrent(Task.run, .{&pool_inst});
    defer if (running.cancel(io)) |_| {} else |_| {};
    try context.entered.waitTimeout(io, .{ .duration = .{ .clock = .awake, .raw = .fromSeconds(3) } });
    try std.testing.expectEqual(@as(usize, 1), pool_inst.stats().connecting);
    try std.testing.expectEqual(@as(usize, 1), slots.used.load(.acquire));
    try std.testing.expectError(error.PoolExhaustedForHost, pool_inst.getConnection("a", 80));
    try std.testing.expectError(error.PoolExhausted, pool_inst.getConnection("b", 80));
    try std.testing.expectEqual(@as(usize, 1), context.calls.load(.acquire));
    context.resume_event.set(io);
    const first = try running.await(io);
    pool_inst.releaseConnection(first);
    const second = try pool_inst.getConnection("a", 80);
    try std.testing.expect(first == second);
    pool_inst.releaseConnection(second);
    pool_inst.retireIdle();
    try std.testing.expectEqual(@as(usize, 0), slots.used.load(.acquire));
    pool_inst.close();
    try std.testing.expectError(error.ClientClosed, pool_inst.getConnection("a", 80));
}

test "outbound pool shared socket ceiling spans transports and failed creates reclaim reservations" {
    const io = std.testing.io;
    var context: PoolTestContext = .{ .io = io };
    var slots: ConnectionSlots = .{ .limit = 1 };
    var a = TestPool.init(std.testing.allocator, io, &context);
    defer a.deinit();
    var b = TestPool.init(std.testing.allocator, io, &context);
    defer b.deinit();
    a.bindSlots(&slots);
    b.bindSlots(&slots);
    const first = try a.getConnection("a", 80);
    try std.testing.expectError(error.PoolExhausted, b.getConnection("b", 443));
    a.releaseConnection(first);
    a.retireIdle();
    context.fail = true;
    try std.testing.expectError(error.TestConnectFailed, b.getConnection("b", 443));
    try std.testing.expectEqual(@as(usize, 0), b.stats().connecting);
    try std.testing.expectEqual(@as(usize, 0), slots.used.load(.acquire));
    context.fail = false;
    const second = try b.getConnection("b", 443);
    b.releaseConnection(second);
    b.retireIdle();
    try std.testing.expectEqual(@as(usize, 0), slots.used.load(.acquire));
}

test "outbound pool wait preserves original deadline without starting a connection" {
    const io = std.testing.io;
    var context: PoolTestContext = .{ .io = io };
    var pool_inst = TestPool.initWithConfig(std.testing.allocator, io, .{ .max_connections = 1, .max_per_host = 1 }, &context);
    defer pool_inst.deinit();
    const held = try pool_inst.getConnection("a", 80);
    defer pool_inst.releaseConnection(held);
    try std.testing.expectError(error.Timeout, pool_inst.getConnectionWait("a", 80, .{ .deadline_ms = milliTimestamp(io) + 10 }, 1, 1000));
    try std.testing.expectEqual(@as(usize, 1), context.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), pool_inst.waiters);
}

test "outbound pool capacity waits reject reentry before acquiring a released connection" {
    const Case = enum { success, begin_denied, end_denied, canceled };
    const State = struct {
        pool: *TestPool,
        held: *PoolTestEntry,
        case: Case,
        released: bool = false,
        canceled: bool = false,
        parked: bool = false,
        begins: usize = 0,
        ends: usize = 0,
        fn check(raw: ?*anyopaque) @import("../net/socket.zig").WriteControl.Error!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.canceled) return error.Canceled;
        }
        fn begin(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(self.pool.mutex.tryLock());
            self.pool.mutex.unlock(self.pool.io);
            try std.testing.expectEqual(@as(usize, 1), self.pool.waiters);
            self.begins += 1;
            if (self.case == .begin_denied) return error.SchedulerClosed;
            self.parked = true;
            self.pool.releaseConnection(self.held);
            self.released = true;
        }
        fn end(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(self.pool.mutex.tryLock());
            self.pool.mutex.unlock(self.pool.io);
            try std.testing.expect(self.parked);
            self.parked = false;
            self.ends += 1;
            if (self.case == .end_denied) return error.DeadlineExceeded;
            if (self.case == .canceled) self.canceled = true;
        }
    };
    for (std.enums.values(Case)) |case| {
        const io = std.testing.io;
        var context: PoolTestContext = .{ .io = io };
        var slots: ConnectionSlots = .{ .limit = 1 };
        var pool_inst = TestPool.initWithConfig(std.testing.allocator, io, .{ .max_connections = 1, .max_per_host = 1 }, &context);
        defer pool_inst.deinit();
        pool_inst.bindSlots(&slots);
        const held = try pool_inst.getConnection("a", 80);
        var state: State = .{ .pool = &pool_inst, .held = held, .case = case };
        defer if (!state.released) pool_inst.releaseConnection(held);
        const result = pool_inst.getConnectionWait("a", 80, .{ .execution_wait = .{ .ptr = &state, .check_fn = State.check, .wait_begin_fn = State.begin, .wait_end_fn = State.end } }, 1, 1000);
        switch (case) {
            .success => {
                const acquired = try result;
                try std.testing.expect(acquired == held);
                pool_inst.releaseConnection(acquired);
            },
            .begin_denied => try std.testing.expectError(error.SchedulerClosed, result),
            .end_denied => try std.testing.expectError(error.DeadlineExceeded, result),
            .canceled => try std.testing.expectError(error.Canceled, result),
        }
        try std.testing.expectEqual(@as(usize, 1), context.calls.load(.acquire));
        try std.testing.expectEqual(@as(usize, 1), state.begins);
        try std.testing.expectEqual(@as(usize, if (case == .begin_denied) 0 else 1), state.ends);
        try std.testing.expect(!state.parked);
        try std.testing.expectEqual(@as(usize, 0), pool_inst.waiters);
        if (!state.released) {
            pool_inst.releaseConnection(held);
            state.released = true;
        }
        pool_inst.retireIdle();
        try std.testing.expectEqual(@as(usize, 0), slots.used.load(.acquire));
    }
}

test "outbound pool acquisition retires a newly created connection after authority rejection" {
    const Check = struct {
        fn check(raw: ?*anyopaque) @import("../net/socket.zig").WriteControl.Error!void {
            const signal: *std.atomic.Value(bool) = @ptrCast(@alignCast(raw.?));
            if (signal.load(.acquire)) return error.Canceled;
        }
    };
    var signal = std.atomic.Value(bool).init(false);
    const io = std.testing.io;
    var context: PoolTestContext = .{ .io = io, .cancel_after_create = &signal };
    var slots: ConnectionSlots = .{ .limit = 1 };
    var pool_inst = TestPool.initWithConfig(std.testing.allocator, io, .{ .max_connections = 1 }, &context);
    defer pool_inst.deinit();
    pool_inst.bindSlots(&slots);
    try std.testing.expectError(error.Canceled, pool_inst.getConnectionWait("a", 80, .{ .execution_wait = .{ .ptr = &signal, .check_fn = Check.check } }, 1, 1000));
    try std.testing.expectEqual(@as(usize, 1), context.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), pool_inst.totalCount());
    try std.testing.expectEqual(@as(usize, 0), pool_inst.stats().connecting);
    try std.testing.expectEqual(@as(usize, 0), pool_inst.waiters);
    try std.testing.expectEqual(@as(usize, 0), slots.used.load(.acquire));
}

test "outbound pool denied connect resume retires socket and reclaims physical capacity" {
    const State = struct {
        begins: usize = 0,
        ends: usize = 0,
        fn begin(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.begins += 1;
        }
        fn end(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.ends += 1;
            return error.ResumeDenied;
        }
    };
    var lane = Io.Threaded.init(std.testing.allocator, .{});
    defer lane.deinit();
    const io = lane.io();
    var listener = try socket_mod.TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
    defer listener.deinit();
    var slots = ConnectionSlots{ .limit = 1 };
    var pool_inst = ConnectionPool.init(std.testing.allocator, io, {});
    defer pool_inst.deinit();
    pool_inst.bindSlots(&slots);
    var state = State{};
    const options = @import("admission.zig").Options{ .execution_wait = .{ .ptr = &state, .wait_begin_fn = State.begin, .wait_end_fn = State.end } };
    const port = listener.getLocalAddress().getPort();
    try std.testing.expectError(error.ResumeDenied, pool_inst.getConnectionWait("127.0.0.1", port, options, 1, 1000));
    try std.testing.expectEqual(@as(usize, 1), state.begins);
    try std.testing.expectEqual(@as(usize, 1), state.ends);
    try std.testing.expectEqual(@as(usize, 0), slots.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), pool_inst.stats().connecting);
    try std.testing.expectEqual(@as(usize, 0), pool_inst.stats().total);
    var rejected_peer = try listener.accept();
    defer rejected_peer.socket.close();
    rejected_peer.socket.enableNativeTimeouts();
    try rejected_peer.socket.setRecvTimeout(1000);
    var byte: [1]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 0), try rejected_peer.socket.recv(&byte));
    // The same sole capacity slot remains usable after physical retirement.
    const next = try pool_inst.getConnection("127.0.0.1", port);
    defer pool_inst.releaseConnection(next);
    var accepted = try listener.accept();
    defer accepted.socket.close();
    try next.socket.sendAll("x");
    try std.testing.expectEqual(@as(usize, 1), try accepted.socket.recv(&byte));
    try std.testing.expectEqualStrings("x", &byte);
    try std.testing.expectEqual(@as(usize, 1), state.ends);
}

test "outbound pool rejects cancellation arriving during construction without wait callbacks" {
    const Check = struct {
        fn canceled(raw: *const anyopaque) bool {
            const signal: *const std.atomic.Value(bool) = @ptrCast(@alignCast(raw));
            return signal.load(.acquire);
        }
    };
    var signal = std.atomic.Value(bool).init(false);
    const io = std.testing.io;
    var context = PoolTestContext{ .io = io, .cancel_after_create = &signal };
    var slots = ConnectionSlots{ .limit = 1 };
    var pool_inst = TestPool.init(std.testing.allocator, io, &context);
    defer pool_inst.deinit();
    pool_inst.bindSlots(&slots);
    try std.testing.expectError(error.Cancelled, pool_inst.getConnectionWait("a", 80, .{ .context = &signal, .cancelled = Check.canceled }, 1, 1000));
    try std.testing.expectEqual(@as(usize, 1), context.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), pool_inst.totalCount());
    try std.testing.expectEqual(@as(usize, 0), pool_inst.stats().connecting);
    try std.testing.expectEqual(@as(usize, 0), slots.used.load(.acquire));
}

test "H1 pooled connections disable Nagle across reuse" {
    if (@import("builtin").os.tag != .linux and @import("builtin").os.tag != .macos) return error.SkipZigTest;
    const io = std.testing.io;
    var listener = try socket_mod.TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
    defer listener.deinit();
    var pool = ConnectionPool.init(std.testing.allocator, io, {});
    defer pool.deinit();
    const first = try pool.getConnection("127.0.0.1", listener.getLocalAddress().getPort());
    var value: c_int = 0;
    var size: std.posix.socklen_t = @sizeOf(c_int);
    try std.testing.expectEqual(std.posix.E.SUCCESS, std.posix.errno(std.posix.system.getsockopt(first.socket.handle, std.posix.IPPROTO.TCP, std.posix.TCP.NODELAY, @ptrCast(&value), &size)));
    try std.testing.expect(value != 0);
    pool.releaseConnection(first);
    const reused = try pool.getConnection("127.0.0.1", listener.getLocalAddress().getPort());
    defer pool.releaseConnection(reused);
    try std.testing.expectEqual(first, reused);
}
