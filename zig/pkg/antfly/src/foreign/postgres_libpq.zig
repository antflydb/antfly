// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const platform_sync = @import("antfly_platform").sync;
const platform_time = @import("antfly_platform").time;
const builtin = @import("builtin");
const filter = @import("filter.zig");
const foreign_source = @import("source.zig");
const postgres_source = @import("postgres_source.zig");
const sql = @import("sql.zig");

const Allocator = std.mem.Allocator;
const Mutex = std.atomic.Mutex;
const max_connections_per_dsn: usize = 8;
const max_connection_pools: usize = 32;
const max_total_connections: usize = 128;
const connection_pool_idle_ttl_ns: u64 = 5 * std.time.ns_per_min;
const max_pool_reclaims_per_acquire: usize = 1;
const max_column_cache_entries: usize = 1_024;
const column_cache_ttl_ns: u64 = std.time.ns_per_min;
const failed_cutover_cleanup_timeout_ns: u64 = 5 * std.time.ns_per_s;
const cancellation_poll_interval_ns: u64 = 25 * std.time.ns_per_ms;
const exact_cutover_identity_domain = "antfly/postgres-exact-cutover-identity/v1";
const supports_waitable_pool = builtin.os.tag != .freestanding and
    builtin.link_libc and
    @hasDecl(std.c, "pthread_cond_wait") and
    @hasDecl(std.c, "pthread_cond_timedwait");

const PthreadCondAttr = extern struct {
    value: c_int = 0,
};

const pthread_ext = struct {
    extern "c" fn pthread_cond_init(cond: *std.c.pthread_cond_t, attr: ?*const PthreadCondAttr) c_int;
    extern "c" fn pthread_condattr_init(attr: *PthreadCondAttr) c_int;
    extern "c" fn pthread_condattr_destroy(attr: *PthreadCondAttr) c_int;
    extern "c" fn pthread_condattr_setclock(attr: *PthreadCondAttr, clock_id: std.c.clockid_t) c_int;
    extern "c" fn pthread_cond_timedwait_relative_np(
        cond: *std.c.pthread_cond_t,
        mutex: *std.c.pthread_mutex_t,
        relative: *const std.c.timespec,
    ) c_int;
};

const uses_monotonic_condattr = builtin.os.tag == .linux;
const uses_relative_condwait = switch (builtin.os.tag) {
    .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => true,
    else => false,
};

fn exactCutoverProviderIdentity(
    system_id: []const u8,
    database: []const u8,
    database_oid: []const u8,
) foreign_source.ExactCutoverIntent.ProviderIdentity {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(exact_cutover_identity_domain);
    hashIdentityField(&hasher, system_id);
    hashIdentityField(&hasher, database);
    hashIdentityField(&hasher, database_oid);
    var identity: foreign_source.ExactCutoverIntent.ProviderIdentity = undefined;
    hasher.final(&identity);
    return identity;
}

fn hashIdentityField(
    hasher: *std.crypto.hash.sha2.Sha256,
    value: []const u8,
) void {
    var encoded_len: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &encoded_len, @intCast(value.len), .big);
    hasher.update(&encoded_len);
    hasher.update(value);
}

test "postgres exact cutover identity binds cluster database and database incarnation" {
    const baseline = exactCutoverProviderIdentity("734912783", "app", "16384");
    const same = exactCutoverProviderIdentity("734912783", "app", "16384");
    const other_cluster = exactCutoverProviderIdentity("734912784", "app", "16384");
    const other_database = exactCutoverProviderIdentity("734912783", "other", "16384");
    const recreated_database = exactCutoverProviderIdentity("734912783", "app", "24576");
    try std.testing.expectEqualSlices(u8, &baseline, &same);
    try std.testing.expect(!std.mem.eql(u8, &baseline, &other_cluster));
    try std.testing.expect(!std.mem.eql(u8, &baseline, &other_database));
    try std.testing.expect(!std.mem.eql(u8, &baseline, &recreated_database));
}

/// An epoch-based condition avoids missed wakeups between observing pool state
/// and sleeping. Native timed waits use a monotonic clock (Linux) or a relative
/// timeout (Darwin), so wall-clock corrections cannot extend query deadlines.
const PoolAvailability = if (supports_waitable_pool)
    struct {
        const State = struct {
            mutex: std.c.pthread_mutex_t = std.c.PTHREAD_MUTEX_INITIALIZER,
            cond: std.c.pthread_cond_t = undefined,
            epoch: std.atomic.Value(u64) = .init(0),
            waiters: usize = 0,
        };

        alloc: Allocator,
        state: *State,

        fn init(alloc: Allocator) !@This() {
            const state = try alloc.create(State);
            errdefer alloc.destroy(state);
            state.* = .{};
            if (uses_monotonic_condattr) {
                var attr = PthreadCondAttr{};
                if (pthread_ext.pthread_condattr_init(&attr) != 0)
                    return error.SystemResources;
                defer if (pthread_ext.pthread_condattr_destroy(&attr) != 0) unreachable;
                if (pthread_ext.pthread_condattr_setclock(&attr, .MONOTONIC) != 0)
                    return error.SystemResources;
                if (pthread_ext.pthread_cond_init(&state.cond, &attr) != 0)
                    return error.SystemResources;
            } else {
                if (pthread_ext.pthread_cond_init(&state.cond, null) != 0)
                    return error.SystemResources;
            }
            return .{
                .alloc = alloc,
                .state = state,
            };
        }

        fn snapshot(self: *@This()) u64 {
            return self.state.epoch.load(.acquire);
        }

        fn waitForChange(self: *@This(), observed: u64, execution_deadline_ns: ?u64) !void {
            const state = self.state;
            if (std.c.pthread_mutex_lock(&state.mutex) != .SUCCESS) unreachable;
            defer if (std.c.pthread_mutex_unlock(&state.mutex) != .SUCCESS) unreachable;
            if (state.epoch.load(.acquire) != observed) return;
            state.waiters += 1;
            defer state.waiters -= 1;

            if (execution_deadline_ns) |deadline_ns| {
                const now_ns = platform_time.monotonicNs();
                if (now_ns >= deadline_ns) return error.Timeout;
                const wait_ns = deadline_ns - now_ns;
                const result = if (uses_monotonic_condattr) blk: {
                    const abstime = monotonicDeadlineTimespec(deadline_ns);
                    break :blk std.c.pthread_cond_timedwait(
                        &state.cond,
                        &state.mutex,
                        &abstime,
                    );
                } else if (uses_relative_condwait) blk: {
                    const relative = durationTimespec(wait_ns);
                    break :blk @as(std.c.E, @fromBackingInt(@intCast(pthread_ext.pthread_cond_timedwait_relative_np(
                        &state.cond,
                        &state.mutex,
                        &relative,
                    ))));
                } else blk: {
                    // The remaining supported pthread targets do not expose a
                    // portable monotonic condition clock. Use a short relative
                    // sleep rather than make deadline correctness depend on
                    // CLOCK_REALTIME.
                    if (std.c.pthread_mutex_unlock(&state.mutex) != .SUCCESS) unreachable;
                    platform_time.sleepNs(@min(wait_ns, std.time.ns_per_ms));
                    if (std.c.pthread_mutex_lock(&state.mutex) != .SUCCESS) unreachable;
                    break :blk std.c.E.TIMEDOUT;
                };
                if (result != .SUCCESS and result != .TIMEDOUT) unreachable;
                try ensureDeadline(deadline_ns);
                return;
            }

            while (state.epoch.load(.acquire) == observed) {
                if (std.c.pthread_cond_wait(&state.cond, &state.mutex) != .SUCCESS) unreachable;
            }
        }

        fn advance(self: *@This()) void {
            const state = self.state;
            if (std.c.pthread_mutex_lock(&state.mutex) != .SUCCESS) unreachable;
            _ = state.epoch.fetchAdd(1, .release);
            if (state.waiters > 0 and std.c.pthread_cond_signal(&state.cond) != .SUCCESS) unreachable;
            if (std.c.pthread_mutex_unlock(&state.mutex) != .SUCCESS) unreachable;
        }

        fn advanceAll(self: *@This()) void {
            const state = self.state;
            if (std.c.pthread_mutex_lock(&state.mutex) != .SUCCESS) unreachable;
            _ = state.epoch.fetchAdd(1, .release);
            if (state.waiters > 0 and std.c.pthread_cond_broadcast(&state.cond) != .SUCCESS) unreachable;
            if (std.c.pthread_mutex_unlock(&state.mutex) != .SUCCESS) unreachable;
        }

        pub fn deinit(self: *@This()) void {
            const state = self.state;
            if (std.c.pthread_cond_destroy(&state.cond) != .SUCCESS) unreachable;
            if (std.c.pthread_mutex_destroy(&state.mutex) != .SUCCESS) unreachable;
            self.alloc.destroy(state);
            self.* = undefined;
        }
    }
else
    struct {
        epoch: std.atomic.Value(u64) = .init(0),

        fn init(_: Allocator) !@This() {
            return .{};
        }

        fn snapshot(self: *@This()) u64 {
            return self.epoch.load(.acquire);
        }

        fn waitForChange(self: *@This(), observed: u64, execution_deadline_ns: ?u64) !void {
            if (self.epoch.load(.acquire) != observed) return;
            if (execution_deadline_ns) |deadline_ns| {
                const now_ns = platform_time.monotonicNs();
                if (now_ns >= deadline_ns) return error.Timeout;
                platform_time.sleepNs(@min(deadline_ns - now_ns, std.time.ns_per_ms));
            } else {
                platform_time.sleepNs(std.time.ns_per_ms);
            }
        }

        fn advance(self: *@This()) void {
            _ = self.epoch.fetchAdd(1, .release);
        }

        fn advanceAll(self: *@This()) void {
            _ = self.epoch.fetchAdd(1, .release);
        }

        pub fn deinit(self: *@This()) void {
            self.* = undefined;
        }
    };

const PGconn = opaque {};
const PGresult = opaque {};

const ConnStatusType = c_uint;
const ExecStatusType = c_uint;
const Oid = c_uint;

const CONNECTION_OK: ConnStatusType = 0;
const PGRES_COMMAND_OK: ExecStatusType = 1;
const PGRES_TUPLES_OK: ExecStatusType = 2;
const PGRES_FATAL_ERROR: ExecStatusType = 7;
const PGRES_SINGLE_TUPLE: ExecStatusType = 9;
const replication_wire_chunk_bytes: usize = 32 * 1024;
const replication_poll_bytes: usize = 32 * 1024 * 1024;
const replication_poll_messages: usize = 65536;
const PG_DIAG_SQLSTATE: c_int = 'C';

const FnPQconnectdb = *const fn ([*:0]const u8) callconv(.c) ?*PGconn;
const FnPQconnectStart = *const fn ([*:0]const u8) callconv(.c) ?*PGconn;
const FnPQconnectPoll = *const fn (?*PGconn) callconv(.c) c_uint;
const FnPQexec = *const fn (?*PGconn, [*:0]const u8) callconv(.c) ?*PGresult;
const FnPQsendQuery = *const fn (?*PGconn, [*:0]const u8) callconv(.c) c_int;
const FnPQstatus = *const fn (?*PGconn) callconv(.c) ConnStatusType;
const ConninfoOption = extern struct {
    keyword: ?[*:0]const u8,
    envvar: ?[*:0]const u8,
    compiled: ?[*:0]const u8,
    value: ?[*:0]const u8,
    label: ?[*:0]const u8,
    display: ?[*:0]const u8,
    display_size: c_int,
};
const FnPQconninfoParse = *const fn ([*:0]const u8, ?*?[*:0]u8) callconv(.c) ?[*]ConninfoOption;
const FnPQconninfoFree = *const fn ([*]ConninfoOption) callconv(.c) void;
const FnPQconnectionUsedPassword = *const fn (?*PGconn) callconv(.c) c_int;
const FnPQerrorMessage = *const fn (?*PGconn) callconv(.c) [*:0]const u8;
const FnPQfinish = *const fn (?*PGconn) callconv(.c) void;
const FnPQexecParams = *const fn (?*PGconn, [*:0]const u8, c_int, ?[*]const Oid, ?[*]const ?[*:0]const u8, ?[*]const c_int, ?[*]const c_int, c_int) callconv(.c) ?*PGresult;
const FnPQsendQueryParams = *const fn (?*PGconn, [*:0]const u8, c_int, ?[*]const Oid, ?[*]const ?[*:0]const u8, ?[*]const c_int, ?[*]const c_int, c_int) callconv(.c) c_int;
const FnPQsetSingleRowMode = *const fn (?*PGconn) callconv(.c) c_int;
const FnPQsetnonblocking = *const fn (?*PGconn, c_int) callconv(.c) c_int;
const FnPQflush = *const fn (?*PGconn) callconv(.c) c_int;
const FnPQconsumeInput = *const fn (?*PGconn) callconv(.c) c_int;
const FnPQisBusy = *const fn (?*PGconn) callconv(.c) c_int;
const FnPQgetResult = *const fn (?*PGconn) callconv(.c) ?*PGresult;
const FnPQsocket = *const fn (?*PGconn) callconv(.c) c_int;
const FnPQresultStatus = *const fn (?*PGresult) callconv(.c) ExecStatusType;
const FnPQresultErrorMessage = *const fn (?*PGresult) callconv(.c) [*:0]const u8;
const FnPQresultErrorField = *const fn (?*PGresult, c_int) callconv(.c) ?[*:0]const u8;
const FnPQntuples = *const fn (?*PGresult) callconv(.c) c_int;
const FnPQnfields = *const fn (?*PGresult) callconv(.c) c_int;
const FnPQfname = *const fn (?*PGresult, c_int) callconv(.c) ?[*:0]const u8;
const FnPQftype = *const fn (?*PGresult, c_int) callconv(.c) Oid;
const FnPQgetisnull = *const fn (?*PGresult, c_int, c_int) callconv(.c) c_int;
const FnPQgetlength = *const fn (?*PGresult, c_int, c_int) callconv(.c) c_int;
const FnPQgetvalue = *const fn (?*PGresult, c_int, c_int) callconv(.c) [*]const u8;
const FnPQclear = *const fn (?*PGresult) callconv(.c) void;

const TestLibpqStubs = struct {
    fn unexpected() noreturn {
        @panic("permit-only test unexpectedly called libpq");
    }

    fn connectdb(_: [*:0]const u8) callconv(.c) ?*PGconn {
        unexpected();
    }
    fn connectStart(_: [*:0]const u8) callconv(.c) ?*PGconn {
        unexpected();
    }
    fn connectPoll(_: ?*PGconn) callconv(.c) c_uint {
        unexpected();
    }
    fn exec(_: ?*PGconn, _: [*:0]const u8) callconv(.c) ?*PGresult {
        unexpected();
    }
    fn sendQuery(_: ?*PGconn, _: [*:0]const u8) callconv(.c) c_int {
        unexpected();
    }
    fn status(_: ?*PGconn) callconv(.c) ConnStatusType {
        unexpected();
    }
    fn errorMessage(_: ?*PGconn) callconv(.c) [*:0]const u8 {
        unexpected();
    }
    fn finish(_: ?*PGconn) callconv(.c) void {}
    fn execParams(
        _: ?*PGconn,
        _: [*:0]const u8,
        _: c_int,
        _: ?[*]const Oid,
        _: ?[*]const ?[*:0]const u8,
        _: ?[*]const c_int,
        _: ?[*]const c_int,
        _: c_int,
    ) callconv(.c) ?*PGresult {
        unexpected();
    }
    fn sendQueryParams(
        _: ?*PGconn,
        _: [*:0]const u8,
        _: c_int,
        _: ?[*]const Oid,
        _: ?[*]const ?[*:0]const u8,
        _: ?[*]const c_int,
        _: ?[*]const c_int,
        _: c_int,
    ) callconv(.c) c_int {
        unexpected();
    }
    fn setnonblocking(_: ?*PGconn, _: c_int) callconv(.c) c_int {
        unexpected();
    }
    fn flush(_: ?*PGconn) callconv(.c) c_int {
        unexpected();
    }
    fn consumeInput(_: ?*PGconn) callconv(.c) c_int {
        unexpected();
    }
    fn isBusy(_: ?*PGconn) callconv(.c) c_int {
        unexpected();
    }
    fn getResult(_: ?*PGconn) callconv(.c) ?*PGresult {
        unexpected();
    }
    fn socket(_: ?*PGconn) callconv(.c) c_int {
        unexpected();
    }
    fn resultStatus(_: ?*PGresult) callconv(.c) ExecStatusType {
        unexpected();
    }
    fn resultErrorMessage(_: ?*PGresult) callconv(.c) [*:0]const u8 {
        unexpected();
    }
    fn resultErrorField(_: ?*PGresult, _: c_int) callconv(.c) ?[*:0]const u8 {
        unexpected();
    }
    fn ntuples(_: ?*PGresult) callconv(.c) c_int {
        unexpected();
    }
    fn nfields(_: ?*PGresult) callconv(.c) c_int {
        unexpected();
    }
    fn fname(_: ?*PGresult, _: c_int) callconv(.c) ?[*:0]const u8 {
        unexpected();
    }
    fn ftype(_: ?*PGresult, _: c_int) callconv(.c) Oid {
        unexpected();
    }
    fn getisnull(_: ?*PGresult, _: c_int, _: c_int) callconv(.c) c_int {
        unexpected();
    }
    fn getlength(_: ?*PGresult, _: c_int, _: c_int) callconv(.c) c_int {
        unexpected();
    }
    fn getvalue(_: ?*PGresult, _: c_int, _: c_int) callconv(.c) [*]const u8 {
        unexpected();
    }
    fn clear(_: ?*PGresult) callconv(.c) void {
        unexpected();
    }
};

const TypeOid = struct {
    const boolean = 16;
    const int2 = 21;
    const int4 = 23;
    const int8 = 20;
    const float4 = 700;
    const float8 = 701;
    const numeric = 1700;
    const json = 114;
    const jsonb = 3802;
};

pub const Executor = struct {
    const ConnectionPool = struct {
        mutex: Mutex = .unlocked,
        control_mutex: Mutex = .unlocked,
        availability: PoolAvailability,
        idle: std.ArrayListUnmanaged(*PGconn) = .empty,
        total: usize = 0,
        refs: usize = 0,
        last_used_ns: u64 = 0,
    };

    const CachedColumns = struct {
        columns: []foreign_source.Column,
        refreshed_at_ns: u64,
        access_sequence: u64,
    };

    const DetachedConnectionPool = struct {
        key: []const u8,
        pool: *ConnectionPool,
    };

    const PermitReclaimOutcome = enum {
        acquired,
        retry,
        unavailable,
    };

    const PermitWaiter = struct {
        previous: ?*PermitWaiter = null,
        next: ?*PermitWaiter = null,
        availability: PoolAvailability,
        count: usize,
        queued: bool = false,
        granted: bool = false,
        reclaiming: bool = false,
    };

    const AsyncResultDriver = struct {
        executor: *Executor,
        conn: ?*PGconn,

        fn flush(self: @This()) c_int {
            return self.executor.pqflush(self.conn);
        }

        fn wait(
            self: @This(),
            events: i16,
            deadline_ns: ?u64,
            cancellation: ?CancellationToken,
        ) !i16 {
            const now_ns = platform_time.monotonicNs();
            const wait_deadline_ns = if (cancellation != null)
                @min(deadline_ns orelse std.math.maxInt(u64), now_ns +| cancellation_poll_interval_ns)
            else
                deadline_ns orelse unreachable;
            return try self.executor.waitForSocketEvents(self.conn, events, wait_deadline_ns, .query);
        }

        fn consumeInput(self: @This()) c_int {
            return self.executor.pqconsumeInput(self.conn);
        }

        fn isBusy(self: @This()) c_int {
            return self.executor.pqisBusy(self.conn);
        }

        fn getResult(self: @This()) ?*PGresult {
            return self.executor.pqgetResult(self.conn);
        }

        fn clear(self: @This(), result: ?*PGresult) void {
            self.executor.pqclear(result);
        }

        fn restoreBlocking(self: @This()) c_int {
            return self.executor.pqsetnonblocking(self.conn, 0);
        }
    };

    const ConnectionLease = struct {
        executor: *Executor,
        pool: *ConnectionPool,
        conn: *PGconn,
        reusable: bool = true,

        fn invalidate(self: *@This()) void {
            self.reusable = false;
        }

        fn release(self: *@This()) void {
            const executor = self.executor;
            const pool = self.pool;
            if (self.reusable and executor.pqstatus(self.conn) == CONNECTION_OK) {
                lock(&pool.mutex);
                pool.idle.append(executor.alloc, self.conn) catch {
                    pool.total -= 1;
                    pool.mutex.unlock();
                    executor.pqfinish(self.conn);
                    executor.releaseGlobalConnections(1);
                    pool.availability.advance();
                    executor.releaseConnectionPool(pool);
                    self.* = undefined;
                    return;
                };
                pool.mutex.unlock();
                pool.availability.advance();
                executor.notifyGlobalPermitHead();
            } else {
                lock(&pool.mutex);
                pool.total -= 1;
                pool.mutex.unlock();
                executor.pqfinish(self.conn);
                executor.releaseGlobalConnections(1);
                pool.availability.advance();
            }
            executor.releaseConnectionPool(pool);
            self.* = undefined;
        }
    };

    alloc: Allocator,
    lib: ?std.DynLib,
    pools_mutex: Mutex = .unlocked,
    reclaim_mutex: Mutex = .unlocked,
    pools: std.StringHashMapUnmanaged(*ConnectionPool) = .empty,
    permit_mutex: Mutex = .unlocked,
    permit_waiter_head: ?*PermitWaiter = null,
    permit_waiter_tail: ?*PermitWaiter = null,
    permit_waiter_count: std.atomic.Value(usize) = .init(0),
    total_connections: std.atomic.Value(usize) = .init(0),
    cache_mutex: Mutex = .unlocked,
    columns_cache: std.StringHashMapUnmanaged(CachedColumns) = .empty,
    column_cache_access_sequence: u64 = 0,
    connections: std.StringHashMapUnmanaged(*PGconn) = .empty,

    pqconnectdb: FnPQconnectdb,
    pqconnectStart: FnPQconnectStart,
    pqconnectPoll: FnPQconnectPoll,
    pqexec: FnPQexec,
    pqsendQuery: FnPQsendQuery,
    pqstatus: FnPQstatus,
    pqconninfoParse: ?FnPQconninfoParse = null,
    pqconninfoFree: ?FnPQconninfoFree = null,
    pqconnectionUsedPassword: ?FnPQconnectionUsedPassword = null,
    pqerrorMessage: FnPQerrorMessage,
    pqfinish: FnPQfinish,
    pqexecParams: FnPQexecParams,
    pqsendQueryParams: FnPQsendQueryParams,
    pqsetSingleRowMode: ?FnPQsetSingleRowMode = null,
    pqsetnonblocking: FnPQsetnonblocking,
    pqflush: FnPQflush,
    pqconsumeInput: FnPQconsumeInput,
    pqisBusy: FnPQisBusy,
    pqgetResult: FnPQgetResult,
    pqsocket: FnPQsocket,
    pqresultStatus: FnPQresultStatus,
    pqresultErrorMessage: FnPQresultErrorMessage,
    pqresultErrorField: FnPQresultErrorField,
    pqntuples: FnPQntuples,
    pqnfields: FnPQnfields,
    pqfname: FnPQfname,
    pqftype: FnPQftype,
    pqgetisnull: FnPQgetisnull,
    pqgetlength: FnPQgetlength,
    pqgetvalue: FnPQgetvalue,
    pqclear: FnPQclear,

    pub fn init(alloc: Allocator) !@This() {
        var lib = try openDefaultLibpq();
        errdefer lib.close();
        return .{
            .alloc = alloc,
            .lib = lib,
            .pqconnectdb = try lookupRequired(&lib, FnPQconnectdb, "PQconnectdb"),
            .pqconnectStart = try lookupRequired(&lib, FnPQconnectStart, "PQconnectStart"),
            .pqconnectPoll = try lookupRequired(&lib, FnPQconnectPoll, "PQconnectPoll"),
            .pqexec = try lookupRequired(&lib, FnPQexec, "PQexec"),
            .pqsendQuery = try lookupRequired(&lib, FnPQsendQuery, "PQsendQuery"),
            .pqstatus = try lookupRequired(&lib, FnPQstatus, "PQstatus"),
            .pqconninfoParse = try lookupRequired(&lib, FnPQconninfoParse, "PQconninfoParse"),
            .pqconninfoFree = try lookupRequired(&lib, FnPQconninfoFree, "PQconninfoFree"),
            .pqconnectionUsedPassword = try lookupRequired(&lib, FnPQconnectionUsedPassword, "PQconnectionUsedPassword"),
            .pqerrorMessage = try lookupRequired(&lib, FnPQerrorMessage, "PQerrorMessage"),
            .pqfinish = try lookupRequired(&lib, FnPQfinish, "PQfinish"),
            .pqexecParams = try lookupRequired(&lib, FnPQexecParams, "PQexecParams"),
            .pqsendQueryParams = try lookupRequired(&lib, FnPQsendQueryParams, "PQsendQueryParams"),
            .pqsetSingleRowMode = try lookupRequired(&lib, FnPQsetSingleRowMode, "PQsetSingleRowMode"),
            .pqsetnonblocking = try lookupRequired(&lib, FnPQsetnonblocking, "PQsetnonblocking"),
            .pqflush = try lookupRequired(&lib, FnPQflush, "PQflush"),
            .pqconsumeInput = try lookupRequired(&lib, FnPQconsumeInput, "PQconsumeInput"),
            .pqisBusy = try lookupRequired(&lib, FnPQisBusy, "PQisBusy"),
            .pqgetResult = try lookupRequired(&lib, FnPQgetResult, "PQgetResult"),
            .pqsocket = try lookupRequired(&lib, FnPQsocket, "PQsocket"),
            .pqresultStatus = try lookupRequired(&lib, FnPQresultStatus, "PQresultStatus"),
            .pqresultErrorMessage = try lookupRequired(&lib, FnPQresultErrorMessage, "PQresultErrorMessage"),
            .pqresultErrorField = try lookupRequired(&lib, FnPQresultErrorField, "PQresultErrorField"),
            .pqntuples = try lookupRequired(&lib, FnPQntuples, "PQntuples"),
            .pqnfields = try lookupRequired(&lib, FnPQnfields, "PQnfields"),
            .pqfname = try lookupRequired(&lib, FnPQfname, "PQfname"),
            .pqftype = try lookupRequired(&lib, FnPQftype, "PQftype"),
            .pqgetisnull = try lookupRequired(&lib, FnPQgetisnull, "PQgetisnull"),
            .pqgetlength = try lookupRequired(&lib, FnPQgetlength, "PQgetlength"),
            .pqgetvalue = try lookupRequired(&lib, FnPQgetvalue, "PQgetvalue"),
            .pqclear = try lookupRequired(&lib, FnPQclear, "PQclear"),
        };
    }

    /// Scheduler and pool-registry tests must not depend on a host libpq
    /// installation. Typed fail-fast stubs keep accidental future libpq use
    /// deterministic instead of invoking an undefined function pointer.
    fn initForPermitTests(alloc: Allocator) @This() {
        if (!builtin.is_test) @compileError("initForPermitTests is test-only");
        return .{
            .alloc = alloc,
            .lib = null,
            .pqconnectdb = TestLibpqStubs.connectdb,
            .pqconnectStart = TestLibpqStubs.connectStart,
            .pqconnectPoll = TestLibpqStubs.connectPoll,
            .pqexec = TestLibpqStubs.exec,
            .pqsendQuery = TestLibpqStubs.sendQuery,
            .pqstatus = TestLibpqStubs.status,
            .pqerrorMessage = TestLibpqStubs.errorMessage,
            .pqfinish = TestLibpqStubs.finish,
            .pqexecParams = TestLibpqStubs.execParams,
            .pqsendQueryParams = TestLibpqStubs.sendQueryParams,
            .pqsetnonblocking = TestLibpqStubs.setnonblocking,
            .pqflush = TestLibpqStubs.flush,
            .pqconsumeInput = TestLibpqStubs.consumeInput,
            .pqisBusy = TestLibpqStubs.isBusy,
            .pqgetResult = TestLibpqStubs.getResult,
            .pqsocket = TestLibpqStubs.socket,
            .pqresultStatus = TestLibpqStubs.resultStatus,
            .pqresultErrorMessage = TestLibpqStubs.resultErrorMessage,
            .pqresultErrorField = TestLibpqStubs.resultErrorField,
            .pqntuples = TestLibpqStubs.ntuples,
            .pqnfields = TestLibpqStubs.nfields,
            .pqfname = TestLibpqStubs.fname,
            .pqftype = TestLibpqStubs.ftype,
            .pqgetisnull = TestLibpqStubs.getisnull,
            .pqgetlength = TestLibpqStubs.getlength,
            .pqgetvalue = TestLibpqStubs.getvalue,
            .pqclear = TestLibpqStubs.clear,
        };
    }

    pub fn deinit(self: *@This()) void {
        lock(&self.permit_mutex);
        std.debug.assert(self.permit_waiter_head == null);
        std.debug.assert(self.permit_waiter_tail == null);
        std.debug.assert(self.permit_waiter_count.load(.acquire) == 0);
        self.permit_mutex.unlock();

        lock(&self.pools_mutex);
        var pool_it = self.pools.iterator();
        while (pool_it.next()) |entry| {
            const pool = entry.value_ptr.*;
            std.debug.assert(pool.refs == 0);
            lock(&pool.mutex);
            for (pool.idle.items) |conn| self.pqfinish(conn);
            std.debug.assert(pool.total == pool.idle.items.len);
            if (pool.total > 0) _ = self.total_connections.fetchSub(pool.total, .acq_rel);
            pool.idle.deinit(self.alloc);
            pool.mutex.unlock();
            pool.availability.deinit();
            self.alloc.destroy(pool);
            self.alloc.free(entry.key_ptr.*);
        }
        self.pools.deinit(self.alloc);
        self.pools_mutex.unlock();
        std.debug.assert(self.total_connections.load(.acquire) == 0);

        var conn_it = self.connections.iterator();
        while (conn_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            self.pqfinish(entry.value_ptr.*);
        }
        self.connections.deinit(self.alloc);

        lock(&self.cache_mutex);
        var it = self.columns_cache.iterator();
        while (it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            freeColumns(self.alloc, entry.value_ptr.columns);
        }
        self.columns_cache.deinit(self.alloc);
        self.cache_mutex.unlock();
        if (self.lib) |*lib| lib.close();
        self.* = undefined;
    }

    pub fn asQueryExecutor(self: *@This()) postgres_source.QueryExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .transaction_envelopes = true,
                .deinit = deinitQueryExecutor,
                .query = Executor.query,
                .statistics = Executor.statistics,
                .statistics_with_deadline = Executor.statisticsWithDeadline,
                .discover_columns = Executor.discoverColumns,
                .refresh_columns = Executor.refreshColumns,
                .begin_snapshot_query = Executor.beginSnapshotQuery,
                .begin_replication_snapshot_query = Executor.beginReplicationSnapshotQuery,
                .begin_prepared_replication_snapshot = Executor.beginPreparedReplicationSnapshot,
                .begin_subscription_snapshot = Executor.beginSubscriptionSnapshot,
                .poll_subscription = Executor.pollSubscription,
                .acknowledge_subscription = Executor.acknowledgeSubscription,
                .prepare_replication = Executor.prepareReplication,
                .poll_changes = Executor.pollChanges,
                .cleanup_replication = Executor.cleanupReplication,
            },
        };
    }

    fn deinitQueryExecutor(ptr: *anyopaque, alloc: Allocator) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.deinit();
        alloc.destroy(self);
    }

    fn query(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, prepared: sql.PreparedQuery, execution_deadline_ns: ?u64, cancellation: ?CancellationToken) !foreign_source.QueryResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.queryPreparedAllocWithDeadline(alloc, dsn, prepared, execution_deadline_ns, cancellation);
    }

    fn statistics(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, table: []const u8) !foreign_source.TableStatistics {
        return try statisticsWithDeadline(ptr, alloc, dsn, table, null);
    }

    fn statisticsWithDeadline(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, table: []const u8, execution_deadline_ns: ?u64) !foreign_source.TableStatistics {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.statisticsAllocWithDeadline(alloc, dsn, table, execution_deadline_ns);
    }

    fn discoverColumns(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, table: []const u8, execution_deadline_ns: ?u64) ![]foreign_source.Column {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.discoverColumnsAllocWithDeadline(alloc, dsn, table, execution_deadline_ns);
    }

    fn refreshColumns(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, table: []const u8, execution_deadline_ns: ?u64) ![]foreign_source.Column {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try self.invalidateColumnsCache(alloc, dsn, table, execution_deadline_ns);
        return try self.discoverColumnsAllocWithDeadline(alloc, dsn, table, execution_deadline_ns);
    }

    const SnapshotQuery = struct {
        executor: *Executor,
        conn: ?*PGconn,
        replication_identity_enabled: bool = false,
        verified_provider_identity: ?foreign_source.ExactCutoverIntent.ProviderIdentity = null,

        capture_digest: ?[32]u8 = null,
        capture_complete: bool = false,
        capture_relation_locked: bool = false,
        capture_relation_id: ?u32 = null,
        multi_relation: bool = false,
        created_slot: ?struct { name: []u8, checkpoint: []const u8 } = null,
        fn asSnapshotQuery(self: *@This()) postgres_source.QueryExecutor.SnapshotQuery {
            const ordinary: postgres_source.QueryExecutor.SnapshotQuery.VTable = .{
                .deinit = SnapshotQuery.destroy,
                .query = SnapshotQuery.query,
            };
            const replication: postgres_source.QueryExecutor.SnapshotQuery.VTable = .{
                .deinit = SnapshotQuery.destroy,
                .query = SnapshotQuery.query,
                .abort_subscription = SnapshotQuery.abortSubscription,
                .capture_query = SnapshotQuery.capture,
                .capture_query_control = SnapshotQuery.captureControlled,
                .replication_identity_control = SnapshotQuery.replicationIdentityControlled,
                .replication_identity = SnapshotQuery.replicationIdentity,
            };
            return .{ .ptr = self, .vtable = if (self.replication_identity_enabled) &replication else &ordinary };
        }

        fn replicationIdentity(ptr: *anyopaque, a: Allocator, table: []const u8, deadline: ?u64) !foreign_source.ReplicationIdentity {
            return replicationIdentityWithControl(ptr, a, table, deadline, null);
        }
        fn replicationIdentityControlled(ptr: *anyopaque, a: Allocator, table: []const u8, deadline: ?u64, cancellation: CancellationToken) !foreign_source.ReplicationIdentity {
            return replicationIdentityWithControl(ptr, a, table, deadline, cancellation);
        }
        fn replicationIdentityWithControl(ptr: *anyopaque, a: Allocator, table: []const u8, deadline: ?u64, cancellation: ?CancellationToken) !foreign_source.ReplicationIdentity {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const conn = self.conn orelse return error.ForeignConnectionFailed;
            const until = deadline orelse platform_time.monotonicNs() +| 30 * std.time.ns_per_s;
            // Keep the source relation's actual identity stable through every
            // page on this reader. Resolving regclass before its first SELECT
            // would otherwise leave a DROP/recreate gap in the origin anchor.
            const quoted_table = try sql.quotePostgresRelationAlloc(a, table);
            defer a.free(quoted_table);
            const lock_sql = try std.fmt.allocPrint(a, "LOCK TABLE {s} IN ACCESS SHARE MODE", .{quoted_table});
            defer a.free(lock_sql);
            const locked = self.executor.execSimpleWithControl(conn, a, lock_sql, until, true, cancellation) catch |err| {
                self.executor.closeCountedConnection(conn);
                self.conn = null;
                return err;
            };
            self.executor.pqclear(locked);
            const origin = self.executor.sqlReplicationIdentityWithControl(a, conn, table, until, cancellation) catch |err| {
                self.executor.closeCountedConnection(conn);
                self.conn = null;
                return err;
            };
            if (self.verified_provider_identity) |expected| {
                if (!std.mem.eql(u8, &expected, &origin.provider_identity)) {
                    self.executor.closeCountedConnection(conn);
                    self.conn = null;
                    return error.ForeignProviderIdentityMismatch;
                }
            }
            if (self.capture_relation_id) |prior| if (prior != origin.relation_id) {
                if (!self.multi_relation or !self.capture_complete) return error.InvalidReplicationSnapshotCursor;
                const closed = try self.executor.execSimpleWithControl(conn, a, "CLOSE antfly_replication_capture", until, true, cancellation);
                self.executor.pqclear(closed);
                self.capture_digest = null;
                self.capture_complete = false;
            };
            self.capture_relation_id = origin.relation_id;
            self.capture_relation_locked = true;
            return origin;
        }

        fn capture(ptr: *anyopaque, a: Allocator, prepared: sql.PreparedQuery, page_rows: usize, deadline: ?u64) !foreign_source.QueryResult {
            return captureWithControl(ptr, a, prepared, page_rows, deadline, null);
        }
        fn captureControlled(ptr: *anyopaque, a: Allocator, prepared: sql.PreparedQuery, page_rows: usize, deadline: ?u64, cancellation: CancellationToken) !foreign_source.QueryResult {
            return captureWithControl(ptr, a, prepared, page_rows, deadline, cancellation);
        }
        fn captureWithControl(ptr: *anyopaque, a: Allocator, prepared: sql.PreparedQuery, page_rows: usize, deadline: ?u64, cancellation: ?CancellationToken) !foreign_source.QueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var owned = prepared;
            defer owned.deinit(a);
            if (!self.replication_identity_enabled or !self.capture_relation_locked or page_rows == 0 or page_rows > 4096) return error.InvalidReplicationSnapshotCursor;
            const conn = self.conn orelse return error.ForeignConnectionFailed;
            const until = deadline orelse platform_time.monotonicNs() +| 30 * std.time.ns_per_s;
            // One declared cursor, not separate OFFSET plans, owns the exact
            // source row sequence until complete capture has been retained.
            var hashing = std.Io.Writer.Hashing(std.crypto.hash.sha2.Sha256).initHasher(std.crypto.hash.sha2.Sha256.init(.{}), &.{});
            try std.json.Stringify.value(prepared, .{}, &hashing.writer);
            var digest: [32]u8 = undefined;
            hashing.hasher.final(&digest);
            errdefer {
                self.executor.closeCountedConnection(conn);
                self.conn = null;
            }
            if (self.capture_digest) |expected| {
                if (!std.mem.eql(u8, &expected, &digest)) return error.InvalidReplicationSnapshotCursor;
            } else {
                const declaration = try std.fmt.allocPrint(a, "DECLARE antfly_replication_capture NO SCROLL CURSOR FOR {s}", .{prepared.sql_text});
                defer a.free(declaration);
                const result = try self.executor.execPreparedWithDeadlineInternal(conn, a, .{ .sql_text = declaration, .args = prepared.args }, until, true, cancellation);
                self.executor.pqclear(result);
                self.capture_digest = digest;
            }
            if (self.capture_complete) return .{};
            const fetch = try std.fmt.allocPrint(a, "FETCH FORWARD {d} FROM antfly_replication_capture", .{page_rows});
            defer a.free(fetch);
            const result = try self.executor.execSimpleWithControl(conn, a, fetch, until, false, cancellation);
            defer self.executor.pqclear(result);
            const rows = try self.executor.readQueryResultWithProvenance(a, result, until, cancellation, true);
            if (rows.rows.len == 0) self.capture_complete = true;
            return rows;
        }

        fn abortSubscription(ptr: *anyopaque, a: Allocator) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const created = self.created_slot orelse return error.UnsupportedReplicationCleanup;
            const conn = self.conn orelse return error.ForeignConnectionFailed;
            defer {
                self.executor.closeCountedConnection(conn);
                self.conn = null;
            }
            try self.executor.abortSubscriptionExport(a, conn, created.name, created.checkpoint, true);
        }

        fn destroy(ptr: *anyopaque, alloc: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.conn) |conn| self.executor.closeCountedConnection(conn);
            if (self.created_slot) |created| alloc.free(created.name);
            alloc.destroy(self);
        }

        fn query(
            ptr: *anyopaque,
            alloc: Allocator,
            prepared: sql.PreparedQuery,
            execution_deadline_ns: ?u64,
        ) !foreign_source.QueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var owned = prepared;
            defer owned.deinit(alloc);

            const conn = self.conn orelse return error.ForeignConnectionFailed;
            const result = self.executor.execPreparedWithDeadline(conn, alloc, owned, execution_deadline_ns, null) catch |err| {
                // PostgreSQL aborts the entire transaction after any statement
                // error. A snapshot connection is therefore never reusable
                // after a failed query, even when the underlying socket is
                // healthy.
                self.executor.closeCountedConnection(conn);
                self.conn = null;
                return err;
            };
            defer self.executor.pqclear(result);
            return try self.executor.readQueryResultWithProvenance(alloc, result, execution_deadline_ns, null, self.replication_identity_enabled);
        }
    };

    fn beginSnapshotQuery(ptr: *anyopaque, alloc: Allocator, dsn: []const u8) !postgres_source.QueryExecutor.SnapshotQuery {
        const self: *@This() = @ptrCast(@alignCast(ptr));

        const conn = try self.connectCountedFresh(alloc, dsn, null);
        errdefer self.closeCountedConnection(conn);
        const begin_result = try self.execSimpleAllowCommand(conn, alloc, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
        self.pqclear(begin_result);

        const snapshot_query = try alloc.create(SnapshotQuery);
        snapshot_query.* = .{
            .executor = self,
            .conn = conn,
        };
        return snapshot_query.asSnapshotQuery();
    }

    fn beginReplicationSnapshotQuery(ptr: *anyopaque, a: Allocator, dsn: []const u8) !postgres_source.QueryExecutor.SnapshotQuery {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const deadline = platform_time.monotonicNs() +| 30 * std.time.ns_per_s;
        const conn = try self.connectCountedFresh(a, dsn, deadline);
        errdefer self.closeCountedConnection(conn);
        const begun = try self.execSimpleWithDeadline(conn, a, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", deadline, true);
        self.pqclear(begun);
        const snapshot_query = try a.create(SnapshotQuery);
        snapshot_query.* = .{ .executor = self, .conn = conn, .replication_identity_enabled = true };
        return snapshot_query.asSnapshotQuery();
    }

    fn beginPreparedReplicationSnapshot(
        ptr: *anyopaque,
        alloc: Allocator,
        dsn: []const u8,
        params: foreign_source.ReplicationPollParams,
        execution_deadline_ns: u64,
    ) !postgres_source.QueryExecutor.PreparedReplicationSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try ensureDeadline(execution_deadline_ns);
        const slot_name = params.slot_name orelse return error.InvalidQueryRequest;
        const publication_name = params.publication_name orelse return error.InvalidQueryRequest;
        const cutover_lock_name = params.cutover_lock_name orelse
            return error.InvalidQueryRequest;
        const custody = try params.snapshotCustody();
        if ((params.retired_slot_name == null) !=
            (params.retired_publication_name == null))
            return error.InvalidQueryRequest;

        const pool = try self.getOrCreateConnectionPool(dsn, execution_deadline_ns, null);
        defer self.releaseConnectionPool(pool);
        try lockUntil(&pool.control_mutex, execution_deadline_ns);
        defer pool.control_mutex.unlock();

        // Reserve both sockets atomically. Acquiring the replication socket
        // after opening the SQL socket can deadlock when many cutovers reach
        // the global ceiling together.
        try self.acquireGlobalConnectionPermits(2, execution_deadline_ns, null);
        var release_reserved_permits = true;
        errdefer if (release_reserved_permits) self.releaseGlobalConnections(2);
        var sql_conn: ?*PGconn = try self.connectFreshWithDeadline(dsn, execution_deadline_ns, null);
        errdefer if (sql_conn) |conn| self.pqfinish(conn);
        try self.setCutoverAdvisoryLockAlloc(
            alloc,
            sql_conn,
            cutover_lock_name,
            true,
            execution_deadline_ns,
        );
        const slot_exists = try self.logicalReplicationSlotExistsAlloc(
            alloc,
            sql_conn,
            slot_name,
            execution_deadline_ns,
        );
        if (slot_exists and !params.reclaim_exact_cutover_slot)
            return error.UnsupportedExactCutover;

        var repl_conn: ?*PGconn = try self.connectReplicationFreshWithDeadline(
            alloc,
            dsn,
            execution_deadline_ns,
        );
        errdefer if (repl_conn) |conn| self.pqfinish(conn);
        const provider_identity = try self.identifyExactCutoverProvider(
            alloc,
            sql_conn,
            repl_conn,
            execution_deadline_ns,
        );

        // Fence the current authority and authenticate the target before any
        // persistent provider mutation. A stale leader may know the stable
        // ownership identity, but cannot get its fresh authority token applied
        // after losing leadership. Credentials are deliberately absent from
        // provider_identity, so rotating them cannot invalidate ownership.
        try ensureDeadline(execution_deadline_ns);
        const source_relation_id = try self.pollRelationIdentityAlloc(alloc, sql_conn, params.table, execution_deadline_ns);
        const actual_origin: foreign_source.ReplicationIdentity = .{ .provider_identity = provider_identity, .relation_id = source_relation_id };
        try params.checkOrigin(actual_origin);
        try custody.bindOrigin(actual_origin);
        try custody.check();
        try ensureDeadline(execution_deadline_ns);

        if (params.retired_slot_name) |retired_slot_name| {
            const retired_publication_name = params.retired_publication_name.?;
            try custody.check();
            try self.dropInactiveLogicalReplicationSlotIfExistsAlloc(
                alloc,
                sql_conn,
                retired_slot_name,
                execution_deadline_ns,
            );
            if (try self.logicalReplicationSlotExistsAlloc(
                alloc,
                sql_conn,
                retired_slot_name,
                execution_deadline_ns,
            )) return error.ExactCutoverCleanupPending;
            try custody.check();
            try self.dropPublicationIfExistsAlloc(
                alloc,
                sql_conn,
                retired_publication_name,
                execution_deadline_ns,
            );
            try custody.check();
            try custody.retirementComplete();
            try custody.check();
        }

        try custody.check();
        try self.ensurePublicationAlloc(
            alloc,
            sql_conn,
            publication_name,
            params.table,
            params.filter_query_json,
            true,
            execution_deadline_ns,
        );
        if (slot_exists) {
            try custody.check();
            try self.dropInactiveLogicalReplicationSlotIfExistsAlloc(
                alloc,
                sql_conn,
                slot_name,
                execution_deadline_ns,
            );
            if (try self.logicalReplicationSlotExistsAlloc(
                alloc,
                sql_conn,
                slot_name,
                execution_deadline_ns,
            )) return error.ExactCutoverCleanupPending;
        }

        try custody.check();
        var slot_created = false;
        errdefer if (slot_created) {
            // Clean up only a slot whose successful create response this
            // attempt observed. A timeout is ambiguous and must be recovered
            // through the durable pending intent: deleting by name here could
            // otherwise remove a concurrently successful attempt's slot.
            // Close both possibly-busy sessions, then spend one already-
            // reserved permit on a bounded idempotent cleanup.
            if (repl_conn) |conn| {
                self.pqfinish(conn);
                repl_conn = null;
            }
            if (sql_conn) |conn| {
                self.pqfinish(conn);
                sql_conn = null;
            }
            const cleanup_deadline_ns =
                platform_time.monotonicNs() +| failed_cutover_cleanup_timeout_ns;
            self.dropLogicalReplicationSlotIfExistsWithReservedPermit(
                alloc,
                dsn,
                slot_name,
                cleanup_deadline_ns,
            ) catch |cleanup_err| {
                std.log.err(
                    "postgres exact cutover cleanup failed slot={s}: {s}",
                    .{ slot_name, @errorName(cleanup_err) },
                );
            };
        };
        var exported = try self.createLogicalReplicationSlotExportSnapshotAlloc(
            alloc,
            repl_conn,
            slot_name,
            execution_deadline_ns,
            &slot_created,
        );
        defer exported.deinit(alloc);
        try self.setCutoverAdvisoryLockAlloc(
            alloc,
            sql_conn,
            cutover_lock_name,
            false,
            execution_deadline_ns,
        );

        const begin_result = try self.execSimpleWithDeadline(
            sql_conn,
            alloc,
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
            execution_deadline_ns,
            true,
        );
        self.pqclear(begin_result);
        const quoted_snapshot = try quoteSqlStringLiteralAlloc(alloc, exported.snapshot_name);
        defer alloc.free(quoted_snapshot);
        const import_sql = try std.fmt.allocPrint(
            alloc,
            "SET TRANSACTION SNAPSHOT {s}",
            .{quoted_snapshot},
        );
        defer alloc.free(import_sql);
        const import_result = try self.execSimpleWithDeadline(
            sql_conn,
            alloc,
            import_sql,
            execution_deadline_ns,
            true,
        );
        self.pqclear(import_result);

        const snapshot_query = try alloc.create(SnapshotQuery);
        errdefer alloc.destroy(snapshot_query);
        snapshot_query.* = .{
            .executor = self,
            .conn = sql_conn,
            .replication_identity_enabled = true,
            .verified_provider_identity = provider_identity,
        };
        const binding = try self.publicationBinding(alloc, sql_conn, publication_name, params.table, params.filter_query_json != null, execution_deadline_ns);
        if (binding.relation != source_relation_id) return error.ReplicationPublicationBindingChanged;
        const checkpoint = try binding.checkpoint(alloc, exported.checkpoint);
        errdefer alloc.free(checkpoint);
        const snapshot_token = try alloc.dupe(u8, exported.snapshot_name);
        errdefer alloc.free(snapshot_token);
        self.pqfinish(repl_conn);
        repl_conn = null;
        self.releaseGlobalConnections(1);
        release_reserved_permits = false;
        slot_created = false;
        return .{
            .origin = .{ .provider_identity = provider_identity, .relation_id = source_relation_id },
            .checkpoint = checkpoint,
            .snapshot_token = snapshot_token,
            .snapshot_query = snapshot_query.asSnapshotQuery(),
        };
    }

    fn pollChanges(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPollResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const pool = try self.getOrCreateConnectionPool(dsn, params.execution_deadline_ns, null);
        defer self.releaseConnectionPool(pool);
        if (params.execution_deadline_ns) |deadline_ns|
            try lockUntil(&pool.control_mutex, deadline_ns)
        else
            lock(&pool.control_mutex);
        defer pool.control_mutex.unlock();
        return try self.pollChangesAlloc(alloc, dsn, params);
    }

    fn prepareReplication(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPrepareResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const pool = try self.getOrCreateConnectionPool(dsn, params.execution_deadline_ns, null);
        defer self.releaseConnectionPool(pool);
        if (params.execution_deadline_ns) |deadline_ns|
            try lockUntil(&pool.control_mutex, deadline_ns)
        else
            lock(&pool.control_mutex);
        defer pool.control_mutex.unlock();
        return try self.prepareReplicationAlloc(alloc, dsn, params);
    }

    fn cleanupReplication(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationCleanupParams) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try params.check();
        const execution_deadline_ns = params.execution_deadline_ns orelse
            platform_time.monotonicNs() +| 30 * std.time.ns_per_s;
        const pool = try self.getOrCreateConnectionPool(
            dsn,
            execution_deadline_ns,
            null,
        );
        defer self.releaseConnectionPool(pool);
        try lockUntil(&pool.control_mutex, execution_deadline_ns);
        defer pool.control_mutex.unlock();
        var bounded_params = params;
        bounded_params.execution_deadline_ns = execution_deadline_ns;
        return try self.cleanupReplicationAlloc(alloc, dsn, bounded_params);
    }

    fn queryPreparedAlloc(self: *@This(), alloc: Allocator, dsn: []const u8, prepared: sql.PreparedQuery) !foreign_source.QueryResult {
        return try self.queryPreparedAllocWithDeadline(alloc, dsn, prepared, null, null);
    }

    fn queryPreparedAllocWithDeadline(
        self: *@This(),
        alloc: Allocator,
        dsn: []const u8,
        prepared: sql.PreparedQuery,
        execution_deadline_ns: ?u64,
        cancellation: ?CancellationToken,
    ) !foreign_source.QueryResult {
        if (cancellation) |value| if (value.isCancelled()) return error.Cancelled;
        var owned = prepared;
        defer owned.deinit(alloc);

        std.log.info("postgres libpq query begin sql_len={d}", .{owned.sql_text.len});
        var lease = try self.acquireConnection(dsn, execution_deadline_ns, cancellation);
        defer lease.release();
        std.log.info("postgres libpq query connected sql_len={d}", .{owned.sql_text.len});
        if (cancellation) |value| if (value.isCancelled()) {
            lease.invalidate();
            return error.Cancelled;
        };
        const result = self.execPreparedWithDeadline(lease.conn, alloc, owned, execution_deadline_ns, cancellation) catch |err| {
            if (invalidatesConnection(err)) lease.invalidate();
            return err;
        };
        defer self.pqclear(result);

        return self.readQueryResultAllocWithDeadline(alloc, result, execution_deadline_ns, cancellation) catch |err| {
            if (invalidatesConnection(err)) lease.invalidate();
            return err;
        };
    }

    fn statisticsAlloc(self: *@This(), alloc: Allocator, dsn: []const u8, table: []const u8) !foreign_source.TableStatistics {
        return try self.statisticsAllocWithDeadline(alloc, dsn, table, null);
    }

    fn statisticsAllocWithDeadline(
        self: *@This(),
        alloc: Allocator,
        dsn: []const u8,
        table: []const u8,
        execution_deadline_ns: ?u64,
    ) !foreign_source.TableStatistics {
        var lease = try self.acquireConnection(dsn, execution_deadline_ns, null);
        defer lease.release();

        var prepared = try relationPreparedQueryAlloc(
            alloc,
            "SELECT COALESCE(c.reltuples, 0)::bigint, COALESCE(pg_total_relation_size(c.oid), 0) FROM pg_class AS c WHERE c.oid = to_regclass($1)",
            table,
        );
        defer prepared.deinit(alloc);

        const result = self.execPreparedWithDeadline(lease.conn, alloc, prepared, execution_deadline_ns, null) catch |err| {
            if (invalidatesConnection(err)) lease.invalidate();
            return err;
        };
        defer self.pqclear(result);

        if (self.pqntuples(result) == 0 or self.pqnfields(result) < 2) return .{};
        if (self.pqgetisnull(result, 0, 0) != 0 or self.pqgetisnull(result, 0, 1) != 0) return .{};
        if (execution_deadline_ns) |deadline_ns| try ensureDeadline(deadline_ns);

        return .{
            .row_count = try parseIntCell(self.pqgetvalue(result, 0, 0), @intCast(self.pqgetlength(result, 0, 0))),
            .size_bytes = try parseIntCell(self.pqgetvalue(result, 0, 1), @intCast(self.pqgetlength(result, 0, 1))),
        };
    }

    fn discoverColumnsAlloc(self: *@This(), alloc: Allocator, dsn: []const u8, table: []const u8) ![]foreign_source.Column {
        return try self.discoverColumnsAllocWithDeadline(alloc, dsn, table, null);
    }

    fn invalidateColumnsCache(
        self: *@This(),
        alloc: Allocator,
        dsn: []const u8,
        table: []const u8,
        execution_deadline_ns: ?u64,
    ) !void {
        const relation_sql = try sql.quotePostgresRelationAlloc(alloc, table);
        defer alloc.free(relation_sql);
        const lookup_key = try columnCacheKeyAlloc(alloc, dsn, relation_sql);
        defer alloc.free(lookup_key);
        try lockUntil(&self.cache_mutex, execution_deadline_ns);
        defer self.cache_mutex.unlock();
        if (self.columns_cache.fetchRemove(lookup_key)) |removed| {
            self.alloc.free(removed.key);
            freeColumns(self.alloc, removed.value.columns);
        }
    }

    fn discoverColumnsAllocWithDeadline(
        self: *@This(),
        alloc: Allocator,
        dsn: []const u8,
        table: []const u8,
        execution_deadline_ns: ?u64,
    ) ![]foreign_source.Column {
        const relation_sql = try sql.quotePostgresRelationAlloc(alloc, table);
        defer alloc.free(relation_sql);
        const lookup_key = try columnCacheKeyAlloc(alloc, dsn, relation_sql);
        defer alloc.free(lookup_key);

        try lockUntil(&self.cache_mutex, execution_deadline_ns);
        const now_ns = platform_time.monotonicNs();
        if (self.columns_cache.getPtr(lookup_key)) |cached| {
            if (now_ns -| cached.refreshed_at_ns < column_cache_ttl_ns) {
                self.column_cache_access_sequence +%= 1;
                cached.access_sequence = self.column_cache_access_sequence;
                defer self.cache_mutex.unlock();
                if (execution_deadline_ns) |deadline_ns| try ensureDeadline(deadline_ns);
                const cloned = try cloneColumnsAlloc(alloc, cached.columns);
                errdefer freeColumns(alloc, cloned);
                if (execution_deadline_ns) |deadline_ns| try ensureDeadline(deadline_ns);
                return cloned;
            }
            if (self.columns_cache.fetchRemove(lookup_key)) |expired| {
                self.alloc.free(expired.key);
                freeColumns(self.alloc, expired.value.columns);
            }
        }
        self.cache_mutex.unlock();

        var lease = try self.acquireConnection(dsn, execution_deadline_ns, null);
        defer lease.release();
        var prepared = try tableNamePreparedQueryAlloc(
            alloc,
            "WITH relation AS (SELECT to_regclass($1) AS oid) SELECT a.attname, format_type(a.atttypid, a.atttypmod), CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END FROM relation AS r JOIN pg_attribute AS a ON a.attrelid = r.oid WHERE a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
            relation_sql,
        );
        defer prepared.deinit(alloc);

        const result = self.execPreparedWithDeadline(lease.conn, alloc, prepared, execution_deadline_ns, null) catch |err| {
            if (invalidatesConnection(err)) lease.invalidate();
            return err;
        };
        defer self.pqclear(result);

        const column_count: usize = @intCast(self.pqntuples(result));
        if (column_count == 0) return error.ForeignTableNotFound;

        const discovered = try alloc.alloc(foreign_source.Column, column_count);
        var initialized_columns: usize = 0;
        errdefer {
            for (discovered[0..initialized_columns]) |*column| column.deinit(alloc);
            alloc.free(discovered);
        }
        var rows_until_deadline_check: u8 = 1;
        for (0..column_count) |row_idx| {
            rows_until_deadline_check -|= 1;
            if (rows_until_deadline_check == 0) {
                rows_until_deadline_check = 64;
                if (execution_deadline_ns) |deadline_ns| try ensureDeadline(deadline_ns);
            }
            const row: c_int = @intCast(row_idx);
            const name = try copyCellAlloc(alloc, self.pqgetvalue(result, row, 0), @intCast(self.pqgetlength(result, row, 0)));
            errdefer alloc.free(name);
            const data_type = try copyCellAlloc(alloc, self.pqgetvalue(result, row, 1), @intCast(self.pqgetlength(result, row, 1)));
            errdefer alloc.free(data_type);
            const nullable_text = try copyCellAlloc(alloc, self.pqgetvalue(result, row, 2), @intCast(self.pqgetlength(result, row, 2)));
            defer alloc.free(nullable_text);
            discovered[row_idx] = .{
                .name = name,
                .data_type = data_type,
                .nullable = std.mem.eql(u8, nullable_text, "YES"),
            };
            initialized_columns += 1;
        }

        const cache_key = try columnCacheKeyAlloc(self.alloc, dsn, relation_sql);
        errdefer self.alloc.free(cache_key);
        const cached_columns = try cloneColumnsAlloc(self.alloc, discovered);
        errdefer freeColumns(self.alloc, cached_columns);
        if (execution_deadline_ns) |deadline_ns| try ensureDeadline(deadline_ns);
        try lockUntil(&self.cache_mutex, execution_deadline_ns);
        defer self.cache_mutex.unlock();
        if (!self.columns_cache.contains(cache_key)) {
            while (self.columns_cache.count() >= max_column_cache_entries) {
                if (!self.evictOldestColumnCacheLocked()) return error.ForeignColumnCacheLimitExceeded;
            }
            self.column_cache_access_sequence +%= 1;
            try self.columns_cache.put(self.alloc, cache_key, .{
                .columns = cached_columns,
                .refreshed_at_ns = platform_time.monotonicNs(),
                .access_sequence = self.column_cache_access_sequence,
            });
        } else {
            self.alloc.free(cache_key);
            freeColumns(self.alloc, cached_columns);
        }

        return discovered;
    }

    fn getOrCreateConnectionPool(
        self: *@This(),
        dsn: []const u8,
        execution_deadline_ns: ?u64,
        cancellation: ?CancellationToken,
    ) !*ConnectionPool {
        var reclaim_count: usize = 0;
        while (true) {
            try lockUntilCancellable(&self.pools_mutex, execution_deadline_ns, cancellation);
            const now_ns = platform_time.monotonicNs();
            if (self.pools.get(dsn)) |pool| {
                pool.refs += 1;
                pool.last_used_ns = now_ns;
                self.pools_mutex.unlock();
                return pool;
            }

            var detached: ?DetachedConnectionPool = null;
            if (reclaim_count < max_pool_reclaims_per_acquire) {
                detached = self.detachOneConnectionPoolLocked(
                    now_ns,
                    self.pools.count() < max_connection_pools,
                );
            }
            if (detached) |victim| {
                reclaim_count += 1;
                self.pools_mutex.unlock();
                self.destroyDetachedConnectionPool(victim);
                if (execution_deadline_ns) |deadline_ns| try ensureDeadline(deadline_ns);
                try ensureNotCancelled(cancellation);
                continue;
            }
            if (self.pools.count() >= max_connection_pools) {
                self.pools_mutex.unlock();
                return error.ForeignConnectionPoolLimitExceeded;
            }

            const owned_dsn = self.alloc.dupe(u8, dsn) catch |err| {
                self.pools_mutex.unlock();
                return err;
            };
            const pool = self.alloc.create(ConnectionPool) catch |err| {
                self.alloc.free(owned_dsn);
                self.pools_mutex.unlock();
                return err;
            };
            const availability = PoolAvailability.init(self.alloc) catch |err| {
                self.alloc.destroy(pool);
                self.alloc.free(owned_dsn);
                self.pools_mutex.unlock();
                return err;
            };
            pool.* = .{
                .availability = availability,
                .refs = 1,
                .last_used_ns = now_ns,
            };
            self.pools.put(self.alloc, owned_dsn, pool) catch |err| {
                pool.availability.deinit();
                self.alloc.destroy(pool);
                self.alloc.free(owned_dsn);
                self.pools_mutex.unlock();
                return err;
            };
            self.pools_mutex.unlock();
            return pool;
        }
    }

    fn releaseConnectionPool(self: *@This(), pool: *ConnectionPool) void {
        lock(&self.pools_mutex);
        std.debug.assert(pool.refs > 0);
        pool.refs -= 1;
        pool.last_used_ns = platform_time.monotonicNs();
        self.pools_mutex.unlock();
    }

    fn detachOneConnectionPoolLocked(
        self: *@This(),
        now_ns: u64,
        expired_only: bool,
    ) ?DetachedConnectionPool {
        var candidate_key: ?[]const u8 = null;
        var candidate_pool: ?*ConnectionPool = null;
        var it = self.pools.iterator();
        while (it.next()) |entry| {
            const pool = entry.value_ptr.*;
            if (pool.refs != 0) continue;
            if (expired_only and now_ns -| pool.last_used_ns < connection_pool_idle_ttl_ns) continue;
            if (candidate_pool == null or pool.last_used_ns < candidate_pool.?.last_used_ns) {
                candidate_key = entry.key_ptr.*;
                candidate_pool = pool;
            }
        }
        const key = candidate_key orelse return null;
        const removed = self.pools.fetchRemove(key) orelse unreachable;
        return .{
            .key = removed.key,
            .pool = removed.value,
        };
    }

    fn destroyDetachedConnectionPool(self: *@This(), detached: DetachedConnectionPool) void {
        defer self.alloc.free(detached.key);
        const pool = detached.pool;
        std.debug.assert(pool.refs == 0);
        lock(&pool.mutex);
        std.debug.assert(pool.total == pool.idle.items.len);
        for (pool.idle.items) |conn| self.pqfinish(conn);
        const closed_count = pool.total;
        pool.total = 0;
        pool.idle.deinit(self.alloc);
        pool.mutex.unlock();
        if (closed_count > 0) self.releaseGlobalConnections(closed_count);
        pool.availability.deinit();
        self.alloc.destroy(pool);
    }

    fn reserveGlobalConnections(self: *@This(), count: usize) bool {
        std.debug.assert(count > 0 and count <= max_total_connections);
        var current = self.total_connections.load(.acquire);
        while (current <= max_total_connections - count) {
            if (self.total_connections.cmpxchgWeak(
                current,
                current + count,
                .acq_rel,
                .acquire,
            )) |observed| {
                current = observed;
            } else {
                return true;
            }
        }
        return false;
    }

    fn enqueueGlobalPermitWaiterLocked(self: *@This(), waiter: *PermitWaiter) void {
        std.debug.assert(!waiter.queued and !waiter.granted);
        std.debug.assert(waiter.previous == null and waiter.next == null);
        if (self.permit_waiter_tail) |tail| {
            tail.next = waiter;
            waiter.previous = tail;
        } else {
            std.debug.assert(self.permit_waiter_head == null);
            self.permit_waiter_head = waiter;
        }
        self.permit_waiter_tail = waiter;
        waiter.queued = true;
        // Queue publication participates in the zero-waiter notification
        // handshake. A producer that observed zero before this RMW publishes
        // its capacity change to the newly queued waiter; a producer after
        // this RMW must observe a non-zero count and enter the scheduler.
        _ = self.permit_waiter_count.fetchAdd(1, .seq_cst);
    }

    fn removeGlobalPermitWaiterLocked(self: *@This(), waiter: *PermitWaiter) bool {
        if (!waiter.queued) return false;
        if (waiter.previous) |previous| {
            previous.next = waiter.next;
        } else {
            std.debug.assert(self.permit_waiter_head == waiter);
            self.permit_waiter_head = waiter.next;
        }
        if (waiter.next) |next| {
            next.previous = waiter.previous;
        } else {
            std.debug.assert(self.permit_waiter_tail == waiter);
            self.permit_waiter_tail = waiter.previous;
        }
        waiter.previous = null;
        waiter.next = null;
        waiter.queued = false;
        const old_count = self.permit_waiter_count.fetchSub(1, .seq_cst);
        std.debug.assert(old_count > 0);
        return true;
    }

    fn hasPublishedGlobalPermitWaiters(self: *@This()) bool {
        // Queue membership changes and this advisory gate share the global
        // sequentially consistent order:
        //
        // * a producer ordered after enqueue observes a waiter and schedules it;
        // * a producer ordered before enqueue may skip the mutex, but the new
        //   waiter immediately rechecks atomic capacity and idle pools under
        //   their publication locks before it can sleep.
        //
        // A read-only load keeps the common no-waiter path cache-friendly
        // without allowing a stale zero to strand the FIFO head.
        return self.permit_waiter_count.load(.seq_cst) != 0;
    }

    fn grantAvailableGlobalPermitWaitersLocked(self: *@This()) void {
        var head_changed = false;
        while (self.permit_waiter_head) |waiter| {
            // The FIFO head remains queued while it reclaims idle sockets
            // without the scheduler lock. Its reservation is committed by
            // that thread, so a release must not grant the same waiter twice.
            if (waiter.reclaiming) break;
            if (!self.reserveGlobalConnections(waiter.count)) break;
            std.debug.assert(self.removeGlobalPermitWaiterLocked(waiter));
            waiter.granted = true;
            head_changed = true;
            // Every waiter owns its condition, so this is a targeted handoff
            // rather than a broadcast to all saturated callers.
            waiter.availability.advance();
        }
        // A newly exposed head may be able to replace an idle pooled socket
        // even when there is not enough unreserved capacity.
        if (head_changed) {
            if (self.permit_waiter_head) |waiter| waiter.availability.advance();
        }
    }

    fn notifyGlobalPermitHead(self: *@This()) void {
        if (!self.hasPublishedGlobalPermitWaiters()) return;
        lock(&self.permit_mutex);
        if (self.permit_waiter_head) |waiter| waiter.availability.advance();
        self.permit_mutex.unlock();
    }

    fn releaseGlobalConnectionsRaw(self: *@This(), count: usize) void {
        std.debug.assert(count > 0);
        const previous = self.total_connections.fetchSub(count, .acq_rel);
        std.debug.assert(previous >= count);
    }

    fn cancelGlobalPermitWaiter(self: *@This(), waiter: *PermitWaiter) void {
        lock(&self.permit_mutex);
        std.debug.assert(!waiter.reclaiming);
        if (waiter.granted) {
            waiter.granted = false;
            self.releaseGlobalConnectionsRaw(waiter.count);
        } else {
            _ = self.removeGlobalPermitWaiterLocked(waiter);
        }
        self.grantAvailableGlobalPermitWaitersLocked();
        if (self.permit_waiter_head) |head| head.availability.advance();
        self.permit_mutex.unlock();
    }

    fn acquireGlobalConnectionPermits(
        self: *@This(),
        count: usize,
        execution_deadline_ns: ?u64,
        cancellation: ?CancellationToken,
    ) !void {
        std.debug.assert(count > 0 and count <= max_total_connections);
        try ensureNotCancelled(cancellation);

        // All production reservations pass through this short gate. Once a
        // waiter is queued, later one-slot requests cannot bypass it.
        try lockUntilCancellable(&self.permit_mutex, execution_deadline_ns, cancellation);
        if (self.permit_waiter_head == null and self.reserveGlobalConnections(count)) {
            self.permit_mutex.unlock();
            if (execution_deadline_ns) |deadline_ns| {
                ensureDeadline(deadline_ns) catch |err| {
                    self.releaseGlobalConnections(count);
                    return err;
                };
            }
            return;
        }
        self.permit_mutex.unlock();

        var waiter_availability = try PoolAvailability.init(self.alloc);
        defer waiter_availability.deinit();
        var waiter = PermitWaiter{
            .availability = waiter_availability,
            .count = count,
        };

        lockUntilCancellable(&self.permit_mutex, execution_deadline_ns, cancellation) catch |err| return err;
        if (self.permit_waiter_head == null and self.reserveGlobalConnections(count)) {
            self.permit_mutex.unlock();
            if (execution_deadline_ns) |deadline_ns| {
                ensureDeadline(deadline_ns) catch |err| {
                    self.releaseGlobalConnections(count);
                    return err;
                };
            }
            return;
        }
        self.enqueueGlobalPermitWaiterLocked(&waiter);

        while (true) {
            if (waiter.granted) {
                waiter.granted = false;
                self.permit_mutex.unlock();
                if (execution_deadline_ns) |deadline_ns| {
                    ensureDeadline(deadline_ns) catch |err| {
                        self.releaseGlobalConnections(count);
                        return err;
                    };
                }
                return;
            }

            if (self.permit_waiter_head == &waiter) {
                if (self.reserveGlobalConnections(count)) {
                    std.debug.assert(self.removeGlobalPermitWaiterLocked(&waiter));
                    self.grantAvailableGlobalPermitWaitersLocked();
                    self.permit_mutex.unlock();
                    if (execution_deadline_ns) |deadline_ns| {
                        ensureDeadline(deadline_ns) catch |err| {
                            self.releaseGlobalConnections(count);
                            return err;
                        };
                    }
                    return;
                }

                // Observe the targeted wake epoch before checking idle pools.
                // Any idle return during the unlocked reclaim phase then
                // forces another predicate check instead of becoming a lost
                // notification.
                const reclaim_observed = waiter.availability.snapshot();
                waiter.reclaiming = true;
                self.permit_mutex.unlock();
                const reclaim_result = self.tryAcquireGlobalConnectionPermitsByReclaiming(
                    count,
                    execution_deadline_ns,
                    cancellation,
                );

                // Reclaim may close sockets or wait on pool locks, so it never
                // holds the global scheduler lock. Reacquire without a
                // deadline to commit or roll back the stack-owned waiter
                // before returning.
                lock(&self.permit_mutex);
                std.debug.assert(waiter.queued);
                std.debug.assert(self.permit_waiter_head == &waiter);
                std.debug.assert(waiter.reclaiming);
                waiter.reclaiming = false;

                const reclaim_outcome = reclaim_result catch |err| {
                    std.debug.assert(self.removeGlobalPermitWaiterLocked(&waiter));
                    self.grantAvailableGlobalPermitWaitersLocked();
                    if (self.permit_waiter_head) |head| head.availability.advance();
                    self.permit_mutex.unlock();
                    return err;
                };
                if (reclaim_outcome == .acquired) {
                    std.debug.assert(self.removeGlobalPermitWaiterLocked(&waiter));
                    self.grantAvailableGlobalPermitWaitersLocked();
                    if (self.permit_waiter_head) |head| head.availability.advance();
                    self.permit_mutex.unlock();
                    if (execution_deadline_ns) |deadline_ns| {
                        ensureDeadline(deadline_ns) catch |err| {
                            self.releaseGlobalConnections(count);
                            return err;
                        };
                    }
                    return;
                }

                // A release may have supplied capacity while reclamation was
                // running. The FIFO owner gets first claim before any younger
                // waiter is considered.
                if (self.reserveGlobalConnections(count)) {
                    std.debug.assert(self.removeGlobalPermitWaiterLocked(&waiter));
                    self.grantAvailableGlobalPermitWaitersLocked();
                    if (self.permit_waiter_head) |head| head.availability.advance();
                    self.permit_mutex.unlock();
                    if (execution_deadline_ns) |deadline_ns| {
                        ensureDeadline(deadline_ns) catch |err| {
                            self.releaseGlobalConnections(count);
                            return err;
                        };
                    }
                    return;
                }

                if (execution_deadline_ns) |deadline_ns| {
                    ensureDeadline(deadline_ns) catch |err| {
                        std.debug.assert(self.removeGlobalPermitWaiterLocked(&waiter));
                        self.grantAvailableGlobalPermitWaitersLocked();
                        if (self.permit_waiter_head) |head| head.availability.advance();
                        self.permit_mutex.unlock();
                        return err;
                    };
                }

                // Partial reclamation freed capacity but not enough for this
                // weighted request. Continue immediately so the head can
                // collect the remaining idle sockets.
                if (reclaim_outcome == .retry) continue;
                if (waiter.availability.snapshot() != reclaim_observed) continue;

                self.permit_mutex.unlock();
                waitForAvailabilityChange(
                    &waiter.availability,
                    reclaim_observed,
                    execution_deadline_ns,
                    cancellation,
                ) catch |err| {
                    self.cancelGlobalPermitWaiter(&waiter);
                    return err;
                };
                lockUntilCancellable(&self.permit_mutex, execution_deadline_ns, cancellation) catch |err| {
                    self.cancelGlobalPermitWaiter(&waiter);
                    return err;
                };
                continue;
            }

            const observed = waiter.availability.snapshot();
            self.permit_mutex.unlock();
            waitForAvailabilityChange(
                &waiter.availability,
                observed,
                execution_deadline_ns,
                cancellation,
            ) catch |err| {
                self.cancelGlobalPermitWaiter(&waiter);
                return err;
            };
            lockUntilCancellable(&self.permit_mutex, execution_deadline_ns, cancellation) catch |err| {
                self.cancelGlobalPermitWaiter(&waiter);
                return err;
            };
        }
    }

    /// Replaces the minimum number of idle sockets with permits for this
    /// caller in one atomic counter transition. The reclaimed capacity cannot
    /// be stolen by another waiter between closing a socket and reserving its
    /// replacement, and a two-socket cutover only evicts one idle connection
    /// when one global slot was already free.
    fn tryAcquireGlobalConnectionPermitsByReclaiming(
        self: *@This(),
        count: usize,
        execution_deadline_ns: ?u64,
        cancellation: ?CancellationToken,
    ) !PermitReclaimOutcome {
        var reclaimed: [max_total_connections]*PGconn = undefined;
        var reclaimed_count: usize = 0;

        try lockUntilCancellable(&self.reclaim_mutex, execution_deadline_ns, cancellation);
        defer self.reclaim_mutex.unlock();

        while (true) {
            const current = self.total_connections.load(.acquire);
            std.debug.assert(current >= reclaimed_count);
            const desired = current - reclaimed_count + count;
            if (desired <= max_total_connections) {
                if (self.total_connections.cmpxchgWeak(
                    current,
                    desired,
                    .acq_rel,
                    .acquire,
                ) != null) continue;

                for (reclaimed[0..reclaimed_count]) |conn| self.pqfinish(conn);
                if (execution_deadline_ns) |deadline_ns| {
                    ensureDeadline(deadline_ns) catch |err| {
                        self.releaseGlobalConnectionsRaw(count);
                        return err;
                    };
                }
                return .acquired;
            }

            const additional_needed = desired - max_total_connections;
            std.debug.assert(additional_needed > 0);
            std.debug.assert(reclaimed_count + additional_needed <= count);

            lockUntilCancellable(&self.pools_mutex, execution_deadline_ns, cancellation) catch |err| {
                for (reclaimed[0..reclaimed_count]) |conn| self.pqfinish(conn);
                if (reclaimed_count > 0) self.releaseGlobalConnectionsRaw(reclaimed_count);
                return err;
            };
            var added: usize = 0;
            var it = self.pools.iterator();
            while (it.next()) |entry| {
                if (added >= additional_needed) break;
                const pool = entry.value_ptr.*;
                const pool_reclaimed_start = reclaimed_count;
                lockUntilCancellable(&pool.mutex, execution_deadline_ns, cancellation) catch |err| {
                    self.pools_mutex.unlock();
                    for (reclaimed[0..reclaimed_count]) |conn| self.pqfinish(conn);
                    if (reclaimed_count > 0) self.releaseGlobalConnectionsRaw(reclaimed_count);
                    return err;
                };
                while (added < additional_needed) {
                    reclaimed[reclaimed_count] = pool.idle.pop() orelse break;
                    reclaimed_count += 1;
                    added += 1;
                    pool.total -= 1;
                }
                pool.mutex.unlock();
                if (reclaimed_count > pool_reclaimed_start) pool.availability.advanceAll();
            }
            self.pools_mutex.unlock();

            if (added < additional_needed) {
                for (reclaimed[0..reclaimed_count]) |conn| self.pqfinish(conn);
                if (reclaimed_count > 0) {
                    self.releaseGlobalConnectionsRaw(reclaimed_count);
                    return .retry;
                }
                return .unavailable;
            }
        }
    }

    fn releaseGlobalConnections(self: *@This(), count: usize) void {
        self.releaseGlobalConnectionsRaw(count);
        if (!self.hasPublishedGlobalPermitWaiters()) return;
        lock(&self.permit_mutex);
        self.grantAvailableGlobalPermitWaitersLocked();
        self.permit_mutex.unlock();
    }

    fn acquireConnection(
        self: *@This(),
        dsn: []const u8,
        execution_deadline_ns: ?u64,
        cancellation: ?CancellationToken,
    ) !ConnectionLease {
        try ensureNotCancelled(cancellation);
        const pool = try self.getOrCreateConnectionPool(dsn, execution_deadline_ns, cancellation);
        errdefer self.releaseConnectionPool(pool);
        while (true) {
            try lockUntilCancellable(&pool.mutex, execution_deadline_ns, cancellation);
            if (pool.idle.pop()) |conn| {
                pool.mutex.unlock();
                if (self.pqstatus(conn) == CONNECTION_OK) {
                    return .{ .executor = self, .pool = pool, .conn = conn };
                }
                self.pqfinish(conn);
                lock(&pool.mutex);
                pool.total -= 1;
                pool.mutex.unlock();
                self.releaseGlobalConnections(1);
                pool.availability.advance();
                continue;
            }
            if (pool.total < max_connections_per_dsn) {
                pool.mutex.unlock();
                try self.acquireGlobalConnectionPermits(1, execution_deadline_ns, cancellation);
                lockUntilCancellable(&pool.mutex, execution_deadline_ns, cancellation) catch |err| {
                    self.releaseGlobalConnections(1);
                    return err;
                };
                // Another waiter may have filled the per-DSN pool while this
                // request waited for a global permit. Return the permit and
                // re-evaluate rather than exceeding the per-DSN bound.
                if (pool.total >= max_connections_per_dsn or pool.idle.items.len > 0) {
                    pool.mutex.unlock();
                    self.releaseGlobalConnections(1);
                    continue;
                }
                pool.total += 1;
                pool.mutex.unlock();
                const conn = self.connectFreshWithDeadline(dsn, execution_deadline_ns, cancellation) catch |err| {
                    lock(&pool.mutex);
                    pool.total -= 1;
                    pool.mutex.unlock();
                    self.releaseGlobalConnections(1);
                    pool.availability.advance();
                    return err;
                };
                return .{ .executor = self, .pool = pool, .conn = conn };
            }
            const observed = pool.availability.snapshot();
            pool.mutex.unlock();
            try waitForAvailabilityChange(&pool.availability, observed, execution_deadline_ns, cancellation);
        }
    }

    fn evictOldestColumnCacheLocked(self: *@This()) bool {
        var candidate_key: ?[]const u8 = null;
        var candidate_sequence: u64 = std.math.maxInt(u64);
        var it = self.columns_cache.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.access_sequence <= candidate_sequence) {
                candidate_key = entry.key_ptr.*;
                candidate_sequence = entry.value_ptr.access_sequence;
            }
        }
        const key = candidate_key orelse return false;
        const removed = self.columns_cache.fetchRemove(key) orelse unreachable;
        self.alloc.free(removed.key);
        freeColumns(self.alloc, removed.value.columns);
        return true;
    }

    fn connect(self: *@This(), dsn: []const u8) !?*PGconn {
        if (self.connections.get(dsn)) |cached| {
            if (self.pqstatus(cached) == CONNECTION_OK) return cached;
            if (self.connections.fetchRemove(dsn)) |removed| {
                self.alloc.free(removed.key);
                self.pqfinish(removed.value);
            }
        }

        const dsn_z = try self.alloc.dupeSentinel(u8, dsn, 0);
        defer self.alloc.free(dsn_z);
        const conn = self.pqconnectdb(dsn_z.ptr) orelse return error.ForeignConnectionFailed;
        if (self.pqstatus(conn) != CONNECTION_OK) {
            self.logConnectFailure(conn, dsn);
            self.pqfinish(conn);
            return error.ForeignConnectionFailed;
        }
        const owned_dsn = try self.alloc.dupe(u8, dsn);
        errdefer self.alloc.free(owned_dsn);
        try self.connections.put(self.alloc, owned_dsn, conn);
        return conn;
    }

    /// Keep PostgreSQL's reason (for example "permission denied to start WAL
    /// sender" or "connection refused") in the Antfly log; the returned error
    /// only names the class. libpq's conninfo/URI parsers can echo raw
    /// credential text in their diagnostics -- an invalid percent-encoded
    /// byte inside a URI password, for instance, is reported by quoting that
    /// password token verbatim -- and `dsn` may already carry a secret
    /// resolved from the keystore, so the message is sanitized before it
    /// ever reaches the log rather than trusted to omit credentials.
    fn logConnectFailure(self: *@This(), conn: *PGconn, dsn: []const u8) void {
        const message = std.mem.trim(u8, std.mem.span(self.pqerrorMessage(conn)), " \t\r\n");
        if (message.len == 0) return;
        const sanitized = sanitizeConnectionErrorAlloc(self.alloc, message, dsn) catch {
            std.log.warn("postgres libpq connect failed: [redacted: could not sanitize connection error]", .{});
            return;
        };
        defer self.alloc.free(sanitized);
        std.log.warn("postgres libpq connect failed: {s}", .{sanitized});
    }

    fn connectWithDeadline(self: *@This(), dsn: []const u8, execution_deadline_ns: ?u64) !?*PGconn {
        const deadline_ns = execution_deadline_ns orelse return try self.connect(dsn);
        try ensureDeadline(deadline_ns);
        if (self.connections.get(dsn)) |cached| {
            if (self.pqstatus(cached) == CONNECTION_OK) return cached;
            self.invalidateConnection(dsn, cached);
        }

        const dsn_z = try self.alloc.dupeSentinel(u8, dsn, 0);
        defer self.alloc.free(dsn_z);
        const conn = self.pqconnectStart(dsn_z.ptr) orelse return error.ForeignConnectionFailed;
        errdefer self.pqfinish(conn);
        if (self.pqsetnonblocking(conn, 1) != 0) return error.ForeignConnectionFailed;

        while (true) {
            const polling_status = self.pqconnectPoll(conn);
            switch (polling_status) {
                0 => {
                    self.logConnectFailure(conn, dsn);
                    return error.ForeignConnectionFailed;
                },
                1 => try self.waitForSocket(conn, std.posix.POLL.IN, deadline_ns),
                2 => try self.waitForSocket(conn, std.posix.POLL.OUT, deadline_ns),
                3 => break,
                4 => {
                    try ensureDeadline(deadline_ns);
                    spinOrYield();
                },
                else => return error.ForeignConnectionFailed,
            }
        }
        if (self.pqstatus(conn) != CONNECTION_OK) {
            self.logConnectFailure(conn, dsn);
            return error.ForeignConnectionFailed;
        }
        if (self.pqsetnonblocking(conn, 0) != 0) return error.ForeignConnectionFailed;

        const owned_dsn = try self.alloc.dupe(u8, dsn);
        errdefer self.alloc.free(owned_dsn);
        try self.connections.put(self.alloc, owned_dsn, conn);
        return conn;
    }

    fn connectFreshWithDeadline(
        self: *@This(),
        dsn: []const u8,
        execution_deadline_ns: ?u64,
        cancellation: ?CancellationToken,
    ) !*PGconn {
        if (execution_deadline_ns == null and cancellation == null) {
            return (try self.connectFresh(self.alloc, dsn)) orelse error.ForeignConnectionFailed;
        }
        try ensureNotCancelled(cancellation);
        if (execution_deadline_ns) |deadline_ns| try ensureDeadline(deadline_ns);

        const dsn_z = try self.alloc.dupeSentinel(u8, dsn, 0);
        defer self.alloc.free(dsn_z);
        const conn = self.pqconnectStart(dsn_z.ptr) orelse return error.ForeignConnectionFailed;
        errdefer self.pqfinish(conn);
        if (self.pqsetnonblocking(conn, 1) != 0) return error.ForeignConnectionFailed;

        while (true) {
            try ensureNotCancelled(cancellation);
            switch (self.pqconnectPoll(conn)) {
                0 => {
                    self.logConnectFailure(conn, dsn);
                    return error.ForeignConnectionFailed;
                },
                1 => try self.waitForSocketCancellable(conn, std.posix.POLL.IN, execution_deadline_ns, cancellation),
                2 => try self.waitForSocketCancellable(conn, std.posix.POLL.OUT, execution_deadline_ns, cancellation),
                3 => break,
                4 => {
                    if (execution_deadline_ns) |deadline_ns| try ensureDeadline(deadline_ns);
                    spinOrYield();
                },
                else => return error.ForeignConnectionFailed,
            }
        }
        if (self.pqstatus(conn) != CONNECTION_OK) {
            self.logConnectFailure(conn, dsn);
            return error.ForeignConnectionFailed;
        }
        return conn;
    }

    fn invalidateConnection(self: *@This(), dsn: []const u8, conn: ?*PGconn) void {
        if (self.connections.get(dsn)) |cached| {
            if (cached == conn) {
                if (self.connections.fetchRemove(dsn)) |removed| {
                    self.alloc.free(removed.key);
                    self.pqfinish(removed.value);
                }
                return;
            }
        }
        self.pqfinish(conn);
    }

    fn waitForSocketEvents(self: *@This(), conn: ?*PGconn, events: i16, deadline_ns: u64, mode: enum { connect, query }) !i16 {
        const socket = self.pqsocket(conn);
        if (socket < 0) return error.ForeignConnectionFailed;
        const now_ns = platform_time.monotonicNs();
        if (now_ns >= deadline_ns) return error.Timeout;
        const remaining_ns = deadline_ns - now_ns;
        const remaining_ms = @max(@as(u64, 1), (remaining_ns +| std.time.ns_per_ms - 1) / std.time.ns_per_ms);
        const timeout_ms: i32 = @intCast(@min(remaining_ms, @as(u64, std.math.maxInt(i32))));
        var poll_fds = [_]std.posix.pollfd{.{
            .fd = socket,
            .events = events,
            .revents = 0,
        }};
        if (try std.posix.poll(&poll_fds, timeout_ms) == 0) return error.Timeout;
        const revents = poll_fds[0].revents;
        if (revents & std.posix.POLL.NVAL != 0) return error.ForeignConnectionFailed;
        // During connection setup libpq handles socket failures itself.
        // Poll it again so it can try the next address or configured host.
        if (mode == .query and revents & (std.posix.POLL.ERR | std.posix.POLL.HUP) != 0)
            return error.ForeignConnectionFailed;
        return poll_fds[0].revents;
    }

    fn waitForSocket(self: *@This(), conn: ?*PGconn, events: i16, deadline_ns: u64) !void {
        _ = try self.waitForSocketEvents(conn, events, deadline_ns, .query);
    }

    fn waitForSocketCancellable(
        self: *@This(),
        conn: ?*PGconn,
        events: i16,
        execution_deadline_ns: ?u64,
        cancellation: ?CancellationToken,
    ) !void {
        while (true) {
            try ensureNotCancelled(cancellation);
            const wait_deadline_ns = boundedCancellationWaitDeadline(
                execution_deadline_ns,
                cancellation,
            ) orelse unreachable;
            _ = self.waitForSocketEvents(conn, events, wait_deadline_ns, .connect) catch |err| {
                if (err != error.Timeout) return err;
                try ensureNotCancelled(cancellation);
                if (execution_deadline_ns) |deadline_ns| try ensureDeadline(deadline_ns);
                continue;
            };
            return;
        }
    }

    fn connectFresh(self: *@This(), alloc: Allocator, dsn: []const u8) !?*PGconn {
        const dsn_z = try alloc.dupeSentinel(u8, dsn, 0);
        defer alloc.free(dsn_z);
        const conn = self.pqconnectdb(dsn_z.ptr) orelse return error.ForeignConnectionFailed;
        if (self.pqstatus(conn) != CONNECTION_OK) {
            self.logConnectFailure(conn, dsn);
            self.pqfinish(conn);
            return error.ForeignConnectionFailed;
        }
        return conn;
    }

    fn connectCountedFresh(
        self: *@This(),
        alloc: Allocator,
        dsn: []const u8,
        execution_deadline_ns: ?u64,
    ) !*PGconn {
        try self.acquireGlobalConnectionPermits(1, execution_deadline_ns, null);
        errdefer self.releaseGlobalConnections(1);
        return if (execution_deadline_ns != null)
            try self.connectFreshWithDeadline(dsn, execution_deadline_ns, null)
        else
            (try self.connectFresh(alloc, dsn)) orelse error.ForeignConnectionFailed;
    }

    fn closeCountedConnection(self: *@This(), conn: ?*PGconn) void {
        self.pqfinish(conn);
        self.releaseGlobalConnections(1);
    }

    fn connectReplicationFresh(self: *@This(), alloc: Allocator, dsn: []const u8) !?*PGconn {
        const repl_dsn = try appendReplicationModeAlloc(alloc, dsn);
        defer alloc.free(repl_dsn);
        return try self.connectFresh(alloc, repl_dsn);
    }

    fn connectReplicationFreshWithDeadline(
        self: *@This(),
        alloc: Allocator,
        dsn: []const u8,
        execution_deadline_ns: u64,
    ) !*PGconn {
        const repl_dsn = try appendReplicationModeAlloc(alloc, dsn);
        defer alloc.free(repl_dsn);
        return try self.connectFreshWithDeadline(repl_dsn, execution_deadline_ns, null);
    }

    fn execPrepared(self: *@This(), conn: ?*PGconn, alloc: Allocator, prepared: sql.PreparedQuery) !?*PGresult {
        return try self.execPreparedInternal(conn, alloc, prepared, false);
    }

    fn execPreparedWithDeadline(
        self: *@This(),
        conn: ?*PGconn,
        alloc: Allocator,
        prepared: sql.PreparedQuery,
        execution_deadline_ns: ?u64,
        cancellation: ?CancellationToken,
    ) !?*PGresult {
        if (execution_deadline_ns == null and cancellation == null)
            return try self.execPrepared(conn, alloc, prepared);
        return try self.execPreparedWithDeadlineInternal(
            conn,
            alloc,
            prepared,
            execution_deadline_ns,
            false,
            cancellation,
        );
    }

    fn execPreparedAllowCommandWithDeadline(
        self: *@This(),
        conn: ?*PGconn,
        alloc: Allocator,
        prepared: sql.PreparedQuery,
        execution_deadline_ns: u64,
    ) !?*PGresult {
        return try self.execPreparedWithDeadlineInternal(conn, alloc, prepared, execution_deadline_ns, true, null);
    }

    fn execPreparedWithDeadlineInternal(
        self: *@This(),
        conn: ?*PGconn,
        alloc: Allocator,
        prepared: sql.PreparedQuery,
        deadline_ns: ?u64,
        allow_command_ok: bool,
        cancellation: ?CancellationToken,
    ) !?*PGresult {
        if (cancellation) |value| if (value.isCancelled()) return error.Cancelled;
        if (deadline_ns) |deadline| try ensureDeadline(deadline);
        var owned_args = try OwnedArgs.init(alloc, prepared.args);
        defer owned_args.deinit(alloc);
        const sql_text_z = try alloc.dupeSentinel(u8, prepared.sql_text, 0);
        defer alloc.free(sql_text_z);

        if (self.pqsetnonblocking(conn, 1) != 0) return error.ForeignQueryFailed;
        if (self.pqsendQueryParams(
            conn,
            sql_text_z.ptr,
            @intCast(prepared.args.len),
            null,
            if (owned_args.values.len > 0) owned_args.values.ptr else null,
            if (owned_args.lengths.len > 0) owned_args.lengths.ptr else null,
            if (owned_args.formats.len > 0) owned_args.formats.ptr else null,
            0,
        ) != 1) return error.ForeignQueryFailed;

        return try self.readAsyncResultWithDeadline(conn, deadline_ns, allow_command_ok, cancellation);
    }

    fn readAsyncResultWithDeadline(
        self: *@This(),
        conn: ?*PGconn,
        deadline_ns: ?u64,
        allow_command_ok: bool,
        cancellation: ?CancellationToken,
    ) !?*PGresult {
        const result = try readSingleAsyncResultWithDeadline(
            AsyncResultDriver{ .executor = self, .conn = conn },
            deadline_ns,
            cancellation,
        );
        const status = self.pqresultStatus(result);
        if (status != PGRES_TUPLES_OK and !(allow_command_ok and status == PGRES_COMMAND_OK)) {
            defer self.pqclear(result);
            return mapResultError(self.pqresultErrorField(result, PG_DIAG_SQLSTATE), self.pqresultErrorMessage(result));
        }
        return result;
    }

    fn execPreparedAllowCommand(self: *@This(), conn: ?*PGconn, alloc: Allocator, prepared: sql.PreparedQuery) !?*PGresult {
        return try self.execPreparedInternal(conn, alloc, prepared, true);
    }

    fn execPreparedInternal(self: *@This(), conn: ?*PGconn, alloc: Allocator, prepared: sql.PreparedQuery, allow_command_ok: bool) !?*PGresult {
        var owned_args = try OwnedArgs.init(alloc, prepared.args);
        defer owned_args.deinit(alloc);

        const sql_text_z = try alloc.dupeSentinel(u8, prepared.sql_text, 0);
        defer alloc.free(sql_text_z);

        const result = self.pqexecParams(
            conn,
            sql_text_z.ptr,
            @intCast(prepared.args.len),
            null,
            if (owned_args.values.len > 0) owned_args.values.ptr else null,
            if (owned_args.lengths.len > 0) owned_args.lengths.ptr else null,
            if (owned_args.formats.len > 0) owned_args.formats.ptr else null,
            0,
        ) orelse return error.ForeignQueryFailed;

        const status = self.pqresultStatus(result);
        if (status != PGRES_TUPLES_OK and !(allow_command_ok and status == PGRES_COMMAND_OK)) {
            defer self.pqclear(result);
            return mapResultError(self.pqresultErrorField(result, PG_DIAG_SQLSTATE), self.pqresultErrorMessage(result));
        }
        return result;
    }

    fn execSimple(self: *@This(), conn: ?*PGconn, alloc: Allocator, sql_text: []const u8) !?*PGresult {
        return try self.execSimpleInternal(conn, alloc, sql_text, false);
    }

    fn execSimpleAllowCommand(self: *@This(), conn: ?*PGconn, alloc: Allocator, sql_text: []const u8) !?*PGresult {
        return try self.execSimpleInternal(conn, alloc, sql_text, true);
    }

    fn execSimpleWithDeadline(
        self: *@This(),
        conn: ?*PGconn,
        alloc: Allocator,
        sql_text: []const u8,
        execution_deadline_ns: u64,
        allow_command_ok: bool,
    ) !?*PGresult {
        return self.execSimpleWithControl(conn, alloc, sql_text, execution_deadline_ns, allow_command_ok, null);
    }
    fn execSimpleWithControl(self: *@This(), conn: ?*PGconn, alloc: Allocator, sql_text: []const u8, execution_deadline_ns: u64, allow_command_ok: bool, cancellation: ?CancellationToken) !?*PGresult {
        if (cancellation) |token| try token.check();
        try ensureDeadline(execution_deadline_ns);
        const sql_text_z = try alloc.dupeSentinel(u8, sql_text, 0);
        defer alloc.free(sql_text_z);

        if (self.pqsetnonblocking(conn, 1) != 0) return error.ForeignQueryFailed;
        if (self.pqsendQuery(conn, sql_text_z.ptr) != 1) return error.ForeignQueryFailed;
        return try self.readAsyncResultWithDeadline(
            conn,
            execution_deadline_ns,
            allow_command_ok,
            cancellation,
        );
    }

    fn execSimpleInternal(self: *@This(), conn: ?*PGconn, alloc: Allocator, sql_text: []const u8, allow_command_ok: bool) !?*PGresult {
        const sql_text_z = try alloc.dupeSentinel(u8, sql_text, 0);
        defer alloc.free(sql_text_z);

        const result = self.pqexec(conn, sql_text_z.ptr) orelse return error.ForeignQueryFailed;
        const status = self.pqresultStatus(result);
        if (status != PGRES_TUPLES_OK and !(allow_command_ok and status == PGRES_COMMAND_OK)) {
            defer self.pqclear(result);
            return mapResultError(self.pqresultErrorField(result, PG_DIAG_SQLSTATE), self.pqresultErrorMessage(result));
        }
        return result;
    }

    fn readQueryResultAlloc(self: *@This(), alloc: Allocator, result: ?*PGresult) !foreign_source.QueryResult {
        return try self.readQueryResultAllocWithDeadline(alloc, result, null, null);
    }

    fn readQueryResultAllocWithDeadline(
        self: *@This(),
        alloc: Allocator,
        result: ?*PGresult,
        execution_deadline_ns: ?u64,
        cancellation: ?CancellationToken,
    ) !foreign_source.QueryResult {
        return self.readQueryResultWithProvenance(alloc, result, execution_deadline_ns, cancellation, false);
    }

    fn readQueryResultWithProvenance(
        self: *@This(),
        alloc: Allocator,
        result: ?*PGresult,
        execution_deadline_ns: ?u64,
        cancellation: ?CancellationToken,
        capture_json_nulls: bool,
    ) !foreign_source.QueryResult {
        try ensureNotCancelled(cancellation);
        const rows_len: usize = @intCast(self.pqntuples(result));
        const cols_len: usize = @intCast(self.pqnfields(result));
        const rows = try alloc.alloc(std.json.Value, rows_len);
        errdefer alloc.free(rows);

        var row_idx: usize = 0;
        errdefer {
            for (rows[0..row_idx]) |*row| foreign_source.deinitJsonValue(alloc, row);
        }
        const provenance: [][]const []const u8 = if (capture_json_nulls) try alloc.alloc([]const []const u8, rows_len) else &.{};
        var provenance_len: usize = 0;
        errdefer if (capture_json_nulls) {
            for (provenance[0..provenance_len]) |fields| {
                for (fields) |field| alloc.free(field);
                alloc.free(fields);
            }
            alloc.free(provenance);
        };
        var cells_until_deadline_check: u8 = 1;
        while (row_idx < rows_len) : (row_idx += 1) {
            try ensureNotCancelled(cancellation);
            if (execution_deadline_ns) |deadline_ns| try ensureDeadline(deadline_ns);
            var nulls: std.ArrayListUnmanaged([]const u8) = .empty;
            errdefer {
                for (nulls.items) |field| alloc.free(field);
                nulls.deinit(alloc);
            }
            var object = std.json.ObjectMap.empty;
            errdefer {
                var it = object.iterator();
                while (it.next()) |entry| {
                    alloc.free(@constCast(entry.key_ptr.*));
                    foreign_source.deinitJsonValue(alloc, entry.value_ptr);
                }
                object.deinit(alloc);
            }
            for (0..cols_len) |col_idx| {
                try checkResultDecodeProgress(
                    execution_deadline_ns,
                    cancellation,
                    &cells_until_deadline_check,
                );
                const col: c_int = @intCast(col_idx);
                const key_z = self.pqfname(result, col) orelse return error.ForeignQueryFailed;
                const key = try alloc.dupe(u8, std.mem.span(key_z));
                errdefer alloc.free(key);
                var value = if (self.pqgetisnull(result, @intCast(row_idx), col) != 0)
                    std.json.Value.null
                else
                    try parseCellValueAlloc(
                        alloc,
                        self.pqftype(result, col),
                        self.pqgetvalue(result, @intCast(row_idx), col),
                        @intCast(self.pqgetlength(result, @intCast(row_idx), col)),
                    );
                errdefer foreign_source.deinitJsonValue(alloc, &value);
                if (capture_json_nulls and value == .null and self.pqgetisnull(result, @intCast(row_idx), col) == 0) {
                    const oid = self.pqftype(result, col);
                    if (oid == TypeOid.json or oid == TypeOid.jsonb) {
                        const field = try alloc.dupe(u8, key);
                        errdefer alloc.free(field);
                        try nulls.append(alloc, field);
                    }
                }
                try object.put(alloc, key, value);
            }
            if (capture_json_nulls) {
                provenance[row_idx] = try nulls.toOwnedSlice(alloc);
                provenance_len += 1;
            }
            rows[row_idx] = .{ .object = object };
        }

        return .{
            .rows = rows,
            .json_null_fields = provenance,
            .total = rows_len,
        };
    }

    const VerifiedReplicationConnection = struct {
        executor: *Executor,
        conn: *PGconn,
        origin: foreign_source.ReplicationIdentity,
        fn deinit(self: *VerifiedReplicationConnection) void {
            self.executor.closeCountedConnection(self.conn);
        }
    };

    fn openVerifiedReplicationConnection(self: *@This(), a: Allocator, dsn: []const u8, table: []const u8, deadline: u64) !VerifiedReplicationConnection {
        const conn = try self.connectCountedFresh(a, dsn, deadline);
        errdefer self.closeCountedConnection(conn);
        return .{ .executor = self, .conn = conn, .origin = try self.sqlReplicationIdentityAlloc(a, conn, table, deadline) };
    }

    fn sqlProviderIdentityAlloc(self: *@This(), a: Allocator, conn: ?*PGconn, deadline: u64) ![32]u8 {
        return self.sqlProviderIdentityWithControl(a, conn, deadline, null);
    }
    fn sqlProviderIdentityWithControl(self: *@This(), a: Allocator, conn: ?*PGconn, deadline: u64, cancellation: ?CancellationToken) ![32]u8 {
        // Read identity on the exact SQL session that owns this operation.
        // Missing operator permission to inspect the real cluster identity fails
        // before snapshot/apply effects; no DSN or endpoint hash substitutes.
        const result = try self.execSimpleWithControl(conn, a,
            \\SELECT (pg_catalog.pg_control_system()).system_identifier::text,
            \\       pg_catalog.current_database(), oid::text
            \\FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()
        , deadline, false, cancellation);
        defer self.pqclear(result);
        if (self.pqntuples(result) != 1 or self.pqnfields(result) != 3) return error.ForeignProviderIdentityMismatch;
        for (0..3) |i| if (self.pqgetisnull(result, 0, @intCast(i)) != 0) return error.ForeignProviderIdentityMismatch;
        const system_id = self.pqgetvalue(result, 0, 0)[0..@intCast(self.pqgetlength(result, 0, 0))];
        const database = self.pqgetvalue(result, 0, 1)[0..@intCast(self.pqgetlength(result, 0, 1))];
        const database_oid = self.pqgetvalue(result, 0, 2)[0..@intCast(self.pqgetlength(result, 0, 2))];
        if (system_id.len == 0 or database.len == 0 or database_oid.len == 0) return error.ForeignProviderIdentityMismatch;
        return exactCutoverProviderIdentity(system_id, database, database_oid);
    }

    fn sqlReplicationIdentityAlloc(self: *@This(), a: Allocator, conn: ?*PGconn, table: []const u8, deadline: u64) !foreign_source.ReplicationIdentity {
        return .{ .provider_identity = try self.sqlProviderIdentityAlloc(a, conn, deadline), .relation_id = try self.pollRelationIdentityAlloc(a, conn, table, deadline) };
    }
    fn sqlReplicationIdentityWithControl(self: *@This(), a: Allocator, conn: ?*PGconn, table: []const u8, deadline: u64, cancellation: ?CancellationToken) !foreign_source.ReplicationIdentity {
        return .{ .provider_identity = try self.sqlProviderIdentityWithControl(a, conn, deadline, cancellation), .relation_id = try self.pollRelationIdentityWithControl(a, conn, table, deadline, cancellation) };
    }

    fn pollRelationIdentityAlloc(self: *@This(), a: Allocator, conn: ?*PGconn, table: []const u8, deadline: u64) !u32 {
        return self.pollRelationIdentityWithControl(a, conn, table, deadline, null);
    }
    fn pollRelationIdentityWithControl(self: *@This(), a: Allocator, conn: ?*PGconn, table: []const u8, deadline: u64, cancellation: ?CancellationToken) !u32 {
        const args = try a.alloc(sql.ParameterValue, 1);
        args[0] = .{ .string = try sql.quotePostgresRelationAlloc(a, table) };
        var request: sql.PreparedQuery = .{ .sql_text = try a.dupe(u8, "SELECT $1::pg_catalog.regclass::oid::text"), .args = args };
        defer request.deinit(a);
        const result = try self.execPreparedWithDeadline(conn, a, request, deadline, cancellation);
        defer self.pqclear(result);
        if (self.pqntuples(result) != 1 or self.pqnfields(result) != 1 or self.pqgetisnull(result, 0, 0) != 0) return error.ForeignTableNotFound;
        const bytes = self.pqgetvalue(result, 0, 0)[0..@intCast(self.pqgetlength(result, 0, 0))];
        const id = std.fmt.parseInt(u32, bytes, 10) catch return error.ForeignTableNotFound;
        if (id == 0) return error.ForeignTableNotFound;
        return id;
    }

    fn logicalLsn(text: []const u8) !u64 {
        const slash = std.mem.indexOfScalar(u8, text, '/') orelse return error.InvalidReplicationTransaction;
        const high = std.fmt.parseInt(u32, text[0..slash], 16) catch return error.InvalidReplicationTransaction;
        const low = std.fmt.parseInt(u32, text[slash + 1 ..], 16) catch return error.InvalidReplicationTransaction;
        return (@as(u64, high) << 32) | low;
    }

    const PublicationBinding = struct {
        relation: u32,
        fingerprint: [32]u8,

        fn checkpoint(self: PublicationBinding, a: Allocator, position: []const u8) ![]u8 {
            if (position.len == 0) return a.dupe(u8, position);
            _ = try logicalLsn(position);
            return std.fmt.allocPrint(a, "pg1:{s}:{s}", .{ std.fmt.bytesToHex(self.fingerprint, .lower), position });
        }

        fn lsn(self: PublicationBinding, value: []const u8) ![]const u8 {
            const prefix = "pg1:";
            if (!std.mem.startsWith(u8, value, prefix)) {
                _ = try logicalLsn(value);
                return value;
            }
            const hash_end = prefix.len + self.fingerprint.len * 2;
            if (value.len <= hash_end or value[hash_end] != ':') return error.InvalidReplicationCheckpoint;
            const expected = std.fmt.bytesToHex(self.fingerprint, .lower);
            if (!std.mem.eql(u8, value[prefix.len..hash_end], &expected)) return error.ReplicationPublicationBindingChanged;
            const position = value[hash_end + 1 ..];
            _ = try logicalLsn(position);
            return position;
        }
    };

    fn publicationBinding(
        self: *@This(),
        alloc: Allocator,
        conn: ?*PGconn,
        publication: []const u8,
        table: []const u8,
        allow_row_filter: bool,
        deadline_ns: ?u64,
    ) !PublicationBinding {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const quoted_table = try sql.postgresDialect().quote_relation(a, table);
        const args = try a.alloc(sql.ParameterValue, 3);
        args[0] = .{ .string = try a.dupe(u8, publication) };
        args[1] = .{ .string = quoted_table };
        args[2] = .{ .bool = allow_row_filter };
        var prepared = sql.PreparedQuery{
            .sql_text = try a.dupe(u8,
                \\SELECT CASE WHEN c.oid = pg_catalog.to_regclass($2::text)
                \\             AND ($3::boolean OR (pg_catalog.to_jsonb(t) ->> 'rowfilter') IS NULL)
                \\            THEN c.oid::text END,
                \\       pg_catalog.to_jsonb(t) ->> 'rowfilter',
                \\       p.pubviaroot::text
                \\  FROM pg_catalog.pg_publication p
                \\  JOIN pg_catalog.pg_publication_tables t ON t.pubname = p.pubname
                \\  JOIN pg_catalog.pg_namespace n ON n.nspname = t.schemaname
                \\  JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = t.tablename
                \\ WHERE p.pubname = $1 AND p.pubinsert AND p.pubupdate AND p.pubdelete
                \\   AND NOT p.puballtables
                \\   AND NOT EXISTS (
                \\       SELECT 1 FROM pg_catalog.pg_publication_rel r
                \\        WHERE r.prpubid = p.oid AND (pg_catalog.to_jsonb(r) ->> 'prattrs') IS NOT NULL
                \\   )
                \\ LIMIT 2
            ),
            .args = args,
        };
        defer prepared.deinit(a);
        // Compare the entire membership before advancing the slot. Filtering
        // the catalog query to the configured table would hide extra members.
        const result = try self.execPreparedWithDeadline(conn, alloc, prepared, deadline_ns, null);
        defer self.pqclear(result);
        if (self.pqntuples(result) != 1 or self.pqgetisnull(result, 0, 0) != 0)
            return error.ReplicationPublicationBindingChanged;
        const actual = self.pqgetvalue(result, 0, 0)[0..@intCast(self.pqgetlength(result, 0, 0))];
        const oid = std.fmt.parseInt(u32, actual, 10) catch return error.ReplicationPublicationBindingChanged;
        // Bind the deparsed expression to durable progress. Recreating a
        // publication with the same relation, projection and expression keeps
        // this identity. Changing the row filter or table incarnation does not.
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:postgres-publication-binding:v1");
        var encoded: [4]u8 = undefined;
        std.mem.writeInt(u32, &encoded, oid, .big);
        hash.update(&encoded);
        for (1..3) |column| {
            const col: c_int = @intCast(column);
            if (self.pqgetisnull(result, 0, col) != 0) {
                hash.update(&.{0});
                continue;
            }
            hash.update(&.{1});
            const cell = self.pqgetvalue(result, 0, col)[0..@intCast(self.pqgetlength(result, 0, col))];
            std.mem.writeInt(u32, &encoded, @intCast(cell.len), .big);
            hash.update(&encoded);
            hash.update(cell);
        }
        return .{ .relation = oid, .fingerprint = hash.finalResult() };
    }

    fn pollChangesAlloc(self: *@This(), alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPollResult {
        const slot_name = params.slot_name orelse return error.InvalidQueryRequest;
        const publication_name = params.publication_name orelse return error.InvalidQueryRequest;
        std.log.info("postgres libpq poll begin table={s} slot={s}", .{ params.table, slot_name });
        const deadline = params.execution_deadline_ns orelse platform_time.monotonicNs() +| 30 * std.time.ns_per_s;
        var verified = try self.openVerifiedReplicationConnection(alloc, dsn, params.table, deadline);
        defer verified.deinit();
        const conn = verified.conn;
        try params.observeInitializationOrigin(verified.origin);
        const provider_identity = verified.origin.provider_identity;
        const source_relation_id = verified.origin.relation_id;
        std.log.info("postgres libpq poll connected table={s} slot={s}", .{ params.table, slot_name });
        if (params.expected_provider_identity) |expected| {
            if (!std.meta.eql(expected, provider_identity)) return error.ReplicationCutoverAuthorityLost;
        }
        var observed_checkpoint: ?[]u8 = null;
        errdefer if (observed_checkpoint) |value| alloc.free(value);
        try self.ensurePublicationAlloc(
            alloc,
            conn,
            publication_name,
            params.table,
            params.filter_query_json,
            if (params.checkpoint) |saved| !std.mem.startsWith(u8, saved, "pg1:") else true,
            deadline,
        );
        const binding = try self.publicationBinding(alloc, conn, publication_name, params.table, params.filter_query_json != null, deadline);
        if (binding.relation != source_relation_id) return error.ReplicationPublicationBindingChanged;
        if (params.checkpoint) |saved| {
            if (saved.len > 0) {
                const checkpoint = try binding.lsn(saved);
                const confirmed = try self.loadLogicalReplicationSlotCheckpointAlloc(alloc, conn, slot_name, deadline);
                defer alloc.free(confirmed);
                if (confirmed.len == 0) return error.ForeignReplicationSlotMissing;
                const durable_lsn = try logicalLsn(checkpoint);
                const confirmed_lsn = try logicalLsn(confirmed);
                if (confirmed_lsn > durable_lsn) return error.ReplicationCutoverAuthorityLost;
                const advanced_checkpoint = if (confirmed_lsn == durable_lsn)
                    confirmed
                else
                    try self.advanceLogicalReplicationSlotCheckpointAlloc(
                        alloc,
                        conn,
                        slot_name,
                        checkpoint,
                        deadline,
                    );
                defer if (confirmed_lsn != durable_lsn) alloc.free(advanced_checkpoint);
                observed_checkpoint = try binding.checkpoint(alloc, advanced_checkpoint);
                if (try logicalLsn(checkpoint) != try logicalLsn(advanced_checkpoint))
                    return error.ReplicationCutoverAuthorityLost;
            }
        } else {
            _ = try self.ensureLogicalReplicationSlotAlloc(
                alloc,
                conn,
                slot_name,
                deadline,
            );
        }

        if (params.checkpoint == null) {
            const confirmed = try self.loadLogicalReplicationSlotCheckpointAlloc(alloc, conn, slot_name, deadline);
            defer alloc.free(confirmed);
            if (confirmed.len == 0) return error.ForeignReplicationSlotMissing;
        }
        var poll = try self.pollLogicalSlotAlloc(alloc, conn, slot_name, publication_name, params.limit orelse 256, deadline, null, binding);
        errdefer poll.deinit(alloc);
        const observed_binding = try self.publicationBinding(alloc, conn, publication_name, params.table, params.filter_query_json != null, deadline);
        if (!std.meta.eql(binding, observed_binding)) return error.ReplicationPublicationBindingChanged;
        if (poll.checkpoint.len == 0) if (observed_checkpoint) |checkpoint| {
            alloc.free(poll.checkpoint);
            poll.checkpoint = checkpoint;
            observed_checkpoint = null;
        };
        if (observed_checkpoint) |checkpoint| alloc.free(checkpoint);
        poll.provider_identity = provider_identity;
        poll.source_relation_id = source_relation_id;
        return poll;
    }

    fn subscriptionPublicationNames(a: Allocator, names: []const []const u8, sql_literals: bool) ![]u8 {
        var result: std.ArrayListUnmanaged(u8) = .empty;
        errdefer result.deinit(a);
        for (names, 0..) |name, i| {
            const quoted = if (sql_literals) try quoteSqlStringLiteralAlloc(a, name) else try sql.postgresDialect().quote_identifier(a, name);
            defer a.free(quoted);
            if (i != 0) try result.append(a, ',');
            try result.appendSlice(a, quoted);
        }
        return result.toOwnedSlice(a);
    }

    fn subscriptionOriginAlloc(self: *@This(), a: Allocator, conn: ?*PGconn, request: foreign_source.SubscriptionRequest) !foreign_source.SubscriptionOrigin.Observation {
        const names = try subscriptionPublicationNames(a, request.publications, true);
        defer a.free(names);
        const text = try std.fmt.allocPrint(a,
            \\WITH members AS MATERIALIZED (
            \\ SELECT c.oid, n.nspname, c.relname, c.relreplident, pt.attnames, pt.rowfilter
            \\ FROM pg_catalog.pg_publication_tables pt
            \\ JOIN pg_catalog.pg_namespace n ON n.nspname=pt.schemaname
            \\ JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname=pt.tablename
            \\ WHERE pt.pubname IN ({s})
            \\), relation_groups AS (
            \\ SELECT oid,nspname,relname,relreplident,pg_catalog.min(attnames::text)::pg_catalog.name[] AS attnames,
            \\ CASE WHEN pg_catalog.bool_or(rowfilter IS NULL) THEN NULL ELSE pg_catalog.string_agg(DISTINCT '('||rowfilter||')',' OR ' ORDER BY '('||rowfilter||')') END AS row_filter,
            \\ pg_catalog.count(DISTINCT attnames::text)=1 AS same_columns
            \\ FROM members GROUP BY oid,nspname,relname,relreplident
            \\), relation_rows AS (
            \\ SELECT m.oid, pg_catalog.jsonb_build_object(
            \\ 'id',m.oid::bigint,'namespace',m.nspname,'name',m.relname,
            \\ 'replica_identity',CASE m.relreplident WHEN 'd' THEN 'default' WHEN 'n' THEN 'nothing' WHEN 'f' THEN 'full' WHEN 'i' THEN 'index' END,
            \\ 'row_filter',m.row_filter,
            \\ 'columns',(SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
            \\   'name',a.attname,'type_oid',a.atttypid::bigint,'type_modifier',a.atttypmod,
            \\   'key',(m.relreplident='f' OR EXISTS(SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid=m.oid AND ((m.relreplident='i' AND i.indisreplident) OR (m.relreplident='d' AND i.indisprimary)) AND a.attnum=ANY((i.indkey::smallint[])[0:i.indnkeyatts-1])))) ORDER BY a.attnum)
            \\   FROM pg_catalog.pg_attribute a WHERE a.attrelid=m.oid AND a.attnum>0 AND NOT a.attisdropped AND a.attname=ANY(m.attnames))
            \\ ) AS body,m.same_columns
            \\ FROM relation_groups m
            \\), packed AS (
            \\ SELECT COALESCE(pg_catalog.jsonb_agg(body ORDER BY oid),'[]'::jsonb)::text AS body, COALESCE(pg_catalog.bool_and(same_columns),true) AS compatible FROM relation_rows
            \\)
            \\SELECT (SELECT pg_catalog.count(*) FROM pg_catalog.pg_publication WHERE pubname IN ({s}))::text,
            \\ CASE WHEN compatible AND pg_catalog.octet_length(body)<={d} THEN body ELSE NULL END FROM packed
        , .{ names, names, foreign_source.SubscriptionOrigin.max_encoded_bytes });
        defer a.free(text);
        const result = try self.execSimpleWithDeadline(conn, a, text, request.execution_deadline_ns, false);
        defer self.pqclear(result);
        if (self.pqntuples(result) != 1 or self.pqnfields(result) != 2 or self.pqgetisnull(result, 0, 0) != 0 or self.pqgetisnull(result, 0, 1) != 0) return error.InvalidLogicalReplicationOrigin;
        const count = std.fmt.parseInt(usize, self.pqgetvalue(result, 0, 0)[0..@intCast(self.pqgetlength(result, 0, 0))], 10) catch return error.InvalidLogicalReplicationOrigin;
        if (count != request.publications.len) return error.ForeignPublicationNotFound;
        const bytes = self.pqgetvalue(result, 0, 1)[0..@intCast(self.pqgetlength(result, 0, 1))];
        if (bytes.len > foreign_source.SubscriptionOrigin.max_encoded_bytes) return error.ProgramLimitExceeded;
        const parsed = try std.json.parseFromSlice([]const foreign_source.SubscriptionOrigin.Relation, a, bytes, .{ .allocate = .alloc_always });
        defer parsed.deinit();
        const observed: foreign_source.SubscriptionOrigin.Observation = .{ .provider_identity = try self.sqlProviderIdentityAlloc(a, conn, request.execution_deadline_ns), .relations = parsed.value };
        return observed.clone(a);
    }

    fn requireExplicitSubscriptionPassword(self: *@This(), a: Allocator, dsn: []const u8) !void {
        const parse = self.pqconninfoParse orelse return error.ForeignAuthFailed;
        const free = self.pqconninfoFree orelse return error.ForeignAuthFailed;
        if (std.mem.indexOfScalar(u8, dsn, 0) != null) return error.ForeignAuthFailed;
        const text = try a.dupeSentinel(u8, dsn, 0);
        defer a.free(text);
        const options = parse(text.ptr, null) orelse return error.ForeignAuthFailed;
        defer free(options);
        var index: usize = 0;
        while (options[index].keyword) |keyword| : (index += 1) {
            if (!std.mem.eql(u8, std.mem.span(keyword), "password")) continue;
            const password = options[index].value orelse return error.ForeignAuthFailed;
            if (password[0] != 0) return;
            break;
        }
        return error.ForeignAuthFailed;
    }

    fn requireSubscriptionPasswordUsed(self: *@This(), conn: ?*PGconn) !void {
        const used = self.pqconnectionUsedPassword orelse return error.ForeignAuthFailed;
        if (used(conn) != 1) return error.ForeignAuthFailed;
    }

    fn pollSubscription(ptr: *anyopaque, a: Allocator, dsn: []const u8, request: foreign_source.SubscriptionRequest) !foreign_source.SubscriptionPollResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try request.validate(a);
        if (request.password_required) try self.requireExplicitSubscriptionPassword(a, dsn);
        if (request.expected == null) return error.ForeignProviderIdentityMismatch;
        if (request.advance_durable_checkpoint) {
            try request.authority.acknowledged(request.checkpoint orelse return error.InvalidQueryRequest);
        } else {
            try request.authority.check();
        }
        const conn = try self.connectCountedFresh(a, dsn, request.execution_deadline_ns);
        defer self.closeCountedConnection(conn);
        if (request.password_required) try self.requireSubscriptionPasswordUsed(conn);
        try self.setCutoverAdvisoryLockAlloc(a, conn, request.lock_name, true, request.execution_deadline_ns);
        var origin = try self.subscriptionOriginAlloc(a, conn, request);
        errdefer origin.deinit(a);
        try request.observe(a, origin);
        const confirmed = try self.requireSubscriptionSlotAlloc(a, conn, request.slot_name, request.execution_deadline_ns);
        // A recreated/externally advanced slot cannot silently skip a gap.
        const durable = try foreign_source.requireSubscriptionCheckpoint(confirmed, request.checkpoint);
        if (request.advance_durable_checkpoint and confirmed < durable) {
            const checkpoint = request.checkpoint.?;
            try request.authority.acknowledged(checkpoint);
            const actual = try self.advanceLogicalReplicationSlotCheckpointAlloc(a, conn, request.slot_name, checkpoint, request.execution_deadline_ns);
            defer a.free(actual);
            if (!std.mem.eql(u8, actual, checkpoint)) return error.ForeignProviderIdentityMismatch;
        }
        const publications = try subscriptionPublicationNames(a, request.publications, false);
        defer a.free(publications);
        var poll = try self.pollLogicalSlotAlloc(a, conn, request.slot_name, publications, request.limit, request.execution_deadline_ns, origin, null);
        errdefer poll.deinit(a);
        // The decoder owns transaction framing; provider identity belongs to
        // the verified connection/roster above, including empty polls. Keep
        // source_relation_id unset because one subscription spans relations.
        poll.provider_identity = origin.provider_identity;
        try request.authority.check();
        try ensureDeadline(request.execution_deadline_ns);
        return .{ .origin = origin, .poll = poll };
    }

    fn acknowledgeSubscription(ptr: *anyopaque, a: Allocator, dsn: []const u8, request: foreign_source.SubscriptionRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try request.validate(a);
        if (request.password_required) try self.requireExplicitSubscriptionPassword(a, dsn);
        if (request.expected == null) return error.ForeignProviderIdentityMismatch;
        const checkpoint = request.checkpoint orelse return error.InvalidQueryRequest;
        try request.authority.acknowledged(checkpoint);
        const conn = try self.connectCountedFresh(a, dsn, request.execution_deadline_ns);
        defer self.closeCountedConnection(conn);
        if (request.password_required) try self.requireSubscriptionPasswordUsed(conn);
        try self.setCutoverAdvisoryLockAlloc(a, conn, request.lock_name, true, request.execution_deadline_ns);
        var origin = try self.subscriptionOriginAlloc(a, conn, request);
        defer origin.deinit(a);
        try request.observe(a, origin);
        // Rejoin the actual native checkpoint AFTER acquiring the same slot
        // lock, before the sole upstream acknowledgement effect.
        try request.authority.acknowledged(checkpoint);
        const confirmed = try self.requireSubscriptionSlotAlloc(a, conn, request.slot_name, request.execution_deadline_ns);
        // A recreated/externally advanced slot cannot silently skip a gap.
        _ = try foreign_source.requireSubscriptionCheckpoint(confirmed, request.checkpoint);
        const actual = try self.advanceLogicalReplicationSlotCheckpointAlloc(a, conn, request.slot_name, checkpoint, request.execution_deadline_ns);
        defer a.free(actual);
        if (!std.mem.eql(u8, actual, checkpoint)) return error.ForeignProviderIdentityMismatch;
        try request.authority.check();
        try ensureDeadline(request.execution_deadline_ns);
    }

    fn beginSubscriptionSnapshot(ptr: *anyopaque, a: Allocator, dsn: []const u8, request: foreign_source.SubscriptionRequest) !postgres_source.QueryExecutor.PreparedSubscriptionSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try request.validate(a);
        if (request.password_required) try self.requireExplicitSubscriptionPassword(a, dsn);
        if (request.checkpoint != null) return error.InvalidQueryRequest;
        try request.authority.check();
        // One exported slot snapshot for the entire roster. Reserve both
        // connections before opening either, as the ordinary cutover does.
        try self.acquireGlobalConnectionPermits(2, request.execution_deadline_ns, null);
        var owned_permits: usize = 2;
        errdefer self.releaseGlobalConnections(owned_permits);
        var sql_conn: ?*PGconn = try self.connectFreshWithDeadline(dsn, request.execution_deadline_ns, null);
        errdefer if (sql_conn) |conn| self.pqfinish(conn);
        if (request.password_required) try self.requireSubscriptionPasswordUsed(sql_conn);
        try self.setCutoverAdvisoryLockAlloc(a, sql_conn, request.lock_name, true, request.execution_deadline_ns);
        var repl_conn: ?*PGconn = try self.connectReplicationFreshWithDeadline(a, dsn, request.execution_deadline_ns);
        errdefer if (repl_conn) |conn| self.pqfinish(conn);
        if (request.password_required) try self.requireSubscriptionPasswordUsed(repl_conn);
        const provider = try self.identifyExactCutoverProvider(a, sql_conn, repl_conn, request.execution_deadline_ns);
        try self.lockSubscriptionSlotCreation(a, repl_conn, request.slot_name, request.execution_deadline_ns);
        var before = try self.subscriptionOriginAlloc(a, sql_conn, request);
        defer before.deinit(a);
        if (!std.mem.eql(u8, &provider, &before.provider_identity)) return error.ForeignProviderIdentityMismatch;
        // Do not publish native slot ownership until this invocation actually
        // creates the slot. A pre-existing pgoutput slot is not our artifact.
        // Existing slots cannot reproduce an already-lost exported snapshot.
        // Native recovery must retain/resume its actual captured pages or
        // explicitly retire the old birth before proposing another one.
        if (try self.logicalReplicationSlotExistsAlloc(a, sql_conn, request.slot_name, request.execution_deadline_ns)) return error.UnsupportedExactCutover;
        const exported = try self.createLogicalReplicationSlotExportResult(a, repl_conn, request.slot_name, request.execution_deadline_ns, .{ .authority = request.authority, .locked_observer = sql_conn, .provider_identity = provider });
        defer self.pqclear(exported);
        const checkpoint = self.pqgetvalue(exported, 0, 1)[0..@intCast(self.pqgetlength(exported, 0, 1))];
        const snapshot_name = self.pqgetvalue(exported, 0, 2)[0..@intCast(self.pqgetlength(exported, 0, 2))];
        var imported_transaction = false;
        errdefer {
            // Keep the exact cut available for cleanup despite allocation failure.
            if (repl_conn) |conn| {
                self.pqfinish(conn);
                repl_conn = null;
            }
            self.abortSubscriptionExport(self.alloc, sql_conn, request.slot_name, checkpoint, imported_transaction) catch {};
        }
        // CREATE succeeded on our retained replication connection. Only now
        // may observation bind this slot to the durable native preparation.
        try request.observe(a, before);
        try request.authority.exported(provider, try foreign_source.parseReplicationCheckpoint(checkpoint));
        const begin = try self.execSimpleWithDeadline(sql_conn, a, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", request.execution_deadline_ns, true);
        self.pqclear(begin);
        imported_transaction = true;
        const quoted = try quoteSqlStringLiteralAlloc(a, snapshot_name);
        defer a.free(quoted);
        const import_sql = try std.fmt.allocPrint(a, "SET TRANSACTION SNAPSHOT {s}", .{quoted});
        defer a.free(import_sql);
        const imported = try self.execSimpleWithDeadline(sql_conn, a, import_sql, request.execution_deadline_ns, true);
        self.pqclear(imported);
        // Pin EVERY roster relation before returning the cut. Locking each
        // table only on its first page would allow ALTER/DROP of a later
        // relation after READY's immutable roster had already been captured.
        // Sorted provider OIDs give all subscription readers one lock order.
        for (before.relations) |relation| {
            const namespace = try sql.postgresDialect().quote_identifier(a, relation.namespace);
            defer a.free(namespace);
            const name = try sql.postgresDialect().quote_identifier(a, relation.name);
            defer a.free(name);
            const table = try std.fmt.allocPrint(a, "{s}.{s}", .{ namespace, name });
            defer a.free(table);
            const lock_sql = try std.fmt.allocPrint(a, "LOCK TABLE {s} IN ACCESS SHARE MODE", .{table});
            defer a.free(lock_sql);
            const locked = try self.execSimpleWithDeadline(sql_conn, a, lock_sql, request.execution_deadline_ns, true);
            self.pqclear(locked);
            if (try self.pollRelationIdentityAlloc(a, sql_conn, table, request.execution_deadline_ns) != relation.id)
                return error.ForeignProviderIdentityMismatch;
            try request.authority.check();
        }
        // The imported SQL transaction sees its exported MVCC cut. Rejoin
        // CURRENT catalog facts on the still-owned logical replication
        // connection after ALL locks are held, otherwise a pre-lock ALTER
        // could be hidden by that older catalog snapshot. Logical replication
        // mode supports simple SQL; the exported token is no longer needed
        // after successful import, so consuming it here does not reopen a cut.
        var origin = try self.subscriptionOriginAlloc(a, repl_conn, request);
        errdefer origin.deinit(a);
        if (!std.mem.eql(u8, &try before.digest(a), &try origin.digest(a))) return error.ForeignProviderIdentityMismatch;
        try request.observe(a, origin);
        try ensureDeadline(request.execution_deadline_ns);
        const reader = try a.create(SnapshotQuery);
        errdefer a.destroy(reader);
        const owned_checkpoint = try a.dupe(u8, checkpoint);
        errdefer a.free(owned_checkpoint);
        const token = try a.dupe(u8, snapshot_name);
        errdefer a.free(token);
        const owned_slot = try a.dupe(u8, request.slot_name);
        reader.* = .{ .executor = self, .conn = sql_conn, .replication_identity_enabled = true, .verified_provider_identity = provider, .multi_relation = true, .created_slot = .{ .name = owned_slot, .checkpoint = owned_checkpoint } };
        self.pqfinish(repl_conn);
        repl_conn = null;
        self.releaseGlobalConnections(1);
        owned_permits = 0;
        sql_conn = null;
        return .{ .origin = origin, .checkpoint = owned_checkpoint, .snapshot_token = token, .snapshot_query = reader.asSnapshotQuery() };
    }

    fn abortSubscriptionExport(self: *@This(), a: Allocator, conn: ?*PGconn, slot_name: []const u8, checkpoint: []const u8, imported_transaction: bool) !void {
        _ = try foreign_source.parseReplicationCheckpoint(checkpoint);
        // This connection still owns the slot lock and has the exact export
        // result. A canceled or revoked attempt must still undo its own slot.
        const deadline = platform_time.monotonicNs() +| 2 * std.time.ns_per_s;
        // Release the imported transaction while retaining the session lock.
        if (imported_transaction) {
            const rolled_back = try self.execSimpleWithDeadline(conn, a, "ROLLBACK", deadline, true);
            self.pqclear(rolled_back);
        }
        try self.dropSubscriptionSlotAtCheckpoint(a, conn, slot_name, checkpoint, deadline);
    }

    fn dropSubscriptionSlotAtCheckpoint(self: *@This(), a: Allocator, conn: ?*PGconn, slot_name: []const u8, checkpoint: []const u8, deadline: u64) !void {
        const statement =
            "SELECT pg_catalog.pg_drop_replication_slot(slot_name) " ++
            "FROM pg_catalog.pg_replication_slots WHERE slot_name = $1 " ++
            "AND database = pg_catalog.current_database() AND slot_type = 'logical' " ++
            "AND plugin = 'pgoutput' AND NOT temporary AND NOT active " ++
            "AND confirmed_flush_lsn = $2::pg_catalog.pg_lsn";
        const prepared: sql.PreparedQuery = .{
            .sql_text = @constCast(statement),
            .args = @constCast(&[_]sql.ParameterValue{ .{ .string = @constCast(slot_name) }, .{ .string = @constCast(checkpoint) } }),
        };
        const dropped = try self.execPreparedWithDeadline(conn, a, prepared, deadline, null);
        self.pqclear(dropped);
    }

    fn pollLogicalSlotAlloc(self: *@This(), alloc: Allocator, conn: ?*PGconn, slot_name: []const u8, publications: []const u8, limit: usize, deadline: u64, expected: ?foreign_source.SubscriptionOrigin.Observation, binding: ?PublicationBinding) !foreign_source.ReplicationPollResult {
        const quoted_slot_name = try quoteSqlStringLiteralAlloc(alloc, slot_name);
        defer alloc.free(quoted_slot_name);
        const quoted_publications = try quoteSqlStringLiteralAlloc(alloc, publications);
        defer alloc.free(quoted_publications);
        // SQL projects a bounded bytea slice BEFORE libpq sees a DataRow.
        // PQ single-row mode alone still buffers one arbitrarily large bytea.
        // Ordinal+offset carry physical framing only; source commit boundaries
        // remain the pgoutput B/C envelope and are never acknowledged here.
        const request = try std.fmt.allocPrint(alloc,
            \\WITH messages AS MATERIALIZED (
            \\ SELECT lsn::text AS lsn, data, ordinality AS ordinal,
            \\        pg_catalog.octet_length(data) AS bytes
            \\ FROM pg_catalog.pg_logical_slot_peek_binary_changes({s}, NULL, {d}, 'proto_version', '2', 'publication_names', {s}) WITH ORDINALITY
            \\), measured AS (
            \\ SELECT *, pg_catalog.sum(bytes) OVER (ORDER BY ordinal ROWS UNBOUNDED PRECEDING) AS used
            \\ FROM messages
            \\), page AS (
            \\ SELECT *, used <= {d} AND ordinal <= {d} AS admitted
            \\ FROM measured WHERE used - bytes <= {d} AND ordinal <= {d}
            \\)
            \\SELECT lsn, ordinal::text, bytes::text, offsets.chunk_offset::text,
            \\       CASE WHEN admitted THEN pg_catalog.substring(data, offsets.chunk_offset + 1, {d}) END
            \\FROM page CROSS JOIN LATERAL pg_catalog.generate_series(0, CASE WHEN admitted THEN GREATEST(bytes - 1, 0) ELSE 0 END, {d}) AS offsets(chunk_offset)
            \\ORDER BY ordinal, offsets.chunk_offset
        , .{ quoted_slot_name, limit, quoted_publications, replication_poll_bytes, replication_poll_messages, replication_poll_bytes, replication_poll_messages + 1, replication_wire_chunk_bytes, replication_wire_chunk_bytes });
        defer alloc.free(request);
        try self.startReplicationChunkQuery(alloc, conn, request, deadline);

        var relation_cache = std.AutoHashMapUnmanaged(u32, PgoutputRelation).empty;
        defer deinitPgoutputRelationCache(alloc, &relation_cache);

        var decoded: PgoutputTransactions = .{ .binding = binding };
        defer decoded.deinit(alloc);
        var lag_millis: u64 = 0;
        const poll_now_ms: i64 = @intCast(currentRealtimeMillis());

        var framing: ReplicationChunkFraming = .{};
        var message_bytes: std.ArrayListUnmanaged(u8) = .empty;
        defer message_bytes.deinit(alloc);
        var message_lsn: std.ArrayListUnmanaged(u8) = .empty;
        defer message_lsn.deinit(alloc);
        var completed_result = false;
        var truncated = false;
        while (try self.nextReplicationChunkResult(conn, deadline)) |result| {
            defer self.pqclear(result);
            const status = self.pqresultStatus(result);
            if (status == PGRES_TUPLES_OK) {
                if (completed_result or self.pqntuples(result) != 0 or self.pqnfields(result) != 5) return error.ForeignQueryFailed;
                completed_result = true;
                continue;
            }
            if (status != PGRES_SINGLE_TUPLE) return mapResultError(self.pqresultErrorField(result, PG_DIAG_SQLSTATE), self.pqresultErrorMessage(result));
            if (completed_result or truncated or self.pqntuples(result) != 1 or self.pqnfields(result) != 5) return error.ForeignQueryFailed;
            for (0..4) |column| if (self.pqgetisnull(result, 0, @intCast(column)) != 0) return error.InvalidReplicationSourceRow;
            const lsn = self.pqgetvalue(result, 0, 0)[0..@intCast(self.pqgetlength(result, 0, 0))];
            const ordinal = try self.replicationChunkInteger(result, 1);
            const total = try self.replicationChunkInteger(result, 2);
            const offset = try self.replicationChunkInteger(result, 3);
            if (self.pqgetisnull(result, 0, 4) != 0) {
                // SQL emits one sentinel at the first message that crosses
                // the page budget. Only an already complete prefix may escape.
                if (framing.in_message or ordinal != framing.next_ordinal or offset != 0) return error.InvalidReplicationSourceRow;
                if (decoded.transactions.items.len == 0) return error.ReplicationCaptureLimitExceeded;
                truncated = true;
                continue;
            }
            const chunk_len: usize = @intCast(self.pqgetlength(result, 0, 4));
            if (lsn.len == 0 or lsn.len > 32 or chunk_len > replication_wire_chunk_bytes) return error.InvalidReplicationSourceRow;
            const chunk = self.pqgetvalue(result, 0, 4)[0..chunk_len];
            try framing.accept(ordinal, total, offset, chunk.len);
            if (offset == 0) {
                message_lsn.clearRetainingCapacity();
                try message_lsn.appendSlice(alloc, lsn);
            } else if (!std.mem.eql(u8, message_lsn.items, lsn)) return error.InvalidReplicationSourceRow;
            try message_bytes.appendSlice(alloc, chunk);
            if (!framing.in_message) {
                if (binding) |identity| try requirePublicationMessageRelation(message_bytes.items, identity.relation);
                const message = try parsePgoutputMessageAlloc(alloc, message_bytes.items, &relation_cache);
                if (expected) |origin| if (message_bytes.items[0] == 'R') try requirePgoutputOrigin(message_bytes.items, &relation_cache, origin);
                try decoded.accept(alloc, message, message_lsn.items);
                if (message == .commit) {
                    const timestamp = message.commit.timestamp_ms;
                    if (timestamp > 0 and poll_now_ms > 0) lag_millis = @max(lag_millis, @as(u64, @intCast(@max(@as(i64, 0), poll_now_ms - @as(i64, @intCast(timestamp))))));
                }
                message_bytes.clearRetainingCapacity();
            }
        }
        if (!completed_result or framing.in_message) return error.InvalidReplicationSourceRow;
        if (self.pqsetnonblocking(conn, 0) != 0) return error.ForeignQueryFailed;
        // A provider page ending inside a transaction cannot expose any of
        // that transaction's rows or advance its slot. The next peek retries
        // from the last complete commit and retains ordinary relation frames.
        decoded.discardIncomplete(alloc);

        return decoded.finish(alloc, lag_millis);
    }

    fn startReplicationChunkQuery(self: *@This(), a: Allocator, conn: ?*PGconn, request: []const u8, deadline: u64) !void {
        try ensureDeadline(deadline);
        const single_row = self.pqsetSingleRowMode orelse return error.ForeignQueryFailed;
        const text = try a.dupeSentinel(u8, request, 0);
        defer a.free(text);
        if (self.pqsetnonblocking(conn, 1) != 0 or self.pqsendQueryParams(conn, text.ptr, 0, null, null, null, null, 1) != 1 or single_row(conn) != 1) return error.ForeignQueryFailed;
        const driver: AsyncResultDriver = .{ .executor = self, .conn = conn };
        while (true) {
            try ensureDeadline(deadline);
            const flushed = driver.flush();
            if (flushed == 0) return;
            if (flushed < 0) return error.ForeignQueryFailed;
            const events = driver.wait(std.posix.POLL.IN | std.posix.POLL.OUT, deadline, null) catch |err| switch (err) {
                error.Timeout => continue,
                else => return err,
            };
            if (events & std.posix.POLL.IN != 0 and driver.consumeInput() != 1) return error.ForeignQueryFailed;
        }
    }

    fn nextReplicationChunkResult(self: *@This(), conn: ?*PGconn, deadline: u64) !?*PGresult {
        const driver: AsyncResultDriver = .{ .executor = self, .conn = conn };
        while (driver.isBusy() != 0) {
            try ensureDeadline(deadline);
            const events = driver.wait(std.posix.POLL.IN, deadline, null) catch |err| switch (err) {
                error.Timeout => continue,
                else => return err,
            };
            if (events & std.posix.POLL.IN != 0 and driver.consumeInput() != 1) return error.ForeignQueryFailed;
        }
        try ensureDeadline(deadline);
        return driver.getResult();
    }

    fn replicationChunkInteger(self: *@This(), result: ?*PGresult, column: c_int) !u64 {
        const length = self.pqgetlength(result, 0, column);
        if (length <= 0 or length > 20) return error.InvalidReplicationSourceRow;
        return std.fmt.parseInt(u64, self.pqgetvalue(result, 0, column)[0..@intCast(length)], 10) catch error.InvalidReplicationSourceRow;
    }

    fn lockInitialization(self: *@This(), a: Allocator, conn: ?*PGconn, params: foreign_source.ReplicationPollParams, deadline: u64) !bool {
        const owner = params.initialization_origin orelse return false;
        // Cleanup takes this same birth-specific advisory lock and rechecks
        // native retirement after acquiring it. A late creator must recheck
        // its claim after the lock too, so it cannot resurrect retired state.
        const name = params.cutover_lock_name orelse return error.InvalidQueryRequest;
        _ = try params.snapshotCustody();
        try self.setCutoverAdvisoryLockAlloc(a, conn, name, true, deadline);
        errdefer self.setCutoverAdvisoryLockAlloc(a, conn, name, false, deadline) catch {};
        try owner.check();
        return true;
    }
    fn unlockInitialization(self: *@This(), a: Allocator, conn: ?*PGconn, params: foreign_source.ReplicationPollParams, deadline: u64) void {
        self.setCutoverAdvisoryLockAlloc(a, conn, params.cutover_lock_name.?, false, deadline) catch {};
    }

    fn prepareReplicationAlloc(self: *@This(), alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPrepareResult {
        const slot_name = params.slot_name orelse return error.InvalidQueryRequest;
        const publication_name = params.publication_name orelse return error.InvalidQueryRequest;
        std.log.info("postgres libpq prepare replication table={s} slot={s}", .{ params.table, slot_name });
        const deadline = params.execution_deadline_ns orelse platform_time.monotonicNs() +| 30 * std.time.ns_per_s;
        var verified = try self.openVerifiedReplicationConnection(alloc, dsn, params.table, deadline);
        defer verified.deinit();
        const conn = verified.conn;
        const initialization_locked = try self.lockInitialization(alloc, conn, params, deadline);
        defer if (initialization_locked) self.unlockInitialization(alloc, conn, params, deadline);
        // Identity is sampled again after waiting for attempt ownership.
        const origin = if (initialization_locked) try self.sqlReplicationIdentityAlloc(alloc, conn, params.table, deadline) else verified.origin;
        try params.observeInitializationOrigin(origin);
        try self.ensurePublicationAlloc(
            alloc,
            conn,
            publication_name,
            params.table,
            params.filter_query_json,
            true,
            params.execution_deadline_ns,
        );
        const binding = try self.publicationBinding(alloc, conn, publication_name, params.table, params.filter_query_json != null, params.execution_deadline_ns);
        if (params.initialization_origin) |owner| try owner.check();
        const slot_existed = try self.ensureLogicalReplicationSlotAlloc(
            alloc,
            conn,
            slot_name,
            params.execution_deadline_ns,
        );
        const position = try self.loadLogicalReplicationSlotCheckpointAlloc(alloc, conn, slot_name, params.execution_deadline_ns);
        defer alloc.free(position);
        return .{
            .origin = origin,
            .checkpoint = try binding.checkpoint(alloc, position),
            .slot_existed = slot_existed,
        };
    }

    fn cleanupReplicationAlloc(self: *@This(), alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationCleanupParams) !void {
        try params.check();
        if (params.password_required) try self.requireExplicitSubscriptionPassword(alloc, dsn);
        const execution_deadline_ns = params.execution_deadline_ns orelse platform_time.monotonicNs() +| 30 * std.time.ns_per_s;
        const conn = try self.connectCountedFresh(alloc, dsn, execution_deadline_ns);
        defer self.closeCountedConnection(conn);
        if (params.password_required) try self.requireSubscriptionPasswordUsed(conn);
        if (params.native_source) |native| {
            const actual = try self.sqlProviderIdentityAlloc(alloc, conn, execution_deadline_ns);
            if (!std.mem.eql(u8, &actual, &native.provider_identity)) return error.ForeignProviderIdentityMismatch;
            try self.setCutoverAdvisoryLockAlloc(alloc, conn, native.lock_name, true, execution_deadline_ns);
            try self.lockSubscriptionSlotCreation(alloc, conn, params.slot_name, execution_deadline_ns);
            // Rejoin native drain/custody AFTER the shared provider lock. A
            // stale worker cannot recreate or acknowledge a retired slot.
            try native.authority.check();
            if (!native.require_absent) if (try self.requireSubscriptionCleanupSlotAlloc(alloc, conn, params.slot_name, native.snapshot_checkpoint, native.durable_checkpoint, execution_deadline_ns)) |point| {
                const checkpoint = try std.fmt.allocPrint(alloc, "{X}/{X}", .{ @as(u32, @intCast(point >> 32)), @as(u32, @truncate(point)) });
                defer alloc.free(checkpoint);
                try params.check();
                try self.dropSubscriptionSlotAtCheckpoint(alloc, conn, params.slot_name, checkpoint, execution_deadline_ns);
            };
        } else {
            try self.dropInactiveLogicalReplicationSlotIfExistsAlloc(alloc, conn, params.slot_name, execution_deadline_ns);
        }
        if (try self.logicalReplicationSlotExistsAlloc(alloc, conn, params.slot_name, execution_deadline_ns)) return error.ExactCutoverCleanupPending;
        if (params.publication_name) |publication| {
            try params.check();
            try self.dropPublicationIfExistsAlloc(alloc, conn, publication, execution_deadline_ns);
        }
        try params.check();
        try ensureDeadline(execution_deadline_ns);
    }

    /// Cleanup accepts a genuinely absent slot or lost WAL, but never another
    /// database/plugin/temporary slot. An active consumer is retryable debt.
    fn requireSubscriptionCleanupSlotAlloc(self: *@This(), a: Allocator, conn: ?*PGconn, slot_name: []const u8, snapshot: u64, durable: u64, deadline: u64) !?u64 {
        var prepared = try tableNamePreparedQueryAlloc(
            a,
            "SELECT (slot_type = 'logical' AND plugin = 'pgoutput' AND database = current_database() AND NOT temporary)::text, active::text, confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            slot_name,
        );
        defer prepared.deinit(a);
        const result = try self.execPreparedWithDeadline(conn, a, prepared, deadline, null);
        defer self.pqclear(result);
        if (self.pqntuples(result) == 0) return null;
        if (self.pqntuples(result) != 1 or self.pqnfields(result) != 3 or self.pqgetisnull(result, 0, 0) != 0 or self.pqgetisnull(result, 0, 1) != 0) return error.ForeignProviderIdentityMismatch;
        const valid = self.pqgetvalue(result, 0, 0)[0..@intCast(self.pqgetlength(result, 0, 0))];
        const active = self.pqgetvalue(result, 0, 1)[0..@intCast(self.pqgetlength(result, 0, 1))];
        if (!std.mem.eql(u8, valid, "true")) return error.ForeignProviderIdentityMismatch;
        if (!std.mem.eql(u8, active, "false")) return error.ExactCutoverCleanupPending;
        if (self.pqgetisnull(result, 0, 2) != 0) return error.ForeignProviderIdentityMismatch;
        const checkpoint = self.pqgetvalue(result, 0, 2)[0..@intCast(self.pqgetlength(result, 0, 2))];
        const point = foreign_source.parseReplicationCheckpoint(checkpoint) catch return error.ForeignProviderIdentityMismatch;
        if (point < snapshot or point > durable) return error.ForeignProviderIdentityMismatch;
        return point;
    }

    fn ensurePublicationAlloc(
        self: *@This(),
        alloc: Allocator,
        conn: ?*PGconn,
        publication_name: []const u8,
        table: []const u8,
        filter_query_json: ?[]const u8,
        verify_filter: bool,
        execution_deadline_ns: ?u64,
    ) !void {
        const check_args = try alloc.alloc(sql.ParameterValue, 2);
        check_args[0] = .{ .string = try alloc.dupe(u8, publication_name) };
        check_args[1] = .{ .string = try alloc.dupe(u8, table) };
        var check_prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8, "SELECT p.pubviaroot, c.relkind, EXISTS (SELECT 1 FROM pg_catalog.pg_publication_tables pt JOIN pg_catalog.pg_namespace n ON n.nspname = pt.schemaname JOIN pg_catalog.pg_class published ON published.relnamespace = n.oid AND published.relname = pt.tablename WHERE pt.pubname = p.pubname AND published.oid = c.oid) FROM pg_catalog.pg_publication p JOIN pg_catalog.pg_class c ON c.oid = $2::regclass WHERE p.pubname = $1"),
            .args = check_args,
        };
        defer check_prepared.deinit(alloc);
        const check_result = try self.execPreparedWithDeadline(
            conn,
            alloc,
            check_prepared,
            execution_deadline_ns,
            null,
        );
        defer self.pqclear(check_result);
        const exists = self.pqntuples(check_result) != 0;
        var publish_via_root = true;
        if (exists) {
            if (self.pqntuples(check_result) != 1 or self.pqnfields(check_result) != 3) return error.InvalidReplicationSourceConfig;
            for (0..3) |column| if (self.pqgetisnull(check_result, 0, @intCast(column)) != 0) return error.InvalidReplicationSourceConfig;
            const via_root = self.pqgetvalue(check_result, 0, 0)[0..@intCast(self.pqgetlength(check_result, 0, 0))];
            const relation_kind = self.pqgetvalue(check_result, 0, 1)[0..@intCast(self.pqgetlength(check_result, 0, 1))];
            const member = self.pqgetvalue(check_result, 0, 2)[0..@intCast(self.pqgetlength(check_result, 0, 2))];
            try requirePublicationSourceIdentity(via_root, relation_kind, member);
            publish_via_root = std.mem.eql(u8, via_root, "t");
            if (!verify_filter or filter_query_json == null) return;
        }

        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const predicate = if (filter_query_json) |json| try filter.postgresPublicationPredicateAlloc(a, json, &.{}) else "";
        if (!exists) return self.createPublication(a, conn, publication_name, table, predicate, publish_via_root, execution_deadline_ns);
        if (predicate.len == 0) {
            _ = try self.publicationBinding(a, conn, publication_name, table, false, execution_deadline_ns);
            return;
        }

        // Use PostgreSQL's own parser and catalog deparser for both expressions.
        // The comparison publication is visible only within this transaction.
        const deadline = execution_deadline_ns orelse platform_time.monotonicNs() +| 5 * std.time.ns_per_s;
        self.pqclear(try self.execSimpleWithDeadline(conn, a, "BEGIN", deadline, true));
        errdefer {
            if (self.execSimpleWithDeadline(conn, a, "ROLLBACK", deadline, true)) |result| self.pqclear(result) else |_| {}
        }
        const probe = try std.fmt.allocPrint(a, "antfly_filter_{x}_{x}", .{ @intFromPtr(conn), platform_time.monotonicNs() });
        try self.createPublication(a, conn, probe, table, predicate, publish_via_root, deadline);
        const expected = try self.publicationBinding(a, conn, probe, table, true, deadline);
        const actual = try self.publicationBinding(a, conn, publication_name, table, true, deadline);
        if (!std.meta.eql(expected, actual)) return error.ReplicationPublicationBindingChanged;
        self.pqclear(try self.execSimpleWithDeadline(conn, a, "ROLLBACK", deadline, true));
    }

    fn createPublication(self: *@This(), a: Allocator, conn: ?*PGconn, name: []const u8, table: []const u8, predicate: []const u8, publish_via_root: bool, deadline: ?u64) !void {
        const quoted_name = try sql.postgresDialect().quote_identifier(a, name);
        defer a.free(quoted_name);
        const quoted_table = try sql.postgresDialect().quote_relation(a, table);
        defer a.free(quoted_table);
        const options = if (publish_via_root) " WITH (publish_via_partition_root = true)" else "";
        const statement = if (predicate.len == 0)
            try std.fmt.allocPrint(a, "CREATE PUBLICATION {s} FOR TABLE {s}{s}", .{ quoted_name, quoted_table, options })
        else
            try std.fmt.allocPrint(a, "CREATE PUBLICATION {s} FOR TABLE {s} WHERE ({s}){s}", .{ quoted_name, quoted_table, predicate, options });
        defer a.free(statement);
        const bounded = deadline orelse platform_time.monotonicNs() +| 5 * std.time.ns_per_s;
        self.pqclear(try self.execSimpleWithDeadline(conn, a, statement, bounded, true));
    }

    fn ensureLogicalReplicationSlotAlloc(
        self: *@This(),
        alloc: Allocator,
        conn: ?*PGconn,
        slot_name: []const u8,
        execution_deadline_ns: ?u64,
    ) !bool {
        const check_args = try alloc.alloc(sql.ParameterValue, 1);
        check_args[0] = .{ .string = try alloc.dupe(u8, slot_name) };
        var check_prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8, "SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1"),
            .args = check_args,
        };
        defer check_prepared.deinit(alloc);
        const check_result = try self.execPreparedWithDeadline(
            conn,
            alloc,
            check_prepared,
            execution_deadline_ns,
            null,
        );
        defer self.pqclear(check_result);
        if (@as(usize, @intCast(self.pqntuples(check_result))) > 0) return true;

        const create_args = try alloc.alloc(sql.ParameterValue, 1);
        create_args[0] = .{ .string = try alloc.dupe(u8, slot_name) };
        var create_prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8, "SELECT * FROM pg_create_logical_replication_slot($1, 'pgoutput')"),
            .args = create_args,
        };
        defer create_prepared.deinit(alloc);
        const create_result = try self.execPreparedWithDeadline(
            conn,
            alloc,
            create_prepared,
            execution_deadline_ns,
            null,
        );
        defer self.pqclear(create_result);
        return false;
    }

    fn loadLogicalReplicationSlotCheckpointAlloc(
        self: *@This(),
        alloc: Allocator,
        conn: ?*PGconn,
        slot_name: []const u8,
        execution_deadline_ns: ?u64,
    ) ![]u8 {
        const args = try alloc.alloc(sql.ParameterValue, 1);
        args[0] = .{ .string = try alloc.dupe(u8, slot_name) };
        var prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8,
                \\SELECT CASE WHEN slot_type = 'logical' AND plugin = 'pgoutput'
                \\                  AND database = current_database() AND restart_lsn IS NOT NULL
                \\                  AND (to_jsonb(s) ->> 'invalidation_reason') IS NULL
                \\            THEN confirmed_flush_lsn::text END
                \\  FROM pg_catalog.pg_replication_slots s WHERE slot_name = $1
            ),
            .args = args,
        };
        defer prepared.deinit(alloc);
        const result = try self.execPreparedWithDeadline(
            conn,
            alloc,
            prepared,
            execution_deadline_ns,
            null,
        );
        defer self.pqclear(result);
        if (self.pqntuples(result) == 0) return try alloc.dupe(u8, "");
        if (self.pqgetisnull(result, 0, 0) != 0) return error.ReplicationCutoverAuthorityLost;
        return try copyCellAlloc(alloc, self.pqgetvalue(result, 0, 0), @intCast(self.pqgetlength(result, 0, 0)));
    }

    fn advanceLogicalReplicationSlotCheckpointAlloc(
        self: *@This(),
        alloc: Allocator,
        conn: ?*PGconn,
        slot_name: []const u8,
        checkpoint: []const u8,
        execution_deadline_ns: ?u64,
    ) ![]u8 {
        const args = try alloc.alloc(sql.ParameterValue, 2);
        args[0] = .{ .string = try alloc.dupe(u8, slot_name) };
        args[1] = .{ .string = try alloc.dupe(u8, checkpoint) };
        var prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8,
                \\WITH eligible AS MATERIALIZED (
                \\    SELECT slot_name FROM pg_catalog.pg_replication_slots s
                \\    WHERE slot_name = $1 AND slot_type = 'logical' AND plugin = 'pgoutput'
                \\      AND database = current_database() AND restart_lsn IS NOT NULL
                \\      AND (to_jsonb(s) ->> 'invalidation_reason') IS NULL
                \\      AND confirmed_flush_lsn <= $2::pg_lsn
                \\)
                \\SELECT advanced.end_lsn::text FROM eligible
                \\CROSS JOIN LATERAL pg_catalog.pg_replication_slot_advance(eligible.slot_name, $2::pg_lsn) advanced
            ),
            .args = args,
        };
        defer prepared.deinit(alloc);
        const result = try self.execPreparedWithDeadline(
            conn,
            alloc,
            prepared,
            execution_deadline_ns,
            null,
        );
        defer self.pqclear(result);
        if (self.pqntuples(result) != 1 or self.pqgetisnull(result, 0, 0) != 0) return error.ReplicationCutoverAuthorityLost;
        return try copyCellAlloc(alloc, self.pqgetvalue(result, 0, 0), @intCast(self.pqgetlength(result, 0, 0)));
    }

    const ExportedReplicationSnapshot = struct {
        checkpoint: []u8,
        snapshot_name: []u8,

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            alloc.free(self.checkpoint);
            alloc.free(self.snapshot_name);
            self.* = undefined;
        }
    };

    // A birth-specific name alone does not prove the retained provider slot.
    // Check the actual provider/database/plugin before either peek or ACK.
    fn requireSubscriptionSlotAlloc(self: *@This(), a: Allocator, conn: ?*PGconn, slot_name: []const u8, deadline: u64) !u64 {
        var slot_query: sql.PreparedQuery = blk: {
            const args = try a.alloc(sql.ParameterValue, 1);
            errdefer a.free(args);
            args[0] = .{ .string = try a.dupe(u8, slot_name) };
            errdefer a.free(args[0].string);
            break :blk .{
                .sql_text = try a.dupe(u8, "SELECT (slot_type = 'logical' AND plugin = 'pgoutput' AND database = current_database() AND NOT temporary AND NOT active AND wal_status IS DISTINCT FROM 'lost')::text, confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots WHERE slot_name = $1"),
                .args = args,
            };
        };
        defer slot_query.deinit(a);
        const result = try self.execPreparedWithDeadline(conn, a, slot_query, deadline, null);
        defer self.pqclear(result);
        if (self.pqntuples(result) == 0) return error.ForeignReplicationSlotMissing;
        if (self.pqntuples(result) != 1 or self.pqgetisnull(result, 0, 0) != 0) return error.ForeignProviderIdentityMismatch;
        const valid = self.pqgetvalue(result, 0, 0)[0..@intCast(self.pqgetlength(result, 0, 0))];
        if (!std.mem.eql(u8, valid, "true") or self.pqgetisnull(result, 0, 1) != 0) return error.ForeignProviderIdentityMismatch;
        const confirmed = self.pqgetvalue(result, 0, 1)[0..@intCast(self.pqgetlength(result, 0, 1))];
        return foreign_source.parseReplicationCheckpoint(confirmed) catch error.ForeignProviderIdentityMismatch;
    }

    fn logicalReplicationSlotExistsAlloc(
        self: *@This(),
        alloc: Allocator,
        conn: ?*PGconn,
        slot_name: []const u8,
        execution_deadline_ns: ?u64,
    ) !bool {
        const args = try alloc.alloc(sql.ParameterValue, 1);
        args[0] = .{ .string = try alloc.dupe(u8, slot_name) };
        var prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8, "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1"),
            .args = args,
        };
        defer prepared.deinit(alloc);
        const result = try self.execPreparedWithDeadline(
            conn,
            alloc,
            prepared,
            execution_deadline_ns,
            null,
        );
        defer self.pqclear(result);
        return @as(usize, @intCast(self.pqntuples(result))) > 0;
    }

    fn identifyExactCutoverProvider(
        self: *@This(),
        alloc: Allocator,
        sql_conn: ?*PGconn,
        replication_conn: ?*PGconn,
        execution_deadline_ns: u64,
    ) !foreign_source.ExactCutoverIntent.ProviderIdentity {
        const result = try self.execSimpleWithDeadline(
            replication_conn,
            alloc,
            "IDENTIFY_SYSTEM",
            execution_deadline_ns,
            false,
        );
        defer self.pqclear(result);
        if (self.pqntuples(result) != 1 or self.pqnfields(result) < 4)
            return error.ForeignQueryFailed;
        if (self.pqgetisnull(result, 0, 0) != 0 or
            self.pqgetisnull(result, 0, 3) != 0)
            return error.ForeignQueryFailed;
        const system_id = self.pqgetvalue(result, 0, 0)[0..@intCast(
            self.pqgetlength(result, 0, 0),
        )];
        const database = self.pqgetvalue(result, 0, 3)[0..@intCast(
            self.pqgetlength(result, 0, 3),
        )];
        if (system_id.len == 0 or database.len == 0)
            return error.ForeignQueryFailed;

        // Authenticate the SQL and replication sockets as the same database.
        // The database OID additionally rejects drop/recreate of an identically
        // named database within the same cluster.
        const sql_identity_result = try self.execSimpleWithDeadline(
            sql_conn,
            alloc,
            \\SELECT (pg_catalog.pg_control_system()).system_identifier::text,
            \\       pg_catalog.current_database(),
            \\       oid::text
            \\  FROM pg_catalog.pg_database
            \\ WHERE datname = pg_catalog.current_database()
        ,
            execution_deadline_ns,
            false,
        );
        defer self.pqclear(sql_identity_result);
        if (self.pqntuples(sql_identity_result) != 1 or
            self.pqnfields(sql_identity_result) < 3)
            return error.ForeignQueryFailed;
        for (0..3) |column| {
            if (self.pqgetisnull(sql_identity_result, 0, @intCast(column)) != 0)
                return error.ForeignQueryFailed;
        }
        const sql_system_id =
            self.pqgetvalue(sql_identity_result, 0, 0)[0..@intCast(
                self.pqgetlength(sql_identity_result, 0, 0),
            )];
        const sql_database =
            self.pqgetvalue(sql_identity_result, 0, 1)[0..@intCast(
                self.pqgetlength(sql_identity_result, 0, 1),
            )];
        const database_oid =
            self.pqgetvalue(sql_identity_result, 0, 2)[0..@intCast(
                self.pqgetlength(sql_identity_result, 0, 2),
            )];
        if (!std.mem.eql(u8, system_id, sql_system_id) or
            !std.mem.eql(u8, database, sql_database) or
            database_oid.len == 0)
            return error.ForeignProviderIdentityMismatch;
        return exactCutoverProviderIdentity(system_id, database, database_oid);
    }

    fn cloneCreatedReplicationSnapshotAlloc(
        alloc: Allocator,
        slot_created: *bool,
        checkpoint: []const u8,
        snapshot_name: []const u8,
    ) !ExportedReplicationSnapshot {
        // The caller invokes this only after validating a successful
        // CREATE_REPLICATION_SLOT result. Mark ownership before allocation so
        // later local failures still trigger cleanup of this attempt's slot.
        slot_created.* = true;
        const checkpoint_copy = try alloc.dupe(u8, checkpoint);
        errdefer alloc.free(checkpoint_copy);
        return .{
            .checkpoint = checkpoint_copy,
            .snapshot_name = try alloc.dupe(u8, snapshot_name),
        };
    }

    fn createLogicalReplicationSlotExportSnapshotAlloc(
        self: *@This(),
        alloc: Allocator,
        conn: ?*PGconn,
        slot_name: []const u8,
        execution_deadline_ns: u64,
        slot_created: *bool,
    ) !ExportedReplicationSnapshot {
        const result = try self.createLogicalReplicationSlotExportResult(alloc, conn, slot_name, execution_deadline_ns, null);
        defer self.pqclear(result);
        return try cloneCreatedReplicationSnapshotAlloc(
            alloc,
            slot_created,
            self.pqgetvalue(result, 0, 1)[0..@intCast(self.pqgetlength(result, 0, 1))],
            self.pqgetvalue(result, 0, 2)[0..@intCast(self.pqgetlength(result, 0, 2))],
        );
    }

    fn createLogicalReplicationSlotExportResult(
        self: *@This(),
        alloc: Allocator,
        conn: ?*PGconn,
        slot_name: []const u8,
        execution_deadline_ns: u64,
        capture: ?struct { authority: foreign_source.SubscriptionAuthority, locked_observer: ?*PGconn, provider_identity: [32]u8 },
    ) !?*PGresult {
        const quoted_slot = try sql.postgresDialect().quote_identifier(alloc, slot_name);
        defer alloc.free(quoted_slot);
        const command = try std.fmt.allocPrintSentinel(alloc, "CREATE_REPLICATION_SLOT {s} LOGICAL pgoutput EXPORT_SNAPSHOT", .{quoted_slot}, 0);
        defer alloc.free(command);
        try ensureDeadline(execution_deadline_ns);
        if (self.pqsetnonblocking(conn, 1) != 0) return error.ForeignQueryFailed;
        if (capture) |attempt| try attempt.authority.start(attempt.provider_identity);
        ensureDeadline(execution_deadline_ns) catch |err| {
            if (capture) |attempt| attempt.authority.refused() catch {};
            return err;
        };
        if (self.pqsendQuery(conn, command.ptr) != 1) return error.ForeignQueryFailed;
        const result = try readSingleAsyncResultWithDeadline(AsyncResultDriver{ .executor = self, .conn = conn }, execution_deadline_ns, null);
        errdefer self.pqclear(result);
        const status = self.pqresultStatus(result);
        if (status != PGRES_TUPLES_OK) {
            const state = self.pqresultErrorField(result, PG_DIAG_SQLSTATE);
            // A complete server refusal plus confirmed absence under the
            // publisher lock can retire this attempt. Transport errors and
            // uncertain observations leave custody for later recovery.
            if (capture) |attempt| {
                if (status == PGRES_FATAL_ERROR and state != null and self.pqstatus(conn) == CONNECTION_OK) {
                    const exists = self.logicalReplicationSlotExistsAlloc(alloc, attempt.locked_observer, slot_name, execution_deadline_ns) catch true;
                    if (!exists) attempt.authority.refused() catch {};
                }
            }
            return mapResultError(state, self.pqresultErrorMessage(result));
        }
        if (self.pqntuples(result) != 1 or self.pqnfields(result) < 3) return error.ForeignQueryFailed;
        if (self.pqgetisnull(result, 0, 1) != 0 or self.pqgetisnull(result, 0, 2) != 0) return error.ForeignQueryFailed;
        return result;
    }

    fn dropInactiveLogicalReplicationSlotIfExistsAlloc(
        self: *@This(),
        alloc: Allocator,
        conn: ?*PGconn,
        slot_name: []const u8,
        execution_deadline_ns: u64,
    ) !void {
        var prepared = try tableNamePreparedQueryAlloc(
            alloc,
            "SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            slot_name,
        );
        defer prepared.deinit(alloc);
        const result = try self.execPreparedWithDeadline(
            conn,
            alloc,
            prepared,
            execution_deadline_ns,
            null,
        );
        self.pqclear(result);
    }

    fn dropPublicationIfExistsAlloc(
        self: *@This(),
        alloc: Allocator,
        conn: ?*PGconn,
        publication_name: []const u8,
        execution_deadline_ns: u64,
    ) !void {
        const quoted_publication = try sql.postgresDialect().quote_identifier(
            alloc,
            publication_name,
        );
        defer alloc.free(quoted_publication);
        var prepared = sql.PreparedQuery{
            .sql_text = try std.fmt.allocPrint(
                alloc,
                "DROP PUBLICATION IF EXISTS {s}",
                .{quoted_publication},
            ),
            .args = &.{},
        };
        defer prepared.deinit(alloc);
        const result = try self.execPreparedAllowCommandWithDeadline(
            conn,
            alloc,
            prepared,
            execution_deadline_ns,
        );
        self.pqclear(result);
    }

    /// Keep a server-side fence on the connection that runs CREATE. If the
    /// process dies, its SQL connection can lose the outer lock before the
    /// replication backend has finished the outstanding command.
    fn lockSubscriptionSlotCreation(self: *@This(), a: Allocator, conn: ?*PGconn, name: []const u8, deadline: u64) !void {
        const quoted = try quoteSqlStringLiteralAlloc(a, name);
        defer a.free(quoted);
        const statement = try std.fmt.allocPrint(a, "SELECT pg_catalog.pg_advisory_lock(pg_catalog.hashtextextended({s}, 1))", .{quoted});
        defer a.free(statement);
        const result = try self.execSimpleWithDeadline(conn, a, statement, deadline, false);
        self.pqclear(result);
    }

    fn setCutoverAdvisoryLockAlloc(
        self: *@This(),
        alloc: Allocator,
        conn: ?*PGconn,
        lock_name: []const u8,
        acquire: bool,
        execution_deadline_ns: u64,
    ) !void {
        const args = try alloc.alloc(sql.ParameterValue, 1);
        args[0] = .{ .string = try alloc.dupe(u8, lock_name) };
        var prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(
                u8,
                if (acquire)
                    "SELECT pg_advisory_lock(hashtextextended($1, 0))"
                else
                    "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
            ),
            .args = args,
        };
        defer prepared.deinit(alloc);
        const result = try self.execPreparedWithDeadline(
            conn,
            alloc,
            prepared,
            execution_deadline_ns,
            null,
        );
        defer self.pqclear(result);
        if (self.pqntuples(result) != 1 or self.pqnfields(result) != 1)
            return error.ForeignQueryFailed;
        if (!acquire) {
            if (self.pqgetisnull(result, 0, 0) != 0)
                return error.ForeignQueryFailed;
            const value = self.pqgetvalue(result, 0, 0)[0..@intCast(
                self.pqgetlength(result, 0, 0),
            )];
            if (!std.mem.eql(u8, value, "t"))
                return error.ForeignQueryFailed;
        }
    }

    fn dropLogicalReplicationSlotIfExistsWithReservedPermit(
        self: *@This(),
        alloc: Allocator,
        dsn: []const u8,
        slot_name: []const u8,
        execution_deadline_ns: u64,
    ) !void {
        const conn = try self.connectFreshWithDeadline(dsn, execution_deadline_ns, null);
        defer self.pqfinish(conn);
        try self.dropInactiveLogicalReplicationSlotIfExistsAlloc(
            alloc,
            conn,
            slot_name,
            execution_deadline_ns,
        );
    }
};

// --- Connection-error sanitization -----------------------------------------
//
// libpq's conninfo and URI parsers can put raw credential text straight into
// their diagnostic messages. For example, an invalid percent-encoded byte in
// a URI password is reported by quoting that password token verbatim (e.g.
// `invalid percent-encoded token: "p%zzw0rd"`, or just `"%z"` for a
// one-character password), and some conninfo errors quote the whole
// offending connection string. Any of that text may also be a secret
// resolved from the keystore, since by the time it reaches libpq the DSN
// has already had `${secret:...}` references substituted in. None of this
// can be fixed by trusting libpq to omit passwords, so every connection-
// error message is sanitized here before it is logged.
//
// Two different matching strategies are combined because they trade off
// safety against coverage in opposite directions:
//   - Blanket literal substring matching (`redactLiteralSecretsAlloc`) can
//     scan the whole free-form message, but is only safe for credentials at
//     or above `min_blanket_literal_secret_len`: a one- or two-character
//     credential would also match incidental characters inside unrelated
//     words (an "u" username turning "refused" into "ref[redacted]sed").
//   - Quoted-token matching (`redactQuotedCredentialTokensAlloc`) is safe at
//     any length, including a single character, because it only touches
//     text libpq has already set off in quotes as an echoed token -- it
//     never reaches into unquoted free text, so it cannot mangle a word
//     like "refused" or a quoted hostname like `"host"`.
// Together they catch both a long password sitting in plain text and a
// short one libpq only ever shows inside a quoted diagnostic token.

/// Upper bound on the sanitized connection-error text handed to the log.
const max_sanitized_connection_error_len: usize = 300;

/// Minimum credential length eligible for blanket free-text literal
/// redaction. See the module comment above for why short credentials are
/// instead left to the quoted-token and structural passes.
const min_blanket_literal_secret_len: usize = 3;

/// Redacts credential material from a libpq connection-error `message`
/// produced while attempting to connect with `dsn`. Returns a newly
/// allocated, length-bounded string that keeps the useful parts of the
/// error (host, port, database, libpq's stated reason) but never contains
/// the password, the user when it carries a password, or the full
/// connection string.
fn sanitizeConnectionErrorAlloc(alloc: Allocator, message: []const u8, dsn: []const u8) ![]u8 {
    var secrets: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (secrets.items) |secret| alloc.free(secret);
        secrets.deinit(alloc);
    }
    try collectDsnSecretsAlloc(alloc, dsn, &secrets);

    var quoted_candidates: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (quoted_candidates.items) |candidate| alloc.free(candidate);
        quoted_candidates.deinit(alloc);
    }
    try collectDsnQuotedCandidatesAlloc(alloc, dsn, &quoted_candidates);

    // `working` changes owner at every reassignment below: the old buffer is
    // freed explicitly right after each pass succeeds, immediately before
    // `working` is pointed at the new one, so there is never a moment where
    // `working` names a stale or already-freed buffer. That means a single
    // `errdefer` registered here, naming the variable (not a snapshot of its
    // current value), correctly frees whichever buffer is live if any later
    // pass fails -- including the truncation path at the end of this
    // function, which allocates one more buffer before freeing `working`.
    var working = try redactLiteralSecretsAlloc(alloc, message, secrets.items);
    errdefer alloc.free(working);

    const quoted_redacted = try redactQuotedCredentialTokensAlloc(alloc, working, quoted_candidates.items);
    alloc.free(working);
    working = quoted_redacted;

    const uri_redacted = try redactUriCredentialsAlloc(alloc, working);
    alloc.free(working);
    working = uri_redacted;

    const kv_redacted = try redactConninfoPasswordFieldsAlloc(alloc, working);
    alloc.free(working);
    working = kv_redacted;

    // Defensive final pass: some libpq conninfo diagnostics quote the whole
    // attempted connection string rather than just the offending field.
    if (dsn.len > 0) {
        const literal: [1][]const u8 = .{dsn};
        const dsn_redacted = try redactLiteralSecretsAlloc(alloc, working, &literal);
        alloc.free(working);
        working = dsn_redacted;
    }

    if (working.len <= max_sanitized_connection_error_len) return working;
    const ellipsis = "...";
    const truncated = try alloc.alloc(u8, max_sanitized_connection_error_len);
    @memcpy(truncated, working[0..max_sanitized_connection_error_len]);
    if (truncated.len > ellipsis.len) {
        @memcpy(truncated[truncated.len - ellipsis.len ..], ellipsis);
    }
    alloc.free(working);
    return truncated;
}

/// Extracts the literal credential substrings carried by `dsn` that are
/// long enough to redact as blanket free-text literals (see
/// `min_blanket_literal_secret_len`) -- the password for URI-style DSNs, or
/// the `password=` value for key/value conninfo strings -- so they can be
/// redacted wherever they appear in a libpq error message, even if libpq
/// echoes only a fragment rather than the whole connection string. The
/// username is deliberately never included here; `redactUriCredentialsAlloc`
/// redacts it structurally instead, only when it appears as part of a
/// `user:password@` URI fragment.
fn collectDsnSecretsAlloc(alloc: Allocator, dsn: []const u8, out: *std.ArrayListUnmanaged([]const u8)) !void {
    if (isPostgresUri(dsn)) {
        try collectUriCredentialsAlloc(alloc, dsn, min_blanket_literal_secret_len, false, out);
    } else {
        try collectConninfoCredentialsAlloc(alloc, dsn, min_blanket_literal_secret_len, out);
    }
}

/// Extracts credential text from `dsn` for matching against text libpq has
/// already set off in quotes as an echoed token (see
/// `redactQuotedCredentialTokensAlloc`). Unlike `collectDsnSecretsAlloc`,
/// there is no minimum length -- a quoted match is safe at any length,
/// including a single character -- and the full `user:password` userinfo is
/// included for URI-style DSNs in case libpq quotes it together rather than
/// the password alone.
fn collectDsnQuotedCandidatesAlloc(alloc: Allocator, dsn: []const u8, out: *std.ArrayListUnmanaged([]const u8)) !void {
    if (isPostgresUri(dsn)) {
        try collectUriCredentialsAlloc(alloc, dsn, 1, true, out);
    } else {
        try collectConninfoCredentialsAlloc(alloc, dsn, 1, out);
    }
}

fn isPostgresUri(dsn: []const u8) bool {
    return std.mem.startsWith(u8, dsn, "postgres://") or std.mem.startsWith(u8, dsn, "postgresql://");
}

/// Index just past the end of the authority component (host[:port], plus an
/// optional "user[:password]@" prefix) of a URI with its scheme stripped off.
fn uriAuthorityLen(rest: []const u8) usize {
    var i: usize = 0;
    while (i < rest.len) : (i += 1) {
        switch (rest[i]) {
            '/', '?' => return i,
            else => {},
        }
    }
    return rest.len;
}

/// Extracts the password (and, when `include_userinfo` is set, the full
/// `user:password` userinfo text) from a URI-style `dsn`, plus any
/// credential-bearing query parameter (see `collectUriQueryCredentialsAlloc`).
/// Only candidates at least `min_password_len` long are collected, letting
/// callers choose between a safety floor for blanket free-text matching and
/// no floor at all for quoted-token matching.
fn collectUriCredentialsAlloc(
    alloc: Allocator,
    dsn: []const u8,
    min_password_len: usize,
    include_userinfo: bool,
    out: *std.ArrayListUnmanaged([]const u8),
) !void {
    const scheme_end = std.mem.indexOf(u8, dsn, "://") orelse return;
    const authority_start = scheme_end + 3;
    if (authority_start > dsn.len) return;
    const rest = dsn[authority_start..];
    const authority_len = uriAuthorityLen(rest);
    const authority = rest[0..authority_len];
    if (std.mem.lastIndexOfScalar(u8, authority, '@')) |at| {
        const userinfo = authority[0..at];
        if (userinfo.len != 0) {
            if (include_userinfo) try appendOwnedDupe(alloc, out, userinfo);
            if (std.mem.indexOfScalar(u8, userinfo, ':')) |colon| {
                const password = userinfo[colon + 1 ..];
                if (password.len >= min_password_len) try appendOwnedDupe(alloc, out, password);
            }
        }
    }
    // The authority may carry no "user[:password]@" prefix at all, but a
    // query-string password can still be present, e.g.
    // `postgresql://localhost/db?password=secret`; `rest[authority_len..]`
    // is the `/path?query` tail that follows the authority. libpq has no
    // fragment component: a literal # remains part of its current field.
    try collectUriQueryCredentialsAlloc(alloc, rest[authority_len..], min_password_len, out);
}

/// Duplicates `value` and appends it to `out`, transferring ownership of the
/// duplicate to `out` only once the append has actually succeeded. Appending
/// `try alloc.dupe(...)` directly as a call argument would instead leak the
/// duplicate if `dupe` succeeds but the list's subsequent growth fails.
fn appendOwnedDupe(alloc: Allocator, out: *std.ArrayListUnmanaged([]const u8), value: []const u8) !void {
    const owned = try alloc.dupe(u8, value);
    errdefer alloc.free(owned);
    try out.append(alloc, owned);
}

/// Duplicates `value` moved out of `buf` (whose storage `buf.toOwnedSlice`
/// transfers) and appends it to `out`, freeing the slice rather than leaking
/// it if the append fails.
fn appendOwnedSlice(alloc: Allocator, out: *std.ArrayListUnmanaged([]const u8), buf: *std.ArrayListUnmanaged(u8)) !void {
    const owned = try buf.toOwnedSlice(alloc);
    errdefer alloc.free(owned);
    try out.append(alloc, owned);
}

/// libpq URI query-parameter keywords that carry credential material: the
/// database `password`, and `sslpassword`, the passphrase used to decrypt a
/// client certificate private key named by `sslkey`. See
/// `conninfo_uri_parse_params` in libpq's fe-connect.c for the full set of
/// keywords a URI query string may carry; these are the only two that hold a
/// secret libpq might echo back in a diagnostic. libpq's own keyword lookup
/// (`conninfo_find`, used after `conninfo_uri_decode` has decoded the key)
/// is a case-sensitive `strcmp` against exactly these lowercase spellings.
const uri_query_credential_keys = [_][]const u8{ "password", "sslpassword" };

/// Case-insensitive check against `uri_query_credential_keys`. Used as a
/// conservative baseline for a key exactly as written in the query string
/// (no percent-decoding involved): libpq itself would reject an
/// unexpectedly-cased *literal* key like `Password=...` as an unrecognized
/// URI query parameter (case-sensitive `strcmp` in `conninfo_find`) and
/// never treat its value as a real credential, but redacting it anyway
/// costs nothing and errs on the side of caution.
fn isUriQueryCredentialKeyIgnoreCase(key: []const u8) bool {
    for (uri_query_credential_keys) |candidate_key| {
        if (std.ascii.eqlIgnoreCase(key, candidate_key)) return true;
    }
    return false;
}

/// Case-sensitive check against `uri_query_credential_keys`, matching
/// libpq's own `conninfo_find` (`strcmp`). Used for a key that has already
/// been percent-decoded: at that point its casing is exactly what libpq
/// would compare, so matching libpq's case sensitivity here (rather than
/// the looser `isUriQueryCredentialKeyIgnoreCase` above) correctly leaves
/// `%50assword` (which decodes to `Password`) unflagged -- see the test
/// "...a decoded key libpq would reject as unrecognized due to case...".
/// libpq would fail that connection attempt with "invalid URI query
/// parameter" quoting the *key*, never touching the value as a credential.
fn isUriQueryCredentialKeyExact(key: []const u8) bool {
    for (uri_query_credential_keys) |candidate_key| {
        if (std.mem.eql(u8, key, candidate_key)) return true;
    }
    return false;
}

/// Best-effort fallback for a URI query key whose percent-encoding is
/// malformed, so `percentDecodeIfValidAlloc` fails with
/// `error.InvalidPercentEncoding` -- the same condition under which libpq's
/// `conninfo_uri_decode` aborts the whole connection attempt, quoting the
/// raw key text itself rather than any value. We cannot know what the
/// malformed escape was meant to decode to, so -- conservatively --
/// percent-decode only the leading run of well-formed escapes (stopping at
/// the first malformed one) and treat the key as a possible credential key
/// if that decoded prefix is, case-insensitively, a prefix of (or equal to)
/// one of `uri_query_credential_keys`. This catches realistic cases like
/// `password%zz` (decoded prefix is the whole word "password") or
/// `pass%zzword` (decoded prefix "pass" is a prefix of "password"), at the
/// cost of being unable to recognize a malformed escape that would, if
/// well-formed, fall strictly before the matching text (e.g. `%zzpassword`).
fn couldDecodeToCredentialKeyAlloc(alloc: Allocator, key: []const u8) !bool {
    var decoded_prefix: std.ArrayListUnmanaged(u8) = .empty;
    defer decoded_prefix.deinit(alloc);
    var i: usize = 0;
    while (i < key.len) {
        if (key[i] == '%') {
            if (i + 2 < key.len) {
                const hi = std.fmt.charToDigit(key[i + 1], 16) catch null;
                const lo = std.fmt.charToDigit(key[i + 2], 16) catch null;
                if (hi != null and lo != null) {
                    try decoded_prefix.append(alloc, @intCast(hi.? * 16 + lo.?));
                    i += 3;
                    continue;
                }
            }
            break; // malformed escape: stop decoding, keep what we have so far
        }
        try decoded_prefix.append(alloc, key[i]);
        i += 1;
    }
    if (decoded_prefix.items.len == 0) return false;
    for (uri_query_credential_keys) |candidate_key| {
        if (decoded_prefix.items.len > candidate_key.len) continue;
        if (std.ascii.eqlIgnoreCase(decoded_prefix.items, candidate_key[0..decoded_prefix.items.len])) return true;
    }
    return false;
}

/// Extracts credential values from the query-string portion of a URI-style
/// DSN (`path_query` is everything after the authority, i.e.
/// `/path?query`, possibly with no query at all). For each
/// credential-bearing key (see `uri_query_credential_keys`) -- decoded
/// first, the same way libpq's `conninfo_uri_parse_params` decodes the key
/// before ever comparing it against a known keyword, so a percent-encoded
/// key like `%70assword` is recognized as `password` -- collects the value
/// exactly as written -- which is what libpq's `conninfo_uri_decode`
/// echoes verbatim when the value contains an invalid percent-encoded
/// sequence, e.g. `invalid percent-encoded token: "supersecret%zz"` -- and,
/// separately, the percent-decoded value whenever the encoding is
/// well-formed, since a successfully decoded password can surface in other
/// diagnostics (for example an authentication failure reported with the
/// decoded value). Malformed encoding is left undecoded rather than guessed
/// at; only the raw form is collected for it. Only candidates at least
/// `min_password_len` long are collected, matching
/// `collectUriCredentialsAlloc`'s callers.
fn collectUriQueryCredentialsAlloc(
    alloc: Allocator,
    path_query: []const u8,
    min_password_len: usize,
    out: *std.ArrayListUnmanaged([]const u8),
) !void {
    const query_start = std.mem.indexOfScalar(u8, path_query, '?') orelse return;
    // libpq does not recognize URI fragments; # is literal credential text.
    var query = path_query[query_start + 1 ..];
    while (query.len > 0) {
        const amp = std.mem.indexOfScalar(u8, query, '&');
        const pair = if (amp) |idx| query[0..idx] else query;
        query = if (amp) |idx| query[idx + 1 ..] else "";
        const eq = std.mem.indexOfScalar(u8, pair, '=') orelse continue;
        const key = pair[0..eq];
        const value = pair[eq + 1 ..];
        if (value.len == 0) continue;

        // Conservative baseline: the key exactly as written, matched
        // case-insensitively (see `isUriQueryCredentialKeyIgnoreCase`).
        var is_credential = isUriQueryCredentialKeyIgnoreCase(key);
        if (!is_credential) {
            // Decode the key the way libpq does before comparing it, so
            // e.g. `%70assword` is recognized as `password`. If the key's
            // own encoding is malformed, libpq would abort the whole parse
            // right there (quoting the key, not this value) -- but decide
            // conservatively rather than assume the value is safe.
            decode_key: {
                var decoded_key = percentDecodeIfValidAlloc(alloc, key) catch |err| switch (err) {
                    error.InvalidPercentEncoding => {
                        is_credential = try couldDecodeToCredentialKeyAlloc(alloc, key);
                        break :decode_key;
                    },
                    else => return err,
                };
                defer decoded_key.deinit(alloc);
                is_credential = isUriQueryCredentialKeyExact(decoded_key.items);
            }
        }
        if (!is_credential) continue;

        if (value.len >= min_password_len) try appendOwnedDupe(alloc, out, value);

        var decoded = percentDecodeIfValidAlloc(alloc, value) catch |err| switch (err) {
            error.InvalidPercentEncoding => continue,
            else => return err,
        };
        defer decoded.deinit(alloc);
        if (decoded.items.len >= min_password_len and !std.mem.eql(u8, decoded.items, value)) {
            try appendOwnedSlice(alloc, out, &decoded);
        }
    }
}

/// Percent-decodes `s`, returning `error.InvalidPercentEncoding` if any `%`
/// is not followed by two valid hex digits -- matching libpq's
/// `conninfo_uri_decode`, which treats that as malformed and echoes the
/// original (undecoded) text rather than attempting a partial decode.
fn percentDecodeIfValidAlloc(alloc: Allocator, s: []const u8) !std.ArrayListUnmanaged(u8) {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(alloc);
    var i: usize = 0;
    while (i < s.len) {
        if (s[i] == '%') {
            if (i + 2 >= s.len) return error.InvalidPercentEncoding;
            const hi = std.fmt.charToDigit(s[i + 1], 16) catch return error.InvalidPercentEncoding;
            const lo = std.fmt.charToDigit(s[i + 2], 16) catch return error.InvalidPercentEncoding;
            try out.append(alloc, @intCast(hi * 16 + lo));
            i += 3;
        } else {
            try out.append(alloc, s[i]);
            i += 1;
        }
    }
    return out;
}

/// Extracts the `password=...` value from a key/value conninfo string,
/// supporting both the quoted form (with backslash escapes) and the bare
/// unquoted form. Collects both the raw-as-written and decoded forms, since
/// libpq's diagnostics are not consistent about which one they echo. Only
/// candidates at least `min_password_len` long are collected; see
/// `collectUriCredentialsAlloc` for why callers choose different floors.
fn collectConninfoCredentialsAlloc(alloc: Allocator, dsn: []const u8, min_password_len: usize, out: *std.ArrayListUnmanaged([]const u8)) !void {
    var i: usize = 0;
    while (i < dsn.len) {
        while (i < dsn.len and std.ascii.isWhitespace(dsn[i])) i += 1;
        if (i >= dsn.len) break;
        const key_start = i;
        while (i < dsn.len and dsn[i] != '=' and !std.ascii.isWhitespace(dsn[i])) i += 1;
        const key = dsn[key_start..i];
        while (i < dsn.len and std.ascii.isWhitespace(dsn[i])) i += 1;
        if (i >= dsn.len or dsn[i] != '=') break; // not a well-formed token; stop rather than guess
        i += 1;
        while (i < dsn.len and std.ascii.isWhitespace(dsn[i])) i += 1;
        const is_password = std.ascii.eqlIgnoreCase(key, "password");
        if (i < dsn.len and (dsn[i] == '\'' or dsn[i] == '"')) {
            const quote = dsn[i];
            const raw_start = i + 1;
            i += 1;
            var decoded: std.ArrayListUnmanaged(u8) = .empty;
            defer decoded.deinit(alloc);
            while (i < dsn.len and dsn[i] != quote) {
                if (dsn[i] == '\\' and i + 1 < dsn.len) {
                    if (is_password) try decoded.append(alloc, dsn[i + 1]);
                    i += 2;
                } else {
                    if (is_password) try decoded.append(alloc, dsn[i]);
                    i += 1;
                }
            }
            const raw_end = i;
            if (i < dsn.len) i += 1; // consume closing quote
            if (is_password) {
                const raw = dsn[raw_start..raw_end];
                if (raw.len >= min_password_len) try appendOwnedDupe(alloc, out, raw);
                if (decoded.items.len >= min_password_len) {
                    try appendOwnedSlice(alloc, out, &decoded);
                }
            }
        } else {
            const value_start = i;
            while (i < dsn.len and !std.ascii.isWhitespace(dsn[i])) i += 1;
            if (is_password) {
                const value = dsn[value_start..i];
                if (value.len >= min_password_len) try appendOwnedDupe(alloc, out, value);
            }
        }
    }
}

/// Replaces every occurrence of any `secrets` entry in `message` with
/// `[redacted]`, preferring the longest match at each position so that, for
/// example, a captured "user:password" fragment is not left half-redacted by
/// a shorter overlapping match.
fn redactLiteralSecretsAlloc(alloc: Allocator, message: []const u8, secrets: []const []const u8) ![]u8 {
    if (secrets.len == 0) return try alloc.dupe(u8, message);
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(alloc);
    var i: usize = 0;
    while (i < message.len) {
        var matched_len: usize = 0;
        for (secrets) |secret| {
            if (secret.len == 0 or secret.len <= matched_len) continue;
            if (i + secret.len <= message.len and std.mem.eql(u8, message[i .. i + secret.len], secret)) {
                matched_len = secret.len;
            }
        }
        if (matched_len > 0) {
            try out.appendSlice(alloc, "[redacted]");
            i += matched_len;
        } else {
            try out.append(alloc, message[i]);
            i += 1;
        }
    }
    return try out.toOwnedSlice(alloc);
}

/// Redacts a quoted (`"..."` or `'...'`) token in `message` whenever its
/// content is found inside one of `candidates` (the DSN's password and,
/// for URI-style DSNs, the full `user:password` userinfo). This has no
/// minimum length -- see the module comment above -- because it only ever
/// touches text libpq has already set off in quotes as an echoed token, for
/// example `invalid percent-encoded token: "%z"`; it never reaches into
/// unquoted free text, so a one-character password is caught without
/// risking a short username mangling unrelated words like "refused" or an
/// unrelated quoted token like a hostname.
fn redactQuotedCredentialTokensAlloc(alloc: Allocator, message: []const u8, candidates: []const []const u8) ![]u8 {
    if (candidates.len == 0) return try alloc.dupe(u8, message);
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(alloc);
    var i: usize = 0;
    while (i < message.len) {
        const quote = message[i];
        if (quote != '"' and quote != '\'') {
            try out.append(alloc, message[i]);
            i += 1;
            continue;
        }
        const span_len = conninfoValueLen(message[i..]);
        // `conninfoValueLen` returns the whole message tail when it never
        // finds a closing quote; only treat this as a quoted token when a
        // matching close was actually found.
        if (span_len < 2 or message[i + span_len - 1] != quote) {
            try out.append(alloc, message[i]);
            i += 1;
            continue;
        }
        const token = message[i + 1 .. i + span_len - 1];
        var matched = false;
        if (token.len > 0) {
            for (candidates) |candidate| {
                if (candidate.len == 0) continue;
                if (std.mem.indexOf(u8, candidate, token) != null) {
                    matched = true;
                    break;
                }
            }
        }
        if (matched) {
            try out.append(alloc, quote);
            try out.appendSlice(alloc, "[redacted]");
            try out.append(alloc, quote);
        } else {
            try out.appendSlice(alloc, message[i .. i + span_len]);
        }
        i += span_len;
    }
    return try out.toOwnedSlice(alloc);
}

fn matchUriSchemeLen(s: []const u8) ?usize {
    if (std.mem.startsWith(u8, s, "postgresql://")) return "postgresql://".len;
    if (std.mem.startsWith(u8, s, "postgres://")) return "postgres://".len;
    return null;
}

/// Index just past the end of a URI-looking token embedded in free-form text
/// (a libpq error message), i.e. up to the next whitespace, closing
/// quote/bracket, or comma.
fn uriTokenLen(s: []const u8) usize {
    var i: usize = 0;
    while (i < s.len) : (i += 1) {
        switch (s[i]) {
            ' ', '\t', '\r', '\n', '"', '\'', ')', ']', ',' => return i,
            else => {},
        }
    }
    return s.len;
}

/// Scans `message` for `postgres://` / `postgresql://` tokens and redacts
/// the userinfo (user and password) of each one in place, leaving the
/// scheme, host, port, database, and query string untouched. This catches
/// credentials in a connection URI even when they were not captured from the
/// attempted DSN itself (for example, a message that names a different or
/// only partially-parsed URI).
fn redactUriCredentialsAlloc(alloc: Allocator, message: []const u8) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(alloc);
    var i: usize = 0;
    while (i < message.len) {
        const scheme_len = matchUriSchemeLen(message[i..]) orelse {
            try out.append(alloc, message[i]);
            i += 1;
            continue;
        };
        const token_len = uriTokenLen(message[i..]);
        const token = message[i .. i + token_len];
        const rest = token[scheme_len..];
        const authority_len = uriAuthorityLen(rest);
        const authority = rest[0..authority_len];
        try out.appendSlice(alloc, token[0..scheme_len]);
        if (std.mem.lastIndexOfScalar(u8, authority, '@')) |at| {
            if (at > 0) try out.appendSlice(alloc, "[redacted]");
            try out.appendSlice(alloc, "@");
            try out.appendSlice(alloc, authority[at + 1 ..]);
        } else {
            try out.appendSlice(alloc, authority);
        }
        try out.appendSlice(alloc, rest[authority_len..]);
        i += token_len;
    }
    return try out.toOwnedSlice(alloc);
}

/// Percent-decodes `s`, passing any malformed `%` escape through literally
/// (just the `%` character, so decoding continues past it) rather than
/// failing. Unlike `percentDecodeIfValidAlloc`, this never errors -- it is
/// only used to decide, conservatively, whether free-form message text
/// that libpq would itself treat as malformed could still read as a
/// credential-bearing key; it never recovers an actual credential value.
fn lenientPercentDecodeAlloc(alloc: Allocator, s: []const u8) !std.ArrayListUnmanaged(u8) {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(alloc);
    var i: usize = 0;
    while (i < s.len) {
        if (s[i] == '%' and i + 2 < s.len) {
            const hi = std.fmt.charToDigit(s[i + 1], 16) catch null;
            const lo = std.fmt.charToDigit(s[i + 2], 16) catch null;
            if (hi != null and lo != null) {
                try out.append(alloc, @intCast(hi.? * 16 + lo.?));
                i += 3;
                continue;
            }
        }
        try out.append(alloc, s[i]);
        i += 1;
    }
    return out;
}

/// True if `s`, compared case-insensitively, ends with "password". This is
/// deliberately a suffix check rather than an exact-match one so a single
/// test also covers `sslpassword` (which ends in "password") the same way
/// `matchCredentialKeyLen` below covers both of `uri_query_credential_keys`
/// with one check, while still rejecting a look-alike like
/// "password_hash" (which does not end in "password").
fn endsWithPasswordIgnoreCase(s: []const u8) bool {
    const needle = "password";
    if (s.len < needle.len) return false;
    return std.ascii.eqlIgnoreCase(s[s.len - needle.len ..], needle);
}

/// Characters that can appear in a conninfo key or a URI query-string key,
/// whether or not it is percent-encoded.
fn credentialKeyTokenLen(s: []const u8) usize {
    var i: usize = 0;
    while (i < s.len) : (i += 1) {
        switch (s[i]) {
            'a'...'z', 'A'...'Z', '0'...'9', '%', '-', '_', '.', '~' => {},
            else => break,
        }
    }
    return i;
}

/// Returns the length of a conninfo/URI-query key token at the start of
/// `s` whose decoded form ends in "password" -- covering both `password`
/// and `sslpassword` (see `endsWithPasswordIgnoreCase`) -- including when
/// the token is percent-encoded, e.g. `%70assword` or `%73slpassword`.
/// This is defense in depth on top of `collectUriQueryCredentialsAlloc`:
/// it scans the libpq-produced message text directly, so it also catches a
/// credential-bearing key that message happens to quote even when it was
/// not recognized while parsing `dsn` (for example a different URI quoted
/// in the message, or a key `dsn`'s own parse never reached). A malformed
/// escape inside the token is passed through literally rather than
/// rejected (see `lenientPercentDecodeAlloc`), matching the conservative
/// stance taken in `couldDecodeToCredentialKeyAlloc`.
fn matchCredentialKeyLen(alloc: Allocator, s: []const u8) !?usize {
    const token_len = credentialKeyTokenLen(s);
    if (token_len == 0) return null;
    const token = s[0..token_len];
    if (std.mem.indexOfScalar(u8, token, '%') == null) {
        // Fast, allocation-free path for the common literal case.
        return if (endsWithPasswordIgnoreCase(token)) token_len else null;
    }
    var decoded = try lenientPercentDecodeAlloc(alloc, token);
    defer decoded.deinit(alloc);
    return if (endsWithPasswordIgnoreCase(decoded.items)) token_len else null;
}

/// Index just past the end of a conninfo-style value starting at `s`:
/// a quoted (optionally backslash-escaped) run, or an unquoted run bounded
/// by whitespace or a URI query-string separator.
fn conninfoValueLen(s: []const u8) usize {
    if (s.len == 0) return 0;
    if (s[0] == '\'' or s[0] == '"') {
        const quote = s[0];
        var i: usize = 1;
        while (i < s.len) {
            if (s[i] == '\\' and i + 1 < s.len) {
                i += 2;
                continue;
            }
            if (s[i] == quote) {
                i += 1;
                break;
            }
            i += 1;
        }
        return i;
    }
    var i: usize = 0;
    while (i < s.len) : (i += 1) {
        switch (s[i]) {
            ' ', '\t', '\r', '\n', '&', ')', ']', ',' => return i,
            else => {},
        }
    }
    return s.len;
}

/// Redacts `password=value` (quoted or unquoted) fragments anywhere in
/// `message`, such as a conninfo key/value pair or a URI query parameter --
/// including a percent-encoded key spelling like `%70assword=` (see
/// `matchCredentialKeyLen`). The key and `=` are kept so the reader can
/// still see a password was present; only the value is replaced.
fn redactConninfoPasswordFieldsAlloc(alloc: Allocator, message: []const u8) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(alloc);
    var i: usize = 0;
    while (i < message.len) {
        // Consume each token once. Retrying at every suffix would repeatedly
        // scan and percent-decode a long malformed key (quadratic work) before
        // the final output-length cap is applied.
        const token_len = credentialKeyTokenLen(message[i..]);
        if (token_len == 0) {
            try out.append(alloc, message[i]);
            i += 1;
            continue;
        }
        const token = message[i .. i + token_len];
        const key_len = (try matchCredentialKeyLen(alloc, token)) orelse {
            try out.appendSlice(alloc, token);
            i += token_len;
            continue;
        };
        var j = i + key_len;
        while (j < message.len and (message[j] == ' ' or message[j] == '\t')) j += 1;
        if (j >= message.len or message[j] != '=') {
            try out.appendSlice(alloc, token);
            i += token_len;
            continue;
        }
        j += 1;
        while (j < message.len and (message[j] == ' ' or message[j] == '\t')) j += 1;
        const value_len = conninfoValueLen(message[j..]);
        try out.appendSlice(alloc, message[i .. i + key_len]);
        try out.appendSlice(alloc, "=[redacted]");
        i = j + value_len;
    }
    return try out.toOwnedSlice(alloc);
}

test "postgres libpq connection-error sanitizer redacts a malformed percent-encoded URI password" {
    const alloc = std.testing.allocator;
    const dsn = "postgres://u:p%zzw0rd@h/db";
    const message = "invalid percent-encoded token: \"p%zzw0rd\"";
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, message, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "p%zzw0rd") == null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "invalid percent-encoded token") != null);
}

test "postgres libpq connection-error sanitizer redacts a one-character malformed-escape password" {
    const alloc = std.testing.allocator;
    const dsn = "postgres://u:%z@h/db";
    const message = "invalid percent-encoded token: \"%z\"";
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, message, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "%z") == null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "invalid percent-encoded token") != null);
}

test "postgres libpq connection-error sanitizer redacts a two-character trailing-escape password" {
    const alloc = std.testing.allocator;
    const dsn = "postgres://u:a%@h/db";
    const message = "invalid percent-encoded token: \"a%\"";
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, message, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "a%") == null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "invalid percent-encoded token") != null);
}

test "postgres libpq connection-error sanitizer keeps an unrelated quoted token intact with a one-character username" {
    const alloc = std.testing.allocator;
    const dsn = "postgres://u:a-genuinely-long-realistic-password-123@host:5432/db";
    const message = "connection to server at \"host\" (1.2.3.4), port 5432 failed: Connection refused\n" ++
        "\tIs the server running on that host and accepting TCP/IP connections?";
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, message, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "a-genuinely-long-realistic-password-123") == null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "Connection refused") != null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "\"host\"") != null);
}

test "postgres libpq connection-error sanitizer redacts credentials from an embedded malformed URI" {
    const alloc = std.testing.allocator;
    const dsn = "postgres://u:p%zzw0rd@h/db";
    const message = "invalid percent-encoded token: \"postgres://u:p%zzw0rd@h/db\"";
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, message, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "p%zzw0rd") == null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "u:p") == null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "h/db") != null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "invalid percent-encoded token") != null);
}

test "postgres libpq connection-error sanitizer keeps the reason for an unrelated failure with a valid password" {
    const alloc = std.testing.allocator;
    const dsn = "postgres://u:realpass@host:5432/db";
    const message = "connection to server at \"host\" (1.2.3.4), port 5432 failed: Connection refused\n" ++
        "\tIs the server running on that host and accepting TCP/IP connections?";
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, message, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "realpass") == null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "Connection refused") != null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "port 5432") != null);
}

test "postgres libpq connection-error sanitizer redacts key/value conninfo password fragments" {
    const alloc = std.testing.allocator;
    const dsn = "host=h dbname=db user=u password='sec ret'";
    const message = "connection info string contains password='sec ret' near \"password\": " ++
        "FATAL: password authentication failed";
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, message, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "sec ret") == null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "password authentication failed") != null);
}

test "postgres libpq connection-error sanitizer redacts a secret-store-resolved password literal" {
    const alloc = std.testing.allocator;
    const secret_value = "kv-resolved-S3cr3t!";
    const dsn = try std.fmt.allocPrint(alloc, "postgres://svc:{s}@db.internal:5432/app", .{secret_value});
    defer alloc.free(dsn);
    const message = try std.fmt.allocPrint(alloc, "could not parse connection string: \"{s}\"", .{dsn});
    defer alloc.free(message);
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, message, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, secret_value) == null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "could not parse connection string") != null);
}

test "postgres libpq connection-error sanitizer bounds output length" {
    const alloc = std.testing.allocator;
    const dsn = "postgres://u:realpass@host/db";
    var long: std.ArrayListUnmanaged(u8) = .empty;
    defer long.deinit(alloc);
    try long.appendSlice(alloc, "connection failed: realpass ");
    while (long.items.len < 1000) try long.appendSlice(alloc, "padding ");
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, long.items, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(sanitized.len <= max_sanitized_connection_error_len);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "realpass") == null);
}

test "postgres libpq connection-error sanitizer handles a real libpq conninfo parse error" {
    const alloc = std.testing.allocator;
    var executor = Executor.init(alloc) catch return error.SkipZigTest;
    defer executor.deinit();
    const dsn = "postgres://u:p%zzw0rd@h/db";
    const dsn_z = try alloc.dupeSentinel(u8, dsn, 0);
    defer alloc.free(dsn_z);
    const conn = executor.pqconnectdb(dsn_z.ptr) orelse return error.SkipZigTest;
    defer executor.pqfinish(conn);
    if (executor.pqstatus(conn) == CONNECTION_OK) return error.SkipZigTest;
    const raw = std.mem.trim(u8, std.mem.span(executor.pqerrorMessage(conn)), " \t\r\n");
    if (raw.len == 0) return error.SkipZigTest;
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, raw, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "p%zzw0rd") == null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "u:p%zzw0rd") == null);
}

test "postgres libpq connection-error sanitizer redacts a malformed percent-encoded URI query password" {
    const alloc = std.testing.allocator;
    const dsn = "postgresql://localhost/db?password=supersecret%zz";
    const message = "invalid percent-encoded token: \"supersecret%zz\"";
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, message, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "supersecret") == null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "invalid percent-encoded token") != null);
}

test "postgres libpq connection-error sanitizer redacts a short malformed URI query password" {
    const alloc = std.testing.allocator;
    const dsn = "postgresql://localhost/db?password=%z";
    const message = "invalid percent-encoded token: \"%z\"";
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, message, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "%z") == null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "invalid percent-encoded token") != null);
}

test "postgres libpq connection-error sanitizer redacts a URI query password alongside other params" {
    const alloc = std.testing.allocator;
    const dsn = "postgresql://localhost/db?sslmode=require&password=supersecret%zz&connect_timeout=5";
    const message = "invalid percent-encoded token: \"supersecret%zz\"";
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, message, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "supersecret") == null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "invalid percent-encoded token") != null);
}

test "postgres libpq connection-error sanitizer redacts password= structurally inside a quoted URI query string" {
    const alloc = std.testing.allocator;
    const dsn = "postgresql://localhost/db?password=realquerysecret";
    const message = "could not parse connection string: \"postgresql://localhost/db?password=realquerysecret\"";
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, message, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "realquerysecret") == null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "password=[redacted]") != null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "could not parse connection string") != null);
}

test "postgres libpq connection-error sanitizer handles a real libpq malformed URI query password" {
    const alloc = std.testing.allocator;
    var executor = Executor.init(alloc) catch return error.SkipZigTest;
    defer executor.deinit();
    const dsn = "postgresql://localhost/db?password=supersecret%zz";
    const dsn_z = try alloc.dupeSentinel(u8, dsn, 0);
    defer alloc.free(dsn_z);
    const conn = executor.pqconnectdb(dsn_z.ptr) orelse return error.SkipZigTest;
    defer executor.pqfinish(conn);
    if (executor.pqstatus(conn) == CONNECTION_OK) return error.SkipZigTest;
    const raw = std.mem.trim(u8, std.mem.span(executor.pqerrorMessage(conn)), " \t\r\n");
    if (raw.len == 0) return error.SkipZigTest;
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, raw, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "supersecret") == null);
}

test "postgres libpq connection-error sanitizer redacts a malformed value behind a percent-encoded password key" {
    // libpq's conninfo_uri_parse_params decodes the *key* before comparing
    // it against known keywords: `%70assword` decodes to `password`, which
    // is why libpq accepts it as the password option at all, and why its
    // (malformed) value is then quoted verbatim in the decode error.
    const alloc = std.testing.allocator;
    const dsn = "postgresql://localhost/db?%70assword=supersecret%zz";
    const message = "invalid percent-encoded token: \"supersecret%zz\"";
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, message, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "supersecret") == null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "invalid percent-encoded token") != null);
}

test "postgres libpq connection-error sanitizer redacts a malformed value behind a percent-encoded sslpassword key" {
    const alloc = std.testing.allocator;
    const dsn = "postgresql://localhost/db?%73slpassword=supersecret%zz";
    const message = "invalid percent-encoded token: \"supersecret%zz\"";
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, message, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "supersecret") == null);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "invalid percent-encoded token") != null);
}

test "postgres libpq connection-error sanitizer matches libpq's case-sensitive decoded URI query key lookup" {
    // libpq's own keyword lookup (`conninfo_find`) is a case-sensitive
    // `strcmp` against the registered option names, which are lowercase.
    // `%50assword` decodes to "Password", which libpq rejects as an
    // unrecognized URI query parameter rather than as the `password`
    // option -- it never uses that value as a real credential, quoting
    // only the key (not the value) in its error. `isUriQueryCredentialKeyExact`
    // mirrors that case sensitivity for a key already decoded by
    // `collectUriQueryCredentialsAlloc`, unlike the case-insensitive
    // `isUriQueryCredentialKeyIgnoreCase` baseline used for a key exactly
    // as written (not percent-encoded), which stays deliberately
    // over-cautious. See the structural test below for the defense-in-depth
    // pass, which still catches this case independently.
    try std.testing.expect(!isUriQueryCredentialKeyExact("Password"));
    try std.testing.expect(isUriQueryCredentialKeyExact("password"));
    try std.testing.expect(isUriQueryCredentialKeyExact("sslpassword"));
}

test "postgres libpq connection-error sanitizer still redacts a mixed-case percent-encoded key structurally" {
    // Even though `%50assword` does not match libpq's case-sensitive
    // `password` keyword (see the test above), the structural
    // `password=` redaction pass matches case-insensitively -- the same
    // deliberately conservative choice already made for a literal,
    // non-percent-encoded key -- so a message that echoes this query
    // fragment verbatim (e.g. a quoted connection string) still has its
    // value redacted.
    const alloc = std.testing.allocator;
    const dsn = "postgresql://localhost/db?%50assword=realquerysecret";
    const message = "could not parse connection string: \"postgresql://localhost/db?%50assword=realquerysecret\"";
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, message, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "realquerysecret") == null);
}

test "postgres libpq connection-error sanitizer handles a real libpq percent-encoded-key query password" {
    const alloc = std.testing.allocator;
    var executor = Executor.init(alloc) catch return error.SkipZigTest;
    defer executor.deinit();
    const dsn = "postgresql://localhost/db?%70assword=supersecret%zz";
    const dsn_z = try alloc.dupeSentinel(u8, dsn, 0);
    defer alloc.free(dsn_z);
    const conn = executor.pqconnectdb(dsn_z.ptr) orelse return error.SkipZigTest;
    defer executor.pqfinish(conn);
    if (executor.pqstatus(conn) == CONNECTION_OK) return error.SkipZigTest;
    const raw = std.mem.trim(u8, std.mem.span(executor.pqerrorMessage(conn)), " \t\r\n");
    if (raw.len == 0) return error.SkipZigTest;
    const sanitized = try sanitizeConnectionErrorAlloc(alloc, raw, dsn);
    defer alloc.free(sanitized);
    try std.testing.expect(std.mem.indexOf(u8, sanitized, "supersecret") == null);
}

test "postgres libpq connection-error sanitizer redacts literal hash characters in credentials" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{
        "postgresql://localhost/db?password=abc#supersecret%zz",
        "postgresql://localhost/db?%73slpassword=abc#supersecret%zz",
        "postgresql://u:abc#supersecret%zz@localhost/db",
    }) |dsn| {
        const sanitized = try sanitizeConnectionErrorAlloc(alloc, "invalid percent-encoded token: \"abc#supersecret%zz\"", dsn);
        defer alloc.free(sanitized);
        try std.testing.expectEqualStrings("invalid percent-encoded token: \"[redacted]\"", sanitized);
    }
}

test "postgres libpq connection-error sanitizer handles real libpq credentials containing a hash" {
    const alloc = std.testing.allocator;
    var executor = Executor.init(alloc) catch return error.SkipZigTest;
    defer executor.deinit();
    for ([_][]const u8{
        "postgresql://localhost/db?password=abc#supersecret%zz",
        "postgresql://localhost/db?sslpassword=abc#supersecret%zz",
        "postgresql://u:abc#supersecret%zz@localhost/db",
    }) |dsn| {
        const dsn_z = try alloc.dupeSentinel(u8, dsn, 0);
        defer alloc.free(dsn_z);
        const conn = executor.pqconnectdb(dsn_z.ptr) orelse return error.TestUnexpectedResult;
        defer executor.pqfinish(conn);
        try std.testing.expect(executor.pqstatus(conn) != CONNECTION_OK);
        const raw = std.mem.trim(u8, std.mem.span(executor.pqerrorMessage(conn)), " \t\r\n");
        try std.testing.expect(std.mem.indexOf(u8, raw, "supersecret") != null);
        const sanitized = try sanitizeConnectionErrorAlloc(alloc, raw, dsn);
        defer alloc.free(sanitized);
        try std.testing.expect(std.mem.indexOf(u8, sanitized, "supersecret") == null);
    }
}

test "postgres libpq connection-error sanitizer bounds allocation work for long malformed keys" {
    const alloc = std.testing.allocator;
    for ([_]usize{ 1024, 8192 }) |len| {
        const key = try alloc.alloc(u8, len);
        defer alloc.free(key);
        @memset(key, 'a');
        const dsn = try std.fmt.allocPrint(alloc, "postgresql://localhost/db?{s}%zz=x", .{key});
        defer alloc.free(dsn);
        const message = try std.fmt.allocPrint(alloc, "invalid percent-encoded token: \"{s}%zz\"", .{key});
        defer alloc.free(message);
        // Count cumulative allocation bytes rather than wall-clock time: a
        // malformed key must not be decoded again at every character offset.
        var counting = std.testing.FailingAllocator.init(alloc, .{});
        const sanitized = try sanitizeConnectionErrorAlloc(counting.allocator(), message, dsn);
        defer counting.allocator().free(sanitized);
        try std.testing.expect(counting.allocated_bytes <= 64 * message.len);
        try std.testing.expect(sanitized.len <= max_sanitized_connection_error_len);
    }
}

fn sanitizeConnectionErrorAllocationFixture(alloc: Allocator) !void {
    const Case = struct { dsn: []const u8, message: []const u8 };
    const cases = [_]Case{
        .{
            .dsn = "postgresql://localhost/db?password=abc#supersecret%zz",
            .message = "invalid percent-encoded token: \"abc#supersecret%zz\"",
        },
        .{
            .dsn = "postgresql://u:abc#supersecret%zz@localhost/db",
            .message = "invalid percent-encoded token: \"abc#supersecret%zz\"",
        },
        .{
            .dsn = "postgres://u:p%zzw0rd@h/db",
            .message = "invalid percent-encoded token: \"p%zzw0rd\"",
        },
        .{
            .dsn = "postgresql://localhost/db?password=supersecret%zz",
            .message = "invalid percent-encoded token: \"supersecret%zz\"",
        },
        .{
            .dsn = "postgresql://localhost/db?sslmode=require&password=sup%zzer&connect_timeout=5",
            .message = "invalid percent-encoded token: \"sup%zzer\"",
        },
        .{
            .dsn = "postgresql://localhost/db?%70assword=supersecret%zz",
            .message = "invalid percent-encoded token: \"supersecret%zz\"",
        },
        .{
            .dsn = "host=h dbname=db user=u password='sec ret'",
            .message = "connection info string contains password='sec ret' near \"password\": " ++
                "FATAL: password authentication failed",
        },
        .{
            .dsn = "postgres://u:realpass@host:5432/db",
            .message = "connection to server at \"host\" (1.2.3.4), port 5432 failed: Connection refused",
        },
    };
    for (cases) |case| {
        const sanitized = try sanitizeConnectionErrorAlloc(alloc, case.message, case.dsn);
        alloc.free(sanitized);
    }
}

test "postgres libpq connection-error sanitizer is allocation-failure safe" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, sanitizeConnectionErrorAllocationFixture, .{});
}

/// Drive libpq's nonblocking command protocol without ever calling
/// PQgetResult while libpq still needs socket input. In particular, flushing
/// must service readable notices/errors as well as writable output, and every
/// result (including the final null result) gets its own PQisBusy gate.
fn readSingleAsyncResultWithDeadline(driver: anytype, deadline_ns: ?u64, cancellation: ?CancellationToken) !?*PGresult {
    while (true) {
        if (cancellation) |value| if (value.isCancelled()) return error.Cancelled;
        if (deadline_ns) |deadline| try ensureDeadline(deadline);
        const flush_status = driver.flush();
        if (flush_status == 0) break;
        if (flush_status < 0) return error.ForeignQueryFailed;

        const ready = driver.wait(
            std.posix.POLL.IN | std.posix.POLL.OUT,
            deadline_ns,
            cancellation,
        ) catch |err| switch (err) {
            error.Timeout => {
                if (cancellation) |value| if (value.isCancelled()) return error.Cancelled;
                if (deadline_ns) |deadline| try ensureDeadline(deadline);
                continue;
            },
            else => return err,
        };
        if (ready & std.posix.POLL.IN != 0) {
            if (cancellation) |value| if (value.isCancelled()) return error.Cancelled;
            if (driver.consumeInput() != 1) return error.ForeignQueryFailed;
        }
    }

    var first_result: ?*PGresult = null;
    errdefer if (first_result) |result| driver.clear(result);
    var saw_extra_result = false;
    while (true) {
        while (driver.isBusy() != 0) {
            if (cancellation) |value| if (value.isCancelled()) return error.Cancelled;
            const ready = driver.wait(std.posix.POLL.IN, deadline_ns, cancellation) catch |err| switch (err) {
                error.Timeout => {
                    if (cancellation) |value| if (value.isCancelled()) return error.Cancelled;
                    if (deadline_ns) |deadline| try ensureDeadline(deadline);
                    continue;
                },
                else => return err,
            };
            if (ready & std.posix.POLL.IN == 0) continue;
            if (cancellation) |value| if (value.isCancelled()) return error.Cancelled;
            if (driver.consumeInput() != 1) return error.ForeignQueryFailed;
        }

        if (deadline_ns) |deadline| try ensureDeadline(deadline);
        const next_result = driver.getResult() orelse break;
        if (first_result == null) {
            first_result = next_result;
        } else {
            saw_extra_result = true;
            driver.clear(next_result);
        }
    }

    const result = first_result orelse return error.ForeignQueryFailed;
    if (driver.restoreBlocking() != 0) return error.ForeignQueryFailed;
    if (saw_extra_result) return error.ForeignQueryFailed;
    return result;
}

const LazyExecutor = struct {
    alloc: Allocator,
    mutex: Mutex = .unlocked,
    executor: ?*Executor = null,
    warned_init_failure: bool = false,

    pub fn deinit(self: *@This()) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.executor) |executor| {
            executor.deinit();
            self.alloc.destroy(executor);
            self.executor = null;
        }
    }

    fn asQueryExecutor(self: *@This()) postgres_source.QueryExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .transaction_envelopes = true,
                .begin_replication_snapshot_query = beginReplicationSnapshotQuery,
                .deinit = deinitQueryExecutor,
                .query = query,
                .statistics = statistics,
                .statistics_with_deadline = statisticsWithDeadline,
                .discover_columns = discoverColumns,
                .refresh_columns = refreshColumns,
                .begin_snapshot_query = beginSnapshotQuery,
                .begin_prepared_replication_snapshot = beginPreparedReplicationSnapshot,
                .begin_subscription_snapshot = beginSubscriptionSnapshot,
                .poll_subscription = pollSubscription,
                .acknowledge_subscription = acknowledgeSubscription,
                .prepare_replication = prepareReplication,
                .poll_changes = pollChanges,
                .cleanup_replication = cleanupReplication,
            },
        };
    }

    fn ensureExecutor(self: *@This(), execution_deadline_ns: ?u64) !*Executor {
        try lockUntil(&self.mutex, execution_deadline_ns);
        defer self.mutex.unlock();
        if (self.executor) |executor| return executor;

        const executor = self.alloc.create(Executor) catch |err| return err;
        errdefer self.alloc.destroy(executor);
        executor.* = Executor.init(self.alloc) catch |err| {
            if (!self.warned_init_failure and err != error.OutOfMemory) {
                std.log.warn("postgres libpq unavailable until first successful Postgres-backed use: {}", .{err});
                self.warned_init_failure = true;
            }
            return err;
        };
        self.executor = executor;
        return executor;
    }

    fn deinitQueryExecutor(ptr: *anyopaque, alloc: Allocator) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.deinit();
        alloc.destroy(self);
    }

    fn query(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, prepared: sql.PreparedQuery, execution_deadline_ns: ?u64, cancellation: ?CancellationToken) !foreign_source.QueryResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = self.ensureExecutor(execution_deadline_ns) catch |err| {
            var owned = prepared;
            owned.deinit(alloc);
            return err;
        };
        return try executor.asQueryExecutor().query(alloc, dsn, prepared, execution_deadline_ns, cancellation);
    }

    fn statistics(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, table: []const u8) !foreign_source.TableStatistics {
        return try statisticsWithDeadline(ptr, alloc, dsn, table, null);
    }

    fn statisticsWithDeadline(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, table: []const u8, execution_deadline_ns: ?u64) !foreign_source.TableStatistics {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor(execution_deadline_ns);
        return try executor.asQueryExecutor().statisticsWithDeadline(alloc, dsn, table, execution_deadline_ns);
    }

    fn discoverColumns(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, table: []const u8, execution_deadline_ns: ?u64) ![]foreign_source.Column {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor(execution_deadline_ns);
        return try executor.asQueryExecutor().discoverColumns(alloc, dsn, table, execution_deadline_ns);
    }

    fn refreshColumns(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, table: []const u8, execution_deadline_ns: ?u64) ![]foreign_source.Column {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor(execution_deadline_ns);
        return try executor.asQueryExecutor().refreshColumns(alloc, dsn, table, execution_deadline_ns);
    }

    fn beginSnapshotQuery(ptr: *anyopaque, alloc: Allocator, dsn: []const u8) !postgres_source.QueryExecutor.SnapshotQuery {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor(null);
        return try executor.asQueryExecutor().beginSnapshotQuery(alloc, dsn);
    }

    fn beginReplicationSnapshotQuery(ptr: *anyopaque, alloc: Allocator, dsn: []const u8) !postgres_source.QueryExecutor.SnapshotQuery {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor(null);
        return try executor.asQueryExecutor().beginReplicationSnapshotQuery(alloc, dsn);
    }

    fn beginPreparedReplicationSnapshot(
        ptr: *anyopaque,
        alloc: Allocator,
        dsn: []const u8,
        params: foreign_source.ReplicationPollParams,
        execution_deadline_ns: u64,
    ) !postgres_source.QueryExecutor.PreparedReplicationSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor(execution_deadline_ns);
        return try executor.asQueryExecutor().beginPreparedReplicationSnapshot(
            alloc,
            dsn,
            params,
            execution_deadline_ns,
        );
    }

    fn prepareReplication(
        ptr: *anyopaque,
        alloc: Allocator,
        dsn: []const u8,
        params: foreign_source.ReplicationPollParams,
    ) !foreign_source.ReplicationPrepareResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor(null);
        return try executor.asQueryExecutor().prepareReplication(alloc, dsn, params);
    }

    fn pollChanges(
        ptr: *anyopaque,
        alloc: Allocator,
        dsn: []const u8,
        params: foreign_source.ReplicationPollParams,
    ) !foreign_source.ReplicationPollResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor(null);
        return try executor.asQueryExecutor().pollChanges(alloc, dsn, params);
    }

    fn beginSubscriptionSnapshot(ptr: *anyopaque, a: Allocator, dsn: []const u8, request: foreign_source.SubscriptionRequest) !postgres_source.QueryExecutor.PreparedSubscriptionSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor(request.execution_deadline_ns);
        return executor.asQueryExecutor().beginSubscriptionSnapshot(a, dsn, request);
    }
    fn pollSubscription(ptr: *anyopaque, a: Allocator, dsn: []const u8, request: foreign_source.SubscriptionRequest) !foreign_source.SubscriptionPollResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor(request.execution_deadline_ns);
        return executor.asQueryExecutor().pollSubscription(a, dsn, request);
    }
    fn acknowledgeSubscription(ptr: *anyopaque, a: Allocator, dsn: []const u8, request: foreign_source.SubscriptionRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor(request.execution_deadline_ns);
        return executor.asQueryExecutor().acknowledgeSubscription(a, dsn, request);
    }

    fn cleanupReplication(
        ptr: *anyopaque,
        alloc: Allocator,
        dsn: []const u8,
        params: foreign_source.ReplicationCleanupParams,
    ) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = try self.ensureExecutor(null);
        return try executor.asQueryExecutor().cleanupReplication(alloc, dsn, params);
    }
};

fn appendReplicationModeAlloc(alloc: Allocator, dsn: []const u8) ![]u8 {
    // libpq accepts both URI and keyword/value conninfo. A question mark in a
    // quoted keyword value is data, not a URI query delimiter. The final real
    // parameter must also override an explicit replication=false/true value:
    // this socket always needs database-scoped logical replication protocol.
    if (std.mem.startsWith(u8, dsn, "postgresql://") or std.mem.startsWith(u8, dsn, "postgres://")) {
        const separator: u8 = if (std.mem.indexOfScalar(u8, dsn, '?') != null) '&' else '?';
        return std.fmt.allocPrint(alloc, "{s}{c}replication=database", .{ dsn, separator });
    }
    return std.fmt.allocPrint(alloc, "{s} replication=database", .{dsn});
}

test "postgres libpq replication connection mode handles both conninfo syntaxes and overrides protocol downgrade" {
    const a = std.testing.allocator;
    const cases = .{
        .{ "host=localhost dbname=postgres", "host=localhost dbname=postgres replication=database" },
        .{ "host=localhost password='literal?replication=false' replication = false", "host=localhost password='literal?replication=false' replication = false replication=database" },
        .{ "", " replication=database" },
        .{ "postgres://localhost/postgres", "postgres://localhost/postgres?replication=database" },
        .{ "postgresql://localhost/postgres?replication=false", "postgresql://localhost/postgres?replication=false&replication=database" },
        .{ "postgres://localhost/postgres?application_name=replication%3Ddatabase", "postgres://localhost/postgres?application_name=replication%3Ddatabase&replication=database" },
    };
    inline for (cases) |case| {
        const actual = try appendReplicationModeAlloc(a, case[0]);
        defer a.free(actual);
        try std.testing.expectEqualStrings(case[1], actual);
    }
}

fn cloneParameterValuesAlloc(alloc: Allocator, args: []const sql.ParameterValue) ![]sql.ParameterValue {
    if (args.len == 0) return &.{};
    const out = try alloc.alloc(sql.ParameterValue, args.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*arg| arg.deinit(alloc);
        alloc.free(out);
    }
    for (args, 0..) |arg, i| {
        out[i] = switch (arg) {
            .string => |value| .{ .string = try alloc.dupe(u8, value) },
            .integer => |value| .{ .integer = value },
            .float => |value| .{ .float = value },
            .bool => |value| .{ .bool = value },
            .null => .null,
        };
        initialized += 1;
    }
    return out;
}

const PgoutputColumn = struct {
    name: []u8,
    data_type: Oid,
    type_modifier: i32 = -1,
    key: bool = false,
};

const PgoutputRelation = struct {
    namespace: []u8,
    relation_name: []u8,
    columns: []PgoutputColumn,
    replica_identity: u8 = 'd',
};

fn deinitPgoutputRelationCache(alloc: Allocator, cache: *std.AutoHashMapUnmanaged(u32, PgoutputRelation)) void {
    var it = cache.iterator();
    while (it.next()) |entry| {
        alloc.free(entry.value_ptr.namespace);
        alloc.free(entry.value_ptr.relation_name);
        for (entry.value_ptr.columns) |column| alloc.free(column.name);
        if (entry.value_ptr.columns.len > 0) alloc.free(entry.value_ptr.columns);
    }
    cache.deinit(alloc);
}

const ParsedPgoutputMessage = union(enum) {
    none,
    begin: struct { final_lsn: u64, timestamp_ms: u64, xid: u32 },
    commit: struct { commit_lsn: u64, end_lsn: u64, timestamp_ms: u64 },
    change: foreign_source.ReplicationChange,
};

fn clonePgoutputRelationIdentity(a: Allocator, id: u32, relation: PgoutputRelation) !foreign_source.ReplicationRelation {
    const namespace = try a.dupe(u8, relation.namespace);
    errdefer a.free(namespace);
    return .{ .id = id, .namespace = namespace, .name = try a.dupe(u8, relation.relation_name) };
}

const PgoutputTransactions = struct {
    binding: ?Executor.PublicationBinding = null,
    changes: std.ArrayListUnmanaged(foreign_source.ReplicationChange) = .empty,
    transactions: std.ArrayListUnmanaged(foreign_source.ReplicationTransaction) = .empty,
    pending: ?struct { first: usize, final_lsn: u64, timestamp_ms: u64, xid: u32 } = null,

    fn deinit(self: *PgoutputTransactions, a: Allocator) void {
        for (self.changes.items) |*change| change.deinit(a);
        self.changes.deinit(a);
        for (self.transactions.items) |*transaction| transaction.deinit(a);
        self.transactions.deinit(a);
    }

    /// Consumes a row message even on error; B/C carry no owned memory.
    fn accept(self: *PgoutputTransactions, a: Allocator, message: ParsedPgoutputMessage, checkpoint: []const u8) !void {
        switch (message) {
            .none => {},
            .begin => |begin| {
                if (self.pending != null) return error.InvalidReplicationTransaction;
                self.pending = .{ .first = self.changes.items.len, .final_lsn = begin.final_lsn, .timestamp_ms = begin.timestamp_ms, .xid = begin.xid };
            },
            .change => |value| {
                var change = value;
                errdefer change.deinit(a);
                if (self.pending == null) return error.InvalidReplicationTransaction;
                try self.changes.append(a, change);
            },
            .commit => |commit| {
                const pending = self.pending orelse return error.InvalidReplicationTransaction;
                if (pending.final_lsn != commit.commit_lsn or pending.timestamp_ms != commit.timestamp_ms or checkpoint.len == 0)
                    return error.InvalidReplicationTransaction;
                if (self.transactions.getLastOrNull()) |previous| if (commit.end_lsn <= previous.end_lsn or commit.commit_lsn < previous.end_lsn) return error.InvalidReplicationTransaction;
                // Reserve and copy everything before changing owned row fields.
                try self.transactions.ensureUnusedCapacity(a, 1);
                // The decoded COMMIT end_lsn is the complete transaction
                // boundary. Never advance from an intermediate SQL output-row
                // location or from a row inside this transaction.
                var position_buffer: [17]u8 = undefined;
                const position = std.fmt.bufPrint(&position_buffer, "{X}/{X}", .{ @as(u32, @truncate(commit.end_lsn >> 32)), @as(u32, @truncate(commit.end_lsn)) }) catch unreachable;
                const owned_checkpoint = if (self.binding) |binding| try binding.checkpoint(a, position) else try a.dupe(u8, position);
                errdefer a.free(owned_checkpoint);
                for (self.changes.items[pending.first..]) |*change| {
                    const next = try a.dupe(u8, owned_checkpoint);
                    a.free(change.checkpoint);
                    change.checkpoint = next;
                    change.commit_timestamp_ms = commit.timestamp_ms;
                }
                self.transactions.appendAssumeCapacity(.{ .xid = pending.xid, .final_lsn = pending.final_lsn, .commit_lsn = commit.commit_lsn, .end_lsn = commit.end_lsn, .commit_timestamp_ms = commit.timestamp_ms, .first_change = pending.first, .change_count = self.changes.items.len - pending.first, .checkpoint = owned_checkpoint });
                self.pending = null;
            },
        }
    }

    fn discardIncomplete(self: *PgoutputTransactions, a: Allocator) void {
        const pending = self.pending orelse return;
        for (self.changes.items[pending.first..]) |*change| change.deinit(a);
        self.changes.shrinkRetainingCapacity(pending.first);
        self.pending = null;
    }

    fn finish(self: *PgoutputTransactions, a: Allocator, lag_millis: u64) !foreign_source.ReplicationPollResult {
        self.discardIncomplete(a);
        const checkpoint = if (self.transactions.getLastOrNull()) |last| try a.dupe(u8, last.checkpoint) else try a.alloc(u8, 0);
        errdefer a.free(checkpoint);
        const changes = try self.changes.toOwnedSlice(a);
        errdefer {
            for (changes) |*change| change.deinit(a);
            a.free(changes);
        }
        const transactions = try self.transactions.toOwnedSlice(a);
        return .{ .changes = changes, .transactions = transactions, .checkpoint = checkpoint, .lag_millis = lag_millis };
    }
};

/// Bounded physical transport framing, independent of logical transaction
/// commit. No omitted/duplicated/reordered source bytes can enter the decoder.
const ReplicationChunkFraming = struct {
    next_ordinal: u64 = 1,
    total: u64 = 0,
    offset: u64 = 0,
    in_message: bool = false,

    fn accept(self: *@This(), ordinal: u64, total: u64, offset: u64, bytes: usize) !void {
        if (ordinal != self.next_ordinal or bytes == 0 or bytes > replication_wire_chunk_bytes or total == 0 or offset != self.offset or bytes > total or offset > total - bytes) return error.InvalidReplicationSourceRow;
        if (!self.in_message) {
            if (offset != 0) return error.InvalidReplicationSourceRow;
            self.total = total;
            self.in_message = true;
        } else if (total != self.total) return error.InvalidReplicationSourceRow;
        const next = offset + bytes;
        if (next < total and bytes != replication_wire_chunk_bytes) return error.InvalidReplicationSourceRow;
        self.offset = next;
        if (next == total) {
            self.next_ordinal = std.math.add(u64, self.next_ordinal, 1) catch return error.InvalidReplicationSourceRow;
            self.offset = 0;
            self.total = 0;
            self.in_message = false;
        }
    }
};

fn requirePgoutputOrigin(bytes: []const u8, cache: *const std.AutoHashMapUnmanaged(u32, PgoutputRelation), origin: foreign_source.SubscriptionOrigin.Observation) !void {
    if (bytes.len < 5 or bytes[0] != 'R') return error.InvalidReplicationSourceRow;
    const id = std.mem.readInt(u32, bytes[1..5], .big);
    const expected = origin.find(id) orelse return error.ForeignProviderIdentityMismatch;
    const actual = cache.get(id) orelse return error.InvalidReplicationSourceRow;
    const replica_identity: u8 = switch (expected.replica_identity) {
        .default => 'd',
        .nothing => 'n',
        .full => 'f',
        .index => 'i',
    };
    if (actual.replica_identity != replica_identity or !std.mem.eql(u8, actual.namespace, expected.namespace) or
        !std.mem.eql(u8, actual.relation_name, expected.name) or actual.columns.len != expected.columns.len) return error.ForeignProviderIdentityMismatch;
    for (actual.columns, expected.columns) |column, frozen| {
        if (!std.mem.eql(u8, column.name, frozen.name) or column.data_type != frozen.type_oid or column.type_modifier != frozen.type_modifier or column.key != frozen.key) return error.ForeignProviderIdentityMismatch;
    }
}

fn parsePgoutputMessageAlloc(
    alloc: Allocator,
    data: []const u8,
    relation_cache: *std.AutoHashMapUnmanaged(u32, PgoutputRelation),
) !ParsedPgoutputMessage {
    if (data.len == 0) return .none;
    var pos: usize = 0;
    const tag = data[pos];
    pos += 1;
    switch (tag) {
        'B' => {
            const final_lsn = try readU64(data, &pos);
            const timestamp = try readI64(data, &pos);
            const xid = try readU32(data, &pos);
            if (pos != data.len or final_lsn == 0 or xid == 0) return error.InvalidReplicationTransaction;
            return .{ .begin = .{ .final_lsn = final_lsn, .timestamp_ms = postgresEpochMicrosToUnixMillis(timestamp), .xid = xid } };
        },
        'C' => {
            if (try readByte(data, &pos) != 0) return error.InvalidReplicationTransaction;
            const commit_lsn = try readU64(data, &pos);
            const end_lsn = try readU64(data, &pos);
            const commit_ts = try readI64(data, &pos);
            if (pos != data.len or commit_lsn == 0 or end_lsn <= commit_lsn) return error.InvalidReplicationTransaction;
            return .{ .commit = .{ .commit_lsn = commit_lsn, .end_lsn = end_lsn, .timestamp_ms = postgresEpochMicrosToUnixMillis(commit_ts) } };
        },
        'O' => return .none,
        'Y' => return .none,
        'M' => return .none,
        // Never acknowledge TRUNCATE or streamed/prepared transactions as
        // ignored metadata. They require explicit transaction-aware apply.
        'S', 'E', 'c', 'A', 'b', 'P', 'p', 'K', 'r' => return error.UnsupportedReplicationStreaming,
        'T' => return error.UnsupportedReplicationTruncate,
        'R' => {
            const relation_id = try readU32(data, &pos);
            const namespace = try readCStringAlloc(alloc, data, &pos);
            errdefer alloc.free(namespace);
            const relation_name = try readCStringAlloc(alloc, data, &pos);
            errdefer alloc.free(relation_name);
            const replica_identity = try readByte(data, &pos);
            if (replica_identity != 'd' and replica_identity != 'n' and replica_identity != 'f' and replica_identity != 'i') return error.InvalidReplicationSourceRow;
            const column_count = try readU16(data, &pos);
            const columns = try alloc.alloc(PgoutputColumn, column_count);
            var initialized: usize = 0;
            errdefer {
                for (columns[0..initialized]) |column| alloc.free(column.name);
                alloc.free(columns);
            }
            for (0..column_count) |i| {
                const flags = try readByte(data, &pos);
                if (flags & ~@as(u8, 1) != 0) return error.InvalidReplicationSourceRow;
                columns[i] = .{
                    .name = try readCStringAlloc(alloc, data, &pos),
                    .data_type = try readU32(data, &pos),
                    .key = flags & 1 != 0,
                };
                initialized += 1;
                columns[i].type_modifier = @bitCast(try readU32(data, &pos));
            }
            if (pos != data.len) return error.InvalidReplicationSourceRow;
            if (relation_cache.getPtr(relation_id)) |existing| {
                alloc.free(existing.namespace);
                alloc.free(existing.relation_name);
                for (existing.columns) |column| alloc.free(column.name);
                if (existing.columns.len > 0) alloc.free(existing.columns);
                existing.* = .{
                    .namespace = namespace,
                    .relation_name = relation_name,
                    .columns = columns,
                    .replica_identity = replica_identity,
                };
            } else {
                try relation_cache.put(alloc, relation_id, .{
                    .namespace = namespace,
                    .relation_name = relation_name,
                    .columns = columns,
                    .replica_identity = replica_identity,
                });
            }
            return .none;
        },
        'I' => {
            const relation_id = try readU32(data, &pos);
            const relation = relation_cache.get(relation_id) orelse return error.InvalidReplicationSourceRow;
            const tuple_tag = try readByte(data, &pos);
            if (tuple_tag != 'N') return error.InvalidReplicationSourceRow;
            var json_nulls: std.ArrayListUnmanaged([]const u8) = .empty;
            errdefer {
                for (json_nulls.items) |field| alloc.free(field);
                json_nulls.deinit(alloc);
            }
            var row = try parsePgoutputTupleWithNullsAlloc(alloc, data, &pos, relation, &json_nulls);
            errdefer foreign_source.deinitJsonValue(alloc, &row);
            if (pos != data.len) return error.InvalidReplicationSourceRow;
            var identity = try clonePgoutputRelationIdentity(alloc, relation_id, relation);
            errdefer identity.deinit(alloc);
            return .{ .change = .{
                .op = .insert,
                .checkpoint = &.{},
                .relation = identity,
                .json_null_fields = try json_nulls.toOwnedSlice(alloc),
                .row = row,
                .row_complete = row.object.count() == relation.columns.len,
            } };
        },
        'U' => {
            const relation_id = try readU32(data, &pos);
            const relation = relation_cache.get(relation_id) orelse return error.InvalidReplicationSourceRow;
            var marker = try readByte(data, &pos);
            var old_row: ?std.json.Value = null;
            errdefer if (old_row) |*old| foreign_source.deinitJsonValue(alloc, old);
            var old_json_nulls: std.ArrayListUnmanaged([]const u8) = .empty;
            errdefer {
                for (old_json_nulls.items) |field| alloc.free(field);
                old_json_nulls.deinit(alloc);
            }
            var before_kind: foreign_source.ReplicationBeforeKind = .none;
            if (marker == 'K' or marker == 'O') {
                before_kind = if (marker == 'K') .key else .full;
                old_row = try parsePgoutputTupleWithNullsAlloc(alloc, data, &pos, relation, &old_json_nulls);
                if (marker == 'K') retainPgoutputKeyColumns(alloc, &old_row.?, relation, &old_json_nulls);
                marker = try readByte(data, &pos);
            }
            if (marker != 'N') return error.InvalidReplicationSourceRow;
            var json_nulls: std.ArrayListUnmanaged([]const u8) = .empty;
            errdefer {
                for (json_nulls.items) |field| alloc.free(field);
                json_nulls.deinit(alloc);
            }
            var row = try parsePgoutputTupleWithNullsAlloc(alloc, data, &pos, relation, &json_nulls);
            errdefer foreign_source.deinitJsonValue(alloc, &row);
            if (pos != data.len) return error.InvalidReplicationSourceRow;
            if (old_row == null) {
                old_row = try clonePgoutputKeyTuple(alloc, row, relation, json_nulls.items, &old_json_nulls);
                if (old_row.?.object.count() != 0) {
                    before_kind = .key;
                } else {
                    foreign_source.deinitJsonValue(alloc, &old_row.?);
                    old_row = null;
                }
            }
            var identity = try clonePgoutputRelationIdentity(alloc, relation_id, relation);
            errdefer identity.deinit(alloc);
            const old_fields = try old_json_nulls.toOwnedSlice(alloc);
            errdefer {
                for (old_fields) |field| alloc.free(field);
                alloc.free(old_fields);
            }
            const new_fields = try json_nulls.toOwnedSlice(alloc);
            return .{ .change = .{
                .op = .update,
                .checkpoint = &.{},
                .relation = identity,
                .json_null_fields = new_fields,
                .row = row,
                .before = old_row,
                .before_kind = before_kind,
                .row_complete = row.object.count() == relation.columns.len,
                .before_complete = before_kind == .full and old_row.?.object.count() == relation.columns.len,
                .before_json_null_fields = old_fields,
            } };
        },
        'D' => {
            const relation_id = try readU32(data, &pos);
            const relation = relation_cache.get(relation_id) orelse return error.InvalidReplicationSourceRow;
            const marker = try readByte(data, &pos);
            if (marker != 'K' and marker != 'O') return error.InvalidReplicationSourceRow;
            var json_nulls: std.ArrayListUnmanaged([]const u8) = .empty;
            errdefer {
                for (json_nulls.items) |field| alloc.free(field);
                json_nulls.deinit(alloc);
            }
            var row = try parsePgoutputTupleWithNullsAlloc(alloc, data, &pos, relation, &json_nulls);
            errdefer foreign_source.deinitJsonValue(alloc, &row);
            if (pos != data.len) return error.InvalidReplicationSourceRow;
            if (marker == 'K') retainPgoutputKeyColumns(alloc, &row, relation, &json_nulls);
            const complete = marker == 'O' and row.object.count() == relation.columns.len;
            var identity = try clonePgoutputRelationIdentity(alloc, relation_id, relation);
            errdefer identity.deinit(alloc);
            return .{ .change = .{
                .op = .delete,
                .checkpoint = &.{},
                .relation = identity,
                .json_null_fields = try json_nulls.toOwnedSlice(alloc),
                .row = row,
                .before_kind = if (marker == 'K') .key else .full,
                .row_complete = complete,
                .before_complete = complete,
            } };
        },
        else => return error.UnsupportedReplicationMessage,
    }
}

/// Key-tuple NULL placeholders are not historical values of non-key fields.
fn requirePublicationMessageRelation(message: []const u8, relation_id: u32) !void {
    if (message.len == 0) return error.InvalidReplicationSourceRow;
    if (std.mem.indexOfScalar(u8, "RIUD", message[0]) == null) return;
    var position: usize = 1;
    if (try readU32(message, &position) != relation_id) return error.ReplicationPublicationBindingChanged;
}

fn clonePgoutputKeyTuple(alloc: Allocator, row: std.json.Value, relation: PgoutputRelation, json_nulls: []const []const u8, key_nulls: *std.ArrayListUnmanaged([]const u8)) !std.json.Value {
    var key: std.json.Value = .{ .object = .empty };
    errdefer foreign_source.deinitJsonValue(alloc, &key);
    for (relation.columns) |column| {
        if (!column.key) continue;
        const value = row.object.get(column.name) orelse continue;
        {
            const name = try alloc.dupe(u8, column.name);
            errdefer alloc.free(name);
            var copy = try cloneJsonValueAlloc(alloc, value);
            errdefer foreign_source.deinitJsonValue(alloc, &copy);
            try key.object.put(alloc, name, copy);
        }
        for (json_nulls) |field| {
            if (!std.mem.eql(u8, field, column.name)) continue;
            const owned = try alloc.dupe(u8, field);
            errdefer alloc.free(owned);
            try key_nulls.append(alloc, owned);
            break;
        }
    }
    return key;
}

fn retainPgoutputKeyColumns(alloc: Allocator, row: *std.json.Value, relation: PgoutputRelation, json_nulls: *std.ArrayListUnmanaged([]const u8)) void {
    for (relation.columns) |column| {
        if (column.key) continue;
        for (json_nulls.items, 0..) |field, i| {
            if (!std.mem.eql(u8, field, column.name)) continue;
            alloc.free(json_nulls.orderedRemove(i));
            break;
        }
        if (row.object.fetchOrderedRemove(column.name)) |removed| {
            alloc.free(@constCast(removed.key));
            var value = removed.value;
            foreign_source.deinitJsonValue(alloc, &value);
        }
    }
}

fn parsePgoutputTupleAlloc(alloc: Allocator, data: []const u8, pos: *usize, relation: PgoutputRelation) !std.json.Value {
    return parsePgoutputTupleWithNullsAlloc(alloc, data, pos, relation, null);
}

fn parsePgoutputTupleWithNullsAlloc(alloc: Allocator, data: []const u8, pos: *usize, relation: PgoutputRelation, json_nulls: ?*std.ArrayListUnmanaged([]const u8)) !std.json.Value {
    const column_count = try readU16(data, pos);
    if (column_count != relation.columns.len) return error.InvalidReplicationSourceRow;
    var object = std.json.ObjectMap.empty;
    errdefer {
        var it = object.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            foreign_source.deinitJsonValue(alloc, entry.value_ptr);
        }
        object.deinit(alloc);
    }

    var idx: usize = 0;
    while (idx < column_count and idx < relation.columns.len) : (idx += 1) {
        const kind = try readByte(data, pos);
        switch (kind) {
            'n' => {
                const name = try alloc.dupe(u8, relation.columns[idx].name);
                errdefer alloc.free(name);
                if (object.contains(name)) return error.InvalidReplicationSourceRow;
                try object.put(alloc, name, .null);
            },
            'u' => {},
            't', 'b' => {
                const len = try readI32(data, pos);
                if (len < 0 or pos.* + @as(usize, @intCast(len)) > data.len) return error.InvalidReplicationSourceRow;
                const raw = data[pos.* .. pos.* + @as(usize, @intCast(len))];
                pos.* += @as(usize, @intCast(len));
                var decoded = try parseLogicalValueAlloc(alloc, relation.columns[idx].data_type, raw, kind == 'b');
                errdefer foreign_source.deinitJsonValue(alloc, &decoded);
                if (json_nulls) |fields| if ((relation.columns[idx].data_type == TypeOid.json or relation.columns[idx].data_type == TypeOid.jsonb) and decoded == .null) {
                    const name = try alloc.dupe(u8, relation.columns[idx].name);
                    errdefer alloc.free(name);
                    try fields.append(alloc, name);
                };
                const name = try alloc.dupe(u8, relation.columns[idx].name);
                errdefer alloc.free(name);
                if (object.contains(name)) return error.InvalidReplicationSourceRow;
                try object.put(alloc, name, decoded);
            },
            else => return error.InvalidReplicationSourceRow,
        }
    }

    return .{ .object = object };
}

fn parseLogicalValueAlloc(alloc: Allocator, oid: Oid, raw: []const u8, binary: bool) !std.json.Value {
    if (binary) return parseLogicalBinaryValueAlloc(alloc, oid, raw);
    return switch (oid) {
        TypeOid.boolean => .{ .bool = parseBoolCell(raw) },
        TypeOid.int2, TypeOid.int4, TypeOid.int8 => .{ .integer = try std.fmt.parseInt(i64, raw, 10) },
        TypeOid.float4, TypeOid.float8 => .{ .float = try std.fmt.parseFloat(f64, raw) },
        TypeOid.numeric => .{ .number_string = try alloc.dupe(u8, raw) },
        TypeOid.json, TypeOid.jsonb => blk: {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
            defer parsed.deinit();
            break :blk try cloneJsonValueAlloc(alloc, parsed.value);
        },
        else => .{ .string = try alloc.dupe(u8, raw) },
    };
}

/// Binary tuple cells use the PostgreSQL type's send representation. Unknown
/// OIDs must fail before admission, never become arbitrary UTF-8 strings.
fn parseLogicalBinaryValueAlloc(a: Allocator, oid: Oid, raw: []const u8) !std.json.Value {
    return switch (oid) {
        TypeOid.boolean => blk: {
            if (raw.len != 1 or raw[0] > 1) return error.InvalidReplicationSourceRow;
            break :blk .{ .bool = raw[0] == 1 };
        },
        TypeOid.int2 => if (raw.len == 2) .{ .integer = std.mem.readInt(i16, raw[0..2], .big) } else error.InvalidReplicationSourceRow,
        TypeOid.int4 => if (raw.len == 4) .{ .integer = std.mem.readInt(i32, raw[0..4], .big) } else error.InvalidReplicationSourceRow,
        TypeOid.int8 => if (raw.len == 8) .{ .integer = std.mem.readInt(i64, raw[0..8], .big) } else error.InvalidReplicationSourceRow,
        26 => if (raw.len == 4) .{ .integer = std.mem.readInt(u32, raw[0..4], .big) } else error.InvalidReplicationSourceRow,
        TypeOid.float4 => blk: {
            if (raw.len != 4) return error.InvalidReplicationSourceRow;
            const value: f32 = @bitCast(std.mem.readInt(u32, raw[0..4], .big));
            if (!std.math.isFinite(value)) return error.UnsupportedReplicationType;
            break :blk .{ .float = value };
        },
        TypeOid.float8 => blk: {
            if (raw.len != 8) return error.InvalidReplicationSourceRow;
            const value: f64 = @bitCast(std.mem.readInt(u64, raw[0..8], .big));
            if (!std.math.isFinite(value)) return error.UnsupportedReplicationType;
            break :blk .{ .float = value };
        },
        TypeOid.numeric => blk: {
            const native = @import("antfly_local_sources");
            var context: native.sql_numeric_value.Context = .{ .alloc = a };
            var number = try native.sql_numeric_binary.decode(&context, raw, .{});
            defer number.deinit();
            if (number.value.kind != .finite) return error.UnsupportedReplicationType;
            break :blk .{ .number_string = try native.sql_numeric_value.format(&context, number.value) };
        },
        TypeOid.json, TypeOid.jsonb => blk: {
            const bytes = if (oid == TypeOid.jsonb) body: {
                if (raw.len == 0 or raw[0] != 1) return error.InvalidReplicationSourceRow;
                break :body raw[1..];
            } else raw;
            var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
            defer parsed.deinit();
            break :blk try cloneJsonValueAlloc(a, parsed.value);
        },
        18, 19, 25, 1042, 1043 => blk: {
            if (!std.unicode.utf8ValidateSlice(raw) or std.mem.indexOfScalar(u8, raw, 0) != null) return error.InvalidReplicationSourceRow;
            break :blk .{ .string = try a.dupe(u8, raw) };
        },
        else => error.UnsupportedReplicationType,
    };
}

test "postgres logical binary tuples decode actual type OIDs and refuse opaque or malformed cells" {
    var region = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer region.deinit();
    const a = region.allocator();
    try std.testing.expectEqual(@as(i64, -2), (try parseLogicalValueAlloc(a, TypeOid.int8, &.{ 255, 255, 255, 255, 255, 255, 255, 254 }, true)).integer);
    try std.testing.expectEqual(@as(i64, 17), (try parseLogicalValueAlloc(a, TypeOid.int4, &.{ 0, 0, 0, 17 }, true)).integer);
    try std.testing.expect((try parseLogicalValueAlloc(a, TypeOid.boolean, &.{1}, true)).bool);
    try std.testing.expectEqualStrings("plan", (try parseLogicalValueAlloc(a, 25, "plan", true)).string);
    const json = try parseLogicalValueAlloc(a, TypeOid.jsonb, "\x01{\"n\":17,\"v\":null}", true);
    try std.testing.expectEqual(@as(i64, 17), json.object.get("n").?.integer);
    try std.testing.expect(json.object.get("v").? == .null);
    try std.testing.expectError(error.InvalidReplicationSourceRow, parseLogicalValueAlloc(a, TypeOid.int4, &.{ 0, 17 }, true));
    try std.testing.expectError(error.InvalidReplicationSourceRow, parseLogicalValueAlloc(a, TypeOid.boolean, &.{2}, true));
    try std.testing.expectError(error.InvalidReplicationSourceRow, parseLogicalValueAlloc(a, TypeOid.jsonb, "\x02{}", true));
    try std.testing.expectError(error.UnsupportedReplicationType, parseLogicalValueAlloc(a, 99999, "no-type-proof", true));
}

fn decodeByteaHexAlloc(alloc: Allocator, encoded: []const u8) ![]u8 {
    if (encoded.len >= 2 and encoded[0] == '\\' and encoded[1] == 'x') {
        const hex = encoded[2..];
        if (hex.len % 2 != 0) return error.ForeignQueryFailed;
        const out = try alloc.alloc(u8, hex.len / 2);
        errdefer alloc.free(out);
        for (0..out.len) |i| {
            out[i] = try std.fmt.parseInt(u8, hex[i * 2 .. i * 2 + 2], 16);
        }
        return out;
    }
    return try alloc.dupe(u8, encoded);
}

fn quoteSqlStringLiteralAlloc(alloc: Allocator, value: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '\'');
    for (value) |byte| {
        if (byte == '\'') try out.append(alloc, '\'');
        try out.append(alloc, byte);
    }
    try out.append(alloc, '\'');
    return try out.toOwnedSlice(alloc);
}

fn readByte(data: []const u8, pos: *usize) !u8 {
    if (pos.* >= data.len) return error.InvalidReplicationSourceRow;
    const value = data[pos.*];
    pos.* += 1;
    return value;
}

fn readU16(data: []const u8, pos: *usize) !u16 {
    if (pos.* + 2 > data.len) return error.InvalidReplicationSourceRow;
    const value = std.mem.readInt(u16, data[pos.* .. pos.* + 2][0..2], .big);
    pos.* += 2;
    return value;
}

fn readU32(data: []const u8, pos: *usize) !u32 {
    if (pos.* + 4 > data.len) return error.InvalidReplicationSourceRow;
    const value = std.mem.readInt(u32, data[pos.* .. pos.* + 4][0..4], .big);
    pos.* += 4;
    return value;
}

fn readU64(data: []const u8, pos: *usize) !u64 {
    if (pos.* + 8 > data.len) return error.InvalidReplicationSourceRow;
    const value = std.mem.readInt(u64, data[pos.* .. pos.* + 8][0..8], .big);
    pos.* += 8;
    return value;
}

fn readI32(data: []const u8, pos: *usize) !i32 {
    if (pos.* + 4 > data.len) return error.InvalidReplicationSourceRow;
    const value = std.mem.readInt(i32, data[pos.* .. pos.* + 4][0..4], .big);
    pos.* += 4;
    return value;
}

fn readI64(data: []const u8, pos: *usize) !i64 {
    if (pos.* + 8 > data.len) return error.InvalidReplicationSourceRow;
    const value = std.mem.readInt(i64, data[pos.* .. pos.* + 8][0..8], .big);
    pos.* += 8;
    return value;
}

fn postgresEpochMicrosToUnixMillis(micros_since_2000: i64) u64 {
    const micros_per_milli: i64 = 1_000;
    const unix_offset_millis: i64 = 946_684_800_000;
    if (micros_since_2000 <= 0) return 0;
    return @intCast(@max(@as(i64, 0), @divTrunc(micros_since_2000, micros_per_milli) + unix_offset_millis));
}

fn currentRealtimeMillis() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => {},
        else => return 0,
    }
    const sec: u64 = @intCast(@max(ts.sec, 0));
    const nsec: u64 = @intCast(@max(ts.nsec, 0));
    return sec * std.time.ms_per_s + @divTrunc(nsec, std.time.ns_per_ms);
}

fn readCStringAlloc(alloc: Allocator, data: []const u8, pos: *usize) ![]u8 {
    const end = std.mem.indexOfScalarPos(u8, data, pos.*, 0) orelse return error.InvalidReplicationSourceRow;
    const out = try alloc.dupe(u8, data[pos.*..end]);
    pos.* = end + 1;
    return out;
}

const OwnedArgs = struct {
    values: []?[*:0]const u8 = &.{},
    lengths: []c_int = &.{},
    formats: []c_int = &.{},
    owned_strings: []?[:0]u8 = &.{},

    fn init(alloc: Allocator, args: []const sql.ParameterValue) !@This() {
        if (args.len == 0) return .{};

        const values = try alloc.alloc(?[*:0]const u8, args.len);
        errdefer alloc.free(values);
        const lengths = try alloc.alloc(c_int, args.len);
        errdefer alloc.free(lengths);
        const formats = try alloc.alloc(c_int, args.len);
        errdefer alloc.free(formats);
        const owned_strings = try alloc.alloc(?[:0]u8, args.len);
        errdefer alloc.free(owned_strings);

        var initialized: usize = 0;
        errdefer {
            for (owned_strings[0..initialized]) |maybe_buffer| {
                if (maybe_buffer) |buffer| alloc.free(buffer);
            }
        }

        for (args, 0..) |arg, idx| {
            formats[idx] = 0;
            switch (arg) {
                .null => {
                    values[idx] = null;
                    lengths[idx] = 0;
                    owned_strings[idx] = null;
                },
                .bool => |value| {
                    const printed = try std.fmt.allocPrint(alloc, "{}", .{value});
                    defer alloc.free(printed);
                    const text = try alloc.dupeSentinel(u8, printed, 0);
                    values[idx] = text.ptr;
                    lengths[idx] = @intCast(printed.len);
                    owned_strings[idx] = text;
                },
                .integer => |value| {
                    const printed = try std.fmt.allocPrint(alloc, "{d}", .{value});
                    defer alloc.free(printed);
                    const text = try alloc.dupeSentinel(u8, printed, 0);
                    values[idx] = text.ptr;
                    lengths[idx] = @intCast(printed.len);
                    owned_strings[idx] = text;
                },
                .float => |value| {
                    const printed = try std.fmt.allocPrint(alloc, "{d}", .{value});
                    defer alloc.free(printed);
                    const text = try alloc.dupeSentinel(u8, printed, 0);
                    values[idx] = text.ptr;
                    lengths[idx] = @intCast(printed.len);
                    owned_strings[idx] = text;
                },
                .string => |value| {
                    const text = try alloc.dupeSentinel(u8, value, 0);
                    values[idx] = text.ptr;
                    lengths[idx] = @intCast(value.len);
                    owned_strings[idx] = text;
                },
            }
            initialized += 1;
        }

        return .{
            .values = values,
            .lengths = lengths,
            .formats = formats,
            .owned_strings = owned_strings,
        };
    }

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.owned_strings) |buffer| {
            if (buffer) |owned| alloc.free(owned);
        }
        if (self.owned_strings.len > 0) alloc.free(self.owned_strings);
        if (self.formats.len > 0) alloc.free(self.formats);
        if (self.lengths.len > 0) alloc.free(self.lengths);
        if (self.values.len > 0) alloc.free(self.values);
        self.* = undefined;
    }
};

fn openDefaultLibpq() !std.DynLib {
    if (comptime builtin.link_libc) if (std.c.getenv("ANTFLY_LIBPQ_PATH")) |value_z| {
        return std.DynLib.open(std.mem.span(value_z)) catch error.LibpqUnavailable;
    };
    const candidates = [_][]const u8{
        "/opt/homebrew/lib/postgresql@18/libpq.dylib",
        "/opt/homebrew/opt/postgresql@18/lib/libpq.dylib",
        "/usr/local/opt/postgresql/lib/libpq.dylib",
        "libpq.dylib",
        "libpq.so.5",
        "libpq.so",
    };
    for (candidates) |candidate| {
        return std.DynLib.open(candidate) catch continue;
    }
    return error.LibpqUnavailable;
}

fn lookupRequired(lib: *std.DynLib, comptime T: type, name: [:0]const u8) !T {
    return lib.lookup(T, name) orelse error.MissingLibpqSymbol;
}

fn mapResultError(sqlstate_z: ?[*:0]const u8, message_z: [*:0]const u8) anyerror {
    const message = std.mem.span(message_z);
    if (message.len > 0) std.log.warn("postgres libpq result error: {s}", .{message});
    const sqlstate = if (sqlstate_z) |value| std.mem.span(value) else "";
    if (std.mem.eql(u8, sqlstate, "42P01")) return error.ForeignTableNotFound;
    if (std.mem.eql(u8, sqlstate, "42704")) return error.ForeignReplicationSlotMissing;
    if (std.mem.eql(u8, sqlstate, "42703")) return error.UnknownColumn;
    if (std.mem.eql(u8, sqlstate, "28P01") or std.mem.eql(u8, sqlstate, "28000")) return error.ForeignAuthFailed;
    if (std.mem.eql(u8, sqlstate, "08001") or std.mem.eql(u8, sqlstate, "08006")) return error.ForeignConnectionFailed;
    if (std.mem.eql(u8, sqlstate, "42601")) return error.InvalidQueryRequest;
    return error.ForeignQueryFailed;
}

fn parseCellValueAlloc(alloc: Allocator, oid: Oid, value_ptr: [*]const u8, len: usize) !std.json.Value {
    const bytes = value_ptr[0..len];
    return switch (oid) {
        TypeOid.boolean => .{ .bool = parseBoolCell(bytes) },
        TypeOid.int2, TypeOid.int4, TypeOid.int8 => .{ .integer = try std.fmt.parseInt(i64, bytes, 10) },
        TypeOid.float4, TypeOid.float8 => .{ .float = try std.fmt.parseFloat(f64, bytes) },
        TypeOid.numeric => .{ .number_string = try alloc.dupe(u8, bytes) },
        TypeOid.json, TypeOid.jsonb => blk: {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, bytes, .{});
            defer parsed.deinit();
            break :blk try cloneJsonValueAlloc(alloc, parsed.value);
        },
        else => .{ .string = try alloc.dupe(u8, bytes) },
    };
}

fn parseBoolCell(bytes: []const u8) bool {
    return bytes.len > 0 and (bytes[0] == 't' or bytes[0] == '1');
}

fn parseIntCell(value_ptr: [*]const u8, len: usize) !i64 {
    return try std.fmt.parseInt(i64, value_ptr[0..len], 10);
}

fn copyCellAlloc(alloc: Allocator, value_ptr: [*]const u8, len: usize) ![]u8 {
    return try alloc.dupe(u8, value_ptr[0..len]);
}

fn cloneJsonValueAlloc(alloc: Allocator, value: std.json.Value) !std.json.Value {
    return switch (value) {
        .null => .null,
        .bool => |v| .{ .bool = v },
        .integer => |v| .{ .integer = v },
        .float => |v| .{ .float = v },
        .number_string => |v| .{ .number_string = try alloc.dupe(u8, v) },
        .string => |v| .{ .string = try alloc.dupe(u8, v) },
        .array => |arr| blk: {
            var out = std.json.Array.init(alloc);
            errdefer {
                for (out.items) |*item| foreign_source.deinitJsonValue(alloc, item);
                out.deinit();
            }
            for (arr.items) |item| {
                try out.append(try cloneJsonValueAlloc(alloc, item));
            }
            break :blk .{ .array = out };
        },
        .object => |obj| blk: {
            var out = std.json.ObjectMap.empty;
            errdefer {
                var it = out.iterator();
                while (it.next()) |entry| {
                    alloc.free(@constCast(entry.key_ptr.*));
                    foreign_source.deinitJsonValue(alloc, entry.value_ptr);
                }
                out.deinit(alloc);
            }
            var it = obj.iterator();
            while (it.next()) |entry| {
                {
                    const key = try alloc.dupe(u8, entry.key_ptr.*);
                    errdefer alloc.free(key);
                    var item = try cloneJsonValueAlloc(alloc, entry.value_ptr.*);
                    errdefer foreign_source.deinitJsonValue(alloc, &item);
                    try out.put(alloc, key, item);
                }
            }
            break :blk .{ .object = out };
        },
    };
}

fn cloneColumnsAlloc(alloc: Allocator, columns: []const foreign_source.Column) ![]foreign_source.Column {
    if (columns.len == 0) return &.{};
    const out = try alloc.alloc(foreign_source.Column, columns.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*column| column.deinit(alloc);
        alloc.free(out);
    }
    for (columns, 0..) |column, idx| {
        const name = try alloc.dupe(u8, column.name);
        errdefer alloc.free(name);
        const data_type = try alloc.dupe(u8, column.data_type);
        errdefer alloc.free(data_type);
        out[idx] = .{
            .name = name,
            .data_type = data_type,
            .nullable = column.nullable,
        };
        initialized += 1;
    }
    return out;
}

fn freeColumns(alloc: Allocator, columns: []foreign_source.Column) void {
    for (columns) |*column| column.deinit(alloc);
    if (columns.len > 0) alloc.free(columns);
}

fn columnCacheKeyAlloc(alloc: Allocator, dsn: []const u8, table: []const u8) ![]u8 {
    const prefix_len = @sizeOf(u64);
    const payload_len = std.math.add(usize, dsn.len, table.len) catch return error.OutOfMemory;
    const key_len = std.math.add(usize, prefix_len, payload_len) catch return error.OutOfMemory;
    const key = try alloc.alloc(u8, key_len);
    std.mem.writeInt(u64, key[0..prefix_len], @intCast(dsn.len), .little);
    @memcpy(key[prefix_len .. prefix_len + dsn.len], dsn);
    @memcpy(key[prefix_len + dsn.len ..], table);
    return key;
}

fn relationPreparedQueryAlloc(
    alloc: Allocator,
    sql_text: []const u8,
    relation: []const u8,
) !sql.PreparedQuery {
    const relation_sql = try sql.quotePostgresRelationAlloc(alloc, relation);
    defer alloc.free(relation_sql);
    return try tableNamePreparedQueryAlloc(alloc, sql_text, relation_sql);
}

fn tableNamePreparedQueryAlloc(
    alloc: Allocator,
    sql_text: []const u8,
    table: []const u8,
) !sql.PreparedQuery {
    const args = try alloc.alloc(sql.ParameterValue, 1);
    errdefer alloc.free(args);
    const table_arg = try alloc.dupe(u8, table);
    errdefer alloc.free(table_arg);
    args[0] = .{ .string = table_arg };
    return .{
        .sql_text = try alloc.dupe(u8, sql_text),
        .args = args,
    };
}

fn lock(mutex: *Mutex) void {
    platform_sync.lockYielding(mutex);
}

fn ensureDeadline(deadline_ns: u64) !void {
    if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
}

fn durationTimespec(duration_ns: u64) std.c.timespec {
    return .{
        .sec = @intCast(duration_ns / std.time.ns_per_s),
        .nsec = @intCast(duration_ns % std.time.ns_per_s),
    };
}

fn monotonicDeadlineTimespec(deadline_ns: u64) std.c.timespec {
    return durationTimespec(deadline_ns);
}

fn invalidatesConnection(err: anyerror) bool {
    return err == error.Timeout or
        err == error.Cancelled or
        err == error.ForeignConnectionFailed or
        err == error.ForeignQueryFailed;
}

fn ensureNotCancelled(cancellation: ?CancellationToken) !void {
    if (cancellation) |value| if (value.isCancelled()) return error.Cancelled;
}

fn checkResultDecodeProgress(
    execution_deadline_ns: ?u64,
    cancellation: ?CancellationToken,
    cells_until_check: *u8,
) !void {
    cells_until_check.* -|= 1;
    if (cells_until_check.* != 0) return;
    cells_until_check.* = 64;
    if (execution_deadline_ns) |deadline_ns| try ensureDeadline(deadline_ns);
    try ensureNotCancelled(cancellation);
}

fn boundedCancellationWaitDeadline(
    execution_deadline_ns: ?u64,
    cancellation: ?CancellationToken,
) ?u64 {
    if (cancellation == null) return execution_deadline_ns;
    return @min(
        execution_deadline_ns orelse std.math.maxInt(u64),
        platform_time.monotonicNs() +| cancellation_poll_interval_ns,
    );
}

fn waitForAvailabilityChange(
    availability: *PoolAvailability,
    observed: u64,
    execution_deadline_ns: ?u64,
    cancellation: ?CancellationToken,
) !void {
    while (true) {
        try ensureNotCancelled(cancellation);
        availability.waitForChange(
            observed,
            boundedCancellationWaitDeadline(execution_deadline_ns, cancellation),
        ) catch |err| {
            if (err != error.Timeout) return err;
            try ensureNotCancelled(cancellation);
            if (execution_deadline_ns) |deadline_ns| try ensureDeadline(deadline_ns);
            continue;
        };
        return;
    }
}

fn lockUntil(mutex: *Mutex, deadline_ns: ?u64) !void {
    const deadline = deadline_ns orelse {
        lock(mutex);
        return;
    };
    while (!mutex.tryLock()) {
        try ensureDeadline(deadline);
        spinOrYield();
    }
    // Winning the race at the deadline must not start new foreign work.
    ensureDeadline(deadline) catch |err| {
        mutex.unlock();
        return err;
    };
}

fn lockUntilCancellable(
    mutex: *Mutex,
    deadline_ns: ?u64,
    cancellation: ?CancellationToken,
) !void {
    while (!mutex.tryLock()) {
        try ensureNotCancelled(cancellation);
        if (deadline_ns) |deadline| try ensureDeadline(deadline);
        spinOrYield();
    }
    ensureNotCancelled(cancellation) catch |err| {
        mutex.unlock();
        return err;
    };
    if (deadline_ns) |deadline| ensureDeadline(deadline) catch |err| {
        mutex.unlock();
        return err;
    };
}

fn spinOrYield() void {
    if (builtin.os.tag == .freestanding) {
        std.atomic.spinLoopHint();
    } else {
        @import("antfly_platform").time.yieldNow();
    }
}

pub fn registerDefaultExecutor(alloc: Allocator, registry: *foreign_source.Registry) !void {
    const executor = try alloc.create(LazyExecutor);
    errdefer alloc.destroy(executor);
    executor.* = .{
        .alloc = alloc,
    };
    try postgres_source.registerExecutor(alloc, registry, executor.asQueryExecutor());
}

test "postgres libpq async reader services input while flushing and between results" {
    const Driver = struct {
        flush_calls: usize = 0,
        wait_events: [3]i16 = undefined,
        wait_calls: usize = 0,
        consume_calls: usize = 0,
        busy_calls: usize = 0,
        get_result_calls: usize = 0,
        restored_blocking: bool = false,

        fn flush(self: *@This()) c_int {
            defer self.flush_calls += 1;
            return if (self.flush_calls == 0) 1 else 0;
        }

        fn wait(self: *@This(), events: i16, _: ?u64, _: ?CancellationToken) !i16 {
            self.wait_events[self.wait_calls] = events;
            self.wait_calls += 1;
            return std.posix.POLL.IN;
        }

        fn consumeInput(self: *@This()) c_int {
            self.consume_calls += 1;
            return 1;
        }

        fn isBusy(self: *@This()) c_int {
            const sequence = [_]c_int{ 1, 0, 1, 0 };
            const result = sequence[self.busy_calls];
            self.busy_calls += 1;
            return result;
        }

        fn getResult(self: *@This()) ?*PGresult {
            defer self.get_result_calls += 1;
            return if (self.get_result_calls == 0) @ptrFromInt(16) else null;
        }

        fn clear(_: *@This(), _: ?*PGresult) void {
            unreachable;
        }

        fn restoreBlocking(self: *@This()) c_int {
            self.restored_blocking = true;
            return 0;
        }
    };

    var driver = Driver{};
    const result = try readSingleAsyncResultWithDeadline(
        &driver,
        platform_time.monotonicNs() + std.time.ns_per_s,
        null,
    );
    try std.testing.expectEqual(@as(?*PGresult, @ptrFromInt(16)), result);
    try std.testing.expectEqual(@as(usize, 3), driver.wait_calls);
    try std.testing.expectEqual(
        @as(i16, std.posix.POLL.IN | std.posix.POLL.OUT),
        driver.wait_events[0],
    );
    try std.testing.expectEqual(@as(i16, std.posix.POLL.IN), driver.wait_events[1]);
    try std.testing.expectEqual(@as(i16, std.posix.POLL.IN), driver.wait_events[2]);
    try std.testing.expectEqual(@as(usize, 3), driver.consume_calls);
    try std.testing.expectEqual(@as(usize, 4), driver.busy_calls);
    try std.testing.expectEqual(@as(usize, 2), driver.get_result_calls);
    try std.testing.expect(driver.restored_blocking);
}

test "postgres libpq async reader rejects and clears additional results" {
    const Driver = struct {
        get_result_calls: usize = 0,
        clear_calls: usize = 0,
        restored_blocking: bool = false,

        fn flush(_: *@This()) c_int {
            return 0;
        }

        fn wait(_: *@This(), _: i16, _: ?u64, _: ?CancellationToken) !i16 {
            unreachable;
        }

        fn consumeInput(_: *@This()) c_int {
            unreachable;
        }

        fn isBusy(_: *@This()) c_int {
            return 0;
        }

        fn getResult(self: *@This()) ?*PGresult {
            defer self.get_result_calls += 1;
            return switch (self.get_result_calls) {
                0 => @ptrFromInt(16),
                1 => @ptrFromInt(32),
                else => null,
            };
        }

        fn clear(self: *@This(), _: ?*PGresult) void {
            self.clear_calls += 1;
        }

        fn restoreBlocking(self: *@This()) c_int {
            self.restored_blocking = true;
            return 0;
        }
    };

    var driver = Driver{};
    try std.testing.expectError(
        error.ForeignQueryFailed,
        readSingleAsyncResultWithDeadline(
            &driver,
            platform_time.monotonicNs() + std.time.ns_per_s,
            null,
        ),
    );
    try std.testing.expectEqual(@as(usize, 3), driver.get_result_calls);
    try std.testing.expectEqual(@as(usize, 2), driver.clear_calls);
    try std.testing.expect(driver.restored_blocking);
}

test "postgres libpq async reader observes cancellation after bounded wait before consuming input" {
    const Driver = struct {
        cancellation: *std.atomic.Value(bool),
        wait_calls: usize = 0,
        consume_calls: usize = 0,
        saw_no_deadline: bool = false,

        fn flush(_: *@This()) c_int {
            return 0;
        }

        fn wait(
            self: *@This(),
            _: i16,
            deadline_ns: ?u64,
            _: ?CancellationToken,
        ) anyerror!i16 {
            self.wait_calls += 1;
            self.saw_no_deadline = deadline_ns == null;
            self.cancellation.store(true, .release);
            return error.Timeout;
        }

        fn consumeInput(self: *@This()) c_int {
            self.consume_calls += 1;
            return 1;
        }

        fn isBusy(_: *@This()) c_int {
            return 1;
        }

        fn getResult(_: *@This()) ?*PGresult {
            unreachable;
        }

        fn clear(_: *@This(), _: ?*PGresult) void {
            unreachable;
        }

        fn restoreBlocking(_: *@This()) c_int {
            unreachable;
        }
    };

    var cancellation = std.atomic.Value(bool).init(false);
    var driver = Driver{ .cancellation = &cancellation };
    try std.testing.expectError(
        error.Cancelled,
        readSingleAsyncResultWithDeadline(&driver, null, CancellationToken.fromAtomic(&cancellation)),
    );
    try std.testing.expectEqual(@as(usize, 1), driver.wait_calls);
    try std.testing.expectEqual(@as(usize, 0), driver.consume_calls);
    try std.testing.expect(driver.saw_no_deadline);
}

test "postgres libpq async reader returns cancelled before waiting or consuming input" {
    const Driver = struct {
        fn flush(_: *@This()) c_int {
            unreachable;
        }

        fn wait(_: *@This(), _: i16, _: ?u64, _: ?CancellationToken) anyerror!i16 {
            unreachable;
        }

        fn consumeInput(_: *@This()) c_int {
            unreachable;
        }

        fn isBusy(_: *@This()) c_int {
            unreachable;
        }

        fn getResult(_: *@This()) ?*PGresult {
            unreachable;
        }

        fn clear(_: *@This(), _: ?*PGresult) void {
            unreachable;
        }

        fn restoreBlocking(_: *@This()) c_int {
            unreachable;
        }
    };

    var cancellation = std.atomic.Value(bool).init(true);
    var driver = Driver{};
    try std.testing.expectError(
        error.Cancelled,
        readSingleAsyncResultWithDeadline(&driver, null, CancellationToken.fromAtomic(&cancellation)),
    );
}

test "postgres libpq result decoding observes cancellation at periodic checkpoints" {
    var cells_until_check: u8 = 1;
    var cancellation = std.atomic.Value(bool).init(false);
    try checkResultDecodeProgress(null, CancellationToken.fromAtomic(&cancellation), &cells_until_check);
    try std.testing.expectEqual(@as(u8, 64), cells_until_check);

    cells_until_check = 1;
    cancellation.store(true, .release);
    try std.testing.expectError(
        error.Cancelled,
        checkResultDecodeProgress(null, CancellationToken.fromAtomic(&cancellation), &cells_until_check),
    );
}

test "postgres libpq global permit wait observes cancellation without a deadline" {
    const alloc = std.testing.allocator;
    var executor = Executor.initForPermitTests(alloc);
    defer executor.deinit();
    executor.total_connections.store(max_total_connections, .release);

    const Worker = struct {
        fn run(
            inner: *Executor,
            cancellation: *const std.atomic.Value(bool),
            cancelled: *std.atomic.Value(bool),
            failed: *std.atomic.Value(bool),
        ) void {
            inner.acquireGlobalConnectionPermits(1, null, CancellationToken.fromAtomic(cancellation)) catch |err| {
                if (err == error.Cancelled) {
                    cancelled.store(true, .release);
                } else {
                    failed.store(true, .release);
                }
                return;
            };
            failed.store(true, .release);
        }
    };

    var cancellation = std.atomic.Value(bool).init(false);
    var cancelled = std.atomic.Value(bool).init(false);
    var failed = std.atomic.Value(bool).init(false);
    var thread = try std.testing.io.concurrent(Worker.run, .{
        &executor,
        &cancellation,
        &cancelled,
        &failed,
    });
    while (executor.permit_waiter_count.load(.acquire) == 0) spinOrYield();
    cancellation.store(true, .release);
    thread.await(std.testing.io);

    try std.testing.expect(cancelled.load(.acquire));
    try std.testing.expect(!failed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), executor.permit_waiter_count.load(.acquire));
    try std.testing.expectEqual(max_total_connections, executor.total_connections.load(.acquire));
    executor.releaseGlobalConnections(max_total_connections);
}

test "postgres libpq pool wait observes cancellation without a deadline" {
    const alloc = std.testing.allocator;
    var executor = Executor.initForPermitTests(alloc);
    defer executor.deinit();
    const dsn = "postgres://pool-cancel";
    const pool = try executor.getOrCreateConnectionPool(dsn, null, null);
    defer executor.releaseConnectionPool(pool);
    lock(&pool.mutex);
    pool.total = max_connections_per_dsn;
    pool.mutex.unlock();

    const Worker = struct {
        fn run(
            inner: *Executor,
            inner_dsn: []const u8,
            cancellation: *const std.atomic.Value(bool),
            cancelled: *std.atomic.Value(bool),
            failed: *std.atomic.Value(bool),
        ) void {
            var lease = inner.acquireConnection(inner_dsn, null, CancellationToken.fromAtomic(cancellation)) catch |err| {
                if (err == error.Cancelled) {
                    cancelled.store(true, .release);
                } else {
                    failed.store(true, .release);
                }
                return;
            };
            lease.release();
            failed.store(true, .release);
        }
    };

    var cancellation = std.atomic.Value(bool).init(false);
    var cancelled = std.atomic.Value(bool).init(false);
    var failed = std.atomic.Value(bool).init(false);
    var thread = try std.testing.io.concurrent(Worker.run, .{
        &executor,
        dsn,
        &cancellation,
        &cancelled,
        &failed,
    });
    platform_time.sleepNs(cancellation_poll_interval_ns);
    cancellation.store(true, .release);
    thread.await(std.testing.io);

    try std.testing.expect(cancelled.load(.acquire));
    try std.testing.expect(!failed.load(.acquire));
    lock(&pool.mutex);
    try std.testing.expectEqual(max_connections_per_dsn, pool.total);
    pool.total = 0;
    pool.mutex.unlock();
}

test "postgres libpq cancellation during connect polling closes the fresh connection" {
    const Fake = struct {
        var cancellation: ?*std.atomic.Value(bool) = null;
        var finish_count: usize = 0;

        fn connectStart(_: [*:0]const u8) callconv(.c) ?*PGconn {
            return @ptrFromInt(1);
        }

        fn connectPoll(_: ?*PGconn) callconv(.c) c_uint {
            cancellation.?.store(true, .release);
            return 1; // PGRES_POLLING_READING
        }

        fn setNonblocking(_: ?*PGconn, _: c_int) callconv(.c) c_int {
            return 0;
        }

        fn finish(_: ?*PGconn) callconv(.c) void {
            finish_count += 1;
        }
    };

    var executor = Executor.initForPermitTests(std.testing.allocator);
    defer executor.deinit();
    executor.pqconnectStart = Fake.connectStart;
    executor.pqconnectPoll = Fake.connectPoll;
    executor.pqsetnonblocking = Fake.setNonblocking;
    executor.pqfinish = Fake.finish;

    var cancellation = std.atomic.Value(bool).init(false);
    Fake.cancellation = &cancellation;
    Fake.finish_count = 0;
    try std.testing.expectError(
        error.Cancelled,
        executor.connectFreshWithDeadline("postgres://connect-cancel", null, CancellationToken.fromAtomic(&cancellation)),
    );
    try std.testing.expectEqual(@as(usize, 1), Fake.finish_count);
}

test "postgres libpq registration succeeds without libpq and fails on first use" {
    const alloc = std.testing.allocator;
    const c = struct {
        extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern fn unsetenv(name: [*:0]const u8) c_int;
    };

    const previous = std.c.getenv("ANTFLY_LIBPQ_PATH");
    const restore = if (previous) |value| try alloc.dupeSentinel(u8, std.mem.span(value), 0) else null;
    defer if (restore) |value| {
        _ = c.setenv("ANTFLY_LIBPQ_PATH", value, 1);
        alloc.free(value);
    } else {
        _ = c.unsetenv("ANTFLY_LIBPQ_PATH");
    };
    try std.testing.expectEqual(@as(c_int, 0), c.setenv("ANTFLY_LIBPQ_PATH", "/definitely/missing/libpq", 1));

    var registry = foreign_source.Registry{};
    defer registry.deinit(alloc);
    try registerDefaultExecutor(alloc, &registry);

    var source = try registry.create(alloc, .{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, "postgres://ignored"),
    });
    defer source.deinit(alloc);

    try std.testing.expectError(error.LibpqUnavailable, source.statistics("users"));
}

test "postgres libpq bounded lock rejects an expired deadline" {
    var mutex: Mutex = .unlocked;
    lock(&mutex);
    defer mutex.unlock();
    try std.testing.expectError(
        error.Timeout,
        lockUntil(&mutex, platform_time.monotonicNs()),
    );
}

test "postgres libpq pool registry evicts inactive DSNs at its hard bound" {
    const alloc = std.testing.allocator;
    var executor = Executor.initForPermitTests(alloc);
    defer executor.deinit();

    for (0..max_connection_pools + 8) |idx| {
        const dsn = try std.fmt.allocPrint(alloc, "postgres://bounded-pool-{d}", .{idx});
        defer alloc.free(dsn);
        const pool = try executor.getOrCreateConnectionPool(dsn, null, null);
        executor.releaseConnectionPool(pool);
    }
    try std.testing.expectEqual(max_connection_pools, executor.pools.count());
}

test "postgres libpq global permits are atomic and bounded" {
    const alloc = std.testing.allocator;
    var executor = Executor.initForPermitTests(alloc);
    defer executor.deinit();

    try std.testing.expect(executor.reserveGlobalConnections(max_total_connections));
    try std.testing.expect(!executor.reserveGlobalConnections(1));
    executor.releaseGlobalConnections(max_total_connections);
    try std.testing.expectEqual(@as(usize, 0), executor.total_connections.load(.acquire));
}

test "postgres libpq permit saturation preserves zero-connection pools" {
    const alloc = std.testing.allocator;
    var executor = Executor.initForPermitTests(alloc);
    defer executor.deinit();

    const pool_count = 4;
    for (0..pool_count) |idx| {
        const dsn = try std.fmt.allocPrint(alloc, "postgres://saturated-empty-{d}", .{idx});
        defer alloc.free(dsn);
        const pool = try executor.getOrCreateConnectionPool(dsn, null, null);
        executor.releaseConnectionPool(pool);
    }
    try std.testing.expect(executor.reserveGlobalConnections(max_total_connections));
    defer executor.releaseGlobalConnections(max_total_connections);

    try std.testing.expectError(
        error.Timeout,
        executor.acquireGlobalConnectionPermits(
            1,
            platform_time.monotonicNs() + 20 * std.time.ns_per_ms,
            null,
        ),
    );
    try std.testing.expectEqual(pool_count, executor.pools.count());
}

test "postgres libpq weighted FIFO preserves a queued two-permit cutover" {
    const alloc = std.testing.allocator;
    var executor = Executor.initForPermitTests(alloc);
    defer executor.deinit();

    try std.testing.expect(executor.reserveGlobalConnections(max_total_connections));
    var large_acquired: std.atomic.Value(bool) = .init(false);
    var small_acquired: std.atomic.Value(bool) = .init(false);
    var allow_release: std.atomic.Value(bool) = .init(false);
    var failed: std.atomic.Value(bool) = .init(false);
    const Contender = struct {
        fn run(
            inner: *Executor,
            count: usize,
            deadline_ns: u64,
            acquired: *std.atomic.Value(bool),
            release_permits: *std.atomic.Value(bool),
            failure: *std.atomic.Value(bool),
        ) void {
            inner.acquireGlobalConnectionPermits(count, deadline_ns, null) catch {
                failure.store(true, .release);
                return;
            };
            acquired.store(true, .release);
            while (!release_permits.load(.acquire)) spinOrYield();
            inner.releaseGlobalConnections(count);
        }
    };
    const deadline_ns = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    var large = try std.testing.io.concurrent(
        Contender.run,
        .{ &executor, 2, deadline_ns, &large_acquired, &allow_release, &failed },
    );
    var large_joined = false;
    defer {
        allow_release.store(true, .release);
        if (!large_joined) large.await(std.testing.io);
        const remaining = executor.total_connections.load(.acquire);
        if (remaining > 0) executor.releaseGlobalConnections(remaining);
    }

    while (executor.permit_waiter_count.load(.acquire) != 1) {
        try ensureDeadline(deadline_ns);
        spinOrYield();
    }
    var small = try std.testing.io.concurrent(
        Contender.run,
        .{ &executor, 1, deadline_ns, &small_acquired, &allow_release, &failed },
    );
    var small_joined = false;
    defer {
        allow_release.store(true, .release);
        if (!small_joined) small.await(std.testing.io);
    }
    while (executor.permit_waiter_count.load(.acquire) != 2) {
        try ensureDeadline(deadline_ns);
        spinOrYield();
    }

    // A single free slot must remain reserved for the FIFO head. The younger
    // one-permit waiter cannot steal it.
    executor.releaseGlobalConnections(1);
    try std.testing.expectEqual(@as(usize, max_total_connections - 1), executor.total_connections.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), executor.permit_waiter_count.load(.acquire));
    try std.testing.expect(!large_acquired.load(.acquire));
    try std.testing.expect(!small_acquired.load(.acquire));

    executor.releaseGlobalConnections(1);
    while (!large_acquired.load(.acquire)) {
        try ensureDeadline(deadline_ns);
        spinOrYield();
    }
    try std.testing.expect(!small_acquired.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), executor.permit_waiter_count.load(.acquire));

    allow_release.store(true, .release);
    large.await(std.testing.io);
    large_joined = true;
    small.await(std.testing.io);
    small_joined = true;
    try std.testing.expect(!failed.load(.acquire));
    try std.testing.expect(small_acquired.load(.acquire));
}

test "postgres libpq timed out weighted head hands released capacity to follower" {
    const alloc = std.testing.allocator;
    var executor = Executor.initForPermitTests(alloc);
    defer executor.deinit();

    try std.testing.expect(executor.reserveGlobalConnections(max_total_connections));
    var head_timed_out: std.atomic.Value(bool) = .init(false);
    var follower_acquired: std.atomic.Value(bool) = .init(false);
    var failed: std.atomic.Value(bool) = .init(false);

    const Head = struct {
        fn run(
            inner: *Executor,
            deadline_ns: u64,
            timed_out: *std.atomic.Value(bool),
            failure: *std.atomic.Value(bool),
        ) void {
            inner.acquireGlobalConnectionPermits(2, deadline_ns, null) catch |err| {
                if (err == error.Timeout) {
                    timed_out.store(true, .release);
                } else {
                    failure.store(true, .release);
                }
                return;
            };
            failure.store(true, .release);
            inner.releaseGlobalConnections(2);
        }
    };
    const Follower = struct {
        fn run(
            inner: *Executor,
            deadline_ns: u64,
            acquired: *std.atomic.Value(bool),
            failure: *std.atomic.Value(bool),
        ) void {
            inner.acquireGlobalConnectionPermits(1, deadline_ns, null) catch {
                failure.store(true, .release);
                return;
            };
            acquired.store(true, .release);
            inner.releaseGlobalConnections(1);
        }
    };

    const head_deadline_ns = platform_time.monotonicNs() + std.time.ns_per_s;
    const test_deadline_ns = head_deadline_ns + 5 * std.time.ns_per_s;
    var head = try std.testing.io.concurrent(
        Head.run,
        .{ &executor, head_deadline_ns, &head_timed_out, &failed },
    );
    var head_joined = false;
    defer {
        if (!head_joined) head.await(std.testing.io);
        const remaining = executor.total_connections.load(.acquire);
        if (remaining > 0) executor.releaseGlobalConnections(remaining);
    }

    while (executor.permit_waiter_count.load(.acquire) != 1) {
        try ensureDeadline(test_deadline_ns);
        spinOrYield();
    }
    var follower = try std.testing.io.concurrent(
        Follower.run,
        .{ &executor, test_deadline_ns, &follower_acquired, &failed },
    );
    var follower_joined = false;
    defer if (!follower_joined) follower.await(std.testing.io);

    while (executor.permit_waiter_count.load(.acquire) != 2) {
        try ensureDeadline(test_deadline_ns);
        spinOrYield();
    }
    while (platform_time.monotonicNs() < head_deadline_ns) spinOrYield();

    // This release deliberately races the head's timed wait cleanup. Whether
    // cancellation or release enters the scheduler first, the expired
    // two-permit owner must roll back exactly once and the one-permit follower
    // must receive the available slot.
    executor.releaseGlobalConnections(1);
    head.await(std.testing.io);
    head_joined = true;
    follower.await(std.testing.io);
    follower_joined = true;

    try std.testing.expect(head_timed_out.load(.acquire));
    try std.testing.expect(follower_acquired.load(.acquire));
    try std.testing.expect(!failed.load(.acquire));
    try std.testing.expectEqual(@as(usize, max_total_connections - 1), executor.total_connections.load(.acquire));
}

test "postgres libpq cancelled FIFO head hands capacity to next waiter" {
    const alloc = std.testing.allocator;
    var executor = Executor.initForPermitTests(alloc);
    defer executor.deinit();

    try std.testing.expect(executor.reserveGlobalConnections(max_total_connections));
    defer {
        const remaining = executor.total_connections.load(.acquire);
        if (remaining > 0) executor.releaseGlobalConnections(remaining);
    }

    var head_availability = try PoolAvailability.init(alloc);
    defer head_availability.deinit();
    var next_availability = try PoolAvailability.init(alloc);
    defer next_availability.deinit();
    var head = Executor.PermitWaiter{
        .availability = head_availability,
        .count = 2,
    };
    var next = Executor.PermitWaiter{
        .availability = next_availability,
        .count = 1,
    };

    lock(&executor.permit_mutex);
    executor.enqueueGlobalPermitWaiterLocked(&head);
    executor.enqueueGlobalPermitWaiterLocked(&next);
    executor.permit_mutex.unlock();

    // One slot cannot satisfy the weighted head, so it remains reserved.
    executor.releaseGlobalConnections(1);
    try std.testing.expect(head.queued);
    try std.testing.expect(next.queued);

    // Timeout cleanup uses this exact cancellation path. Removing the head
    // must atomically hand the available slot to the next FIFO waiter.
    executor.cancelGlobalPermitWaiter(&head);
    try std.testing.expect(!head.queued);
    try std.testing.expect(!head.granted);
    try std.testing.expect(!next.queued);
    try std.testing.expect(next.granted);
    try std.testing.expectEqual(@as(usize, 0), executor.permit_waiter_count.load(.acquire));

    executor.cancelGlobalPermitWaiter(&next);
}

test "postgres libpq idle reclamation transfers only missing capacity" {
    const alloc = std.testing.allocator;
    var executor = Executor.initForPermitTests(alloc);
    defer executor.deinit();

    const Fake = struct {
        var finish_count: usize = 0;

        fn finish(_: ?*PGconn) callconv(.c) void {
            finish_count += 1;
        }
    };
    Fake.finish_count = 0;
    executor.pqfinish = Fake.finish;

    const pool = try executor.getOrCreateConnectionPool("postgres://permit-transfer", null, null);
    defer executor.releaseConnectionPool(pool);
    const fake_conn: *PGconn = @ptrFromInt(1);
    lock(&pool.mutex);
    pool.idle.append(alloc, fake_conn) catch |err| {
        pool.mutex.unlock();
        return err;
    };
    pool.total = 1;
    pool.mutex.unlock();

    try std.testing.expect(executor.reserveGlobalConnections(max_total_connections - 1));
    defer {
        const remaining = executor.total_connections.load(.acquire);
        if (remaining > 0) executor.releaseGlobalConnections(remaining);
    }
    try executor.acquireGlobalConnectionPermits(
        2,
        platform_time.monotonicNs() + std.time.ns_per_s,
        null,
    );
    try std.testing.expectEqual(@as(usize, 1), Fake.finish_count);
    try std.testing.expectEqual(max_total_connections, executor.total_connections.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), pool.total);
    try std.testing.expectEqual(@as(usize, 0), pool.idle.items.len);

    executor.releaseGlobalConnections(2);
}

test "postgres libpq reclamation leaves permit scheduling responsive" {
    const alloc = std.testing.allocator;
    var executor = Executor.initForPermitTests(alloc);
    defer executor.deinit();

    const Fake = struct {
        var finish_entered: std.atomic.Value(bool) = .init(false);
        var allow_finish: std.atomic.Value(bool) = .init(false);

        fn finish(_: ?*PGconn) callconv(.c) void {
            finish_entered.store(true, .release);
            while (!allow_finish.load(.acquire)) spinOrYield();
        }
    };
    Fake.finish_entered.store(false, .release);
    Fake.allow_finish.store(false, .release);
    executor.pqfinish = Fake.finish;

    const pool = try executor.getOrCreateConnectionPool("postgres://permit-responsive", null, null);
    defer executor.releaseConnectionPool(pool);
    const fake_conn: *PGconn = @ptrFromInt(1);
    lock(&pool.mutex);
    pool.idle.append(alloc, fake_conn) catch |err| {
        pool.mutex.unlock();
        return err;
    };
    pool.total = 1;
    pool.mutex.unlock();

    try std.testing.expect(executor.reserveGlobalConnections(max_total_connections - 1));
    defer {
        const remaining = executor.total_connections.load(.acquire);
        if (remaining > 0) executor.releaseGlobalConnections(remaining);
    }

    var head_acquired: std.atomic.Value(bool) = .init(false);
    var release_head: std.atomic.Value(bool) = .init(false);
    var head_failed: std.atomic.Value(bool) = .init(false);
    const Head = struct {
        fn run(
            inner: *Executor,
            acquired: *std.atomic.Value(bool),
            release_permits: *std.atomic.Value(bool),
            failed: *std.atomic.Value(bool),
        ) void {
            inner.acquireGlobalConnectionPermits(
                2,
                platform_time.monotonicNs() + 5 * std.time.ns_per_s,
                null,
            ) catch {
                failed.store(true, .release);
                return;
            };
            acquired.store(true, .release);
            while (!release_permits.load(.acquire)) spinOrYield();
            inner.releaseGlobalConnections(2);
        }
    };
    var head = try std.testing.io.concurrent(
        Head.run,
        .{ &executor, &head_acquired, &release_head, &head_failed },
    );
    var head_joined = false;
    defer {
        Fake.allow_finish.store(true, .release);
        release_head.store(true, .release);
        if (!head_joined) head.await(std.testing.io);
    }

    const deadline_ns = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!Fake.finish_entered.load(.acquire)) {
        try ensureDeadline(deadline_ns);
        spinOrYield();
    }

    // The head is blocked in pqfinish, after atomically transferring its
    // reclaimed permit. Scheduling must remain independently available.
    const scheduler_lock_available = executor.permit_mutex.tryLock();
    if (scheduler_lock_available) executor.permit_mutex.unlock();
    try std.testing.expect(scheduler_lock_available);

    var next_availability = try PoolAvailability.init(alloc);
    defer next_availability.deinit();
    var next = Executor.PermitWaiter{
        .availability = next_availability,
        .count = 1,
    };
    var next_cancelled = false;
    defer if (!next_cancelled) executor.cancelGlobalPermitWaiter(&next);

    lock(&executor.permit_mutex);
    const head_is_reclaiming = if (executor.permit_waiter_head) |queued_head|
        queued_head.reclaiming
    else
        false;
    if (head_is_reclaiming) executor.enqueueGlobalPermitWaiterLocked(&next);
    executor.permit_mutex.unlock();
    try std.testing.expect(head_is_reclaiming);

    // A concurrent release can enter the scheduler, but must leave the permit
    // for the reclaiming FIFO owner rather than double-granting it or allowing
    // the younger one-slot waiter to bypass.
    executor.releaseGlobalConnections(1);
    try std.testing.expect(next.queued);
    try std.testing.expect(!next.granted);

    Fake.allow_finish.store(true, .release);
    while (!head_acquired.load(.acquire) or !next.granted) {
        try ensureDeadline(deadline_ns);
        spinOrYield();
    }
    try std.testing.expect(!head_failed.load(.acquire));
    try std.testing.expect(!next.queued);

    executor.cancelGlobalPermitWaiter(&next);
    next_cancelled = true;
    release_head.store(true, .release);
    head.await(std.testing.io);
    head_joined = true;
}

test "postgres libpq availability broadcast wakes every waiter" {
    const alloc = std.testing.allocator;
    var availability = try PoolAvailability.init(alloc);
    defer availability.deinit();

    const Waiter = struct {
        fn run(
            inner: *PoolAvailability,
            observed: u64,
            completed: *std.atomic.Value(usize),
            failed: *std.atomic.Value(bool),
        ) void {
            inner.waitForChange(observed, null) catch {
                failed.store(true, .release);
                return;
            };
            _ = completed.fetchAdd(1, .acq_rel);
        }
    };
    const waiter_count = 4;
    const observed = availability.snapshot();
    var completed: std.atomic.Value(usize) = .init(0);
    var failed: std.atomic.Value(bool) = .init(false);
    var waiters: [waiter_count]std.Io.Future(void) = undefined;
    var started_tasks: usize = 0;
    defer {
        availability.advanceAll();
        for (waiters[0..started_tasks]) |*task| task.await(std.testing.io);
    }
    for (&waiters) |*waiter| {
        waiter.* = try std.testing.io.concurrent(
            Waiter.run,
            .{ &availability, observed, &completed, &failed },
        );
        started_tasks += 1;
    }
    availability.advanceAll();
    for (&waiters) |*waiter| waiter.await(std.testing.io);
    try std.testing.expect(!failed.load(.acquire));
    try std.testing.expectEqual(waiter_count, completed.load(.acquire));
}

test "postgres libpq column cache keys isolate sources and schemas" {
    const alloc = std.testing.allocator;
    const first = try columnCacheKeyAlloc(alloc, "postgres://one", "\"public\".\"users\"");
    defer alloc.free(first);
    const second = try columnCacheKeyAlloc(alloc, "postgres://two", "\"public\".\"users\"");
    defer alloc.free(second);
    const third = try columnCacheKeyAlloc(alloc, "postgres://one", "\"audit\".\"users\"");
    defer alloc.free(third);
    const embedded_nul_left = try columnCacheKeyAlloc(alloc, "a\x00b", "c");
    defer alloc.free(embedded_nul_left);
    const embedded_nul_right = try columnCacheKeyAlloc(alloc, "a", "b\x00c");
    defer alloc.free(embedded_nul_right);
    try std.testing.expect(!std.mem.eql(u8, first, second));
    try std.testing.expect(!std.mem.eql(u8, first, third));
    try std.testing.expect(!std.mem.eql(u8, embedded_nul_left, embedded_nul_right));
    try std.testing.expectEqual(
        @as(u64, "postgres://one".len),
        std.mem.readInt(u64, first[0..@sizeOf(u64)], .little),
    );
}

test "postgres libpq clone helpers are allocation-failure safe" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            var args = [_]sql.ParameterValue{
                .{ .string = @constCast("value") },
                .{ .integer = 7 },
            };
            const cloned_args = try cloneParameterValuesAlloc(alloc, &args);
            defer {
                for (cloned_args) |*arg| arg.deinit(alloc);
                if (cloned_args.len > 0) alloc.free(cloned_args);
            }
            var owned_args = try OwnedArgs.init(alloc, &args);
            defer owned_args.deinit(alloc);

            var columns = [_]foreign_source.Column{.{
                .name = @constCast("id"),
                .data_type = @constCast("uuid"),
                .nullable = false,
            }};
            const cloned_columns = try cloneColumnsAlloc(alloc, &columns);
            defer freeColumns(alloc, cloned_columns);

            const cache_key = try columnCacheKeyAlloc(alloc, "postgres://one", "\"public\".\"users\"");
            defer alloc.free(cache_key);
            var prepared = try tableNamePreparedQueryAlloc(alloc, "SELECT $1", "public.users");
            defer prepared.deinit(alloc);
            var relation_prepared = try relationPreparedQueryAlloc(alloc, "SELECT to_regclass($1)", "public.users");
            defer relation_prepared.deinit(alloc);
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "postgres libpq created replication snapshot cloning is allocation-failure safe" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            var slot_created = false;
            var snapshot = Executor.cloneCreatedReplicationSnapshotAlloc(
                alloc,
                &slot_created,
                "0/16B6C50",
                "00000003-0000001B-1",
            ) catch |err| {
                // Ownership is recorded before either allocation, ensuring
                // every post-create local failure selects bounded cleanup.
                try std.testing.expect(slot_created);
                return err;
            };
            defer snapshot.deinit(alloc);
            try std.testing.expect(slot_created);
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "Source tuple DELETE retains full old image markers and rejects changed tuple widths" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var cache: std.AutoHashMapUnmanaged(u32, PgoutputRelation) = .empty;
    const columns = try scratch.alloc(PgoutputColumn, 1);
    columns[0] = .{ .name = try scratch.dupe(u8, "payload"), .data_type = TypeOid.jsonb, .key = true };
    try cache.put(scratch, 42, .{ .namespace = try scratch.dupe(u8, "public"), .relation_name = try scratch.dupe(u8, "rows"), .columns = columns, .replica_identity = 'f' });
    for ([_]u8{ 'O', 'K' }) |marker| {
        var bytes = [_]u8{ 'D', 0, 0, 0, 42, marker, 0, 1, 't', 0, 0, 0, 4, 'n', 'u', 'l', 'l' };
        var change = (try parsePgoutputMessageAlloc(a, &bytes, &cache)).change;
        defer change.deinit(a);
        try std.testing.expectEqual(marker == 'O', (change.before_kind == .full));
        try std.testing.expect(change.row.?.object.get("payload").? == .null);
        try std.testing.expectEqualStrings("payload", change.json_null_fields[0]);
        bytes[7] = 0;
        try std.testing.expectError(error.InvalidReplicationSourceRow, parsePgoutputMessageAlloc(a, &bytes, &cache));
        bytes[7] = 2;
        try std.testing.expectError(error.InvalidReplicationSourceRow, parsePgoutputMessageAlloc(a, &bytes, &cache));
    }
}

test "postgres libpq parser rejects truncate and unknown effects rather than acknowledging lost mutations" {
    const alloc = std.testing.allocator;
    var relations = std.AutoHashMapUnmanaged(u32, PgoutputRelation).empty;
    defer deinitPgoutputRelationCache(alloc, &relations);
    try std.testing.expectError(error.UnsupportedReplicationTruncate, parsePgoutputMessageAlloc(alloc, "T", &relations));
    try std.testing.expectError(error.UnsupportedReplicationMessage, parsePgoutputMessageAlloc(alloc, "?", &relations));
}

test "postgres libpq parser decodes pgoutput relation and row changes" {
    const alloc = std.testing.allocator;

    var relation_cache = std.AutoHashMapUnmanaged(u32, PgoutputRelation).empty;
    defer deinitPgoutputRelationCache(alloc, &relation_cache);

    const Builder = struct {
        fn appendU16(out: *std.ArrayListUnmanaged(u8), alloc_inner: Allocator, value: u16) !void {
            var buf: [2]u8 = undefined;
            std.mem.writeInt(u16, &buf, value, .big);
            try out.appendSlice(alloc_inner, &buf);
        }

        fn appendU32(out: *std.ArrayListUnmanaged(u8), alloc_inner: Allocator, value: u32) !void {
            var buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &buf, value, .big);
            try out.appendSlice(alloc_inner, &buf);
        }

        fn appendU64(out: *std.ArrayListUnmanaged(u8), alloc_inner: Allocator, value: u64) !void {
            var buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &buf, value, .big);
            try out.appendSlice(alloc_inner, &buf);
        }

        fn appendI32(out: *std.ArrayListUnmanaged(u8), alloc_inner: Allocator, value: i32) !void {
            var buf: [4]u8 = undefined;
            std.mem.writeInt(i32, &buf, value, .big);
            try out.appendSlice(alloc_inner, &buf);
        }

        fn appendI64(out: *std.ArrayListUnmanaged(u8), alloc_inner: Allocator, value: i64) !void {
            var buf: [8]u8 = undefined;
            std.mem.writeInt(i64, &buf, value, .big);
            try out.appendSlice(alloc_inner, &buf);
        }

        fn appendCString(out: *std.ArrayListUnmanaged(u8), alloc_inner: Allocator, value: []const u8) !void {
            try out.appendSlice(alloc_inner, value);
            try out.append(alloc_inner, 0);
        }

        fn appendTextTupleValue(out: *std.ArrayListUnmanaged(u8), alloc_inner: Allocator, value: []const u8) !void {
            try out.append(alloc_inner, 't');
            try appendI32(out, alloc_inner, @intCast(value.len));
            try out.appendSlice(alloc_inner, value);
        }
    };

    var relation_bytes = std.ArrayListUnmanaged(u8).empty;
    defer relation_bytes.deinit(alloc);
    try relation_bytes.append(alloc, 'R');
    try Builder.appendU32(&relation_bytes, alloc, 42);
    try Builder.appendCString(&relation_bytes, alloc, "public");
    try Builder.appendCString(&relation_bytes, alloc, "users");
    try relation_bytes.append(alloc, 'd');
    try Builder.appendU16(&relation_bytes, alloc, 3);
    inline for (.{ .{ "id", TypeOid.int8 }, .{ "name", TypeOid.json }, .{ "tier", TypeOid.boolean } }, 0..) |column, index| {
        try relation_bytes.append(alloc, @intFromBool(index == 0));
        try Builder.appendCString(&relation_bytes, alloc, column.@"0");
        try Builder.appendU32(&relation_bytes, alloc, column.@"1");
        try Builder.appendU32(&relation_bytes, alloc, 0);
    }
    try std.testing.expectEqual(ParsedPgoutputMessage.none, try parsePgoutputMessageAlloc(alloc, relation_bytes.items, &relation_cache));

    var begin_bytes = std.ArrayListUnmanaged(u8).empty;
    defer begin_bytes.deinit(alloc);
    try begin_bytes.append(alloc, 'B');
    try Builder.appendU64(&begin_bytes, alloc, 1);
    try Builder.appendI64(&begin_bytes, alloc, 0);
    try Builder.appendU32(&begin_bytes, alloc, 9);
    const parsed_begin = try parsePgoutputMessageAlloc(alloc, begin_bytes.items, &relation_cache);
    try std.testing.expect(parsed_begin == .begin);
    try std.testing.expectEqual(@as(u32, 9), parsed_begin.begin.xid);
    try std.testing.expectEqual(@as(u64, 1), parsed_begin.begin.final_lsn);

    var insert_bytes = std.ArrayListUnmanaged(u8).empty;
    defer insert_bytes.deinit(alloc);
    try insert_bytes.append(alloc, 'I');
    try Builder.appendU32(&insert_bytes, alloc, 42);
    try insert_bytes.append(alloc, 'N');
    try Builder.appendU16(&insert_bytes, alloc, 3);
    try Builder.appendTextTupleValue(&insert_bytes, alloc, "7");
    try Builder.appendTextTupleValue(&insert_bytes, alloc, "{\"city\":\"sf\"}");
    try Builder.appendTextTupleValue(&insert_bytes, alloc, "t");

    var insert = switch (try parsePgoutputMessageAlloc(alloc, insert_bytes.items, &relation_cache)) {
        .change => |change| change,
        else => return error.TestUnexpectedResult,
    };
    defer insert.deinit(alloc);
    try std.testing.expectEqual(foreign_source.ReplicationOp.insert, insert.op);
    const insert_row = insert.row.?;
    try std.testing.expectEqual(@as(i64, 7), insert_row.object.get("id").?.integer);
    try std.testing.expectEqualStrings("sf", insert_row.object.get("name").?.object.get("city").?.string);
    try std.testing.expect(insert_row.object.get("tier").?.bool);

    var update_bytes = std.ArrayListUnmanaged(u8).empty;
    defer update_bytes.deinit(alloc);
    try update_bytes.append(alloc, 'U');
    try Builder.appendU32(&update_bytes, alloc, 42);
    try update_bytes.append(alloc, 'N');
    try Builder.appendU16(&update_bytes, alloc, 3);
    try Builder.appendTextTupleValue(&update_bytes, alloc, "7");
    try Builder.appendTextTupleValue(&update_bytes, alloc, "{\"city\":\"la\"}");
    try Builder.appendTextTupleValue(&update_bytes, alloc, "f");

    var update = switch (try parsePgoutputMessageAlloc(alloc, update_bytes.items, &relation_cache)) {
        .change => |change| change,
        else => return error.TestUnexpectedResult,
    };
    defer update.deinit(alloc);
    try std.testing.expectEqual(foreign_source.ReplicationOp.update, update.op);
    try std.testing.expectEqual(foreign_source.ReplicationBeforeKind.key, update.before_kind);
    try std.testing.expectEqual(@as(usize, 1), update.beforeImage().?.object.count());
    try std.testing.expectEqual(update.row.?.object.get("id").?.integer, update.beforeImage().?.object.get("id").?.integer);
    try std.testing.expectEqualStrings("la", update.row.?.object.get("name").?.object.get("city").?.string);
    try std.testing.expect(!update.row.?.object.get("tier").?.bool);

    // UPDATE K retains the old replica identity; otherwise a source primary
    // key move could create the new document and leave the old one behind.
    var moved_bytes = std.ArrayListUnmanaged(u8).empty;
    defer moved_bytes.deinit(alloc);
    try moved_bytes.append(alloc, 'U');
    try Builder.appendU32(&moved_bytes, alloc, 42);
    try moved_bytes.append(alloc, 'K');
    try Builder.appendU16(&moved_bytes, alloc, 3);
    try Builder.appendTextTupleValue(&moved_bytes, alloc, "7");
    try moved_bytes.appendSlice(alloc, "nn");
    try moved_bytes.appendSlice(alloc, update_bytes.items[5..]);
    var moved = switch (try parsePgoutputMessageAlloc(alloc, moved_bytes.items, &relation_cache)) {
        .change => |change| change,
        else => return error.TestUnexpectedResult,
    };
    defer moved.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 7), moved.before.?.object.get("id").?.integer);
    try std.testing.expect(!(moved.before_kind == .full));
    moved_bytes.items[5] = 'O';
    var full_old = switch (try parsePgoutputMessageAlloc(alloc, moved_bytes.items, &relation_cache)) {
        .change => |change| change,
        else => return error.TestUnexpectedResult,
    };
    defer full_old.deinit(alloc);
    try std.testing.expect((full_old.before_kind == .full));
    for ([_]bool{ true, false }) |old_is_json_null| {
        var old_null_bytes = std.ArrayListUnmanaged(u8).empty;
        defer old_null_bytes.deinit(alloc);
        try old_null_bytes.append(alloc, 'U');
        try Builder.appendU32(&old_null_bytes, alloc, 42);
        try old_null_bytes.append(alloc, 'O');
        try Builder.appendU16(&old_null_bytes, alloc, 3);
        try Builder.appendTextTupleValue(&old_null_bytes, alloc, "7");
        if (old_is_json_null) try Builder.appendTextTupleValue(&old_null_bytes, alloc, "null") else try old_null_bytes.append(alloc, 'n');
        try old_null_bytes.append(alloc, 'n');
        try old_null_bytes.appendSlice(alloc, update_bytes.items[5..]);
        var old_null = switch (try parsePgoutputMessageAlloc(alloc, old_null_bytes.items, &relation_cache)) {
            .change => |change| change,
            else => return error.TestUnexpectedResult,
        };
        defer old_null.deinit(alloc);
        try std.testing.expect(old_null.before.?.object.get("name").? == .null);
        try std.testing.expectEqual(@as(usize, if (old_is_json_null) 1 else 0), old_null.before_json_null_fields.len);
        if (old_is_json_null) try std.testing.expectEqualStrings("name", old_null.before_json_null_fields[0]);
        try std.testing.expectEqual(@as(usize, 0), old_null.json_null_fields.len);
    }

    var key_update_bytes = std.ArrayListUnmanaged(u8).empty;
    defer key_update_bytes.deinit(alloc);
    try key_update_bytes.append(alloc, 'U');
    try Builder.appendU32(&key_update_bytes, alloc, 42);
    try key_update_bytes.append(alloc, 'K');
    try Builder.appendU16(&key_update_bytes, alloc, 3);
    try Builder.appendTextTupleValue(&key_update_bytes, alloc, "7");
    try key_update_bytes.appendSlice(alloc, "nn");
    try key_update_bytes.append(alloc, 'N');
    try Builder.appendU16(&key_update_bytes, alloc, 3);
    try Builder.appendTextTupleValue(&key_update_bytes, alloc, "8");
    try key_update_bytes.appendSlice(alloc, "un");
    var key_update = (try parsePgoutputMessageAlloc(alloc, key_update_bytes.items, &relation_cache)).change;
    defer key_update.deinit(alloc);
    try std.testing.expectEqual(foreign_source.ReplicationBeforeKind.key, key_update.before_kind);
    try std.testing.expect(!key_update.before_complete and !key_update.row_complete);
    try std.testing.expectEqual(@as(i64, 7), key_update.beforeImage().?.object.get("id").?.integer);
    try std.testing.expectEqual(@as(usize, 1), key_update.beforeImage().?.object.count());
    try std.testing.expectEqual(@as(i64, 8), key_update.row.?.object.get("id").?.integer);
    try std.testing.expect(key_update.row.?.object.get("name") == null);
    try std.testing.expect(key_update.row.?.object.get("tier").? == .null);
    var full_update_bytes = std.ArrayListUnmanaged(u8).empty;
    defer full_update_bytes.deinit(alloc);
    try full_update_bytes.append(alloc, 'U');
    try Builder.appendU32(&full_update_bytes, alloc, 42);
    try full_update_bytes.append(alloc, 'O');
    try Builder.appendU16(&full_update_bytes, alloc, 3);
    try Builder.appendTextTupleValue(&full_update_bytes, alloc, "7");
    try Builder.appendTextTupleValue(&full_update_bytes, alloc, "{\"city\":\"la\"}");
    try full_update_bytes.append(alloc, 'n');
    try full_update_bytes.appendSlice(alloc, update_bytes.items[5..]);
    var full_update = (try parsePgoutputMessageAlloc(alloc, full_update_bytes.items, &relation_cache)).change;
    defer full_update.deinit(alloc);
    try std.testing.expectEqual(foreign_source.ReplicationBeforeKind.full, full_update.before_kind);
    try std.testing.expect(full_update.before_complete and full_update.row_complete);
    try std.testing.expect(full_update.beforeImage().?.object.get("tier").? == .null);
    try std.testing.expectEqualStrings("la", full_update.beforeImage().?.object.get("name").?.object.get("city").?.string);
    const malformed_tuple = [_]u8{ 'I', 0, 0, 0, 42, 'N', 0, 2 };
    try std.testing.expectError(error.InvalidReplicationSourceRow, parsePgoutputMessageAlloc(alloc, &malformed_tuple, &relation_cache));

    var delete_bytes = std.ArrayListUnmanaged(u8).empty;
    defer delete_bytes.deinit(alloc);
    try delete_bytes.append(alloc, 'D');
    try Builder.appendU32(&delete_bytes, alloc, 42);
    try delete_bytes.append(alloc, 'O');
    try Builder.appendU16(&delete_bytes, alloc, 3);
    try Builder.appendTextTupleValue(&delete_bytes, alloc, "7");
    try Builder.appendTextTupleValue(&delete_bytes, alloc, "{\"city\":\"la\"}");
    try Builder.appendTextTupleValue(&delete_bytes, alloc, "f");

    var delete = switch (try parsePgoutputMessageAlloc(alloc, delete_bytes.items, &relation_cache)) {
        .change => |change| change,
        else => return error.TestUnexpectedResult,
    };
    defer delete.deinit(alloc);
    try std.testing.expectEqual(foreign_source.ReplicationOp.delete, delete.op);
    try std.testing.expectEqual(foreign_source.ReplicationBeforeKind.full, delete.before_kind);
    try std.testing.expect(delete.before_complete);
    try std.testing.expectEqual(@as(i64, 7), delete.row.?.object.get("id").?.integer);

    var commit_bytes = std.ArrayListUnmanaged(u8).empty;
    defer commit_bytes.deinit(alloc);
    try commit_bytes.append(alloc, 'C');
    try commit_bytes.append(alloc, 0);
    try Builder.appendU64(&commit_bytes, alloc, 1);
    try Builder.appendU64(&commit_bytes, alloc, 2);
    try Builder.appendI64(&commit_bytes, alloc, 1_000_000);
    const parsed_commit = try parsePgoutputMessageAlloc(alloc, commit_bytes.items, &relation_cache);
    try std.testing.expect(parsed_commit == .commit);
    try std.testing.expect(parsed_commit.commit.timestamp_ms > 946_684_800_000);
}

test "postgres libpq decodes bytea hex text" {
    const alloc = std.testing.allocator;
    const decoded = try decodeByteaHexAlloc(alloc, "\\x4869");
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings("Hi", decoded);
}

test "postgres libpq live deadline cancels slow query and pool remains reusable" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = Executor.init(alloc) catch return error.SkipZigTest;
    defer executor.deinit();
    const probe_conn = executor.connectFresh(alloc, dsn) catch return error.SkipZigTest;
    executor.pqfinish(probe_conn);
    try std.testing.expectError(
        error.Timeout,
        executor.statisticsAllocWithDeadline(
            alloc,
            dsn,
            "pg_catalog.pg_class",
            platform_time.monotonicNs(),
        ),
    );

    const stats = try executor.statisticsAllocWithDeadline(
        alloc,
        dsn,
        "pg_catalog.pg_class",
        platform_time.monotonicNs() + std.time.ns_per_s,
    );
    try std.testing.expect(stats.size_bytes > 0);

    {
        const before_snapshot = executor.total_connections.load(.acquire);
        var snapshot = try executor.asQueryExecutor().beginSnapshotQuery(alloc, dsn);
        try std.testing.expectEqual(before_snapshot + 1, executor.total_connections.load(.acquire));
        snapshot.deinit(alloc);
        try std.testing.expectEqual(before_snapshot, executor.total_connections.load(.acquire));
    }

    {
        var first_lease = try executor.acquireConnection(
            dsn,
            platform_time.monotonicNs() + std.time.ns_per_s,
            null,
        );
        defer first_lease.release();
        var second_lease = try executor.acquireConnection(
            dsn,
            platform_time.monotonicNs() + std.time.ns_per_s,
            null,
        );
        defer second_lease.release();
        try std.testing.expect(first_lease.conn != second_lease.conn);
    }

    {
        var saturation_leases: [max_connections_per_dsn]Executor.ConnectionLease = undefined;
        var initialized: usize = 0;
        defer {
            while (initialized > 0) {
                initialized -= 1;
                saturation_leases[initialized].release();
            }
        }
        while (initialized < saturation_leases.len) : (initialized += 1) {
            saturation_leases[initialized] = try executor.acquireConnection(
                dsn,
                platform_time.monotonicNs() + 2 * std.time.ns_per_s,
                null,
            );
        }
        try std.testing.expectError(
            error.Timeout,
            executor.acquireConnection(
                dsn,
                platform_time.monotonicNs() + 25 * std.time.ns_per_ms,
                null,
            ),
        );

        const SignaledWaiter = struct {
            fn run(
                inner_executor: *Executor,
                inner_dsn: []const u8,
                acquired: *std.atomic.Value(bool),
                failed: *std.atomic.Value(bool),
            ) void {
                var lease = inner_executor.acquireConnection(
                    inner_dsn,
                    platform_time.monotonicNs() + std.time.ns_per_s,
                    null,
                ) catch {
                    failed.store(true, .release);
                    return;
                };
                acquired.store(true, .release);
                lease.release();
            }
        };
        var acquired: std.atomic.Value(bool) = .init(false);
        var failed: std.atomic.Value(bool) = .init(false);
        var waiter = try std.testing.io.concurrent(
            SignaledWaiter.run,
            .{ &executor, dsn, &acquired, &failed },
        );
        platform_time.sleepNs(25 * std.time.ns_per_ms);
        initialized -= 1;
        saturation_leases[initialized].release();
        waiter.await(std.testing.io);
        try std.testing.expect(acquired.load(.acquire));
        try std.testing.expect(!failed.load(.acquire));
    }

    const slow_query = sql.PreparedQuery{
        .sql_text = try alloc.dupe(u8, "SELECT pg_sleep(0.25)"),
    };
    try std.testing.expectError(
        error.Timeout,
        executor.queryPreparedAllocWithDeadline(
            alloc,
            dsn,
            slow_query,
            platform_time.monotonicNs() + 25 * std.time.ns_per_ms,
            null,
        ),
    );

    const reuse_query = sql.PreparedQuery{
        .sql_text = try alloc.dupe(u8, "SELECT 1 AS value"),
    };
    var result = try executor.queryPreparedAllocWithDeadline(
        alloc,
        dsn,
        reuse_query,
        platform_time.monotonicNs() + std.time.ns_per_s,
        null,
    );
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(i64, 1), result.rows[0].object.get("value").?.integer);
}

test "postgres libpq live relation resolution supports schemas quoted dots and cache refresh" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = Executor.init(alloc) catch return error.SkipZigTest;
    defer executor.deinit();
    const conn = executor.connectFresh(alloc, dsn) catch return error.SkipZigTest;
    defer executor.pqfinish(conn);

    live_poll_test_counter += 1;
    const schema_name = try std.fmt.allocPrint(alloc, "antfly_relation_probe_{d}", .{live_poll_test_counter});
    defer alloc.free(schema_name);
    const relation_name = try std.fmt.allocPrint(alloc, "{s}.\"User.Events\"", .{schema_name});
    defer alloc.free(relation_name);
    const quoted_schema = try sql.postgresDialect().quote_identifier(alloc, schema_name);
    defer alloc.free(quoted_schema);
    const quoted_relation = try sql.quotePostgresRelationAlloc(alloc, relation_name);
    defer alloc.free(quoted_relation);

    const drop_schema_sql = try std.fmt.allocPrint(alloc, "DROP SCHEMA IF EXISTS {s} CASCADE", .{quoted_schema});
    defer alloc.free(drop_schema_sql);
    defer execCommandForTest(&executor, conn, alloc, drop_schema_sql) catch {};
    try execCommandForTest(&executor, conn, alloc, drop_schema_sql);

    const create_schema_sql = try std.fmt.allocPrint(alloc, "CREATE SCHEMA {s}", .{quoted_schema});
    defer alloc.free(create_schema_sql);
    try execCommandForTest(&executor, conn, alloc, create_schema_sql);
    const create_table_sql = try std.fmt.allocPrint(
        alloc,
        "CREATE TABLE {s} (id bigint PRIMARY KEY, payload text NOT NULL)",
        .{quoted_relation},
    );
    defer alloc.free(create_table_sql);
    try execCommandForTest(&executor, conn, alloc, create_table_sql);
    const insert_sql = try std.fmt.allocPrint(
        alloc,
        "INSERT INTO {s} (id, payload) VALUES (1, 'ok')",
        .{quoted_relation},
    );
    defer alloc.free(insert_sql);
    try execCommandForTest(&executor, conn, alloc, insert_sql);

    const initial_columns = try executor.discoverColumnsAlloc(alloc, dsn, relation_name);
    defer freeColumns(alloc, initial_columns);
    try std.testing.expectEqual(@as(usize, 2), initial_columns.len);
    try std.testing.expectEqualStrings("id", initial_columns[0].name);
    try std.testing.expectEqualStrings("payload", initial_columns[1].name);
    try std.testing.expect(!initial_columns[0].nullable);
    try std.testing.expect(!initial_columns[1].nullable);

    const select_sql = try sql.buildSelectStatementAlloc(alloc, sql.postgresDialect(), .{
        .table = relation_name,
    });
    var selected = try executor.queryPreparedAlloc(alloc, dsn, .{
        .sql_text = select_sql,
    });
    defer selected.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), selected.rows.len);
    try std.testing.expectEqual(@as(i64, 1), selected.rows[0].object.get("id").?.integer);

    const stats = try executor.statisticsAlloc(alloc, dsn, relation_name);
    try std.testing.expect(stats.size_bytes > 0);

    const alter_sql = try std.fmt.allocPrint(
        alloc,
        "ALTER TABLE {s} ADD COLUMN extra boolean",
        .{quoted_relation},
    );
    defer alloc.free(alter_sql);
    try execCommandForTest(&executor, conn, alloc, alter_sql);

    const runtime = try alloc.create(postgres_source.RuntimeSource);
    runtime.* = .{
        .alloc = alloc,
        .executor = executor.asQueryExecutor(),
        .dsn = try alloc.dupe(u8, dsn),
    };
    var source = runtime.asSource();
    defer source.deinit(alloc);
    var extra_fields = [_][]u8{@constCast("extra")};
    var refreshed_query = try source.query(alloc, .{
        .table = @constCast(relation_name),
        .fields = &extra_fields,
    });
    defer refreshed_query.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), refreshed_query.rows.len);
    try std.testing.expect(refreshed_query.rows[0].object.get("extra").? == .null);

    const refreshed_columns = try executor.discoverColumnsAlloc(alloc, dsn, relation_name);
    defer freeColumns(alloc, refreshed_columns);
    try std.testing.expectEqual(@as(usize, 3), refreshed_columns.len);
    try std.testing.expectEqualStrings("extra", refreshed_columns[2].name);
    try std.testing.expect(refreshed_columns[2].nullable);

    const drop_column_sql = try std.fmt.allocPrint(
        alloc,
        "ALTER TABLE {s} DROP COLUMN extra",
        .{quoted_relation},
    );
    defer alloc.free(drop_column_sql);
    try execCommandForTest(&executor, conn, alloc, drop_column_sql);
    try std.testing.expectError(
        error.UnknownColumn,
        source.query(alloc, .{
            .table = @constCast(relation_name),
            .fields = &extra_fields,
        }),
    );
}

test "postgres libpq live logical poll returns inserted row" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = try Executor.init(alloc);
    defer executor.deinit();

    const conn = executor.connect(dsn) catch return error.SkipZigTest;
    const wal_level_result = executor.execSimple(conn, alloc, "show wal_level") catch return error.SkipZigTest;
    defer executor.pqclear(wal_level_result);
    if (executor.pqntuples(wal_level_result) == 0 or executor.pqgetisnull(wal_level_result, 0, 0) != 0) {
        return error.SkipZigTest;
    }
    const wal_level = executor.pqgetvalue(wal_level_result, 0, 0)[0..@intCast(executor.pqgetlength(wal_level_result, 0, 0))];
    if (!std.ascii.eqlIgnoreCase(wal_level, "logical")) return error.SkipZigTest;

    live_poll_test_counter += 1;
    const suffix = live_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_probe_live_{d}", .{suffix});
    defer alloc.free(table_name);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_probe_live_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_probe_live_pub_{d}", .{suffix});
    defer alloc.free(publication_name);

    const drop_publication_sql = try std.fmt.allocPrint(alloc, "drop publication if exists {s}", .{publication_name});
    defer alloc.free(drop_publication_sql);
    const drop_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select pg_drop_replication_slot('{s}') from pg_replication_slots where slot_name = '{s}' and not active",
        .{ slot_name, slot_name },
    );
    defer alloc.free(drop_slot_sql);
    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{table_name});
    defer alloc.free(drop_table_sql);
    defer {
        execCommandForTest(&executor, conn, alloc, drop_publication_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_slot_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_table_sql) catch {};
    }

    const create_table_sql = try std.fmt.allocPrint(alloc, "create table {s} (id text primary key, name text not null)", .{table_name});
    defer alloc.free(create_table_sql);
    const create_publication_sql = try std.fmt.allocPrint(alloc, "create publication {s} for table {s}", .{ publication_name, table_name });
    defer alloc.free(create_publication_sql);
    const create_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select * from pg_create_logical_replication_slot('{s}', 'pgoutput')",
        .{slot_name},
    );
    defer alloc.free(create_slot_sql);
    const insert_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u1', 'Alice')", .{table_name});
    defer alloc.free(insert_sql);

    try execCommandForTest(&executor, conn, alloc, create_table_sql);
    try execCommandForTest(&executor, conn, alloc, create_publication_sql);
    try execCommandForTest(&executor, conn, alloc, create_slot_sql);

    var poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .limit = 16,
    };
    defer poll_params.deinit(alloc);

    var empty_poll_result = try executor.pollChangesAlloc(alloc, dsn, poll_params);
    defer empty_poll_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), empty_poll_result.changes.len);

    try execCommandForTest(&executor, conn, alloc, insert_sql);

    const snapshot_query = sql.PreparedQuery{
        .sql_text = try std.fmt.allocPrint(alloc, "SELECT * FROM {s} LIMIT 16 OFFSET 1", .{table_name}),
    };
    var snapshot_query_result = try executor.queryPreparedAlloc(alloc, dsn, snapshot_query);
    defer snapshot_query_result.deinit(alloc);

    var poll_result = try executor.pollChangesAlloc(alloc, dsn, poll_params);
    defer poll_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), poll_result.changes.len);
    try std.testing.expectEqual(foreign_source.ReplicationOp.insert, poll_result.changes[0].op);
    try std.testing.expectEqualStrings("u1", poll_result.changes[0].row.?.object.get("id").?.string);
    try std.testing.expectEqualStrings("Alice", poll_result.changes[0].row.?.object.get("name").?.string);
}

test "postgres libpq live logical poll can auto-create publication and slot before later insert" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = try Executor.init(alloc);
    defer executor.deinit();

    const conn = executor.connect(dsn) catch return error.SkipZigTest;
    const wal_level_result = executor.execSimple(conn, alloc, "show wal_level") catch return error.SkipZigTest;
    defer executor.pqclear(wal_level_result);
    if (executor.pqntuples(wal_level_result) == 0 or executor.pqgetisnull(wal_level_result, 0, 0) != 0) {
        return error.SkipZigTest;
    }
    const wal_level = executor.pqgetvalue(wal_level_result, 0, 0)[0..@intCast(executor.pqgetlength(wal_level_result, 0, 0))];
    if (!std.ascii.eqlIgnoreCase(wal_level, "logical")) return error.SkipZigTest;

    live_poll_test_counter += 1;
    const suffix = live_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_auto_live_{d}", .{suffix});
    defer alloc.free(table_name);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_auto_live_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_auto_live_pub_{d}", .{suffix});
    defer alloc.free(publication_name);

    const drop_publication_sql = try std.fmt.allocPrint(alloc, "drop publication if exists {s}", .{publication_name});
    defer alloc.free(drop_publication_sql);
    const drop_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select pg_drop_replication_slot('{s}') from pg_replication_slots where slot_name = '{s}' and not active",
        .{ slot_name, slot_name },
    );
    defer alloc.free(drop_slot_sql);
    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{table_name});
    defer alloc.free(drop_table_sql);
    defer {
        execCommandForTest(&executor, conn, alloc, drop_publication_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_slot_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_table_sql) catch {};
    }

    const create_table_sql = try std.fmt.allocPrint(alloc, "create table {s} (id text primary key, name text not null)", .{table_name});
    defer alloc.free(create_table_sql);
    const seed_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u0', 'Seed')", .{table_name});
    defer alloc.free(seed_sql);
    const insert_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u1', 'Alice')", .{table_name});
    defer alloc.free(insert_sql);

    try execCommandForTest(&executor, conn, alloc, create_table_sql);
    try execCommandForTest(&executor, conn, alloc, seed_sql);

    var poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .limit = 16,
    };
    defer poll_params.deinit(alloc);

    var empty_poll_result = try executor.pollChangesAlloc(alloc, dsn, poll_params);
    defer empty_poll_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), empty_poll_result.changes.len);

    try execCommandForTest(&executor, conn, alloc, insert_sql);

    var poll_result = try executor.pollChangesAlloc(alloc, dsn, poll_params);
    defer poll_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), poll_result.changes.len);
    try std.testing.expectEqual(foreign_source.ReplicationOp.insert, poll_result.changes[0].op);
    try std.testing.expectEqualStrings("u1", poll_result.changes[0].row.?.object.get("id").?.string);
    try std.testing.expectEqualStrings("Alice", poll_result.changes[0].row.?.object.get("name").?.string);
}

test "postgres libpq live prepare replication returns slot checkpoint" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = try Executor.init(alloc);
    defer executor.deinit();

    const conn = executor.connect(dsn) catch return error.SkipZigTest;
    const wal_level_result = executor.execSimple(conn, alloc, "show wal_level") catch return error.SkipZigTest;
    defer executor.pqclear(wal_level_result);
    if (executor.pqntuples(wal_level_result) == 0 or executor.pqgetisnull(wal_level_result, 0, 0) != 0) {
        return error.SkipZigTest;
    }
    const wal_level = executor.pqgetvalue(wal_level_result, 0, 0)[0..@intCast(executor.pqgetlength(wal_level_result, 0, 0))];
    if (!std.ascii.eqlIgnoreCase(wal_level, "logical")) return error.SkipZigTest;

    live_poll_test_counter += 1;
    const suffix = live_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_prepare_live_{d}", .{suffix});
    defer alloc.free(table_name);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_prepare_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_prepare_pub_{d}", .{suffix});
    defer alloc.free(publication_name);

    const drop_publication_sql = try std.fmt.allocPrint(alloc, "drop publication if exists {s}", .{publication_name});
    defer alloc.free(drop_publication_sql);
    const drop_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select pg_drop_replication_slot('{s}') from pg_replication_slots where slot_name = '{s}' and not active",
        .{ slot_name, slot_name },
    );
    defer alloc.free(drop_slot_sql);
    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{table_name});
    defer alloc.free(drop_table_sql);
    defer {
        execCommandForTest(&executor, conn, alloc, drop_publication_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_slot_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_table_sql) catch {};
    }

    const create_table_sql = try std.fmt.allocPrint(alloc, "create table {s} (id text primary key, name text not null)", .{table_name});
    defer alloc.free(create_table_sql);
    try execCommandForTest(&executor, conn, alloc, create_table_sql);

    var prepare_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
    };
    defer prepare_params.deinit(alloc);

    var prepare_result = try executor.prepareReplicationAlloc(alloc, dsn, prepare_params);
    defer prepare_result.deinit(alloc);

    try std.testing.expect(prepare_result.checkpoint.len > 0);
    try std.testing.expect(!prepare_result.slot_existed);

    var second_prepare = try executor.prepareReplicationAlloc(alloc, dsn, prepare_params);
    defer second_prepare.deinit(alloc);
    try std.testing.expect(second_prepare.checkpoint.len > 0);
    try std.testing.expect(second_prepare.slot_existed);
}

test "postgres libpq live logical poll works after snapshot query on same connection" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = try Executor.init(alloc);
    defer executor.deinit();

    const conn = executor.connect(dsn) catch return error.SkipZigTest;
    const wal_level_result = executor.execSimple(conn, alloc, "show wal_level") catch return error.SkipZigTest;
    defer executor.pqclear(wal_level_result);
    if (executor.pqntuples(wal_level_result) == 0 or executor.pqgetisnull(wal_level_result, 0, 0) != 0) {
        return error.SkipZigTest;
    }
    const wal_level = executor.pqgetvalue(wal_level_result, 0, 0)[0..@intCast(executor.pqgetlength(wal_level_result, 0, 0))];
    if (!std.ascii.eqlIgnoreCase(wal_level, "logical")) return error.SkipZigTest;

    live_poll_test_counter += 1;
    const suffix = live_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_snapshot_then_stream_{d}", .{suffix});
    defer alloc.free(table_name);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_snapshot_then_stream_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_snapshot_then_stream_pub_{d}", .{suffix});
    defer alloc.free(publication_name);

    const drop_publication_sql = try std.fmt.allocPrint(alloc, "drop publication if exists {s}", .{publication_name});
    defer alloc.free(drop_publication_sql);
    const drop_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select pg_drop_replication_slot('{s}') from pg_replication_slots where slot_name = '{s}' and not active",
        .{ slot_name, slot_name },
    );
    defer alloc.free(drop_slot_sql);
    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{table_name});
    defer alloc.free(drop_table_sql);
    defer {
        execCommandForTest(&executor, conn, alloc, drop_publication_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_slot_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_table_sql) catch {};
    }

    const create_table_sql = try std.fmt.allocPrint(alloc, "create table {s} (id text primary key, name text not null)", .{table_name});
    defer alloc.free(create_table_sql);
    const seed_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u0', 'Seed')", .{table_name});
    defer alloc.free(seed_sql);
    const insert_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u1', 'Alice')", .{table_name});
    defer alloc.free(insert_sql);

    try execCommandForTest(&executor, conn, alloc, create_table_sql);
    try execCommandForTest(&executor, conn, alloc, seed_sql);

    const snapshot_query = sql.PreparedQuery{
        .sql_text = try std.fmt.allocPrint(alloc, "SELECT * FROM {s} LIMIT 16 OFFSET 0", .{table_name}),
    };
    var snapshot_query_result = try executor.queryPreparedAlloc(alloc, dsn, snapshot_query);
    defer snapshot_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), snapshot_query_result.rows.len);

    var poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .limit = 16,
    };
    defer poll_params.deinit(alloc);

    var empty_poll_result = try executor.pollChangesAlloc(alloc, dsn, poll_params);
    defer empty_poll_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), empty_poll_result.changes.len);

    try execCommandForTest(&executor, conn, alloc, insert_sql);

    var poll_result = try executor.pollChangesAlloc(alloc, dsn, poll_params);
    defer poll_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), poll_result.changes.len);
    try std.testing.expectEqual(foreign_source.ReplicationOp.insert, poll_result.changes[0].op);
    try std.testing.expectEqualStrings("u1", poll_result.changes[0].row.?.object.get("id").?.string);
    try std.testing.expectEqualStrings("Alice", poll_result.changes[0].row.?.object.get("name").?.string);
}

test "postgres libpq consistent snapshot query holds repeatable read view" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = try Executor.init(alloc);
    defer executor.deinit();

    const conn = executor.connect(dsn) catch return error.SkipZigTest;

    live_poll_test_counter += 1;
    const suffix = live_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_snapshot_probe_{d}", .{suffix});
    defer alloc.free(table_name);

    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{table_name});
    defer alloc.free(drop_table_sql);
    defer execCommandForTest(&executor, conn, alloc, drop_table_sql) catch {};

    const create_table_sql = try std.fmt.allocPrint(alloc, "create table {s} (id text primary key, name text not null)", .{table_name});
    defer alloc.free(create_table_sql);
    const seed_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u0', 'Seed')", .{table_name});
    defer alloc.free(seed_sql);
    const insert_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u1', 'Alice')", .{table_name});
    defer alloc.free(insert_sql);
    const select_sql = try std.fmt.allocPrint(alloc, "SELECT id, name FROM {s} ORDER BY id ASC", .{table_name});
    defer alloc.free(select_sql);

    try execCommandForTest(&executor, conn, alloc, drop_table_sql);
    try execCommandForTest(&executor, conn, alloc, create_table_sql);
    try execCommandForTest(&executor, conn, alloc, seed_sql);

    var snapshot = try executor.asQueryExecutor().beginSnapshotQuery(alloc, dsn);
    defer snapshot.deinit(alloc);

    var first_result = try snapshot.queryPrepared(alloc, .{
        .sql_text = try alloc.dupe(u8, select_sql),
    }, null);
    defer first_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), first_result.rows.len);

    try execCommandForTest(&executor, conn, alloc, insert_sql);

    var second_result = try snapshot.queryPrepared(alloc, .{
        .sql_text = try alloc.dupe(u8, select_sql),
    }, null);
    defer second_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), second_result.rows.len);
    try std.testing.expectEqualStrings("u0", second_result.rows[0].object.get("id").?.string);

    var fresh_result = try executor.queryPreparedAlloc(alloc, dsn, .{
        .sql_text = try alloc.dupe(u8, select_sql),
    });
    defer fresh_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), fresh_result.rows.len);
}

test "postgres libpq prepared replication snapshot bridges initial cutover" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = try Executor.init(alloc);
    defer executor.deinit();

    const conn = executor.connect(dsn) catch return error.SkipZigTest;
    const wal_level_result = executor.execSimple(conn, alloc, "show wal_level") catch return error.SkipZigTest;
    defer executor.pqclear(wal_level_result);
    if (executor.pqntuples(wal_level_result) == 0 or executor.pqgetisnull(wal_level_result, 0, 0) != 0) return error.SkipZigTest;
    const wal_level = executor.pqgetvalue(wal_level_result, 0, 0)[0..@intCast(executor.pqgetlength(wal_level_result, 0, 0))];
    if (!std.ascii.eqlIgnoreCase(wal_level, "logical")) return error.SkipZigTest;

    live_poll_test_counter += 1;
    const suffix = live_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_exact_cutover_{d}", .{suffix});
    defer alloc.free(table_name);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_exact_cutover_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_exact_cutover_pub_{d}", .{suffix});
    defer alloc.free(publication_name);

    const drop_publication_sql = try std.fmt.allocPrint(alloc, "drop publication if exists {s}", .{publication_name});
    defer alloc.free(drop_publication_sql);
    const drop_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select pg_drop_replication_slot('{s}') from pg_replication_slots where slot_name = '{s}' and not active",
        .{ slot_name, slot_name },
    );
    defer alloc.free(drop_slot_sql);
    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{table_name});
    defer alloc.free(drop_table_sql);
    defer {
        execCommandForTest(&executor, conn, alloc, drop_publication_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_slot_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_table_sql) catch {};
    }

    const create_table_sql = try std.fmt.allocPrint(alloc, "create table {s} (id text primary key, name text not null)", .{table_name});
    defer alloc.free(create_table_sql);
    const seed_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u0', 'Seed')", .{table_name});
    defer alloc.free(seed_sql);
    const insert_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name) values ('u1', 'Alice')", .{table_name});
    defer alloc.free(insert_sql);
    try execCommandForTest(&executor, conn, alloc, drop_table_sql);
    try execCommandForTest(&executor, conn, alloc, create_table_sql);
    try execCommandForTest(&executor, conn, alloc, seed_sql);

    var intent_persisted = false;
    const Intent = struct {
        fn persist(
            ptr: *anyopaque,
            _: foreign_source.ExactCutoverIntent.ProviderIdentity,
        ) !void {
            const persisted: *bool = @ptrCast(@alignCast(ptr));
            persisted.* = true;
        }

        fn check(_: *anyopaque) !void {}

        fn retirementComplete(_: *anyopaque) !void {}
    };
    var begin_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .cutover_lock_name = try alloc.dupe(u8, slot_name),
        .exact_cutover_intent = .{
            .ptr = &intent_persisted,
            .persist_fn = Intent.persist,
            .check_fn = Intent.check,
            .retirement_complete_fn = Intent.retirementComplete,
        },
    };
    defer begin_params.deinit(alloc);
    var prepared = try executor.asQueryExecutor().beginPreparedReplicationSnapshot(
        alloc,
        dsn,
        begin_params,
        platform_time.monotonicNs() + 30 * std.time.ns_per_s,
    );
    defer prepared.deinit(alloc);
    try std.testing.expect(intent_persisted);
    try std.testing.expect(prepared.checkpoint.len > 0);

    var snapshot_result = try prepared.snapshot_query.queryPrepared(alloc, .{
        .sql_text = try std.fmt.allocPrint(alloc, "SELECT id, name FROM {s} ORDER BY id ASC", .{table_name}),
    }, null);
    defer snapshot_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), snapshot_result.rows.len);
    try std.testing.expectEqualStrings("u0", snapshot_result.rows[0].object.get("id").?.string);

    try execCommandForTest(&executor, conn, alloc, insert_sql);

    var poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .checkpoint = try alloc.dupe(u8, prepared.checkpoint),
        .limit = 16,
    };
    defer poll_params.deinit(alloc);
    var poll_result = try executor.pollChangesAlloc(alloc, dsn, poll_params);
    defer poll_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), poll_result.changes.len);
    try std.testing.expectEqualStrings("u1", poll_result.changes[0].row.?.object.get("id").?.string);
}

test "postgres libpq exact cutover authority failure precedes provider mutation" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = try Executor.init(alloc);
    defer executor.deinit();

    const conn = executor.connect(dsn) catch return error.SkipZigTest;
    const wal_level_result = executor.execSimple(conn, alloc, "show wal_level") catch return error.SkipZigTest;
    defer executor.pqclear(wal_level_result);
    if (executor.pqntuples(wal_level_result) == 0 or executor.pqgetisnull(wal_level_result, 0, 0) != 0) return error.SkipZigTest;
    const wal_level = executor.pqgetvalue(wal_level_result, 0, 0)[0..@intCast(executor.pqgetlength(wal_level_result, 0, 0))];
    if (!std.ascii.eqlIgnoreCase(wal_level, "logical")) return error.SkipZigTest;

    live_poll_test_counter += 1;
    const suffix = live_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_fenced_cutover_{d}", .{suffix});
    defer alloc.free(table_name);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_fenced_cutover_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_fenced_cutover_pub_{d}", .{suffix});
    defer alloc.free(publication_name);

    const drop_publication_sql = try std.fmt.allocPrint(alloc, "drop publication if exists {s}", .{publication_name});
    defer alloc.free(drop_publication_sql);
    const drop_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select pg_drop_replication_slot('{s}') from pg_replication_slots where slot_name = '{s}' and not active",
        .{ slot_name, slot_name },
    );
    defer alloc.free(drop_slot_sql);
    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{table_name});
    defer alloc.free(drop_table_sql);
    defer {
        execCommandForTest(&executor, conn, alloc, drop_publication_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_slot_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_table_sql) catch {};
    }

    const create_table_sql = try std.fmt.allocPrint(
        alloc,
        "create table {s} (id text primary key)",
        .{table_name},
    );
    defer alloc.free(create_table_sql);
    const create_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select * from pg_create_logical_replication_slot('{s}', 'pgoutput')",
        .{slot_name},
    );
    defer alloc.free(create_slot_sql);
    try execCommandForTest(&executor, conn, alloc, drop_table_sql);
    try execCommandForTest(&executor, conn, alloc, create_table_sql);
    try execCommandForTest(&executor, conn, alloc, create_slot_sql);

    var persist_calls: usize = 0;
    const DeniedIntent = struct {
        fn persist(
            ptr: *anyopaque,
            _: foreign_source.ExactCutoverIntent.ProviderIdentity,
        ) !void {
            const calls: *usize = @ptrCast(@alignCast(ptr));
            calls.* += 1;
            return error.TestAuthorityDenied;
        }

        fn check(_: *anyopaque) !void {}

        fn retirementComplete(_: *anyopaque) !void {}
    };
    var begin_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .cutover_lock_name = try alloc.dupe(u8, slot_name),
        .reclaim_exact_cutover_slot = true,
        .exact_cutover_intent = .{
            .ptr = &persist_calls,
            .persist_fn = DeniedIntent.persist,
            .check_fn = DeniedIntent.check,
            .retirement_complete_fn = DeniedIntent.retirementComplete,
        },
    };
    defer begin_params.deinit(alloc);
    try std.testing.expectError(
        error.TestAuthorityDenied,
        executor.asQueryExecutor().beginPreparedReplicationSnapshot(
            alloc,
            dsn,
            begin_params,
            platform_time.monotonicNs() + 30 * std.time.ns_per_s,
        ),
    );
    try std.testing.expectEqual(@as(usize, 1), persist_calls);
    try std.testing.expect(try executor.logicalReplicationSlotExistsAlloc(
        alloc,
        conn,
        slot_name,
        platform_time.monotonicNs() + 30 * std.time.ns_per_s,
    ));

    const publication_query = try std.fmt.allocPrint(
        alloc,
        "select 1 from pg_publication where pubname = '{s}'",
        .{publication_name},
    );
    defer alloc.free(publication_query);
    const publication_result = try executor.execSimple(conn, alloc, publication_query);
    defer executor.pqclear(publication_result);
    try std.testing.expectEqual(@as(c_int, 0), executor.pqntuples(publication_result));
}

test "postgres libpq poll resumes from durable checkpoint across multiple transactions" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    var executor = try Executor.init(alloc);
    defer executor.deinit();

    const conn = executor.connect(dsn) catch return error.SkipZigTest;
    const wal_level_result = executor.execSimple(conn, alloc, "show wal_level") catch return error.SkipZigTest;
    defer executor.pqclear(wal_level_result);
    if (executor.pqntuples(wal_level_result) == 0 or executor.pqgetisnull(wal_level_result, 0, 0) != 0) return error.SkipZigTest;
    const wal_level = executor.pqgetvalue(wal_level_result, 0, 0)[0..@intCast(executor.pqgetlength(wal_level_result, 0, 0))];
    if (!std.ascii.eqlIgnoreCase(wal_level, "logical")) return error.SkipZigTest;

    live_poll_test_counter += 1;
    const suffix = live_poll_test_counter;
    const table_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_resume_{d}", .{suffix});
    defer alloc.free(table_name);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_resume_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_cdc_resume_pub_{d}", .{suffix});
    defer alloc.free(publication_name);

    const drop_publication_sql = try std.fmt.allocPrint(alloc, "drop publication if exists {s}", .{publication_name});
    defer alloc.free(drop_publication_sql);
    const drop_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select pg_drop_replication_slot('{s}') from pg_replication_slots where slot_name = '{s}' and not active",
        .{ slot_name, slot_name },
    );
    defer alloc.free(drop_slot_sql);
    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{table_name});
    defer alloc.free(drop_table_sql);
    defer {
        execCommandForTest(&executor, conn, alloc, drop_publication_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_slot_sql) catch {};
        execCommandForTest(&executor, conn, alloc, drop_table_sql) catch {};
    }

    const create_table_sql = try std.fmt.allocPrint(alloc, "create table {s} (id text primary key, name text not null, tier text not null)", .{table_name});
    defer alloc.free(create_table_sql);
    const insert_sql = try std.fmt.allocPrint(alloc, "insert into {s} (id, name, tier) values ('u1', 'Alice', 'gold')", .{table_name});
    defer alloc.free(insert_sql);
    const update_sql = try std.fmt.allocPrint(alloc, "update {s} set tier = 'platinum' where id = 'u1'", .{table_name});
    defer alloc.free(update_sql);
    try execCommandForTest(&executor, conn, alloc, drop_table_sql);
    try execCommandForTest(&executor, conn, alloc, create_table_sql);

    var prepare_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .limit = 16,
    };
    defer prepare_params.deinit(alloc);
    var prepare_result = try executor.prepareReplicationAlloc(alloc, dsn, prepare_params);
    defer prepare_result.deinit(alloc);
    try std.testing.expect(prepare_result.checkpoint.len > 0);

    try execCommandForTest(&executor, conn, alloc, insert_sql);

    var first_poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .checkpoint = try alloc.dupe(u8, prepare_result.checkpoint),
        .limit = 16,
    };
    defer first_poll_params.deinit(alloc);
    var first_poll = try executor.pollChangesAlloc(alloc, dsn, first_poll_params);
    defer first_poll.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), first_poll.changes.len);
    try std.testing.expect(first_poll.checkpoint.len > 0);
    try std.testing.expectEqualStrings("u1", first_poll.changes[0].row.?.object.get("id").?.string);
    try std.testing.expectEqualStrings(first_poll.changes[0].checkpoint, first_poll.checkpoint);

    try execCommandForTest(&executor, conn, alloc, update_sql);

    var second_poll_params = foreign_source.ReplicationPollParams{
        .table = try alloc.dupe(u8, table_name),
        .slot_name = try alloc.dupe(u8, slot_name),
        .publication_name = try alloc.dupe(u8, publication_name),
        .checkpoint = try alloc.dupe(u8, first_poll.checkpoint),
        .limit = 16,
    };
    defer second_poll_params.deinit(alloc);
    const deadline = platform_time.monotonicNs() +| 10 * std.time.ns_per_s;
    const replication = try executor.connectReplicationFreshWithDeadline(alloc, dsn, deadline);
    defer executor.pqfinish(replication);
    const identity = try executor.identifyExactCutoverProvider(alloc, conn, replication, deadline);
    var wrong_identity = identity;
    wrong_identity[0] ^= 1;
    const before_rejection = try executor.loadLogicalReplicationSlotCheckpointAlloc(alloc, conn, slot_name, deadline);
    defer alloc.free(before_rejection);
    second_poll_params.expected_provider_identity = wrong_identity;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, executor.pollChangesAlloc(alloc, dsn, second_poll_params));
    const after_rejection = try executor.loadLogicalReplicationSlotCheckpointAlloc(alloc, conn, slot_name, deadline);
    defer alloc.free(after_rejection);
    try std.testing.expectEqualStrings(before_rejection, after_rejection);
    second_poll_params.expected_provider_identity = identity;

    const extra_table = try std.fmt.allocPrint(alloc, "{s}_other", .{table_name});
    defer alloc.free(extra_table);
    const create_extra = try std.fmt.allocPrint(alloc, "CREATE TABLE {s} (LIKE {s} INCLUDING ALL)", .{ extra_table, table_name });
    defer alloc.free(create_extra);
    const drop_extra = try std.fmt.allocPrint(alloc, "DROP TABLE IF EXISTS {s}", .{extra_table});
    defer alloc.free(drop_extra);
    try execCommandForTest(&executor, conn, alloc, create_extra);
    defer execCommandForTest(&executor, conn, alloc, drop_extra) catch {};
    const add_extra = try std.fmt.allocPrint(alloc, "ALTER PUBLICATION {s} ADD TABLE {s}", .{ publication_name, extra_table });
    defer alloc.free(add_extra);
    try execCommandForTest(&executor, conn, alloc, add_extra);
    try std.testing.expectError(error.ReplicationPublicationBindingChanged, executor.pollChangesAlloc(alloc, dsn, second_poll_params));

    const drift_filter = try std.fmt.allocPrint(alloc, "ALTER PUBLICATION {s} SET TABLE {s} WHERE (id <> 'u1')", .{ publication_name, table_name });
    defer alloc.free(drift_filter);
    try execCommandForTest(&executor, conn, alloc, drift_filter);
    {
        var configured = prepare_params;
        configured.filter_query_json = try alloc.dupe(u8, "{\"term\":\"u1\",\"field\":\"id\"}");
        defer alloc.free(configured.filter_query_json.?);
        try std.testing.expectError(error.ReplicationPublicationBindingChanged, executor.prepareReplicationAlloc(alloc, dsn, configured));
        const matching = try std.fmt.allocPrint(alloc, "ALTER PUBLICATION {s} SET TABLE {s} WHERE (id = 'u1')", .{ publication_name, table_name });
        defer alloc.free(matching);
        try execCommandForTest(&executor, conn, alloc, matching);
        var prepared = try executor.prepareReplicationAlloc(alloc, dsn, configured);
        defer prepared.deinit(alloc);
        try std.testing.expect(prepared.slot_existed);
    }
    {
        var filtered = second_poll_params;
        filtered.filter_query_json = try alloc.dupe(u8, "{}");
        defer alloc.free(filtered.filter_query_json.?);
        try std.testing.expectError(error.ReplicationPublicationBindingChanged, executor.pollChangesAlloc(alloc, dsn, filtered));
    }
    const restore_publication = try std.fmt.allocPrint(alloc, "ALTER PUBLICATION {s} SET TABLE {s}", .{ publication_name, table_name });
    defer alloc.free(restore_publication);
    try execCommandForTest(&executor, conn, alloc, restore_publication);
    const after_drift = try executor.loadLogicalReplicationSlotCheckpointAlloc(alloc, conn, slot_name, deadline);
    defer alloc.free(after_drift);
    try std.testing.expectEqualStrings(before_rejection, after_drift);

    var second_poll = try executor.pollChangesAlloc(alloc, dsn, second_poll_params);
    defer second_poll.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), second_poll.changes.len);
    try std.testing.expectEqual(foreign_source.ReplicationOp.update, second_poll.changes[0].op);
    try std.testing.expectEqualStrings("u1", second_poll.changes[0].row.?.object.get("id").?.string);
    try std.testing.expectEqualStrings("platinum", second_poll.changes[0].row.?.object.get("tier").?.string);
    try std.testing.expect(second_poll.checkpoint.len > 0);
    try std.testing.expectEqualStrings(second_poll.changes[0].checkpoint, second_poll.checkpoint);
    first_poll_params.expected_provider_identity = identity;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, executor.pollChangesAlloc(alloc, dsn, first_poll_params));
}

test "postgres libpq module compiles" {
    _ = Executor;
    _ = registerDefaultExecutor;
}

fn testPgDsnAlloc(alloc: Allocator) ![]u8 {
    if (std.c.getenv("ANTFLY_TEST_PG_DSN")) |value_z| {
        return try alloc.dupe(u8, std.mem.span(value_z));
    }
    if (std.c.getenv("PG_DSN")) |value_z| {
        return try alloc.dupe(u8, std.mem.span(value_z));
    }
    return try alloc.dupe(u8, "postgres://localhost:5432/postgres?sslmode=disable");
}

var live_poll_test_counter: u64 = 0;

fn execCommandForTest(executor: *Executor, conn: ?*PGconn, alloc: Allocator, sql_text: []const u8) !void {
    var prepared = sql.PreparedQuery{
        .sql_text = try alloc.dupe(u8, sql_text),
    };
    defer prepared.deinit(alloc);
    const result = try executor.execPreparedAllowCommand(conn, alloc, prepared);
    executor.pqclear(result);
}

test "postgres libpq subscription cleanup verifies provider checkpoint and authority and preserves publications" {
    // This provider integration gate requires an explicit disposable oracle.
    // Its dedicated build target uses --require-no-skips.
    if (std.c.getenv("ANTFLY_TEST_PG_DSN") == null and std.c.getenv("PG_DSN") == null) return error.SkipZigTest;
    const a = std.testing.allocator;
    const dsn = try testPgDsnAlloc(a);
    defer a.free(dsn);
    var executor = try Executor.init(a);
    defer executor.deinit();
    const conn = try executor.connect(dsn);
    const suffix = platform_time.monotonicNs();
    const slot = try std.fmt.allocPrint(a, "antfly_source_cleanup_{d}", .{suffix});
    defer a.free(slot);
    const publication = try std.fmt.allocPrint(a, "antfly_source_pub_{d}", .{suffix});
    defer a.free(publication);
    const provider_lock_name = try std.fmt.allocPrint(a, "antfly/source-cleanup/{d}", .{suffix});
    defer a.free(provider_lock_name);
    const deadline = platform_time.monotonicNs() +| 30 * std.time.ns_per_s;
    const create_pub = try std.fmt.allocPrint(a, "CREATE PUBLICATION {s}", .{publication});
    defer a.free(create_pub);
    const drop_pub = try std.fmt.allocPrint(a, "DROP PUBLICATION IF EXISTS {s}", .{publication});
    defer a.free(drop_pub);
    const create_slot = try std.fmt.allocPrint(a, "SELECT * FROM pg_create_logical_replication_slot('{s}', 'pgoutput')", .{slot});
    defer a.free(create_slot);
    try execCommandForTest(&executor, conn, a, create_pub);
    defer execCommandForTest(&executor, conn, a, drop_pub) catch {};
    try execCommandForTest(&executor, conn, a, create_slot);
    defer executor.dropInactiveLogicalReplicationSlotIfExistsAlloc(a, conn, slot, deadline) catch {};
    try std.testing.expectError(error.ForeignAuthFailed, executor.requireExplicitSubscriptionPassword(a, "host=localhost passfile=/tmp/password"));
    try std.testing.expectError(error.ForeignAuthFailed, executor.requireExplicitSubscriptionPassword(a, "host=localhost password="));
    try executor.requireExplicitSubscriptionPassword(a, "postgresql://account:fixture@localhost/database");
    const authenticated = std.c.getenv("ANTFLY_TEST_PG_REQUIRE_PASSWORD") != null;
    if (authenticated) {
        try executor.requireExplicitSubscriptionPassword(a, dsn);
        try executor.requireSubscriptionPasswordUsed(conn);
    } else if (executor.pqconnectionUsedPassword.?(conn) == 0) {
        try std.testing.expectError(error.ForeignAuthFailed, executor.requireSubscriptionPasswordUsed(conn));
    }
    const provider = try executor.sqlProviderIdentityAlloc(a, conn, deadline);
    const Scope = struct {
        denied: bool = false,
        checks: usize = 0,
        starts: usize = 0,
        refuse_start: bool = false,
        refusals: usize = 0,
        observations: usize = 0,
        fn refused(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refusals += 1;
        }
        fn start(raw: *anyopaque, identity: [32]u8) !void {
            try std.testing.expect(!std.mem.allEqual(u8, &identity, 0));
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.starts += 1;
            if (self.refuse_start) return error.ReplicationCutoverAuthorityLost;
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.denied) return error.ReplicationCutoverAuthorityLost;
        }
        fn observe(raw: *anyopaque, _: foreign_source.SubscriptionOrigin.Observation) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observations += 1;
            self.denied = true;
            return error.ReplicationCutoverAuthorityLost;
        }
        fn acknowledged(_: *anyopaque, _: []const u8) !void {
            return error.TestUnexpectedResult;
        }
    };
    var scope: Scope = .{};
    {
        // A disconnected worker's SQL lock can be gone while the backend
        // running CREATE is still alive. Cleanup must honor that backend.
        const replication = try executor.connectReplicationFreshWithDeadline(a, dsn, deadline);
        defer executor.pqfinish(replication);
        try executor.lockSubscriptionSlotCreation(a, replication, slot, deadline);
        var blocked: foreign_source.ReplicationCleanupParams = .{
            .slot_name = slot,
            .native_source = .{ .provider_identity = provider, .snapshot_checkpoint = 1, .durable_checkpoint = 1, .lock_name = provider_lock_name, .authority = .{ .ptr = &scope, .check_fn = Scope.check, .observe_fn = Scope.observe, .acknowledged_fn = Scope.acknowledged } },
            .password_required = authenticated,
        };
        blocked.execution_deadline_ns = platform_time.monotonicNs() +| 250 * std.time.ns_per_ms;
        if (executor.asQueryExecutor().cleanupReplication(a, dsn, blocked)) |_| {
            return error.TestUnexpectedResult;
        } else |err| switch (err) {
            error.Timeout, error.DeadlineExceeded => {},
            else => return err,
        }
        try std.testing.expect(try executor.logicalReplicationSlotExistsAlloc(a, conn, slot, deadline));
    }
    const checkpoint = try executor.requireSubscriptionSlotAlloc(a, conn, slot, deadline);
    var params: foreign_source.ReplicationCleanupParams = .{
        .slot_name = slot,
        .native_source = .{ .provider_identity = provider, .snapshot_checkpoint = checkpoint, .durable_checkpoint = checkpoint, .lock_name = provider_lock_name, .authority = .{ .ptr = &scope, .check_fn = Scope.check, .observe_fn = Scope.observe, .acknowledged_fn = Scope.acknowledged } },
        .execution_deadline_ns = deadline,
        .password_required = authenticated,
    };
    // A native preparation is not evidence that it created a slot. Refusing
    // an existing foreign slot must precede the callback that binds ownership;
    // otherwise later recovery could classify that foreign slot as ours.
    var capture_authority = params.native_source.?.authority;
    capture_authority.start_fn = Scope.start;
    capture_authority.refused_fn = Scope.refused;
    {
        // An invalid slot identifier is refused by PostgreSQL after send.
        const observer = try executor.connectFreshWithDeadline(dsn, deadline, null);
        defer executor.pqfinish(observer);
        try executor.setCutoverAdvisoryLockAlloc(a, observer, provider_lock_name, true, deadline);
        const replication = try executor.connectReplicationFreshWithDeadline(a, dsn, deadline);
        defer executor.pqfinish(replication);
        try std.testing.expectError(error.ForeignQueryFailed, executor.createLogicalReplicationSlotExportResult(a, replication, "InvalidSlot", deadline, .{
            .authority = capture_authority,
            .locked_observer = observer,
            .provider_identity = provider,
        }));
        try std.testing.expectEqual(@as(usize, 1), scope.refusals);
        scope.starts = 0;
    }
    try std.testing.expectError(error.UnsupportedExactCutover, Executor.beginSubscriptionSnapshot(&executor, a, dsn, .{
        .publications = &.{publication},
        .slot_name = slot,
        .lock_name = provider_lock_name,
        .expected = null,
        .authority = capture_authority,
        .execution_deadline_ns = deadline,
        .password_required = authenticated,
    }));
    try std.testing.expectEqual(@as(usize, 0), scope.observations);
    try std.testing.expectEqual(@as(usize, 0), scope.starts);
    try std.testing.expect(try executor.logicalReplicationSlotExistsAlloc(a, conn, slot, deadline));
    // Endpoint/credential changes cannot retarget cleanup to a different
    // physical PostgreSQL database, even if its slot names happen to match.
    const aborted = try std.fmt.allocPrint(a, "antfly_aborted_export_{d}", .{suffix});
    defer a.free(aborted);
    const capture_request: foreign_source.SubscriptionRequest = .{
        .publications = &.{publication},
        .slot_name = aborted,
        .lock_name = provider_lock_name,
        .expected = null,
        .authority = capture_authority,
        .execution_deadline_ns = deadline,
        .password_required = authenticated,
    };
    scope.refuse_start = true;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, Executor.beginSubscriptionSnapshot(&executor, a, dsn, capture_request));
    try std.testing.expectEqual(@as(usize, 1), scope.starts);
    try std.testing.expectEqual(@as(usize, 0), scope.observations);
    try std.testing.expect(!try executor.logicalReplicationSlotExistsAlloc(a, conn, aborted, deadline));
    scope.refuse_start = false;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, Executor.beginSubscriptionSnapshot(&executor, a, dsn, capture_request));
    try std.testing.expectEqual(@as(usize, 2), scope.starts);
    try std.testing.expectEqual(@as(usize, 1), scope.observations);
    try std.testing.expect(!try executor.logicalReplicationSlotExistsAlloc(a, conn, aborted, deadline));
    scope.denied = false;
    var absence = params;
    absence.native_source.?.snapshot_checkpoint = 0;
    absence.native_source.?.durable_checkpoint = 0;
    absence.native_source.?.require_absent = true;
    try std.testing.expectError(error.ExactCutoverCleanupPending, executor.asQueryExecutor().cleanupReplication(a, dsn, absence));
    try std.testing.expect(try executor.logicalReplicationSlotExistsAlloc(a, conn, slot, deadline));
    absence.slot_name = aborted;
    try executor.asQueryExecutor().cleanupReplication(a, dsn, absence);
    absence.native_source.?.provider_identity[0] ^= 1;
    try std.testing.expectError(error.ForeignProviderIdentityMismatch, executor.asQueryExecutor().cleanupReplication(a, dsn, absence));
    params.native_source.?.provider_identity[0] ^= 1;
    try std.testing.expectError(error.ForeignProviderIdentityMismatch, executor.asQueryExecutor().cleanupReplication(a, dsn, params));
    try std.testing.expect(try executor.logicalReplicationSlotExistsAlloc(a, conn, slot, deadline));
    params.native_source.?.provider_identity = provider;
    scope.denied = true;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, executor.asQueryExecutor().cleanupReplication(a, dsn, params));
    try std.testing.expect(try executor.logicalReplicationSlotExistsAlloc(a, conn, slot, deadline));
    scope.denied = false;
    params.native_source.?.snapshot_checkpoint = checkpoint + 1;
    params.native_source.?.durable_checkpoint = checkpoint + 1;
    try std.testing.expectError(error.ForeignProviderIdentityMismatch, executor.asQueryExecutor().cleanupReplication(a, dsn, params));
    try std.testing.expect(try executor.logicalReplicationSlotExistsAlloc(a, conn, slot, deadline));
    params.native_source.?.snapshot_checkpoint = checkpoint;
    params.native_source.?.durable_checkpoint = checkpoint;
    try executor.asQueryExecutor().cleanupReplication(a, dsn, params);
    try std.testing.expect(!try executor.logicalReplicationSlotExistsAlloc(a, conn, slot, deadline));
    // A repeated actual provider observation is idempotent. No provider ACK,
    // Source checkpoint advance or publication mutation is manufactured.
    try executor.asQueryExecutor().cleanupReplication(a, dsn, params);
    var query = try tableNamePreparedQueryAlloc(a, "SELECT count(*)::text FROM pg_publication WHERE pubname=$1", publication);
    defer query.deinit(a);
    const result = try executor.execPreparedWithDeadline(conn, a, query, deadline, null);
    defer executor.pqclear(result);
    try std.testing.expectEqual(@as(c_int, 1), executor.pqntuples(result));
    try std.testing.expectEqualStrings("1", executor.pqgetvalue(result, 0, 0)[0..@intCast(executor.pqgetlength(result, 0, 0))]);
    // The shared existing path still removes an explicitly owned publication.
    try execCommandForTest(&executor, conn, a, create_slot);
    params.native_source = null;
    params.publication_name = publication;
    try executor.asQueryExecutor().cleanupReplication(a, dsn, params);
    const missing = try executor.execPreparedWithDeadline(conn, a, query, deadline, null);
    defer executor.pqclear(missing);
    try std.testing.expectEqualStrings("0", executor.pqgetvalue(missing, 0, 0)[0..@intCast(executor.pqgetlength(missing, 0, 0))]);
}

test "postgres pgoutput transaction envelopes discard incomplete suffix and retain exact commit evidence" {
    const a = std.testing.allocator;
    var decoded: PgoutputTransactions = .{};
    defer decoded.deinit(a);
    try decoded.accept(a, .{ .begin = .{ .xid = 9, .final_lsn = 16, .timestamp_ms = 7 } }, "0/01");
    for (0..2) |_| try decoded.accept(a, .{ .change = .{ .op = .insert, .checkpoint = try a.alloc(u8, 0) } }, "0/02");
    try decoded.accept(a, .{ .commit = .{ .commit_lsn = 16, .end_lsn = 24, .timestamp_ms = 7 } }, "0/18");
    try decoded.accept(a, .{ .begin = .{ .xid = 10, .final_lsn = 32, .timestamp_ms = 8 } }, "0/20");
    try decoded.accept(a, .{ .change = .{ .op = .delete, .checkpoint = try a.alloc(u8, 0) } }, "0/21");
    var result = try decoded.finish(a, 0);
    defer result.deinit(a);
    try result.validateTransactions();
    try std.testing.expectEqual(@as(usize, 2), result.changes.len);
    try std.testing.expectEqual(@as(usize, 1), result.transactions.?.len);
    try std.testing.expectEqual(@as(u32, 9), result.transactions.?[0].xid);
    try std.testing.expectEqual(@as(u64, 24), result.transactions.?[0].end_lsn);
    try std.testing.expectEqualStrings("0/18", result.checkpoint);
    try std.testing.expectEqualStrings("0/18", result.changes[0].checkpoint);
    try std.testing.expectEqualStrings("0/18", result.changes[1].checkpoint);
}

test "postgres pgoutput transaction envelopes reject missing begin and conflicting commit before handoff" {
    const a = std.testing.allocator;
    var decoded: PgoutputTransactions = .{};
    defer decoded.deinit(a);
    try std.testing.expectError(error.InvalidReplicationTransaction, decoded.accept(a, .{ .change = .{ .op = .insert, .checkpoint = try a.alloc(u8, 0) } }, "0/02"));
    try decoded.accept(a, .{ .begin = .{ .xid = 9, .final_lsn = 16, .timestamp_ms = 7 } }, "0/01");
    try std.testing.expectError(error.InvalidReplicationTransaction, decoded.accept(a, .{ .begin = .{ .xid = 10, .final_lsn = 32, .timestamp_ms = 8 } }, "0/02"));
    try std.testing.expectError(error.InvalidReplicationTransaction, decoded.accept(a, .{ .commit = .{ .commit_lsn = 15, .end_lsn = 24, .timestamp_ms = 7 } }, "0/18"));
    var relation_cache = std.AutoHashMapUnmanaged(u32, PgoutputRelation).empty;
    defer deinitPgoutputRelationCache(a, &relation_cache);
    try std.testing.expectError(error.UnsupportedReplicationTruncate, parsePgoutputMessageAlloc(a, "T", &relation_cache));
    try std.testing.expectError(error.UnsupportedReplicationStreaming, parsePgoutputMessageAlloc(a, "S", &relation_cache));
}

test "postgres pgoutput transaction envelope ownership survives every allocation failure" {
    const Campaign = struct {
        fn run(a: Allocator) !void {
            var decoded: PgoutputTransactions = .{};
            defer decoded.deinit(a);
            try decoded.accept(a, .{ .begin = .{ .xid = 9, .final_lsn = 16, .timestamp_ms = 7 } }, "0/01");
            for (0..3) |_| try decoded.accept(a, .{ .change = .{ .op = .insert, .checkpoint = try a.alloc(u8, 0) } }, "0/02");
            try decoded.accept(a, .{ .commit = .{ .commit_lsn = 16, .end_lsn = 24, .timestamp_ms = 7 } }, "0/18");
            var result = try decoded.finish(a, 0);
            defer result.deinit(a);
            try result.validateTransactions();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "postgres lazy registry retains native transaction and snapshot identity capabilities" {
    var lazy: LazyExecutor = .{ .alloc = std.testing.allocator };
    defer lazy.deinit();
    const executor = lazy.asQueryExecutor();
    try std.testing.expect(executor.vtable.transaction_envelopes);
    try std.testing.expect(executor.vtable.begin_replication_snapshot_query != null);
    var runtime: postgres_source.RuntimeSource = .{ .alloc = std.testing.allocator, .executor = executor, .dsn = try std.testing.allocator.dupe(u8, "host=unused dbname=unused") };
    defer runtime.deinit();
    const source = runtime.asSource();
    try std.testing.expect(source.vtable.transaction_envelopes);
    try std.testing.expect(source.vtable.begin_replication_snapshot_query != null);
    try std.testing.expect(lazy.executor == null);
}

test "postgres replication wire chunks reject reordered missing duplicated and oversized source bytes" {
    const size = replication_wire_chunk_bytes;
    var frame: ReplicationChunkFraming = .{};
    try frame.accept(1, size * 2 + 7, 0, size);
    try std.testing.expect(frame.in_message);
    try std.testing.expectError(error.InvalidReplicationSourceRow, frame.accept(1, size * 2 + 7, size + 1, size));
    try std.testing.expectError(error.InvalidReplicationSourceRow, frame.accept(1, size * 2 + 8, size, size));
    try std.testing.expectError(error.InvalidReplicationSourceRow, frame.accept(2, size * 2 + 7, size, size));
    try frame.accept(1, size * 2 + 7, size, size);
    try std.testing.expectError(error.InvalidReplicationSourceRow, frame.accept(1, size * 2 + 7, size, size));
    try std.testing.expect(frame.in_message);
    try frame.accept(1, size * 2 + 7, size * 2, 7);
    try std.testing.expect(!frame.in_message);
    try std.testing.expectEqual(@as(u64, 2), frame.next_ordinal);
    try std.testing.expectError(error.InvalidReplicationSourceRow, frame.accept(2, size + 1, 0, size + 1));
    try std.testing.expectError(error.InvalidReplicationSourceRow, frame.accept(2, size + 1, 0, size - 1));
    frame = .{};
    try std.testing.expectError(error.InvalidReplicationSourceRow, frame.accept(1, 0, 0, 0));
    try std.testing.expectError(error.InvalidReplicationSourceRow, frame.accept(1, 2, 1, 1));
}

const SnapshotNullFixture = struct {
    fn rows(_: ?*PGresult) callconv(.c) c_int {
        return 1;
    }
    fn columns(_: ?*PGresult) callconv(.c) c_int {
        return 3;
    }
    fn name(_: ?*PGresult, column: c_int) callconv(.c) ?[*:0]const u8 {
        return switch (column) {
            0 => "sql_null",
            1 => "json_null",
            else => "text_null",
        };
    }
    fn oid(_: ?*PGresult, column: c_int) callconv(.c) Oid {
        return if (column == 2) TypeOid.text else TypeOid.jsonb;
    }
    fn isNull(_: ?*PGresult, _: c_int, column: c_int) callconv(.c) c_int {
        return if (column == 0) 1 else 0;
    }
    fn length(_: ?*PGresult, _: c_int, _: c_int) callconv(.c) c_int {
        return 4;
    }
    fn value(_: ?*PGresult, _: c_int, _: c_int) callconv(.c) [*]const u8 {
        return "null";
    }
    fn check(a: Allocator) !void {
        var executor = Executor.initForPermitTests(a);
        defer executor.deinit();
        executor.pqntuples = rows;
        executor.pqnfields = columns;
        executor.pqfname = name;
        executor.pqftype = oid;
        executor.pqgetisnull = isNull;
        executor.pqgetlength = length;
        executor.pqgetvalue = value;
        var replication = try executor.readQueryResultWithProvenance(a, null, null, null, true);
        defer replication.deinit(a);
        try std.testing.expectEqual(@as(usize, 1), replication.json_null_fields.len);
        try std.testing.expectEqual(@as(usize, 1), replication.json_null_fields[0].len);
        try std.testing.expectEqualStrings("json_null", replication.json_null_fields[0][0]);
        try std.testing.expect(replication.rows[0].object.get("sql_null").? == .null);
        try std.testing.expect(replication.rows[0].object.get("json_null").? == .null);
        try std.testing.expectEqualStrings("null", replication.rows[0].object.get("text_null").?.string);
        var ordinary = try executor.readQueryResultAllocWithDeadline(a, null, null, null);
        defer ordinary.deinit(a);
        try std.testing.expectEqual(@as(usize, 0), ordinary.json_null_fields.len);
    }
};
test "postgres replication snapshot retains SQL null versus JSON null without changing ordinary queries" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, SnapshotNullFixture.check, .{});
}

/// Publication membership must expose exactly the configured source identity.
/// A leaf published via an ancestor is not that identity, either. This check
/// runs before slot peek/advance and cannot acknowledge a filtered-out prefix.
fn requirePublicationSourceIdentity(via_root: []const u8, relation_kind: []const u8, member: []const u8) !void {
    if (!std.mem.eql(u8, member, "t")) return error.InvalidReplicationSourceConfig;
    if (std.mem.eql(u8, relation_kind, "p") and !std.mem.eql(u8, via_root, "t")) return error.InvalidReplicationSourceConfig;
    if (!std.mem.eql(u8, relation_kind, "r") and !std.mem.eql(u8, relation_kind, "p")) return error.InvalidReplicationSourceConfig;
}

test "replication publication rejects partition identity loss before checkpoint advancement" {
    try requirePublicationSourceIdentity("t", "p", "t");
    try requirePublicationSourceIdentity("f", "r", "t");
    try std.testing.expectError(error.InvalidReplicationSourceConfig, requirePublicationSourceIdentity("f", "p", "t"));
    try std.testing.expectError(error.InvalidReplicationSourceConfig, requirePublicationSourceIdentity("t", "r", "f"));
    try std.testing.expectError(error.InvalidReplicationSourceConfig, requirePublicationSourceIdentity("t", "v", "t"));
}

test "postgres libpq retained subscription snapshot renews quantum cutoff and cancels actual FETCH" {
    if (std.c.getenv("ANTFLY_TEST_PG_DSN") == null and std.c.getenv("PG_DSN") == null) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const dsn = try testPgDsnAlloc(a);
    var executor = try Executor.init(a);
    defer executor.deinit();
    const conn = try executor.connect(dsn);
    const suffix = platform_time.monotonicNs();
    const table = try std.fmt.allocPrint(a, "antfly_quantum_rows_{d}", .{suffix});
    const role = try std.fmt.allocPrint(a, "antfly_quantum_role_{d}", .{suffix});
    const function = try std.fmt.allocPrint(a, "antfly_quantum_wait_{d}", .{suffix});
    const pub1 = try std.fmt.allocPrint(a, "antfly_quantum_a_{d}", .{suffix});
    const pub2 = try std.fmt.allocPrint(a, "antfly_quantum_b_{d}", .{suffix});
    const slot = try std.fmt.allocPrint(a, "antfly_quantum_slot_{d}", .{suffix});
    const provider_lock = try std.fmt.allocPrint(a, "antfly/quantum/{d}", .{suffix});
    const wait_key = suffix & @as(u64, std.math.maxInt(i64));
    try execCommandForTest(&executor, conn, a, try std.fmt.allocPrint(a, "CREATE ROLE {s} NOLOGIN", .{role}));
    defer {
        execCommandForTest(&executor, conn, a, std.fmt.allocPrint(a, "DROP OWNED BY {s}", .{role}) catch unreachable) catch {};
        execCommandForTest(&executor, conn, a, std.fmt.allocPrint(a, "DROP ROLE {s}", .{role}) catch unreachable) catch {};
    }
    try execCommandForTest(&executor, conn, a, try std.fmt.allocPrint(a, "CREATE TABLE {s}(id bigint PRIMARY KEY, value text)", .{table}));
    defer execCommandForTest(&executor, conn, a, std.fmt.allocPrint(a, "DROP TABLE IF EXISTS {s} CASCADE", .{table}) catch unreachable) catch {};
    try execCommandForTest(&executor, conn, a, try std.fmt.allocPrint(a, "INSERT INTO {s} VALUES (1,'first'),(2,'second'),(3,'blocked')", .{table}));
    try execCommandForTest(&executor, conn, a, try std.fmt.allocPrint(a, "CREATE FUNCTION {s}(wanted bigint) RETURNS bool LANGUAGE plpgsql VOLATILE AS $q$ BEGIN IF wanted=3 THEN PERFORM pg_catalog.pg_advisory_xact_lock({d}); END IF; RETURN true; END $q$", .{ function, wait_key }));
    defer execCommandForTest(&executor, conn, a, std.fmt.allocPrint(a, "DROP FUNCTION IF EXISTS {s}(bigint) CASCADE", .{function}) catch unreachable) catch {};
    try execCommandForTest(&executor, conn, a, try std.fmt.allocPrint(a, "ALTER TABLE {s} ENABLE ROW LEVEL SECURITY", .{table}));
    try execCommandForTest(&executor, conn, a, try std.fmt.allocPrint(a, "CREATE POLICY quantum_read ON {s} FOR SELECT USING ({s}(id))", .{ table, function }));
    try execCommandForTest(&executor, conn, a, try std.fmt.allocPrint(a, "GRANT SELECT ON {s} TO {s}", .{ table, role }));
    try execCommandForTest(&executor, conn, a, try std.fmt.allocPrint(a, "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO {s}", .{role}));
    for ([_][]const u8{ pub1, pub2 }) |publication| try execCommandForTest(&executor, conn, a, try std.fmt.allocPrint(a, "CREATE PUBLICATION {s} FOR TABLE {s}", .{ publication, table }));
    defer for ([_][]const u8{ pub1, pub2 }) |publication| execCommandForTest(&executor, conn, a, std.fmt.allocPrint(a, "DROP PUBLICATION IF EXISTS {s}", .{publication}) catch unreachable) catch {};
    const Scope = struct {
        checks: usize = 0,
        expired: bool = false,
        watch: ?struct { executor: *Executor, conn: ?*PGconn, a: Allocator, query: []const u8 } = null,
        observed_fetch: bool = false,
        observed: ?[32]u8 = null,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.expired) return error.DeadlineExceeded;
            if (self.watch) |watch| {
                const result = try watch.executor.execSimpleWithDeadline(watch.conn, watch.a, watch.query, platform_time.monotonicNs() +| std.time.ns_per_s, false);
                defer watch.executor.pqclear(result);
                if (watch.executor.pqntuples(result) == 1) {
                    self.observed_fetch = true;
                    return error.Canceled;
                }
            }
        }
        fn checkToken(raw: *const anyopaque) !void {
            return check(@constCast(raw));
        }
        fn observe(raw: *anyopaque, origin: foreign_source.SubscriptionOrigin.Observation) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (origin.relations.len != 1) return error.TestUnexpectedResult;
            if (self.observed) |prior| if (!std.mem.eql(u8, &prior, &origin.provider_identity)) return error.ForeignProviderIdentityMismatch;
            self.observed = origin.provider_identity;
        }
        fn acknowledged(_: *anyopaque, _: []const u8) !void {
            return error.TestUnexpectedResult;
        }
        fn authority(self: *@This()) foreign_source.SubscriptionAuthority {
            return .{ .ptr = self, .check_fn = check, .observe_fn = observe, .acknowledged_fn = acknowledged };
        }
        fn token(self: *@This()) CancellationToken {
            return .{ .ptr = self, .check_fn = checkToken };
        }
    };
    var initial: Scope = .{};
    const old_cutoff = platform_time.monotonicNs() +| 3 * std.time.ns_per_s;
    var snapshot = try executor.asQueryExecutor().beginSubscriptionSnapshot(a, dsn, .{ .publications = &.{ pub1, pub2 }, .slot_name = slot, .lock_name = provider_lock, .expected = null, .authority = initial.authority(), .execution_deadline_ns = old_cutoff });
    defer executor.dropInactiveLogicalReplicationSlotIfExistsAlloc(a, conn, slot, platform_time.monotonicNs() +| 30 * std.time.ns_per_s) catch {};
    defer snapshot.deinit(a);
    const reader: *Executor.SnapshotQuery = @ptrCast(@alignCast(snapshot.snapshot_query.ptr));
    const original_connection = reader.conn;
    const original_token = try a.dupe(u8, snapshot.snapshot_token);
    const as_reader = try executor.execSimpleWithDeadline(reader.conn, a, try std.fmt.allocPrint(a, "SET ROLE {s}", .{role}), old_cutoff, true);
    executor.pqclear(as_reader);
    var first = try snapshot.captureRelation(a, snapshot.origin.relations[0].id, 1, old_cutoff, initial.token());
    defer first.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), first.rows.len);
    const pid_result = try executor.execSimpleWithDeadline(reader.conn, a, "SELECT pg_catalog.pg_backend_pid()::text", old_cutoff, false);
    defer executor.pqclear(pid_result);
    const backend_pid = try std.fmt.parseInt(u32, executor.pqgetvalue(pid_result, 0, 0)[0..@intCast(executor.pqgetlength(pid_result, 0, 0))], 10);
    const old_checks = initial.checks;
    platform_time.sleepNs((old_cutoff -| platform_time.monotonicNs()) +| std.time.ns_per_ms);
    initial.expired = true;
    var current: Scope = .{};
    const fresh_cutoff = platform_time.monotonicNs() +| 30 * std.time.ns_per_s;
    var second = try snapshot.captureRelation(a, snapshot.origin.relations[0].id, 1, fresh_cutoff, current.token());
    defer second.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), second.rows.len);
    try std.testing.expectEqual(old_checks, initial.checks);
    try std.testing.expectEqual(original_connection, reader.conn);
    try std.testing.expectEqualStrings(original_token, snapshot.snapshot_token);
    // The real third FETCH blocks in the selected role's RLS policy. Cancel
    // through this quantum's checkpoint while libpq is waiting on its socket.
    try execCommandForTest(&executor, conn, a, try std.fmt.allocPrint(a, "SELECT pg_catalog.pg_advisory_lock({d})", .{wait_key}));
    defer execCommandForTest(&executor, conn, a, std.fmt.allocPrint(a, "SELECT pg_catalog.pg_advisory_unlock({d})", .{wait_key}) catch unreachable) catch {};
    current.watch = .{ .executor = &executor, .conn = conn, .a = a, .query = try std.fmt.allocPrint(a, "SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid={d} AND wait_event_type='Lock' AND wait_event='advisory' AND query LIKE 'FETCH FORWARD%antfly_replication_capture%'", .{backend_pid}) };
    try std.testing.expectError(error.Cancelled, snapshot.captureRelation(a, snapshot.origin.relations[0].id, 1, fresh_cutoff, current.token()));
    try std.testing.expect(current.observed_fetch);
    try std.testing.expect(reader.conn == null);
    try std.testing.expectEqualStrings(original_token, snapshot.snapshot_token);
}
test "postgres libpq subscription continuity preserves whole transactions replay and ACK fences" {
    if (std.c.getenv("ANTFLY_TEST_PG_DSN") == null and std.c.getenv("PG_DSN") == null) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const dsn = try testPgDsnAlloc(a);
    var executor = try Executor.init(a);
    defer executor.deinit();
    const conn = try executor.connect(dsn);
    const suffix = platform_time.monotonicNs();
    const table = try std.fmt.allocPrint(a, "antfly_stream_rows_{d}", .{suffix});
    const publication = try std.fmt.allocPrint(a, "antfly_stream_pub_{d}", .{suffix});
    const slot = try std.fmt.allocPrint(a, "antfly_stream_slot_{d}", .{suffix});
    const deadline = platform_time.monotonicNs() +| 30 * std.time.ns_per_s;
    try execCommandForTest(&executor, conn, a, try std.fmt.allocPrint(a, "CREATE TABLE {s}(id numeric, weight real, value text, PRIMARY KEY (id, weight))", .{table}));
    defer execCommandForTest(&executor, conn, a, std.fmt.allocPrint(a, "DROP TABLE IF EXISTS {s} CASCADE", .{table}) catch unreachable) catch {};
    try execCommandForTest(&executor, conn, a, try std.fmt.allocPrint(a, "CREATE PUBLICATION {s} FOR TABLE {s}", .{ publication, table }));
    defer execCommandForTest(&executor, conn, a, std.fmt.allocPrint(a, "DROP PUBLICATION IF EXISTS {s}", .{publication}) catch unreachable) catch {};
    try execCommandForTest(&executor, conn, a, try std.fmt.allocPrint(a, "SELECT * FROM pg_create_logical_replication_slot('{s}', 'pgoutput')", .{slot}));
    defer executor.dropInactiveLogicalReplicationSlotIfExistsAlloc(a, conn, slot, platform_time.monotonicNs() +| 30 * std.time.ns_per_s) catch {};
    // Controlled authority qualifies the actual provider boundary only. Native
    // receipt custody has separate DocStore tests; this is not a physical ACK.
    const Authority = struct {
        allowed: bool = false,
        checkpoint: []const u8 = "",
        checks: usize = 0,
        acknowledgements: usize = 0,
        deny_at: ?usize = null,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
        }
        fn observe(_: *anyopaque, _: foreign_source.SubscriptionOrigin.Observation) !void {}
        fn acknowledged(raw: *anyopaque, checkpoint: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.acknowledgements += 1;
            if (!self.allowed or !std.mem.eql(u8, checkpoint, self.checkpoint) or (self.deny_at != null and self.acknowledgements == self.deny_at.?)) return error.ReplicationCutoverAuthorityLost;
        }
        fn value(self: *@This()) foreign_source.SubscriptionAuthority {
            return .{ .ptr = self, .check_fn = check, .observe_fn = observe, .acknowledged_fn = acknowledged };
        }
    };
    var authority: Authority = .{};
    const initial = try executor.loadLogicalReplicationSlotCheckpointAlloc(a, conn, slot, deadline);
    var request: foreign_source.SubscriptionRequest = .{ .publications = &.{publication}, .slot_name = slot, .lock_name = try std.fmt.allocPrint(a, "antfly/stream/{s}", .{slot}), .expected = null, .authority = authority.value(), .checkpoint = initial, .limit = 1, .password_required = std.c.getenv("ANTFLY_TEST_PG_REQUIRE_PASSWORD") != null, .execution_deadline_ns = deadline };
    var origin = try executor.subscriptionOriginAlloc(a, conn, request);
    defer origin.deinit(a);
    request.expected = origin;
    var empty = try executor.asQueryExecutor().pollSubscription(a, dsn, request);
    defer empty.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), empty.poll.transactions.?.len);
    try std.testing.expectEqual(origin.provider_identity, empty.poll.provider_identity.?);
    try std.testing.expect(empty.poll.source_relation_id == null);
    // upto_nchanges is a soft bound: all three changes of this one real COMMIT
    // must survive a limit of one without exposing an uncommitted prefix.
    try execCommandForTest(&executor, conn, a, try std.fmt.allocPrint(a, "INSERT INTO {s} VALUES (1,0,'a'),(2,0.1,'b'),(3,0.2,'c')", .{table}));
    var first = try executor.asQueryExecutor().pollSubscription(a, dsn, request);
    defer first.deinit(a);
    try first.poll.validateTransactions();
    try std.testing.expectEqual(origin.provider_identity, first.poll.provider_identity.?);
    try std.testing.expect(first.poll.source_relation_id == null);
    try std.testing.expectEqual(@as(usize, 1), first.poll.transactions.?.len);
    try std.testing.expectEqual(@as(usize, 3), first.poll.changes.len);
    const transaction = first.poll.transactions.?[0];
    try std.testing.expectEqual(@as(usize, 3), transaction.change_count);
    var replay = try executor.asQueryExecutor().pollSubscription(a, dsn, request);
    defer replay.deinit(a);
    try std.testing.expectEqual(transaction.xid, replay.poll.transactions.?[0].xid);
    try std.testing.expectEqual(transaction.end_lsn, replay.poll.transactions.?[0].end_lsn);
    try std.testing.expectEqual(@as(usize, 3), replay.poll.changes.len);
    const wal = @import("../api/replication_subscription_rows.zig");
    const durable = try foreign_source.parseReplicationCheckpoint(initial);
    const original_work = (try wal.nextWalTransaction(first.poll, durable)).?;
    const replayed_work = (try wal.nextWalTransaction(replay.poll, durable)).?;
    try std.testing.expectEqual(original_work.event, replayed_work.event);
    try std.testing.expectEqual(@as(usize, 3), replayed_work.changes.len);
    try std.testing.expect((try wal.nextWalTransaction(replay.poll, transaction.end_lsn)) == null);

    try std.testing.expectEqualStrings(initial, try executor.loadLogicalReplicationSlotCheckpointAlloc(a, conn, slot, deadline));
    request.checkpoint = transaction.checkpoint;
    authority.checkpoint = transaction.checkpoint;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, executor.asQueryExecutor().acknowledgeSubscription(a, dsn, request));
    try std.testing.expectEqualStrings(initial, try executor.loadLogicalReplicationSlotCheckpointAlloc(a, conn, slot, deadline));
    authority.allowed = true;
    authority.acknowledgements = 0;
    authority.deny_at = 2;
    // Revocation after acquiring the real slot lock must still precede the
    // upstream effect. The second independent receipt check is mandatory.
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, executor.asQueryExecutor().acknowledgeSubscription(a, dsn, request));
    try std.testing.expectEqual(@as(usize, 2), authority.acknowledgements);
    try std.testing.expectEqualStrings(initial, try executor.loadLogicalReplicationSlotCheckpointAlloc(a, conn, slot, deadline));
    request.advance_durable_checkpoint = true;
    authority.acknowledgements = 0;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, executor.asQueryExecutor().pollSubscription(a, dsn, request));
    try std.testing.expectEqualStrings(initial, try executor.loadLogicalReplicationSlotCheckpointAlloc(a, conn, slot, deadline));
    authority.acknowledgements = 0;
    authority.deny_at = null;
    var caught_up_inline = try executor.asQueryExecutor().pollSubscription(a, dsn, request);
    defer caught_up_inline.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), caught_up_inline.poll.transactions.?.len);
    request.advance_durable_checkpoint = false;
    try std.testing.expectEqual(@as(usize, 2), authority.acknowledgements);
    try std.testing.expectEqualStrings(transaction.checkpoint, try executor.loadLogicalReplicationSlotCheckpointAlloc(a, conn, slot, deadline));
    // A cold provider instance must detect an externally advanced/replaced slot
    // instead of starting above its caller's durable position and losing rows.
    request.checkpoint = initial;
    authority.checkpoint = initial;
    var cold = try Executor.init(a);
    defer cold.deinit();
    try std.testing.expectError(error.ForeignProviderIdentityMismatch, cold.asQueryExecutor().pollSubscription(a, dsn, request));
    try std.testing.expectError(error.ForeignProviderIdentityMismatch, cold.asQueryExecutor().acknowledgeSubscription(a, dsn, request));
    try std.testing.expectEqualStrings(transaction.checkpoint, try executor.loadLogicalReplicationSlotCheckpointAlloc(a, conn, slot, deadline));
    request.checkpoint = transaction.checkpoint;
    authority.checkpoint = transaction.checkpoint;
    var caught_up = try cold.asQueryExecutor().pollSubscription(a, dsn, request);
    defer caught_up.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), caught_up.poll.transactions.?.len);
    try execCommandForTest(&executor, conn, a, "BEGIN");
    try execCommandForTest(&executor, conn, a, try std.fmt.allocPrint(a, "UPDATE {s} SET id=1.000, weight='-0'::real WHERE id=1", .{table}));
    try execCommandForTest(&executor, conn, a, try std.fmt.allocPrint(a, "UPDATE {s} SET id=8 WHERE id=1", .{table}));
    try execCommandForTest(&executor, conn, a, "COMMIT");
    var moved = try cold.asQueryExecutor().pollSubscription(a, dsn, request);
    defer moved.deinit(a);
    const move = (try wal.nextWalTransaction(moved.poll, transaction.end_lsn)).?;
    try std.testing.expectEqual(@as(usize, 2), move.changes.len);
    const relation = moved.origin.relations[0];
    const scaled = try wal.addressWalRow(a, origin.provider_identity, relation, move.changes[0]);
    const addressed = try wal.addressWalRow(a, origin.provider_identity, relation, move.changes[1]);
    const insert = try wal.addressWalRow(a, origin.provider_identity, relation, first.poll.changes[0]);
    try std.testing.expectEqualStrings(insert.after_key.?, scaled.before_key.?);
    try std.testing.expectEqualStrings(insert.after_key.?, scaled.after_key.?);
    try std.testing.expectEqualStrings(insert.after_key.?, addressed.before_key.?);
    try std.testing.expect(!std.mem.eql(u8, addressed.before_key.?, addressed.after_key.?));
}

test "postgres libpq subscription capture retains and replays a real exported snapshot" {
    if (std.c.getenv("ANTFLY_TEST_PG_DSN") == null and std.c.getenv("PG_DSN") == null) return error.SkipZigTest;
    const local = @import("antfly_local_sources");
    const capture_mod = @import("../api/replication_subscription_capture.zig");
    const codec = @import("../system_catalog/replication/snapshot_codec.zig");
    const payload = @import("../system_catalog/replication/payload.zig");
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const dsn = try testPgDsnAlloc(a);
    var executor = try Executor.init(a);
    defer executor.deinit();
    const conn = try executor.connect(dsn);
    const table = "antfly_capture_rows";
    const publication = "antfly_capture_publication";
    const slot = "antfly_capture_slot";
    try execCommandForTest(&executor, conn, a, "CREATE TABLE " ++ table ++ "(id bigint, payload jsonb, PRIMARY KEY (id) INCLUDE (payload))");
    defer execCommandForTest(&executor, conn, a, "DROP TABLE IF EXISTS " ++ table ++ " CASCADE") catch {};
    try execCommandForTest(&executor, conn, a, "INSERT INTO " ++ table ++ " VALUES (1,'null'),(2,NULL)");
    try execCommandForTest(&executor, conn, a, "CREATE PUBLICATION " ++ publication ++ " FOR TABLE " ++ table);
    defer execCommandForTest(&executor, conn, a, "DROP PUBLICATION IF EXISTS " ++ publication) catch {};
    const deadline = platform_time.monotonicNs() +| 30 * std.time.ns_per_s;
    defer executor.dropInactiveLogicalReplicationSlotIfExistsAlloc(a, conn, slot, deadline) catch {};
    const Authority = struct {
        binding: ?[32]u8 = null,
        fail_at: ?usize = 1,
        observations: usize = 0,
        export_checkpoint: ?u64 = null,
        fn check(_: *anyopaque) !void {}
        fn observe(raw: *anyopaque, observed: foreign_source.SubscriptionOrigin.Observation) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 1), observed.relations.len);
            self.observations += 1;
            if (self.fail_at == self.observations) return error.TestInjectedObservationFailure;
            self.binding = observed.provider_identity;
        }
        fn exported(raw: *anyopaque, provider: [32]u8, checkpoint: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.binding.?, provider);
            try std.testing.expect(self.export_checkpoint == null);
            self.export_checkpoint = checkpoint;
        }
        fn ack(_: *anyopaque, _: []const u8) !void {
            return error.TestUnexpectedResult;
        }
    };
    var owner: Authority = .{};
    const authority: foreign_source.SubscriptionAuthority = .{ .ptr = &owner, .exported_fn = Authority.exported, .check_fn = Authority.check, .observe_fn = Authority.observe, .acknowledged_fn = Authority.ack };
    var runtime: postgres_source.RuntimeSource = .{ .alloc = a, .executor = executor.asQueryExecutor(), .dsn = try a.dupe(u8, dsn) };
    defer runtime.deinit();
    const request: foreign_source.SubscriptionRequest = .{
        .publications = &.{publication},
        .slot_name = slot,
        .lock_name = "antfly/capture/test",
        .expected = null,
        .authority = authority,
        .password_required = std.c.getenv("ANTFLY_TEST_PG_REQUIRE_PASSWORD") != null,
        .execution_deadline_ns = deadline,
    };
    try std.testing.expectError(error.TestInjectedObservationFailure, runtime.asSource().beginSubscriptionSnapshot(a, request));
    try std.testing.expect(!try executor.logicalReplicationSlotExistsAlloc(a, conn, slot, deadline));
    try std.testing.expect(owner.export_checkpoint == null);
    owner.fail_at = 3;
    try std.testing.expectError(error.TestInjectedObservationFailure, runtime.asSource().beginSubscriptionSnapshot(a, request));
    try std.testing.expect(!try executor.logicalReplicationSlotExistsAlloc(a, conn, slot, deadline));
    try std.testing.expect(owner.export_checkpoint != null);
    owner.export_checkpoint = null;
    owner.fail_at = null;
    var snapshot = try runtime.asSource().beginSubscriptionSnapshot(a, request);
    defer snapshot.deinit(a);
    try std.testing.expectEqual(owner.export_checkpoint.?, try foreign_source.parseReplicationCheckpoint(snapshot.checkpoint));
    try std.testing.expect(snapshot.origin.relations[0].columns[0].key);
    try std.testing.expect(!snapshot.origin.relations[0].columns[1].key);
    const identity: foreign_source.SubscriptionOrigin.Identity = .{
        .owner = .{ .logical_subscription = .{ .database_id = 2, .subscription_id = 3 } },
        .origin_generation = 1,
        .provider_identity = owner.binding.?,
        .recipe_digest = @splat(4),
        .slot_name = slot,
        .roster_digest = try snapshot.origin.digest(a),
    };
    const incarnation = "0123456789abcdef0123456789abcdef".*;
    const held: local.api_replication_source_purpose.Purpose = .{
        .metadata_group = 1,
        .incarnation = incarnation,
        .owner = identity.owner,
        .birth = 1,
        .source_digest = try @import("../metadata/replication_digest.zig").digest("antfly/source-origin/v3", .{ @as(u64, 1), incarnation, identity }, foreign_source.SubscriptionOrigin.max_encoded_bytes),
        .recipe_digest = identity.recipe_digest,
        .roster_digest = identity.roster_digest,
        .upload = .{ .token = 2, .nonce = @splat(5), .expires_at_unix_ns = 100 },
    };
    var dir = try local.common_test_directory.TestDirectory.init("postgres-subscription-capture");
    defer dir.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, dir.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    var capture = try capture_mod.Capture.init(a, &store, identity, held, &snapshot, true);
    defer capture.deinit();
    try execCommandForTest(&executor, conn, a, "INSERT INTO " ++ table ++ " VALUES (3,'{}')");
    for (0..2) |_| try std.testing.expect(!try capture.step(1, deadline, authority));
    try std.testing.expect(try capture.step(1, deadline, authority));
    const context = try payload.retain(a, &store, capture.scope, "test execution context", .none);
    const ready = try capture.finish(context, authority);
    try std.testing.expectEqual(@as(u64, 2), ready.total_rows);
    var replay = try codec.Snapshot.open(a, &store, identity, held, ready, snapshot.origin, .none);
    defer replay.deinit();
    var json_null_seen = false;
    var sql_null_seen = false;
    while (try replay.next(a, .none)) |page| {
        defer page.deinit();
        const row = page.value.rows[0];
        const id = row.value.object.get("id").?.number_string;
        try std.testing.expect(row.value.object.get("payload").? == .null);
        if (std.mem.eql(u8, id, "1")) {
            try std.testing.expectEqualStrings("payload", row.json_null_fields[0]);
            json_null_seen = true;
        } else {
            try std.testing.expectEqualStrings("2", id);
            try std.testing.expectEqual(@as(usize, 0), row.json_null_fields.len);
            sql_null_seen = true;
        }
    }
    try std.testing.expect(json_null_seen and sql_null_seen);
    try snapshot.abort(a);
    try std.testing.expect(!try executor.logicalReplicationSlotExistsAlloc(a, conn, slot, deadline));
}
