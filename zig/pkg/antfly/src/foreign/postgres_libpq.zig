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

const std = @import("std");
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
                    break :blk @as(std.c.E, @enumFromInt(pthread_ext.pthread_cond_timedwait_relative_np(
                        &state.cond,
                        &state.mutex,
                        &relative,
                    )));
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

        fn deinit(self: *@This()) void {
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

        fn deinit(self: *@This()) void {
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
const PG_DIAG_SQLSTATE: c_int = 'C';

const FnPQconnectdb = *const fn ([*:0]const u8) callconv(.c) ?*PGconn;
const FnPQconnectStart = *const fn ([*:0]const u8) callconv(.c) ?*PGconn;
const FnPQconnectPoll = *const fn (?*PGconn) callconv(.c) c_uint;
const FnPQexec = *const fn (?*PGconn, [*:0]const u8) callconv(.c) ?*PGresult;
const FnPQsendQuery = *const fn (?*PGconn, [*:0]const u8) callconv(.c) c_int;
const FnPQstatus = *const fn (?*PGconn) callconv(.c) ConnStatusType;
const FnPQerrorMessage = *const fn (?*PGconn) callconv(.c) [*:0]const u8;
const FnPQfinish = *const fn (?*PGconn) callconv(.c) void;
const FnPQexecParams = *const fn (?*PGconn, [*:0]const u8, c_int, ?[*]const Oid, ?[*]const ?[*:0]const u8, ?[*]const c_int, ?[*]const c_int, c_int) callconv(.c) ?*PGresult;
const FnPQsendQueryParams = *const fn (?*PGconn, [*:0]const u8, c_int, ?[*]const Oid, ?[*]const ?[*:0]const u8, ?[*]const c_int, ?[*]const c_int, c_int) callconv(.c) c_int;
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

        fn wait(self: @This(), events: i16, deadline_ns: u64) !i16 {
            return try self.executor.waitForSocketEvents(self.conn, events, deadline_ns);
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
    pqerrorMessage: FnPQerrorMessage,
    pqfinish: FnPQfinish,
    pqexecParams: FnPQexecParams,
    pqsendQueryParams: FnPQsendQueryParams,
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
            .pqerrorMessage = try lookupRequired(&lib, FnPQerrorMessage, "PQerrorMessage"),
            .pqfinish = try lookupRequired(&lib, FnPQfinish, "PQfinish"),
            .pqexecParams = try lookupRequired(&lib, FnPQexecParams, "PQexecParams"),
            .pqsendQueryParams = try lookupRequired(&lib, FnPQsendQueryParams, "PQsendQueryParams"),
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
                .deinit = deinitQueryExecutor,
                .query = Executor.query,
                .statistics = Executor.statistics,
                .statistics_with_deadline = Executor.statisticsWithDeadline,
                .discover_columns = Executor.discoverColumns,
                .refresh_columns = Executor.refreshColumns,
                .begin_snapshot_query = Executor.beginSnapshotQuery,
                .begin_prepared_replication_snapshot = Executor.beginPreparedReplicationSnapshot,
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

    fn query(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, prepared: sql.PreparedQuery, execution_deadline_ns: ?u64) !foreign_source.QueryResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.queryPreparedAllocWithDeadline(alloc, dsn, prepared, execution_deadline_ns);
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

        fn asSnapshotQuery(self: *@This()) postgres_source.QueryExecutor.SnapshotQuery {
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = SnapshotQuery.destroy,
                    .query = SnapshotQuery.query,
                },
            };
        }

        fn destroy(ptr: *anyopaque, alloc: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.conn) |conn| {
                _ = self.executor.execSimpleAllowCommand(conn, alloc, "ROLLBACK") catch {};
                self.executor.closeCountedConnection(conn);
            }
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
            const result = self.executor.execPreparedWithDeadline(conn, alloc, owned, execution_deadline_ns) catch |err| {
                // PostgreSQL aborts the entire transaction after any statement
                // error. A snapshot connection is therefore never reusable
                // after a failed query, even when the underlying socket is
                // healthy.
                self.executor.closeCountedConnection(conn);
                self.conn = null;
                return err;
            };
            defer self.executor.pqclear(result);
            return try self.executor.readQueryResultAllocWithDeadline(alloc, result, execution_deadline_ns);
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
        const exact_cutover_intent = params.exact_cutover_intent orelse
            return error.InvalidQueryRequest;
        if ((params.retired_slot_name == null) !=
            (params.retired_publication_name == null))
            return error.InvalidQueryRequest;

        const pool = try self.getOrCreateConnectionPool(dsn, execution_deadline_ns);
        defer self.releaseConnectionPool(pool);
        try lockUntil(&pool.control_mutex, execution_deadline_ns);
        defer pool.control_mutex.unlock();

        // Reserve both sockets atomically. Acquiring the replication socket
        // after opening the SQL socket can deadlock when many cutovers reach
        // the global ceiling together.
        try self.acquireGlobalConnectionPermits(2, execution_deadline_ns);
        var release_reserved_permits = true;
        errdefer if (release_reserved_permits) self.releaseGlobalConnections(2);
        var sql_conn: ?*PGconn = try self.connectFreshWithDeadline(dsn, execution_deadline_ns);
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
        try exact_cutover_intent.persist(provider_identity);
        try exact_cutover_intent.check();
        try ensureDeadline(execution_deadline_ns);

        if (params.retired_slot_name) |retired_slot_name| {
            const retired_publication_name = params.retired_publication_name.?;
            try exact_cutover_intent.check();
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
            try exact_cutover_intent.check();
            try self.dropPublicationIfExistsAlloc(
                alloc,
                sql_conn,
                retired_publication_name,
                execution_deadline_ns,
            );
            try exact_cutover_intent.check();
            try exact_cutover_intent.retirementComplete();
            try exact_cutover_intent.check();
        }

        try exact_cutover_intent.check();
        try self.ensurePublicationAlloc(
            alloc,
            dsn,
            sql_conn,
            publication_name,
            params.table,
            params.filter_query_json,
            execution_deadline_ns,
        );
        if (slot_exists) {
            try exact_cutover_intent.check();
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

        try exact_cutover_intent.check();
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
        };
        const checkpoint = try alloc.dupe(u8, exported.checkpoint);
        self.pqfinish(repl_conn);
        repl_conn = null;
        self.releaseGlobalConnections(1);
        release_reserved_permits = false;
        slot_created = false;
        return .{
            .checkpoint = checkpoint,
            .snapshot_query = snapshot_query.asSnapshotQuery(),
        };
    }

    fn pollChanges(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPollResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const pool = try self.getOrCreateConnectionPool(dsn, params.execution_deadline_ns);
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
        const pool = try self.getOrCreateConnectionPool(dsn, params.execution_deadline_ns);
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
        const execution_deadline_ns = params.execution_deadline_ns orelse
            platform_time.monotonicNs() +| 30 * std.time.ns_per_s;
        const pool = try self.getOrCreateConnectionPool(
            dsn,
            execution_deadline_ns,
        );
        defer self.releaseConnectionPool(pool);
        try lockUntil(&pool.control_mutex, execution_deadline_ns);
        defer pool.control_mutex.unlock();
        var bounded_params = params;
        bounded_params.execution_deadline_ns = execution_deadline_ns;
        return try self.cleanupReplicationAlloc(alloc, dsn, bounded_params);
    }

    fn queryPreparedAlloc(self: *@This(), alloc: Allocator, dsn: []const u8, prepared: sql.PreparedQuery) !foreign_source.QueryResult {
        return try self.queryPreparedAllocWithDeadline(alloc, dsn, prepared, null);
    }

    fn queryPreparedAllocWithDeadline(
        self: *@This(),
        alloc: Allocator,
        dsn: []const u8,
        prepared: sql.PreparedQuery,
        execution_deadline_ns: ?u64,
    ) !foreign_source.QueryResult {
        var owned = prepared;
        defer owned.deinit(alloc);

        std.log.info("postgres libpq query begin sql_len={d}", .{owned.sql_text.len});
        var lease = try self.acquireConnection(dsn, execution_deadline_ns);
        defer lease.release();
        std.log.info("postgres libpq query connected sql_len={d}", .{owned.sql_text.len});
        const result = self.execPreparedWithDeadline(lease.conn, alloc, owned, execution_deadline_ns) catch |err| {
            if (invalidatesConnection(err)) lease.invalidate();
            return err;
        };
        defer self.pqclear(result);

        return self.readQueryResultAllocWithDeadline(alloc, result, execution_deadline_ns) catch |err| {
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
        var lease = try self.acquireConnection(dsn, execution_deadline_ns);
        defer lease.release();

        var prepared = try relationPreparedQueryAlloc(
            alloc,
            "SELECT COALESCE(c.reltuples, 0)::bigint, COALESCE(pg_total_relation_size(c.oid), 0) FROM pg_class AS c WHERE c.oid = to_regclass($1)",
            table,
        );
        defer prepared.deinit(alloc);

        const result = self.execPreparedWithDeadline(lease.conn, alloc, prepared, execution_deadline_ns) catch |err| {
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

        var lease = try self.acquireConnection(dsn, execution_deadline_ns);
        defer lease.release();
        var prepared = try tableNamePreparedQueryAlloc(
            alloc,
            "WITH relation AS (SELECT to_regclass($1) AS oid) SELECT a.attname, format_type(a.atttypid, a.atttypmod), CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END FROM relation AS r JOIN pg_attribute AS a ON a.attrelid = r.oid WHERE a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
            relation_sql,
        );
        defer prepared.deinit(alloc);

        const result = self.execPreparedWithDeadline(lease.conn, alloc, prepared, execution_deadline_ns) catch |err| {
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
    ) !*ConnectionPool {
        var reclaim_count: usize = 0;
        while (true) {
            try lockUntil(&self.pools_mutex, execution_deadline_ns);
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
    ) !void {
        std.debug.assert(count > 0 and count <= max_total_connections);

        // All production reservations pass through this short gate. Once a
        // waiter is queued, later one-slot requests cannot bypass it.
        try lockUntil(&self.permit_mutex, execution_deadline_ns);
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

        lockUntil(&self.permit_mutex, execution_deadline_ns) catch |err| return err;
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
                waiter.availability.waitForChange(
                    reclaim_observed,
                    execution_deadline_ns,
                ) catch |err| {
                    self.cancelGlobalPermitWaiter(&waiter);
                    return err;
                };
                lockUntil(&self.permit_mutex, execution_deadline_ns) catch |err| {
                    self.cancelGlobalPermitWaiter(&waiter);
                    return err;
                };
                continue;
            }

            const observed = waiter.availability.snapshot();
            self.permit_mutex.unlock();
            waiter.availability.waitForChange(observed, execution_deadline_ns) catch |err| {
                self.cancelGlobalPermitWaiter(&waiter);
                return err;
            };
            lockUntil(&self.permit_mutex, execution_deadline_ns) catch |err| {
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
    ) !PermitReclaimOutcome {
        var reclaimed: [max_total_connections]*PGconn = undefined;
        var reclaimed_count: usize = 0;

        try lockUntil(&self.reclaim_mutex, execution_deadline_ns);
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

            lockUntil(&self.pools_mutex, execution_deadline_ns) catch |err| {
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
                lockUntil(&pool.mutex, execution_deadline_ns) catch |err| {
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
    ) !ConnectionLease {
        const pool = try self.getOrCreateConnectionPool(dsn, execution_deadline_ns);
        errdefer self.releaseConnectionPool(pool);
        while (true) {
            try lockUntil(&pool.mutex, execution_deadline_ns);
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
                try self.acquireGlobalConnectionPermits(1, execution_deadline_ns);
                lockUntil(&pool.mutex, execution_deadline_ns) catch |err| {
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
                const conn = self.connectFreshWithDeadline(dsn, execution_deadline_ns) catch |err| {
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
            try pool.availability.waitForChange(observed, execution_deadline_ns);
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

        const dsn_z = try self.alloc.dupeZ(u8, dsn);
        defer self.alloc.free(dsn_z);
        const conn = self.pqconnectdb(dsn_z.ptr) orelse return error.ForeignConnectionFailed;
        if (self.pqstatus(conn) != CONNECTION_OK) {
            _ = self.pqerrorMessage(conn);
            self.pqfinish(conn);
            return error.ForeignConnectionFailed;
        }
        const owned_dsn = try self.alloc.dupe(u8, dsn);
        errdefer self.alloc.free(owned_dsn);
        try self.connections.put(self.alloc, owned_dsn, conn);
        return conn;
    }

    fn connectWithDeadline(self: *@This(), dsn: []const u8, execution_deadline_ns: ?u64) !?*PGconn {
        const deadline_ns = execution_deadline_ns orelse return try self.connect(dsn);
        try ensureDeadline(deadline_ns);
        if (self.connections.get(dsn)) |cached| {
            if (self.pqstatus(cached) == CONNECTION_OK) return cached;
            self.invalidateConnection(dsn, cached);
        }

        const dsn_z = try self.alloc.dupeZ(u8, dsn);
        defer self.alloc.free(dsn_z);
        const conn = self.pqconnectStart(dsn_z.ptr) orelse return error.ForeignConnectionFailed;
        errdefer self.pqfinish(conn);
        if (self.pqsetnonblocking(conn, 1) != 0) return error.ForeignConnectionFailed;

        while (true) {
            const polling_status = self.pqconnectPoll(conn);
            switch (polling_status) {
                0 => return error.ForeignConnectionFailed,
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
        if (self.pqstatus(conn) != CONNECTION_OK) return error.ForeignConnectionFailed;
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
    ) !*PGconn {
        const deadline_ns = execution_deadline_ns orelse {
            return (try self.connectFresh(self.alloc, dsn)) orelse error.ForeignConnectionFailed;
        };
        try ensureDeadline(deadline_ns);

        const dsn_z = try self.alloc.dupeZ(u8, dsn);
        defer self.alloc.free(dsn_z);
        const conn = self.pqconnectStart(dsn_z.ptr) orelse return error.ForeignConnectionFailed;
        errdefer self.pqfinish(conn);
        if (self.pqsetnonblocking(conn, 1) != 0) return error.ForeignConnectionFailed;

        while (true) {
            switch (self.pqconnectPoll(conn)) {
                0 => return error.ForeignConnectionFailed,
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
        if (self.pqstatus(conn) != CONNECTION_OK) return error.ForeignConnectionFailed;
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

    fn waitForSocketEvents(self: *@This(), conn: ?*PGconn, events: i16, deadline_ns: u64) !i16 {
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
        if (poll_fds[0].revents & (std.posix.POLL.ERR | std.posix.POLL.HUP | std.posix.POLL.NVAL) != 0)
            return error.ForeignConnectionFailed;
        return poll_fds[0].revents;
    }

    fn waitForSocket(self: *@This(), conn: ?*PGconn, events: i16, deadline_ns: u64) !void {
        _ = try self.waitForSocketEvents(conn, events, deadline_ns);
    }

    fn connectFresh(self: *@This(), alloc: Allocator, dsn: []const u8) !?*PGconn {
        const dsn_z = try alloc.dupeZ(u8, dsn);
        defer alloc.free(dsn_z);
        const conn = self.pqconnectdb(dsn_z.ptr) orelse return error.ForeignConnectionFailed;
        if (self.pqstatus(conn) != CONNECTION_OK) {
            _ = self.pqerrorMessage(conn);
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
        try self.acquireGlobalConnectionPermits(1, execution_deadline_ns);
        errdefer self.releaseGlobalConnections(1);
        return if (execution_deadline_ns != null)
            try self.connectFreshWithDeadline(dsn, execution_deadline_ns)
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
        return try self.connectFreshWithDeadline(repl_dsn, execution_deadline_ns);
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
    ) !?*PGresult {
        const deadline_ns = execution_deadline_ns orelse return try self.execPrepared(conn, alloc, prepared);
        return try self.execPreparedWithDeadlineInternal(conn, alloc, prepared, deadline_ns, false);
    }

    fn execPreparedAllowCommandWithDeadline(
        self: *@This(),
        conn: ?*PGconn,
        alloc: Allocator,
        prepared: sql.PreparedQuery,
        execution_deadline_ns: u64,
    ) !?*PGresult {
        return try self.execPreparedWithDeadlineInternal(conn, alloc, prepared, execution_deadline_ns, true);
    }

    fn execPreparedWithDeadlineInternal(
        self: *@This(),
        conn: ?*PGconn,
        alloc: Allocator,
        prepared: sql.PreparedQuery,
        deadline_ns: u64,
        allow_command_ok: bool,
    ) !?*PGresult {
        try ensureDeadline(deadline_ns);
        var owned_args = try OwnedArgs.init(alloc, prepared.args);
        defer owned_args.deinit(alloc);
        const sql_text_z = try alloc.dupeZ(u8, prepared.sql_text);
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

        return try self.readAsyncResultWithDeadline(conn, deadline_ns, allow_command_ok);
    }

    fn readAsyncResultWithDeadline(
        self: *@This(),
        conn: ?*PGconn,
        deadline_ns: u64,
        allow_command_ok: bool,
    ) !?*PGresult {
        const result = try readSingleAsyncResultWithDeadline(
            AsyncResultDriver{ .executor = self, .conn = conn },
            deadline_ns,
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

        const sql_text_z = try alloc.dupeZ(u8, prepared.sql_text);
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
        try ensureDeadline(execution_deadline_ns);
        const sql_text_z = try alloc.dupeZ(u8, sql_text);
        defer alloc.free(sql_text_z);

        if (self.pqsetnonblocking(conn, 1) != 0) return error.ForeignQueryFailed;
        if (self.pqsendQuery(conn, sql_text_z.ptr) != 1) return error.ForeignQueryFailed;
        return try self.readAsyncResultWithDeadline(
            conn,
            execution_deadline_ns,
            allow_command_ok,
        );
    }

    fn execSimpleInternal(self: *@This(), conn: ?*PGconn, alloc: Allocator, sql_text: []const u8, allow_command_ok: bool) !?*PGresult {
        const sql_text_z = try alloc.dupeZ(u8, sql_text);
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
        return try self.readQueryResultAllocWithDeadline(alloc, result, null);
    }

    fn readQueryResultAllocWithDeadline(
        self: *@This(),
        alloc: Allocator,
        result: ?*PGresult,
        execution_deadline_ns: ?u64,
    ) !foreign_source.QueryResult {
        const rows_len: usize = @intCast(self.pqntuples(result));
        const cols_len: usize = @intCast(self.pqnfields(result));
        const rows = try alloc.alloc(std.json.Value, rows_len);
        errdefer alloc.free(rows);

        var row_idx: usize = 0;
        errdefer {
            for (rows[0..row_idx]) |*row| foreign_source.deinitJsonValue(alloc, row);
        }
        var cells_until_deadline_check: u8 = 1;
        while (row_idx < rows_len) : (row_idx += 1) {
            if (execution_deadline_ns) |deadline_ns| try ensureDeadline(deadline_ns);
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
                cells_until_deadline_check -|= 1;
                if (cells_until_deadline_check == 0) {
                    cells_until_deadline_check = 64;
                    if (execution_deadline_ns) |deadline_ns| try ensureDeadline(deadline_ns);
                }
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
                try object.put(alloc, key, value);
            }
            rows[row_idx] = .{ .object = object };
        }

        return .{
            .rows = rows,
            .total = rows_len,
        };
    }

    fn pollChangesAlloc(self: *@This(), alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPollResult {
        const slot_name = params.slot_name orelse return error.InvalidQueryRequest;
        const publication_name = params.publication_name orelse return error.InvalidQueryRequest;
        std.log.info("postgres libpq poll begin table={s} slot={s}", .{ params.table, slot_name });
        const conn = try self.connectCountedFresh(alloc, dsn, params.execution_deadline_ns);
        defer self.closeCountedConnection(conn);
        std.log.info("postgres libpq poll connected table={s} slot={s}", .{ params.table, slot_name });
        var observed_checkpoint: ?[]u8 = null;
        errdefer if (observed_checkpoint) |value| alloc.free(value);
        try self.ensurePublicationAlloc(
            alloc,
            dsn,
            conn,
            publication_name,
            params.table,
            params.filter_query_json,
            params.execution_deadline_ns,
        );
        if (params.checkpoint) |checkpoint| {
            if (checkpoint.len > 0 and !try self.logicalReplicationSlotExistsAlloc(
                alloc,
                conn,
                slot_name,
                params.execution_deadline_ns,
            )) {
                return error.ForeignReplicationSlotMissing;
            }
            if (checkpoint.len > 0) {
                const advanced_checkpoint = try self.advanceLogicalReplicationSlotCheckpointAlloc(
                    alloc,
                    conn,
                    slot_name,
                    checkpoint,
                    params.execution_deadline_ns,
                );
                observed_checkpoint = advanced_checkpoint;
            }
        } else {
            _ = try self.ensureLogicalReplicationSlotAlloc(
                alloc,
                conn,
                slot_name,
                params.execution_deadline_ns,
            );
        }

        const quoted_slot_name = try quoteSqlStringLiteralAlloc(alloc, slot_name);
        defer alloc.free(quoted_slot_name);
        const quoted_publication_name = try quoteSqlStringLiteralAlloc(alloc, publication_name);
        defer alloc.free(quoted_publication_name);

        var prepared = sql.PreparedQuery{
            .sql_text = try std.fmt.allocPrint(
                alloc,
                "SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes({s}, NULL, {d}, 'proto_version', '2', 'publication_names', {s})",
                .{ quoted_slot_name, params.limit orelse 256, quoted_publication_name },
            ),
        };
        defer prepared.deinit(alloc);

        const result = try self.execPreparedWithDeadline(
            conn,
            alloc,
            prepared,
            params.execution_deadline_ns,
        );
        defer self.pqclear(result);

        var relation_cache = std.AutoHashMapUnmanaged(u32, PgoutputRelation).empty;
        defer deinitPgoutputRelationCache(alloc, &relation_cache);

        var changes = std.ArrayListUnmanaged(foreign_source.ReplicationChange).empty;
        errdefer {
            for (changes.items) |*change| change.deinit(alloc);
            changes.deinit(alloc);
        }
        var current_txn_first_change_idx: ?usize = null;
        var lag_millis: u64 = 0;
        const poll_now_ms: i64 = @intCast(currentRealtimeMillis());

        const rows_len: usize = @intCast(self.pqntuples(result));
        for (0..rows_len) |row_idx| {
            const row: c_int = @intCast(row_idx);
            if (self.pqgetisnull(result, row, 0) != 0 or self.pqgetisnull(result, row, 1) != 0) continue;
            const lsn = try copyCellAlloc(alloc, self.pqgetvalue(result, row, 0), @intCast(self.pqgetlength(result, row, 0)));
            if (observed_checkpoint) |value| alloc.free(value);
            observed_checkpoint = lsn;
            const data_hex = self.pqgetvalue(result, row, 1)[0..@intCast(self.pqgetlength(result, row, 1))];
            const data = try decodeByteaHexAlloc(alloc, data_hex);
            defer alloc.free(data);

            switch (try parsePgoutputMessageAlloc(alloc, data, &relation_cache)) {
                .none => {},
                .begin => {
                    current_txn_first_change_idx = changes.items.len;
                },
                .commit => |commit_timestamp_ms| {
                    if (current_txn_first_change_idx) |start_idx| {
                        const computed_lag_ms: u64 = if (commit_timestamp_ms > 0 and poll_now_ms > 0)
                            @intCast(@max(@as(i64, 0), poll_now_ms - @as(i64, @intCast(commit_timestamp_ms))))
                        else
                            0;
                        for (changes.items[start_idx..]) |*change| {
                            change.commit_timestamp_ms = commit_timestamp_ms;
                            alloc.free(change.checkpoint);
                            change.checkpoint = try alloc.dupe(u8, lsn);
                        }
                        lag_millis = @max(lag_millis, computed_lag_ms);
                    }
                    current_txn_first_change_idx = null;
                },
                .change => |parsed_change| {
                    var change_without_checkpoint = parsed_change;
                    errdefer change_without_checkpoint.deinit(alloc);
                    alloc.free(change_without_checkpoint.checkpoint);
                    change_without_checkpoint.checkpoint = try alloc.dupe(u8, lsn);
                    change_without_checkpoint.lag_records = 0;
                    try changes.append(alloc, change_without_checkpoint);
                },
            }
        }

        if (rows_len > 0 or changes.items.len > 0) {
            std.log.info(
                "postgres libpq logical poll table={s} slot={s} rows={d} changes={d}",
                .{ params.table, slot_name, rows_len, changes.items.len },
            );
        }

        return .{
            .changes = try changes.toOwnedSlice(alloc),
            .checkpoint = if (observed_checkpoint) |value| value else &.{},
            .lag_records = 0,
            .lag_millis = lag_millis,
        };
    }

    fn prepareReplicationAlloc(self: *@This(), alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationPollParams) !foreign_source.ReplicationPrepareResult {
        const slot_name = params.slot_name orelse return error.InvalidQueryRequest;
        const publication_name = params.publication_name orelse return error.InvalidQueryRequest;
        std.log.info("postgres libpq prepare replication table={s} slot={s}", .{ params.table, slot_name });
        const conn = try self.connectCountedFresh(alloc, dsn, params.execution_deadline_ns);
        defer self.closeCountedConnection(conn);
        try self.ensurePublicationAlloc(
            alloc,
            dsn,
            conn,
            publication_name,
            params.table,
            params.filter_query_json,
            params.execution_deadline_ns,
        );
        const slot_existed = try self.ensureLogicalReplicationSlotAlloc(
            alloc,
            conn,
            slot_name,
            params.execution_deadline_ns,
        );
        return .{
            .checkpoint = try self.loadLogicalReplicationSlotCheckpointAlloc(
                alloc,
                conn,
                slot_name,
                params.execution_deadline_ns,
            ),
            .slot_existed = slot_existed,
        };
    }

    fn cleanupReplicationAlloc(self: *@This(), alloc: Allocator, dsn: []const u8, params: foreign_source.ReplicationCleanupParams) !void {
        std.log.info("postgres libpq cleanup replication slot={s} publication={s}", .{ params.slot_name, params.publication_name });
        const execution_deadline_ns = params.execution_deadline_ns orelse
            platform_time.monotonicNs() +| 30 * std.time.ns_per_s;
        const conn = try self.connectCountedFresh(alloc, dsn, execution_deadline_ns);
        defer self.closeCountedConnection(conn);

        try self.dropInactiveLogicalReplicationSlotIfExistsAlloc(
            alloc,
            conn,
            params.slot_name,
            execution_deadline_ns,
        );
        if (try self.logicalReplicationSlotExistsAlloc(
            alloc,
            conn,
            params.slot_name,
            execution_deadline_ns,
        )) return error.ExactCutoverCleanupPending;
        try self.dropPublicationIfExistsAlloc(
            alloc,
            conn,
            params.publication_name,
            execution_deadline_ns,
        );
    }

    fn ensurePublicationAlloc(
        self: *@This(),
        alloc: Allocator,
        dsn: []const u8,
        conn: ?*PGconn,
        publication_name: []const u8,
        table: []const u8,
        filter_query_json: ?[]const u8,
        execution_deadline_ns: ?u64,
    ) !void {
        const check_args = try alloc.alloc(sql.ParameterValue, 1);
        check_args[0] = .{ .string = try alloc.dupe(u8, publication_name) };
        var check_prepared = sql.PreparedQuery{
            .sql_text = try alloc.dupe(u8, "SELECT 1 FROM pg_publication WHERE pubname = $1"),
            .args = check_args,
        };
        defer check_prepared.deinit(alloc);
        const check_result = try self.execPreparedWithDeadline(
            conn,
            alloc,
            check_prepared,
            execution_deadline_ns,
        );
        defer self.pqclear(check_result);
        if (@as(usize, @intCast(self.pqntuples(check_result))) > 0) return;

        const quoted_publication = try sql.postgresDialect().quote_identifier(alloc, publication_name);
        defer alloc.free(quoted_publication);
        const quoted_table = try sql.postgresDialect().quote_relation(alloc, table);
        defer alloc.free(quoted_table);
        var create_sql = std.ArrayListUnmanaged(u8).empty;
        defer create_sql.deinit(alloc);
        const create_prefix = try std.fmt.allocPrint(alloc, "CREATE PUBLICATION {s} FOR TABLE {s}", .{ quoted_publication, quoted_table });
        defer alloc.free(create_prefix);
        try create_sql.appendSlice(alloc, create_prefix);

        var translated_filter: ?filter.Translation = null;
        defer if (translated_filter) |*value| value.deinit(alloc);
        if (filter_query_json) |query_json| {
            const columns = try self.discoverColumnsAllocWithDeadline(
                alloc,
                dsn,
                table,
                execution_deadline_ns,
            );
            defer freeColumns(alloc, columns);
            translated_filter = try filter.translateAlloc(alloc, sql.postgresDialect(), query_json, columns);
            if (translated_filter.?.where_sql.len > 0) {
                const where_suffix = try std.fmt.allocPrint(alloc, " WHERE ({s})", .{translated_filter.?.where_sql});
                defer alloc.free(where_suffix);
                try create_sql.appendSlice(alloc, where_suffix);
            }
        }

        var create_prepared = sql.PreparedQuery{
            .sql_text = try create_sql.toOwnedSlice(alloc),
            .args = if (translated_filter) |value| try cloneParameterValuesAlloc(alloc, value.args) else &.{},
        };
        defer create_prepared.deinit(alloc);
        const create_result = if (execution_deadline_ns) |deadline_ns|
            try self.execPreparedAllowCommandWithDeadline(
                conn,
                alloc,
                create_prepared,
                deadline_ns,
            )
        else
            try self.execPreparedAllowCommand(conn, alloc, create_prepared);
        defer self.pqclear(create_result);
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
            .sql_text = try alloc.dupe(u8, "SELECT COALESCE(confirmed_flush_lsn::text, restart_lsn::text, '') FROM pg_replication_slots WHERE slot_name = $1"),
            .args = args,
        };
        defer prepared.deinit(alloc);
        const result = try self.execPreparedWithDeadline(
            conn,
            alloc,
            prepared,
            execution_deadline_ns,
        );
        defer self.pqclear(result);
        if (self.pqntuples(result) == 0 or self.pqgetisnull(result, 0, 0) != 0) return try alloc.dupe(u8, "");
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
            .sql_text = try alloc.dupe(u8, "SELECT COALESCE(end_lsn::text, '') FROM pg_replication_slot_advance($1, $2::pg_lsn)"),
            .args = args,
        };
        defer prepared.deinit(alloc);
        const result = try self.execPreparedWithDeadline(
            conn,
            alloc,
            prepared,
            execution_deadline_ns,
        );
        defer self.pqclear(result);
        if (self.pqntuples(result) == 0 or self.pqgetisnull(result, 0, 0) != 0) return try alloc.dupe(u8, checkpoint);
        return try copyCellAlloc(alloc, self.pqgetvalue(result, 0, 0), @intCast(self.pqgetlength(result, 0, 0)));
    }

    const ExportedReplicationSnapshot = struct {
        checkpoint: []u8,
        snapshot_name: []u8,

        fn deinit(self: *@This(), alloc: Allocator) void {
            alloc.free(self.checkpoint);
            alloc.free(self.snapshot_name);
            self.* = undefined;
        }
    };

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
            \\SELECT (pg_control_system()).system_identifier::text,
            \\       current_database(),
            \\       oid::text
            \\  FROM pg_database
            \\ WHERE datname = current_database()
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
        const quoted_slot = try sql.postgresDialect().quote_identifier(alloc, slot_name);
        defer alloc.free(quoted_slot);
        const create_sql = try std.fmt.allocPrint(
            alloc,
            "CREATE_REPLICATION_SLOT {s} LOGICAL pgoutput EXPORT_SNAPSHOT",
            .{quoted_slot},
        );
        defer alloc.free(create_sql);
        const result = try self.execSimpleWithDeadline(
            conn,
            alloc,
            create_sql,
            execution_deadline_ns,
            false,
        );
        defer self.pqclear(result);
        if (self.pqntuples(result) == 0 or self.pqnfields(result) < 3) return error.ForeignQueryFailed;
        if (self.pqgetisnull(result, 0, 1) != 0 or self.pqgetisnull(result, 0, 2) != 0) return error.ForeignQueryFailed;
        return try cloneCreatedReplicationSnapshotAlloc(
            alloc,
            slot_created,
            self.pqgetvalue(result, 0, 1)[0..@intCast(self.pqgetlength(result, 0, 1))],
            self.pqgetvalue(result, 0, 2)[0..@intCast(self.pqgetlength(result, 0, 2))],
        );
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
        const conn = try self.connectFreshWithDeadline(dsn, execution_deadline_ns);
        defer self.pqfinish(conn);
        try self.dropInactiveLogicalReplicationSlotIfExistsAlloc(
            alloc,
            conn,
            slot_name,
            execution_deadline_ns,
        );
    }
};

/// Drive libpq's nonblocking command protocol without ever calling
/// PQgetResult while libpq still needs socket input. In particular, flushing
/// must service readable notices/errors as well as writable output, and every
/// result (including the final null result) gets its own PQisBusy gate.
fn readSingleAsyncResultWithDeadline(driver: anytype, deadline_ns: u64) !?*PGresult {
    while (true) {
        try ensureDeadline(deadline_ns);
        const flush_status = driver.flush();
        if (flush_status == 0) break;
        if (flush_status < 0) return error.ForeignQueryFailed;

        const ready = try driver.wait(
            std.posix.POLL.IN | std.posix.POLL.OUT,
            deadline_ns,
        );
        if (ready & std.posix.POLL.IN != 0 and driver.consumeInput() != 1)
            return error.ForeignQueryFailed;
    }

    var first_result: ?*PGresult = null;
    errdefer if (first_result) |result| driver.clear(result);
    var saw_extra_result = false;
    while (true) {
        while (driver.isBusy() != 0) {
            const ready = try driver.wait(std.posix.POLL.IN, deadline_ns);
            if (ready & std.posix.POLL.IN == 0) continue;
            if (driver.consumeInput() != 1) return error.ForeignQueryFailed;
        }

        try ensureDeadline(deadline_ns);
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

    fn deinit(self: *@This()) void {
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
                .deinit = deinitQueryExecutor,
                .query = query,
                .statistics = statistics,
                .statistics_with_deadline = statisticsWithDeadline,
                .discover_columns = discoverColumns,
                .refresh_columns = refreshColumns,
                .begin_snapshot_query = beginSnapshotQuery,
                .begin_prepared_replication_snapshot = beginPreparedReplicationSnapshot,
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

    fn query(ptr: *anyopaque, alloc: Allocator, dsn: []const u8, prepared: sql.PreparedQuery, execution_deadline_ns: ?u64) !foreign_source.QueryResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const executor = self.ensureExecutor(execution_deadline_ns) catch |err| {
            var owned = prepared;
            owned.deinit(alloc);
            return err;
        };
        return try executor.asQueryExecutor().query(alloc, dsn, prepared, execution_deadline_ns);
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
    if (std.mem.indexOf(u8, dsn, "replication=") != null) return try alloc.dupe(u8, dsn);
    if (std.mem.indexOfScalar(u8, dsn, '?') != null) {
        return try std.fmt.allocPrint(alloc, "{s}&replication=database", .{dsn});
    }
    return try std.fmt.allocPrint(alloc, "{s}?replication=database", .{dsn});
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
};

const PgoutputRelation = struct {
    namespace: []u8,
    relation_name: []u8,
    columns: []PgoutputColumn,
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
    begin,
    commit: u64,
    change: foreign_source.ReplicationChange,
};

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
            _ = try readU64(data, &pos); // final lsn
            _ = try readI64(data, &pos); // commit timestamp
            _ = try readU32(data, &pos); // xid
            return .begin;
        },
        'C' => {
            _ = try readByte(data, &pos); // flags
            _ = try readU64(data, &pos); // commit lsn
            _ = try readU64(data, &pos); // end lsn
            const commit_ts = try readI64(data, &pos);
            return .{ .commit = postgresEpochMicrosToUnixMillis(commit_ts) };
        },
        'O' => return .none,
        'Y' => return .none,
        'M' => return .none,
        'T' => return .none,
        'R' => {
            const relation_id = try readU32(data, &pos);
            const namespace = try readCStringAlloc(alloc, data, &pos);
            errdefer alloc.free(namespace);
            const relation_name = try readCStringAlloc(alloc, data, &pos);
            errdefer alloc.free(relation_name);
            _ = try readByte(data, &pos); // replica identity
            const column_count = try readU16(data, &pos);
            const columns = try alloc.alloc(PgoutputColumn, column_count);
            var initialized: usize = 0;
            errdefer {
                for (columns[0..initialized]) |column| alloc.free(column.name);
                alloc.free(columns);
            }
            for (0..column_count) |i| {
                _ = try readByte(data, &pos); // flags
                columns[i] = .{
                    .name = try readCStringAlloc(alloc, data, &pos),
                    .data_type = try readU32(data, &pos),
                };
                initialized += 1;
                _ = try readU32(data, &pos); // type modifier
            }
            if (relation_cache.getPtr(relation_id)) |existing| {
                alloc.free(existing.namespace);
                alloc.free(existing.relation_name);
                for (existing.columns) |column| alloc.free(column.name);
                if (existing.columns.len > 0) alloc.free(existing.columns);
                existing.* = .{
                    .namespace = namespace,
                    .relation_name = relation_name,
                    .columns = columns,
                };
            } else {
                try relation_cache.put(alloc, relation_id, .{
                    .namespace = namespace,
                    .relation_name = relation_name,
                    .columns = columns,
                });
            }
            return .none;
        },
        'I' => {
            const relation = relation_cache.get(try readU32(data, &pos)) orelse return error.InvalidReplicationSourceRow;
            const tuple_tag = try readByte(data, &pos);
            if (tuple_tag != 'N') return error.InvalidReplicationSourceRow;
            const row = try parsePgoutputTupleAlloc(alloc, data, &pos, relation);
            return .{ .change = .{
                .op = .insert,
                .checkpoint = try alloc.alloc(u8, 0),
                .row = row,
            } };
        },
        'U' => {
            const relation = relation_cache.get(try readU32(data, &pos)) orelse return error.InvalidReplicationSourceRow;
            var marker = try readByte(data, &pos);
            if (marker == 'K' or marker == 'O') {
                var old_row = try parsePgoutputTupleAlloc(alloc, data, &pos, relation);
                foreign_source.deinitJsonValue(alloc, &old_row);
                marker = try readByte(data, &pos);
            }
            if (marker != 'N') return error.InvalidReplicationSourceRow;
            const row = try parsePgoutputTupleAlloc(alloc, data, &pos, relation);
            return .{ .change = .{
                .op = .update,
                .checkpoint = try alloc.alloc(u8, 0),
                .row = row,
            } };
        },
        'D' => {
            const relation = relation_cache.get(try readU32(data, &pos)) orelse return error.InvalidReplicationSourceRow;
            const marker = try readByte(data, &pos);
            if (marker != 'K' and marker != 'O') return error.InvalidReplicationSourceRow;
            const row = try parsePgoutputTupleAlloc(alloc, data, &pos, relation);
            return .{ .change = .{
                .op = .delete,
                .checkpoint = try alloc.alloc(u8, 0),
                .row = row,
            } };
        },
        else => return .none,
    }
}

fn parsePgoutputTupleAlloc(alloc: Allocator, data: []const u8, pos: *usize, relation: PgoutputRelation) !std.json.Value {
    const column_count = try readU16(data, pos);
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
                try object.put(alloc, try alloc.dupe(u8, relation.columns[idx].name), .null);
            },
            'u' => {},
            't', 'b' => {
                const len = try readI32(data, pos);
                if (len < 0 or pos.* + @as(usize, @intCast(len)) > data.len) return error.InvalidReplicationSourceRow;
                const raw = data[pos.* .. pos.* + @as(usize, @intCast(len))];
                pos.* += @as(usize, @intCast(len));
                try object.put(
                    alloc,
                    try alloc.dupe(u8, relation.columns[idx].name),
                    try parseLogicalValueAlloc(alloc, relation.columns[idx].data_type, raw, kind == 'b'),
                );
            },
            else => return error.InvalidReplicationSourceRow,
        }
    }

    while (idx < column_count) : (idx += 1) {
        const kind = try readByte(data, pos);
        switch (kind) {
            'n', 'u' => {},
            't', 'b' => {
                const len = try readI32(data, pos);
                if (len < 0 or pos.* + @as(usize, @intCast(len)) > data.len) return error.InvalidReplicationSourceRow;
                pos.* += @as(usize, @intCast(len));
            },
            else => return error.InvalidReplicationSourceRow,
        }
    }

    return .{ .object = object };
}

fn parseLogicalValueAlloc(alloc: Allocator, oid: Oid, raw: []const u8, binary: bool) !std.json.Value {
    if (binary) return .{ .string = try alloc.dupe(u8, raw) };
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
                    const text = try alloc.dupeZ(u8, printed);
                    values[idx] = text.ptr;
                    lengths[idx] = @intCast(printed.len);
                    owned_strings[idx] = text;
                },
                .integer => |value| {
                    const printed = try std.fmt.allocPrint(alloc, "{d}", .{value});
                    defer alloc.free(printed);
                    const text = try alloc.dupeZ(u8, printed);
                    values[idx] = text.ptr;
                    lengths[idx] = @intCast(printed.len);
                    owned_strings[idx] = text;
                },
                .float => |value| {
                    const printed = try std.fmt.allocPrint(alloc, "{d}", .{value});
                    defer alloc.free(printed);
                    const text = try alloc.dupeZ(u8, printed);
                    values[idx] = text.ptr;
                    lengths[idx] = @intCast(printed.len);
                    owned_strings[idx] = text;
                },
                .string => |value| {
                    const text = try alloc.dupeZ(u8, value);
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

    fn deinit(self: *@This(), alloc: Allocator) void {
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
        err == error.ForeignConnectionFailed or
        err == error.ForeignQueryFailed;
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

fn spinOrYield() void {
    if (builtin.os.tag == .freestanding) {
        std.atomic.spinLoopHint();
    } else {
        std.Thread.yield() catch {};
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

        fn wait(self: *@This(), events: i16, _: u64) !i16 {
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

        fn wait(_: *@This(), _: i16, _: u64) !i16 {
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
        ),
    );
    try std.testing.expectEqual(@as(usize, 3), driver.get_result_calls);
    try std.testing.expectEqual(@as(usize, 2), driver.clear_calls);
    try std.testing.expect(driver.restored_blocking);
}

test "postgres libpq registration succeeds without libpq and fails on first use" {
    const alloc = std.testing.allocator;
    const c = struct {
        extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern fn unsetenv(name: [*:0]const u8) c_int;
    };

    const previous = std.c.getenv("ANTFLY_LIBPQ_PATH");
    const restore = if (previous) |value| try alloc.dupeZ(u8, std.mem.span(value)) else null;
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
        const pool = try executor.getOrCreateConnectionPool(dsn, null);
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
        const pool = try executor.getOrCreateConnectionPool(dsn, null);
        executor.releaseConnectionPool(pool);
    }
    try std.testing.expect(executor.reserveGlobalConnections(max_total_connections));
    defer executor.releaseGlobalConnections(max_total_connections);

    try std.testing.expectError(
        error.Timeout,
        executor.acquireGlobalConnectionPermits(
            1,
            platform_time.monotonicNs() + 20 * std.time.ns_per_ms,
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
            inner.acquireGlobalConnectionPermits(count, deadline_ns) catch {
                failure.store(true, .release);
                return;
            };
            acquired.store(true, .release);
            while (!release_permits.load(.acquire)) spinOrYield();
            inner.releaseGlobalConnections(count);
        }
    };
    const deadline_ns = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    const large = try std.Thread.spawn(
        .{},
        Contender.run,
        .{ &executor, 2, deadline_ns, &large_acquired, &allow_release, &failed },
    );
    var large_joined = false;
    defer {
        allow_release.store(true, .release);
        if (!large_joined) large.join();
        const remaining = executor.total_connections.load(.acquire);
        if (remaining > 0) executor.releaseGlobalConnections(remaining);
    }

    while (executor.permit_waiter_count.load(.acquire) != 1) {
        try ensureDeadline(deadline_ns);
        spinOrYield();
    }
    const small = try std.Thread.spawn(
        .{},
        Contender.run,
        .{ &executor, 1, deadline_ns, &small_acquired, &allow_release, &failed },
    );
    var small_joined = false;
    defer {
        allow_release.store(true, .release);
        if (!small_joined) small.join();
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
    large.join();
    large_joined = true;
    small.join();
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
            inner.acquireGlobalConnectionPermits(2, deadline_ns) catch |err| {
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
            inner.acquireGlobalConnectionPermits(1, deadline_ns) catch {
                failure.store(true, .release);
                return;
            };
            acquired.store(true, .release);
            inner.releaseGlobalConnections(1);
        }
    };

    const head_deadline_ns = platform_time.monotonicNs() + std.time.ns_per_s;
    const test_deadline_ns = head_deadline_ns + 5 * std.time.ns_per_s;
    const head = try std.Thread.spawn(
        .{},
        Head.run,
        .{ &executor, head_deadline_ns, &head_timed_out, &failed },
    );
    var head_joined = false;
    defer {
        if (!head_joined) head.join();
        const remaining = executor.total_connections.load(.acquire);
        if (remaining > 0) executor.releaseGlobalConnections(remaining);
    }

    while (executor.permit_waiter_count.load(.acquire) != 1) {
        try ensureDeadline(test_deadline_ns);
        spinOrYield();
    }
    const follower = try std.Thread.spawn(
        .{},
        Follower.run,
        .{ &executor, test_deadline_ns, &follower_acquired, &failed },
    );
    var follower_joined = false;
    defer if (!follower_joined) follower.join();

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
    head.join();
    head_joined = true;
    follower.join();
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

    const pool = try executor.getOrCreateConnectionPool("postgres://permit-transfer", null);
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

    const pool = try executor.getOrCreateConnectionPool("postgres://permit-responsive", null);
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
            ) catch {
                failed.store(true, .release);
                return;
            };
            acquired.store(true, .release);
            while (!release_permits.load(.acquire)) spinOrYield();
            inner.releaseGlobalConnections(2);
        }
    };
    const head = try std.Thread.spawn(
        .{},
        Head.run,
        .{ &executor, &head_acquired, &release_head, &head_failed },
    );
    var head_joined = false;
    defer {
        Fake.allow_finish.store(true, .release);
        release_head.store(true, .release);
        if (!head_joined) head.join();
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
    head.join();
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
    var waiters: [waiter_count]std.Thread = undefined;
    for (&waiters) |*waiter| {
        waiter.* = try std.Thread.spawn(
            .{},
            Waiter.run,
            .{ &availability, observed, &completed, &failed },
        );
    }
    availability.advanceAll();
    for (waiters) |waiter| waiter.join();
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
    inline for (.{ .{ "id", TypeOid.int8 }, .{ "name", TypeOid.json }, .{ "tier", TypeOid.boolean } }) |column| {
        try relation_bytes.append(alloc, 0);
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
    try std.testing.expectEqual(ParsedPgoutputMessage.begin, try parsePgoutputMessageAlloc(alloc, begin_bytes.items, &relation_cache));

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
    try std.testing.expectEqualStrings("la", update.row.?.object.get("name").?.object.get("city").?.string);
    try std.testing.expect(!update.row.?.object.get("tier").?.bool);

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
    try std.testing.expectEqual(@as(i64, 7), delete.row.?.object.get("id").?.integer);

    var commit_bytes = std.ArrayListUnmanaged(u8).empty;
    defer commit_bytes.deinit(alloc);
    try commit_bytes.append(alloc, 'C');
    try commit_bytes.append(alloc, 0);
    try Builder.appendU64(&commit_bytes, alloc, 1);
    try Builder.appendU64(&commit_bytes, alloc, 1);
    try Builder.appendI64(&commit_bytes, alloc, 1_000_000);
    const parsed_commit = try parsePgoutputMessageAlloc(alloc, commit_bytes.items, &relation_cache);
    try std.testing.expect(parsed_commit == .commit);
    try std.testing.expect(parsed_commit.commit > 946_684_800_000);
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
        );
        defer first_lease.release();
        var second_lease = try executor.acquireConnection(
            dsn,
            platform_time.monotonicNs() + std.time.ns_per_s,
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
            );
        }
        try std.testing.expectError(
            error.Timeout,
            executor.acquireConnection(
                dsn,
                platform_time.monotonicNs() + 25 * std.time.ns_per_ms,
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
        const waiter = try std.Thread.spawn(
            .{},
            SignaledWaiter.run,
            .{ &executor, dsn, &acquired, &failed },
        );
        platform_time.sleepNs(25 * std.time.ns_per_ms);
        initialized -= 1;
        saturation_leases[initialized].release();
        waiter.join();
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
    var second_poll = try executor.pollChangesAlloc(alloc, dsn, second_poll_params);
    defer second_poll.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), second_poll.changes.len);
    try std.testing.expectEqual(foreign_source.ReplicationOp.update, second_poll.changes[0].op);
    try std.testing.expectEqualStrings("u1", second_poll.changes[0].row.?.object.get("id").?.string);
    try std.testing.expectEqualStrings("platinum", second_poll.changes[0].row.?.object.get("tier").?.string);
    try std.testing.expect(second_poll.checkpoint.len > 0);
    try std.testing.expectEqualStrings(second_poll.changes[0].checkpoint, second_poll.checkpoint);
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
