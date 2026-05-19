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
const zig_lmdb = @import("lmdb_engine");
const sim_fixture = @import("sim_fixture.zig");
const lmdb_sim_fixture = @import("lmdb_sim_fixture.zig");
const c = @cImport(@cInclude("lmdb.h"));
var lmdb_sim_tmp_nonce: u64 = 0;

fn nextLmdbSimTmpNonce() u64 {
    return @atomicRmw(u64, &lmdb_sim_tmp_nonce, .Add, 1, .seq_cst);
}

fn nowNs() u64 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

pub fn namespace(comptime Api: type) type {
    const Error = Api.Error;
    const EnvironmentOptions = Api.EnvironmentOptions;
    const DbOptions = Api.DbOptions;
    const TransactionOptions = Api.TransactionOptions;
    const Entry = Api.Entry;

    return struct {
        const Self = @This();

        const DifferentialBackend = enum {
            c,
            zig,
        };

        const DifferentialDbi = union(enum) {
            c: c.MDB_dbi,
            zig: zig_lmdb.txn.Dbi,
        };

        const DifferentialDbHandles = struct {
            main: DifferentialDbi,
            docs: DifferentialDbi,
            dups: DifferentialDbi,
        };

        const DifferentialEnvironment = struct {
            backend: DifferentialBackend,
            path_z: [:0]u8,
            opts: EnvironmentOptions,
            c_env: ?*c.MDB_env = null,
            zig_env: ?zig_lmdb.env.Environment = null,

            fn open(path: [*:0]const u8, opts: EnvironmentOptions, backend: DifferentialBackend) Error!DifferentialEnvironment {
                if (backend == .zig and opts.map_async and !opts.write_map) return Error.Incompatible;

                const path_owned = std.heap.c_allocator.dupeZ(u8, std.mem.span(path)) catch return Error.LmdbUnexpected;
                errdefer std.heap.c_allocator.free(path_owned);

                var c_env: ?*c.MDB_env = null;
                var zig_env: ?zig_lmdb.env.Environment = null;

                switch (backend) {
                    .c => {
                        try Api.check(c.mdb_env_create(&c_env));
                        const env = c_env.?;
                        errdefer c.mdb_env_close(env);

                        if (opts.max_dbs > 0) try Api.check(c.mdb_env_set_maxdbs(env, opts.max_dbs));
                        try Api.check(c.mdb_env_set_maxreaders(env, opts.max_readers));
                        try Api.check(c.mdb_env_set_mapsize(env, opts.map_size));

                        var flags: c_uint = 0;
                        if (opts.fixed_map) flags |= c.MDB_FIXEDMAP;
                        if (opts.no_subdir) flags |= c.MDB_NOSUBDIR;
                        if (opts.read_only) flags |= c.MDB_RDONLY;
                        if (opts.no_sync) flags |= c.MDB_NOSYNC;
                        if (opts.no_meta_sync) flags |= c.MDB_NOMETASYNC;
                        if (opts.no_tls) flags |= c.MDB_NOTLS;
                        if (opts.write_map) flags |= c.MDB_WRITEMAP;
                        if (opts.map_async) flags |= c.MDB_MAPASYNC;
                        if (opts.no_lock) flags |= c.MDB_NOLOCK;
                        if (opts.no_read_ahead) flags |= c.MDB_NORDAHEAD;
                        if (opts.no_mem_init) flags |= c.MDB_NOMEMINIT;

                        try Api.check(c.mdb_env_open(env, path_owned, flags, @intCast(opts.mode)));
                    },
                    .zig => {
                        zig_env = zig_lmdb.env.Environment.open(path_owned, .{
                            .no_subdir = opts.no_subdir,
                            .read_only = opts.read_only,
                            .fixed_map = opts.fixed_map,
                            .write_map = opts.write_map,
                            .map_async = opts.map_async,
                            .no_read_ahead = opts.no_read_ahead,
                            .no_sync = opts.no_sync,
                            .no_meta_sync = opts.no_meta_sync,
                            .no_tls = opts.no_tls,
                            .no_lock = opts.no_lock,
                            .no_mem_init = opts.no_mem_init,
                            .artificial_sync_delay_ns = opts.artificial_sync_delay_ns,
                            .commit_backend = opts.commit_backend,
                        }) catch |err| switch (err) {
                            error.FileNotFound => blk: {
                                if (opts.read_only) return Api.mapZigError(err);
                                try Api.bootstrapZigDataFile(path_owned, opts);
                                break :blk zig_lmdb.env.Environment.open(path_owned, .{
                                    .no_subdir = opts.no_subdir,
                                    .read_only = opts.read_only,
                                    .fixed_map = opts.fixed_map,
                                    .write_map = opts.write_map,
                                    .map_async = opts.map_async,
                                    .no_read_ahead = opts.no_read_ahead,
                                    .no_sync = opts.no_sync,
                                    .no_meta_sync = opts.no_meta_sync,
                                    .no_tls = opts.no_tls,
                                    .no_lock = opts.no_lock,
                                    .no_mem_init = opts.no_mem_init,
                                    .artificial_sync_delay_ns = opts.artificial_sync_delay_ns,
                                    .commit_backend = opts.commit_backend,
                                }) catch |retry_err| return Api.mapZigError(retry_err);
                            },
                            else => return Api.mapZigError(err),
                        };
                    },
                }

                return .{
                    .backend = backend,
                    .path_z = path_owned,
                    .opts = opts,
                    .c_env = c_env,
                    .zig_env = zig_env,
                };
            }

            fn close(self: *DifferentialEnvironment) void {
                if (self.zig_env) |*zig_env| zig_env.close();
                if (self.c_env) |env| c.mdb_env_close(env);
                std.heap.c_allocator.free(self.path_z);
                self.* = undefined;
            }

            fn begin(self: *DifferentialEnvironment, txn_opts: TransactionOptions) Error!DifferentialTransaction {
                return DifferentialTransaction.begin(self, txn_opts);
            }
        };

        const DifferentialTransaction = struct {
            backend: union(enum) {
                c: *c.MDB_txn,
                zig: *ZigTxn,
            },

            const ZigTxn = struct {
                env: *zig_lmdb.env.Environment,
                txn: zig_lmdb.txn.Transaction,
            };

            fn begin(env: *DifferentialEnvironment, txn_opts: TransactionOptions) Error!DifferentialTransaction {
                switch (env.backend) {
                    .c => {
                        var txn: ?*c.MDB_txn = null;
                        const flags: c_uint = if (txn_opts.read_only) c.MDB_RDONLY else 0;
                        try Api.check(c.mdb_txn_begin(env.c_env orelse return Error.LmdbUnexpected, null, flags, &txn));
                        return .{ .backend = .{ .c = txn.? } };
                    },
                    .zig => {
                        const zig_env = &(env.zig_env orelse return Error.LmdbUnexpected);
                        zig_env.refresh() catch |err| return Api.mapZigError(err);
                        const zig_txn = zig_lmdb.txn.Transaction.begin(zig_env, .{ .read_only = txn_opts.read_only }) catch |err| {
                            return Api.mapZigError(err);
                        };
                        const zig_state = std.heap.c_allocator.create(ZigTxn) catch return Error.LmdbUnexpected;
                        errdefer std.heap.c_allocator.destroy(zig_state);
                        zig_state.* = .{
                            .env = zig_env,
                            .txn = zig_txn,
                        };
                        return .{ .backend = .{ .zig = zig_state } };
                    },
                }
            }

            fn beginChild(self: *DifferentialTransaction) Error!DifferentialTransaction {
                switch (self.backend) {
                    .c => |txn| {
                        var child: ?*c.MDB_txn = null;
                        try Api.check(c.mdb_txn_begin(c.mdb_txn_env(txn), txn, 0, &child));
                        return .{ .backend = .{ .c = child.? } };
                    },
                    .zig => |zig_txn| {
                        const child_txn = zig_lmdb.txn.Transaction.beginChild(&zig_txn.txn) catch |err| return Api.mapZigError(err);
                        const child_state = std.heap.c_allocator.create(ZigTxn) catch return Error.LmdbUnexpected;
                        errdefer std.heap.c_allocator.destroy(child_state);
                        child_state.* = .{
                            .env = zig_txn.env,
                            .txn = child_txn,
                        };
                        child_state.txn.rebindEnv(child_state.env);
                        return .{ .backend = .{ .zig = child_state } };
                    },
                }
            }

            fn commit(self: *DifferentialTransaction) Error!void {
                switch (self.backend) {
                    .c => |txn| try Api.check(c.mdb_txn_commit(txn)),
                    .zig => |zig_txn| {
                        zig_txn.txn.commit() catch |err| return Api.mapZigError(err);
                        std.heap.c_allocator.destroy(zig_txn);
                    },
                }
                self.* = undefined;
            }

            fn abort(self: *DifferentialTransaction) void {
                switch (self.backend) {
                    .c => |txn| c.mdb_txn_abort(txn),
                    .zig => |zig_txn| {
                        zig_txn.txn.abort();
                        std.heap.c_allocator.destroy(zig_txn);
                    },
                }
                self.* = undefined;
            }

            fn openDb(self: *DifferentialTransaction, name: ?[*:0]const u8, db_opts: DbOptions) Error!DifferentialDbi {
                switch (self.backend) {
                    .c => |txn| {
                        var dbi: c.MDB_dbi = 0;
                        var flags: c_uint = 0;
                        if (db_opts.create) flags |= c.MDB_CREATE;
                        if (db_opts.reverse_key) flags |= c.MDB_REVERSEKEY;
                        if (db_opts.integer_key) flags |= c.MDB_INTEGERKEY;
                        if (db_opts.dup_sort) flags |= c.MDB_DUPSORT;
                        if (db_opts.dup_fixed) flags |= c.MDB_DUPFIXED;
                        if (db_opts.integer_dup) flags |= c.MDB_INTEGERDUP;
                        if (db_opts.reverse_dup) flags |= c.MDB_REVERSEDUP;
                        try Api.check(c.mdb_dbi_open(txn, name, flags, &dbi));
                        return .{ .c = dbi };
                    },
                    .zig => |zig_txn| {
                        const zig_name = if (name) |n| std.mem.span(n) else null;
                        const dbi = zig_txn.txn.openDb(zig_name, .{
                            .create = db_opts.create,
                            .reverse_key = db_opts.reverse_key,
                            .integer_key = db_opts.integer_key,
                            .dup_sort = db_opts.dup_sort,
                            .dup_fixed = db_opts.dup_fixed,
                            .integer_dup = db_opts.integer_dup,
                            .reverse_dup = db_opts.reverse_dup,
                        }) catch |err| return Api.mapZigError(err);
                        return .{ .zig = dbi };
                    },
                }
            }

            fn openKnownDbs(self: *DifferentialTransaction, create: bool) Error!DifferentialDbHandles {
                return .{
                    .main = try self.openDb(null, .{ .create = create }),
                    .docs = try self.openDb("docs", .{ .create = create }),
                    .dups = try self.openDb("dups", .{ .create = create, .dup_sort = true }),
                };
            }

            fn put(self: *DifferentialTransaction, dbi: DifferentialDbi, key: []const u8, value: []const u8) Error!void {
                switch (self.backend) {
                    .c => |txn| {
                        const c_dbi = switch (dbi) {
                            .c => |value_dbi| value_dbi,
                            .zig => return Error.BadDbi,
                        };
                        var k = toVal(key);
                        var v = toVal(value);
                        try Api.check(c.mdb_put(txn, c_dbi, &k, &v, 0));
                    },
                    .zig => |zig_txn| {
                        const zig_dbi = switch (dbi) {
                            .zig => |value_dbi| value_dbi,
                            .c => return Error.BadDbi,
                        };
                        zig_txn.txn.put(zig_dbi, key, value, .{}) catch |err| return Api.mapZigError(err);
                    },
                }
            }

            fn delete(self: *DifferentialTransaction, dbi: DifferentialDbi, key: []const u8) Error!void {
                switch (self.backend) {
                    .c => |txn| {
                        const c_dbi = switch (dbi) {
                            .c => |value_dbi| value_dbi,
                            .zig => return Error.BadDbi,
                        };
                        var k = toVal(key);
                        try Api.check(c.mdb_del(txn, c_dbi, &k, null));
                    },
                    .zig => |zig_txn| {
                        const zig_dbi = switch (dbi) {
                            .zig => |value_dbi| value_dbi,
                            .c => return Error.BadDbi,
                        };
                        zig_txn.txn.delete(zig_dbi, key) catch |err| return Api.mapZigError(err);
                    },
                }
            }

            fn deleteValue(self: *DifferentialTransaction, dbi: DifferentialDbi, key: []const u8, value: []const u8) Error!void {
                switch (self.backend) {
                    .c => |txn| {
                        const c_dbi = switch (dbi) {
                            .c => |value_dbi| value_dbi,
                            .zig => return Error.BadDbi,
                        };
                        var k = toVal(key);
                        var v = toVal(value);
                        try Api.check(c.mdb_del(txn, c_dbi, &k, &v));
                    },
                    .zig => |zig_txn| {
                        const zig_dbi = switch (dbi) {
                            .zig => |value_dbi| value_dbi,
                            .c => return Error.BadDbi,
                        };
                        zig_txn.txn.deleteValue(zig_dbi, key, value) catch |err| return Api.mapZigError(err);
                    },
                }
            }

            fn cursor(self: *DifferentialTransaction, dbi: DifferentialDbi) Error!DifferentialCursor {
                return DifferentialCursor.open(self, dbi);
            }

            fn publishCommitPhaseForTest(
                self: *DifferentialTransaction,
                phase: zig_lmdb.commit_support.CommitPublishPhase,
            ) Error!void {
                switch (self.backend) {
                    .c => return Error.Incompatible,
                    .zig => |zig_txn| {
                        zig_lmdb.txn.publishCommitPhaseForTest(&zig_txn.txn, phase) catch |err| return Api.mapZigError(err);
                    },
                }
            }
        };

        const DifferentialCursor = struct {
            backend: union(enum) {
                c: *c.MDB_cursor,
                zig: zig_lmdb.cursor.Cursor,
            },

            fn open(txn: *DifferentialTransaction, dbi: DifferentialDbi) Error!DifferentialCursor {
                switch (txn.backend) {
                    .c => |c_txn| {
                        const c_dbi = switch (dbi) {
                            .c => |value_dbi| value_dbi,
                            .zig => return Error.BadDbi,
                        };
                        var cur: ?*c.MDB_cursor = null;
                        try Api.check(c.mdb_cursor_open(c_txn, c_dbi, &cur));
                        return .{ .backend = .{ .c = cur.? } };
                    },
                    .zig => |zig_txn| {
                        const zig_dbi = switch (dbi) {
                            .zig => |value_dbi| value_dbi,
                            .c => return Error.BadDbi,
                        };
                        return .{ .backend = .{ .zig = zig_lmdb.cursor.Cursor.init(&zig_txn.txn, zig_dbi) } };
                    },
                }
            }

            fn close(self: *DifferentialCursor) void {
                switch (self.backend) {
                    .c => |cursor| c.mdb_cursor_close(cursor),
                    .zig => {},
                }
                self.* = undefined;
            }

            fn first(self: *DifferentialCursor) Error!Entry {
                switch (self.backend) {
                    .c => |cursor| {
                        var k: c.MDB_val = undefined;
                        var v: c.MDB_val = undefined;
                        try Api.check(c.mdb_cursor_get(cursor, &k, &v, c.MDB_FIRST));
                        return .{ .key = fromVal(k), .value = fromVal(v) };
                    },
                    .zig => |*cursor| {
                        const entry = cursor.first() catch |err| return Api.mapZigError(err);
                        return .{ .key = entry.key, .value = entry.value };
                    },
                }
            }

            fn next(self: *DifferentialCursor) Error!Entry {
                switch (self.backend) {
                    .c => |cursor| {
                        var k: c.MDB_val = undefined;
                        var v: c.MDB_val = undefined;
                        try Api.check(c.mdb_cursor_get(cursor, &k, &v, c.MDB_NEXT));
                        return .{ .key = fromVal(k), .value = fromVal(v) };
                    },
                    .zig => |*cursor| {
                        const entry = cursor.next() catch |err| return Api.mapZigError(err);
                        return .{ .key = entry.key, .value = entry.value };
                    },
                }
            }

            fn seekRange(self: *DifferentialCursor, key: []const u8) Error!Entry {
                switch (self.backend) {
                    .c => |cursor| {
                        var k = toVal(key);
                        var v: c.MDB_val = undefined;
                        try Api.check(c.mdb_cursor_get(cursor, &k, &v, c.MDB_SET_RANGE));
                        return .{ .key = fromVal(k), .value = fromVal(v) };
                    },
                    .zig => |*cursor| {
                        const entry = cursor.setRange(key) catch |err| return Api.mapZigError(err);
                        return .{ .key = entry.key, .value = entry.value };
                    },
                }
            }
        };

        const DifferentialAction = lmdb_sim_fixture.DifferentialAction;
        const ScheduledAction = lmdb_sim_fixture.ScheduledAction;

        const OwnedEntry = struct {
            key: []u8,
            value: []u8,
        };

        const Snapshot = struct {
            main: []OwnedEntry = &.{},
            docs: []OwnedEntry = &.{},
            dups: []OwnedEntry = &.{},

            fn deinit(self: *Snapshot, allocator: std.mem.Allocator) void {
                freeOwnedEntries(allocator, self.main);
                freeOwnedEntries(allocator, self.docs);
                freeOwnedEntries(allocator, self.dups);
                self.* = .{};
            }
        };

        const ReplayFixture = lmdb_sim_fixture.ReplayFixture;
        const CrashOutcome = lmdb_sim_fixture.CrashOutcome;

        const SnapshotSummary = struct {
            main_count: usize,
            docs_count: usize,
            dups_count: usize,
        };

        pub fn runDifferentialDefault(allocator: std.mem.Allocator) !void {
            try runDifferentialScheduleCaseOrSkip(allocator, .{ .max_dbs = 4 }, 0xA17F_1EED, 72, "diff-default");
        }

        pub fn runDifferentialWriteMapModes(allocator: std.mem.Allocator) !void {
            const cases = [_]struct {
                label: []const u8,
                opts: EnvironmentOptions,
                seed: u64,
                steps: usize,
            }{
                .{ .label = "diff-writemap", .opts = .{ .max_dbs = 4, .write_map = true }, .seed = 0xA17F_2EED, .steps = 40 },
                .{ .label = "diff-mapasync", .opts = .{ .max_dbs = 4, .write_map = true, .map_async = true }, .seed = 0xA17F_3EED, .steps = 40 },
            };
            // MDB_FIXEDMAP persists a concrete mm_address across reopens, which makes
            // randomized reopen schedules sensitive to host VM layout. Keep fixed_map
            // coverage in targeted tests and replay fixtures instead of this matrix.
            for (cases) |case| {
                try runDifferentialScheduleCaseOrSkip(allocator, case.opts, case.seed, case.steps, case.label);
            }
        }

        pub fn runDifferentialSyncPolicyModes(allocator: std.mem.Allocator) !void {
            const cases = [_]struct {
                label: []const u8,
                opts: EnvironmentOptions,
                seed: u64,
                steps: usize,
            }{
                .{ .label = "diff-nosync", .opts = .{ .max_dbs = 4, .no_sync = true }, .seed = 0xA17F_7EED, .steps = 40 },
                .{ .label = "diff-nometasync", .opts = .{ .max_dbs = 4, .no_meta_sync = true }, .seed = 0xA17F_8EED, .steps = 40 },
            };
            for (cases) |case| {
                try runDifferentialScheduleCaseOrSkip(allocator, case.opts, case.seed, case.steps, case.label);
            }
        }

        pub fn runDifferentialCommitBackends(allocator: std.mem.Allocator) !void {
            const cases = [_]struct {
                label: []const u8,
                opts: EnvironmentOptions,
                seed: u64,
                steps: usize,
            }{
                .{ .label = "diff-worker-thread", .opts = .{ .max_dbs = 4, .commit_backend = .worker_thread }, .seed = 0xA17F_4EED, .steps = 48 },
                .{ .label = "diff-async-io", .opts = .{ .max_dbs = 4, .commit_backend = .async_io }, .seed = 0xA17F_5EED, .steps = 48 },
            };
            for (cases) |case| {
                try runDifferentialScheduleCaseOrSkip(allocator, case.opts, case.seed, case.steps, case.label);
            }
        }

        pub fn runCrashDefault(allocator: std.mem.Allocator) !void {
            try runCrashPhaseCaseOrSkip(allocator, .{ .max_dbs = 4 }, "crash-default");
        }

        pub fn runCrashWriteMapModes(allocator: std.mem.Allocator) !void {
            const cases = [_]struct {
                label: []const u8,
                opts: EnvironmentOptions,
            }{
                .{ .label = "crash-writemap", .opts = .{ .max_dbs = 4, .write_map = true } },
                .{ .label = "crash-mapasync", .opts = .{ .max_dbs = 4, .write_map = true, .map_async = true } },
            };
            // See the fixed_map note above: deterministic fixed_map crash coverage lives
            // in explicit targeted tests rather than the randomized reopen harness.
            for (cases) |case| {
                try runCrashPhaseCaseOrSkip(allocator, case.opts, case.label);
            }
        }

        pub fn runCrashSyncPolicyModes(allocator: std.mem.Allocator) !void {
            const cases = [_]struct {
                label: []const u8,
                opts: EnvironmentOptions,
            }{
                .{ .label = "crash-nosync", .opts = .{ .max_dbs = 4, .no_sync = true } },
                .{ .label = "crash-nometasync", .opts = .{ .max_dbs = 4, .no_meta_sync = true } },
            };
            for (cases) |case| {
                try runCrashPhaseCaseOrSkip(allocator, case.opts, case.label);
            }
        }

        pub fn runCrashCommitBackends(allocator: std.mem.Allocator) !void {
            const cases = [_]struct {
                label: []const u8,
                opts: EnvironmentOptions,
            }{
                .{ .label = "crash-worker-thread", .opts = .{ .max_dbs = 4, .commit_backend = .worker_thread } },
                .{ .label = "crash-async-io", .opts = .{ .max_dbs = 4, .commit_backend = .async_io } },
            };
            for (cases) |case| {
                try runCrashPhaseCaseOrSkip(allocator, case.opts, case.label);
            }
        }

        pub fn runReplayFixtures(allocator: std.mem.Allocator) !void {
            var fixtures_dir = std.Io.Dir.cwd().openDir(std.testing.io, "pkg/antfly/src/storage/lmdb_sim_fixtures", .{ .iterate = true }) catch |err| switch (err) {
                error.FileNotFound => return,
                else => return err,
            };
            defer fixtures_dir.close(std.testing.io);

            var fixture_names: std.ArrayListUnmanaged([]u8) = .empty;
            defer {
                for (fixture_names.items) |name| allocator.free(name);
                fixture_names.deinit(allocator);
            }

            var walker = try fixtures_dir.walk(allocator);
            defer walker.deinit();

            while (try walker.next(std.testing.io)) |entry| {
                if (entry.kind != .file) continue;
                if (!std.mem.endsWith(u8, entry.path, ".fixture")) continue;
                try fixture_names.append(allocator, try allocator.dupe(u8, entry.path));
            }

            std.mem.sort([]u8, fixture_names.items, {}, struct {
                fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
                    return std.mem.lessThan(u8, lhs, rhs);
                }
            }.lessThan);

            for (fixture_names.items) |name| {
                try replayFixtureFile(allocator, name);
            }
        }

        pub fn runSoak(allocator: std.mem.Allocator) !void {
            const differential_cases = [_]struct {
                label: []const u8,
                opts: EnvironmentOptions,
                seed: u64,
                steps: usize,
            }{
                .{ .label = "soak-default-a", .opts = .{ .max_dbs = 4 }, .seed = 0xA17F_9001, .steps = 160 },
                .{ .label = "soak-default-b", .opts = .{ .max_dbs = 4 }, .seed = 0xA17F_9002, .steps = 160 },
                .{ .label = "soak-async-io", .opts = .{ .max_dbs = 4, .commit_backend = .async_io }, .seed = 0xA17F_9003, .steps = 128 },
                .{ .label = "soak-mapasync", .opts = .{ .max_dbs = 4, .write_map = true, .map_async = true }, .seed = 0xA17F_9004, .steps = 120 },
            };

            for (differential_cases) |case| {
                try runDifferentialScheduleCaseOrSkip(allocator, case.opts, case.seed, case.steps, case.label);
            }

            const crash_cases = [_]struct {
                label: []const u8,
                opts: EnvironmentOptions,
            }{
                .{ .label = "soak-crash-default", .opts = .{ .max_dbs = 4 } },
                .{ .label = "soak-crash-async-io", .opts = .{ .max_dbs = 4, .commit_backend = .async_io } },
                .{ .label = "soak-crash-mapasync", .opts = .{ .max_dbs = 4, .write_map = true, .map_async = true } },
            };

            for (crash_cases) |case| {
                try runCrashPhaseCaseOrSkip(allocator, case.opts, case.label);
            }
        }

        fn freeOwnedEntries(allocator: std.mem.Allocator, entries: []OwnedEntry) void {
            for (entries) |entry| {
                allocator.free(entry.key);
                allocator.free(entry.value);
            }
            allocator.free(entries);
        }

        fn appendOwnedEntry(
            allocator: std.mem.Allocator,
            entries: *std.ArrayListUnmanaged(OwnedEntry),
            entry: Entry,
        ) std.mem.Allocator.Error!void {
            try entries.append(allocator, .{
                .key = try allocator.dupe(u8, entry.key),
                .value = try allocator.dupe(u8, entry.value),
            });
        }

        fn collectOwnedEntries(
            allocator: std.mem.Allocator,
            txn: *DifferentialTransaction,
            dbi: DifferentialDbi,
        ) (Error || std.mem.Allocator.Error)![]OwnedEntry {
            var entries: std.ArrayListUnmanaged(OwnedEntry) = .empty;
            errdefer {
                for (entries.items) |entry| {
                    allocator.free(entry.key);
                    allocator.free(entry.value);
                }
                entries.deinit(allocator);
            }

            var cursor = try txn.cursor(dbi);
            defer cursor.close();

            const first = cursor.first() catch |err| switch (err) {
                Error.NotFound => return entries.toOwnedSlice(allocator),
                else => return err,
            };
            try appendOwnedEntry(allocator, &entries, first);

            while (true) {
                const entry = cursor.next() catch |err| switch (err) {
                    Error.NotFound => break,
                    else => return err,
                };
                try appendOwnedEntry(allocator, &entries, entry);
            }

            return entries.toOwnedSlice(allocator);
        }

        fn collectMainOwnedEntries(
            allocator: std.mem.Allocator,
            txn: *DifferentialTransaction,
            dbi: DifferentialDbi,
        ) (Error || std.mem.Allocator.Error)![]OwnedEntry {
            var entries: std.ArrayListUnmanaged(OwnedEntry) = .empty;
            errdefer {
                for (entries.items) |entry| {
                    allocator.free(entry.key);
                    allocator.free(entry.value);
                }
                entries.deinit(allocator);
            }

            var cursor = try txn.cursor(dbi);
            defer cursor.close();

            const first = cursor.seekRange("m-") catch |err| switch (err) {
                Error.NotFound => return entries.toOwnedSlice(allocator),
                else => return err,
            };
            if (!std.mem.startsWith(u8, first.key, "m-")) return entries.toOwnedSlice(allocator);
            try appendOwnedEntry(allocator, &entries, first);

            while (true) {
                const entry = cursor.next() catch |err| switch (err) {
                    Error.NotFound => break,
                    else => return err,
                };
                if (!std.mem.startsWith(u8, entry.key, "m-")) break;
                try appendOwnedEntry(allocator, &entries, entry);
            }

            return entries.toOwnedSlice(allocator);
        }

        fn takeDifferentialSnapshot(
            allocator: std.mem.Allocator,
            path: [*:0]const u8,
            backend: DifferentialBackend,
            base_opts: EnvironmentOptions,
        ) (Error || std.mem.Allocator.Error)!Snapshot {
            var read_opts = base_opts;
            read_opts.read_only = true;

            var env = try DifferentialEnvironment.open(path, read_opts, backend);
            defer env.close();

            var txn = try env.begin(.{ .read_only = true });
            defer txn.abort();

            const dbs = try txn.openKnownDbs(false);
            return .{
                .main = try collectMainOwnedEntries(allocator, &txn, dbs.main),
                .docs = try collectOwnedEntries(allocator, &txn, dbs.docs),
                .dups = try collectOwnedEntries(allocator, &txn, dbs.dups),
            };
        }

        fn takeTransactionSnapshot(
            allocator: std.mem.Allocator,
            txn: *DifferentialTransaction,
        ) (Error || std.mem.Allocator.Error)!Snapshot {
            const dbs = try txn.openKnownDbs(false);
            return .{
                .main = try collectMainOwnedEntries(allocator, txn, dbs.main),
                .docs = try collectOwnedEntries(allocator, txn, dbs.docs),
                .dups = try collectOwnedEntries(allocator, txn, dbs.dups),
            };
        }

        fn expectOwnedEntrySlicesEqual(expected: []const OwnedEntry, actual: []const OwnedEntry) !void {
            try std.testing.expectEqual(expected.len, actual.len);
            for (expected, actual) |expected_entry, actual_entry| {
                try std.testing.expectEqualStrings(expected_entry.key, actual_entry.key);
                try std.testing.expectEqualStrings(expected_entry.value, actual_entry.value);
            }
        }

        fn snapshotsEqual(expected: *const Snapshot, actual: *const Snapshot) bool {
            return ownedEntrySlicesEqual(expected.main, actual.main) and
                ownedEntrySlicesEqual(expected.docs, actual.docs) and
                ownedEntrySlicesEqual(expected.dups, actual.dups);
        }

        fn ownedEntrySlicesEqual(expected: []const OwnedEntry, actual: []const OwnedEntry) bool {
            if (expected.len != actual.len) return false;
            for (expected, actual) |expected_entry, actual_entry| {
                if (!std.mem.eql(u8, expected_entry.key, actual_entry.key)) return false;
                if (!std.mem.eql(u8, expected_entry.value, actual_entry.value)) return false;
            }
            return true;
        }

        fn expectSnapshotsEqual(expected: *const Snapshot, actual: *const Snapshot) !void {
            try expectOwnedEntrySlicesEqual(expected.main, actual.main);
            try expectOwnedEntrySlicesEqual(expected.docs, actual.docs);
            try expectOwnedEntrySlicesEqual(expected.dups, actual.dups);
        }

        fn snapshotSummary(snapshot: *const Snapshot) SnapshotSummary {
            return .{
                .main_count = snapshot.main.len,
                .docs_count = snapshot.docs.len,
                .dups_count = snapshot.dups.len,
            };
        }

        fn initializeDifferentialSchema(path: [*:0]const u8, backend: DifferentialBackend, base_opts: EnvironmentOptions) Error!void {
            var env = try DifferentialEnvironment.open(path, base_opts, backend);
            defer env.close();

            var txn = try env.begin(.{});
            errdefer txn.abort();
            _ = try txn.openKnownDbs(true);
            try txn.commit();
        }

        fn applyCommittedAction(path: [*:0]const u8, backend: DifferentialBackend, base_opts: EnvironmentOptions, action: DifferentialAction) Error!void {
            var env = try DifferentialEnvironment.open(path, base_opts, backend);
            defer env.close();

            var txn = try env.begin(.{});
            errdefer txn.abort();

            const dbs = try txn.openKnownDbs(true);
            try applyDifferentialAction(&txn, dbs, action);
            try txn.commit();
        }

        fn applyCrashPublishedAction(
            path: [*:0]const u8,
            base_opts: EnvironmentOptions,
            action: DifferentialAction,
            phase: zig_lmdb.commit_support.CommitPublishPhase,
        ) Error!void {
            var env = try DifferentialEnvironment.open(path, base_opts, .zig);
            defer env.close();

            var txn = try env.begin(.{});
            errdefer txn.abort();

            const dbs = try txn.openKnownDbs(true);
            try applyDifferentialAction(&txn, dbs, action);
            try txn.publishCommitPhaseForTest(phase);
            txn.abort();
        }

        fn applyNestedAction(
            path: [*:0]const u8,
            backend: DifferentialBackend,
            base_opts: EnvironmentOptions,
            parent_action: DifferentialAction,
            child_action: DifferentialAction,
            child_commits: bool,
        ) Error!void {
            var env = try DifferentialEnvironment.open(path, base_opts, backend);
            defer env.close();

            var parent = try env.begin(.{});
            errdefer parent.abort();

            const parent_dbs = try parent.openKnownDbs(true);
            try applyDifferentialAction(&parent, parent_dbs, parent_action);

            var child = try parent.beginChild();
            var child_finished = false;
            defer if (!child_finished) child.abort();

            const child_dbs = try child.openKnownDbs(true);
            try applyDifferentialAction(&child, child_dbs, child_action);
            if (child_commits) {
                try child.commit();
                child_finished = true;
            } else {
                child.abort();
                child_finished = true;
            }

            try parent.commit();
        }

        fn verifyReaderSnapshotIsolation(
            allocator: std.mem.Allocator,
            path: [*:0]const u8,
            backend: DifferentialBackend,
            base_opts: EnvironmentOptions,
            before: *const Snapshot,
            action: DifferentialAction,
        ) anyerror!void {
            var reader_opts = base_opts;
            reader_opts.read_only = true;

            var reader_env = try DifferentialEnvironment.open(path, reader_opts, backend);
            defer reader_env.close();

            var reader_txn = try reader_env.begin(.{ .read_only = true });
            defer reader_txn.abort();

            var reader_before = try takeTransactionSnapshot(allocator, &reader_txn);
            defer reader_before.deinit(allocator);
            try expectSnapshotsEqual(before, &reader_before);

            try applyCommittedAction(path, backend, base_opts, action);

            var reader_after = try takeTransactionSnapshot(allocator, &reader_txn);
            defer reader_after.deinit(allocator);
            try expectSnapshotsEqual(before, &reader_after);
        }

        fn applyDifferentialAction(txn: *DifferentialTransaction, dbs: DifferentialDbHandles, action: DifferentialAction) Error!void {
            var key_buf: [32]u8 = undefined;
            var value_buf: [32]u8 = undefined;

            switch (action) {
                .put_main => |payload| try txn.put(dbs.main, formatMainKey(&key_buf, payload.key_index), formatMainValue(&value_buf, payload.value_index)),
                .delete_main => |payload| txn.delete(dbs.main, formatMainKey(&key_buf, payload.key_index)) catch |err| switch (err) {
                    Error.NotFound => {},
                    else => return err,
                },
                .put_docs => |payload| try txn.put(dbs.docs, formatDocsKey(&key_buf, payload.key_index), formatDocsValue(&value_buf, payload.value_index)),
                .delete_docs => |payload| txn.delete(dbs.docs, formatDocsKey(&key_buf, payload.key_index)) catch |err| switch (err) {
                    Error.NotFound => {},
                    else => return err,
                },
                .put_dup => |payload| try txn.put(dbs.dups, formatDupKey(&key_buf, payload.key_index), formatDupValue(&value_buf, payload.value_index)),
                .delete_dup_value => |payload| txn.deleteValue(dbs.dups, formatDupKey(&key_buf, payload.key_index), formatDupValue(&value_buf, payload.value_index)) catch |err| switch (err) {
                    Error.NotFound => {},
                    else => return err,
                },
            }
        }

        fn formatMainKey(buf: []u8, key_index: u8) []const u8 {
            return std.fmt.bufPrint(buf, "m-{d:0>2}", .{key_index}) catch unreachable;
        }

        fn formatDocsKey(buf: []u8, key_index: u8) []const u8 {
            return std.fmt.bufPrint(buf, "d-{d:0>2}", .{key_index}) catch unreachable;
        }

        fn formatDupKey(buf: []u8, key_index: u8) []const u8 {
            return std.fmt.bufPrint(buf, "dup-{d:0>2}", .{key_index}) catch unreachable;
        }

        fn formatMainValue(buf: []u8, value_index: u16) []const u8 {
            return std.fmt.bufPrint(buf, "main-{d:0>4}", .{value_index}) catch unreachable;
        }

        fn formatDocsValue(buf: []u8, value_index: u16) []const u8 {
            return std.fmt.bufPrint(buf, "docs-{d:0>4}", .{value_index}) catch unreachable;
        }

        fn formatDupValue(buf: []u8, value_index: u16) []const u8 {
            return std.fmt.bufPrint(buf, "dupv-{d:0>4}", .{value_index}) catch unreachable;
        }

        fn parseFixedIndex(value: []const u8, prefix: []const u8) u16 {
            std.debug.assert(std.mem.startsWith(u8, value, prefix));
            return std.fmt.parseUnsigned(u16, value[prefix.len..], 10) catch unreachable;
        }

        fn parseMainKeyIndex(value: []const u8) u8 {
            return @intCast(parseFixedIndex(value, "m-"));
        }

        fn parseDocsKeyIndex(value: []const u8) u8 {
            return @intCast(parseFixedIndex(value, "d-"));
        }

        fn parseDupKeyIndex(value: []const u8) u8 {
            return @intCast(parseFixedIndex(value, "dup-"));
        }

        fn parseDupValueIndex(value: []const u8) u16 {
            return parseFixedIndex(value, "dupv-");
        }

        fn nextDifferentialAction(random: std.Random, step: usize, snapshot: *const Snapshot) DifferentialAction {
            const put_value_index: u16 = @intCast((step * 97 + random.uintLessThan(u16, 700)) % 10_000);
            const put_key_index = random.uintLessThan(u8, 6);

            const choice = random.uintLessThan(u8, 6);
            switch (choice) {
                0 => return .{ .put_main = .{ .key_index = put_key_index, .value_index = put_value_index } },
                1 => return .{ .put_docs = .{ .key_index = put_key_index, .value_index = put_value_index } },
                2 => return .{ .put_dup = .{ .key_index = put_key_index, .value_index = put_value_index } },
                3 => {
                    if (snapshot.main.len == 0) return .{ .put_main = .{ .key_index = put_key_index, .value_index = put_value_index } };
                    const entry = snapshot.main[random.uintLessThan(usize, snapshot.main.len)];
                    return .{ .delete_main = .{ .key_index = parseMainKeyIndex(entry.key) } };
                },
                4 => {
                    if (snapshot.docs.len == 0) return .{ .put_docs = .{ .key_index = put_key_index, .value_index = put_value_index } };
                    const entry = snapshot.docs[random.uintLessThan(usize, snapshot.docs.len)];
                    return .{ .delete_docs = .{ .key_index = parseDocsKeyIndex(entry.key) } };
                },
                else => {
                    if (snapshot.dups.len == 0) return .{ .put_dup = .{ .key_index = put_key_index, .value_index = put_value_index } };
                    const entry = snapshot.dups[random.uintLessThan(usize, snapshot.dups.len)];
                    return .{ .delete_dup_value = .{ .key_index = parseDupKeyIndex(entry.key), .value_index = parseDupValueIndex(entry.value) } };
                },
            }
        }

        fn nextScheduledAction(
            random: std.Random,
            step: usize,
            snapshot: *const Snapshot,
            allow_nested: bool,
            allow_reader: bool,
        ) ScheduledAction {
            if (!allow_nested) {
                if (allow_reader and random.uintLessThan(u8, 5) == 0) {
                    return .{ .reader_then_direct = nextDifferentialAction(random, step, snapshot) };
                }
                return .{ .direct = nextDifferentialAction(random, step, snapshot) };
            }

            const kind = random.uintLessThan(u8, 10);
            if (kind < 5) return .{ .direct = nextDifferentialAction(random, step, snapshot) };
            if (kind < 7) {
                return .{ .nested_commit = .{
                    .parent = nextDifferentialAction(random, step * 2, snapshot),
                    .child = nextDifferentialAction(random, step * 2 + 1, snapshot),
                } };
            }
            if (kind < 9) {
                return .{ .nested_abort = .{
                    .parent = nextDifferentialAction(random, step * 2, snapshot),
                    .child = nextDifferentialAction(random, step * 2 + 1, snapshot),
                } };
            }
            if (allow_reader) return .{ .reader_then_direct = nextDifferentialAction(random, step, snapshot) };
            return .{ .direct = nextDifferentialAction(random, step, snapshot) };
        }

        fn replayScheduledWorkload(
            allocator: std.mem.Allocator,
            env_opts: EnvironmentOptions,
            actions: []const ScheduledAction,
        ) anyerror!SnapshotSummary {
            var c_path_buf: [256]u8 = undefined;
            const c_path = tmpPathWithSuffix(&c_path_buf, "replay-c");
            defer cleanupTmp(c_path);

            var zig_path_buf: [256]u8 = undefined;
            const zig_path = tmpPathWithSuffix(&zig_path_buf, "replay-z");
            defer cleanupTmp(zig_path);

            try initializeDifferentialSchema(c_path, .c, env_opts);
            try initializeDifferentialSchema(zig_path, .zig, env_opts);

            var current_snapshot = try takeDifferentialSnapshot(allocator, c_path, .c, env_opts);
            defer current_snapshot.deinit(allocator);

            var initial_zig_snapshot = try takeDifferentialSnapshot(allocator, zig_path, .zig, env_opts);
            defer initial_zig_snapshot.deinit(allocator);
            try expectSnapshotsEqual(&current_snapshot, &initial_zig_snapshot);

            for (actions) |action| {
                switch (action) {
                    .direct => |direct_action| {
                        try applyCommittedAction(c_path, .c, env_opts, direct_action);
                        try applyCommittedAction(zig_path, .zig, env_opts, direct_action);
                    },
                    .nested_commit => |nested| {
                        try applyNestedAction(c_path, .c, env_opts, nested.parent, nested.child, true);
                        try applyNestedAction(zig_path, .zig, env_opts, nested.parent, nested.child, true);
                    },
                    .nested_abort => |nested| {
                        try applyNestedAction(c_path, .c, env_opts, nested.parent, nested.child, false);
                        try applyNestedAction(zig_path, .zig, env_opts, nested.parent, nested.child, false);
                    },
                    .reader_then_direct => |direct_action| {
                        try verifyReaderSnapshotIsolation(allocator, c_path, .c, env_opts, &current_snapshot, direct_action);
                        try verifyReaderSnapshotIsolation(allocator, zig_path, .zig, env_opts, &current_snapshot, direct_action);
                    },
                }

                var next_c_snapshot = try takeDifferentialSnapshot(allocator, c_path, .c, env_opts);
                errdefer next_c_snapshot.deinit(allocator);
                var next_zig_snapshot = try takeDifferentialSnapshot(allocator, zig_path, .zig, env_opts);
                defer next_zig_snapshot.deinit(allocator);

                try expectSnapshotsEqual(&next_c_snapshot, &next_zig_snapshot);

                current_snapshot.deinit(allocator);
                current_snapshot = next_c_snapshot;
            }

            return snapshotSummary(&current_snapshot);
        }

        fn reportReducedSchedule(
            allocator: std.mem.Allocator,
            env_opts: EnvironmentOptions,
            case_label: []const u8,
            seed: u64,
            actions: []const ScheduledAction,
        ) !void {
            const Replayer = struct {
                allocator: std.mem.Allocator,
                opts: EnvironmentOptions,

                pub fn replay(self: @This(), candidate: []const ScheduledAction) anyerror!void {
                    _ = try replayScheduledWorkload(self.allocator, self.opts, candidate);
                }
            };

            const reduced = try zig_lmdb.sim.reduceFailingSequence(
                ScheduledAction,
                allocator,
                actions,
                Replayer{ .allocator = allocator, .opts = env_opts },
            );
            defer allocator.free(reduced);

            const summary = replayScheduledWorkload(allocator, env_opts, reduced) catch |err| {
                std.debug.print("failed to recompute LMDB replay summary for {s}: {s}\n", .{ case_label, @errorName(err) });
                return;
            };

            const artifact_path = writeReplayFixtureArtifact(
                allocator,
                env_opts,
                case_label,
                seed,
                "expected C and Zig snapshots to stay aligned across reopen cycles",
                summary,
                reduced,
            ) catch |err| blk: {
                std.debug.print("failed to write LMDB replay artifact for {s}: {s}\n", .{ case_label, @errorName(err) });
                break :blk null;
            };
            defer if (artifact_path) |path| allocator.free(path);

            std.debug.print("reduced failing LMDB schedule ({d} actions):\n", .{reduced.len});
            if (artifact_path) |path| {
                std.debug.print("replay fixture: {s}\n", .{path});
            }
            std.debug.print("{s}", .{"const replay = [_]ScheduledAction{\n"});
            for (reduced) |action| {
                std.debug.print("    ", .{});
                try printScheduledActionLiteral(action);
                std.debug.print(",\n", .{});
            }
            std.debug.print("{s}", .{"};\n"});
        }

        fn replayFixtureFile(allocator: std.mem.Allocator, name: []const u8) !void {
            const path = try std.fmt.allocPrint(allocator, "pkg/antfly/src/storage/lmdb_sim_fixtures/{s}", .{name});
            defer allocator.free(path);

            const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, allocator, .limited(64 * 1024));
            defer allocator.free(contents);

            var fixture = try lmdb_sim_fixture.parseFixture(allocator, contents);
            defer fixture.deinit(allocator);

            switch (fixture.mode) {
                .differential => {
                    const summary = try replayScheduledWorkload(allocator, envOptionsFromFixtureOptions(fixture.opts), fixture.actions);
                    try expectReplayFixtureSummary(
                        fixture.case_label orelse fixture.label orelse name,
                        fixture.opts,
                        summary,
                    );
                },
                .crash => {
                    const outcome = try replayCrashWorkload(
                        allocator,
                        envOptionsFromFixtureOptions(fixture.opts),
                        fixture.prelude_actions,
                        fixture.crash_action orelse return error.InvalidFixture,
                        phaseFromFixturePhase(fixture.phase orelse return error.InvalidFixture),
                    );
                    try expectReplayFixtureOutcome(
                        fixture.case_label orelse fixture.label orelse name,
                        fixture.opts,
                        outcome,
                    );
                },
            }
        }

        fn fixtureOptionsFromEnvOptions(env_opts: EnvironmentOptions) lmdb_sim_fixture.Options {
            return .{
                .max_dbs = env_opts.max_dbs,
                .write_map = env_opts.write_map,
                .map_async = env_opts.map_async,
                .fixed_map = env_opts.fixed_map,
                .no_sync = env_opts.no_sync,
                .no_meta_sync = env_opts.no_meta_sync,
                .commit_backend = switch (env_opts.commit_backend) {
                    .sync => .sync,
                    .worker_thread => .worker_thread,
                    .async_io => .async_io,
                    .adaptive => .adaptive,
                },
            };
        }

        fn envOptionsFromFixtureOptions(opts: lmdb_sim_fixture.Options) EnvironmentOptions {
            return .{
                .max_dbs = opts.max_dbs,
                .write_map = opts.write_map,
                .map_async = opts.map_async,
                .fixed_map = opts.fixed_map,
                .no_sync = opts.no_sync,
                .no_meta_sync = opts.no_meta_sync,
                .commit_backend = switch (opts.commit_backend) {
                    .sync => .sync,
                    .worker_thread => .worker_thread,
                    .async_io => .async_io,
                    .adaptive => .adaptive,
                },
            };
        }

        fn fixturePhaseFromCommitPhase(phase: zig_lmdb.commit_support.CommitPublishPhase) lmdb_sim_fixture.CommitPhase {
            return switch (phase) {
                .before_data_sync => .before_data_sync,
                .after_data_sync_before_meta => .after_data_sync_before_meta,
                .after_meta_write_before_meta_sync => .after_meta_write_before_meta_sync,
                .fully_published => .fully_published,
            };
        }

        fn phaseFromFixturePhase(phase: lmdb_sim_fixture.CommitPhase) zig_lmdb.commit_support.CommitPublishPhase {
            return switch (phase) {
                .before_data_sync => .before_data_sync,
                .after_data_sync_before_meta => .after_data_sync_before_meta,
                .after_meta_write_before_meta_sync => .after_meta_write_before_meta_sync,
                .fully_published => .fully_published,
            };
        }

        fn expectReplayFixtureSummary(
            fixture_name: []const u8,
            opts: lmdb_sim_fixture.Options,
            summary: SnapshotSummary,
        ) !void {
            if (opts.expected_main_count) |expected| {
                try sim_fixture.expectFieldEqual(fixture_name, "expected_main_count", expected, summary.main_count);
            }
            if (opts.expected_docs_count) |expected| {
                try sim_fixture.expectFieldEqual(fixture_name, "expected_docs_count", expected, summary.docs_count);
            }
            if (opts.expected_dups_count) |expected| {
                try sim_fixture.expectFieldEqual(fixture_name, "expected_dups_count", expected, summary.dups_count);
            }
        }

        fn expectReplayFixtureOutcome(
            fixture_name: []const u8,
            opts: lmdb_sim_fixture.Options,
            outcome: CrashOutcome,
        ) !void {
            if (opts.expected_outcome) |expected| {
                try sim_fixture.expectFieldEqual(fixture_name, "expected_outcome", expected, outcome);
            }
        }

        fn writeReplayFixtureArtifact(
            allocator: std.mem.Allocator,
            env_opts: EnvironmentOptions,
            case_label: []const u8,
            seed: u64,
            expectation_note: []const u8,
            summary: SnapshotSummary,
            actions: []const ScheduledAction,
        ) !?[]u8 {
            var path_buf: [256]u8 = undefined;
            const artifact_path = replayArtifactPath(&path_buf, case_label);
            const path = try allocator.dupe(u8, artifact_path);
            errdefer allocator.free(path);

            const normalized = try lmdb_sim_fixture.renderDifferentialArtifact(
                allocator,
                blk: {
                    var opts = fixtureOptionsFromEnvOptions(env_opts);
                    opts.expected_main_count = summary.main_count;
                    opts.expected_docs_count = summary.docs_count;
                    opts.expected_dups_count = summary.dups_count;
                    break :blk opts;
                },
                case_label,
                seed,
                expectation_note,
                actions,
            );
            defer allocator.free(normalized);

            try writeReplayArtifactFile(path, normalized);

            return path;
        }

        fn writeCrashReplayFixtureArtifact(
            allocator: std.mem.Allocator,
            env_opts: EnvironmentOptions,
            case_label: []const u8,
            seed: u64,
            phase: zig_lmdb.commit_support.CommitPublishPhase,
            expectation_note: []const u8,
            expected_outcome: CrashOutcome,
            prelude_actions: []const DifferentialAction,
            crash_action: DifferentialAction,
        ) !?[]u8 {
            var path_buf: [256]u8 = undefined;
            const artifact_path = replayArtifactPath(&path_buf, case_label);
            const path = try allocator.dupe(u8, artifact_path);
            errdefer allocator.free(path);

            const normalized = try lmdb_sim_fixture.renderCrashArtifact(
                allocator,
                blk: {
                    var opts = fixtureOptionsFromEnvOptions(env_opts);
                    opts.expected_outcome = expected_outcome;
                    break :blk opts;
                },
                case_label,
                seed,
                fixturePhaseFromCommitPhase(phase),
                expectation_note,
                prelude_actions,
                crash_action,
            );
            defer allocator.free(normalized);

            try writeReplayArtifactFile(path, normalized);

            return path;
        }

        fn writeReplayArtifactFile(
            path: []const u8,
            normalized: []const u8,
        ) !void {
            var file = try std.Io.Dir.createFileAbsolute(std.testing.io, path, .{});
            defer file.close(std.testing.io);

            var file_buf: [4096]u8 = undefined;
            var writer = file.writer(std.testing.io, &file_buf);
            try writer.interface.writeAll(normalized);
            try writer.end();
        }

        fn replayCrashWorkload(
            allocator: std.mem.Allocator,
            env_opts: EnvironmentOptions,
            prelude_actions: []const DifferentialAction,
            crash_action: DifferentialAction,
            phase: zig_lmdb.commit_support.CommitPublishPhase,
        ) anyerror!CrashOutcome {
            var c_path_buf: [256]u8 = undefined;
            const c_path = tmpPathWithSuffix(&c_path_buf, "replay-crash-c");
            defer cleanupTmp(c_path);

            var zig_path_buf: [256]u8 = undefined;
            const zig_path = tmpPathWithSuffix(&zig_path_buf, "replay-crash-z");
            defer cleanupTmp(zig_path);

            try initializeDifferentialSchema(c_path, .c, env_opts);
            try initializeDifferentialSchema(zig_path, .zig, env_opts);

            var current_snapshot = try takeDifferentialSnapshot(allocator, c_path, .c, env_opts);
            defer current_snapshot.deinit(allocator);

            var initial_zig_snapshot = try takeDifferentialSnapshot(allocator, zig_path, .zig, env_opts);
            defer initial_zig_snapshot.deinit(allocator);
            try expectSnapshotsEqual(&current_snapshot, &initial_zig_snapshot);

            for (prelude_actions) |action| {
                try applyCommittedAction(c_path, .c, env_opts, action);
                try applyCommittedAction(zig_path, .zig, env_opts, action);

                var next_c_snapshot = try takeDifferentialSnapshot(allocator, c_path, .c, env_opts);
                errdefer next_c_snapshot.deinit(allocator);
                var next_zig_snapshot = try takeDifferentialSnapshot(allocator, zig_path, .zig, env_opts);
                defer next_zig_snapshot.deinit(allocator);
                try expectSnapshotsEqual(&next_c_snapshot, &next_zig_snapshot);

                current_snapshot.deinit(allocator);
                current_snapshot = next_c_snapshot;
            }

            var before_snapshot = try takeDifferentialSnapshot(allocator, c_path, .c, env_opts);
            defer before_snapshot.deinit(allocator);

            try applyCommittedAction(c_path, .c, env_opts, crash_action);

            var after_snapshot = try takeDifferentialSnapshot(allocator, c_path, .c, env_opts);
            defer after_snapshot.deinit(allocator);

            try applyCrashPublishedAction(zig_path, env_opts, crash_action, phase);

            var zig_reopened_snapshot = try takeDifferentialSnapshot(allocator, zig_path, .zig, env_opts);
            defer zig_reopened_snapshot.deinit(allocator);

            try expectCrashSnapshot(&before_snapshot, &after_snapshot, &zig_reopened_snapshot, phase);
            return switch (phase) {
                .before_data_sync, .after_data_sync_before_meta => .previous,
                .after_meta_write_before_meta_sync => .previous_or_committed,
                .fully_published => .committed,
            };
        }

        fn reportCrashFixture(
            allocator: std.mem.Allocator,
            env_opts: EnvironmentOptions,
            case_label: []const u8,
            seed: u64,
            phase: zig_lmdb.commit_support.CommitPublishPhase,
            prelude_actions: []const DifferentialAction,
            crash_action: DifferentialAction,
        ) !void {
            const Replayer = struct {
                allocator: std.mem.Allocator,
                opts: EnvironmentOptions,
                phase: zig_lmdb.commit_support.CommitPublishPhase,
                crash_action: DifferentialAction,

                pub fn replay(self: @This(), candidate: []const DifferentialAction) anyerror!void {
                    _ = try replayCrashWorkload(self.allocator, self.opts, candidate, self.crash_action, self.phase);
                }
            };

            const reduced = try zig_lmdb.sim.reduceFailingSequence(
                DifferentialAction,
                allocator,
                prelude_actions,
                Replayer{
                    .allocator = allocator,
                    .opts = env_opts,
                    .phase = phase,
                    .crash_action = crash_action,
                },
            );
            defer allocator.free(reduced);

            const artifact_path = writeCrashReplayFixtureArtifact(
                allocator,
                env_opts,
                case_label,
                seed,
                phase,
                expectationNoteForCrashPhase(phase),
                switch (phase) {
                    .before_data_sync, .after_data_sync_before_meta => .previous,
                    .after_meta_write_before_meta_sync => .previous_or_committed,
                    .fully_published => .committed,
                },
                reduced,
                crash_action,
            ) catch |err| blk: {
                std.debug.print("failed to write LMDB crash replay artifact for {s}: {s}\n", .{ case_label, @errorName(err) });
                break :blk null;
            };
            defer if (artifact_path) |path| allocator.free(path);

            std.debug.print("reduced failing LMDB crash prelude ({d} actions):\n", .{reduced.len});
            if (artifact_path) |path| {
                std.debug.print("replay fixture: {s}\n", .{path});
            }
        }

        fn expectationNoteForCrashPhase(phase: zig_lmdb.commit_support.CommitPublishPhase) []const u8 {
            return switch (phase) {
                .before_data_sync, .after_data_sync_before_meta => "expected reopen to preserve the previous committed snapshot",
                .after_meta_write_before_meta_sync => "expected reopen to match either the previous or newly committed snapshot",
                .fully_published => "expected reopen to preserve the newly committed snapshot",
            };
        }

        fn printScheduledActionLiteral(action: ScheduledAction) !void {
            switch (action) {
                .direct => |direct_action| {
                    std.debug.print("{s}", .{".{ .direct = "});
                    try printDifferentialActionLiteral(direct_action);
                    std.debug.print("{s}", .{" }"});
                },
                .nested_commit => |nested| {
                    std.debug.print("{s}", .{".{ .nested_commit = .{ .parent = "});
                    try printDifferentialActionLiteral(nested.parent);
                    std.debug.print("{s}", .{", .child = "});
                    try printDifferentialActionLiteral(nested.child);
                    std.debug.print("{s}", .{" } }"});
                },
                .nested_abort => |nested| {
                    std.debug.print("{s}", .{".{ .nested_abort = .{ .parent = "});
                    try printDifferentialActionLiteral(nested.parent);
                    std.debug.print("{s}", .{", .child = "});
                    try printDifferentialActionLiteral(nested.child);
                    std.debug.print("{s}", .{" } }"});
                },
                .reader_then_direct => |direct_action| {
                    std.debug.print("{s}", .{".{ .reader_then_direct = "});
                    try printDifferentialActionLiteral(direct_action);
                    std.debug.print("{s}", .{" }"});
                },
            }
        }

        fn printDifferentialActionLiteral(action: DifferentialAction) !void {
            switch (action) {
                .put_main => |payload| std.debug.print("{s}{d}{s}{d}{s}", .{ ".{ .put_main = .{ .key_index = ", payload.key_index, ", .value_index = ", payload.value_index, " } }" }),
                .delete_main => |payload| std.debug.print("{s}{d}{s}", .{ ".{ .delete_main = .{ .key_index = ", payload.key_index, " } }" }),
                .put_docs => |payload| std.debug.print("{s}{d}{s}{d}{s}", .{ ".{ .put_docs = .{ .key_index = ", payload.key_index, ", .value_index = ", payload.value_index, " } }" }),
                .delete_docs => |payload| std.debug.print("{s}{d}{s}", .{ ".{ .delete_docs = .{ .key_index = ", payload.key_index, " } }" }),
                .put_dup => |payload| std.debug.print("{s}{d}{s}{d}{s}", .{ ".{ .put_dup = .{ .key_index = ", payload.key_index, ", .value_index = ", payload.value_index, " } }" }),
                .delete_dup_value => |payload| std.debug.print("{s}{d}{s}{d}{s}", .{ ".{ .delete_dup_value = .{ .key_index = ", payload.key_index, ", .value_index = ", payload.value_index, " } }" }),
            }
        }

        fn crashActionForCase(case_index: usize) DifferentialAction {
            const value_index: u16 = @intCast(8_000 + case_index);
            return switch (case_index % 3) {
                0 => .{ .put_main = .{ .key_index = 1, .value_index = value_index } },
                1 => .{ .put_docs = .{ .key_index = 2, .value_index = value_index } },
                else => .{ .put_dup = .{ .key_index = 3, .value_index = value_index } },
            };
        }

        fn expectCrashSnapshot(
            before: *const Snapshot,
            after: *const Snapshot,
            actual: *const Snapshot,
            phase: zig_lmdb.commit_support.CommitPublishPhase,
        ) !void {
            switch (phase) {
                .before_data_sync, .after_data_sync_before_meta => try expectSnapshotsEqual(before, actual),
                .after_meta_write_before_meta_sync => {
                    if (!snapshotsEqual(before, actual) and !snapshotsEqual(after, actual)) {
                        try expectSnapshotsEqual(after, actual);
                    }
                },
                .fully_published => try expectSnapshotsEqual(after, actual),
            }
        }

        fn runDifferentialScheduleCase(
            allocator: std.mem.Allocator,
            base_opts: EnvironmentOptions,
            seed: u64,
            steps: usize,
            case_label: []const u8,
        ) anyerror!void {
            var prng = std.Random.DefaultPrng.init(seed);
            const random = prng.random();
            var schedule: std.ArrayListUnmanaged(ScheduledAction) = .empty;
            defer schedule.deinit(allocator);

            var c_case_buf: [64]u8 = undefined;
            const c_case = std.fmt.bufPrint(&c_case_buf, "{s}-c", .{case_label}) catch unreachable;
            var c_path_buf: [256]u8 = undefined;
            const c_path = tmpPathWithSuffix(&c_path_buf, c_case);
            defer cleanupTmp(c_path);

            var zig_case_buf: [64]u8 = undefined;
            const zig_case = std.fmt.bufPrint(&zig_case_buf, "{s}-zig", .{case_label}) catch unreachable;
            var zig_path_buf: [256]u8 = undefined;
            const zig_path = tmpPathWithSuffix(&zig_path_buf, zig_case);
            defer cleanupTmp(zig_path);

            var env_opts = base_opts;
            env_opts.max_dbs = @max(env_opts.max_dbs, 4);
            const allow_nested = !env_opts.write_map;
            const allow_reader = !env_opts.write_map and !env_opts.fixed_map;

            try initializeDifferentialSchema(c_path, .c, env_opts);
            try initializeDifferentialSchema(zig_path, .zig, env_opts);

            var current_snapshot = try takeDifferentialSnapshot(allocator, c_path, .c, env_opts);
            defer current_snapshot.deinit(allocator);

            var initial_zig_snapshot = try takeDifferentialSnapshot(allocator, zig_path, .zig, env_opts);
            defer initial_zig_snapshot.deinit(allocator);
            try expectSnapshotsEqual(&current_snapshot, &initial_zig_snapshot);

            for (0..steps) |step| {
                const action = nextScheduledAction(random, step, &current_snapshot, allow_nested, allow_reader);
                try schedule.append(allocator, action);

                const step_result: anyerror!void = switch (action) {
                    .direct => |direct_action| {
                        try applyCommittedAction(c_path, .c, env_opts, direct_action);
                        try applyCommittedAction(zig_path, .zig, env_opts, direct_action);
                    },
                    .nested_commit => |nested| {
                        try applyNestedAction(c_path, .c, env_opts, nested.parent, nested.child, true);
                        try applyNestedAction(zig_path, .zig, env_opts, nested.parent, nested.child, true);
                    },
                    .nested_abort => |nested| {
                        try applyNestedAction(c_path, .c, env_opts, nested.parent, nested.child, false);
                        try applyNestedAction(zig_path, .zig, env_opts, nested.parent, nested.child, false);
                    },
                    .reader_then_direct => |direct_action| {
                        try verifyReaderSnapshotIsolation(allocator, c_path, .c, env_opts, &current_snapshot, direct_action);
                        try verifyReaderSnapshotIsolation(allocator, zig_path, .zig, env_opts, &current_snapshot, direct_action);
                    },
                };
                step_result catch |err| {
                    reportReducedSchedule(allocator, env_opts, case_label, seed, schedule.items) catch {};
                    return err;
                };

                var next_c_snapshot = takeDifferentialSnapshot(allocator, c_path, .c, env_opts) catch |err| {
                    reportReducedSchedule(allocator, env_opts, case_label, seed, schedule.items) catch {};
                    return err;
                };
                errdefer next_c_snapshot.deinit(allocator);

                var next_zig_snapshot = takeDifferentialSnapshot(allocator, zig_path, .zig, env_opts) catch |err| {
                    reportReducedSchedule(allocator, env_opts, case_label, seed, schedule.items) catch {};
                    return err;
                };
                defer next_zig_snapshot.deinit(allocator);

                expectSnapshotsEqual(&next_c_snapshot, &next_zig_snapshot) catch |err| {
                    reportReducedSchedule(allocator, env_opts, case_label, seed, schedule.items) catch {};
                    return err;
                };

                current_snapshot.deinit(allocator);
                current_snapshot = next_c_snapshot;
            }
        }

        fn runDifferentialScheduleCaseOrSkip(
            allocator: std.mem.Allocator,
            base_opts: EnvironmentOptions,
            seed: u64,
            steps: usize,
            case_label: []const u8,
        ) !void {
            runDifferentialScheduleCase(allocator, base_opts, seed, steps, case_label) catch |err| switch (err) {
                Error.Incompatible => if (base_opts.fixed_map) return else return err,
                Error.LmdbUnexpected => if (base_opts.fixed_map) return else return err,
                else => return err,
            };
        }

        fn runCrashPhaseCase(allocator: std.mem.Allocator, base_opts: EnvironmentOptions, case_label: []const u8) anyerror!void {
            const phases = [_]zig_lmdb.commit_support.CommitPublishPhase{
                .before_data_sync,
                .after_data_sync_before_meta,
                .after_meta_write_before_meta_sync,
            };

            for (phases, 0..) |phase, phase_index| {
                var c_case_buf: [64]u8 = undefined;
                const c_case = std.fmt.bufPrint(&c_case_buf, "{s}-{d}-c", .{ case_label, phase_index }) catch unreachable;
                var c_path_buf: [256]u8 = undefined;
                const c_path = tmpPathWithSuffix(&c_path_buf, c_case);
                defer cleanupTmp(c_path);

                var zig_case_buf: [64]u8 = undefined;
                const zig_case = std.fmt.bufPrint(&zig_case_buf, "{s}-{d}-zig", .{ case_label, phase_index }) catch unreachable;
                var zig_path_buf: [256]u8 = undefined;
                const zig_path = tmpPathWithSuffix(&zig_path_buf, zig_case);
                defer cleanupTmp(zig_path);

                var env_opts = base_opts;
                env_opts.max_dbs = @max(env_opts.max_dbs, 4);

                try initializeDifferentialSchema(c_path, .c, env_opts);
                try initializeDifferentialSchema(zig_path, .zig, env_opts);

                var current_snapshot = try takeDifferentialSnapshot(allocator, c_path, .c, env_opts);
                defer current_snapshot.deinit(allocator);

                const prelude_seed = 0xBEEF_0000 + phase_index + (@as(u64, case_label.len) << 8);
                var prng = std.Random.DefaultPrng.init(prelude_seed);
                const random = prng.random();
                const crash_action = crashActionForCase(phase_index + case_label.len);
                var prelude_actions: std.ArrayListUnmanaged(DifferentialAction) = .empty;
                defer prelude_actions.deinit(allocator);

                for (0..8) |step| {
                    const action = nextDifferentialAction(random, step, &current_snapshot);
                    try prelude_actions.append(allocator, action);
                    applyCommittedAction(c_path, .c, env_opts, action) catch |err| {
                        reportCrashFixture(allocator, env_opts, case_label, prelude_seed, phase, prelude_actions.items, crash_action) catch {};
                        return err;
                    };
                    applyCommittedAction(zig_path, .zig, env_opts, action) catch |err| {
                        reportCrashFixture(allocator, env_opts, case_label, prelude_seed, phase, prelude_actions.items, crash_action) catch {};
                        return err;
                    };

                    var next_snapshot = takeDifferentialSnapshot(allocator, c_path, .c, env_opts) catch |err| {
                        reportCrashFixture(allocator, env_opts, case_label, prelude_seed, phase, prelude_actions.items, crash_action) catch {};
                        return err;
                    };
                    errdefer next_snapshot.deinit(allocator);
                    var next_zig_snapshot = takeDifferentialSnapshot(allocator, zig_path, .zig, env_opts) catch |err| {
                        reportCrashFixture(allocator, env_opts, case_label, prelude_seed, phase, prelude_actions.items, crash_action) catch {};
                        return err;
                    };
                    defer next_zig_snapshot.deinit(allocator);
                    expectSnapshotsEqual(&next_snapshot, &next_zig_snapshot) catch |err| {
                        reportCrashFixture(allocator, env_opts, case_label, prelude_seed, phase, prelude_actions.items, crash_action) catch {};
                        return err;
                    };

                    current_snapshot.deinit(allocator);
                    current_snapshot = next_snapshot;
                }

                var before_snapshot = takeDifferentialSnapshot(allocator, c_path, .c, env_opts) catch |err| {
                    reportCrashFixture(allocator, env_opts, case_label, prelude_seed, phase, prelude_actions.items, crash_action) catch {};
                    return err;
                };
                defer before_snapshot.deinit(allocator);

                applyCommittedAction(c_path, .c, env_opts, crash_action) catch |err| {
                    reportCrashFixture(allocator, env_opts, case_label, prelude_seed, phase, prelude_actions.items, crash_action) catch {};
                    return err;
                };

                var after_snapshot = takeDifferentialSnapshot(allocator, c_path, .c, env_opts) catch |err| {
                    reportCrashFixture(allocator, env_opts, case_label, prelude_seed, phase, prelude_actions.items, crash_action) catch {};
                    return err;
                };
                defer after_snapshot.deinit(allocator);

                applyCrashPublishedAction(zig_path, env_opts, crash_action, phase) catch |err| {
                    reportCrashFixture(allocator, env_opts, case_label, prelude_seed, phase, prelude_actions.items, crash_action) catch {};
                    return err;
                };

                var zig_reopened_snapshot = takeDifferentialSnapshot(allocator, zig_path, .zig, env_opts) catch |err| {
                    reportCrashFixture(allocator, env_opts, case_label, prelude_seed, phase, prelude_actions.items, crash_action) catch {};
                    return err;
                };
                defer zig_reopened_snapshot.deinit(allocator);

                expectCrashSnapshot(&before_snapshot, &after_snapshot, &zig_reopened_snapshot, phase) catch |err| {
                    reportCrashFixture(allocator, env_opts, case_label, prelude_seed, phase, prelude_actions.items, crash_action) catch {};
                    return err;
                };
            }
        }

        fn runCrashPhaseCaseOrSkip(allocator: std.mem.Allocator, base_opts: EnvironmentOptions, case_label: []const u8) !void {
            runCrashPhaseCase(allocator, base_opts, case_label) catch |err| switch (err) {
                Error.Incompatible => if (base_opts.fixed_map) return else return err,
                Error.LmdbUnexpected => if (base_opts.fixed_map) return else return err,
                else => return err,
            };
        }

        fn tmpPathWithSuffix(buf: []u8, suffix: []const u8) [*:0]const u8 {
            const base = "/tmp/antfly-lmdb-test-";
            const ts = nowNs();
            const nonce = nextLmdbSimTmpNonce();
            const slice = std.fmt.bufPrint(buf, "{s}{d}-{d}-{s}\x00", .{ base, ts, nonce, suffix }) catch unreachable;
            var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
            return @ptrCast(slice.ptr);
        }

        fn replayArtifactPath(buf: []u8, suffix: []const u8) []const u8 {
            const base = "/tmp/antfly-lmdb-replay-";
            const ts = nowNs();
            const nonce = nextLmdbSimTmpNonce();
            return std.fmt.bufPrint(buf, "{s}{d}-{d}-{s}.fixture", .{ base, ts, nonce, suffix }) catch unreachable;
        }

        fn cleanupTmp(path: [*:0]const u8) void {
            var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
        }

        fn toVal(data: []const u8) c.MDB_val {
            return .{ .mv_size = data.len, .mv_data = @ptrCast(@constCast(data.ptr)) };
        }

        fn fromVal(val: c.MDB_val) []const u8 {
            if (val.mv_data == null or val.mv_size == 0) return &.{};
            const ptr: [*]const u8 = @ptrCast(val.mv_data.?);
            return ptr[0..val.mv_size];
        }
    };
}
