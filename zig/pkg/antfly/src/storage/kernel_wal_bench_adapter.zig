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

//! Runtime-benchmark adapter for the opaque WAL client. This deliberately
//! mirrors only the public surface consumed by `bench/storage/wal_bench.zig`;
//! it does not import the native WAL or storage implementation graph.

const client = @import("kernel_wal_client.zig");

pub const platform_time = @import("antfly_platform").time;

pub const CommitBackend = enum {
    sync,
    worker_thread,
    async_io,
    adaptive,
};

pub const StorageBackend = enum {
    lmdb,
    lsm,
    lsm_memory,
};

const Empty = struct {};
const Hook = struct { ctx: ?*anyopaque = null };

pub const WalOptions = struct {
    no_sync: bool = false,
    artificial_sync_delay_ns: u64 = 0,
    group_commit_window_ns: u64 = 0,
    group_commit_max_requests: usize = 64,
    commit_backend: CommitBackend = .adaptive,
    backend: ?StorageBackend = null,
    read_only: bool = false,
    storage: ?*anyopaque = null,
    lsm_options: Empty = .{},
    clock: Hook = .{},
    commit_scheduler: Hook = .{},
    model_commit_backend_completions: bool = false,

    pub fn resolvedBackend(self: WalOptions) StorageBackend {
        return self.backend orelse .lsm;
    }
};

pub const CommitStats = struct {
    publish_calls: u64 = 0,
    selected_sync_calls: u64 = 0,
    selected_worker_thread_calls: u64 = 0,
    selected_async_io_calls: u64 = 0,
    total_page_write_ns: u64 = 0,
    total_data_sync_ns: u64 = 0,
    total_meta_sync_ns: u64 = 0,
    total_publish_ns: u64 = 0,
};

pub const FullStats = struct {
    wal: client.WalStats,
    commit: ?CommitStats = null,
};

pub const WAL = struct {
    inner: client.WAL,

    pub fn open(path: [*:0]const u8, options: WalOptions) !WAL {
        return .{ .inner = try client.WAL.open(path, options) };
    }

    pub fn close(self: *WAL) void {
        self.inner.close();
        self.* = undefined;
    }

    pub fn append(self: *WAL, data: []const u8) !u64 {
        return self.inner.append(data);
    }

    pub fn fullStatsSnapshot(self: *WAL) FullStats {
        return .{ .wal = self.inner.statsSnapshot() };
    }
};
