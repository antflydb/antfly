// Copyright 2026 Antfly, Inc.
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
const antfly = @import("antfly-zig");
const raft_engine = @import("raft_engine");

const platform_time = antfly.platform_time;
const raft_mod = antfly.raft;
const wal_replica_state_mod = antfly.raft.storage.wal_replica_state;
const default_wal_cfg = wal_replica_state_mod.WalReplicaStateConfig{};

const group_id: u64 = 77;
const replica_id: u64 = 1;
const local_node_id: u64 = 1;

const Config = struct {
    docs: usize = 10_000,
    batch_size: usize = 500,
    body_repeat: usize = 8,
    iterations: usize = 3,
    crash_like_restart: bool = false,
    checkpoint_records_threshold: usize = default_wal_cfg.checkpoint_replay_records_threshold,
    checkpoint_bytes_threshold: usize = default_wal_cfg.checkpoint_replay_bytes_threshold,
};

const Summary = struct {
    docs: usize = 0,
    batches: usize = 0,
    ensure_ns: u64 = 0,
    campaign_ns: u64 = 0,
    propose_commit_ns: u64 = 0,
    max_batch_ns: u64 = 0,
    restart_ns: u64 = 0,
    payload_bytes: usize = 0,
    wal_last_index: u64 = 0,
    wal_applied_index: u64 = 0,
    reopened_wal_last_index: u64 = 0,
    reopened_wal_applied_index: u64 = 0,
    apply_latest_commit_index: u64 = 0,
    apply_latest_entry_count: usize = 0,
    wal_persist_ready_calls: u64 = 0,
    wal_ready_persist_calls: u64 = 0,
    wal_applied_index_updates: u64 = 0,
    wal_applied_index_persist_calls: u64 = 0,
    wal_checkpoint_persist_calls: u64 = 0,
    wal_persist_ns: u64 = 0,
    wal_encode_ns: u64 = 0,
    wal_append_ns: u64 = 0,
    wal_truncate_ns: u64 = 0,
    wal_encoded_bytes: u64 = 0,
    wal_applied_watermark_persist_ns: u64 = 0,
    wal_applied_watermark_bytes: u64 = 0,
    wal_replay_debt_records: u64 = 0,
    wal_replay_debt_bytes: u64 = 0,
    reopened_replayed_delta_records: u64 = 0,
    reopened_replayed_delta_bytes: u64 = 0,
    reopened_apply_latest_commit_index: u64 = 0,
    reopened_apply_latest_entry_count: usize = 0,
    reopened_active: bool = false,
};

const AggregateSummary = struct {
    iterations: usize = 0,
    propose_commit_ns_median: u64 = 0,
    propose_commit_ns_min: u64 = 0,
    propose_commit_ns_max: u64 = 0,
    wal_persist_ns_median: u64 = 0,
    wal_persist_ns_min: u64 = 0,
    wal_persist_ns_max: u64 = 0,
    wal_append_ns_median: u64 = 0,
    wal_append_ns_min: u64 = 0,
    wal_append_ns_max: u64 = 0,
    restart_ns_median: u64 = 0,
    restart_ns_min: u64 = 0,
    restart_ns_max: u64 = 0,
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(init.minimal.args);
    const summaries = try alloc.alloc(Summary, cfg.iterations);
    defer alloc.free(summaries);

    for (summaries, 0..) |*summary, i| {
        var path_buf: [256]u8 = undefined;
        const root_path = tempPath(&path_buf, i);
        defer cleanupTempDir(root_path);

        summary.* = try runBench(alloc, std.mem.span(root_path), cfg);
        printSummary(cfg, i, summary.*);
    }

    const aggregate = try aggregateSummaries(alloc, summaries);
    printAggregate(cfg, aggregate);
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, "--docs");
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args, "--batch-size");
        } else if (std.mem.eql(u8, arg, "--body-repeat")) {
            cfg.body_repeat = try parseNextUsize(&args, "--body-repeat");
        } else if (std.mem.eql(u8, arg, "--iterations")) {
            cfg.iterations = try parseNextUsize(&args, "--iterations");
        } else if (std.mem.eql(u8, arg, "--crash-like-restart")) {
            cfg.crash_like_restart = true;
        } else if (std.mem.eql(u8, arg, "--checkpoint-records-threshold")) {
            cfg.checkpoint_records_threshold = try parseNextUsize(&args, "--checkpoint-records-threshold");
        } else if (std.mem.eql(u8, arg, "--checkpoint-bytes-threshold")) {
            cfg.checkpoint_bytes_threshold = try parseNextUsize(&args, "--checkpoint-bytes-threshold");
        } else {
            std.debug.print("invalid argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.batch_size == 0 or cfg.body_repeat == 0 or cfg.iterations == 0) return error.InvalidArgument;
    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(usize, raw, 10);
}

fn runBench(alloc: std.mem.Allocator, root: []const u8, cfg: Config) !Summary {
    var summary: Summary = .{};
    summary.docs = cfg.docs;
    summary.batches = divRoundUp(cfg.docs, cfg.batch_size);

    const replica_catalog_path = try std.fmt.allocPrint(alloc, "{s}/catalog.txt", .{root});
    defer alloc.free(replica_catalog_path);

    var dummy_store = raft_engine.core.MemoryStorage.init(alloc);
    defer dummy_store.deinit();
    var factory = DescriptorFactory{ .alloc = alloc, .store = &dummy_store };

    {
        var managed = try initManagedHost(alloc, root, replica_catalog_path, factory.iface(), cfg, !cfg.crash_like_restart);
        defer managed.deinit();

        const ensure_started_ns = nowNs();
        _ = try managed.host.ensureReplica(.{
            .group_id = group_id,
            .replica_id = replica_id,
            .local_node_id = local_node_id,
            .bootstrap_mode = .persisted,
        });
        summary.ensure_ns = elapsedSince(ensure_started_ns);

        const campaign_started_ns = nowNs();
        try managed.host.campaignGroup(group_id);
        _ = try managed.host.runRound(1, 1);
        summary.campaign_ns = elapsedSince(campaign_started_ns);

        var start: usize = 0;
        while (start < cfg.docs) : (start += cfg.batch_size) {
            const end = @min(start + cfg.batch_size, cfg.docs);
            const before_last_index = try walLastIndex(&managed, group_id);
            const batch_started_ns = nowNs();

            for (start..end) |doc_idx| {
                const op = try putOperationJson(alloc, doc_idx, cfg.body_repeat);
                defer alloc.free(op);
                summary.payload_bytes += op.len;
                try managed.host.propose(group_id, op);
            }

            try runUntilApplied(&managed, group_id, before_last_index + (end - start), 32);

            const batch_ns = elapsedSince(batch_started_ns);
            summary.propose_commit_ns += batch_ns;
            summary.max_batch_ns = @max(summary.max_batch_ns, batch_ns);
        }

        const live_wal_state = try inspectWalState(&managed, group_id);
        summary.wal_last_index = live_wal_state.last_index;
        summary.wal_applied_index = live_wal_state.applied_index;
        summary.wal_persist_ready_calls = live_wal_state.stats.persist_ready_calls;
        summary.wal_ready_persist_calls = live_wal_state.stats.ready_persist_calls;
        summary.wal_applied_index_updates = live_wal_state.stats.applied_index_updates;
        summary.wal_applied_index_persist_calls = live_wal_state.stats.applied_index_persist_calls;
        summary.wal_checkpoint_persist_calls = live_wal_state.stats.checkpoint_persist_calls;
        summary.wal_persist_ns = live_wal_state.stats.persist_ns;
        summary.wal_encode_ns = live_wal_state.stats.encode_ns;
        summary.wal_append_ns = live_wal_state.stats.wal_append_ns;
        summary.wal_truncate_ns = live_wal_state.stats.wal_truncate_ns;
        summary.wal_encoded_bytes = live_wal_state.stats.encoded_bytes;
        summary.wal_applied_watermark_persist_ns = live_wal_state.stats.applied_watermark_persist_ns;
        summary.wal_applied_watermark_bytes = live_wal_state.stats.applied_watermark_bytes;
        summary.wal_replay_debt_records = live_wal_state.stats.replay_debt_records;
        summary.wal_replay_debt_bytes = live_wal_state.stats.replay_debt_bytes;

        const live_apply_state = try inspectApplyStoreState(&managed, group_id);
        summary.apply_latest_commit_index = live_apply_state.commit_index;
        summary.apply_latest_entry_count = live_apply_state.entry_count;
    }

    {
        const restart_started_ns = nowNs();
        var managed = try initManagedHost(alloc, root, replica_catalog_path, factory.iface(), cfg, true);
        defer managed.deinit();
        summary.restart_ns = elapsedSince(restart_started_ns);

        summary.reopened_active = managed.host.status(group_id) == .active;

        const reopened_wal_state = try inspectWalState(&managed, group_id);
        summary.reopened_wal_last_index = reopened_wal_state.last_index;
        summary.reopened_wal_applied_index = reopened_wal_state.applied_index;
        summary.reopened_replayed_delta_records = reopened_wal_state.stats.replayed_delta_records;
        summary.reopened_replayed_delta_bytes = reopened_wal_state.stats.replayed_delta_bytes;

        const reopened_apply_state = try inspectApplyStoreState(&managed, group_id);
        summary.reopened_apply_latest_commit_index = reopened_apply_state.commit_index;
        summary.reopened_apply_latest_entry_count = reopened_apply_state.entry_count;
    }

    return summary;
}

const DescriptorFactory = struct {
    alloc: std.mem.Allocator,
    store: *raft_engine.core.MemoryStorage,

    fn iface(self: *@This()) raft_mod.ReplicaDescriptorFactory {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_descriptor = buildDescriptor,
                .free_descriptor = freeDescriptor,
            },
        };
    }

    fn buildDescriptor(ptr: *anyopaque, record: raft_mod.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
        return .{
            .group = .{
                .group_id = record.group_id,
                .local_node_id = record.local_node_id,
                .raft_config = .{
                    .id = record.local_node_id,
                    .group_id = record.group_id,
                    .peers = peers[0..],
                    .election_tick = 5,
                    .heartbeat_tick = 1,
                    .pre_vote = false,
                    .check_quorum = true,
                },
                .storage = self.store.storage(),
            },
            .bootstrap = .persisted,
        };
    }

    fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        _ = alloc;
        self.alloc.free(desc.group.raft_config.peers);
    }
};

fn initManagedHost(
    alloc: std.mem.Allocator,
    replica_root: []const u8,
    replica_catalog_path: []const u8,
    descriptor_factory: raft_mod.ReplicaDescriptorFactory,
    cfg: Config,
    wal_flush_on_deinit: bool,
) !raft_mod.ManagedHost {
    return try raft_mod.ManagedHost.init(alloc, .{
        .host = .{
            .local_node_id = local_node_id,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
            .replica_state_backend = .wal,
        },
        .wal_replica_state = .{
            .checkpoint_replay_records_threshold = cfg.checkpoint_records_threshold,
            .checkpoint_replay_bytes_threshold = cfg.checkpoint_bytes_threshold,
        },
        .wal_flush_on_deinit = wal_flush_on_deinit,
    }, .{
        .host = .{ .descriptor_factory = descriptor_factory },
    });
}

const WalStateSummary = struct {
    last_index: u64,
    applied_index: u64,
    stats: wal_replica_state_mod.WalReplicaStateStats,
};

fn inspectWalState(managed: *raft_mod.ManagedHost, id: u64) !WalStateSummary {
    const provider = managed.owned_wal_replica_provider orelse return error.MissingWalReplicaProvider;
    const state = provider.stateForGroup(id) orelse return error.MissingWalReplicaState;
    return .{
        .last_index = try state.storage().lastIndex(),
        .applied_index = state.appliedIndex(),
        .stats = state.statsSnapshot(),
    };
}

fn walLastIndex(managed: *raft_mod.ManagedHost, id: u64) !u64 {
    return (try inspectWalState(managed, id)).last_index;
}

const ApplyStoreStateSummary = struct {
    commit_index: u64 = 0,
    entry_count: usize = 0,
};

fn inspectApplyStoreState(managed: *raft_mod.ManagedHost, id: u64) !ApplyStoreStateSummary {
    const store = managed.owned_data_store orelse return .{};
    const latest = (try store.latestBatch(id)) orelse return .{};
    return .{
        .commit_index = latest.commit_index,
        .entry_count = latest.entry_count,
    };
}

fn runUntilApplied(managed: *raft_mod.ManagedHost, id: u64, target_index: u64, max_rounds: usize) !void {
    var rounds: usize = 0;
    while (rounds < max_rounds) : (rounds += 1) {
        _ = try managed.host.runRound(1, 1);
        const state = try inspectWalState(managed, id);
        if (state.last_index >= target_index and state.applied_index >= target_index) return;
    }
    return error.Timeout;
}

fn putOperationJson(alloc: std.mem.Allocator, doc_idx: usize, body_repeat: usize) ![]u8 {
    const doc_json = try documentJson(alloc, doc_idx, body_repeat);
    defer alloc.free(doc_json);
    return try std.fmt.allocPrint(alloc, "put:doc:{d:0>8}={s}", .{ doc_idx, doc_json });
}

fn documentJson(alloc: std.mem.Allocator, doc_idx: usize, body_repeat: usize) ![]u8 {
    var body = std.ArrayList(u8).empty;
    defer body.deinit(alloc);
    for (0..body_repeat) |_| {
        try body.appendSlice(alloc, "managed host wal bench alpha beta gamma delta epsilon ");
    }
    return try std.fmt.allocPrint(
        alloc,
        "{{\"title\":\"doc {d}\",\"body\":\"{s}\"}}",
        .{ doc_idx, body.items },
    );
}

fn printSummary(cfg: Config, iteration_idx: usize, summary: Summary) void {
    std.debug.print(
        "managed_host_wal_bench iteration={d}/{d} docs={d} batch_size={d} body_repeat={d} checkpoint_records_threshold={d} checkpoint_bytes_threshold={d} batches={d} ensure_ms={d:.3} campaign_ms={d:.3} propose_commit_ms={d:.3} max_batch_ms={d:.3} restart_ms={d:.3} payload_bytes={d} wal_last_index={d} wal_applied_index={d} wal_persist_ready_calls={d} wal_ready_persist_calls={d}",
        .{
            iteration_idx + 1,
            cfg.iterations,
            cfg.docs,
            cfg.batch_size,
            cfg.body_repeat,
            cfg.checkpoint_records_threshold,
            cfg.checkpoint_bytes_threshold,
            summary.batches,
            nsToMsFloat(summary.ensure_ns),
            nsToMsFloat(summary.campaign_ns),
            nsToMsFloat(summary.propose_commit_ns),
            nsToMsFloat(summary.max_batch_ns),
            nsToMsFloat(summary.restart_ns),
            summary.payload_bytes,
            summary.wal_last_index,
            summary.wal_applied_index,
            summary.wal_persist_ready_calls,
            summary.wal_ready_persist_calls,
        },
    );
    std.debug.print(
        " wal_applied_index_updates={d} wal_applied_index_persist_calls={d} wal_checkpoint_persist_calls={d} wal_persist_ms={d:.3} wal_encode_ms={d:.3} wal_append_ms={d:.3} wal_truncate_ms={d:.3} wal_encoded_bytes={d} wal_applied_watermark_persist_ms={d:.3} wal_applied_watermark_bytes={d} apply_latest_commit_index={d} apply_latest_entry_count={d} reopened_active={} reopened_wal_last_index={d} reopened_wal_applied_index={d} reopened_apply_latest_commit_index={d} reopened_apply_latest_entry_count={d}\n",
        .{
            summary.wal_applied_index_updates,
            summary.wal_applied_index_persist_calls,
            summary.wal_checkpoint_persist_calls,
            nsToMsFloat(summary.wal_persist_ns),
            nsToMsFloat(summary.wal_encode_ns),
            nsToMsFloat(summary.wal_append_ns),
            nsToMsFloat(summary.wal_truncate_ns),
            summary.wal_encoded_bytes,
            nsToMsFloat(summary.wal_applied_watermark_persist_ns),
            summary.wal_applied_watermark_bytes,
            summary.apply_latest_commit_index,
            summary.apply_latest_entry_count,
            summary.reopened_active,
            summary.reopened_wal_last_index,
            summary.reopened_wal_applied_index,
            summary.reopened_apply_latest_commit_index,
            summary.reopened_apply_latest_entry_count,
        },
    );
    std.debug.print(
        "managed_host_wal_bench_debt live_records={d} live_bytes={d} reopened_replayed_records={d} reopened_replayed_bytes={d}\n",
        .{
            summary.wal_replay_debt_records,
            summary.wal_replay_debt_bytes,
            summary.reopened_replayed_delta_records,
            summary.reopened_replayed_delta_bytes,
        },
    );
    std.debug.print(
        "managed_host_wal_bench_csv iteration,iterations,docs,batch_size,body_repeat,crash_like_restart,checkpoint_records_threshold,checkpoint_bytes_threshold,batches,ensure_ms,campaign_ms,propose_commit_ms,max_batch_ms,restart_ms,payload_bytes,wal_last_index,wal_applied_index,wal_persist_ready_calls,wal_ready_persist_calls",
        .{},
    );
    std.debug.print(
        ",wal_applied_index_updates,wal_applied_index_persist_calls,wal_checkpoint_persist_calls,wal_persist_ms,wal_encode_ms,wal_append_ms,wal_truncate_ms,wal_encoded_bytes,wal_applied_watermark_persist_ms,wal_applied_watermark_bytes,apply_latest_commit_index,apply_latest_entry_count,reopened_active,reopened_wal_last_index,reopened_wal_applied_index,reopened_apply_latest_commit_index,reopened_apply_latest_entry_count\n",
        .{},
    );
    std.debug.print(
        "managed_host_wal_bench_csv {d},{d},{d},{d},{d},{any},{d},{d},{d},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d},{d},{d},{d},{d}",
        .{
            iteration_idx + 1,
            cfg.iterations,
            cfg.docs,
            cfg.batch_size,
            cfg.body_repeat,
            cfg.crash_like_restart,
            cfg.checkpoint_records_threshold,
            cfg.checkpoint_bytes_threshold,
            summary.batches,
            nsToMsFloat(summary.ensure_ns),
            nsToMsFloat(summary.campaign_ns),
            nsToMsFloat(summary.propose_commit_ns),
            nsToMsFloat(summary.max_batch_ns),
            nsToMsFloat(summary.restart_ns),
            summary.payload_bytes,
            summary.wal_last_index,
            summary.wal_applied_index,
            summary.wal_persist_ready_calls,
            summary.wal_ready_persist_calls,
        },
    );
    std.debug.print(
        ",{d},{d},{d},{d:.3},{d:.3},{d:.3},{d:.3},{d},{d:.3},{d},{d},{d},{any},{d},{d},{d},{d}\n",
        .{
            summary.wal_applied_index_updates,
            summary.wal_applied_index_persist_calls,
            summary.wal_checkpoint_persist_calls,
            nsToMsFloat(summary.wal_persist_ns),
            nsToMsFloat(summary.wal_encode_ns),
            nsToMsFloat(summary.wal_append_ns),
            nsToMsFloat(summary.wal_truncate_ns),
            summary.wal_encoded_bytes,
            nsToMsFloat(summary.wal_applied_watermark_persist_ns),
            summary.wal_applied_watermark_bytes,
            summary.apply_latest_commit_index,
            summary.apply_latest_entry_count,
            summary.reopened_active,
            summary.reopened_wal_last_index,
            summary.reopened_wal_applied_index,
            summary.reopened_apply_latest_commit_index,
            summary.reopened_apply_latest_entry_count,
        },
    );
    std.debug.print(
        "managed_host_wal_bench_debt_csv live_records,live_bytes,reopened_replayed_records,reopened_replayed_bytes\nmanaged_host_wal_bench_debt_csv {d},{d},{d},{d}\n",
        .{
            summary.wal_replay_debt_records,
            summary.wal_replay_debt_bytes,
            summary.reopened_replayed_delta_records,
            summary.reopened_replayed_delta_bytes,
        },
    );
}

fn aggregateSummaries(alloc: std.mem.Allocator, summaries: []const Summary) !AggregateSummary {
    var propose_commit = try alloc.alloc(u64, summaries.len);
    defer alloc.free(propose_commit);
    var wal_persist = try alloc.alloc(u64, summaries.len);
    defer alloc.free(wal_persist);
    var wal_append = try alloc.alloc(u64, summaries.len);
    defer alloc.free(wal_append);
    var restart = try alloc.alloc(u64, summaries.len);
    defer alloc.free(restart);

    for (summaries, 0..) |summary, i| {
        propose_commit[i] = summary.propose_commit_ns;
        wal_persist[i] = summary.wal_persist_ns;
        wal_append[i] = summary.wal_append_ns;
        restart[i] = summary.restart_ns;
    }

    return .{
        .iterations = summaries.len,
        .propose_commit_ns_median = medianU64(propose_commit),
        .propose_commit_ns_min = minU64(propose_commit),
        .propose_commit_ns_max = maxU64(propose_commit),
        .wal_persist_ns_median = medianU64(wal_persist),
        .wal_persist_ns_min = minU64(wal_persist),
        .wal_persist_ns_max = maxU64(wal_persist),
        .wal_append_ns_median = medianU64(wal_append),
        .wal_append_ns_min = minU64(wal_append),
        .wal_append_ns_max = maxU64(wal_append),
        .restart_ns_median = medianU64(restart),
        .restart_ns_min = minU64(restart),
        .restart_ns_max = maxU64(restart),
    };
}

fn printAggregate(cfg: Config, aggregate: AggregateSummary) void {
    std.debug.print(
        "managed_host_wal_bench_aggregate iterations={d} docs={d} batch_size={d} body_repeat={d} crash_like_restart={} checkpoint_records_threshold={d} checkpoint_bytes_threshold={d} propose_commit_ms_median={d:.3} propose_commit_ms_min={d:.3} propose_commit_ms_max={d:.3} wal_persist_ms_median={d:.3} wal_persist_ms_min={d:.3} wal_persist_ms_max={d:.3} wal_append_ms_median={d:.3} wal_append_ms_min={d:.3} wal_append_ms_max={d:.3} restart_ms_median={d:.3} restart_ms_min={d:.3} restart_ms_max={d:.3}\n",
        .{
            aggregate.iterations,
            cfg.docs,
            cfg.batch_size,
            cfg.body_repeat,
            cfg.crash_like_restart,
            cfg.checkpoint_records_threshold,
            cfg.checkpoint_bytes_threshold,
            nsToMsFloat(aggregate.propose_commit_ns_median),
            nsToMsFloat(aggregate.propose_commit_ns_min),
            nsToMsFloat(aggregate.propose_commit_ns_max),
            nsToMsFloat(aggregate.wal_persist_ns_median),
            nsToMsFloat(aggregate.wal_persist_ns_min),
            nsToMsFloat(aggregate.wal_persist_ns_max),
            nsToMsFloat(aggregate.wal_append_ns_median),
            nsToMsFloat(aggregate.wal_append_ns_min),
            nsToMsFloat(aggregate.wal_append_ns_max),
            nsToMsFloat(aggregate.restart_ns_median),
            nsToMsFloat(aggregate.restart_ns_min),
            nsToMsFloat(aggregate.restart_ns_max),
        },
    );
    std.debug.print(
        "managed_host_wal_bench_aggregate_csv iterations,docs,batch_size,body_repeat,crash_like_restart,checkpoint_records_threshold,checkpoint_bytes_threshold,propose_commit_ms_median,propose_commit_ms_min,propose_commit_ms_max,wal_persist_ms_median,wal_persist_ms_min,wal_persist_ms_max,wal_append_ms_median,wal_append_ms_min,wal_append_ms_max,restart_ms_median,restart_ms_min,restart_ms_max\n",
        .{},
    );
    std.debug.print(
        "managed_host_wal_bench_aggregate_csv {d},{d},{d},{d},{any},{d},{d},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3}\n",
        .{
            aggregate.iterations,
            cfg.docs,
            cfg.batch_size,
            cfg.body_repeat,
            cfg.crash_like_restart,
            cfg.checkpoint_records_threshold,
            cfg.checkpoint_bytes_threshold,
            nsToMsFloat(aggregate.propose_commit_ns_median),
            nsToMsFloat(aggregate.propose_commit_ns_min),
            nsToMsFloat(aggregate.propose_commit_ns_max),
            nsToMsFloat(aggregate.wal_persist_ns_median),
            nsToMsFloat(aggregate.wal_persist_ns_min),
            nsToMsFloat(aggregate.wal_persist_ns_max),
            nsToMsFloat(aggregate.wal_append_ns_median),
            nsToMsFloat(aggregate.wal_append_ns_min),
            nsToMsFloat(aggregate.wal_append_ns_max),
            nsToMsFloat(aggregate.restart_ns_median),
            nsToMsFloat(aggregate.restart_ns_min),
            nsToMsFloat(aggregate.restart_ns_max),
        },
    );
}

fn divRoundUp(n: usize, d: usize) usize {
    return if (n == 0) 0 else ((n - 1) / d) + 1;
}

fn nsToMsFloat(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(std.time.ns_per_ms));
}

fn nowNs() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSince(started_ns: u64) u64 {
    return nowNs() - started_ns;
}

var temp_path_nonce: u64 = 0;

fn tempPath(buf: []u8, iteration_idx: usize) [*:0]const u8 {
    const ts = nowNs();
    const nonce = @atomicRmw(u64, &temp_path_nonce, .Add, 1, .monotonic);
    const path_bytes = std.fmt.bufPrint(buf, "/tmp/antfly-managed-host-wal-bench-{d}-{d}-{d}\x00", .{ ts, nonce, iteration_idx }) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(path_bytes.ptr)))) catch unreachable;
    return @ptrCast(path_bytes.ptr);
}

fn cleanupTempDir(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

fn lessThanU64(_: void, lhs: u64, rhs: u64) bool {
    return lhs < rhs;
}

fn medianU64(values: []u64) u64 {
    std.mem.sort(u64, values, {}, lessThanU64);
    return values[values.len / 2];
}

fn minU64(values: []const u64) u64 {
    var best = values[0];
    for (values[1..]) |value| best = @min(best, value);
    return best;
}

fn maxU64(values: []const u64) u64 {
    var best = values[0];
    for (values[1..]) |value| best = @max(best, value);
    return best;
}
