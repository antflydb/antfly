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
const raft_state_machine = antfly.raft.state_machine;
const raft_apply_store_mod = antfly.data.storage.raft_apply_store;

const group_id: u64 = 77;

const Config = struct {
    docs: usize = 10_000,
    batch_size: usize = 500,
    body_repeat: usize = 8,
};

const Summary = struct {
    docs: usize = 0,
    batches: usize = 0,
    apply_ns: u64 = 0,
    max_apply_batch_ns: u64 = 0,
    reopen_ns: u64 = 0,
    latest_batch_ns: u64 = 0,
    group_state_ns: u64 = 0,
    snapshot_ns: u64 = 0,
    payload_bytes: usize = 0,
    latest_commit_index: u64 = 0,
    latest_entry_count: usize = 0,
    latest_normal_entry_count: usize = 0,
    state_entries: usize = 0,
    snapshot_bytes: usize = 0,
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(init.minimal.args);

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const summary = try runBench(alloc, std.mem.span(path), cfg);
    printSummary(cfg, summary);
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
        } else {
            std.debug.print("invalid argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.batch_size == 0 or cfg.body_repeat == 0) return error.InvalidArgument;
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
    var next_index: u64 = 1;

    {
        var store = try raft_apply_store_mod.RaftApplyStore.init(alloc, .{ .root_dir = root });
        defer store.deinit();

        const range_started_ns = nowNs();
        const encoded_range = try encodeRangeBatch(alloc, next_index);
        defer alloc.free(encoded_range);
        try store.snapshotBuilder().applyBatch(.{
            .group_id = group_id,
            .commit_index = next_index,
            .entries_bytes = encoded_range,
        });
        summary.apply_ns += elapsedSince(range_started_ns);
        summary.max_apply_batch_ns = summary.apply_ns;
        summary.payload_bytes += encoded_range.len;
        next_index += 1;

        const entries_buf = try alloc.alloc(raft_engine.core.Entry, cfg.batch_size);
        defer alloc.free(entries_buf);

        var start: usize = 0;
        while (start < cfg.docs) : (start += cfg.batch_size) {
            const end = @min(start + cfg.batch_size, cfg.docs);
            const entries = entries_buf[0 .. end - start];
            defer {
                for (entries) |entry| alloc.free(@constCast(entry.data));
            }

            for (start..end, 0..) |doc_idx, i| {
                entries[i] = .{
                    .term = 1,
                    .index = next_index,
                    .entry_type = .normal,
                    .data = try putOperationJson(alloc, doc_idx, cfg.body_repeat),
                };
                next_index += 1;
            }

            const encoded = try raft_state_machine.encodeCommittedEntries(alloc, entries);
            defer alloc.free(encoded);

            const apply_started_ns = nowNs();
            try store.snapshotBuilder().applyBatch(.{
                .group_id = group_id,
                .commit_index = next_index - 1,
                .entries_bytes = encoded,
            });
            const apply_batch_ns = elapsedSince(apply_started_ns);
            summary.apply_ns += apply_batch_ns;
            summary.max_apply_batch_ns = @max(summary.max_apply_batch_ns, apply_batch_ns);
            summary.batches += 1;
            summary.payload_bytes += encoded.len;
        }
        summary.docs = cfg.docs;
    }

    const reopen_started_ns = nowNs();
    var reopened = try raft_apply_store_mod.RaftApplyStore.init(alloc, .{ .root_dir = root });
    defer reopened.deinit();
    summary.reopen_ns = elapsedSince(reopen_started_ns);

    const latest_started_ns = nowNs();
    const latest_batch = (try reopened.latestBatch(group_id)) orelse return error.MissingLatestBatch;
    summary.latest_batch_ns = elapsedSince(latest_started_ns);
    summary.latest_commit_index = latest_batch.commit_index;
    summary.latest_entry_count = latest_batch.entry_count;
    summary.latest_normal_entry_count = latest_batch.normal_entry_count;

    const state_started_ns = nowNs();
    const group_state = try reopened.groupState(alloc, group_id);
    summary.group_state_ns = elapsedSince(state_started_ns);
    summary.state_entries = group_state.len;
    defer freeGroupState(alloc, group_state);

    const snapshot_started_ns = nowNs();
    const snapshot = try reopened.snapshotBuilder().buildSnapshot(alloc, group_id);
    summary.snapshot_ns = elapsedSince(snapshot_started_ns);
    summary.snapshot_bytes = snapshot.len;
    alloc.free(snapshot);

    return summary;
}

fn encodeRangeBatch(alloc: std.mem.Allocator, index: u64) ![]u8 {
    const range_op = try alloc.dupe(u8, "range:doc:00000000:doc:99999999~");
    defer alloc.free(range_op);
    return try raft_state_machine.encodeCommittedEntries(alloc, &.{
        .{
            .term = 1,
            .index = index,
            .entry_type = .normal,
            .data = range_op,
        },
    });
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
        try body.appendSlice(alloc, "raft apply bench alpha beta gamma delta epsilon ");
    }
    return try std.fmt.allocPrint(
        alloc,
        "{{\"title\":\"doc {d}\",\"body\":\"{s}\"}}",
        .{ doc_idx, body.items },
    );
}

fn freeGroupState(alloc: std.mem.Allocator, state: []raft_apply_store_mod.AppliedDataKV) void {
    for (state) |entry| {
        alloc.free(@constCast(entry.key));
        alloc.free(@constCast(entry.value));
    }
    alloc.free(state);
}

fn printSummary(cfg: Config, summary: Summary) void {
    std.debug.print(
        "raft_apply_bench docs={d} batch_size={d} body_repeat={d} batches={d} apply_ms={d:.3} max_apply_batch_ms={d:.3} reopen_ms={d:.3} latest_batch_ms={d:.3} group_state_ms={d:.3} snapshot_ms={d:.3} payload_bytes={d} latest_commit_index={d} latest_entry_count={d} latest_normal_entry_count={d} state_entries={d} snapshot_bytes={d}\n",
        .{
            cfg.docs,
            cfg.batch_size,
            cfg.body_repeat,
            summary.batches,
            nsToMsFloat(summary.apply_ns),
            nsToMsFloat(summary.max_apply_batch_ns),
            nsToMsFloat(summary.reopen_ns),
            nsToMsFloat(summary.latest_batch_ns),
            nsToMsFloat(summary.group_state_ns),
            nsToMsFloat(summary.snapshot_ns),
            summary.payload_bytes,
            summary.latest_commit_index,
            summary.latest_entry_count,
            summary.latest_normal_entry_count,
            summary.state_entries,
            summary.snapshot_bytes,
        },
    );
    std.debug.print(
        "raft_apply_bench_csv docs,batch_size,body_repeat,batches,apply_ms,max_apply_batch_ms,reopen_ms,latest_batch_ms,group_state_ms,snapshot_ms,payload_bytes,latest_commit_index,latest_entry_count,latest_normal_entry_count,state_entries,snapshot_bytes\n",
        .{},
    );
    std.debug.print(
        "raft_apply_bench_csv {d},{d},{d},{d},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d},{d},{d},{d},{d},{d}\n",
        .{
            cfg.docs,
            cfg.batch_size,
            cfg.body_repeat,
            summary.batches,
            nsToMsFloat(summary.apply_ns),
            nsToMsFloat(summary.max_apply_batch_ns),
            nsToMsFloat(summary.reopen_ns),
            nsToMsFloat(summary.latest_batch_ns),
            nsToMsFloat(summary.group_state_ns),
            nsToMsFloat(summary.snapshot_ns),
            summary.payload_bytes,
            summary.latest_commit_index,
            summary.latest_entry_count,
            summary.latest_normal_entry_count,
            summary.state_entries,
            summary.snapshot_bytes,
        },
    );
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

fn tempPath(buf: []u8) [*:0]const u8 {
    const ts = nowNs();
    const nonce = @atomicRmw(u64, &temp_path_nonce, .Add, 1, .monotonic);
    const path_bytes = std.fmt.bufPrint(buf, "/tmp/antfly-raft-apply-bench-{d}-{d}\x00", .{ ts, nonce }) catch unreachable;
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
