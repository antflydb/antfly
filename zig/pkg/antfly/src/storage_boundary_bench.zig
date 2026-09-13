// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Measure the actual internal batch codec against the production storage owner.
//! This is a microbenchmark: report codec cost separately from DB time, without
//! treating local timings as a throughput or release-runner guarantee.
const std = @import("std");
const time = @import("antfly_platform").time;
const batch = @import("api/batch.zig");
const types = @import("storage/db/types.zig");
const query_contract = @import("api/local_query_contract.zig");
const client = @import("storage/kernel_owner_client.zig");

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();
    _ = args.skip();
    const path = args.next() orelse return error.ExpectedEmptyDatabaseDirectory;
    // Explicitly require a new directory so this benchmark cannot overwrite a DB.
    try std.Io.Dir.cwd().createDir(init.io, path, .default_dir);
    defer std.Io.Dir.cwd().deleteTree(init.io, path) catch {};
    var owner = try client.Owner.open(.{
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .group_id = 1,
        .has_identity_namespace = 1,
        .identity_table_id = 1,
        .identity_shard_id = 1,
        .identity_range_id = 1,
    });
    defer owner.deinit();
    const padding = "x" ** 448;
    const documents = [2][]const u8{
        "{\"revision\":0,\"title\":\"alpha\",\"body\":\"" ++ padding ++ "\"}",
        "{\"revision\":1,\"title\":\"beta\",\"body\":\"" ++ padding ++ "\"}",
    };
    const writes = try alloc.alloc(types.BatchWrite, 1000);
    defer alloc.free(writes);
    var initialized: usize = 0;
    defer for (writes[0..initialized]) |write| alloc.free(write.key);
    for (writes, 0..) |*write, index| {
        write.* = .{ .key = try std.fmt.allocPrint(alloc, "doc:{d:0>6}", .{index}), .value = documents[0] };
        initialized += 1;
    }
    const rounds = 10;
    for ([_]usize{ 1, 100, 1000 }) |count| {
        var payloads: [2][]u8 = undefined;
        for (&payloads, 0..) |*payload, parity| {
            for (writes[0..count]) |*write| write.value = documents[parity];
            payload.* = try batch.encodeBatchRequest(alloc, .{ .writes = writes[0..count] });
        }
        defer for (payloads) |payload| alloc.free(payload);
        const codec_start = time.monotonicNs();
        for (0..rounds) |round| {
            for (writes[0..count]) |*write| write.value = documents[round % 2];
            const encoded = try batch.encodeBatchRequest(alloc, .{ .writes = writes[0..count] });
            defer alloc.free(encoded);
            var decoded = try batch.parseInternalBatchRequest(alloc, encoded);
            defer decoded.deinit(alloc);
            if (decoded.req.writes.len != count) return error.InvalidBenchmarkResult;
            std.mem.doNotOptimizeAway(decoded.req);
        }
        const codec_ns = time.monotonicNs() - codec_start;
        const owner_start = time.monotonicNs();
        for (0..rounds) |round| {
            var result = try owner.batchJson("docs", payloads[round % 2]);
            result.deinit();
        }
        const owner_ns = time.monotonicNs() - owner_start;
        std.debug.print("{{\"case\":\"batch\",\"documents\":{d},\"bytes\":{d},\"rounds\":{d},\"encode_decode_ns\":{d},\"owner_batch_ns\":{d}}}\n", .{ count, payloads[0].len, rounds, codec_ns, owner_ns });
    }
    for ([_]usize{ 10, 100, 1000 }) |limit| {
        const query = try std.fmt.allocPrint(alloc, "{{\"query\":{{\"match_all\":{{}}}},\"limit\":{d}}}", .{limit});
        defer alloc.free(query);
        var response = try owner.queryJson("docs", query);
        defer response.deinit();
        const decode_start = time.monotonicNs();
        for (0..rounds) |_| {
            var parsed = try query_contract.parseStorageKernelSearchResult(alloc, response.bytes());
            defer parsed.deinit();
            if (parsed.hits.len != limit) return error.InvalidBenchmarkResult;
            std.mem.doNotOptimizeAway(parsed.hits);
        }
        const decode_ns = time.monotonicNs() - decode_start;
        const query_start = time.monotonicNs();
        for (0..rounds) |_| {
            var result = try owner.queryJson("docs", query);
            result.deinit();
        }
        const query_ns = time.monotonicNs() - query_start;
        std.debug.print("{{\"case\":\"query\",\"limit\":{d},\"bytes\":{d},\"rounds\":{d},\"consumer_decode_ns\":{d},\"owner_query_ns\":{d}}}\n", .{ limit, response.bytes().len, rounds, decode_ns, query_ns });
    }
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_common.zig");
