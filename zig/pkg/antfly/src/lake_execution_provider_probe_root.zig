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

//! Independent test archive for the same complete serverless lake operation.
pub const antfly_sources = @import("source_owner_physical.zig");
const std = @import("std");
const metrics = @import("api/lake_execution_probe_metrics.zig");
var families: metrics.Metrics = .{};
var allocation_denials: std.atomic.Value(usize) = .init(0);
/// Diagnostics belong only to this independently compiled qualification owner;
/// normal runtime allocation admission and ABI layouts are unchanged.
pub fn lakeExecutionProbeAllocationDenied(requested: usize, live: usize, peak: usize, limit: usize, return_address: usize) void {
    if (allocation_denials.fetchAdd(1, .monotonic) >= 16) return;
    std.debug.print("LAKE_ALLOCATION_DENIED requested={} live={} peak={} limit={}\n", .{ requested, live, peak, limit });
    std.debug.dumpCurrentStackTrace(.{ .first_address = return_address });
}

pub fn lakeExecutionProbeParallelJob() void {
    _ = families.parallel_jobs.fetchAdd(1, .monotonic);
}
pub fn lakeExecutionProbeFamily(comptime name: []const u8, elapsed: u64) void {
    families.record(name, elapsed);
}
fn probeParallelJobs() callconv(.c) u64 {
    return families.parallel_jobs.load(.monotonic);
}
fn probeFamilyNs(index: u8) callconv(.c) u64 {
    return if (index < metrics.names.len) families.ns[index].load(.monotonic) else 0;
}
fn probeFamilyCalls(index: u8) callconv(.c) u64 {
    return if (index < metrics.names.len) families.calls[index].load(.monotonic) else 0;
}
fn probeBulkNs(index: u8) callconv(.c) u64 {
    return if (index < metrics.bulk_names.len) families.bulk_ns[index].load(.monotonic) else 0;
}
fn probeBulkCalls(index: u8) callconv(.c) u64 {
    return if (index < metrics.bulk_names.len) families.bulk_calls[index].load(.monotonic) else 0;
}

var cache_decoded_hits: std.atomic.Value(u64) = .init(0);
fn probeDecodedHits() callconv(.c) u64 {
    return cache_decoded_hits.load(.monotonic);
}
var linear_io: std.atomic.Value(bool) = .init(true);
var io_calls: std.atomic.Value(u64) = .init(0);
pub fn runtimeIoDispatchLinear() bool {
    _ = io_calls.fetchAdd(1, .monotonic);
    return linear_io.load(.monotonic);
}
fn probeSetIo(value: u8) callconv(.c) void {
    linear_io.store(value != 0, .monotonic);
}
fn probeIoCalls() callconv(.c) u64 {
    return io_calls.load(.monotonic);
}
var store_ns: std.atomic.Value(u64) = .init(0);
var source_ns: std.atomic.Value(u64) = .init(0);
var physical_ns: std.atomic.Value(u64) = .init(0);
var quota_peak: std.atomic.Value(u64) = .init(0);
pub fn lakeExecutionProbeQuota(peak: usize) void {
    quota_peak.store(peak, .monotonic);
}
var cache_hits: std.atomic.Value(u64) = .init(0);
var cache_reads: std.atomic.Value(u64) = .init(0);
var cache_bytes: std.atomic.Value(u64) = .init(0);
pub fn lakeExecutionProbeCache(stats: anytype, cache: anytype) void {
    cache_decoded_hits.store(metrics.decodedHits(cache), .monotonic);
    _ = cache_hits.fetchAdd(stats.hits, .monotonic);
    _ = cache_reads.fetchAdd(stats.provider_reads, .monotonic);
    _ = cache_bytes.fetchAdd(stats.provider_bytes, .monotonic);
}
pub fn lakeExecutionProbeStage(comptime name: []const u8, elapsed: u64) void {
    const target = if (comptime std.mem.eql(u8, name, "store_open_ns")) &store_ns else if (comptime std.mem.eql(u8, name, "source_open_ns")) &source_ns else &physical_ns;
    _ = target.fetchAdd(elapsed, .monotonic);
}
fn probeReset() callconv(.c) void {
    families.reset();
    cache_decoded_hits.store(0, .monotonic);
    io_calls.store(0, .monotonic);
    quota_peak.store(0, .monotonic);
    store_ns.store(0, .monotonic);
    source_ns.store(0, .monotonic);
    physical_ns.store(0, .monotonic);
    cache_hits.store(0, .monotonic);
    cache_reads.store(0, .monotonic);
    cache_bytes.store(0, .monotonic);
}
fn probeStore() callconv(.c) u64 {
    return store_ns.load(.monotonic);
}
fn probeSource() callconv(.c) u64 {
    return source_ns.load(.monotonic);
}
fn probePhysical() callconv(.c) u64 {
    return physical_ns.load(.monotonic);
}
fn probeQuota() callconv(.c) u64 {
    return quota_peak.load(.monotonic);
}
fn probeHits() callconv(.c) u64 {
    return cache_hits.load(.monotonic);
}
fn probeReads() callconv(.c) u64 {
    return cache_reads.load(.monotonic);
}
fn probeBytes() callconv(.c) u64 {
    return cache_bytes.load(.monotonic);
}
fn probeOptimize() callconv(.c) u8 {
    return switch (@import("builtin").mode) {
        .debug => 0,
        .fast => 1,
        .safe => 2,
        .small => 3,
    };
}
fn probeIsTest() callconv(.c) u8 {
    return @intFromBool(@import("builtin").is_test);
}
comptime {
    @export(&probeOptimize, .{ .name = "antfly_lake_probe_optimize", .visibility = .hidden });
    @export(&probeIsTest, .{ .name = "antfly_lake_probe_builtin_is_test", .visibility = .hidden });
    @export(&probeParallelJobs, .{ .name = "antfly_lake_probe_parallel_jobs", .visibility = .hidden });
    @export(&probeBulkNs, .{ .name = "antfly_lake_probe_bulk_ns", .visibility = .hidden });
    @export(&probeBulkCalls, .{ .name = "antfly_lake_probe_bulk_calls", .visibility = .hidden });
    @export(&probeFamilyNs, .{ .name = "antfly_lake_probe_family_ns", .visibility = .hidden });
    @export(&probeFamilyCalls, .{ .name = "antfly_lake_probe_family_calls", .visibility = .hidden });
    @export(&probeDecodedHits, .{ .name = "antfly_lake_probe_decoded_hits", .visibility = .hidden });
    @export(&probeSetIo, .{ .name = "antfly_lake_probe_set_linear_io", .visibility = .hidden });
    @export(&probeIoCalls, .{ .name = "antfly_lake_probe_io_calls", .visibility = .hidden });
    @export(&probeQuota, .{ .name = "antfly_lake_probe_quota_peak", .visibility = .hidden });
    @export(&probeHits, .{ .name = "antfly_lake_probe_cache_hits", .visibility = .hidden });
    @export(&probeReads, .{ .name = "antfly_lake_probe_cache_reads", .visibility = .hidden });
    @export(&probeBytes, .{ .name = "antfly_lake_probe_cache_bytes", .visibility = .hidden });
    @export(&probeReset, .{ .name = "antfly_lake_probe_reset", .visibility = .hidden });
    @export(&probeStore, .{ .name = "antfly_lake_probe_store_open_ns", .visibility = .hidden });
    @export(&probeSource, .{ .name = "antfly_lake_probe_source_open_ns", .visibility = .hidden });
    @export(&probePhysical, .{ .name = "antfly_lake_probe_physical_ns", .visibility = .hidden });
    @import("api/lake_execution_native_provider.zig").exportOperations();
}
