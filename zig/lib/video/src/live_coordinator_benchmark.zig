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

//! Completed live control-plane timing and exact constructed-window parity.
const std = @import("std");
const media = @import("antfly_media");
const video = @import("antfly_video");
const Case = struct { id: []const u8, path: []const u8, sha256: ?[]const u8 = null, init_end: usize = 0, fragment_ends: []const usize = &.{}, packet_units: bool = false, normalize_clock: bool = false, duration_ns: i64 = 2 * std.time.ns_per_s };
const Suite = struct { cases: []const Case };
const Sample = struct { append_ns: u64, prepare_ns: u64, digest_ns: u64, commit_ns: u64, completed_ns: u64, windows: usize, validation_walks: usize, metadata_snapshots: ?usize, admitted_peak_bytes: u64, digest: [32]u8 };
const Result = struct { case: []const u8, packet_units: bool, normalized_clock: bool, samples: []Sample };
var observed_walks: usize = 0;
// Benchmark-only source instrumentation resolves this private Zig symbol.
export fn antfly_live_benchmark_validation_walk() callconv(.c) void {
    observed_walks += 1;
}
fn now(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}
fn integer(hash: *std.crypto.hash.sha2.Sha256, value: anytype) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, @bitCast(@as(i64, @intCast(value))), .little);
    hash.update(&bytes);
}
fn packetUnit(a: std.mem.Allocator, case: Case, bytes: []const u8, at: usize, pool: *media.admission.Pool) !media.live_mp4.Segment {
    const identity = try a.dupe(u8, case.id);
    errdefer a.free(identity);
    const backing = try media.stream.Snapshot.copy(a, identity, &.{bytes}, .{ .admission_pool = pool });
    errdefer backing.deinit();
    var reader = try media.mp4.Reader.init(a, &backing.input, .{ .max_index_bytes = 256 * 1024 });
    errdefer reader.deinit();
    const selected = try a.dupe(media.mp4.Packet, &.{reader.packets[at]});
    a.free(reader.packets);
    reader.packets = selected;
    try reader.index_reservation.resize(.{ .host_bytes = @sizeOf(media.mp4.Packet) });
    if (case.normalize_clock) {
        reader.packets[0].dts = @intCast(at);
        reader.packets[0].pts = @intCast(at);
        reader.packets[0].duration = 1;
    }
    return .{ .backing = backing, .reader = reader, .identity = identity, .identity_reservation = .{}, .ordinal = at, .epoch = 0 };
}
fn run(a: std.mem.Allocator, io: std.Io, case: Case, bytes: []const u8) !Sample {
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var count: usize = 0;
    var append_ns: u64 = 0;
    var prepare_ns: u64 = 0;
    var digest_ns: u64 = 0;
    var commit_ns: u64 = 0;
    var walks: usize = 0;
    var snapshots: ?usize = null;
    observed_walks = 0;
    const started = now(io);
    {
        var original = media.source.Source{ .allocator = a, .identity = case.id, .storage = .{ .borrowed = bytes } };
        var packet_reader: ?media.mp4.Reader = if (case.packet_units) try media.mp4.Reader.init(a, &original, .{}) else null;
        defer if (packet_reader) |*reader| reader.deinit();
        var ingest: ?media.live_mp4.Ingest = if (case.packet_units) null else try media.live_mp4.Ingest.init(a, case.id, bytes[0..case.init_end], .{ .stream = .{ .admission_pool = &pool } });
        defer if (ingest) |*reader| reader.deinit();
        var flow_options = video.live_coordinator.Options{ .duration_ns = case.duration_ns, .window = .{ .stream = .{ .admission_pool = &pool } } };
        // This benchmark consumes metadata/encoded bytes, never prepared pixels.
        // Keep optional model frame reuse out of both comparison subjects.
        if (@hasField(@TypeOf(flow_options), "max_validated_frame_bytes")) flow_options.max_validated_frame_bytes = 0;
        var flow = try video.live_coordinator.Coordinator.init(a, flow_options);
        defer flow.deinit();
        const units = if (packet_reader) |reader| reader.packets.len else case.fragment_ends.len;
        var begin = case.init_end;
        for (0..units + 1) |at| {
            if (at == units) {
                try flow.finish();
            } else {
                var unit: ?media.live_mp4.Segment = if (case.packet_units) try packetUnit(a, case, bytes, at, &pool) else blk: {
                    try ingest.?.push(bytes[begin..case.fragment_ends[at]]);
                    begin = case.fragment_ends[at];
                    break :blk try ingest.?.finishSegment();
                };
                defer if (unit) |*owner| owner.deinit();
                const append_at = now(io);
                try flow.append(&unit);
                append_ns += now(io) - append_at;
            }
            while (true) {
                const prepare_at = now(io);
                const value = try flow.prepare();
                prepare_ns += now(io) - prepare_at;
                var prepared = value orelse break;
                defer prepared.deinit();
                const digest_at = now(io);
                integer(&hash, prepared.ordinal);
                integer(&hash, prepared.source_start_ns);
                integer(&hash, prepared.source_end_ns);
                integer(&hash, @intFromBool(prepared.has_pictures));
                integer(&hash, prepared.window.reader.packets.len);
                for (prepared.window.reader.packets, 0..) |packet, id| {
                    integer(&hash, packet.pts);
                    integer(&hash, packet.dts);
                    integer(&hash, packet.duration);
                    integer(&hash, packet.size);
                    var payload = try prepared.window.reader.readPacket(id);
                    defer payload.deinit();
                    hash.update(payload.bytes);
                }
                digest_ns += now(io) - digest_at;
                const commit_at = now(io);
                try flow.commit(&prepared);
                commit_ns += now(io) - commit_at;
                count += 1;
            }
        }
        if (@hasField(@TypeOf(flow), "stats")) {
            walks = flow.stats.validation_walks;
            if (observed_walks != 0 and observed_walks != walks) return error.ValidationCounterMismatch;
            snapshots = flow.stats.metadata_snapshots;
        } else {
            if (observed_walks == 0) return error.ValidationInstrumentationRequired;
            walks = observed_walks;
        }
    }
    if (pool.snapshot().host_bytes != 0 or count == 0) return error.IncompleteBenchmark;
    return .{ .append_ns = append_ns, .prepare_ns = prepare_ns, .digest_ns = digest_ns, .commit_ns = commit_ns, .completed_ns = now(io) - started, .windows = count, .validation_walks = walks, .metadata_snapshots = snapshots, .admitted_peak_bytes = pool.high_water.host_bytes, .digest = hash.finalResult() };
}
pub fn main(init: std.process.Init) !void {
    const a = std.heap.c_allocator;
    const args = try init.minimal.args.toSlice(a);
    defer a.free(args);
    if (args.len != 4) return error.InvalidBenchmarkArguments;
    const iterations = try std.fmt.parseInt(usize, args[3], 10);
    if (iterations < 2 or iterations > 100) return error.InvalidBenchmarkArguments;
    const raw = try std.Io.Dir.cwd().readFileAlloc(init.io, args[1], a, .limited(1024 * 1024));
    defer a.free(raw);
    const suite = try std.json.parseFromSlice(Suite, a, raw, .{ .ignore_unknown_fields = true });
    defer suite.deinit();
    if (suite.value.cases.len == 0 or suite.value.cases.len > 32) return error.InvalidBenchmarkArguments;
    const results = try a.alloc(Result, suite.value.cases.len);
    defer a.free(results);
    var built: usize = 0;
    defer for (results[0..built]) |result| a.free(result.samples);
    for (suite.value.cases, results) |case, *result| {
        const fixture_path = try std.fs.path.resolve(a, &.{ std.fs.path.dirname(args[1]) orelse ".", case.path });
        defer a.free(fixture_path);
        const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, fixture_path, a, .limited(64 * 1024 * 1024));
        defer a.free(bytes);
        if (case.sha256) |expected| {
            if (expected.len != 64) return error.InvalidFixtureHash;
            var digest: [32]u8 = undefined;
            _ = std.fmt.hexToBytes(&digest, expected) catch return error.InvalidFixtureHash;
            var actual: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes, &actual, .{});
            if (!std.mem.eql(u8, &digest, &actual)) return error.FixtureChanged;
        }
        if (!case.packet_units) {
            if (case.init_end == 0 or case.init_end >= bytes.len or case.fragment_ends.len == 0) return error.InvalidFixture;
            var previous = case.init_end;
            for (case.fragment_ends) |end| {
                if (end <= previous or end > bytes.len) return error.InvalidFixture;
                previous = end;
            }
            if (previous != bytes.len) return error.InvalidFixture;
        }
        const samples = try a.alloc(Sample, iterations);
        errdefer a.free(samples);
        for (samples, 0..) |*sample, at| {
            sample.* = try run(a, init.io, case, bytes);
            if (at != 0 and !std.mem.eql(u8, &sample.digest, &samples[0].digest)) return error.WindowParityFailed;
        }
        result.* = .{ .case = case.id, .packet_units = case.packet_units, .normalized_clock = case.normalize_clock, .samples = samples };
        built += 1;
    }
    const output = try std.json.Stringify.valueAlloc(a, .{ .status = "pass", .scope = "completed serial live coordinator, owner release and exact constructed-window digest; file acquisition excluded; diagnostic shared-host timing", .cases = results }, .{ .whitespace = .indent_2 });
    defer a.free(output);
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = args[2], .data = output });
}
