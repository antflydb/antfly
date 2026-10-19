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

//! Pretrained live-window adapter against complete-clip source-time inference.
const std = @import("std");
const internal = @import("inference_internal");
const media = @import("antfly_media");
const video = @import("antfly_video");
const factory = internal.architectures.session_factory;
const pipeline = internal.pipelines.embedding_gemma2;
const Case = struct { id: []const u8, path: []const u8, sha256: ?[]const u8 = null, init_end: usize, fragment_ends: []const usize, duration_ns: i64 = 2 * std.time.ns_per_s, qualify_reader_cache: bool = false, advance_ns: ?u64 = null, incremental_validation: bool = true };
const Query = struct { id: []const u8, text: []const u8 };
const Suite = struct { cases: []const Case, queries: []const Query = &.{} };
const QueryResult = struct { id: []const u8, vector: []f32 };
const Measurement = struct {
    case: []const u8,
    ordinal: usize,
    epoch: usize,
    start_ns: i64,
    end_ns: i64,
    input_tokens: usize,
    stream_seconds: f64,
    baseline_seconds: f64,
    coordinator_validation_walks: usize,
    coordinator_metadata_snapshots: usize,
    coordinator_validation_packets: usize,
    coordinator_validation_payload_bytes: u64,
    live_cache_hits: usize,
    live_cache_stored: usize,
    max_abs: f32,
    cosine: f64,
    vector: []f32,
    baseline_vector: []f32,
};
const CacheMeasurement = struct { case: []const u8, hits: usize, stored: usize, max_abs: f32, exact: bool, tokens: usize, cached_seconds: f64, uncached_seconds: f64 };
const Reader = struct {
    ingest: *media.live_mp4.Ingest,
    bytes: []const u8,
    ends: []const usize,
    begin: usize,
    at: usize = 0,
    pub fn next(self: *@This()) !?media.live_mp4.Segment {
        if (self.at == self.ends.len) return null;
        const end = self.ends[self.at];
        try self.ingest.push(self.bytes[self.begin..end]);
        const unit = try self.ingest.finishSegment();
        self.begin = end;
        self.at += 1;
        return unit;
    }
    pub fn ended(self: *const @This()) bool {
        return self.at == self.ends.len;
    }
};
fn now() u64 {
    return internal.platform.time.monotonicNs();
}
fn seconds(start: u64) f64 {
    return @as(f64, @floatFromInt(now() - start)) / std.time.ns_per_s;
}
pub fn main(init: std.process.Init) !void {
    var lifetime = internal.platform.inference_process_supervisor.WorkerLifetime{};
    defer lifetime.deinit(init.io);
    if (try internal.platform.inference_process_supervisor.runIfNeeded(init, 1, &lifetime)) return;
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const args = try init.minimal.args.toSlice(a);
    var model: ?[]const u8 = null;
    var suite_path: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    var metal = false;
    var i: usize = 1;
    while (i < args.len) : (i += 2) {
        if (i + 1 >= args.len) return error.MissingArgument;
        const value = args[i + 1];
        if (std.mem.eql(u8, args[i], "--model")) model = value else if (std.mem.eql(u8, args[i], "--suite")) suite_path = value else if (std.mem.eql(u8, args[i], "--output")) output = value else if (std.mem.eql(u8, args[i], "--backend")) {
            if (!std.mem.eql(u8, value, "native") and !std.mem.eql(u8, value, "metal")) return error.InvalidBackend;
            metal = std.mem.eql(u8, value, "metal");
        } else return error.UnknownArgument;
    }
    const model_path = model orelse return error.MissingModel;
    const raw_suite = try std.Io.Dir.cwd().readFileAlloc(init.io, suite_path orelse return error.MissingSuite, a, .limited(1024 * 1024));
    const parsed = try std.json.parseFromSlice(Suite, a, raw_suite, .{ .ignore_unknown_fields = true });
    if (parsed.value.cases.len == 0 or parsed.value.cases.len > 32) return error.InvalidSuite;
    const watchdog = try internal.hard_cancellation_watchdog.HardCancellationWatchdog.create(std.heap.c_allocator);
    defer watchdog.destroy();
    try watchdog.start(init.io);
    const control = internal.execution_control.InferenceExecutionControl{ .io = init.io, .hard_cancellation = watchdog.boundary() };
    var load_guard = if (metal) try control.enterUninterruptible(.uninterruptible) else internal.execution_control.UninterruptibleGuard{};
    defer load_guard.deinit();
    const session = if (metal) try factory.createMetalSession(a, model_path) else try factory.createNativeSession(a, model_path);
    defer session.close();
    factory.attachIo(session, init.io);
    const tokenizer = try internal.server.model_manager.loadHuggingFaceTokenizerFromDir(a, model_path);
    defer tokenizer.deinitSelf();
    var results = std.ArrayList(Measurement).empty;
    var cache_results = std.ArrayList(CacheMeasurement).empty;
    for (parsed.value.cases) |case| {
        const fixture_path = try std.fs.path.resolve(a, &.{ std.fs.path.dirname(suite_path.?) orelse ".", case.path });
        const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, fixture_path, a, .limited(64 * 1024 * 1024));
        if (case.sha256) |expected| {
            if (expected.len != 64) return error.InvalidFixtureHash;
            var digest: [32]u8 = undefined;
            _ = std.fmt.hexToBytes(&digest, expected) catch return error.InvalidFixtureHash;
            if (!std.mem.eql(u8, &digest, &fixtureDigest(bytes))) return error.FixtureChanged;
        }
        if (case.init_end == 0 or case.init_end >= bytes.len or case.fragment_ends.len == 0 or case.fragment_ends.len > 256) return error.InvalidFixture;
        var previous = case.init_end;
        for (case.fragment_ends) |end| {
            if (end <= previous or end > bytes.len) return error.InvalidFixture;
            previous = end;
        }
        if (previous != bytes.len) return error.InvalidFixture;
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        var ingest = try media.live_mp4.Ingest.init(std.heap.c_allocator, case.id, bytes[0..case.init_end], .{ .stream = .{ .admission_pool = &pool } });
        defer ingest.deinit();
        var reader = Reader{ .ingest = &ingest, .bytes = bytes, .ends = case.fragment_ends, .begin = case.init_end };
        var coordinator = try video.live_coordinator.Coordinator.init(std.heap.c_allocator, .{ .duration_ns = case.duration_ns, .advance_ns = case.advance_ns, .incremental_validation = case.incremental_validation, .window = .{ .stream = .{ .admission_pool = &pool } } });
        defer coordinator.deinit();
        var live_cache = pipeline.FrameCache{ .allocator = std.heap.c_allocator, .session = session, .control = control };
        defer live_cache.deinit();
        var windows: usize = 0;
        while (true) {
            const started = now();
            const event = (try pipeline.embedNextLiveCached(std.heap.c_allocator, session, tokenizer.tokenizer(), &coordinator, &reader, null, .{}, null, control, if (case.qualify_reader_cache) &live_cache else null)) orelse break;
            const stream_seconds = seconds(started);
            const result = event.result orelse return error.UnexpectedGap;
            defer std.heap.c_allocator.free(result.vector);
            const baseline_start = now();
            const baseline = try pipeline.embed(std.heap.c_allocator, session, tokenizer.tokenizer(), .{ .content = &.{.{ .sampled_video = .{ .bytes = bytes, .sampling = .{ .source_timestamps = event.policy } } }} }, .{}, null, control);
            defer std.heap.c_allocator.free(baseline.vector);
            const baseline_seconds = seconds(baseline_start);
            if (baseline.input_tokens != result.input_tokens or baseline.vector.len != result.vector.len) return error.TokenMismatch;
            var max_abs: f32 = 0;
            var dot: f64 = 0;
            var left: f64 = 0;
            var right: f64 = 0;
            for (result.vector, baseline.vector) |x, y| {
                if (!std.math.isFinite(x) or !std.math.isFinite(y)) return error.NonFiniteVector;
                max_abs = @max(max_abs, @abs(x - y));
                dot += @as(f64, x) * y;
                left += @as(f64, x) * x;
                right += @as(f64, y) * y;
            }
            const cosine = dot / @sqrt(left * right);
            if (max_abs > 0.00001 or cosine < 0.999999) return error.StreamParityFailed;
            try results.append(a, .{ .case = case.id, .ordinal = event.ordinal, .epoch = event.epoch, .start_ns = event.source_start_ns, .end_ns = event.source_end_ns, .input_tokens = result.input_tokens, .stream_seconds = stream_seconds, .baseline_seconds = baseline_seconds, .coordinator_validation_walks = coordinator.stats.validation_walks, .coordinator_metadata_snapshots = coordinator.stats.metadata_snapshots, .coordinator_validation_packets = coordinator.stats.validation_packets, .coordinator_validation_payload_bytes = coordinator.stats.validation_payload_bytes, .live_cache_hits = live_cache.stats.hits, .live_cache_stored = live_cache.stats.stored, .max_abs = max_abs, .cosine = cosine, .vector = try a.dupe(f32, result.vector), .baseline_vector = try a.dupe(f32, baseline.vector) });
            windows += 1;
            std.debug.print("live {s}/{d}: tokens={d} max_abs={d} seconds={d}\n", .{ case.id, event.ordinal, result.input_tokens, max_abs, stream_seconds });
        }
        if (windows == 0 or !reader.ended()) return error.IncompleteStream;
        if (case.qualify_reader_cache) {
            const ca = std.heap.c_allocator;
            var input = media.source.Source{ .allocator = ca, .storage = .{ .borrowed = bytes }, .identity = case.id };
            var complete = try media.mp4.Reader.init(ca, &input, .{});
            defer complete.deinit();
            var logical = try video.h264_pictures.streaming(ca, &complete, .{});
            defer logical.deinit();
            const begin = try media.timeline.rescale(logical.pictures[0].pts, logical.timescale, std.time.ns_per_s);
            const first_sampling = @TypeOf(@as(pipeline.SampledVideo, undefined).sampling){ .source_timestamps = .{ .start_ns = begin, .end_ns = begin + 2 * std.time.ns_per_s } };
            const next_sampling = @TypeOf(@as(pipeline.SampledVideo, undefined).sampling){ .source_timestamps = .{ .start_ns = begin + std.time.ns_per_s, .end_ns = begin + 3 * std.time.ns_per_s } };
            var first_selection = try pipeline.ReaderSelection.init(ca, &complete, first_sampling, &logical, null, .{});
            defer first_selection.deinit();
            var next_selection = try pipeline.ReaderSelection.init(ca, &complete, next_sampling, &logical, null, .{});
            defer next_selection.deinit();
            var cache = pipeline.FrameCache{ .allocator = ca, .session = session, .control = control };
            defer cache.deinit();
            const first_group = [_]pipeline.Part{.{ .sampled_video = .{ .reader = &complete, .sampling = first_sampling, .selection = &first_selection } }};
            const next_group = [_]pipeline.Part{.{ .sampled_video = .{ .reader = &complete, .sampling = next_sampling, .selection = &next_selection } }};
            const primed = try pipeline.embedCached(ca, session, tokenizer.tokenizer(), .{ .content = &first_group }, .{}, null, control, &cache);
            defer ca.free(primed.vector);
            const cached_start = now();
            const cached = try pipeline.embedCached(ca, session, tokenizer.tokenizer(), .{ .content = &next_group }, .{}, null, control, &cache);
            defer ca.free(cached.vector);
            const cached_seconds = seconds(cached_start);
            const baseline_start = now();
            const uncached = try pipeline.embed(ca, session, tokenizer.tokenizer(), .{ .content = &next_group }, .{}, null, control);
            defer ca.free(uncached.vector);
            const uncached_seconds = seconds(baseline_start);
            if (cached.input_tokens != uncached.input_tokens or cache.stats.hits == 0) return error.ReaderCacheParityFailed;
            var max_abs: f32 = 0;
            for (cached.vector, uncached.vector) |x, y| {
                if (!std.math.isFinite(x) or !std.math.isFinite(y)) return error.NonFiniteVector;
                max_abs = @max(max_abs, @abs(x - y));
            }
            if (max_abs > 0.00001) return error.ReaderCacheParityFailed;
            try cache_results.append(a, .{ .case = case.id, .hits = cache.stats.hits, .stored = cache.stats.stored, .max_abs = max_abs, .exact = std.mem.eql(f32, cached.vector, uncached.vector), .tokens = cached.input_tokens, .cached_seconds = cached_seconds, .uncached_seconds = uncached_seconds });
        }
    }
    var queries = std.ArrayList(QueryResult).empty;
    for (parsed.value.queries) |query| {
        const result = try pipeline.embed(std.heap.c_allocator, session, tokenizer.tokenizer(), .{ .content = &.{.{ .text = query.text }} }, .{ .task_type = "RETRIEVAL_QUERY" }, null, control);
        defer std.heap.c_allocator.free(result.vector);
        try queries.append(a, .{ .id = query.id, .vector = try a.dupe(f32, result.vector) });
    }
    const report = try std.json.Stringify.valueAlloc(a, .{ .status = "pass", .backend = if (metal) "metal" else "native", .timing_scope = "completed live adapter and complete-clip baseline; diagnostic shared-host timing", .cases = results.items, .queries = queries.items, .reader_cache = cache_results.items }, .{ .whitespace = .indent_2 });
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = output orelse return error.MissingOutput, .data = report });
}

fn fixtureDigest(bytes: []const u8) [32]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return digest;
}
