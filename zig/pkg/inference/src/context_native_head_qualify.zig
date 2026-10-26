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

//! Opt-in whole-head diagnostic on immutable captured Gemma KV and real
//! post-RoPE train/held queries. This does not authorize a serving profile.
const std = @import("std");
const native = @import("backends/context_attention_match.zig");
const solver = @import("runtime/kv/attention_match.zig");

/// Little endian, followed by f16 K, f16 V, f32 training Q, f32 held Q.
/// The external manifest must bind the complete head, every consumer, roles,
/// source provenance, and this immutable input hash before execution.
pub const Header = extern struct {
    magic: [8]u8 = "AMHEAD02".*,
    source_rows: u32,
    head_dim: u32,
    training_rows: u32,
    validation_rows: u32,
    validation_group_rows: u32,
    prefix_rows: u32,
    recent_rows: u32,
    initial_divisor: u32 = 4,
    min_divisor: u32 = 2,
    continuation_rows: u32,
    max_retained_rows: u32,
    staging_budget_mib: u32 = 512,
    scale: f32,
    softcap: f32 = 0,

    fn validate(self: Header) !void {
        if (!std.mem.eql(u8, &self.magic, "AMHEAD02") or self.source_rows == 0 or self.source_rows > 131072 or
            self.head_dim == 0 or self.head_dim > 512 or self.head_dim % 2 != 0 or
            self.training_rows == 0 or self.validation_rows == 0 or self.training_rows +| self.validation_rows > 4096 or
            self.validation_group_rows == 0 or self.validation_rows % self.validation_group_rows != 0 or
            self.training_rows % (self.validation_rows / self.validation_group_rows) != 0 or
            self.prefix_rows >= self.source_rows or self.recent_rows >= self.source_rows - self.prefix_rows or
            (self.source_rows - self.prefix_rows - self.recent_rows) % 512 != 0 or
            self.initial_divisor < 2 or self.initial_divisor > 4 or self.min_divisor < 2 or self.min_divisor > self.initial_divisor or
            self.continuation_rows == 0 or self.max_retained_rows <= self.continuation_rows or self.staging_budget_mib == 0 or self.staging_budget_mib > 1024 or
            !std.math.isFinite(self.scale) or self.scale <= 0 or !std.math.isFinite(self.softcap) or self.softcap < 0)
            return error.InvalidNativeHeadInput;
    }
};
comptime {
    std.debug.assert(@sizeOf(Header) == 64);
}

const Attempt = struct {
    divisor: u32,
    retained_rows: u32,
    outcome: []const u8,
    elapsed_ns: u64 = 0,
    blocks: []const BlockAttempt = &.{},
    metrics: ?native.HeadMetrics = null,
    reference: ?native.HeadMetrics = null,
};

const SolverDiagnostics = struct {
    quality_evaluated: bool,
    nnls_sweeps: usize,
    selection_steps: usize,
    selection_active_solves: usize,
};

const BlockAttempt = struct {
    block: u32,
    logical_start: u32,
    retained_rows: u32,
    outcome: []const u8,
    elapsed_ns: u64,
    solver: ?SolverDiagnostics = null,
};

fn solverDiagnostics(metrics: solver.Metrics) SolverDiagnostics {
    return .{
        .quality_evaluated = metrics.quality_evaluated,
        .nnls_sweeps = metrics.nnls_sweeps,
        .selection_steps = metrics.selection_steps,
        .selection_active_solves = metrics.selection_active_solves,
    };
}

fn elapsedNs(started: std.Io.Timestamp) u64 {
    return @intCast(@max(1, started.durationTo(std.Io.Timestamp.now(std.testing.io, .awake)).nanoseconds));
}

fn isNumericalFitRejection(err: anyerror) bool {
    return switch (err) {
        error.MassFitDidNotConverge, error.SingularValueFit, error.CompactionQualityRejected => true,
        else => false,
    };
}

fn halfSlice(bytes: []const u8) []align(1) const f16 {
    return std.mem.bytesAsSlice(f16, bytes);
}

fn referenceMetrics(a: std.mem.Allocator, header: Header, source_keys: []const u8, source_values: []const u8, queries: []const u8, keys: native.Buffer, values: native.Buffer, bias: native.Buffer) !native.HeadMetrics {
    const d: usize = header.head_dim;
    const source_k = try a.alloc(f64, source_keys.len / 2);
    defer a.free(source_k);
    const source_v = try a.alloc(f64, source_values.len / 2);
    defer a.free(source_v);
    const compact_k = try a.alloc(f64, keys.logical_bytes / 2);
    defer a.free(compact_k);
    const compact_v = try a.alloc(f64, values.logical_bytes / 2);
    defer a.free(compact_v);
    const compact_bias = try a.alloc(f64, bias.logical_bytes / 4);
    defer a.free(compact_bias);
    const qs = try a.alloc(f64, queries.len / 4);
    defer a.free(qs);
    for (source_k, halfSlice(source_keys)) |*dst, src| dst.* = src;
    for (source_v, halfSlice(source_values)) |*dst, src| dst.* = src;
    for (compact_k, halfSlice(keys.bytes[0..keys.logical_bytes])) |*dst, src| dst.* = src;
    for (compact_v, halfSlice(values.bytes[0..values.logical_bytes])) |*dst, src| dst.* = src;
    for (compact_bias, bias.floats()) |*dst, src| dst.* = src;
    for (qs, std.mem.bytesAsSlice(f32, queries)) |*dst, src| dst.* = src;
    const source = solver.Head{ .keys = source_k, .values = source_v, .rows = header.source_rows, .dim = d, .scale = header.scale, .softcap = header.softcap };
    const compact = solver.Head{ .keys = compact_k, .values = compact_v, .bias = compact_bias, .rows = compact_bias.len, .dim = d, .scale = header.scale, .softcap = header.softcap };
    const pooled = try solver.validationMetrics(a, source, compact, qs, .{ .target_rows = compact.rows });
    var result = native.HeadMetrics{ .queries = header.validation_rows, .consumer_groups = header.validation_rows / header.validation_group_rows, .max_mass_error = pooled.max_relative_mass_error, .output_rmse = pooled.relative_output_rmse };
    for (0..result.consumer_groups) |group| {
        const count = @as(usize, header.validation_group_rows) * d;
        const metrics = try solver.validationMetrics(a, source, compact, qs[group * count ..][0..count], .{ .target_rows = compact.rows });
        result.max_consumer_output_rmse = @max(result.max_consumer_output_rmse, metrics.relative_output_rmse);
    }
    return result;
}

fn create(prefix: []const u8, suffix: []const u8) !std.Io.File {
    const name = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}", .{ prefix, suffix });
    defer std.testing.allocator.free(name);
    return std.Io.Dir.cwd().createFile(std.testing.io, name, .{ .exclusive = true });
}

fn runCapture(path: []const u8, expected: [32]u8, output: []const u8) !void {
    const a = std.testing.allocator;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, a, .limited(512 * 1024 * 1024));
    defer a.free(bytes);
    var observed: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &observed, .{});
    if (!std.meta.eql(expected, observed) or bytes.len < @sizeOf(Header)) return error.InvalidNativeHeadInput;
    const header = std.mem.bytesToValue(Header, bytes[0..@sizeOf(Header)]);
    try header.validate();
    const dim: usize = header.head_dim;
    const lengths = [_]usize{ @as(usize, header.source_rows) * dim * 2, @as(usize, header.source_rows) * dim * 2, @as(usize, header.training_rows) * dim * 4, @as(usize, header.validation_rows) * dim * 4 };
    var length: usize = @sizeOf(Header);
    for (lengths) |n| length = try std.math.add(usize, length, n);
    if (bytes.len != length) return error.InvalidNativeHeadInput;
    var kernels = try native.Kernels.initTracked(null);
    defer kernels.deinit();
    var source: [4]native.Buffer = undefined;
    var initialized: usize = 0;
    defer for (source[0..initialized]) |*buffer| buffer.deinit();
    var offset: usize = @sizeOf(Header);
    for (lengths, 0..) |n, i| {
        source[i] = try kernels.core.buffer(n);
        initialized += 1;
        @memcpy(source[i].bytes[0..n], bytes[offset..][0..n]);
        offset += n;
    }
    const chunks = (header.source_rows - header.prefix_rows - header.recent_rows) / 512;
    if (chunks > 256) return error.InvalidNativeHeadInput;
    const row_bytes = dim * 2;
    const budget = @as(usize, header.staging_budget_mib) * 1024 * 1024;
    var attempts: [3]Attempt = undefined;
    var block_records: [3][256]BlockAttempt = undefined;
    var count: usize = 0;
    var accepted = false;
    var abort_ladder = false;
    var terminal_failure: ?[]const u8 = null;
    var diagnostic_candidate_readback: usize = 0;
    var candidate_sha256: ?[32]u8 = null;
    var candidate_bytes: u64 = 0;
    var divisor = header.initial_divisor;
    while (true) : (divisor -= 1) {
        const attempt_started = std.Io.Timestamp.now(std.testing.io, .awake);
        const attempt_index = count;
        const retained = (512 + divisor - 1) / divisor;
        const rows = header.prefix_rows + chunks * retained + header.recent_rows;
        attempts[count] = .{ .divisor = divisor, .retained_rows = rows, .outcome = "not_attempted", .blocks = block_records[attempt_index][0..0] };
        const attempt = &attempts[count];
        count += 1;
        if (rows +| header.continuation_rows > header.max_retained_rows) {
            attempt.outcome = "retained_row_budget_exceeded";
        } else candidate: {
            var keys = try kernels.core.buffer(@as(usize, rows) * row_bytes);
            defer keys.deinit();
            var values = try kernels.core.buffer(keys.logical_bytes);
            defer values.deinit();
            var bias = try kernels.core.buffer(@as(usize, rows) * 4);
            defer bias.deinit();
            @memset(bias.bytes, 0);
            const prefix = @as(usize, header.prefix_rows) * row_bytes;
            @memcpy(keys.bytes[0..prefix], source[0].bytes[0..prefix]);
            @memcpy(values.bytes[0..prefix], source[1].bytes[0..prefix]);
            var block_count: usize = 0;
            for (0..chunks) |chunk| {
                const start = @as(usize, header.prefix_rows) + chunk * 512;
                const target = @as(usize, header.prefix_rows) + chunk * retained;
                const block_started = std.Io.Timestamp.now(std.testing.io, .awake);
                var fitted = kernels.fit(a, .{ .retained_rows = retained, .head_dim = header.head_dim, .training_rows = header.training_rows, .scale = header.scale, .softcap = header.softcap, .staging_budget_bytes = budget }, try source[0].ref().slice(start * row_bytes, 512 * row_bytes), try source[1].ref().slice(start * row_bytes, 512 * row_bytes), source[2].ref()) catch |err| {
                    if (!isNumericalFitRejection(err)) return err;
                    const name = @errorName(err);
                    block_records[attempt_index][block_count] = .{ .block = @intCast(chunk), .logical_start = @intCast(start), .retained_rows = retained, .outcome = name, .elapsed_ns = elapsedNs(block_started) };
                    block_count += 1;
                    attempt.blocks = block_records[attempt_index][0..block_count];
                    attempt.outcome = name;
                    terminal_failure = name;
                    break :candidate;
                };
                defer fitted.deinit();
                block_records[attempt_index][block_count] = .{ .block = @intCast(chunk), .logical_start = @intCast(start), .retained_rows = retained, .outcome = "fitted", .elapsed_ns = elapsedNs(block_started), .solver = solverDiagnostics(fitted.metrics) };
                block_count += 1;
                attempt.blocks = block_records[attempt_index][0..block_count];
                try kernels.stage(&fitted, try keys.ref().slice(target * row_bytes, @as(usize, retained) * row_bytes), try values.ref().slice(target * row_bytes, @as(usize, retained) * row_bytes), try bias.ref().slice(target * 4, @as(usize, retained) * 4));
            }
            const tail = @as(usize, header.recent_rows) * row_bytes;
            const source_end = source[0].logical_bytes;
            @memcpy(keys.bytes[keys.logical_bytes - tail ..][0..tail], source[0].bytes[source_end - tail ..][0..tail]);
            @memcpy(values.bytes[values.logical_bytes - tail ..][0..tail], source[1].bytes[source_end - tail ..][0..tail]);
            const metrics = kernels.validateHead(.{ .source_rows = header.source_rows, .candidate_rows = rows, .head_dim = header.head_dim, .validation_rows = header.validation_rows, .validation_group_rows = header.validation_group_rows, .scale = header.scale, .softcap = header.softcap, .staging_budget_bytes = budget }, source[0].ref(), source[1].ref(), source[3].ref(), keys.ref(), values.ref(), bias.ref()) catch |err| switch (err) {
                error.CompactionQualityRejected => kernels.last_metrics.?,
                else => return err,
            };
            attempt.metrics = metrics;
            const reference = try referenceMetrics(a, header, source[0].bytes[0..source[0].logical_bytes], source[1].bytes[0..source[1].logical_bytes], source[3].bytes[0..source[3].logical_bytes], keys, values, bias);
            attempt.reference = reference;
            diagnostic_candidate_readback += keys.logical_bytes + values.logical_bytes + bias.logical_bytes;
            if (@abs(metrics.max_mass_error - reference.max_mass_error) > 2e-5 or @abs(metrics.output_rmse - reference.output_rmse) > 2e-5 or @abs(metrics.max_consumer_output_rmse - reference.max_consumer_output_rmse) > 2e-5) {
                attempt.outcome = "numerical_parity_rejected";
                terminal_failure = "NativeHeadNumericalParityFailed";
                abort_ladder = true;
                break :candidate;
            }
            accepted = @max(metrics.max_mass_error, reference.max_mass_error) <= 0.05 and
                @max(metrics.output_rmse, reference.output_rmse) <= 0.10 and @max(metrics.max_consumer_output_rmse, reference.max_consumer_output_rmse) <= 0.10;
            attempt.outcome = if (accepted) "accepted_head" else "whole_head_quality_rejected";
            if (!accepted) terminal_failure = "whole_head_quality_rejected";
            if (accepted) {
                var file = try create(output, ".candidate");
                defer file.close(std.testing.io);
                var candidate_hash = std.crypto.hash.sha2.Sha256.init(.{});
                for ([_][]const u8{ keys.bytes[0..keys.logical_bytes], values.bytes[0..values.logical_bytes], bias.bytes[0..bias.logical_bytes] }) |data| {
                    try file.writeStreamingAll(std.testing.io, data);
                    candidate_hash.update(data);
                    candidate_bytes = try std.math.add(u64, candidate_bytes, @intCast(data.len));
                }
                try file.sync(std.testing.io);
                const expected_candidate_bytes = try std.math.mul(u64, @as(u64, rows), try std.math.add(u64, try std.math.mul(u64, 4, @as(u64, header.head_dim)), 4));
                if (candidate_bytes != expected_candidate_bytes) return error.InvalidNativeHeadCandidate;
                candidate_sha256 = candidate_hash.finalResult();
            }
        }
        attempt.elapsed_ns = elapsedNs(attempt_started);
        if (accepted or abort_ladder or divisor == header.min_divisor) break;
    }
    const input_hex = std.fmt.bytesToHex(expected, .lower);
    var candidate_hex: [64]u8 = undefined;
    const candidate_sha256_text: ?[]const u8 = if (candidate_sha256) |digest| blk: {
        candidate_hex = std.fmt.bytesToHex(digest, .lower);
        break :blk &candidate_hex;
    } else null;
    const json = try std.json.Stringify.valueAlloc(a, .{
        .schema = "antfly.context.native.head-qualification.v2",
        .recipe = native.recipe,
        .accepted_head = accepted,
        .outcome = if (accepted) "accepted_head" else "preserved_original",
        .terminal_failure = if (accepted) null else terminal_failure,
        .serving_qualified = false,
        .full_context_memory_qualified = false,
        .input_sha256 = input_hex[0..],
        .source_rows = header.source_rows,
        .head_dim = header.head_dim,
        .training_rows = header.training_rows,
        .validation_rows = header.validation_rows,
        .consumer_groups = header.validation_rows / header.validation_group_rows,
        .exact_prefix_rows = header.prefix_rows,
        .exact_recent_rows = header.recent_rows,
        .continuation_rows = header.continuation_rows,
        .retained_row_cap = header.max_retained_rows,
        .attempts = attempts[0..count],
        .derived_attention_readback_bytes = kernels.derived_readback_bytes,
        .validation_readback_bytes = kernels.status_readback_bytes,
        .diagnostic_candidate_readback_bytes = diagnostic_candidate_readback,
        .candidate_sha256 = candidate_sha256_text,
        .candidate_bytes = candidate_bytes,
    }, .{});
    defer a.free(json);
    var report = try create(output, ".json");
    defer report.close(std.testing.io);
    try report.writeStreamingAll(std.testing.io, json);
    try report.sync(std.testing.io);
    var report_sha256: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(json, &report_sha256, .{});
    const report_hex = std.fmt.bytesToHex(report_sha256, .lower);
    std.debug.print("context_native_head_artifacts: report_sha256={s} candidate_sha256={s} candidate_bytes={d}\n", .{ report_hex[0..], candidate_sha256_text orelse "none", candidate_bytes });
    std.debug.print("context_native_head: accepted={} attempts={d} serving_qualified=false\n", .{ accepted, count });
    if (abort_ladder) return error.NativeHeadNumericalParityFailed;
    if (!accepted) return error.NativeHeadQualityRejected;
}

test "context native head qualification entrypoint compiles and rejects invalid geometry" {
    std.mem.doNotOptimizeAway(&runCapture);
    std.mem.doNotOptimizeAway(&@import("context_query_campaign_capture.zig").Capture.writeNativeFullHeadsDiagnostic);
    std.mem.doNotOptimizeAway(&@import("context_query_campaign_capture.zig").Capture.writeNativeRepeatDiagnostic);
    var header = Header{ .source_rows = 1025, .head_dim = 512, .training_rows = 24, .validation_rows = 8, .validation_group_rows = 2, .prefix_rows = 1, .recent_rows = 512, .continuation_rows = 32, .max_retained_rows = 1028, .scale = 0.044194174 };
    try header.validate();
    header.prefix_rows += 1;
    try std.testing.expectError(error.InvalidNativeHeadInput, header.validate());
}

test "context native head diagnostics classify and serialize numerical failures" {
    try std.testing.expect(isNumericalFitRejection(error.SingularValueFit));
    try std.testing.expect(isNumericalFitRejection(error.MassFitDidNotConverge));
    try std.testing.expect(!isNumericalFitRejection(error.OutOfMemory));
    const blocks = [_]BlockAttempt{.{
        .block = 3,
        .logical_start = 2048,
        .retained_rows = 128,
        .outcome = "SingularValueFit",
        .elapsed_ns = 17,
    }};
    const attempts = [_]Attempt{.{
        .divisor = 4,
        .retained_rows = 4320,
        .outcome = "SingularValueFit",
        .elapsed_ns = 19,
        .blocks = &blocks,
    }};
    const json = try std.json.Stringify.valueAlloc(std.testing.allocator, .{
        .accepted_head = false,
        .outcome = "preserved_original",
        .terminal_failure = @as(?[]const u8, "SingularValueFit"),
        .attempts = &attempts,
    }, .{});
    defer std.testing.allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"terminal_failure\":\"SingularValueFit\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"block\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"logical_start\":2048") != null);
}

test "context native solver diagnostics distinguish counters from quality" {
    const value = solverDiagnostics(.{
        .quality_evaluated = false,
        .nnls_sweeps = 7,
        .selection_steps = 128,
        .selection_active_solves = 2,
    });
    try std.testing.expect(!value.quality_evaluated);
    try std.testing.expectEqual(@as(usize, 7), value.nnls_sweeps);
    try std.testing.expectEqual(@as(usize, 128), value.selection_steps);
    try std.testing.expectEqual(@as(usize, 2), value.selection_active_solves);
}

test "context captured native whole-head Gemma qualification" {
    if (comptime !@import("build_options").enable_metal or @import("builtin").target.cpu.arch.endian() != .little) return error.SkipZigTest;
    const path = std.c.getenv("ANTFLY_CONTEXT_NATIVE_HEAD_INPUT") orelse return error.SkipZigTest;
    const output = std.c.getenv("ANTFLY_CONTEXT_NATIVE_HEAD_OUTPUT") orelse return error.MissingReplayCaptureInput;
    const sha = std.c.getenv("ANTFLY_CONTEXT_NATIVE_HEAD_SHA256") orelse return error.MissingReplayCaptureInput;
    var expected: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&expected, std.mem.span(sha)) catch return error.InvalidNativeHeadInput;
    try runCapture(std.mem.span(path), expected, std.mem.span(output));
}
