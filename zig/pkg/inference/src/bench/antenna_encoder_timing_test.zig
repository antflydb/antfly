// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Encoder-only latency for the Antenna baselines (models/antenna/ANTENNA.md,
//! step 0): the GLiNER2.5 boundary encoder (DeBERTa-v3) against Laya's
//! ModernBERT-large encoder at about 64, 512 and 2048 tokens, batch 1.
//!
//! Run from zig/pkg/inference with a ReleaseFast test build:
//!
//!   ANTFLY_ANTENNA_TIMING_GLINER25=<gliner2.5-base dir> ANTFLY_ANTENNA_TIMING_LAYA=<laya dir> \
//!   ANTFLY_ANTENNA_TIMING_BACKEND=native|metal \
//!     zig build test -Doptimize=ReleaseFast -- --test-filter "antenna encoder timing"
//!
//! GLiNER2.5 is timed through its serving encoders: `encodeNative` on CPU and
//! `encodeDevice` on Metal (the resident `optimized_v2` policy when the
//! session admits it, else `reference_v1`; the line says which) on the
//! session's unmanaged backend, without the serving path's process-isolation
//! watchdog. Its input is
//! a real processor batch (a two-type entity schema plus repeated English
//! text), so the sequence includes the schema prefix. Laya is timed through
//! the generic ModernBERT forward on the session's backend, not its fused
//! decision kernels (which cover only unpacked rows up to 512 tokens), on the
//! first N ids of the same text. Each line is one JSON object: the median of
//! seven warm runs after two warmups.
const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const factory = @import("../architectures/session_factory.zig");
const engine = @import("../architectures/gliner/boundary_engine.zig");
const engine_device = @import("../architectures/gliner/boundary_engine_device.zig");
const modern = @import("../architectures/modern_bert.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const schema_mod = @import("../pipelines/extraction_schema.zig");
const c_file = @import("../util/c_file.zig");
const HfTokenizer = @import("inference_hf_tokenizer").HfTokenizer;
const Allocator = std.mem.Allocator;

const targets = [_]usize{ 64, 512, 2048 };
const warmups = 2;
const runs = 7;
const sentence = "The committee in Geneva reviewed the quarterly report from Acme Robotics, and Maria Lopez asked whether the new factory in Osaka would open before the winter holidays. ";

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts);
    return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec);
}

fn median(values: []u64) f64 {
    std.mem.sort(u64, values, {}, std.sort.asc(u64));
    return @as(f64, @floatFromInt(values[values.len / 2])) / std.time.ns_per_ms;
}

fn longText(a: Allocator, repeats: usize) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    for (0..repeats) |_| try out.appendSlice(a, sentence);
    return out.toOwnedSlice(a);
}

fn loadTokenizer(a: Allocator, dir: []const u8) !*HfTokenizer {
    const path = try std.fs.path.join(a, &.{ dir, "tokenizer.json" });
    defer a.free(path);
    const bytes = try c_file.readFile(a, path);
    defer a.free(bytes);
    return HfTokenizer.loadFromBytes(a, bytes);
}

/// The first whole-word prefix of `text` whose prepared batch reaches `target` tokens.
fn prepare(a: Allocator, tokenizer: @import("inference_tokenizer").Tokenizer, schema: *const schema_mod.CompiledSchema, text: []const u8, target: usize) !processor.PreparedBatch {
    var words: usize = @max(1, target / 2);
    while (true) : (words += @max(1, words / 16)) {
        var end: usize = 0;
        var seen: usize = 0;
        while (end < text.len and seen < words) : (end += 1) {
            if (text[end] == ' ') seen += 1;
        }
        var batch = try processor.prepare(a, tokenizer, &.{.{ .text = text[0..end], .schema = schema }}, .{});
        if (batch.sequence_length >= target or end >= text.len) return batch;
        batch.deinit();
    }
}

const Backend = enum { native, metal };

fn timeGliner(a: Allocator, dir: []const u8, backend: Backend) !void {
    const session = if (backend == .metal) try factory.createMetalSession(a, dir) else try factory.createNativeSession(a, dir);
    defer session.close();
    const config = try factory.getGlinerBoundaryConfig(session);
    const tokenizer = try loadTokenizer(a, dir);
    defer tokenizer.tokenizer().deinitTokenizer();
    var schema = try schema_mod.compile(a, "{\"entities\":[\"person\",\"organization\"]}", .{});
    defer schema.deinit();
    const text = try longText(a, 120);
    defer a.free(text);
    var resident = false;
    if (backend == .metal) {
        factory.prepareGlinerBoundaryResident(session, null) catch {};
        resident = factory.isGlinerBoundaryResidentReady(session);
    }
    for (targets) |target| {
        var prepared = try prepare(a, tokenizer.tokenizer(), &schema, text, target);
        defer prepared.deinit();
        var samples: [runs]u64 = undefined;
        var policy: []const u8 = "native";
        for (0..warmups + runs) |i| {
            const started = nowNs();
            if (backend == .native) {
                const cb = try factory.getComputeBackend(session, a);
                defer cb.deinit();
                var result = try engine.encodeNative(&cb, a, &config, &prepared, .{});
                result.deinit();
            } else {
                // Unmanaged offline use: no hard-cancellation watchdog, which
                // the managed (serving) backend requires on Metal.
                const cb = try factory.getComputeBackend(session, a);
                defer cb.deinit();
                var used_optimized = false;
                if (resident) optimized: {
                    var result = engine_device.encodeDevice(&cb, a, &config, &prepared, .{ .execution_policy = .optimized_v2 }) catch break :optimized;
                    result.deinit();
                    used_optimized = true;
                }
                if (!used_optimized) {
                    var result = try engine_device.encodeDevice(&cb, a, &config, &prepared, .{});
                    result.deinit();
                }
                policy = if (used_optimized) "optimized_v2" else "reference_v1";
            }
            const elapsed = nowNs() - started;
            if (i >= warmups) samples[i - warmups] = elapsed;
        }
        std.debug.print("{{\"model\":\"gliner2.5-base\",\"encoder\":\"deberta-v3-base\",\"backend\":\"{s}\",\"policy\":\"{s}\",\"target_tokens\":{d},\"tokens\":{d},\"median_ms\":{d:.2}}}\n", .{ @tagName(backend), policy, target, prepared.sequence_length, median(&samples) });
    }
}

fn timeLaya(a: Allocator, dir: []const u8, backend: Backend) !void {
    const session = if (backend == .metal) try factory.createMetalSession(a, dir) else try factory.createNativeSession(a, dir);
    defer session.close();
    const config_path = try std.fs.path.join(a, &.{ dir, "config.json" });
    defer a.free(config_path);
    const config_bytes = try c_file.readFile(a, config_path);
    defer a.free(config_bytes);
    const cfg = try modern.parseConfig(a, config_bytes);
    const tokenizer = try loadTokenizer(a, dir);
    defer tokenizer.tokenizer().deinitTokenizer();
    const text = try longText(a, 120);
    defer a.free(text);
    const encoded = try tokenizer.tokenizer().encode(a, text);
    defer a.free(encoded);
    const cb = try factory.getComputeBackend(session, a);
    defer cb.deinit();
    for (targets) |target| {
        const ids = try a.alloc(i64, target);
        defer a.free(ids);
        const mask = try a.alloc(i64, target);
        defer a.free(mask);
        for (ids, mask, 0..) |*id, *valid, i| {
            id.* = encoded[i];
            valid.* = 1;
        }
        var samples: [runs]u64 = undefined;
        for (0..warmups + runs) |i| {
            const started = nowNs();
            const hidden = try modern.forward(&cb, a, cfg, ids, mask, 1, target);
            a.free(hidden);
            const elapsed = nowNs() - started;
            if (i >= warmups) samples[i - warmups] = elapsed;
        }
        std.debug.print("{{\"model\":\"laya\",\"encoder\":\"modernbert-large\",\"backend\":\"{s}\",\"policy\":\"generic_forward\",\"target_tokens\":{d},\"tokens\":{d},\"median_ms\":{d:.2}}}\n", .{ @tagName(backend), target, target, median(&samples) });
    }
}

test "antenna encoder timing for GLiNER2.5 base and Laya ModernBERT-large" {
    if (builtin.mode != .ReleaseFast) return error.SkipZigTest;
    const gliner = platform.env.getenv("ANTFLY_ANTENNA_TIMING_GLINER25");
    const laya = platform.env.getenv("ANTFLY_ANTENNA_TIMING_LAYA");
    if (gliner == null and laya == null) return error.SkipZigTest;
    const name = platform.env.getenv("ANTFLY_ANTENNA_TIMING_BACKEND") orelse "native";
    const backend = std.meta.stringToEnum(Backend, name) orelse return error.InvalidTimingBackend;
    if (backend == .metal and !build_options.enable_metal) return error.SkipZigTest;
    const a = std.heap.c_allocator;
    if (gliner) |dir| try timeGliner(a, dir, backend);
    if (laya) |dir| try timeLaya(a, dir, backend);
}
