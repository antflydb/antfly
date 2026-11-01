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

//! Repeated source-verification stage only: query authorization, initial codec
//! verification, storage, cache-table overhead, transport and models excluded.
//! Compile with the API owner module graph; run with ANTFLY_BENCH_SOURCE_MEDIA=1.
const std = @import("std");
const Source = @import("inference_source.zig");

test "verified source repeated verification benchmark" {
    if (!@import("antfly_platform").env.getenvBool("ANTFLY_BENCH_SOURCE_MEDIA")) return error.SkipZigTest;
    var corpus = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer corpus.deinit();
    const a = corpus.allocator();
    for ([_]u32{ 16, 128, 512 }) |side| {
        const rgba = try a.alloc(u8, @as(usize, side) * side * 4);
        var random = std.Random.DefaultPrng.init(123);
        random.random().bytes(rgba);
        const codec = try @import("antfly_image").png.encodeRgba(a, side, side, rgba);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(codec, &digest, .{});
        const hex = std.fmt.bytesToHex(digest, .lower);
        const prefix = "data:image/png;base64,";
        const url = try a.alloc(u8, prefix.len + std.base64.standard.Encoder.calcSize(codec.len));
        @memcpy(url[0..prefix.len], prefix);
        _ = std.base64.standard.Encoder.encode(url[prefix.len..], codec);
        const reference: Source.Reference = .{ .table = "docs", .document_id = "row", .field = "image", .revision_field = "revision", .revision = "r1", .sha256 = &hex, .mime_type = "image/png", .byte_length = codec.len, .max_pixels = @as(u64, side) * side };
        // Both modes start after the first identical source was verified.
        const prior = try Source.verifyInfoRetained(a, reference, url, a);
        for (0..8) |round| for (0..2) |slot| {
            const reuse = (if (round % 2 == 0) slot else 1 - slot) == 1;
            var measured = std.testing.FailingAllocator.init(std.heap.smp_allocator, .{});
            const heap = measured.allocator();
            const start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
            for (0..32) |_| {
                if (reuse) {
                    if (!Source.matchesVerifiedUrl(reference, url, prior) or prior.pixels > reference.max_pixels) return error.SourceMismatch;
                } else {
                    const checked = try Source.verifyInfoRetained(heap, reference, url, heap);
                    defer heap.free(checked.data);
                    if (checked.bytes != prior.bytes or checked.pixels != prior.pixels) return error.SourceMismatch;
                }
            }
            const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - start;
            try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);
            std.debug.print("source_verification codec_bytes={d} round={d} mode={s} iterations=32 ns={d} allocated_bytes={d}\n", .{ codec.len, round, if (reuse) "reuse" else "verify", elapsed, measured.allocated_bytes });
        };
    }
}

test "verified source segmented envelope construction benchmark" {
    if (!@import("antfly_platform").env.getenvBool("ANTFLY_BENCH_SOURCE_MEDIA")) return error.SkipZigTest;
    const Execution = @import("antfly_inference_execution_context");
    var corpus = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer corpus.deinit();
    const a = corpus.allocator();
    for ([_]usize{ 68, 65536, 1048576 }) |size| {
        const codec = try a.alloc(u8, size);
        @memset(codec, 123);
        for ([_]usize{ 1, 2, 8 }) |count| {
            const Part = struct { type: []const u8 = "image_url", image_url: struct { url: []const u8 = "verified" } = .{} };
            const parts = try a.alloc(Part, count);
            @memset(parts, .{});
            const body = try std.json.Stringify.valueAlloc(a, .{ .model = "fixture", .messages = .{.{ .role = "user", .content = parts }} }, .{});
            const media = [_]Execution.VerifiedMedia{.{ .url = "verified", .bytes = codec, .mime_type = "image/png" }};
            for (0..8) |round| for (0..2) |slot| {
                const segmented = (if (round % 2 == 0) slot else 1 - slot) == 1;
                var measured = std.testing.FailingAllocator.init(std.heap.smp_allocator, .{});
                const heap = measured.allocator();
                var wire_bytes: usize = 0;
                const start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
                for (0..32) |_| {
                    var encoding = if (segmented)
                        try Execution.encodeVerifiedGenerateSegments(heap, heap, body, null, &media)
                    else
                        try Execution.encodeVerifiedGenerate(heap, heap, body, null, &media);
                    defer encoding.deinit(heap);
                    wire_bytes = if (encoding.segments) |segments| segments.total_len else encoding.bytes.len;
                    std.mem.doNotOptimizeAway(wire_bytes);
                }
                const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - start;
                try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);
                std.debug.print("source_segments codec_bytes={d} references={d} round={d} mode={s} iterations=32 ns={d} allocated_bytes={d} wire_bytes={d}\n", .{ size, count, round, if (segmented) "segments" else "flat", elapsed, measured.allocated_bytes, wire_bytes });
            };
        }
    }
}

test "verified source decoded field access benchmark" {
    if (!@import("antfly_platform").env.getenvBool("ANTFLY_BENCH_SOURCE_MEDIA")) return error.SkipZigTest;
    try @import("antfly_local_sources").storage_db_field_projection_source.benchmarkDecodedAccess();
}
