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

//! Transport-only native media comparison after source verification. The
//! candidate consumes the already verified codec bytes; the inline control
//! decodes again. Corpus creation, source authorization, verification, model
//! work and caller-retained inputs are outside the measured allocation scope.
//! Each trial includes exact output comparison. Records go to stderr.
const std = @import("std");
const media = @import("media");
const envelope = @import("httpx").attachment_envelope;
pub fn main(init: std.process.Init) !void {
    const corpus = init.arena.allocator();
    for ([_]usize{ 64, 262144, 1048576 }) |size| {
        const bytes = try corpus.alloc(u8, size);
        for (bytes, 0..) |*v, i| v.* = @truncate(i *% 37);
        const base64 = try corpus.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.len));
        _ = std.base64.standard.Encoder.encode(base64, bytes);
        const url = try std.fmt.allocPrint(corpus, "data:image/png;base64,{s}", .{base64});
        const body = try std.json.Stringify.valueAlloc(corpus, .{ .model = "fixture", .messages = .{.{ .role = "user", .content = .{.{ .type = "image_url", .image_url = .{ .url = url } }} }} }, .{});
        for (0..5) |round| for (0..2) |slot| {
            const mode = if (round % 2 == 0) slot else 1 - slot;
            var measured = std.testing.FailingAllocator.init(std.heap.smp_allocator, .{});
            const started = std.Io.Clock.awake.now(init.io).nanoseconds;
            for (0..64) |_| {
                var arena = std.heap.ArenaAllocator.init(measured.allocator());
                defer arena.deinit();
                const a = arena.allocator();
                var result: []const u8 = undefined;
                if (mode == 0) {
                    const value = try std.json.parseFromSliceLeaky(std.json.Value, a, body, .{});
                    const encoded = value.object.get("messages").?.array.items[0].object.get("content").?.array.items[0].object.get("image_url").?.object.get("url").?.string["data:image/png;base64,".len..];
                    const decoded = try a.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(encoded));
                    try std.base64.standard.Decoder.decode(decoded, encoded);
                    result = decoded;
                    if (!std.mem.eql(u8, bytes, result)) return error.DifferentBytes;
                    std.mem.doNotOptimizeAway(result.ptr);
                } else {
                    var framed = try media.encodeVerifiedGenerate(measured.allocator(), measured.allocator(), body, null, &.{.{ .url = url, .bytes = bytes, .mime_type = "image/png" }});
                    defer framed.deinit(measured.allocator());
                    var parsed = try envelope.parseAlloc(a, framed.bytes, .{});
                    defer parsed.deinit();
                    const value = try std.json.parseFromSliceLeaky(std.json.Value, a, parsed.metadata, .{});
                    if (!std.mem.eql(u8, "attachment:0", value.object.get("messages").?.array.items[0].object.get("content").?.array.items[0].object.get("data").?.string)) return error.InvalidReference;
                    if (parsed.attachments.len != 1) return error.InvalidCardinality;
                    result = parsed.attachments[0].data;
                    if (!std.mem.eql(u8, bytes, result)) return error.DifferentBytes;
                    std.mem.doNotOptimizeAway(result.ptr);
                }
            }
            if (measured.allocated_bytes != measured.freed_bytes) return error.LeakedAllocation;
            std.debug.print("size={d} round={d} mode={d} iterations=64 retained_codec_bytes={d} ns={d} allocations={d} allocated_bytes={d}\n", .{ size, round, mode, if (mode == 0) @as(usize, 0) else size, std.Io.Clock.awake.now(init.io).nanoseconds - started, measured.allocations, measured.allocated_bytes });
        };
    }
}
