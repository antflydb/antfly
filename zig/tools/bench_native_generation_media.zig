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

//! Consumer-local adaptation for the compiled inference provider. Storage and
//! standalone use one implementation for cancellation, progress, binary media,
//! numeric results, and invocation lifetime; the inference node stays opaque.
//! Compare structured generation metadata construction and native receipt after
//! source verification. Excludes model execution and fixed runtime dispatch.
//! Both paths serialize every generation option and parse received metadata.
const std = @import("std");
const inference = @import("inference");
const media = @import("media");
const verified = @import("verified");
const envelope = @import("httpx").attachment_envelope;
pub fn main(init: std.process.Init) !void {
    const corpus = init.arena.allocator();
    for ([_]usize{ 64, 262144, 1048576 }) |size| {
        const bytes = try corpus.alloc(u8, size);
        for (bytes, 0..) |*v, i| v.* = @truncate(i *% 37);
        const base64 = try corpus.alloc(u8, std.base64.standard.Encoder.calcSize(size));
        _ = std.base64.standard.Encoder.encode(base64, bytes);
        const url = try std.fmt.allocPrint(corpus, "data:image/png;base64,{s}", .{base64});
        const parts = [_]inference.ContentPart{.{ .image_url = .{ .url = url } }};
        const messages = [_]inference.ChatMessage{.{ .role = .user, .content = .{ .parts = &parts } }};
        const hint = [_]media.VerifiedMedia{.{ .url = url, .mime_type = "image/png", .bytes = bytes }};
        for (0..7) |round| for (0..2) |slot| {
            const mode = if (round % 2 == 0) slot else 1 - slot;
            var measured = std.testing.FailingAllocator.init(std.heap.smp_allocator, .{});
            const a = measured.allocator();
            const started = std.Io.Clock.awake.now(init.io).nanoseconds;
            for (0..64) |_| {
                var adapted = if (mode == 1) (try verified.prepare(a, &messages, &hint, true)).? else null;
                defer if (adapted) |*value| value.deinit();
                const body = try inference.chatRequestJsonWithOptionsAlloc(a, "fixture", if (adapted) |value| value.messages else &messages, .termite_native, .{
                    .max_tokens = 37,
                    .temperature = 0,
                    .top_p = 0.9,
                    .tools_json = "[{\"type\":\"function\",\"function\":{\"name\":\"inspect\",\"parameters\":{\"type\":\"object\"}}}]",
                    .tool_choice_json = "\"auto\"",
                    .response_format_json = "{\"type\":\"json_object\"}",
                });
                defer a.free(body);
                var encoded = if (mode == 0) try media.encodeVerifiedGenerate(a, a, body, null, &hint) else null;
                defer if (encoded) |*value| value.deinit(a);
                var frame = if (encoded) |value| try envelope.parseAlloc(a, value.bytes, .{}) else null;
                defer if (frame) |*value| value.deinit();
                const metadata = if (frame) |value| value.metadata else body;
                const received = if (frame) |value| value.attachments[0].data else adapted.?.attachments[0].bytes;
                if (mode == 1) {
                    try envelope.validateBorrowed(metadata, &.{.{ .mime_type = "image/png", .data = received }}, .{});
                    if (received.ptr != bytes.ptr) return error.UnexpectedPayloadCopy;
                }
                if (!std.mem.eql(u8, received, bytes)) return error.DifferentBytes;
                var parsed = try std.json.parseFromSlice(std.json.Value, a, metadata, .{});
                defer parsed.deinit();
                const root = parsed.value.object;
                const content = root.get("messages").?.array.items[0].object.get("content").?.array.items;
                if (!std.mem.eql(u8, content[0].object.get("data").?.string, "attachment:0")) return error.InvalidReference;
                if (root.get("max_tokens").?.integer != 37 or root.get("tools") == null or root.get("response_format") == null) return error.DifferentOptions;
                std.mem.doNotOptimizeAway(received.ptr);
            }
            if (measured.allocated_bytes != measured.freed_bytes) return error.LeakedAllocation;
            std.debug.print("size={d} round={d} mode={s} iterations=64 ns={d} allocations={d} allocated_bytes={d}\n", .{ size, round, if (mode == 0) "framed" else "borrowed", std.Io.Clock.awake.now(init.io).nanoseconds - started, measured.allocations, measured.allocated_bytes });
        };
    }
}
