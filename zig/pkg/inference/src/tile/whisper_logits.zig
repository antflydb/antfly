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

//! Whisper decode-step logit statistics as a tile `RowReduce`.
//!
//! Same contract and partial-record layout as the hand-written Metal kernel
//! `termite_whisper_logits_partials` (metal_kernels.m) and the host oracle
//! `whisper_timestamps.hostLogitsStats`: four argmax channels (best allowed,
//! best text, best timestamp, raw) then three log-sum-exp channels (allowed,
//! timestamps, raw), 16 floats per 1024-logit block. The ABI matches, so the
//! generated Metal kernel can replace the hand-written partials pipeline
//! (`ANTFLY_TILE_WHISPER_LOGITS=1`); the hand-written reduce kernel, which
//! also chooses the token and advances the grammar, is unchanged.

const tile = @import("tile.zig");

const index = tile.index;
const param = tile.param;
const ref = tile.ref;

/// Mirrors `ops.whisper_logits_mode_device_window`.
const mode_device_window: u32 = 1;

pub const kernel: tile.RowReduce = .{
    .name = "antfly_tile_whisper_logits_partials",
    .params_type = "antfly_tile_whisper_logits_params",
    // Field order is `ops.WhisperLogitsParams`.
    .params = &.{
        .{ .name = "out_dim" },
        .{ .name = "suppress_count" },
        .{ .name = "ts_begin" },
        .{ .name = "text_allowed", .override = .{ .flag = "mode", .mask = mode_device_window, .buffer = "grammar", .word = 0 } },
        .{ .name = "ts_min", .override = .{ .flag = "mode", .mask = mode_device_window, .buffer = "grammar", .word = 1 } },
        .{ .name = "ts_max", .override = .{ .flag = "mode", .mask = mode_device_window, .buffer = "grammar", .word = 2 } },
        .{ .name = "eot" },
        .{ .name = "probe_id" },
        .{ .name = "mode" },
        .{ .name = "token_slot" },
        .{ .name = "stats_slot" },
        .{ .name = "reserved" },
    },
    // Binding order is the Metal dispatch in
    // termite_metal_decode_runtime_whisper_logits_stats_device.
    .bindings = &.{
        .{ .values = "logits" },
        .{ .i32_list = .{ .name = "suppress_ids", .count = "suppress_count" } },
        .{ .partials = "partials" },
        .params,
        .{ .u32_words = "grammar" },
    },
    .extent = "out_dim",
    .block = 1024,
    .threads = 256,
    .partial_stride = 16,
    .lets = &.{
        .{ .name = "is_ts", .expr = index().ge(param("ts_begin")) },
        .{ .name = "allowed", .expr = index().eq(param("eot")).orElse(
            ref("is_ts").then(
                index().ge(param("ts_min")).andAlso(index().lt(param("ts_max"))),
                param("text_allowed").isSet(),
            ),
        ).andAlso(tile.notListed("suppress_ids")) },
    },
    .channels = &.{
        .{ .name = "best", .reduce = .argmax, .where = ref("allowed") },
        .{ .name = "text", .reduce = .argmax, .where = ref("allowed").andAlso(ref("is_ts").negate()) },
        .{ .name = "ts", .reduce = .argmax, .where = ref("allowed").andAlso(ref("is_ts")) },
        .{ .name = "raw", .reduce = .argmax, .where = tile.always },
        .{ .name = "all_lse", .reduce = .logsumexp, .where = ref("allowed") },
        .{ .name = "ts_lse", .reduce = .logsumexp, .where = ref("allowed").andAlso(ref("is_ts")) },
        .{ .name = "raw_lse", .reduce = .logsumexp, .where = tile.always },
    },
};

pub const Args = kernel.Args();

/// Build args from the `ops.WhisperLogitsParams` field values.
pub fn args(values: []const f32, params: anytype, suppress: []const i32, grammar: []const u32) Args {
    var a: Args = .{ .values = values, .params = undefined };
    inline for (kernel.params, 0..) |p, i| a.params[i] = @field(params, p.name);
    a.lists[0] = suppress;
    a.words[0] = grammar;
    return a;
}
