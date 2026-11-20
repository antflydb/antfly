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

//! Request-owned prepared prefix for full-recompute compiled decoding. Media
//! rows survive every text step; no loaded-model cache owns request values.
const std = @import("std");
const Tensor = @import("../backends/tensor.zig").Tensor;
const Session = @import("../backends/session.zig").Session;
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Prefix = struct {
    embeddings: Tensor,
    /// Zero preserves exact-shape execution; opt-in buckets bound padding.
    bucket_rows: usize = 0,
    retained: ?@import("../backends/session.zig").RunPermit = null,
    ple_ids: ?[]i64 = null,
    axis_positions: ?[]u32 = null,
    position_delta: i64 = 0,
    visibility: ?[]u8 = null,

    pub fn deinit(self: *Prefix, a: std.mem.Allocator) void {
        self.embeddings.deinit();
        if (self.ple_ids) |ids| a.free(ids);
        if (self.axis_positions) |positions| a.free(positions);
        if (self.visibility) |mask| a.free(mask);
        if (self.retained) |*permit| permit.deinit();
        self.* = undefined;
    }

    pub fn runCached(self: *const Prefix, decoder: *@import("resident_decoder.zig").Decoder, history: []const i64, a: std.mem.Allocator, control: ?Control) ![]f32 {
        if (decoder.failed) return error.DecoderRequestFailed;
        if (decoder.position != 0) return error.InvalidResidentDecodePosition;
        errdefer decoder.failed = true;
        const logits = try self.runImpl(decoder, decoder.source, history, a, control);
        decoder.axis_positions = self.axis_positions != null;
        decoder.position_delta = self.position_delta;
        decoder.prefill_steps = 1;
        return logits;
    }

    pub fn run(self: *const Prefix, session: Session, history: []const i64, a: std.mem.Allocator, control: ?Control) ![]f32 {
        return self.runImpl(null, session, history, a, control);
    }

    const Layout = struct { prefix_rows: usize, pad_embeddings: bool, staging_bytes: usize, input_bytes: usize };

    /// One allocation-free layout describes both optional-cache admission and
    /// actual host materialization. Whole-prefix visibility cannot be chunked.
    fn layout(self: *const Prefix, prompt_rows: usize, rows: usize, cached: bool) !Layout {
        try @import("capture.zig").requireHostTensor(self.embeddings, .f32, 3);
        if (self.embeddings.shape[0] != 1 or rows < prompt_rows or prompt_rows == 0) return error.InvalidPreparedPrompt;
        const prefix_rows: usize = @intCast(self.embeddings.shape[1]);
        const width: usize = @intCast(self.embeddings.shape[2]);
        if (prompt_rows < prefix_rows) return error.InvalidPreparedPrompt;
        const mul = std.math.mul;
        const add = std.math.add;
        if (self.ple_ids) |ids| if (ids.len != prefix_rows) return error.InvalidPreparedPrompt;
        if (self.axis_positions) |positions| if (positions.len != try mul(usize, prefix_rows, 3)) return error.InvalidPreparedPrompt;
        if (self.visibility) |mask| if (mask.len != try mul(usize, prefix_rows, prefix_rows)) return error.InvalidPreparedPrompt;
        const pad_embeddings = cached and prefix_rows == prompt_rows and rows != prefix_rows;
        var elements = try mul(usize, rows, 2 + @as(usize, if (self.ple_ids != null) 1 else 0));
        if (self.axis_positions != null) elements = try add(usize, elements, try mul(usize, rows, 6));
        var staging = try mul(usize, elements, @sizeOf(i64));
        var input = try add(usize, try mul(usize, rows, @sizeOf(i64)), try mul(usize, try mul(usize, if (pad_embeddings) rows else prefix_rows, width), @sizeOf(f32)));
        var metadata: usize = 6 * @sizeOf(Tensor) + 2 * @sizeOf(i64);
        if (cached or self.bucket_rows != 0) {
            staging = try add(usize, staging, @sizeOf(i64));
            input = try add(usize, input, @sizeOf(i64));
            metadata = try add(usize, metadata, @sizeOf(i64));
        }
        if (pad_embeddings) {
            staging = try add(usize, staging, try mul(usize, try mul(usize, rows, width), @sizeOf(f32)));
            metadata = try add(usize, metadata, 3 * @sizeOf(i64));
        }
        if (self.ple_ids != null) {
            input = try add(usize, input, try mul(usize, rows, @sizeOf(i64)));
            metadata = try add(usize, metadata, 2 * @sizeOf(i64));
        }
        if (self.axis_positions != null) {
            input = try add(usize, input, try mul(usize, rows, 3 * @sizeOf(i64)));
            metadata = try add(usize, metadata, 2 * @sizeOf(i64));
        }
        if (self.visibility != null) {
            const mask = try mul(usize, rows, rows);
            staging = try add(usize, staging, mask);
            input = try add(usize, input, mask);
            metadata = try add(usize, metadata, 2 * @sizeOf(i64));
        }
        return .{ .prefix_rows = prefix_rows, .pad_embeddings = pad_embeddings, .staging_bytes = try add(usize, staging, metadata), .input_bytes = input };
    }

    pub fn cachedAdmission(self: *const Prefix, prompt_rows: usize, maximum: usize) !@import("resident_decoder.zig").Decoder.WholePrefill {
        const rows = try @import("resident_decoder.zig").prefillRowsFor(prompt_rows, maximum);
        const planned = try self.layout(prompt_rows, rows, true);
        return .{ .rows = rows, .input_bytes = planned.input_bytes, .host_staging_bytes = planned.staging_bytes };
    }

    fn runImpl(self: *const Prefix, decoder: ?*@import("resident_decoder.zig").Decoder, session: Session, history: []const i64, a: std.mem.Allocator, control: ?Control) ![]f32 {
        if (self.bucket_rows > 256) return error.InvalidInputShape;
        if (history.len == 0) return error.InvalidPreparedPrompt;
        const rows = if (decoder) |cached| try @import("resident_decoder.zig").prefillRowsFor(history.len, cached.config.max_position_embeddings) else if (self.bucket_rows == 0) history.len else bucket: {
            const cfg = @import("../architectures/session_factory.zig").getGptConfig(session) orelse return error.InvalidPreparedPrompt;
            const rounded = try std.math.mul(usize, try std.math.divCeil(usize, history.len, self.bucket_rows), self.bucket_rows);
            break :bucket @min(rounded, cfg.max_position_embeddings);
        };
        if (rows < history.len) return error.InvalidInputShape;
        // This is the same layout used before selecting automatic caching.
        const planned = try self.layout(history.len, rows, decoder != null);
        const prefix_rows = planned.prefix_rows;
        const pad_embeddings = planned.pad_embeddings;
        var staging = try session.withAdmissionControl(control).admitRetainedMedia(planned.staging_bytes, 0);
        defer staging.deinit();
        const padded = if (rows != history.len) try a.alloc(i64, rows) else null;
        defer if (padded) |values| a.free(values);
        if (padded) |values| {
            @memcpy(values[0..history.len], history);
            @memset(values[history.len..], 0);
        }
        const padded_ids = padded orelse history;
        // The ABI has at most six inputs. Keep its packet on the stack so
        // materialization cannot outgrow the layout's declared header bank.
        var packet: [6]Tensor = undefined;
        var inputs = std.ArrayList(Tensor).initBuffer(&packet);
        defer for (inputs.items) |*input| input.deinit();
        inputs.appendAssumeCapacity(try Tensor.initInt64(a, "input_ids", &.{ 1, @intCast(rows) }, padded_ids));
        if (self.bucket_rows != 0 or decoder != null) inputs.appendAssumeCapacity(try Tensor.initInt64(a, "logits_row", &.{1}, &.{@intCast(history.len - 1)}));
        if (pad_embeddings) {
            const width: usize = @intCast(self.embeddings.shape[2]);
            const values = try a.alloc(f32, try std.math.mul(usize, rows, width));
            @memcpy(values[0 .. prefix_rows * width], self.embeddings.asFloat32());
            @memset(values[prefix_rows * width ..], 0);
            const imported = Tensor.initFloat32Owned(a, "input_embeddings", &.{ 1, @intCast(rows), @intCast(width) }, values) catch |err| {
                a.free(values);
                return err;
            };
            inputs.appendAssumeCapacity(imported);
        } else inputs.appendAssumeCapacity(self.embeddings.borrowedView("input_embeddings"));
        if (self.ple_ids) |prefix| {
            if (prefix.len != prefix_rows) return error.InvalidPreparedPrompt;
            const ids = try Tensor.initInt64(a, "ple_input_ids", &.{ 1, @intCast(rows) }, padded_ids);
            @memcpy(std.mem.bytesAsSlice(i64, @as([]align(@alignOf(i64)) u8, @alignCast(ids.data)))[0..prefix_rows], prefix);
            inputs.appendAssumeCapacity(ids);
        }
        if (self.axis_positions) |prefix| {
            if (prefix.len != prefix_rows * 3) return error.InvalidPreparedPrompt;
            const positions = try a.alloc(i64, rows * 3);
            defer a.free(positions);
            for (0..3) |axis| {
                for (0..prefix_rows) |row| positions[axis * rows + row] = prefix[axis * prefix_rows + row];
                for (prefix_rows..rows) |row| positions[axis * rows + row] = std.math.add(i64, @intCast(row), self.position_delta) catch return error.InvalidInputShape;
            }
            inputs.appendAssumeCapacity(try Tensor.initInt64(a, "position_ids", &.{ 3, @intCast(rows) }, positions));
        }
        if (self.visibility) |prefix| {
            if (prefix.len != prefix_rows * prefix_rows) return error.InvalidPreparedPrompt;
            const mask = create: {
                const zeros = try a.alloc(u8, try std.math.mul(usize, rows, rows));
                errdefer a.free(zeros);
                @memset(zeros, 0);
                const shape = try a.dupe(i64, &.{ @intCast(rows), @intCast(rows) });
                break :create Tensor{ .data = zeros, .dtype = .u8, .shape = shape, .name = "attention_or_mask", .allocator = a, .owns_data = true, .owns_shape = true };
            };
            for (0..prefix_rows) |row| @memcpy(mask.data[row * rows ..][0..prefix_rows], prefix[row * prefix_rows ..][0..prefix_rows]);
            inputs.appendAssumeCapacity(mask);
        }
        const outputs = if (decoder) |cached| try cached.run(inputs.items, a, control) else try session.runWithControl(inputs.items, a, control);
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        if (outputs.len != 1 or !std.mem.eql(u8, outputs[0].name, "logits") or outputs[0].dtype != .f32) return error.InvalidModelProgramOutput;
        return a.dupe(f32, outputs[0].asFloat32());
    }
};

test "media prefix cached admission includes padded embeddings positions PLE visibility and metadata" {
    const a = std.testing.allocator;
    var tensor = try Tensor.initFloat32(a, "input_embeddings", &.{ 1, 3, 2 }, &.{ 1, 2, 3, 4, 5, 6 });
    defer tensor.deinit();
    var ids = [_]i64{ 1, 2, 3 };
    var positions = [_]u32{ 0, 1, 2, 0, 1, 2, 0, 1, 2 };
    var mask: [9]u8 = @splat(0);
    const prefix = Prefix{ .embeddings = tensor.borrowedView("input_embeddings"), .ple_ids = &ids, .axis_positions = &positions, .visibility = &mask };
    const cached = try prefix.cachedAdmission(3, 1024);
    const layout = try prefix.layout(3, cached.rows, true);
    try std.testing.expectEqual(@as(usize, 4), cached.rows);
    // IDs 32 + row 8 + padded embeddings 32 + PLE 32 + axes 96 + mask 16.
    try std.testing.expectEqual(@as(usize, 216), cached.input_bytes);
    try std.testing.expectEqual(layout.staging_bytes, cached.host_staging_bytes);
    try std.testing.expect(cached.host_staging_bytes >= 9 * 4 * @sizeOf(i64) + 32 + 16 + 8 + 6 * @sizeOf(Tensor));
    const ordinary = try prefix.layout(3, 3, false);
    try std.testing.expect(!ordinary.pad_embeddings);
    try std.testing.expect(cached.host_staging_bytes > ordinary.staging_bytes);
    var invalid = prefix;
    invalid.ple_ids = ids[0..2];
    try std.testing.expectError(error.InvalidPreparedPrompt, invalid.cachedAdmission(3, 1024));
}
