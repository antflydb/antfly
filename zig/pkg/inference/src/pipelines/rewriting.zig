// Copyright 2026 Antfly, Inc.
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

// Seq2Seq text rewriting pipeline (T5/BART via encoder-decoder).
//
// Architecture: encode input text → decode output text autoregressively.
// Uses the EncoderDecoderPipeline for the generation loop — works with any
// backend that provides separate encode/decode sessions (ONNX, native).
//
// Required model files:
//   - encoder_model.onnx (or encoder.onnx)
//   - decoder_model_merged.onnx (or decoder_model.onnx)
//   - tokenizer.json
//   - config.json (model_type: t5, bart, etc.)

const std = @import("std");
const backends = @import("../backends/backends.zig");
const tokenizer_mod = @import("inference_tokenizer");
const enc_dec_mod = @import("encoder_decoder.zig");

pub const RewriteConfig = struct {
    max_length: usize = 512,
};

pub const RewriteResult = struct {
    text: []const u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *RewriteResult) void {
        self.allocator.free(self.text);
    }
};

pub const RewritingPipeline = struct {
    allocator: std.mem.Allocator,
    enc_dec: enc_dec_mod.EncoderDecoderPipeline,
    tokenizer: tokenizer_mod.Tokenizer,
    config: RewriteConfig,

    /// Bounded independent sequences share the stage dispatcher. Tokenizer
    /// calls stay sequential; workers own only tensors and generated token IDs.
    /// Existing runtimes without a dispatcher retain singleton execution.
    pub fn rewriteBatch(self: *RewritingPipeline, io: std.Io, texts: []const []const u8) ![]RewriteResult {
        const allocator = self.allocator;
        const results = try allocator.alloc(RewriteResult, texts.len);
        var initialized: usize = 0;
        errdefer {
            for (results[0..initialized]) |*result| result.deinit();
            allocator.free(results);
        }
        if (self.enc_dec.batch_dispatch == null) {
            for (texts, results) |text, *result| {
                result.* = try self.rewrite(text);
                initialized += 1;
            }
            return results;
        }
        const Job = struct {
            pipeline: enc_dec_mod.EncoderDecoderPipeline,
            ids: []i64,
            mask: []i64,
            output: ?enc_dec_mod.EncoderDecoderResult = null,
            err: ?anyerror = null,

            fn execute(self_job: *@This()) !void {
                const alloc = std.heap.smp_allocator;
                const outputs = try self_job.pipeline.encodeMasked(alloc, self_job.ids, self_job.mask);
                defer {
                    for (outputs) |*output| output.deinit();
                    alloc.free(outputs);
                }
                self_job.output = try self_job.pipeline.greedyDecode(alloc, outputs, self_job.mask, self_job.ids.len);
            }
            fn run(self_job: *@This()) std.Io.Cancelable!void {
                self_job.execute() catch |err| {
                    self_job.err = err;
                };
            }
            fn deinit(self_job: *@This()) void {
                if (self_job.output) |*output| output.deinit();
                std.heap.smp_allocator.free(self_job.ids);
                std.heap.smp_allocator.free(self_job.mask);
            }
        };
        while (initialized < texts.len) {
            var count = @min(@as(usize, 8), texts.len - initialized);
            var preprocess_permit: @import("../backends/session.zig").RunPermit = undefined;
            while (true) {
                const bytes = try self.preprocessBytes(texts[initialized..][0..count]);
                preprocess_permit = self.enc_dec.encoder.admitHostPreprocess(bytes) catch |err| switch (err) {
                    error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable => {
                        if (count == 1) return err;
                        count = @max(@as(usize, 1), count / 2);
                        continue;
                    },
                };
                break;
            }
            defer preprocess_permit.deinit();
            const window = texts[initialized..][0..count];
            var tokenized: [8][]i32 = undefined;
            var tokenized_count: usize = 0;
            defer for (tokenized[0..tokenized_count]) |ids| allocator.free(ids);
            var order: [8]usize = undefined;
            var widths: [8]usize = undefined;
            for (window, 0..) |text, i| {
                if (self.enc_dec.execution_control) |control| try control.check();
                tokenized[i] = try self.tokenizer.encode(allocator, text);
                tokenized_count += 1;
                if (tokenized[i].len == 0) return error.InvalidInputShape;
                order[i] = i;
                const length = @min(tokenized[i].len, self.config.max_length);
                if (length == 0) return error.InvalidInputShape;
                widths[i] = @import("batch_execution.zig").maskedSequenceBucket(self.enc_dec.encoder, length, self.config.max_length);
            }
            std.mem.sort(usize, order[0..count], &widths, struct {
                fn less(sizes: *const [8]usize, a: usize, b: usize) bool {
                    return sizes[a] < sizes[b];
                }
            }.less);
            var completed: [8]?RewriteResult = @splat(null);
            defer for (&completed) |*result| if (result.*) |*value| value.deinit();
            var work_offset: usize = 0;
            while (work_offset < count) {
                const width = widths[order[work_offset]];
                var execute_count: usize = 1;
                while (work_offset + execute_count < count and widths[order[work_offset + execute_count]] == width) execute_count += 1;
                while (execute_count > 1 and !try self.enc_dec.fitsWindow(execute_count, width, try self.preprocessBytes(window)))
                    execute_count = @max(@as(usize, 1), execute_count / 2);
                var jobs: [8]Job = undefined;
                var job_count: usize = 0;
                defer for (jobs[0..job_count]) |*job| job.deinit();
                for (order[work_offset..][0..execute_count], 0..) |original, i| {
                    const tokens = tokenized[original];
                    const alloc = std.heap.smp_allocator;
                    const ids = try alloc.alloc(i64, width);
                    errdefer alloc.free(ids);
                    const mask = try alloc.alloc(i64, width);
                    @memset(ids, self.enc_dec.config.pad_token_id);
                    @memset(mask, 0);
                    for (tokens[0..@min(tokens.len, width)], 0..) |id, j| {
                        ids[j] = id;
                        mask[j] = 1;
                    }
                    var pipeline = self.enc_dec;
                    pipeline.allocator = alloc;
                    // Progress sinks need not be concurrent. Cancellation remains
                    // inherited and is checked by every stage and decode step.
                    if (pipeline.execution_control) |*control| control.progress = null;
                    jobs[i] = .{ .pipeline = pipeline, .ids = ids, .mask = mask };
                    job_count += 1;
                }
                var group = std.Io.Group.init;
                defer group.cancel(io);
                for (jobs[0..execute_count]) |*job| group.async(io, Job.run, .{job});
                try group.await(io);
                for (jobs[0..execute_count], order[work_offset..][0..execute_count]) |*job, original| {
                    if (job.err) |err| return err;
                    if (self.enc_dec.execution_control) |control| try control.check();
                    completed[original] = .{ .allocator = allocator, .text = try self.tokenizer.decode(allocator, job.output.?.text_ids) };
                }
                work_offset += execute_count;
            }
            for (completed[0..count], 0..) |result, i| {
                results[initialized] = result.?;
                completed[i] = null;
                initialized += 1;
            }
        }
        return results;
    }

    fn preprocessBytes(self: *const RewritingPipeline, texts: []const []const u8) !usize {
        const max_tokens = std.math.mul(usize, texts.len, self.config.max_length) catch return error.ResourceLimitExceeded;
        var bytes = std.math.mul(usize, max_tokens, 32) catch return error.ResourceLimitExceeded;
        for (texts) |text| bytes = std.math.add(usize, bytes, std.math.mul(usize, text.len, 8) catch return error.ResourceLimitExceeded) catch return error.ResourceLimitExceeded;
        return bytes;
    }

    pub fn rewrite(self: *RewritingPipeline, text: []const u8) !RewriteResult {
        const allocator = self.allocator;

        // 1. Tokenize input text (raw, no [CLS]/[SEP] — T5/BART have their own special tokens)
        if (self.enc_dec.execution_control) |control| try control.update(.tokenizing, 0, 1);
        const token_ids_i32 = try self.tokenizer.encode(allocator, text);
        defer allocator.free(token_ids_i32);
        if (self.enc_dec.execution_control) |control| try control.update(.tokenizing, 1, 1);

        // Convert i32 token IDs to i64 for the backend
        const seq_len = @min(token_ids_i32.len, self.config.max_length);
        const input_ids = try allocator.alloc(i64, seq_len);
        defer allocator.free(input_ids);
        for (0..seq_len) |i| {
            input_ids[i] = @intCast(token_ids_i32[i]);
        }

        // 2. Run encoder
        const encoder_outputs = try self.enc_dec.encode(allocator, input_ids, seq_len);
        defer {
            for (encoder_outputs) |*o| o.deinit();
            allocator.free(encoder_outputs);
        }

        // 3. Build encoder attention mask (all 1s for real tokens)
        const enc_mask = try allocator.alloc(i64, seq_len);
        defer allocator.free(enc_mask);
        @memset(enc_mask, 1);

        // 4. Greedy decode
        var gen_result = try self.enc_dec.greedyDecode(allocator, encoder_outputs, enc_mask, seq_len);
        defer gen_result.deinit();

        // 5. Decode output token IDs to text
        if (self.enc_dec.execution_control) |control| try control.update(.serializing, 0, 1);
        const output_text = try self.tokenizer.decode(allocator, gen_result.text_ids);

        return .{
            .text = output_text,
            .allocator = allocator,
        };
    }
};

test "rewrite arrays fuse padded encoder and independent decoder stages" {
    const micro = @import("../server/executor_microbatch.zig");
    const tensors = @import("../server/tensor_microbatch.zig");
    const Control = @import("../execution_control.zig").InferenceExecutionControl;
    const Probe = struct {
        broker: micro.Broker,
        gate: std.atomic.Mutex = .unlocked,
        calls: usize = 0,
        largest: usize = 0,
        identify: bool = false,
        encoder_cells: usize = 0,
        fn encode(raw: *anyopaque, allocator: std.mem.Allocator, text: []const u8) ![]i32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const ids = try allocator.alloc(i32, text.len);
            @memset(ids, if (self.identify) @intCast(text.len) else 2);
            return ids;
        }
        fn decode(raw: *anyopaque, allocator: std.mem.Allocator, ids: []const i32) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.identify) return allocator.dupe(u8, if (ids[1] == 3) "long" else "short");
            try std.testing.expectEqualSlices(i32, &.{ 0, 2 }, ids);
            return allocator.dupe(u8, "rewritten");
        }
        fn info(_: *anyopaque) []const backends.TensorInfo {
            return &.{.{ .name = "hidden", .dtype = .f32, .shape = &.{ -1, -1, -1 } }};
        }
        fn independent(_: *anyopaque, _: []const backends.Tensor) bool {
            return true;
        }
        fn backend(_: *anyopaque) backends.BackendType {
            return .native;
        }
        fn close(_: *anyopaque) void {}
        fn controlled(raw: *anyopaque, inputs: []const backends.Tensor, allocator: std.mem.Allocator, control: Control) ![]backends.Tensor {
            try control.check();
            return forward(raw, inputs, allocator);
        }
        fn forward(raw: *anyopaque, inputs: []const backends.Tensor, allocator: std.mem.Allocator) ![]backends.Tensor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const batch: usize = @intCast(inputs[0].shape[0]);
            const width: usize = @intCast(inputs[0].shape[1]);
            const decoder = inputs.len == 3;
            const hidden: usize = if (decoder) 4 else 1;
            self.calls += 1;
            self.largest = @max(self.largest, batch);
            const data = try allocator.alloc(f32, batch * width * hidden);
            defer allocator.free(data);
            @memset(data, 0);
            if (!decoder) {
                self.encoder_cells += batch * width;
                if (self.identify) for (0..batch) |i| {
                    data[i * width] = @floatFromInt(inputs[0].asInt64()[i * width]);
                };
            }
            if (decoder) for (0..batch * width) |i| {
                const token: usize = if (width != 1) 1 else if (self.identify and inputs[2].asFloat32()[i / width * @as(usize, @intCast(inputs[2].shape[1]))] > 16) 3 else 2;
                data[i * 4 + token] = 10;
            };
            var tensor = try backends.Tensor.initFloat32(allocator, if (decoder) "logits" else "last_hidden_state", &.{ @intCast(batch), @intCast(width), @intCast(hidden) }, data);
            errdefer tensor.deinit();
            const outputs = try allocator.alloc(backends.Tensor, 1);
            outputs[0] = tensor;
            return outputs;
        }
        fn dispatch(raw: *anyopaque, task: micro.Task, allocator: std.mem.Allocator, session: backends.Session, permit: ?*@import("../backends/session.zig").RunPermit, _: ?*std.atomic.Mutex, inputs: []const backends.Tensor, control: ?Control) ![]backends.Tensor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return tensors.run(&self.broker, allocator, std.testing.io, task, session, permit, &self.gate, inputs, control, null, 500_000);
        }
    };
    var probe = Probe{ .broker = micro.Broker.init(std.testing.allocator) };
    defer probe.broker.deinit();
    const session = backends.Session{ .ptr = &probe, .vtable = &.{ .run = Probe.forward, .runWithControl = Probe.controlled, .inputInfo = Probe.info, .outputInfo = Probe.info, .backend = Probe.backend, .close = Probe.close, .independentBatchRows = Probe.independent } };
    var pipeline = RewritingPipeline{
        .allocator = std.testing.allocator,
        .enc_dec = .{ .allocator = std.testing.allocator, .encoder = session, .decoder = session, .config = .{ .vocab_size = 4, .max_length = 4, .decoder_start_token_id = 0, .eos_token_id = 1 }, .batch_dispatch = .{ .ptr = &probe, .task = .rewrite, .run_fn = Probe.dispatch } },
        .tokenizer = .{ .ptr = &probe, .vtable = &.{ .encode = Probe.encode, .decode = Probe.decode, .encodeInto = undefined, .encodeForModel = undefined, .encodeGeneration = undefined, .specialTokens = undefined, .vocabSize = undefined, .deinit = undefined } },
        .config = .{ .max_length = 4 },
    };
    const results = try pipeline.rewriteBatch(std.testing.io, &.{ "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab" });
    defer {
        for (results) |*result| result.deinit();
        std.testing.allocator.free(results);
    }
    try std.testing.expectEqual(@as(usize, 3), probe.calls);
    try std.testing.expectEqual(@as(usize, 8), probe.largest);
    for (results) |result| try std.testing.expectEqualStrings("rewritten", result.text);

    // The same request must remain usable when only singleton stage peaks fit.
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    var bounded = session;
    bounded.run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{ .host_limit_bytes = 1500 },
        .static_workspace_bytes = 1,
        .check_live_memory = false,
    };
    pipeline.enc_dec.encoder = bounded;
    pipeline.enc_dec.decoder = bounded;
    probe.calls = 0;
    probe.largest = 0;
    const small = try pipeline.rewriteBatch(std.testing.io, &.{ "a", "ab" });
    defer {
        for (small) |*result| result.deinit();
        std.testing.allocator.free(small);
    }
    try std.testing.expectEqual(@as(usize, 1), probe.largest);
    try std.testing.expectEqual(@as(usize, 6), probe.calls);
    for (small) |result| try std.testing.expectEqualStrings("rewritten", result.text);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
    // Eight rows share physical workspaces rather than reserving eight copies
    // of each session's static scratch allowance.
    bounded.run_admission.?.limits.host_limit_bytes = 3 * 1024 * 1024;
    bounded.run_admission.?.static_workspace_bytes = 1024 * 1024;
    pipeline.enc_dec.encoder = bounded;
    pipeline.enc_dec.decoder = bounded;
    try std.testing.expect(try pipeline.enc_dec.fitsWindow(8, 2, 4096));
    probe.calls = 0;
    probe.largest = 0;
    const fused = try pipeline.rewriteBatch(std.testing.io, &.{ "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab" });
    defer {
        for (fused) |*result| result.deinit();
        std.testing.allocator.free(fused);
    }
    try std.testing.expectEqual(@as(usize, 8), probe.largest);
    try std.testing.expectEqual(@as(usize, 3), probe.calls);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
    probe.identify = true;
    probe.encoder_cells = 0;
    pipeline.config.max_length = 512;
    const long = [_]u8{'a'} ** 512;
    const short = [_]u8{'a'} ** 16;
    const mixed = try pipeline.rewriteBatch(std.testing.io, &.{ &short, &long, &short, &short, &short, &short, &short, &short });
    defer {
        for (mixed) |*result| result.deinit();
        std.testing.allocator.free(mixed);
    }
    try std.testing.expectEqual(@as(usize, 512 + 7 * 16), probe.encoder_cells);
    for (mixed, 0..) |result, index| try std.testing.expectEqualStrings(if (index == 1) "long" else "short", result.text);
}
