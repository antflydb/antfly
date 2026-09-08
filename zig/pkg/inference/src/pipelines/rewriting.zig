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
pub const PreparedTextBatch = @import("prepared_text.zig").PreparedTextBatch;

pub const RewriteConfig = struct {
    max_length: usize = 512,
};

pub const RewriteResult = struct {
    text: []const u8,
    allocator: std.mem.Allocator,
    completion_tokens: usize = 0,

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
        var prepared = try self.prepare(texts);
        defer prepared.deinit();
        return self.rewritePrepared(io, &prepared);
    }

    pub fn prepare(self: *const RewritingPipeline, texts: []const []const u8) !PreparedTextBatch {
        return PreparedTextBatch.init(self.allocator, self.enc_dec.encoder, self.tokenizer, texts, self.config.max_length, self.enc_dec.execution_control);
    }

    pub fn rewritePrepared(self: *RewritingPipeline, io: std.Io, prepared: *const PreparedTextBatch) ![]RewriteResult {
        try prepared.validateFor(self.enc_dec.encoder, self.tokenizer, self.config.max_length);
        const allocator = self.allocator;
        const Work = struct {
            original: usize,
            width: usize,
            fn less(_: void, a: @This(), b: @This()) bool {
                return a.width < b.width or (a.width == b.width and a.original < b.original);
            }
        };
        const queue_bytes = try std.math.mul(usize, prepared.ids.len, @sizeOf(Work) + @sizeOf(RewriteResult));
        var queue_permit = try self.enc_dec.encoder.admitHostPreprocess(queue_bytes);
        defer queue_permit.deinit();
        const order = try allocator.alloc(Work, prepared.ids.len);
        defer allocator.free(order);
        for (order, 0..) |*work, i| work.* = .{ .original = i, .width = 0 };
        const results = try allocator.alloc(RewriteResult, prepared.ids.len);
        var initialized: usize = 0;
        errdefer {
            for (order[0..initialized]) |work| results[work.original].deinit();
            allocator.free(results);
        }
        if (self.enc_dec.batch_dispatch == null) {
            for (prepared.ids, results) |ids, *result| {
                result.* = try self.rewriteTokens(ids);
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
        // Group lightweight indices across the entire bounded prepared queue;
        // tensor materialization and execution remain limited to eight rows.
        for (prepared.ids, order) |ids, *work| {
            if (self.enc_dec.execution_control) |control| try control.check();
            if (ids.len == 0) return error.InvalidInputShape;
            const length = @min(ids.len, self.config.max_length);
            if (length == 0) return error.InvalidInputShape;
            work.width = @import("batch_execution.zig").maskedSequenceBucket(self.enc_dec.encoder, length, self.config.max_length);
        }
        std.mem.sort(Work, order, {}, Work.less);
        while (initialized < order.len) {
            const width = order[initialized].width;
            var execute_count: usize = 1;
            while (execute_count < 8 and initialized + execute_count < order.len and order[initialized + execute_count].width == width) execute_count += 1;
            while (execute_count > 1 and !try self.enc_dec.fitsWindow(execute_count, width, try std.math.add(usize, prepared.reserved_bytes, queue_bytes)))
                execute_count = @max(@as(usize, 1), execute_count / 2);
            var jobs: [8]Job = undefined;
            var job_count: usize = 0;
            defer for (jobs[0..job_count]) |*job| job.deinit();
            const window = order[initialized..][0..execute_count];
            for (window, 0..) |work, i| {
                const tokens = prepared.ids[work.original];
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
            for (jobs[0..execute_count], window) |*job, work| {
                if (job.err) |err| return err;
                if (self.enc_dec.execution_control) |control| try control.check();
                results[work.original] = .{ .allocator = allocator, .text = try self.tokenizer.decode(allocator, job.output.?.text_ids), .completion_tokens = job.output.?.text_ids.len -| 1 };
                initialized += 1;
            }
        }
        return results;
    }

    pub fn rewrite(self: *RewritingPipeline, text: []const u8) !RewriteResult {
        var prepared = try self.prepare(&.{text});
        defer prepared.deinit();
        return self.rewriteTokens(prepared.ids[0]);
    }

    fn rewriteTokens(self: *RewritingPipeline, token_ids_i32: []const i32) !RewriteResult {
        const allocator = self.allocator;
        if (self.enc_dec.execution_control) |control| try control.check();
        if (token_ids_i32.len == 0) return error.InvalidInputShape;
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
            .completion_tokens = gen_result.text_ids.len -| 1,
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
        tokenizations: usize = 0,
        fn encode(raw: *anyopaque, allocator: std.mem.Allocator, text: []const u8) ![]i32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.tokenizations += 1;
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
    var prepared = try pipeline.prepare(&.{ "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab" });
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 2), prepared.max_tokens);
    try std.testing.expectEqual(@as(usize, 16), prepared.total_tokens);
    try std.testing.expectEqual(@as(usize, 8), probe.tokenizations);
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
    pipeline.config.max_length += 1;
    try std.testing.expectError(error.InvalidPreparedTextInputs, pipeline.rewritePrepared(std.testing.io, &prepared));
    pipeline.config.max_length -= 1;
    const results = try pipeline.rewritePrepared(std.testing.io, &prepared);
    defer {
        for (results) |*result| result.deinit();
        std.testing.allocator.free(results);
    }
    try std.testing.expectEqual(@as(usize, 3), probe.calls);
    try std.testing.expectEqual(@as(usize, 8), probe.largest);
    try std.testing.expectEqual(@as(usize, 8), probe.tokenizations);
    for (results) |result| try std.testing.expectEqualStrings("rewritten", result.text);

    // The same request must remain usable when only singleton stage peaks fit.
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    var bounded = session;
    bounded.run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{ .host_limit_bytes = 1500 + @sizeOf(@import("../backends/admitted_allocator.zig").AdmittedAllocator) + 3 * (@import("../backends/admitted_allocator.zig").AdmittedAllocator.allocationOverhead(.of([]i32)) + @import("../backends/admitted_allocator.zig").AdmittedAllocator.reservation_overhead) },
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
    probe.calls = 0;
    probe.encoder_cells = 0;
    const interleaved = try pipeline.rewriteBatch(std.testing.io, &.{ &short, &long, &short, &long, &short, &long, &short, &long, &short, &long, &short, &long, &short, &long, &short, &long });
    defer {
        for (interleaved) |*result| result.deinit();
        std.testing.allocator.free(interleaved);
    }
    // Six physical forwards (encoder + two decode steps for each width),
    // rather than twelve from sorting two adjacent eight-item windows.
    try std.testing.expectEqual(@as(usize, 6), probe.calls);
    try std.testing.expectEqual(@as(usize, 8 * (512 + 16)), probe.encoder_cells);
    for (interleaved, 0..) |result, index| try std.testing.expectEqualStrings(if (index % 2 == 1) "long" else "short", result.text);
    // Symbolic vocabulary metadata must use the same concrete projection as
    // execution, rather than approving a window with a trailing width of one.
    pipeline.enc_dec.encoder.output_geometry = .{ .input_name = "input_ids", .width = 1 };
    pipeline.enc_dec.decoder.output_geometry = .{ .input_name = "input_ids", .width = 32768 };
    pipeline.enc_dec.config.max_length = 128;
    try std.testing.expect(!try pipeline.enc_dec.fitsWindow(8, 16, 4096));
    const before = probe.tokenizations;
    pipeline.enc_dec.encoder.run_admission.?.limits.host_limit_bytes = 1;
    try std.testing.expectError(error.ResourceLimitExceeded, pipeline.prepare(&.{"not tokenized"}));
    try std.testing.expectEqual(before, probe.tokenizations);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}
