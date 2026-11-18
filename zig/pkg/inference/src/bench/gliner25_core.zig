// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Prepared encoder/classifier comparison; excludes tokenization and decoding.
const std = @import("std");
const inference = @import("inference_internal");
const factory = inference.architectures.session_factory;
const deberta = inference.architectures.deberta;
const decision = inference.architectures.gliner_decision_head;
const A = std.heap.c_allocator;
const Input = struct { name: []const u8, shape: []const i64, data: []const i64 };
const Case = struct { id: []const u8, text: []const u8, schema_json: ?[]const u8 = null, texts: ?[]const []const u8 = null, inputs: []const Input, kind: enum { classification, entities } = .classification };
const Manifest = struct { cases: []const Case, architecture: enum { span, boundary } };
const processor = inference.pipelines.gliner_boundary_processor;
const schema_mod = inference.pipelines.extraction_schema;
const Forward = struct { values: []f32, ended: u64 };
const Result = struct { id: []const u8, physical_forwards: usize, samples_seconds: []f64, logits: [][]f32 };
fn now() u64 {
    var t: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &t);
    return @intCast(@as(i128, t.sec) * std.time.ns_per_s + t.nsec);
}
fn boundaryForward(cb: *const inference.ops.ComputeBackend, cfg: inference.models.gliner_boundary.Config, prepared: *const processor.PreparedBatch, entities: bool, bf16_encoder: bool) !Forward {
    const b = prepared.samples.len;
    const words = prepared.word_width;
    const queries = prepared.query_width;
    var encoded = try inference.architectures.gliner_boundary_engine_device.encodeDevice(cb, A, &cfg, prepared, .{ .experimental_bf16_encoder = bf16_encoder });
    defer encoded.deinit();
    if (!entities) {
        var classified = try inference.architectures.gliner_boundary_tasks_device.classify(encoded.math, &cfg, .{
            .choices = b * prepared.classification_width,
            .choice_states = encoded.classification_states orelse return error.MissingClassificationStates,
        }, .{});
        defer classified.deinit();
        const values = try classified.download();
        return .{ .values = values, .ended = now() };
    }
    var head_input = try encoded.asHeadInput(null);
    head_input.required_outputs = .{ .record_candidates = false };
    var head = try inference.architectures.gliner_boundary_device.forwardDevice(cb, A, &cfg, head_input, .{});
    defer head.deinit();
    const capacity = head.pool().capacity;
    var output: std.ArrayList(f32) = .empty;
    errdefer output.deinit(A);
    const fields = [_]struct { tensor: inference.ops.CT, count: usize }{
        .{ .tensor = head.marginals.start_logits, .count = b * queries * (words + 1) },
        .{ .tensor = head.marginals.end_logits, .count = b * queries * (words + 1) },
        .{ .tensor = head.marginals.inside_logits, .count = b * queries * words },
        .{ .tensor = head.null_logits orelse return error.MissingHeadOutput, .count = b * queries },
        .{ .tensor = head.count_log_rates orelse return error.MissingHeadOutput, .count = b * queries },
        .{ .tensor = head.pair_logits, .count = b * queries * capacity },
    };
    for (fields) |field| {
        const values = try head.download(field.tensor, field.count);
        defer A.free(values);
        try output.appendSlice(A, values);
    }
    const pool = head.pool();
    for (0..b) |bi| for (0..queries) |_| for (0..capacity) |ci| {
        const span = pool.indices[bi * capacity + ci];
        try output.appendSlice(A, &.{ @floatFromInt(span.start), @floatFromInt(span.end) });
    };
    for (0..b) |bi| for (0..queries) |_| for (0..capacity) |ci| try output.append(A, if (pool.valid[bi * capacity + ci]) 1 else 0);
    const values = try output.toOwnedSlice(A);
    return .{ .values = values, .ended = now() };
}

const classification_schema = "{\"classifications\":[{\"name\":\"intent\",\"labels\":[\"maintenance\",\"room_change\",\"checkout\",\"billing\",\"complaint\",\"information\"]},{\"name\":\"topics\",\"labels\":[\"hvac\",\"billing\",\"noise\",\"cleanliness\",\"wifi\",\"staff\"],\"multi_label\":true,\"threshold\":0.4},{\"name\":\"urgent\",\"labels\":[\"yes\",\"no\"]}]}";
const entity_schema = "{\"entities\":[\"person\",\"location\",\"organization\",\"species\",\"date\"]}";
pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(A);
    defer A.free(args);
    if (args.len != 7) return error.ExpectedModelManifestOutputWarmupsIterationsPrecision;
    const warmups = try std.fmt.parseInt(usize, args[4], 10);
    const iterations = try std.fmt.parseInt(usize, args[5], 10);
    if (warmups > 100 or iterations == 0 or iterations > 100) return error.InvalidIterations;
    const precision = std.meta.stringToEnum(factory.GlinerCudaPrecision, args[6]) orelse return error.InvalidPrecision;
    const raw = try std.Io.Dir.cwd().readFileAlloc(init.io, args[2], A, .limited(16 * 1024 * 1024));
    defer A.free(raw);
    const parsed = try std.json.parseFromSlice(Manifest, A, raw, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const cfg_path = try std.fs.path.join(A, &.{ args[1], "encoder_config/config.json" });
    defer A.free(cfg_path);
    const cfg_raw = try std.Io.Dir.cwd().readFileAlloc(init.io, cfg_path, A, .limited(1024 * 1024));
    defer A.free(cfg_raw);
    const cfg = try inference.models.deberta.parseConfig(A, cfg_raw);
    // Exercise the production default constructor when qualifying auto;
    // explicit precision remains the diagnostic/control entry point.
    const session = if (precision == .auto)
        try factory.createCudaSession(A, args[1])
    else
        try factory.createGlinerCudaSessionWithPrecision(A, args[1], precision);
    defer session.close();
    const boundary_config = if (parsed.value.architecture == .boundary) try factory.getGlinerBoundaryConfig(session) else null;
    const tokenizer_path = try std.fs.path.join(A, &.{ args[1], "tokenizer.json" });
    defer A.free(tokenizer_path);
    const tokenizer_raw = if (boundary_config != null) try std.Io.Dir.cwd().readFileAlloc(init.io, tokenizer_path, A, .limited(32 * 1024 * 1024)) else try A.alloc(u8, 0);
    defer A.free(tokenizer_raw);
    const tok = if (boundary_config != null) (try inference.hf_tokenizer.HfTokenizer.loadFromBytesWithOptions(A, tokenizer_raw, .{ .strict_unigram_normalizer = true })).tokenizer() else null;
    defer if (tok) |t| t.deinitTokenizer();
    var results: std.ArrayList(Result) = .empty;
    defer {
        for (results.items) |r| {
            for (r.logits) |v| A.free(v);
            A.free(r.logits);
            A.free(r.samples_seconds);
        }
        results.deinit(A);
    }
    errdefer {
        const partial = std.json.Stringify.valueAlloc(A, .{ .incomplete = true, .precision = args[6], .cases = results.items }, .{ .whitespace = .indent_2 }) catch null;
        if (partial) |snapshot| {
            defer A.free(snapshot);
            std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = args[3], .data = snapshot }) catch {};
        }
    }
    for (parsed.value.cases) |case| {
        if (case.inputs.len != (if (case.kind == .entities) @as(usize, 6) else 4)) return error.InvalidInputs;
        for (case.inputs) |input| if (input.shape.len != 2) return error.InvalidShape;
        const batch = std.math.cast(usize, case.inputs[0].shape[0]) orelse return error.InvalidShape;
        const seq = std.math.cast(usize, case.inputs[0].shape[1]) orelse return error.InvalidShape;
        const labels = std.math.cast(usize, case.inputs[2].shape[1]) orelse return error.InvalidShape;
        if (batch == 0 or seq == 0 or seq > 512 or labels == 0) return error.InvalidShape;
        for (case.inputs) |input| {
            if (input.shape[0] != batch or input.shape[1] <= 0 or input.data.len != batch * @as(usize, @intCast(input.shape[1]))) return error.InvalidShape;
        }
        var compiled: ?schema_mod.CompiledSchema = null;
        defer if (compiled) |*value| value.deinit();
        var prepared: ?processor.PreparedBatch = null;
        defer if (prepared) |*value| value.deinit();
        if (boundary_config != null) {
            if (batch > 8) return error.UnsupportedBoundaryBatch;
            compiled = try schema_mod.compile(A, case.schema_json orelse (if (case.kind == .entities) entity_schema else classification_schema), .{});
            const items = try A.alloc(processor.Item, batch);
            defer A.free(items);
            if (case.texts) |texts| if (texts.len != batch) return error.InvalidShape;
            for (items, 0..) |*item, i| item.* = .{ .text = if (case.texts) |texts| texts[i] else case.text, .schema = &compiled.? };
            prepared = try processor.prepare(A, tok.?, items, .{ .max_batch_items = 8, .max_text_words = 512, .max_sequence_tokens = 512, .max_queries = 64 });
            const value = &prepared.?;
            if (value.sequence_length != seq or !std.mem.eql(i64, value.input_ids, case.inputs[0].data) or !std.mem.eql(i64, value.attention_mask, case.inputs[1].data)) return error.PreparedTokenMismatch;
            if (case.kind == .entities) {
                if (!std.mem.eql(i64, value.text_word_indices, case.inputs[2].data) or !std.mem.eql(i64, value.query_marker_indices, case.inputs[4].data)) return error.PreparedRouteMismatch;
            } else if (!std.mem.eql(i64, value.cls_marker_indices, case.inputs[2].data)) return error.PreparedRouteMismatch;
        }
        const samples = try A.alloc(f64, iterations);
        const logits = try A.alloc([]f32, iterations);
        var physical_forwards: usize = 0;
        for (0..warmups + iterations) |rep| {
            var forward_count: usize = 0;
            var values: []f32 = undefined;
            var cb = try factory.getComputeBackend(session, A);
            const started = now();
            var ended: ?u64 = null;
            if (boundary_config != null) {
                const config = boundary_config.?;
                const result = try boundaryForward(&cb, config, &prepared.?, case.kind == .entities, (precision == .bf16 or precision == .bf16_full));
                forward_count += 1;
                values = result.values;
                ended = result.ended;
            } else {
                values = try A.alloc(f32, batch * labels);
                var offset: usize = 0;
                while (offset < batch) : (offset += 8) {
                    const rows = @min(@as(usize, 8), batch - offset);
                    cb.preferEagerQuantMirrors(true);
                    const hidden = try deberta.forwardCt(&cb, A, cfg, case.inputs[0].data[offset * seq ..][0 .. rows * seq], case.inputs[1].data[offset * seq ..][0 .. rows * seq], rows, seq, true);
                    forward_count += 1;
                    defer cb.free(hidden);
                    const head = try decision.forwardCt(&cb, A, hidden, case.inputs[2].data[offset * labels ..][0 .. rows * labels], case.inputs[3].data[offset * labels ..][0 .. rows * labels], rows, seq, labels, cfg.hidden_size);
                    defer cb.free(head.logits);
                    const output = try cb.toFloat32(head.logits, A);
                    defer A.free(output);
                    @memcpy(values[offset * labels ..][0 .. rows * labels], output);
                }
            }
            if (case.kind == .classification) for (values, case.inputs[3].data) |*v, valid| {
                if (!std.math.isFinite(v.*)) return error.NonFiniteOutput;
                if (valid == 0) v.* = -1e4;
            };
            for (values) |v| if (!std.math.isFinite(v)) return error.NonFiniteOutput;
            const elapsed = @as(f64, @floatFromInt((ended orelse now()) - started)) / 1e9;
            cb.deinit();
            if (rep < warmups) A.free(values) else {
                if (rep == warmups) physical_forwards = forward_count else if (physical_forwards != forward_count) return error.InconsistentPhysicalForwards;
                samples[rep - warmups] = elapsed;
                logits[rep - warmups] = values;
            }
        }
        try results.append(A, .{ .id = case.id, .physical_forwards = physical_forwards, .samples_seconds = samples, .logits = logits });
    }
    const encoder_dtype = blk: {
        var backend = try factory.getComputeBackend(session, A);
        defer backend.deinit();
        const weight = try backend.getWeight("encoder.layer.0.attention.self.query_proj.weight");
        defer backend.free(weight);
        break :blk @tagName(try backend.tensorDType(weight));
    };
    const report = try std.json.Stringify.valueAlloc(A, .{ .scope = "prepared input host tensors, encoder and requested heads through synchronized float32 outputs; includes forward temporary allocations; excludes tokenization, final decoding, model load, backend-handle setup and post-readback cleanup", .precision = args[6], .encoder_dtype = encoder_dtype, .cases = results.items, .cuda = factory.getCudaRuntimeStats(session) }, .{ .whitespace = .indent_2 });
    defer A.free(report);
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = args[3], .data = report });
}
