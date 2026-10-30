// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Opt-in original-checkpoint parity, including device CountLSTM dispatch.
const std = @import("std");
const platform = @import("antfly_platform");
const factory = @import("../architectures/session_factory.zig");
const executor = @import("gliner_span_v2_executor.zig");
const core = @import("gliner_span_extraction.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const wire = @import("extraction_v2.zig");
const qualification = @import("../models/gliner_specialty_qualification.zig");
const Backend = enum { native, metal, cuda };
pub const Reference = struct {
    cases: []const struct {
        id: []const u8,
        text: []const u8,
        threshold: f32 = 0.5,
        native_schema_json: []const u8,
        input_ids: []const i64,
        group_parent_markers: []const usize = &.{},
        label_markers: []const usize = &.{},
        heads: []const struct {
            counts: []const f32,
            projected: []const f32,
            /// [label, word, width] in upstream einsum order, slot zero only.
            scores: []const f32,
            span_prefix: []const f32 = &.{},
        } = &.{},
        classifier_logits: []const []const f64 = &.{},
        expected_entities: []const struct { label: []const u8, text: []const u8, start: usize, end: usize, score: f32 } = &.{},
        expected_classifications: []const struct { name: []const u8, label: []const u8, score: f32 } = &.{},
    },
};

/// Environment overrides replay the same reviewed capture, never new evidence.
pub fn verifyCapture(profile: qualification.Profile, bytes: []const u8) !void {
    const pin: struct { size_bytes: usize, sha256: []const u8 } = switch (profile) {
        .pii => .{ .size_bytes = @as(usize, 3532851), .sha256 = "eb694cac399bf4c40b9bbab8d1cc50820190bbf59142c42e427e8833dfb8f3ff" },
        .gliguard => .{ .size_bytes = @as(usize, 74265), .sha256 = "52d2cccfa21b18505e54f0fe98fbd8750538ebc1421e870a6bb544f13909fbb7" },
    };
    if (bytes.len != pin.size_bytes) return error.InvalidGlinerSpecialtyOracle;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    if (!std.mem.eql(u8, &hex, pin.sha256)) return error.InvalidGlinerSpecialtyOracle;
}

test "gliner specialty frozen PII oracle rejects drift" {
    const a = std.testing.allocator;
    const capture = platform.env.getenv("ANTFLY_GLINER_PII_CAPTURE") orelse return error.SkipZigTest;
    const bytes = try @import("../util/c_file.zig").readFileMax(a, capture, 64 * 1024 * 1024);
    defer a.free(bytes);
    try verifyCapture(.pii, bytes);
    bytes[bytes.len / 2] ^= 1;
    try std.testing.expectError(error.InvalidGlinerSpecialtyOracle, verifyCapture(.pii, bytes));
    try std.testing.expectError(error.InvalidGlinerSpecialtyOracle, verifyCapture(.pii, bytes[0 .. bytes.len - 1]));
}

test "gliner specialty frozen GLiGuard oracle rejects drift" {
    const a = std.testing.allocator;
    const capture = platform.env.getenv("ANTFLY_GLIGUARD_CAPTURE") orelse return error.SkipZigTest;
    const bytes = try @import("../util/c_file.zig").readFileMax(a, capture, 64 * 1024 * 1024);
    defer a.free(bytes);
    try verifyCapture(.gliguard, bytes);
    bytes[bytes.len / 2] ^= 1;
    try std.testing.expectError(error.InvalidGlinerSpecialtyOracle, verifyCapture(.gliguard, bytes));
    try std.testing.expectError(error.InvalidGlinerSpecialtyOracle, verifyCapture(.gliguard, bytes[0 .. bytes.len - 1]));
}

fn expectValues(expected: []const f32, actual: []const f32, abs: f32, relative: f32) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |want, got| {
        try std.testing.expect(std.math.isFinite(got));
        try std.testing.expectApproxEqAbs(want, got, abs + relative * @abs(want));
    }
}

/// Measured preparation boundary, shared by head parity and public HTTP tests.
/// Schema fragments and synthetic punctuation are included in the token count.
pub fn textAtPreparedTokenLimit(a: std.mem.Allocator, tokenizer: @import("inference_tokenizer").Tokenizer, compiled: *const @import("../pipelines/extraction_schema.zig").CompiledSchema, target: usize) ![]u8 {
    var unit: ?[]const u8 = null;
    for ([_][]const u8{ "the", "hello", "contact", "test", "word" }) |candidate| {
        const encoded = try tokenizer.encode(a, candidate);
        defer a.free(encoded);
        if (encoded.len == 1) {
            unit = candidate;
            break;
        }
    }
    const fragment = try std.mem.concat(a, u8, &.{ unit orelse return error.TestUnexpectedResult, " " });
    defer a.free(fragment);
    const text = try a.alloc(u8, (target + 1) * fragment.len);
    defer a.free(text);
    for (0..target + 1) |i| @memcpy(text[i * fragment.len ..][0..fragment.len], fragment);
    var low: usize = 0;
    var high: usize = target + 1;
    while (low <= high) {
        const middle = low + (high - low) / 2;
        const prefix = text[0 .. middle * fragment.len];
        var prepared = try processor.prepare(a, tokenizer, &.{.{ .text = prefix, .schema = compiled }}, .{ .max_sequence_tokens = 4096, .max_batch_tokens = 4096 });
        defer prepared.deinit();
        const tokens = prepared.samples[0].input_ids.len;
        if (tokens == target) return a.dupe(u8, prefix);
        if (tokens < target) {
            low = middle + 1;
        } else {
            if (middle == 0) break;
            high = middle - 1;
        }
    }
    return error.TestUnexpectedResult;
}

fn selectCaseIndices(a: std.mem.Allocator, cases: anytype, selector: ?[]const u8) ![]usize {
    var indices = std.ArrayListUnmanaged(usize).empty;
    errdefer indices.deinit(a);
    if (selector) |raw| {
        var tokens = std.mem.splitScalar(u8, raw, ',');
        while (tokens.next()) |token| {
            const id = std.mem.trim(u8, token, " \t\r\n");
            if (id.len == 0) return error.InvalidGlinerSpecialtySanitizerSelection;
            var found: ?usize = null;
            for (cases, 0..) |case, index| if (std.mem.eql(u8, case.id, id)) {
                found = index;
                break;
            };
            const index = found orelse return error.InvalidGlinerSpecialtySanitizerSelection;
            for (indices.items) |previous| if (previous == index) return error.InvalidGlinerSpecialtySanitizerSelection;
            try indices.append(a, index);
        }
    } else for (0..cases.len) |index| try indices.append(a, index);
    if (indices.items.len == 0) return error.InvalidGlinerSpecialtySanitizerSelection;
    return indices.toOwnedSlice(a);
}

test "gliner specialty sanitizer case selector is explicit ordered and fail closed" {
    const a = std.testing.allocator;
    const cases = [_]struct { id: []const u8 }{ .{ .id = "one" }, .{ .id = "two" } };
    const all = try selectCaseIndices(a, &cases, null);
    defer a.free(all);
    try std.testing.expectEqualSlices(usize, &.{ 0, 1 }, all);
    const selected = try selectCaseIndices(a, &cases, "two,one");
    defer a.free(selected);
    try std.testing.expectEqualSlices(usize, &.{ 1, 0 }, selected);
    for ([_][]const u8{ "", "missing", "one,", "one,one" }) |bad|
        try std.testing.expectError(error.InvalidGlinerSpecialtySanitizerSelection, selectCaseIndices(a, &cases, bad));
}

fn parity(profile: qualification.Profile, backend: Backend) !void {
    if (backend == .cuda and !@import("build_options").enable_cuda) return error.SkipZigTest;
    if (backend == .metal and !@import("build_options").enable_metal) return error.SkipZigTest;
    const a = std.testing.allocator;
    const directory = platform.env.getenv(if (profile == .pii) "ANTFLY_GLINER_PII_MODEL_DIR" else "ANTFLY_GLIGUARD_MODEL_DIR") orelse return error.SkipZigTest;
    const capture = platform.env.getenv(if (profile == .pii) "ANTFLY_GLINER_PII_CAPTURE" else "ANTFLY_GLIGUARD_CAPTURE") orelse return error.SkipZigTest;
    const bytes = try @import("../util/c_file.zig").readFileMax(a, capture, 64 * 1024 * 1024);
    defer a.free(bytes);
    try verifyCapture(profile, bytes);
    const reference = try std.json.parseFromSlice(Reference, a, bytes, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    try std.testing.expect(reference.value.cases.len != 0);
    const sanitizer_selector = platform.env.getenv("ANTFLY_GLINER_SPECIALTY_SANITIZER_CASES");
    const case_indices = try selectCaseIndices(a, reference.value.cases, sanitizer_selector);
    defer a.free(case_indices);
    const case_ids = try a.alloc([]const u8, case_indices.len);
    defer a.free(case_ids);
    for (case_indices, case_ids) |index, *id| id.* = reference.value.cases[index].id;
    const session = switch (backend) {
        .native => try factory.createNativeSession(a, directory),
        .metal => try factory.createMetalSession(a, directory),
        .cuda => try factory.createCudaSession(a, directory),
    };
    defer session.close();
    const config = (try factory.getGlinerSpanConfig(session)).deberta;
    const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try @import("../util/c_file.zig").readFile(a, tokenizer_path);
    defer a.free(tokenizer_bytes);
    try factory.sealGlinerDecisionTokenizerDigest(session, qualification.Digest.of(tokenizer_bytes));
    const identity = try factory.getGlinerDecisionIdentity(session);
    try std.testing.expectEqual(profile, try qualification.require(identity));
    const tok = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tok.tokenizer().deinitTokenizer();
    const watchdog = if (backend != .native) try @import("../hard_cancellation_watchdog.zig").HardCancellationWatchdog.create(a) else null;
    defer if (watchdog) |owner| owner.destroy();
    if (watchdog) |owner| try owner.start(std.testing.io);
    const control = @import("../execution_control.zig").InferenceExecutionControl{ .hard_cancellation = if (watchdog) |owner| owner.boundary() else null, .deadline_ns = platform.time.monotonicNs() + 1200 * std.time.ns_per_s };
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    const cb = &managed.backend;
    for (case_indices) |case_index| {
        const case = reference.value.cases[case_index];
        errdefer std.debug.print("specialty {s} {s} parity case {s}\n", .{ @tagName(profile), @tagName(backend), case.id });
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const request_json = try std.fmt.allocPrint(temporary, "{{\"schema_version\":2,\"model\":\"specialty\",\"schema\":{s},\"inputs\":[{{\"content\":{f}}}],\"options\":{{\"offset_unit\":\"unicode_codepoints\",\"threshold\":{d}}}}}", .{ case.native_schema_json, std.json.fmt(case.text, .{}), case.threshold });
        var request = try wire.parseJson(a, request_json, .{});
        defer request.deinit();
        const options = executor.Options{ .specialty_profile = profile, .control = control };
        var planned = try executor.plan(a, tok.tokenizer(), &request, options);
        defer planned.deinit(a);
        const item = &request.items[0];
        const sample = planned.items[0].batches[0].samples[0];
        try std.testing.expectEqualSlices(i64, case.input_ids, sample.input_ids);
        if (std.mem.eql(u8, case.id, reference.value.cases[0].id)) {
            const text512 = try textAtPreparedTokenLimit(temporary, tok.tokenizer(), &item.compiled, 512);
            const text513 = try textAtPreparedTokenLimit(temporary, tok.tokenizer(), &item.compiled, 513);
            var boundary_item = item.*;
            boundary_item.text = text512;
            var accepted = try executor.prepareItem(a, tok.tokenizer(), &boundary_item, .{ .max_sequence_tokens = 4096, .max_batch_tokens = 4096 }, options);
            defer accepted.deinit(a);
            try std.testing.expectEqual(@as(usize, 1), accepted.batches.len);
            try std.testing.expectEqual(@as(usize, 512), accepted.batches[0].samples[0].input_ids.len);
            boundary_item.text = text513;
            try std.testing.expectError(error.BoundarySequenceLimitExceeded, executor.prepareItem(a, tok.tokenizer(), &boundary_item, .{ .max_sequence_tokens = 4096, .max_batch_tokens = 4096 }, options));
        }

        if (case.group_parent_markers.len != 0) {
            try std.testing.expectEqual(case.group_parent_markers.len, sample.groups.len);
            for (case.group_parent_markers, sample.groups) |want, group| try std.testing.expectEqual(want, group.parent_marker);
        }
        if (profile == .gliguard) {
            const counts = try temporary.alloc(usize, item.compiled.schema.classifications.len);
            for (counts, item.compiled.schema.classifications) |*count, task| count.* = task.task.labels.len;
            if (case.label_markers.len != 0) {
                try std.testing.expectEqual(case.label_markers.len, sample.classification_labels.len);
                for (case.label_markers, sample.classification_labels) |want, label| try std.testing.expectEqual(want, label.marker_index);
            }
            const before = cb.trainingRuntimeStats();
            const logits = try executor.classificationLogits(cb, a, .{ .deberta = config }, sample, counts);
            defer {
                for (logits) |row| a.free(row);
                a.free(logits);
            }
            const after = cb.trainingRuntimeStats();
            if (backend == .cuda) {
                try std.testing.expectEqual(before.gliner_label_gru_calls, after.gliner_label_gru_calls);
                try std.testing.expectEqual(before.to_float32_calls + 1, after.to_float32_calls);
            }
            try std.testing.expectEqual(case.classifier_logits.len, logits.len);
            for (case.classifier_logits, logits) |want, got| {
                try std.testing.expectEqual(want.len, got.len);
                for (want, got) |x, y| try std.testing.expectApproxEqAbs(x, y, 2e-3);
            }
            {
                const rows = try temporary.alloc([]const f64, logits.len);
                for (rows, logits) |*row, values| row.* = values;
                var presented = try pipeline.presentClassifications(a, &item.compiled, rows, 1, item.options.native(.{}));
                defer presented.deinit();
                var index: usize = 0;
                for (presented.classifications) |task| for (task.labels) |label| {
                    try std.testing.expect(index < case.expected_classifications.len);
                    const expected = case.expected_classifications[index];
                    try std.testing.expectEqualStrings(expected.name, task.name);
                    try std.testing.expectEqualStrings(expected.label, label.label);
                    try std.testing.expectApproxEqAbs(expected.score, label.confidence, pipeline.fp32_confidence_tolerance);
                    index += 1;
                };
                try std.testing.expectEqual(case.expected_classifications.len, index);
            }
            continue;
        }
        const mask = try temporary.alloc(i64, sample.input_ids.len);
        @memset(mask, 1);
        cb.preferEagerQuantMirrors(true);
        const hidden = try @import("../architectures/deberta.zig").forwardCtProfiled(cb, a, config, sample.input_ids, mask, 1, sample.input_ids.len, @import("../models/deberta.zig").glinerPrefersWeightMirrors(config), null);
        defer cb.free(hidden);
        const H: usize = config.hidden_size;
        const spans = try core.spanRepresentations(cb, a, hidden, sample, H);
        defer cb.free(spans);
        try std.testing.expectEqual(case.heads.len, sample.groups.len);
        for (case.heads, sample.groups) |ref, group| {
            if (ref.span_prefix.len != 0) {
                const raw = try cb.toFloat32(spans, temporary);
                try expectValues(ref.span_prefix, raw[0..ref.span_prefix.len], 2e-3, 2e-6);
            }
            const parent = try core.gather(cb, temporary, hidden, &.{group.parent_marker}, H);
            defer cb.free(parent);
            const count_ct = try core.mlp(cb, temporary, parent, H, 20, "count_pred");
            defer cb.free(count_ct);
            const raw_count = try cb.toFloat32(count_ct, temporary);
            try expectValues(ref.counts, raw_count, 2e-3, 0);
            const labels = try core.gather(cb, temporary, hidden, group.child_markers, H);
            defer cb.free(labels);
            const columns = group.child_markers.len;
            if (case.label_markers.len != 0) try std.testing.expectEqualSlices(usize, case.label_markers, group.child_markers);
            const before = cb.trainingRuntimeStats();
            const projected = if (backend == .native)
                try @import("../architectures/gliner_head.zig").countLstmV1AtStep(cb, temporary, labels, columns, H, 0)
            else
                (try @import("../architectures/gliner_head.zig").countLstmV1SlotZeroResident(cb, labels, columns, H)) orelse return error.TestUnexpectedResult;
            defer cb.free(projected);
            const after = cb.trainingRuntimeStats();
            if (backend == .cuda) {
                try std.testing.expectEqual(before.to_float32_calls, after.to_float32_calls);
                try std.testing.expectEqual(before.gliner_label_gru_no_residual_calls + 1, after.gliner_label_gru_no_residual_calls);
                try std.testing.expectEqual(before.d2h_bytes, after.d2h_bytes);
            }
            const raw_projected = try cb.toFloat32(projected, temporary);
            try expectValues(ref.projected, raw_projected, 2e-3, 0);
            const rows = sample.words.len * 8;
            const scores = if (backend == .native)
                try cb.linearNoBias(spans, projected, rows, H, columns)
            else
                try cb.primDotGeneral(spans, projected, &.{ @intCast(rows), @intCast(H) }, &.{ @intCast(columns), @intCast(H) }, &.{1}, &.{1}, &.{}, &.{});
            defer cb.free(scores);
            const raw_scores = try cb.toFloat32(scores, temporary);
            try std.testing.expectEqual(columns * rows, ref.scores.len);
            for (0..sample.words.len) |start| for (0..@min(8, sample.words.len - start)) |width| for (0..columns) |column| {
                const want = ref.scores[(column * sample.words.len + start) * 8 + width];
                const got = raw_scores[(start * 8 + width) * columns + column];
                try std.testing.expectApproxEqAbs(want, got, 2e-3 + 1e-5 * @abs(want));
                try std.testing.expectApproxEqAbs(1 / (1 + @exp(-want)), 1 / (1 + @exp(-got)), pipeline.fp32_confidence_tolerance);
            };
        }
        const before = cb.trainingRuntimeStats();
        var result = try core.runFromHidden(cb, a, hidden, H, sample, &item.compiled, item.options.native(.{}), .{ .confidence_first_greedy = true, .require_device_gru = true });
        defer result.deinit();
        const after = cb.trainingRuntimeStats();
        if (backend == .cuda) {
            // Only the count gate and (when nonzero) complete span scores may
            // cross to the host. GRU weights, state and labels stay resident.
            const gate_open = std.mem.indexOfMax(f32, case.heads[0].counts) != 0;
            try std.testing.expectEqual(before.to_float32_calls + 1 + @intFromBool(gate_open), after.to_float32_calls);
            try std.testing.expectEqual(before.gliner_label_gru_no_residual_calls + @intFromBool(gate_open), after.gliner_label_gru_no_residual_calls);
        }
        var index: usize = 0;
        for (result.samples[0].entities) |group| for (group.values) |value| {
            try std.testing.expect(index < case.expected_entities.len);
            const expected = case.expected_entities[index];
            try std.testing.expectEqualStrings(expected.label, group.name);
            try std.testing.expectEqualStrings(expected.text, value.text);
            try std.testing.expectEqual(expected.start, value.source.?.start);
            try std.testing.expectEqual(expected.end, value.source.?.end);
            try std.testing.expectApproxEqAbs(expected.score, value.confidence, pipeline.fp32_confidence_tolerance);
            index += 1;
        };
        try std.testing.expectEqual(case.expected_entities.len, index);
    }
    const receipt = try std.json.Stringify.valueAlloc(a, .{
        .profile = @tagName(profile),
        .backend = @tagName(backend),
        .case_ids = case_ids,
        .cases = case_indices.len,
        .fixture_cases = reference.value.cases.len,
        .sanitizer_case_selector = sanitizer_selector,
    }, .{});
    defer a.free(receipt);
    std.debug.print("specialty_head_receipt: {s}\n", .{receipt});
}

test "gliner specialty PII pinned native head oracle" {
    try parity(.pii, .native);
}
test "gliner specialty PII pinned CUDA head oracle" {
    try parity(.pii, .cuda);
}
test "gliner specialty PII pinned Metal head oracle" {
    try parity(.pii, .metal);
}
test "gliner specialty GLiGuard pinned native classifier oracle" {
    try parity(.gliguard, .native);
}
test "gliner specialty GLiGuard pinned CUDA classifier oracle" {
    try parity(.gliguard, .cuda);
}
test "gliner specialty GLiGuard pinned Metal classifier oracle" {
    try parity(.gliguard, .metal);
}
