// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Model-free v2 request/compiler/planner fuzz target. The standard Zig test
//! runner owns coverage, crash reproducers and per-input leak detection.
//! Ordinary runs execute the full checked-in corpus and resource regressions.
const std = @import("std");
const wire = @import("extractors/extraction_v2.zig");
const schema = @import("pipelines/extraction_schema.zig");
const constraints = schema.constraints;
const regex = @import("pipelines/extraction_regex.zig");
const long = @import("pipelines/gliner_boundary_long_document.zig");
const boundary = @import("pipelines/gliner_boundary_decode.zig");
const Budget = @import("runtime/bounded_allocator.zig").BoundedAllocator;
const Control = @import("execution_control.zig").InferenceExecutionControl;
const corpus = @import("fuzz/gliner25_corpus.zig");
const Allocator = std.mem.Allocator;
const Value = std.json.Value;

const max_input_bytes = 8192;
const max_heap_bytes = 2 * 1024 * 1024;
const max_checks = 8192;
const parse_limits = wire.Limits{
    .max_request_bytes = max_input_bytes,
    .max_json_depth = 16,
    .max_inputs = 4,
    .max_text_bytes_per_input = 2048,
    .max_total_text_bytes = 4096,
    .max_total_schema_bytes = 8192,
};
const compiler_options = schema.Options{ .limits = .{
    .max_schema_bytes = 4096,
    .max_json_depth = 12,
    .max_tasks = 8,
    .max_labels = 8,
    .max_total_labels = 32,
    .max_fields = 16,
    .max_attribute_groups = 4,
    .max_examples = 8,
    .max_validators = 4,
    .max_regex_bytes = 128,
    .max_constraint_nodes = 64,
} };
const Observation = struct {
    parsed: bool = false,
    planned: usize = 0,
    multiple_windows: bool = false,
    rejection: ?anyerror = null,
};
const Checks = struct {
    remaining: usize,
    cancelled: bool = false,
    fn check(raw: ?*anyopaque) !void {
        const self: *Checks = @ptrCast(@alignCast(raw.?));
        if (self.remaining == 0) {
            self.cancelled = true;
            return error.Cancelled;
        }
        self.remaining -= 1;
    }
    fn control(self: *Checks) Control {
        return .{ .ptr = self, .check_fn = check };
    }
};

fn expectedRejection(err: anyerror, budget: *const Budget, checks: *const Checks) bool {
    if (err == error.OutOfMemory) return budget.denied;
    if (err == error.Cancelled) return checks.cancelled;
    // Input errors have a closed public mapping. Unrecognized/internal errors
    // remain failures; in particular, never swallow invariant/test errors.
    return wire.errorDetails(err) != null;
}

fn exercise(backing: Allocator, input: []const u8, heap_limit: usize, check_limit: usize) !Observation {
    var budget = Budget{ .backing = backing, .limit = heap_limit };
    var checks = Checks{ .remaining = check_limit };
    var observation = Observation{};
    const result = exerciseOwned(budget.allocator(), input, &checks, &observation);
    // The request, every compiled schema, regex cache and planner owner have
    // already drained on both success and error before inspecting accounting.
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expect(budget.peak <= heap_limit);
    result catch |err| {
        if (!expectedRejection(err, &budget, &checks)) return err;
        observation.rejection = err;
    };
    return observation;
}

fn exerciseOwned(a: Allocator, input: []const u8, checks: *Checks, observation: *Observation) !void {
    var validators = regex.Context.init(a, .{
        .compile_options = .{ .max_pattern_bytes = 128, .max_ast_nodes = 128, .max_states = 256, .max_depth = 12, .max_repeat = 16, .max_class_ranges = 256, .max_steps = 4096, .control = checks.control() },
        .match_options = .{ .max_text_bytes = 2048, .max_steps = 4096, .control = checks.control() },
        .max_patterns = 16,
        .max_total_pattern_bytes = 512,
        .max_total_states = 512,
        .max_total_compile_steps = 16384,
        .max_total_match_steps = 16384,
    });
    defer validators.deinit();
    // Version probing has its own bounded parser and must agree with the
    // strict parser. Do not inspect an unbounded generic JSON tree first.
    const version = try wire.versionJson(a, input, parse_limits);
    const options = wire.ParseOptions{ .limits = parse_limits, .compiler = validators.compilerOptions(compiler_options) };
    var request = blk: {
        const mutable = try a.dupe(u8, input);
        defer a.free(mutable);
        const parsed = try wire.parseJson(a, mutable, options);
        @memset(mutable, 0xa5);
        break :blk parsed;
    };
    defer request.deinit();
    observation.parsed = true;
    try std.testing.expectEqual(@as(u32, 2), version);
    var raw = try std.json.parseFromSlice(Value, a, input, .{ .allocate = .alloc_always, .duplicate_field_behavior = .@"error" });
    defer raw.deinit();
    try std.testing.expectEqualStrings(raw.value.object.get("model").?.string, request.model);
    const inputs = raw.value.object.get("inputs").?.array.items;
    try std.testing.expectEqual(inputs.len, request.items.len);
    for (inputs, request.items) |source, item| {
        try Checks.check(checks);
        if (source.object.get("id")) |id| {
            try std.testing.expect(item.id != null);
            try std.testing.expectEqualStrings(id.string, item.id.?);
        } else try std.testing.expect(item.id == null);
        try checkText(source.object.get("content").?, item.text);
        const effective_schema = source.object.get("schema") orelse raw.value.object.get("schema").?;
        const effective_options = source.object.get("options") orelse raw.value.object.get("options") orelse Value{ .object = .empty };
        try std.testing.expectEqualDeep(try wire.parseOptions(effective_options), item.options);
        // Independently compile the raw replacement, so merged schemas or
        // lost explicit null/false options cannot silently pass this oracle.
        const encoded = try std.json.Stringify.valueAlloc(a, effective_schema, .{});
        defer a.free(encoded);
        var compiled = try schema.compile(a, encoded, options.compiler);
        defer compiled.deinit();
        try std.testing.expectEqual(compiled.fingerprint, item.compiled.fingerprint);
        try checkPresence(effective_schema, item.compiled.schema);
        for (item.compiled.schema.entities) |entity| for (entity.validators) |validator| {
            _ = try regex.Context.validateValue(&validators, validator, item.text);
        };
        for (item.compiled.schema.structures) |structure| for (structure.fields) |field| for (field.validators) |validator| {
            _ = try regex.Context.validateValue(&validators, validator, item.text);
        };
        try checkSolver(a, item.compiled.schema.classification_constraints, checks);
        // Exercise both planner policies under one fixed tiny resource
        // profile; this harness does not execute the user's inference request.
        for ([_]long.Mode{ .reject, .windowed }) |mode| {
            const result = checkPlan(a, item, mode, checks);
            if (result) |windows| {
                observation.planned += 1;
                observation.multiple_windows = observation.multiple_windows or windows > 1;
            } else |err| switch (err) {
                error.LongDocumentWindowingRequired, error.LongDocumentWindowLimitExceeded, error.LongDocumentWorkLimitExceeded, error.BoundaryTextLimitExceeded, error.MemoryBudgetExceeded => {},
                else => return err,
            }
        }
    }
}

fn checkText(source: Value, text: []const u8) !void {
    try std.testing.expect(std.unicode.utf8ValidateSlice(text));
    if (source == .string) return std.testing.expectEqualStrings(source.string, text);
    var at: usize = 0;
    for (source.array.items, 0..) |part, index| {
        if (index != 0) {
            try std.testing.expect(at < text.len and text[at] == '\n');
            at += 1;
        }
        const fragment = part.object.get("text").?.string;
        try std.testing.expect(fragment.len <= text.len - at);
        try std.testing.expectEqualStrings(fragment, text[at..][0..fragment.len]);
        at += fragment.len;
    }
    try std.testing.expectEqual(text.len, at);
}

fn checkPresence(raw: Value, compiled: schema.Schema) !void {
    try std.testing.expectEqual(@as(u32, 2), compiled.source_version);
    if (raw.object.get("classifications")) |tasks| if (tasks == .array) {
        try std.testing.expectEqual(tasks.array.items.len, compiled.classifications.len);
        for (tasks.array.items, compiled.classifications) |source, task| {
            if (source.object.get("max_labels")) |maximum| if (maximum == .null) {
                try std.testing.expect(task.task.max_labels == null);
                try std.testing.expect(task.structured_selection);
            };
            if (source.object.get("ordered")) |ordered| if (ordered == .bool and !ordered.bool)
                try std.testing.expect(!task.task.ordered);
        }
    };
    if (raw.object.get("entity_attributes")) |attributes| if (attributes == .object) {
        for (compiled.entity_attributes) |group| {
            const source = attributes.object.get(group.name).?;
            if (source.object.get("applies_to")) |types| if (types == .array and types.array.items.len == 0) {
                try std.testing.expect(group.applies_to != null);
                try std.testing.expectEqual(@as(usize, 0), group.applies_to.?.len);
            };
        }
    };
}

fn checkSolver(a: Allocator, program: constraints.Program, checks: *Checks) !void {
    if (program.tasks.len == 0) return;
    try std.testing.expect(program.tasks.len <= 8);
    var scores: [8][8]f64 = undefined;
    var rows: [8][]const f64 = undefined;
    for (program.tasks, 0..) |task, t| {
        try std.testing.expect(task.labels.len <= 8);
        for (task.labels, 0..) |label, l| scores[t][l] = @as(f64, @floatFromInt(@as(i32, @intCast(std.hash.Wyhash.hash(t, label) % 17)) - 8)) / 4;
        rows[t] = scores[t][0..task.labels.len];
    }
    var optimum: ?f64 = null;
    for ([_]constraints.Algorithm{ .exact, .beam }) |algorithm| {
        var result = constraints.solve(a, program, rows[0..program.tasks.len], .{
            .algorithm = algorithm,
            .max_local_assignments = 64,
            .max_subset_visits = 256,
            .exact_node_budget = 512,
            .beam_node_budget = 512,
            .beam_width = 8,
            .check_context = checks,
            .check_fn = Checks.check,
        }) catch |err| switch (err) {
            error.ConstraintCandidateLimitExceeded, error.InvalidClassificationLogits => return,
            else => return err,
        };
        defer result.deinit();
        try std.testing.expect(result.visited_nodes <= 512);
        if (!result.valid()) {
            try std.testing.expectEqual(@as(usize, 0), result.selections.len);
            continue;
        }
        try std.testing.expect(std.math.isFinite(result.utility));
        const possible = [_]constraints.Selection{0} ** 8;
        try std.testing.expectEqual(constraints.Truth.yes, try program.evaluate(.{ .selected = result.selections, .possible = possible[0..program.tasks.len] }));
        for (program.tasks, result.selections) |task, selected| {
            const mask = (@as(constraints.Selection, 1) << @as(u7, @intCast(task.labels.len))) - 1;
            try std.testing.expect(selected & ~mask == 0);
            try std.testing.expect(@popCount(selected) >= task.min_labels and @popCount(selected) <= task.maximum());
        }
        if (algorithm == .exact and result.status == .optimal) optimum = result.utility;
        if (algorithm == .beam) if (optimum) |value| try std.testing.expect(result.utility <= value + 1e-9 * @max(1, @abs(value)));
    }
}

fn checkPlan(a: Allocator, item: wire.Item, mode: long.Mode, checks: *Checks) !usize {
    var document = blk: {
        const input = try a.dupe(u8, item.text);
        defer a.free(input);
        const result = try long.plan(a, input, item.compiled.fingerprint, .{
            .mode = mode,
            .word_splitter = item.options.word_splitter,
            .max_window_body_words = 8,
            .overlap_words = 2,
            .max_document_bytes = 2048,
            .max_document_words = 256,
            .max_windows = 16,
            .max_window_scan_bytes = 16384,
            .max_memory_bytes = 256 * 1024,
            .inference_fingerprint = .{1} ** 32,
            .other_record_identity = item.options.long_document.record_identity,
            .control = checks.control(),
        });
        @memset(input, 0xa5);
        break :blk result;
    };
    defer document.deinit();
    try std.testing.expectEqualStrings(item.text, document.text);
    try std.testing.expect(document.windows.len > 0 and document.windows.len <= 16);
    try std.testing.expect(document.descriptor.requires_encoded_token_admission);
    try std.testing.expectEqual(@as(usize, 0), document.windows[0].bytes.start);
    try std.testing.expectEqual(item.text.len, document.windows[document.windows.len - 1].bytes.end);
    var owned_end: usize = 0;
    for (document.windows, 0..) |window, index| {
        try Checks.check(checks);
        try std.testing.expectEqual(index, window.index);
        try std.testing.expect(window.bytes.start <= window.bytes.end and window.bytes.end <= item.text.len);
        try std.testing.expect(window.words.start <= window.owned_words.start and window.owned_words.end <= window.words.end);
        try std.testing.expect(window.words.end <= document.words.len);
        try std.testing.expect(window.words.end - window.words.start + @intFromBool(window.synthetic_terminal_word) <= 8);
        try std.testing.expectEqual(owned_end, window.owned_words.start);
        try std.testing.expect(window.owned_words.start <= window.owned_words.end);
        owned_end = window.owned_words.end;
        try std.testing.expectEqualStrings(item.text[window.bytes.start..window.bytes.end], try document.windowText(index));
        try document.validateIdentity(try document.identity(index));
        var tampered = try document.identity(index);
        tampered.plan_fingerprint[0] ^= 1;
        try std.testing.expectError(error.LongDocumentIdentityMismatch, document.validateIdentity(tampered));
        for (std.enums.values(boundary.OffsetUnit)) |from| {
            const offset = try document.offsets.convert(window.bytes, from);
            try std.testing.expectEqualDeep(window.bytes, try document.offsets.toBytes(offset, from));
            if (window.bytes.end == window.bytes.start) continue;
            for (std.enums.values(boundary.OffsetUnit)) |to| {
                const global = try document.rebase(index, .{ .start = 0, .end = offset.end - offset.start }, from, to);
                const expected = try document.offsets.convert(window.bytes, to);
                try std.testing.expectEqual(expected.start, global.start);
                try std.testing.expectEqual(expected.end, global.end);
                try std.testing.expectEqual(window.bytes.start, global.byte_start);
                try std.testing.expectEqual(window.bytes.end, global.byte_end);
            }
        }
    }
    try std.testing.expectEqual(document.words.len, owned_end);
    var previous: usize = 0;
    for (document.words, 0..) |word, index| {
        try Checks.check(checks);
        try std.testing.expect(previous <= word.start and word.start < word.end and word.end <= item.text.len);
        previous = word.end;
        const owner = try document.ownerOfSpan(.{ .start = word.start, .end = word.end });
        try std.testing.expect(owner < document.windows.len);
        const window = document.windows[owner];
        try std.testing.expect(index >= window.owned_words.start and index < window.owned_words.end);
        _ = try document.offsets.convert(.{ .start = word.start, .end = word.end }, .unicode_codepoints);
    }
    return document.windows.len;
}

fn framed(comptime bytes: []const u8) [bytes.len + 4]u8 {
    var result: [bytes.len + 4]u8 = undefined;
    std.mem.writeInt(u32, result[0..4], @intCast(bytes.len), .little);
    @memcpy(result[4..], bytes);
    return result;
}
const smith_corpus = blk: {
    var inputs: [corpus.all.len][]const u8 = undefined;
    for (corpus.all, 0..) |bytes, index| inputs[index] = &framed(bytes);
    break :blk inputs;
};

test "GLiNER25 fuzz v2 request schema and document source invariants" {
    try std.testing.fuzz({}, struct {
        fn run(_: void, smith: *std.testing.Smith) !void {
            // Smith.in is null under the real fuzzer. Always use its typed
            // input API; checking only Smith.in would silently fuzz nothing.
            var bytes: [max_input_bytes + 1]u8 = undefined;
            const len = smith.sliceWithHash(&bytes, 0x676c3235);
            _ = try exercise(std.testing.allocator, bytes[0..len], max_heap_bytes, max_checks);
        }
    }.run, .{ .corpus = &smith_corpus });
}

test "GLiNER25 fuzz corpus reaches mixed tasks replacements regex and multiwindow plans" {
    var multiple = false;
    for (corpus.valid, 0..) |bytes, index| {
        const observation = try exercise(std.testing.allocator, bytes, max_heap_bytes, max_checks);
        if (!observation.parsed or observation.rejection != null)
            std.debug.print("valid GLiNER25 corpus seed {d}: {s}\n", .{ index, if (observation.rejection) |err| @errorName(err) else "not parsed" });
        try std.testing.expect(observation.parsed);
        try std.testing.expect(observation.rejection == null);
        try std.testing.expect(observation.planned > 0);
        multiple = multiple or observation.multiple_windows;
    }
    try std.testing.expect(multiple);
    for (corpus.invalid) |bytes| {
        const observation = try exercise(std.testing.allocator, bytes, max_heap_bytes, max_checks);
        try std.testing.expect(!observation.parsed);
        try std.testing.expect(observation.rejection != null);
    }
}

test "GLiNER25 fuzz malformed truncation and delimiter mutation are owned and bounded" {
    // Every proper prefix of this one complete object is invalid JSON. This
    // exercises rejection at each string/escape/container boundary without
    // deriving the expected result from our parser's implementation.
    for (0..corpus.basic.len) |length| {
        const observation = try exercise(std.testing.allocator, corpus.basic[0..length], max_heap_bytes, max_checks);
        try std.testing.expect(!observation.parsed);
        try std.testing.expect(observation.rejection != null);
    }
    var mutation: [corpus.basic.len]u8 = undefined;
    for (corpus.basic, 0..) |byte, index| {
        if (std.mem.indexOfScalar(u8, "{}[],:\"", byte) == null) continue;
        @memcpy(&mutation, corpus.basic);
        mutation[index] = 0;
        const observation = try exercise(std.testing.allocator, &mutation, max_heap_bytes, max_checks);
        try std.testing.expect(!observation.parsed);
        try std.testing.expect(observation.rejection != null);
    }
}

fn allocationProbe(a: Allocator) !void {
    try std.testing.expect((try exercise(a, corpus.basic, max_heap_bytes, max_checks)).parsed);
}
test "GLiNER25 fuzz typed rejection cancellation allocation failure and recovery" {
    const a = std.testing.allocator;
    try std.testing.expectEqual(error.ExtractionRequestLimitExceeded, (try exercise(a, &([_]u8{'x'} ** (max_input_bytes + 1)), 0, max_checks)).rejection.?);
    try std.testing.expectEqual(error.ExtractionRequestLimitExceeded, (try exercise(a, &([_]u8{'['} ** 17 ++ [_]u8{']'} ** 17), 0, max_checks)).rejection.?);
    try std.testing.expectEqual(error.OutOfMemory, (try exercise(a, corpus.mixed, 64, max_checks)).rejection.?);
    try std.testing.expectEqual(error.Cancelled, (try exercise(a, corpus.mixed, max_heap_bytes, 0)).rejection.?);
    try std.testing.expectEqual(error.ExtractionRegexLimitExceeded, (try exercise(a, corpus.regex_casefold_limit, max_heap_bytes, max_checks)).rejection.?);
    try allocationProbe(a);
    try std.testing.checkAllAllocationFailures(a, allocationProbe, .{});
}

fn solverLimitsOwned(a: Allocator) !void {
    var request = try wire.parseJson(a, corpus.ordinal, .{ .limits = parse_limits, .compiler = compiler_options });
    defer request.deinit();
    const program = request.items[0].compiled.schema.classification_constraints;
    const scores = [_][]const f64{&.{ 3, 2, 1 }};
    const options = constraints.SolveOptions{ .algorithm = .exact, .max_local_assignments = 8, .max_subset_visits = 8, .exact_node_budget = 0, .beam_node_budget = 8, .beam_width = 2 };
    var exhausted = try constraints.solve(a, program, &scores, options);
    defer exhausted.deinit();
    try std.testing.expectEqual(constraints.Status.search_exhausted, exhausted.status);
    try std.testing.expect(exhausted.exhausted and !exhausted.valid());
    try std.testing.expectEqual(@as(usize, 0), exhausted.selections.len);
    try std.testing.expectEqual(@as(usize, 0), exhausted.visited_nodes);

    var limited = options;
    limited.max_local_assignments = 1;
    try std.testing.expectError(error.ConstraintCandidateLimitExceeded, constraints.solve(a, program, &scores, limited));
    limited = options;
    limited.beam_width = 0;
    try std.testing.expectError(error.InvalidConstraintSolveOptions, constraints.solve(a, program, &scores, limited));
    limited = options;
    limited.exact_node_budget = 8;
    var recovered = try constraints.solve(a, program, &scores, limited);
    defer recovered.deinit();
    // MinLevel excludes the highest-scoring low label. A resource retry must
    // retain the constraint and return the admissible medium label.
    try std.testing.expectEqual(constraints.Status.optimal, recovered.status);
    try std.testing.expect(!recovered.exhausted);
    try std.testing.expectEqual(@as(constraints.Selection, 2), recovered.selections[0]);
    try std.testing.expectEqual(@as(f64, 2), recovered.utility);
    try std.testing.expectEqual(constraints.Truth.yes, try program.evaluate(.{ .selected = recovered.selections, .possible = &.{0} }));

    limited.algorithm = .beam;
    limited.beam_node_budget = 0;
    var beam = try constraints.solve(a, program, &scores, limited);
    defer beam.deinit();
    try std.testing.expectEqual(constraints.Status.search_exhausted, beam.status);
    try std.testing.expect(beam.exhausted and !beam.valid());
}

test "GLiNER25 fuzz compiled solver limits preserve exhaustion validity and recovery" {
    var budget = Budget{ .backing = std.testing.allocator, .limit = max_heap_bytes };
    const result = solverLimitsOwned(budget.allocator());
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expect(budget.peak <= max_heap_bytes);
    try result;
}
