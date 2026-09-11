// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Actual learned three-window task composition on one pinned small FP32 model.
//! This is an Antfly window-policy regression, not an upstream global-window
//! oracle or quality claim. Relation exercise thresholds are fixed before any
//! run; source tensors, proposal floors and the global threshold .5 stay intact.
const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const factory = @import("../architectures/session_factory.zig");
const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const model = @import("../models/gliner_boundary.zig");
const wire = @import("extraction_v2.zig");
const executor = @import("gliner_boundary_long_executor.zig");
const observer_mod = @import("extraction_observer.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const document = @import("../pipelines/gliner_boundary_long_document.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const schema_mod = @import("../pipelines/extraction_schema.zig");
const classification = @import("../pipelines/extraction_constraints.zig");
const joint = @import("../pipelines/extraction_joint_ie.zig");
const relation = @import("../pipelines/gliner_boundary_relations.zig");
const offsets = @import("../pipelines/gliner_boundary_decode.zig");
const snapshot = @import("../runtime/file_snapshot.zig");
const Budget = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Watchdog = @import("../hard_cancellation_watchdog.zig").HardCancellationWatchdog;
const Allocator = std.mem.Allocator;
const mib = 1024 * 1024;
const confidence_tolerance: f32 = 5e-4;
const item_cap = 128;
const Kind = enum { attributes, natural, latent, anchorless, classification, ordinary_relations, joint_relations };
const Spec = struct { id: []const u8, kind: Kind };
const specs = [_]Spec{
    .{ .id = "entity_attributes", .kind = .attributes },
    .{ .id = "record_natural", .kind = .natural },
    .{ .id = "record_latent", .kind = .latent },
    .{ .id = "record_anchorless", .kind = .anchorless },
    .{ .id = "constrained_classification", .kind = .classification },
    .{ .id = "mixed_tasks", .kind = .ordinary_relations },
    .{ .id = "joint_ie", .kind = .joint_relations },
};
const Case = struct {
    spec: Spec,
    original: []const u8,
    expected: pipeline.ExpectedSample,
    text: []const u8,
    second_start: usize,
    window_words: usize,
    overlap_words: usize,
    schema: std.json.Value,
    compiled: schema_mod.CompiledSchema,
};
const Corpus = struct {
    backing: Allocator,
    arena: std.heap.ArenaAllocator,
    reference: pipeline.ReferenceFixture = undefined,
    cases: [specs.len]Case = undefined,
    initialized: usize = 0,

    fn create(backing: Allocator) !*Corpus {
        const self = try backing.create(Corpus);
        self.* = .{ .backing = backing, .arena = std.heap.ArenaAllocator.init(backing) };
        errdefer self.destroy();
        const a = self.arena.allocator();
        const bytes = try fixtures.fixtureBytes(a, "pipeline_cases.json");
        var fixture_hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &fixture_hash, .{});
        try std.testing.expectEqualStrings("c0e9fe877751585d141832d5e8f4a27d51074568452966f113365ed255ed5c90", &std.fmt.bytesToHex(fixture_hash, .lower));
        self.reference = try std.json.parseFromSliceLeaky(pipeline.ReferenceFixture, a, bytes, .{ .allocate = .alloc_always });
        try std.testing.expectEqual(@as(u32, 2), self.reference.format_version);
        try std.testing.expectEqualStrings("cab1bddfd30fda7b803a4691c41f90378a2d517a", self.reference.revision);
        try std.testing.expectEqualStrings("030c979419cee5fd209ba6a6f23f2e0cc7744e396d4bf3357925651293f5fe5f", self.reference.requests_sha256);
        try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", self.reference.source_commit);
        try std.testing.expectEqualStrings("small", self.reference.model);
        try std.testing.expectEqualStrings("unicode_codepoints", self.reference.offset_unit);
        for (specs, &self.cases) |spec, *out| {
            const source = for (self.reference.cases) |value| {
                if (std.mem.eql(u8, spec.id, value.id)) break value;
            } else return error.MissingLearnedWindowFixture;
            // All selected published inputs are ASCII with terminal punctuation.
            // The generated document adds Unicode only between complete copies.
            for (source.text) |byte| try std.testing.expect(byte < 128);
            try std.testing.expect(source.text.len > 0 and source.text[source.text.len - 1] == '.');
            const ranges = try processor.sourceWordRanges(a, source.text, .{ .max_text_bytes = 4096, .max_words = 64 });
            defer a.free(ranges);
            const count = ranges.len;
            try std.testing.expect(count >= 5 and count <= 16);
            try std.testing.expect(!try processor.terminalWordAdded(a, source.text, .whitespace));
            // The actual splitter counts punctuation separately. These exact
            // profiles have a middle window ending mid-sentence, so reserve
            // one source-word slot for its synthetic terminal period. With
            // odd (count + separator_words), 2*stride - 1 reaches copy two.
            const separator = if (count % 2 == 0) " 🙂 🙂 🙂 " else " 🙂 🙂 ";
            const separator_words: usize = if (count % 2 == 0) 3 else 2;
            const stride = (count + separator_words + 1) / 2;
            const text = try std.mem.concat(a, u8, &.{ source.text, separator, source.text });
            const source_schema = try std.json.Stringify.valueAlloc(a, source.schema, .{});
            var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, source_schema, .{ .allocate = .alloc_always });
            if (spec.kind == .ordinary_relations) {
                try schema.object.getPtr("relations").?.array.items[0].object.put(a, "threshold", .{ .float = 0 });
            } else if (spec.kind == .joint_relations) {
                const relation_schema = schema.object.getPtr("joint_ie").?.object.getPtr("relations").?.object.getPtr("works_for").?;
                try relation_schema.object.put(a, "threshold", .{ .float = 1e-6 });
                try relation_schema.object.put(a, "candidate_threshold", .{ .float = 0 });
            }
            const schema_bytes = try std.json.Stringify.valueAlloc(a, schema, .{});
            var compiled = try schema_mod.compile(a, schema_bytes, .{ .schema_version = 2 });
            errdefer compiled.deinit();
            var plan = try document.plan(a, text, compiled.fingerprint, .{
                .mode = .windowed,
                .max_window_body_words = count,
                .overlap_words = count - stride,
                .max_windows = 3,
                .inference_fingerprint = [_]u8{1} ** 32,
            });
            defer plan.deinit();
            try std.testing.expectEqual(@as(usize, 3), plan.windows.len);
            try std.testing.expectEqualStrings(source.text, try plan.windowText(0));
            try std.testing.expectEqualStrings(source.text, try plan.windowText(2));
            try std.testing.expect(plan.windows[1].synthetic_terminal_word);
            try std.testing.expectEqual(count - 1, plan.windows[1].words.end - plan.windows[1].words.start);
            try std.testing.expectEqual(source.text.len + separator.len, plan.windows[2].bytes.start);
            for (plan.windows[1..], 1..) |window, index| {
                try std.testing.expect(window.words.start < plan.windows[index - 1].words.end);
                try std.testing.expectEqual(plan.windows[index - 1].owned_words.end, window.owned_words.start);
            }
            out.* = .{ .spec = spec, .original = source.text, .expected = source.expected, .text = text, .second_start = source.text.len + separator.len, .window_words = count, .overlap_words = count - stride, .schema = schema, .compiled = compiled };
            self.initialized += 1;
        }
        return self;
    }

    fn destroy(self: *Corpus) void {
        for (self.cases[0..self.initialized]) |*case| case.compiled.deinit();
        self.arena.deinit();
        self.backing.destroy(self);
    }
};

const Captures = struct {
    backing: Allocator,
    budget: Budget,
    results: [specs.len]executor.Result = undefined,
    initialized: usize = 0,
    fn create(a: Allocator) !*Captures {
        const self = try a.create(Captures);
        self.* = .{ .backing = a, .budget = .{ .backing = a, .limit = 256 * mib } };
        return self;
    }
    fn destroy(self: *Captures) void {
        for (self.results[0..self.initialized]) |*result| result.deinit();
        std.debug.assert(self.budget.live == 0);
        self.backing.destroy(self);
    }
};

// Request-owned schemas/text are overwritten before freeing. Result strings
// are checked after that destruction and after physical model/session close.
const Poison = struct {
    backing: Allocator,
    fn allocator(self: *Poison) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        const self: *Poison = @ptrCast(@alignCast(raw));
        return self.backing.rawAlloc(len, alignment, ret);
    }
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret: usize) void {
        const self: *Poison = @ptrCast(@alignCast(raw));
        @memset(bytes, 0xdd);
        self.backing.rawFree(bytes, alignment, ret);
    }
};
const Progress = struct {
    windows: usize = 0,
    merges: usize = 0,
    planned: usize = 0,
    cancel_after: ?usize = null,
    fn receive(raw: ?*anyopaque, event: observer_mod.Event) void {
        const self: *Progress = @ptrCast(@alignCast(raw.?));
        switch (event) {
            .window_completed => self.windows += 1,
            .document_planned => |count| self.planned = count,
            .phase => |phase| if (phase == .merging) {
                self.merges += 1;
            },
            else => {},
        }
    }
    fn check(raw: ?*anyopaque) !void {
        const self: *Progress = @ptrCast(@alignCast(raw.?));
        if (self.cancel_after) |maximum| if (self.windows >= maximum) return error.Cancelled;
    }
};

fn pinnedFiles(a: Allocator, directory: []const u8, pins: pipeline.PublishedModelFiles, control: Control) !void {
    inline for (@typeInfo(pipeline.PublishedModelFiles).@"struct".fields) |field| {
        const pin = @field(pins, field.name);
        const path = try std.fs.path.join(a, &.{ directory, field.name });
        defer a.free(path);
        const digest = try snapshot.digest(std.testing.io, std.Io.Dir.cwd(), path, 1024 * mib, control);
        try std.testing.expectEqual(pin.size_bytes, digest.size_bytes);
        try std.testing.expectEqualStrings(pin.sha256, &std.fmt.bytesToHex(digest.sha256, .lower));
    }
}
fn pinnedIdentity(pins: pipeline.PublishedModelFiles, identity: bundle.Identity) !void {
    try std.testing.expectEqual(.small, identity.backbone);
    try std.testing.expectEqual(.fp32, identity.precision);
    try std.testing.expectEqualStrings(pins.@"model.safetensors".sha256, &identity.weight.sha256);
    try std.testing.expectEqual(pins.@"model.safetensors".size_bytes, identity.weight.size_bytes);
    inline for (bundle.sidecar_names, 0..) |name, index| {
        try std.testing.expectEqualStrings(@field(pins, name).sha256, &identity.sidecars[index].sha256);
        try std.testing.expectEqual(@field(pins, name).size_bytes, identity.sidecars[index].size_bytes);
    }
}
fn requestJson(a: Allocator, case: Case) ![]u8 {
    // These are diagnostic decoder settings, fixed independently of observed
    // outputs. No source model/proposal configuration is edited by this test.
    const common = .{
        .threshold = @as(f32, 0.5),
        .include_spans = true,
        .include_confidence = true,
        .offset_unit = "utf16_codeunits",
        .long_document = .{ .mode = "window", .window_words = case.window_words, .overlap_words = case.overlap_words, .max_windows = @as(usize, 3), .record_identity = "occurrence" },
    };
    const common_bytes = try std.json.Stringify.valueAlloc(a, common, .{});
    defer a.free(common_bytes);
    var options = try std.json.parseFromSliceLeaky(std.json.Value, a, common_bytes, .{ .allocate = .alloc_always });
    // Non-Joint schemas intentionally reject the presence of this key.
    // Every allocation here belongs to the caller's short request arena.
    if (case.spec.kind == .joint_relations) {
        const joint_bytes = try std.json.Stringify.valueAlloc(a, .{ .candidate_threshold = @as(f64, 0), .entity_threshold = @as(f64, 0.5), .relation_role_threshold = @as(f64, 0), .top_k_entities = @as(usize, 4), .top_k_roles = @as(usize, 4), .relation_pair_cap = @as(usize, 4), .max_edges_per_type = @as(usize, 4) }, .{});
        defer a.free(joint_bytes);
        const config = try std.json.parseFromSliceLeaky(std.json.Value, a, joint_bytes, .{ .allocate = .alloc_always });
        try options.object.put(a, "joint_ie", config);
    }
    return std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = "boundary-small-pinned",
        .schema = case.schema,
        .inputs = .{.{ .content = case.text }},
        .options = options,
    }, .{});
}

fn one(cb: *const @import("../ops/ops.zig").ComputeBackend, a: Allocator, config: *const model.Config, tokenizer: @import("inference_tokenizer").Tokenizer, identity: bundle.Identity, case: Case, control: Control, progress: *Progress, output_cap: usize) !executor.Result {
    var request_budget = Budget{ .backing = std.testing.allocator, .limit = 8 * mib };
    defer std.debug.assert(request_budget.live == 0);
    var poison = Poison{ .backing = request_budget.allocator() };
    var request_arena = std.heap.ArenaAllocator.init(poison.allocator());
    defer request_arena.deinit();
    const request_allocator = request_arena.allocator();
    const bytes = try requestJson(request_allocator, case);
    var request = try wire.parseJson(request_allocator, bytes, .{});
    defer request.deinit();
    try std.testing.expectEqualSlices(u8, &case.compiled.fingerprint, &request.items[0].compiled.fingerprint);
    var options = executor.Options{
        .identity = identity,
        .control = control,
        .observer = .{ .context = progress, .emit_fn = Progress.receive },
        .limits = .{ .max_total_encoded_tokens = 3 * 512, .max_total_attention_work = 256 * mib, .max_retained_evidence_bytes = 32 * mib, .max_request_windows = 3 },
    };
    options.limits.plan.max_windows = 3;
    options.limits.plan.max_document_bytes = 4096;
    options.limits.plan.max_document_words = 64;
    options.limits.plan.max_memory_bytes = 4 * mib;
    options.limits.merge.max_input_candidates = 2048;
    options.pipeline.max_output_values = output_cap;
    return if (cb.kind() == .metal)
        executor.executeDevice(cb, a, config, tokenizer, &request.items[0], options)
    else
        executor.executeNative(cb, a, config, tokenizer, &request.items[0], options);
}

fn runBackend(comptime metal: bool, directory: []const u8, corpus: *const Corpus) !*Captures {
    const a = std.testing.allocator;
    const captures = try Captures.create(a);
    errdefer captures.destroy();
    const output = captures.budget.allocator();
    const watchdog: ?*Watchdog = if (metal) try Watchdog.create(a) else null;
    defer if (watchdog) |value| value.destroy();
    if (watchdog) |value| try value.start(std.testing.io);
    const lifetime = Control{ .io = std.testing.io, .deadline_ns = platform.time.monotonicNs() + 600 * std.time.ns_per_s, .hard_cancellation = if (watchdog) |value| value.boundary() else null };
    // This independent guard spans constructor and physical close. The
    // deliberate cancellation is cooperative at a completed-window cut, and
    // is never installed as a fatal watchdog callback.
    var lifetime_guard = try lifetime.enterUninterruptible(if (metal) .process_required else .cooperative);
    defer lifetime_guard.deinit();
    try pinnedFiles(a, directory, corpus.reference.model_files, lifetime);
    const session = if (metal) try factory.createMetalSession(a, directory) else try factory.createNativeSession(a, directory);
    defer session.close();
    const identity = try factory.getGlinerBoundaryIdentity(session);
    try pinnedIdentity(corpus.reference.model_files, identity);
    const config = try factory.getGlinerBoundaryConfig(session);
    const token_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(token_path);
    const token_bytes = try snapshot.read(a, std.testing.io, std.Io.Dir.cwd(), token_path, 32 * mib, lifetime);
    defer a.free(token_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, token_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    var managed = try factory.getManagedComputeBackend(session, a, null, lifetime);
    defer managed.deinit();
    var progress = Progress{};
    var controlled = lifetime;
    // The independently armed lifetime guard and managed backend retain the
    // hard carrier. A request-local cooperative callback must never be armed
    // by a nested process-fatal guard, including after one completed window.
    controlled.hard_cancellation = null;
    controlled.ptr = &progress;
    controlled.check_fn = Progress.check;
    for (corpus.cases, &captures.results, 0..) |case, *result, index| {
        errdefer std.debug.print("learned multi-window backend={s} case={s} completed={d} merges={d}\n", .{ if (metal) "metal" else "native", case.spec.id, progress.windows, progress.merges });
        progress = .{};
        result.* = try one(&managed.backend, output, &config, tokenizer.tokenizer(), identity, case, controlled, &progress, 256);
        captures.initialized += 1;
        try std.testing.expectEqual(@as(usize, 3), progress.planned);
        try std.testing.expectEqual(@as(usize, 3), progress.windows);
        try std.testing.expectEqual(@as(usize, 1), progress.merges);
        try validateCase(a, case, result.*);
        if (case.spec.kind == .natural) {
            const baseline = captures.budget.live;
            progress = .{ .cancel_after = 1 };
            try expectFailed(error.Cancelled, one(&managed.backend, output, &config, tokenizer.tokenizer(), identity, case, controlled, &progress, 256));
            try std.testing.expectEqual(@as(usize, 1), progress.windows);
            try std.testing.expectEqual(@as(usize, 0), progress.merges);
            try std.testing.expectEqual(baseline, captures.budget.live);
            progress = .{};
            try expectFailed(error.ExtractionOutputLimitExceeded, one(&managed.backend, output, &config, tokenizer.tokenizer(), identity, case, controlled, &progress, 1));
            try std.testing.expectEqual(@as(usize, 3), progress.windows);
            try std.testing.expectEqual(@as(usize, 1), progress.merges);
            try std.testing.expectEqual(baseline, captures.budget.live);
            progress = .{};
            {
                var retry = try one(&managed.backend, output, &config, tokenizer.tokenizer(), identity, case, controlled, &progress, 256);
                defer retry.deinit();
                try std.testing.expect(sameSample(captures.results[index].sample, retry.sample));
                try std.testing.expectEqualSlices(u8, &result.inference_fingerprint, &retry.inference_fingerprint);
            }
            try std.testing.expectEqual(baseline, captures.budget.live);
        }
    }
    try std.testing.expect(!captures.budget.denied);
    try pinnedFiles(a, directory, corpus.reference.model_files, lifetime);
    return captures;
}

fn expectFailed(expected: anyerror, attempted: anyerror!executor.Result) !void {
    if (attempted) |returned| {
        var result = returned;
        result.deinit();
        std.debug.print("expected {s}, received a complete learned-window result\n", .{@errorName(expected)});
        return error.TestUnexpectedResult;
    } else |err| try std.testing.expectEqual(expected, err);
}
fn knownName(items: anytype, name: []const u8) bool {
    for (items) |item| if (std.mem.eql(u8, item.name, name)) return true;
    return false;
}
fn prob(value: f32) !void {
    try std.testing.expect(std.math.isFinite(value) and value >= 0 and value <= 1);
}
fn spanValid(map: offsets.OffsetMap, source: pipeline.SourceSpan) !void {
    try std.testing.expect(source.byte_start < source.byte_end and source.byte_end <= map.entries[map.entries.len - 1].byte);
    try std.testing.expectEqual(.utf16_codeunits, source.unit);
    const converted = try map.convert(.{ .start = source.byte_start, .end = source.byte_end }, .utf16_codeunits);
    try std.testing.expectEqual(converted.start, source.start);
    try std.testing.expectEqual(converted.end, source.end);
}
fn valueValid(map: offsets.OffsetMap, text: []const u8, value: pipeline.Value) !void {
    try prob(value.confidence);
    const source = value.source orelse return error.ExpectedDocumentBackedLearnedValue;
    try spanValid(map, source);
    try std.testing.expectEqualStrings(text[source.byte_start..source.byte_end], value.text);
    for (value.attributes) |attribute| for (attribute.labels) |label| try prob(label.confidence);
}
fn late(value: pipeline.Value, second: usize) bool {
    return if (value.source) |source| source.byte_start >= second else false;
}
fn expectedValue(want: pipeline.ExpectedValue, actual: pipeline.Value, scores: bool) bool {
    if (!std.mem.eql(u8, want.text, actual.text)) return false;
    if ((want.source == null) != (actual.source == null)) return false;
    if (want.source) |source| if (actual.source.?.byte_start != source.start or actual.source.?.byte_end != source.end) return false;
    if (scores and !close(want.confidence, actual.confidence)) return false;
    if (want.attributes.len != actual.attributes.len) return false;
    for (want.attributes) |attribute| {
        const found = for (actual.attributes) |value| {
            if (std.mem.eql(u8, attribute.name, value.name)) break value;
        } else return false;
        if (!sameLabels(attribute.labels, found.labels, scores)) return false;
    }
    return true;
}
fn expectedRecord(want: anytype, actual: pipeline.Record, scores: bool) bool {
    if (want.fields.len != actual.fields.len) return false;
    for (want.fields) |field| {
        const found = for (actual.fields) |value| {
            if (std.mem.eql(u8, field.name, value.name)) break value;
        } else return false;
        if (field.values.len != found.values.len) return false;
        for (field.values, found.values) |value, got| if (!expectedValue(value, got, scores)) return false;
    }
    return true;
}
fn validateCase(a: Allocator, case: Case, result: executor.Result) !void {
    try std.testing.expectEqual(@as(usize, 3), result.window_count);
    try std.testing.expect(result.prompt_tokens > 0 and result.prompt_tokens <= 3 * 512);
    try std.testing.expect(result.attention_work_items > 0 and result.attention_work_items <= 256 * mib);
    try std.testing.expect(result.peak_evidence_bytes > 0 and result.peak_evidence_bytes <= 32 * mib);
    const metadata = result.sample.long_document orelse return error.MissingLongDocumentMetadata;
    try std.testing.expectEqual(@as(usize, 3), metadata.window_count);
    try std.testing.expectEqual(.occurrence, metadata.other_record_identity);
    var map = try offsets.OffsetMap.init(a, case.text, 4096);
    defer map.deinit();
    var has_late = false;
    for (result.sample.entities, 0..) |group, index| {
        for (result.sample.entities[0..index]) |previous| try std.testing.expect(!std.mem.eql(u8, group.name, previous.name));
        if (case.compiled.schema.joint_ie) |js| {
            try std.testing.expect(knownName(js.entities, group.name));
        } else try std.testing.expect(knownName(case.compiled.schema.entities, group.name));
        for (group.values, 0..) |value, value_index| {
            try valueValid(map, case.text, value);
            has_late = has_late or late(value, case.second_start);
            for (group.values[0..value_index]) |previous| try std.testing.expect(!sameSource(value.source, previous.source));
            for (value.attributes, 0..) |attribute, attribute_index| {
                try std.testing.expect(knownName(case.compiled.schema.entity_attributes, attribute.name));
                for (value.attributes[0..attribute_index]) |previous| try std.testing.expect(!std.mem.eql(u8, attribute.name, previous.name));
            }
        }
    }
    for (result.sample.classifications, 0..) |group, index| {
        for (result.sample.classifications[0..index]) |previous| try std.testing.expect(!std.mem.eql(u8, group.name, previous.name));
        const task = for (case.compiled.schema.classifications) |definition| {
            if (std.mem.eql(u8, definition.task.name, group.name)) break definition.task;
        } else return error.UnknownClassification;
        for (group.labels, 0..) |label, label_index| {
            for (group.labels[0..label_index]) |previous| try std.testing.expect(!std.mem.eql(u8, label.label, previous.label));
            try prob(label.confidence);
            var known = false;
            for (task.labels) |name| known = known or std.mem.eql(u8, name, label.label);
            try std.testing.expect(known);
        }
    }
    for (result.sample.relations) |edge| {
        if (case.compiled.schema.joint_ie) |js| {
            try std.testing.expect(knownName(js.relations, edge.name));
        } else try std.testing.expect(knownName(case.compiled.schema.relations, edge.name));
        try valueValid(map, case.text, edge.head);
        try valueValid(map, case.text, edge.tail);
        try prob(edge.confidence);
    }
    for (result.sample.structures, 0..) |group, index| {
        for (result.sample.structures[0..index]) |previous| try std.testing.expect(!std.mem.eql(u8, group.name, previous.name));
        const definition = for (case.compiled.schema.structures) |structure| {
            if (std.mem.eql(u8, structure.name, group.name)) break structure;
        } else return error.UnknownStructure;
        for (group.instances) |record| {
            try std.testing.expectEqual(definition.fields.len, record.fields.len);
            for (record.fields, 0..) |field, field_index| {
                try std.testing.expect(knownName(definition.fields, field.name));
                for (record.fields[0..field_index]) |previous| try std.testing.expect(!std.mem.eql(u8, field.name, previous.name));
            }
            if (record.confidence) |value| try prob(value);
            if (record.anchor) |source| try spanValid(map, source);
            if (record.occurrence) |occurrence| if (occurrence.seed_source) |source| try spanValid(map, source);
            for (record.fields) |field| for (field.values) |value| {
                try valueValid(map, case.text, value);
                has_late = has_late or late(value, case.second_start);
            };
        }
    }
    switch (case.spec.kind) {
        .attributes => {
            try std.testing.expect(has_late);
            try std.testing.expectEqual(@as(usize, 1), result.sample.entities.len);
            const values = result.sample.entities[0].values;
            try std.testing.expect(values.len >= 2);
            var known_first = false;
            for (values) |value| {
                try std.testing.expectEqual(@as(usize, 2), value.attributes.len);
                for (value.attributes) |attribute| if (std.mem.eql(u8, attribute.name, "sentiment")) try std.testing.expectEqual(@as(usize, 1), attribute.labels.len);
                known_first = known_first or expectedValue(case.expected.entities[0].values[0], value, true);
            }
            try std.testing.expect(known_first);
        },
        .natural, .latent => {
            try std.testing.expect(has_late);
            try std.testing.expectEqual(@as(usize, 1), result.sample.structures.len);
            const records = result.sample.structures[0].instances;
            try std.testing.expect(records.len >= 2);
            const diagnostic = result.sample.record_solver orelse return error.MissingRecordSolver;
            try std.testing.expect(!diagnostic.exhausted);
            if (case.spec.kind == .natural) {
                var known_first = false;
                for (records, 0..) |record, index| {
                    const anchor = record.anchor orelse return error.MissingNaturalAnchor;
                    try std.testing.expect(record.fields[0].values.len == 1);
                    try std.testing.expect(sameSource(anchor, record.fields[0].values[0].source));
                    for (records[0..index]) |previous| {
                        try std.testing.expect(!sameSource(anchor, previous.anchor));
                        for (record.fields[1].values) |value| for (previous.fields[1].values) |other| try std.testing.expect(!sameSource(value.source, other.source));
                    }
                    known_first = known_first or expectedRecord(case.expected.structures[0].instances[0], record, true);
                }
                try std.testing.expect(known_first);
            } else {
                // The pinned first full window has three distinct learned
                // instances displaying John/Apple at the same exact spans.
                // Preserve that multiplicity; a displayed-field set is unsafe.
                var maximum: usize = 0;
                for (case.expected.structures[0].instances) |wanted| {
                    var count: usize = 0;
                    for (records) |record| {
                        try std.testing.expect(record.anchor == null);
                        if (expectedRecord(wanted, record, false)) count += 1;
                    }
                    maximum = @max(maximum, count);
                }
                try std.testing.expect(maximum >= 3);
            }
        },
        .anchorless => {
            // Negative-output coverage only. The published original fixture is
            // empty at .5; this is not positive anchorless qualification.
            try std.testing.expectEqual(@as(usize, 0), case.expected.structures.len);
            try std.testing.expectEqual(@as(usize, 0), result.sample.structures.len);
        },
        .classification => {
            const tasks = case.compiled.schema.classifications;
            try std.testing.expectEqual(@as(usize, 2), result.sample.classifications.len);
            const selected = try a.alloc(classification.Selection, tasks.len);
            defer a.free(selected);
            const possible = try a.alloc(classification.Selection, tasks.len);
            defer a.free(possible);
            @memset(selected, 0);
            @memset(possible, 0);
            for (tasks, 0..) |task, index| {
                const found = for (result.sample.classifications) |value| {
                    if (std.mem.eql(u8, task.task.name, value.name)) break value;
                } else return error.MissingClassification;
                try std.testing.expectEqual(@as(usize, 1), found.labels.len);
                try std.testing.expectEqualStrings("delete", found.labels[0].label);
                try prob(found.labels[0].confidence);
                for (task.task.labels, 0..) |label, l| if (std.mem.eql(u8, label, found.labels[0].label)) {
                    selected[index] |= @as(classification.Selection, 1) << @as(std.math.Log2Int(classification.Selection), @intCast(l));
                };
            }
            try std.testing.expectEqual(.yes, try case.compiled.schema.classification_constraints.evaluate(.{ .selected = selected, .possible = possible }));
            const diagnostic = result.sample.classification_solver orelse return error.MissingClassificationSolver;
            try std.testing.expect(!diagnostic.exhausted and diagnostic.visited_nodes > 0);
        },
        .ordinary_relations => {
            try std.testing.expect(has_late and result.sample.relations.len > 0);
            for (result.sample.relations, 0..) |edge, index| {
                const head = try relation.semanticText(a, edge.head.text);
                defer a.free(head);
                const tail = try relation.semanticText(a, edge.tail.text);
                defer a.free(tail);
                for (result.sample.relations[0..index]) |previous| {
                    const ph = try relation.semanticText(a, previous.head.text);
                    defer a.free(ph);
                    const pt = try relation.semanticText(a, previous.tail.text);
                    defer a.free(pt);
                    try std.testing.expect(!std.mem.eql(u8, head, ph) or !std.mem.eql(u8, tail, pt));
                }
            }
        },
        .joint_relations => {
            try std.testing.expect(has_late and result.sample.relations.len >= 2);
            try validateJoint(a, case.compiled.schema.joint_ie.?, result.sample, case.second_start);
            const diagnostic = result.sample.joint_solver orelse return error.MissingJointSolver;
            try std.testing.expect(!diagnostic.exhausted and diagnostic.visited_nodes > 0);
        },
    }
}
fn validateJoint(a: Allocator, schema: schema_mod.JointSchema, sample: pipeline.Sample, second: usize) !void {
    var nodes = std.ArrayListUnmanaged(joint.Node).empty;
    defer nodes.deinit(a);
    for (sample.entities) |group| {
        const kind = for (schema.entities, 0..) |entity, index| {
            if (std.mem.eql(u8, entity.name, group.name)) break index;
        } else return error.UnknownJointEntity;
        for (group.values) |value| try nodes.append(a, .{ .entity_type = kind, .start = value.source.?.byte_start, .end = value.source.?.byte_end, .probability = value.confidence, .utility = 0 });
    }
    const edges = try a.alloc(joint.DecodedEdge, sample.relations.len);
    defer a.free(edges);
    var late_edge = false;
    for (sample.relations, edges, 0..) |edge, *out, index| {
        try std.testing.expect(!edge.derived);
        const relation_type = for (schema.relations, 0..) |definition, r| {
            if (std.mem.eql(u8, definition.name, edge.name)) break r;
        } else return error.UnknownJointRelation;
        const h = try nodeIndex(nodes.items, edge.head_entity_type orelse return error.MissingJointType, edge.head.source.?);
        const t = try nodeIndex(nodes.items, edge.tail_entity_type orelse return error.MissingJointType, edge.tail.source.?);
        out.* = .{ .relation_type = relation_type, .head = h, .tail = t, .probability = edge.confidence, .utility = 0, .source_index = index, .derived = false };
        late_edge = late_edge or late(edge.head, second) or late(edge.tail, second);
    }
    try std.testing.expect(late_edge);
    // Utility zero is validation-only: no new decoding, scoring or probability
    // claim. This checks the emitted graph's typed endpoints/global degree.
    try std.testing.expect(try joint.validateGlobal(a, schema, nodes.items, edges, .{}));
}
fn nodeIndex(nodes: []const joint.Node, kind: usize, source: pipeline.SourceSpan) !usize {
    for (nodes, 0..) |node, index| if (node.entity_type == kind and node.start == source.byte_start and node.end == source.byte_end) return index;
    return error.MissingJointEndpoint;
}

fn close(a: f32, b: f32) bool {
    return std.math.isFinite(a) and std.math.isFinite(b) and @abs(a - b) <= confidence_tolerance;
}
fn sameSource(a: ?pipeline.SourceSpan, b: ?pipeline.SourceSpan) bool {
    if ((a == null) != (b == null)) return false;
    return if (a) |value| std.meta.eql(value, b.?) else true;
}
fn sameLabelName(a: pipeline.Label, b: pipeline.Label) bool {
    return std.mem.eql(u8, a.label, b.label);
}
fn sameLabel(a: pipeline.Label, b: pipeline.Label) bool {
    return sameLabelName(a, b) and close(a.confidence, b.confidence);
}
fn sameLabels(a: []const pipeline.Label, b: []const pipeline.Label, scores: bool) bool {
    return if (scores) sameMultiset(pipeline.Label, a, b, sameLabel) else sameMultiset(pipeline.Label, a, b, sameLabelName);
}
fn sameValue(a: pipeline.Value, b: pipeline.Value) bool {
    if (!std.mem.eql(u8, a.text, b.text) or !sameSource(a.source, b.source) or a.derived != b.derived or !close(a.confidence, b.confidence) or a.attributes.len != b.attributes.len) return false;
    for (a.attributes) |attribute| {
        const found = for (b.attributes) |other| {
            if (std.mem.eql(u8, attribute.name, other.name)) break other;
        } else return false;
        if (attribute.multi_label != found.multi_label or !sameLabels(attribute.labels, found.labels, true)) return false;
    }
    return true;
}
fn sameRecord(a: pipeline.Record, b: pipeline.Record) bool {
    if (!sameSource(a.anchor, b.anchor) or a.fields.len != b.fields.len or (a.confidence == null) != (b.confidence == null)) return false;
    if (a.confidence) |value| if (!close(value, b.confidence.?)) return false;
    if ((a.occurrence == null) != (b.occurrence == null)) return false;
    if (a.occurrence) |identity| if (identity.seed_field != b.occurrence.?.seed_field or !sameSource(identity.seed_source, b.occurrence.?.seed_source)) return false;
    // Local learned slot ordinals are internal. Source seeds and observed
    // occurrence multiplicity are preserved by exact multiset matching.
    for (a.fields) |field| {
        const found = for (b.fields) |other| {
            if (std.mem.eql(u8, field.name, other.name)) break other;
        } else return false;
        if (field.dtype != found.dtype or !sameMultiset(pipeline.Value, field.values, found.values, sameValue)) return false;
    }
    return true;
}
fn sameRelation(a: pipeline.Relation, b: pipeline.Relation) bool {
    return std.mem.eql(u8, a.name, b.name) and a.derived == b.derived and a.head_entity_type == b.head_entity_type and a.tail_entity_type == b.tail_entity_type and a.schema_index == b.schema_index and close(a.confidence, b.confidence) and sameValue(a.head, b.head) and sameValue(a.tail, b.tail);
}
fn augment(comptime T: type, left: []const T, right: []const T, comptime equivalent: fn (T, T) bool, index: usize, matched: *[item_cap]?usize, visited: *[item_cap]bool) bool {
    for (right, 0..) |candidate, j| {
        if (visited[j] or !equivalent(left[index], candidate)) continue;
        visited[j] = true;
        if (matched[j] == null or augment(T, left, right, equivalent, matched[j].?, matched, visited)) {
            matched[j] = index;
            return true;
        }
    }
    return false;
}
fn sameMultiset(comptime T: type, left: []const T, right: []const T, comptime equivalent: fn (T, T) bool) bool {
    if (left.len != right.len or left.len > item_cap) return false;
    var matched: [item_cap]?usize = @splat(null);
    for (left, 0..) |_, index| {
        var visited: [item_cap]bool = @splat(false);
        if (!augment(T, left, right, equivalent, index, &matched, &visited)) return false;
    }
    return true;
}
fn sameDiagnostic(a: ?pipeline.Diagnostics, b: ?pipeline.Diagnostics) bool {
    if ((a == null) != (b == null)) return false;
    return if (a) |value| value.status == b.?.status and value.exhausted == b.?.exhausted else true;
}
fn sameSample(a: pipeline.Sample, b: pipeline.Sample) bool {
    if (a.entities.len != b.entities.len or a.classifications.len != b.classifications.len or a.structures.len != b.structures.len or !std.meta.eql(a.long_document, b.long_document)) return false;
    if (!sameDiagnostic(a.classification_solver, b.classification_solver) or !sameDiagnostic(a.record_solver, b.record_solver) or !sameDiagnostic(a.joint_solver, b.joint_solver)) return false;
    for (a.entities) |group| {
        const found = for (b.entities) |other| {
            if (std.mem.eql(u8, group.name, other.name)) break other;
        } else return false;
        if (group.dtype != found.dtype or !sameMultiset(pipeline.Value, group.values, found.values, sameValue)) return false;
    }
    for (a.classifications) |group| {
        const found = for (b.classifications) |other| {
            if (std.mem.eql(u8, group.name, other.name)) break other;
        } else return false;
        if (group.multi_label != found.multi_label or !sameLabels(group.labels, found.labels, true)) return false;
    }
    for (a.structures) |group| {
        const found = for (b.structures) |other| {
            if (std.mem.eql(u8, group.name, other.name)) break other;
        } else return false;
        if (!sameMultiset(pipeline.Record, group.instances, found.instances, sameRecord)) return false;
    }
    return sameMultiset(pipeline.Relation, a.relations, b.relations, sameRelation);
}

test "gliner boundary learned multi window comparator retains multiplicity and confidence bounds" {
    var first = pipeline.Value{ .text = "Ada", .confidence = 0.8, .source = .{ .start = 0, .end = 3, .unit = .utf16_codeunits, .byte_start = 0, .byte_end = 3 }, .token_span = null };
    var nearby = first;
    nearby.confidence += 4e-4;
    try std.testing.expect(sameMultiset(pipeline.Value, &.{ first, nearby }, &.{ nearby, first }, sameValue));
    try std.testing.expect(!sameMultiset(pipeline.Value, &.{ first, nearby }, &.{first}, sameValue));
    nearby.confidence += 2e-4;
    try std.testing.expect(!sameValue(first, nearby));
    var left_high = first;
    left_high.confidence = 0.8008;
    var right_high = first;
    right_high.confidence = 0.8004;
    var right_low = first;
    right_low.confidence = 0.7996;
    // A greedy first match fails; the augmenting path preserves multiplicity.
    try std.testing.expect(sameMultiset(pipeline.Value, &.{ first, left_high }, &.{ right_high, right_low }, sameValue));
    first.source.?.byte_start = 1;
    try std.testing.expect(!sameValue(first, nearby));
}

test "gliner boundary learned multi window fixed source profiles plan original endpoints with Unicode overlap" {
    const corpus = try Corpus.create(std.testing.allocator);
    defer corpus.destroy();
    for (corpus.cases) |case| {
        var poison = Poison{ .backing = std.testing.allocator };
        var arena = std.heap.ArenaAllocator.init(poison.allocator());
        defer arena.deinit();
        var request = try wire.parseJson(arena.allocator(), try requestJson(arena.allocator(), case), .{});
        defer request.deinit();
        try std.testing.expectEqualSlices(u8, &case.compiled.fingerprint, &request.items[0].compiled.fingerprint);
    }
}

test "gliner boundary learned multi window native pinned small task ownership and retry" {
    const directory = platform.env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    const corpus = try Corpus.create(std.testing.allocator);
    defer corpus.destroy();
    const captures = try runBackend(false, directory, corpus);
    defer captures.destroy();
    for (corpus.cases, captures.results) |case, result| try validateCase(std.testing.allocator, case, result);
}

test "gliner boundary learned multi window Metal pinned small semantic task parity and retry" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const directory = platform.env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    const corpus = try Corpus.create(std.testing.allocator);
    defer corpus.destroy();
    // runBackend closes its physical model owner before returning. Only owned
    // scalar outputs remain while the next backend opens the same five pins.
    const native = try runBackend(false, directory, corpus);
    defer native.destroy();
    const metal = try runBackend(true, directory, corpus);
    defer metal.destroy();
    for (corpus.cases, native.results, metal.results) |case, cpu, gpu| {
        errdefer std.debug.print("learned multi-window CPU/Metal mismatch: {s}\n", .{case.spec.id});
        try validateCase(std.testing.allocator, case, cpu);
        try validateCase(std.testing.allocator, case, gpu);
        try std.testing.expectEqual(cpu.prompt_tokens, gpu.prompt_tokens);
        try std.testing.expectEqual(cpu.attention_work_items, gpu.attention_work_items);
        try std.testing.expect(std.meta.eql(cpu.descriptor, gpu.descriptor));
        try std.testing.expect(sameSample(cpu.sample, gpu.sample));
    }
}
