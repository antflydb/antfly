// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Ordinary relations use document-wide semantic deduplication after exact
//! rebasing. JointIE edges belong to the separate globally constrained graph.
//! Inputs are threshold-admitted local pairs before semantic deduplication.
const std = @import("std");
const Allocator = std.mem.Allocator;
const document_mod = @import("gliner_boundary_long_document.zig");
const pipeline = @import("gliner_boundary_pipeline.zig");
const schema_mod = @import("extraction_schema.zig");
const decode = @import("gliner_boundary_decode.zig");
const relations = @import("gliner_boundary_relations.zig");
const BoundedAllocator = @import("../runtime/bounded_allocator.zig").BoundedAllocator;

pub const Window = struct { identity: document_mod.Identity, relations: []const pipeline.Relation };
pub const Options = struct {
    merge: document_mod.MergeOptions = .{},
    output_unit: decode.OffsetUnit = .utf8_bytes,
    max_edges_per_type: usize = 4096,
};
const Owner = struct {
    backing: Allocator,
    budget: BoundedAllocator,
    arena: std.heap.ArenaAllocator,
    fn init(backing: Allocator, limit: usize) !*Owner {
        if (limit == 0 or limit > 1024 * 1024 * 1024) return error.InvalidLongDocumentLimits;
        const self = try backing.create(Owner);
        self.* = .{ .backing = backing, .budget = .{ .backing = backing, .limit = limit }, .arena = undefined };
        self.arena = std.heap.ArenaAllocator.init(self.budget.allocator());
        return self;
    }
    fn deinit(self: *Owner) void {
        const backing = self.backing;
        self.arena.deinit();
        std.debug.assert(self.budget.live == 0);
        backing.destroy(self);
    }
};
pub const Result = struct {
    owner: *Owner,
    relations: []const pipeline.Relation,
    comparisons: usize,
    pub fn deinit(self: *Result) void {
        self.owner.deinit();
        self.* = undefined;
    }
};
const Work = struct {
    options: Options,
    steps: usize = 0,
    inputs: usize = 0,
    text_bytes: usize = 0,
    fn tick(self: *Work) !void {
        if (self.steps >= self.options.merge.max_work) return error.LongDocumentWorkLimitExceeded;
        self.steps += 1;
        if (self.steps % 64 == 1) if (self.options.merge.control) |control| try control.check();
    }
    fn text(self: *Work, count: usize) !void {
        if (count > self.options.merge.max_text_bytes -| self.text_bytes) return error.LongDocumentTextLimitExceeded;
        self.text_bytes += count;
        try self.tick();
    }
};

fn rebaseMention(document: document_mod.Plan, window: usize, value: pipeline.Value, work: *Work) !relations.Mention {
    try work.text(value.text.len);
    if (value.derived or value.attributes.len != 0 or !std.math.isFinite(value.confidence) or value.confidence < 0 or value.confidence > 1)
        return error.InvalidLongDocumentRelation;
    const source = try document.rebaseSource(window, value.source orelse return error.InvalidLongDocumentRelation, .unicode_codepoints);
    // Ordinary endpoints are copied source surfaces. Reject a payload whose
    // claimed source and value disagree instead of reconstructing its text.
    if (!std.mem.eql(u8, value.text, document.text[source.byte_start..source.byte_end])) return error.InvalidLongDocumentRelation;
    return .{ .text = value.text, .start = source.start, .end = source.end };
}
fn valueFromMention(a: Allocator, document: document_mod.Plan, mention: relations.Mention, probability: f32, unit: decode.OffsetUnit) !pipeline.Value {
    const bytes = try document.offsets.toBytes(.{ .start = mention.start, .end = mention.end }, .unicode_codepoints);
    const requested = try document.offsets.convert(bytes, unit);
    return .{ .text = try a.dupe(u8, mention.text), .confidence = probability, .source = .{ .start = requested.start, .end = requested.end, .unit = unit, .byte_start = bytes.start, .byte_end = bytes.end }, .token_span = null };
}

pub fn merge(allocator: Allocator, document: document_mod.Plan, compiled: *const schema_mod.CompiledSchema, windows: []const Window, options: Options) !Result {
    if (options.merge.control) |control| try control.check();
    if (!std.mem.eql(u8, &compiled.fingerprint, &document.schema_fingerprint)) return error.LongDocumentIdentityMismatch;
    if (windows.len != document.windows.len) return error.IncompleteLongDocumentWindows;
    if (options.max_edges_per_type == 0 or options.max_edges_per_type > 65536 or
        options.merge.max_input_candidates == 0 or options.merge.max_input_candidates > 1048576 or
        options.merge.max_output_candidates == 0 or options.merge.max_output_candidates > 65536 or
        options.merge.max_work == 0 or options.merge.max_work > 1000000000 or
        options.merge.max_text_bytes == 0 or options.merge.max_text_bytes > 16 * 1024 * 1024)
        return error.InvalidLongDocumentLimits;
    const owner = try Owner.init(allocator, options.merge.max_memory_bytes);
    errdefer owner.deinit();
    const a = owner.arena.allocator();
    var work = Work{ .options = options };
    const order = try a.alloc(usize, windows.len);
    @memset(order, std.math.maxInt(usize));
    for (windows, 0..) |window, index| {
        try work.tick();
        try document.validateIdentity(window.identity);
        if (order[window.identity.window_index] != std.math.maxInt(usize)) return error.DuplicateLongDocumentWindow;
        order[window.identity.window_index] = index;
        if (window.relations.len > options.merge.max_input_candidates -| work.inputs) return error.LongDocumentCandidateLimitExceeded;
        work.inputs += window.relations.len;
    }
    const groups = try a.alloc(std.ArrayListUnmanaged(relations.Edge), compiled.schema.relations.len);
    @memset(groups, .empty);
    for (order, 0..) |position, window_index| for (windows[position].relations) |edge| {
        try work.tick();
        if (edge.derived or edge.head_entity_type != null or edge.tail_entity_type != null or
            !std.math.isFinite(edge.confidence) or edge.confidence < 0 or edge.confidence > 1)
            return error.InvalidLongDocumentRelation;
        var kind: ?usize = edge.schema_index;
        if (kind) |index| {
            if (index >= compiled.schema.relations.len or !std.mem.eql(u8, compiled.schema.relations[index].name, edge.name)) return error.InvalidLongDocumentRelation;
        } else {
            for (compiled.schema.relations, 0..) |spec, index| {
                try work.tick();
                if (std.mem.eql(u8, spec.name, edge.name)) {
                    if (kind != null) return error.AmbiguousLongDocumentRelationRoute;
                    kind = index;
                }
            }
        }
        const index = kind orelse return error.InvalidLongDocumentRelation;
        if (groups[index].items.len >= options.max_edges_per_type) return error.LongDocumentCandidateLimitExceeded;
        const head = try rebaseMention(document, window_index, edge.head, &work);
        const tail = try rebaseMention(document, window_index, edge.tail, &work);
        try groups[index].append(a, .{ .head = head, .tail = tail, .probability = edge.confidence });
    };
    var output = std.ArrayListUnmanaged(pipeline.Relation).empty;
    for (groups, compiled.schema.relations, 0..) |group, spec, schema_index| {
        try work.tick();
        // Reclaim a type's temporary normalized strings before the next one.
        // The request-wide work ceiling is carried across all relation types.
        var dedup = try relations.deduplicate(owner.budget.allocator(), group.items, .{
            .max_edges = options.max_edges_per_type,
            .max_text_bytes = options.merge.max_text_bytes,
            .max_comparisons = options.merge.max_work - work.steps,
            .control = options.merge.control,
        });
        defer dedup.deinit();
        work.steps += dedup.comparisons;
        if (dedup.edges.len > options.merge.max_output_candidates -| output.items.len) return error.LongDocumentCandidateLimitExceeded;
        for (dedup.edges) |edge| {
            try work.tick();
            try output.append(a, .{ .name = try a.dupe(u8, spec.name), .head = try valueFromMention(a, document, edge.head, edge.probability, options.output_unit), .tail = try valueFromMention(a, document, edge.tail, edge.probability, options.output_unit), .confidence = edge.probability, .schema_index = schema_index });
        }
    }
    if (options.merge.control) |control| try control.check();
    return .{ .owner = owner, .relations = try output.toOwnedSlice(a), .comparisons = work.steps };
}

fn testValue(document: document_mod.Plan, window: usize, start: usize, end: usize, probability: f32) !pipeline.Value {
    const base = try document.offsets.convert(document.windows[window].bytes, .utf16_codeunits);
    const units = try document.offsets.convert(.{ .start = start, .end = end }, .utf16_codeunits);
    return .{ .text = document.text[start..end], .confidence = probability, .source = .{
        .start = units.start - base.start,
        .end = units.end - base.start,
        .unit = .utf16_codeunits,
        .byte_start = start - document.windows[window].bytes.start,
        .byte_end = end - document.windows[window].bytes.start,
    }, .token_span = null };
}

test "gliner boundary long relations rebase Unicode before global repeated mention selection" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, "{\"relations\":[{\"type\":\"works\"}]}", .{});
    defer compiled.deinit();
    var document = try document_mod.plan(a, "İ A Acme x A Acme y Z!", compiled.fingerprint, .{
        .mode = .windowed,
        .max_window_body_words = 4,
        .overlap_words = 1,
        .inference_fingerprint = [_]u8{9} ** 32,
    });
    defer document.deinit();
    const windows = try a.alloc(Window, document.windows.len);
    defer a.free(windows);
    var first = [_]pipeline.Relation{undefined};
    var second = [_]pipeline.Relation{undefined};
    var first_found = false;
    var second_found = false;
    for (document.windows, 0..) |window, index| {
        windows[index] = .{ .identity = try document.identity(index), .relations = &.{} };
        if (!first_found and window.bytes.start <= 3 and window.bytes.end >= 9) {
            first[0] = .{ .name = "works", .head = try testValue(document, index, 3, 4, 0.8), .tail = try testValue(document, index, 5, 9, 0.8), .confidence = 0.8 };
            windows[index].relations = &first;
            first_found = true;
        }
        if (!second_found and window.bytes.start <= 12 and window.bytes.end >= 18) {
            second[0] = .{ .name = "works", .head = try testValue(document, index, 12, 13, 0.9), .tail = try testValue(document, index, 14, 18, 0.9), .confidence = 0.9 };
            windows[index].relations = &second;
            second_found = true;
        }
    }
    try std.testing.expect(first_found and second_found);
    std.mem.reverse(Window, windows);
    var result = try merge(a, document, &compiled, windows, .{ .output_unit = .utf16_codeunits });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.relations.len);
    try std.testing.expectEqualStrings("A", result.relations[0].head.text);
    try std.testing.expectEqual(@as(usize, 12), result.relations[0].head.source.?.byte_start);
    try std.testing.expectEqual(@as(usize, 11), result.relations[0].head.source.?.start);
    try std.testing.expectEqual(@as(f32, 0.9), result.relations[0].confidence);
    try std.testing.expectError(error.IncompleteLongDocumentWindows, merge(a, document, &compiled, windows[0..1], .{}));
    try std.testing.expectError(error.LongDocumentWorkLimitExceeded, merge(a, document, &compiled, windows, .{ .merge = .{ .max_work = 1 } }));
    const Check = struct {
        fn run(allocator: Allocator, doc: document_mod.Plan, schema: *const schema_mod.CompiledSchema, inputs: []const Window) !void {
            var output = try merge(allocator, doc, schema, inputs, .{});
            defer output.deinit();
            try std.testing.expectEqual(@as(usize, 1), output.relations.len);
        }
        fn cancel(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{ document, &compiled, windows });
    try std.testing.expectError(error.Cancelled, merge(a, document, &compiled, windows, .{ .merge = .{ .control = .{ .check_fn = Check.cancel } } }));
    const original = second[0];
    second[0].head.text = "B";
    try std.testing.expectError(error.InvalidLongDocumentRelation, merge(a, document, &compiled, windows, .{}));
    second[0] = original;
    second[0].head_entity_type = 0;
    try std.testing.expectError(error.InvalidLongDocumentRelation, merge(a, document, &compiled, windows, .{}));
    second[0] = original;
    windows[0].identity = windows[1].identity;
    try std.testing.expectError(error.DuplicateLongDocumentWindow, merge(a, document, &compiled, windows, .{}));
}
