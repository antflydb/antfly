// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! GLiNER boundary inference preprocessing, pinned to Fastino commit
//! 3c913c7369301133d3b7699252074c4303ada50e. Schema fragments are encoded
//! independently without model wrappers. Routing follows structural slots,
//! never a scan of IDs that could mistake a description for a query marker.
const std = @import("std");
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const ir = @import("extraction_schema.zig");
const unicode = @import("../finetune/gliner2_unicode_tables.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

pub const unicode_version = unicode.unicode_version;
pub const WordSplitter = enum { whitespace, char };
pub const Item = struct { text: []const u8, schema: *const ir.CompiledSchema };
pub const Options = struct {
    max_batch_items: usize = 64,
    max_text_bytes: usize = 1024 * 1024,
    max_text_words: usize = 4096,
    max_total_words: usize = 8192,
    max_sequence_tokens: usize = 16384,
    max_batch_tokens: usize = 65536,
    max_queries: usize = 512,
    max_classification_labels: usize = 512,
    max_groups: usize = 64,
    max_fragment_bytes: usize = 65536,
    word_splitter: WordSplitter = .whitespace,
    control: ?Control = null,

    fn check(self: Options) !void {
        if (self.control) |control| try control.check();
    }
};
pub const ByteRange = struct { start: usize, end: usize };
pub const WordRangeOptions = struct {
    word_splitter: WordSplitter = .whitespace,
    max_text_bytes: usize = 1024 * 1024,
    max_words: usize = 131072,
    control: ?Control = null,
};

/// Original-text boundaries for explicit document windowing. This shares the
/// processor's exact splitter; it adds no enum prefix, terminal punctuation,
/// lowercasing, or model-token wrappers. Caller owns the returned ranges.
pub fn sourceWordRanges(allocator: Allocator, text: []const u8, options: WordRangeOptions) ![]ByteRange {
    if (options.control) |control| try control.check();
    if (text.len > options.max_text_bytes) return error.BoundaryTextLimitExceeded;
    if (!std.unicode.utf8ValidateSlice(text)) return error.InvalidUtf8;
    var result = std.ArrayListUnmanaged(ByteRange).empty;
    errdefer result.deinit(allocator);
    var position: usize = 0;
    while (position < text.len) {
        if (options.control) |control| try control.check();
        if (unicode.isWhitespace(codepoint(text, position))) {
            position = nextCodepoint(text, position);
            continue;
        }
        if (result.items.len >= options.max_words) return error.BoundaryTextLimitExceeded;
        const start = position;
        position = wordEnd(text, position, options.word_splitter);
        try result.append(allocator, .{ .start = start, .end = position });
    }
    return result.toOwnedSlice(allocator);
}

/// Whether the processor's synthetic final period consumes another word.
/// A URL suffix, and an ASCII run under the char splitter, may absorb it.
pub fn terminalWordAdded(allocator: Allocator, text: []const u8, splitter: WordSplitter) !bool {
    if (!std.unicode.utf8ValidateSlice(text)) return error.InvalidUtf8;
    if (text.len == 0) return true;
    if (std.mem.indexOfScalar(u8, ".!?", text[text.len - 1]) != null) return false;
    var position: usize = 0;
    var last: ByteRange = .{ .start = 0, .end = 0 };
    while (position < text.len) {
        if (unicode.isWhitespace(codepoint(text, position))) {
            position = nextCodepoint(text, position);
            continue;
        }
        last.start = position;
        position = wordEnd(text, position, splitter);
        last.end = position;
    }
    if (last.end != text.len) return true;
    const suffix = try std.mem.concat(allocator, u8, &.{ text[last.start..], "." });
    defer allocator.free(suffix);
    return wordEnd(suffix, 0, splitter) < suffix.len;
}
pub const Word = struct {
    /// Lowercased body token, or verbatim synthetic enum-prefix fragment.
    text: []const u8,
    /// Half-open UTF-8 bytes into the immutable caller document, never into
    /// lowercased text. Null denotes wholly synthetic prefix/punctuation.
    source: ?ByteRange,
    /// A terminal period can extend a URL token. Its source range excludes
    /// the period even though its encoded form includes the upstream suffix.
    has_synthetic_suffix: bool = false,
    input_start: usize,
    input_end: usize,
};
pub const GroupKind = enum { structure, entities, relation, classification };
pub const QueryKind = enum { field, entity, attribute, relation_head, relation_tail };
pub const Group = struct {
    kind: GroupKind,
    /// Index into the corresponding canonical schema family.
    schema_index: usize,
    name: []const u8,
    /// Upstream QueryLayout strips descriptions but keeps a parent prompt.
    /// Decoder output keys should use name, not this model-facing string.
    model_name: []const u8,
    fragments: []const []const u8,
    parent_marker: usize,
    child_markers: []const usize,
};
pub const Query = struct {
    kind: QueryKind,
    group_index: usize,
    /// Structure/relation/entity index, or attribute-group index.
    schema_index: usize,
    /// Position in the complete group, including hidden attribute queries.
    role_index: usize,
    /// Position in the original field/label/attribute-group declaration.
    label_index: usize,
    name: []const u8,
    marker_index: usize,
};
pub const ClassificationLabel = struct {
    group_index: usize,
    schema_index: usize,
    label_index: usize,
    name: []const u8,
    marker_index: usize,
};
pub const EnumChoice = struct {
    structure_index: usize,
    field_index: usize,
    choice_index: usize,
    /// Half-open word coordinates in the synthetic prefix, scored by the
    /// explicit-span scorer. A multiword choice remains one processor word.
    start: usize,
    end: usize,
    /// Upstream _find_choice_idx uses the first case-insensitive matching
    /// prefix token, including choices repeated in another field.
    score_start: usize,
};
pub const Sample = struct {
    original_text: []const u8,
    schema_fingerprint: [32]u8,
    input_ids: []const i64,
    words: []const Word,
    groups: []const Group,
    queries: []const Query,
    classification_labels: []const ClassificationLabel,
    enum_choices: []const EnumChoice,
    prefix_word_count: usize,
    body_word_count: usize,
    terminal_period_added: bool,
    is_joint_ie: bool,
};
pub const PreparedBatch = struct {
    arena: std.heap.ArenaAllocator,
    samples: []const Sample,
    sequence_length: usize,
    word_width: usize,
    query_width: usize,
    classification_width: usize,
    group_width: usize,
    input_ids: []i64,
    attention_mask: []i64,
    text_word_indices: []i64,
    text_word_mask: []bool,
    query_marker_indices: []i64,
    query_marker_mask: []bool,
    query_group_index: []i64,
    cls_marker_indices: []i64,
    cls_marker_mask: []bool,
    cls_group_index: []i64,
    parent_marker_indices: []i64,
    parent_marker_mask: []bool,

    pub fn deinit(self: *PreparedBatch) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

const Marker = enum { p, c, e, r, l, sep_struct, sep_text, description, example, output };
const marker_names = [_][]const u8{ "[P]", "[C]", "[E]", "[R]", "[L]", "[SEP_STRUCT]", "[SEP_TEXT]", "[DESCRIPTION]", "[EXAMPLE]", "[OUTPUT]" };
const Markers = struct {
    ids: [marker_names.len]i32,
    fn resolve(allocator: Allocator, tok: Tokenizer) !Markers {
        const special_ids = try tok.allSpecialTokenIds(allocator);
        defer allocator.free(special_ids);
        var result: Markers = undefined;
        for (marker_names, 0..) |name, i| {
            const ids = try tok.encode(allocator, name);
            defer allocator.free(ids);
            if (ids.len != 1 or ids[0] < 0 or ids[0] == tok.specialTokens().unk_id or
                @as(usize, @intCast(ids[0])) >= tok.vocabSize()) return error.InvalidGlinerBoundaryTokenizer;
            var declared = false;
            for (special_ids) |special_id| if (special_id == @as(u32, @intCast(ids[0]))) {
                declared = true;
                break;
            };
            if (!declared) return error.MissingGlinerBoundaryMarker;
            for (result.ids[0..i]) |other| if (other == ids[0]) return error.InvalidGlinerBoundaryTokenizer;
            result.ids[i] = ids[0];
        }
        return result;
    }
    fn id(self: Markers, marker: Marker) i32 {
        return self.ids[@intFromEnum(marker)];
    }
};

/// Prepares the complete batch atomically. Every output byte belongs to the
/// returned arena. The tokenizer owner must provide the artifact's exact
/// normalizer/subword semantics; this function deliberately uses encode(),
/// which adds no BOS/CLS/SEP and performs no length truncation.
pub fn prepare(allocator: Allocator, tokenizer: Tokenizer, items: []const Item, options: Options) !PreparedBatch {
    if (items.len == 0 or items.len > options.max_batch_items or options.max_batch_items > 1024 or
        options.max_text_bytes == 0 or options.max_text_words == 0 or options.max_total_words == 0 or
        options.max_sequence_tokens == 0 or options.max_sequence_tokens > std.math.maxInt(i32) or
        options.max_batch_tokens == 0 or options.max_queries == 0 or options.max_classification_labels == 0 or
        options.max_groups == 0 or options.max_fragment_bytes == 0) return error.InvalidBoundaryProcessorOptions;
    try options.check();
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const alloc = arena.allocator();
    const markers = try Markers.resolve(alloc, tokenizer);
    const samples = try alloc.alloc(Sample, items.len);
    var sequence_length: usize = 0;
    var word_width: usize = 0;
    var query_width: usize = 0;
    var classification_width: usize = 0;
    var group_width: usize = 0;
    var actual_tokens: usize = 0;
    for (items, samples) |item, *sample| {
        try options.check();
        var builder = Builder{ .allocator = alloc, .tokenizer = tokenizer, .markers = markers, .options = options };
        sample.* = try builder.build(item);
        actual_tokens = std.math.add(usize, actual_tokens, sample.input_ids.len) catch return error.BoundaryBatchLimitExceeded;
        if (actual_tokens > options.max_batch_tokens) return error.BoundaryBatchLimitExceeded;
        sequence_length = @max(sequence_length, sample.input_ids.len);
        word_width = @max(word_width, sample.words.len);
        query_width = @max(query_width, sample.queries.len);
        classification_width = @max(classification_width, sample.classification_labels.len);
        group_width = @max(group_width, sample.groups.len);
    }
    const padded_tokens = try product(items.len, sequence_length);
    if (padded_tokens > options.max_batch_tokens) return error.BoundaryBatchLimitExceeded;
    var batch = PreparedBatch{
        .arena = arena,
        .samples = samples,
        .sequence_length = sequence_length,
        .word_width = word_width,
        .query_width = query_width,
        .classification_width = classification_width,
        .group_width = group_width,
        // Upstream _pad_batch uses zeros, independently of pad_token_id.
        .input_ids = try zeros(i64, alloc, padded_tokens),
        .attention_mask = try zeros(i64, alloc, padded_tokens),
        .text_word_indices = try zeros(i64, alloc, try product(items.len, word_width)),
        .text_word_mask = try zeros(bool, alloc, try product(items.len, word_width)),
        .query_marker_indices = try zeros(i64, alloc, try product(items.len, query_width)),
        .query_marker_mask = try zeros(bool, alloc, try product(items.len, query_width)),
        .query_group_index = try zeros(i64, alloc, try product(items.len, query_width)),
        .cls_marker_indices = try zeros(i64, alloc, try product(items.len, classification_width)),
        .cls_marker_mask = try zeros(bool, alloc, try product(items.len, classification_width)),
        .cls_group_index = try zeros(i64, alloc, try product(items.len, classification_width)),
        .parent_marker_indices = try zeros(i64, alloc, try product(items.len, group_width)),
        .parent_marker_mask = try zeros(bool, alloc, try product(items.len, group_width)),
    };
    for (samples, 0..) |sample, row| {
        try options.check();
        @memcpy(batch.input_ids[row * sequence_length ..][0..sample.input_ids.len], sample.input_ids);
        @memset(batch.attention_mask[row * sequence_length ..][0..sample.input_ids.len], 1);
        for (sample.words, 0..) |word, i| {
            batch.text_word_indices[row * word_width + i] = @intCast(word.input_start);
            batch.text_word_mask[row * word_width + i] = true;
        }
        for (sample.queries, 0..) |query, i| {
            batch.query_marker_indices[row * query_width + i] = @intCast(query.marker_index);
            batch.query_marker_mask[row * query_width + i] = true;
            batch.query_group_index[row * query_width + i] = @intCast(query.group_index);
        }
        for (sample.classification_labels, 0..) |label, i| {
            batch.cls_marker_indices[row * classification_width + i] = @intCast(label.marker_index);
            batch.cls_marker_mask[row * classification_width + i] = true;
            batch.cls_group_index[row * classification_width + i] = @intCast(label.group_index);
        }
        for (sample.groups, 0..) |group, i| {
            batch.parent_marker_indices[row * group_width + i] = @intCast(group.parent_marker);
            batch.parent_marker_mask[row * group_width + i] = true;
        }
    }
    try options.check();
    // All allocations above used arena through alloc; transfer its latest state.
    batch.arena = arena;
    return batch;
}

const FieldPrompt = struct {
    name: []const u8,
    description: ?[]const u8 = null,
    kind: QueryKind = .field,
    schema_index: usize,
    role_index: usize,
};
const Builder = struct {
    allocator: Allocator,
    tokenizer: Tokenizer,
    markers: Markers,
    options: Options,
    ids: std.ArrayListUnmanaged(i64) = .empty,
    words: std.ArrayListUnmanaged(Word) = .empty,
    groups: std.ArrayListUnmanaged(Group) = .empty,
    queries: std.ArrayListUnmanaged(Query) = .empty,
    classifications: std.ArrayListUnmanaged(ClassificationLabel) = .empty,
    enum_choices: std.ArrayListUnmanaged(EnumChoice) = .empty,

    fn build(self: *Builder, item: Item) !Sample {
        if (item.text.len > self.options.max_text_bytes) return error.BoundaryTextLimitExceeded;
        if (!std.unicode.utf8ValidateSlice(item.text)) return error.InvalidUtf8;
        const schema = item.schema.schema;
        const original_text = try self.allocator.dupe(u8, item.text);
        if (schema.joint_ie) |joint| {
            const fields = try self.allocator.alloc(FieldPrompt, joint.entities.len);
            for (joint.entities, fields, 0..) |entity, *field, i| field.* = .{
                .name = entity.name,
                .description = entity.description,
                .kind = .entity,
                .schema_index = i,
                .role_index = i,
            };
            try self.addGroup(.entities, 0, "entities", .e, fields, null, &.{});
            for (joint.relations, 0..) |relation, i| {
                // Pinned JointIE compiler omits relation_descriptions from its
                // lowered model schema. Keep this distinct from ordinary IE.
                try self.addRelation(i, relation.name, null);
            }
        } else {
            for (schema.structures, 0..) |structure, i| {
                const fields = try self.allocator.alloc(FieldPrompt, structure.fields.len);
                for (structure.fields, fields, 0..) |field, *out, j| out.* = .{
                    .name = field.name,
                    .description = field.description,
                    .schema_index = i,
                    .role_index = j,
                };
                try self.addGroup(.structure, i, structure.name, .c, fields, null, &.{});
            }
            if (schema.entities.len > 0) try self.addEntities(schema);
            for (schema.relations, 0..) |relation, i| try self.addRelation(i, relation.name, relation.description);
            for (schema.classifications, 0..) |classification, i| {
                if (classification.hypothesis_template != null) return error.UnsupportedBoundaryHypothesisTemplate;
                const fields = try self.allocator.alloc(FieldPrompt, classification.task.labels.len);
                for (classification.task.labels, classification.label_definitions, fields, 0..) |label, definition, *field, j| field.* = .{
                    .name = label,
                    .description = definition.description,
                    .schema_index = i,
                    .role_index = j,
                };
                try self.addGroup(.classification, i, classification.task.name, .l, fields, classification.prompt, classification.examples);
            }
        }
        if (self.groups.items.len == 0) return error.EmptyExtractionSchema;
        try self.appendMarker(.sep_text);
        try self.addEnumPrefix(schema.structures);
        const prefix_count = self.words.items.len;
        const terminal_period = original_text.len == 0 or (original_text[original_text.len - 1] != '.' and original_text[original_text.len - 1] != '!' and original_text[original_text.len - 1] != '?');
        const effective_text = if (terminal_period) try std.mem.concat(self.allocator, u8, &.{ original_text, "." }) else original_text;
        var pos: usize = 0;
        while (pos < effective_text.len) {
            try self.options.check();
            if (unicode.isWhitespace(codepoint(effective_text, pos))) {
                pos = nextCodepoint(effective_text, pos);
                continue;
            }
            if (self.words.items.len - prefix_count >= self.options.max_text_words) return error.BoundaryTextLimitExceeded;
            const start = pos;
            pos = wordEnd(effective_text, pos, self.options.word_splitter);
            const lowered = try lower(self.allocator, effective_text[start..pos]);
            const source: ?ByteRange = if (start < original_text.len) .{ .start = start, .end = @min(pos, original_text.len) } else null;
            try self.appendWord(lowered, source, pos > original_text.len);
        }
        const body_count = self.words.items.len - prefix_count;
        return .{
            .original_text = original_text,
            .schema_fingerprint = item.schema.fingerprint,
            .input_ids = try self.ids.toOwnedSlice(self.allocator),
            .words = try self.words.toOwnedSlice(self.allocator),
            .groups = try self.groups.toOwnedSlice(self.allocator),
            .queries = try self.queries.toOwnedSlice(self.allocator),
            .classification_labels = try self.classifications.toOwnedSlice(self.allocator),
            .enum_choices = try self.enum_choices.toOwnedSlice(self.allocator),
            .prefix_word_count = prefix_count,
            .body_word_count = body_count,
            .terminal_period_added = terminal_period,
            .is_joint_ie = schema.joint_ie != null,
        };
    }

    fn addEntities(self: *Builder, schema: ir.Schema) !void {
        var count = schema.entities.len;
        for (schema.entity_attributes) |group| count = try std.math.add(usize, count, group.labels.len);
        if (count > self.options.max_queries) return error.BoundaryQueryLimitExceeded;
        const fields = try self.allocator.alloc(FieldPrompt, count);
        for (schema.entities, fields[0..schema.entities.len], 0..) |entity, *field, i| field.* = .{
            .name = entity.name,
            .description = entity.description,
            .kind = .entity,
            .schema_index = i,
            .role_index = i,
        };
        var cursor = schema.entities.len;
        for (schema.entity_attributes, 0..) |group, i| for (group.prompt_labels, 0..) |label, j| {
            fields[cursor] = .{ .name = label, .kind = .attribute, .schema_index = i, .role_index = j };
            cursor += 1;
        };
        const Sort = struct {
            fn less(_: void, a: FieldPrompt, b: FieldPrompt) bool {
                return std.mem.order(u8, a.name, b.name) == .lt;
            }
        };
        std.mem.sort(FieldPrompt, fields[schema.entities.len..], {}, Sort.less);
        try self.addGroup(.entities, 0, "entities", .e, fields, null, &.{});
    }

    fn addRelation(self: *Builder, index: usize, name: []const u8, description: ?[]const u8) !void {
        const fields = [_]FieldPrompt{
            .{ .name = "head", .kind = .relation_head, .schema_index = index, .role_index = 0 },
            .{ .name = "tail", .kind = .relation_tail, .schema_index = index, .role_index = 1 },
        };
        try self.addGroup(.relation, index, name, .r, &fields, description, &.{});
    }

    fn addGroup(self: *Builder, kind: GroupKind, schema_index: usize, name: []const u8, marker: Marker, fields: []const FieldPrompt, prompt: ?[]const u8, examples: []const ir.Example) !void {
        if (self.groups.items.len >= self.options.max_groups) return error.BoundaryGroupLimitExceeded;
        if (kind == .classification) {
            if (self.classifications.items.len + fields.len > self.options.max_classification_labels) return error.BoundaryQueryLimitExceeded;
        } else if (self.queries.items.len + fields.len > self.options.max_queries) return error.BoundaryQueryLimitExceeded;
        if (self.groups.items.len > 0) try self.appendMarker(.sep_struct);
        const group_index = self.groups.items.len;
        var header = std.ArrayListUnmanaged(u8).empty;
        try header.appendSlice(self.allocator, name);
        if (prompt) |p| if (p.len > 0) {
            try header.appendSlice(self.allocator, ": ");
            try header.appendSlice(self.allocator, p);
        };
        for (fields) |field| if (field.description) |description| {
            try header.appendSlice(self.allocator, " [DESCRIPTION] ");
            try header.appendSlice(self.allocator, field.name);
            try header.appendSlice(self.allocator, ": ");
            try header.appendSlice(self.allocator, description);
        };
        for (examples) |example| {
            try header.appendSlice(self.allocator, " [EXAMPLE] ");
            try header.appendSlice(self.allocator, example.input);
            try header.appendSlice(self.allocator, " [OUTPUT] ");
            try header.appendSlice(self.allocator, example.label);
        }
        const fragments = try self.allocator.alloc([]const u8, 6 + fields.len * 2);
        fragments[0] = "(";
        fragments[1] = marker_names[@intFromEnum(Marker.p)];
        fragments[2] = try header.toOwnedSlice(self.allocator);
        fragments[3] = "(";
        try self.appendEncoded(fragments[0]);
        const parent_marker = self.ids.items.len;
        try self.appendMarker(.p);
        try self.appendEncoded(fragments[2]);
        try self.appendEncoded(fragments[3]);
        const positions = try self.allocator.alloc(usize, fields.len);
        for (fields, positions, 0..) |field, *position, i| {
            const owned_name = try self.allocator.dupe(u8, field.name);
            fragments[4 + i * 2] = marker_names[@intFromEnum(marker)];
            fragments[5 + i * 2] = owned_name;
            position.* = self.ids.items.len;
            try self.appendMarker(marker);
            try self.appendEncoded(owned_name);
            if (kind == .classification) {
                try self.classifications.append(self.allocator, .{ .group_index = group_index, .schema_index = schema_index, .label_index = field.role_index, .name = owned_name, .marker_index = position.* });
            } else {
                try self.queries.append(self.allocator, .{ .kind = field.kind, .group_index = group_index, .schema_index = field.schema_index, .role_index = i, .label_index = field.role_index, .name = owned_name, .marker_index = position.* });
            }
        }
        fragments[fragments.len - 2] = ")";
        fragments[fragments.len - 1] = ")";
        try self.appendEncoded(")");
        try self.appendEncoded(")");
        const model_name = if (kind == .entities) "entities" else fragments[2][0 .. std.mem.indexOf(u8, fragments[2], " [DESCRIPTION] ") orelse fragments[2].len];
        try self.groups.append(self.allocator, .{ .kind = kind, .schema_index = schema_index, .name = try self.allocator.dupe(u8, name), .model_name = model_name, .fragments = fragments, .parent_marker = parent_marker, .child_markers = positions });
    }

    fn addEnumPrefix(self: *Builder, structures: []const ir.Structure) !void {
        for (structures, 0..) |structure, i| {
            var first = true;
            for (structure.fields, 0..) |field, j| {
                if (field.choices.len == 0) continue;
                if (first) {
                    try self.appendWord("(", null, false);
                    try self.appendWord(try std.fmt.allocPrint(self.allocator, "{s}:", .{structure.name}), null, false);
                    first = false;
                } else try self.appendWord(",", null, false);
                try self.appendWord(try self.allocator.dupe(u8, field.name), null, false);
                try self.appendWord("(", null, false);
                for (field.choices, 0..) |choice, k| {
                    if (k > 0) try self.appendWord("|", null, false);
                    const start = self.words.items.len;
                    try self.appendWord(try self.allocator.dupe(u8, choice), null, false);
                    try self.enum_choices.append(self.allocator, .{ .structure_index = i, .field_index = j, .choice_index = k, .start = start, .end = start + 1, .score_start = start });
                }
                try self.appendWord(")", null, false);
            }
            if (!first) try self.appendWord(")", null, false);
        }
        var first_positions = std.StringHashMapUnmanaged(usize).empty;
        for (self.words.items, 0..) |word, i| {
            try self.options.check();
            const folded = try lower(self.allocator, word.text);
            const entry = try first_positions.getOrPut(self.allocator, folded);
            if (!entry.found_existing) entry.value_ptr.* = i;
        }
        for (self.enum_choices.items) |*choice| {
            const folded = try lower(self.allocator, self.words.items[choice.start].text);
            choice.score_start = first_positions.get(folded).?;
        }
    }

    fn appendWord(self: *Builder, text: []const u8, source: ?ByteRange, synthetic_suffix: bool) !void {
        if (self.words.items.len >= self.options.max_total_words) return error.BoundaryTextLimitExceeded;
        const start = self.ids.items.len;
        try self.appendEncoded(text);
        try self.words.append(self.allocator, .{ .text = text, .source = source, .has_synthetic_suffix = synthetic_suffix, .input_start = start, .input_end = self.ids.items.len });
    }

    fn appendMarker(self: *Builder, marker: Marker) !void {
        try self.options.check();
        if (self.ids.items.len >= self.options.max_sequence_tokens) return error.BoundarySequenceLimitExceeded;
        try self.ids.append(self.allocator, self.markers.id(marker));
    }

    fn appendEncoded(self: *Builder, fragment: []const u8) !void {
        try self.options.check();
        if (fragment.len > self.options.max_fragment_bytes) return error.BoundaryFragmentLimitExceeded;
        const encoded = try self.tokenizer.encode(self.allocator, fragment);
        defer self.allocator.free(encoded);
        try self.options.check();
        if (encoded.len == 0) return error.UnencodableBoundaryFragment;
        if (encoded.len > self.options.max_sequence_tokens - self.ids.items.len) return error.BoundarySequenceLimitExceeded;
        try self.ids.ensureUnusedCapacity(self.allocator, encoded.len);
        for (encoded) |id| {
            if (id < 0 or @as(usize, @intCast(id)) >= self.tokenizer.vocabSize()) return error.InvalidGlinerBoundaryTokenizer;
            self.ids.appendAssumeCapacity(id);
        }
    }
};

fn product(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.BoundaryBatchLimitExceeded;
}
fn zeros(comptime T: type, allocator: Allocator, n: usize) ![]T {
    const out = try allocator.alloc(T, n);
    @memset(out, if (T == bool) false else 0);
    return out;
}

// Python's default splitter regex, evaluated over the original UTF-8 document.
// These helpers use the repository's pinned Python 3.12 / Unicode 15 tables.
fn wordEnd(text: []const u8, start: usize, splitter: WordSplitter) usize {
    if (splitter == .char) {
        var end = start;
        while (end < text.len and (std.ascii.isAlphanumeric(text[end]) or std.mem.indexOfScalar(u8, "@._-+", text[end]) != null)) end += 1;
        return if (end > start) end else nextCodepoint(text, start);
    }
    if (urlPrefixEnd(text, start)) |prefix_end| if (prefix_end < text.len and !unicode.isWhitespace(codepoint(text, prefix_end))) {
        var end = prefix_end;
        while (end < text.len and !unicode.isWhitespace(codepoint(text, end))) end = nextCodepoint(text, end);
        return end;
    };
    if (emailEnd(text, start)) |end| return end;
    if (text[start] == '@' and start + 1 < text.len and asciiRegexWord(codepoint(text, start + 1))) {
        var end = start + 1;
        while (end < text.len and asciiRegexWord(codepoint(text, end))) end = nextCodepoint(text, end);
        return end;
    }
    if (unicode.isWord(codepoint(text, start))) {
        var end = start;
        while (end < text.len and unicode.isWord(codepoint(text, end))) end = nextCodepoint(text, end);
        while (end + 1 < text.len and (text[end] == '-' or text[end] == '_') and unicode.isWord(codepoint(text, end + 1))) {
            end += 1;
            while (end < text.len and unicode.isWord(codepoint(text, end))) end = nextCodepoint(text, end);
        }
        return end;
    }
    return nextCodepoint(text, start);
}
fn urlPrefixEnd(text: []const u8, start: usize) ?usize {
    for ([_][]const u8{ "https://", "http://", "www." }) |prefix| {
        var pos = start;
        for (prefix) |expected| {
            if (pos == text.len) break;
            const cp = codepoint(text, pos);
            if (unicode.simpleLower(cp) != expected and !(expected == 's' and cp == 0x017f)) break;
            pos = nextCodepoint(text, pos);
        } else return pos;
    }
    return null;
}
fn emailEnd(text: []const u8, start: usize) ?usize {
    var pos = start;
    while (pos < text.len) {
        const cp = codepoint(text, pos);
        if (!asciiRegexLetter(cp) and !(cp >= '0' and cp <= '9') and !(cp < 128 and std.mem.indexOfScalar(u8, "._%+-", @intCast(cp)) != null)) break;
        pos = nextCodepoint(text, pos);
    }
    if (pos == start or pos == text.len or text[pos] != '@') return null;
    pos += 1;
    const domain_start = pos;
    var separator_valid = false;
    var tld_letters: usize = 0;
    var best: ?usize = null;
    while (pos < text.len) {
        const cp = codepoint(text, pos);
        if (!asciiRegexLetter(cp) and !(cp >= '0' and cp <= '9') and cp != '.' and cp != '-') break;
        const end = nextCodepoint(text, pos);
        if (cp == '.') {
            separator_valid = pos > domain_start;
            tld_letters = 0;
        } else if (separator_valid and asciiRegexLetter(cp)) {
            tld_letters += 1;
            if (tld_letters >= 2) best = end;
        } else {
            separator_valid = false;
            tld_letters = 0;
        }
        pos = end;
    }
    return best;
}
fn asciiRegexLetter(cp: u21) bool {
    const folded = unicode.simpleLower(cp);
    return (folded >= 'a' and folded <= 'z') or cp == 0x0130 or cp == 0x0131 or cp == 0x017f;
}
fn asciiRegexWord(cp: u21) bool {
    return asciiRegexLetter(cp) or (cp >= '0' and cp <= '9') or cp == '_';
}
fn codepoint(text: []const u8, start: usize) u21 {
    return std.unicode.utf8Decode(text[start..nextCodepoint(text, start)]) catch unreachable;
}
fn nextCodepoint(text: []const u8, start: usize) usize {
    return start + (std.unicode.utf8ByteSequenceLength(text[start]) catch unreachable);
}
fn previousCodepoint(text: []const u8, end: usize) usize {
    var pos = end - 1;
    while (pos > 0 and text[pos] & 0xc0 == 0x80) pos -= 1;
    return pos;
}
fn finalSigma(token: []const u8, start: usize) bool {
    var before = start;
    var cased_before = false;
    while (before > 0) {
        before = previousCodepoint(token, before);
        const cp = codepoint(token, before);
        if (unicode.isCaseIgnorable(cp)) continue;
        cased_before = unicode.isCased(cp);
        break;
    }
    if (!cased_before) return false;
    var after = nextCodepoint(token, start);
    while (after < token.len) : (after = nextCodepoint(token, after)) {
        const cp = codepoint(token, after);
        if (unicode.isCaseIgnorable(cp)) continue;
        return !unicode.isCased(cp);
    }
    return true;
}
fn lower(allocator: Allocator, token: []const u8) ![]const u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    var pos: usize = 0;
    while (pos < token.len) : (pos = nextCodepoint(token, pos)) {
        const cp = codepoint(token, pos);
        if (cp == 0x0130) {
            // Python lower expands İ to i + combining dot. Offset storage is
            // independent of token bytes, so the expansion is safe here.
            try out.appendSlice(allocator, "i\u{0307}");
            continue;
        }
        const lowered = if (cp == 0x03a3 and finalSigma(token, pos)) @as(u21, 0x03c2) else unicode.simpleLower(cp);
        var bytes: [4]u8 = undefined;
        const n = try std.unicode.utf8Encode(lowered, &bytes);
        try out.appendSlice(allocator, bytes[0..n]);
    }
    return out.toOwnedSlice(allocator);
}

const rich_test_schema =
    \\{"entities":["person","organization"],"entity_definitions":{"person":{"description":"A named person"},"organization":{"description":"An employer"}},"entity_attributes":{"status":{"labels":["active","former"],"qualify_labels":true},"sentiment":{"labels":["positive","negative"],"qualify_labels":true}},"structures":{"review":{"fields":{"product":{"type":"str","description":"The reviewed product"},"rating":{"type":"str","choices":["great","poor"],"description":"Overall product rating"}}}},"relations":[{"type":"works_for","description":"A person is employed by an organization"}],"classifications":[{"name":"sentiment","labels":["positive","negative"],"prompt":"Choose the review sentiment","label_definitions":{"positive":{"description":"A favorable review"},"negative":{"description":"An unfavorable review"}},"examples":[["It works well","positive"],["It broke","negative"]]}]}
;
const simple_test_schema = "{\"entities\":[\"location\",\"person\"]}";

const TestTokenizer = struct {
    drop_fragment: ?[]const u8 = null,
    context: u8 = 0,

    fn tokenizer(self: *TestTokenizer) Tokenizer {
        return .{ .ptr = self, .vtable = &.{
            .encode = encode,
            .encodeInto = encodeInto,
            .encodeForModel = undefined,
            .encodeGeneration = undefined,
            .decode = undefined,
            .specialTokens = specialTokens,
            .allSpecialTokenIds = specialIds,
            .vocabSize = vocabSize,
            .deinit = undefined,
        } };
    }
    fn encode(raw: *anyopaque, allocator: Allocator, text: []const u8) ![]i32 {
        var out = std.ArrayListUnmanaged(i32).empty;
        errdefer out.deinit(allocator);
        try encodeInto(raw, allocator, text, &out);
        return out.toOwnedSlice(allocator);
    }
    fn encodeInto(raw: *anyopaque, allocator: Allocator, text: []const u8, out: *std.ArrayListUnmanaged(i32)) !void {
        const self: *TestTokenizer = @ptrCast(@alignCast(raw));
        if (self.drop_fragment) |drop| if (std.mem.eql(u8, drop, text)) return;
        var pos: usize = 0;
        while (pos < text.len) {
            var matched = false;
            for (marker_names, 0..) |name, i| {
                if (std.mem.startsWith(u8, text[pos..], name)) {
                    try out.append(allocator, @as(i32, @intCast(300 + i)));
                    pos += name.len;
                    matched = true;
                    break;
                }
            }
            if (matched) continue;
            try out.append(allocator, @as(i32, text[pos]) + 10);
            pos += 1;
        }
    }
    fn specialTokens(_: *anyopaque) @import("inference_tokenizer").SpecialTokens {
        return .{ .unk_id = 1 };
    }
    fn specialIds(_: *anyopaque, allocator: Allocator) ![]u32 {
        const ids = try allocator.alloc(u32, marker_names.len);
        for (ids, 0..) |*id, i| id.* = @intCast(300 + i);
        return ids;
    }
    fn vocabSize(_: *anyopaque) usize {
        return 512;
    }
};

fn readGolden(allocator: Allocator) ![]u8 {
    const c_file = @import("../util/c_file.zig");
    for ([_][]const u8{ "", "pkg/inference/", "zig/pkg/inference/" }) |prefix| {
        const path = try std.fmt.allocPrint(allocator, "{s}testdata/gliner25/preprocessing.json", .{prefix});
        defer allocator.free(path);
        const bytes = c_file.readFile(allocator, path) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        return bytes;
    }
    return error.FileNotFound;
}

fn expectGolden(allocator: Allocator, tokenizer: Tokenizer, golden: std.json.Value, compare_token_ids: bool) !void {
    for (golden.object.get("cases").?.array.items) |case| {
        const id = case.object.get("id").?.string;
        const samples = case.object.get("samples").?.array.items;
        const compiled = try allocator.alloc(ir.CompiledSchema, samples.len);
        defer allocator.free(compiled);
        var compiled_count: usize = 0;
        defer for (compiled[0..compiled_count]) |*schema| schema.deinit();
        const items = try allocator.alloc(Item, samples.len);
        defer allocator.free(items);
        for (samples, items, 0..) |sample, *item, row| {
            const rich = row == 0 and (std.mem.eql(u8, id, "rich_schema") or std.mem.eql(u8, id, "heterogeneous_batch"));
            compiled[row] = try ir.compile(allocator, if (rich) rich_test_schema else simple_test_schema, .{});
            compiled_count += 1;
            item.* = .{ .text = sample.object.get("text").?.string, .schema = &compiled[row] };
        }
        var batch = try prepare(allocator, tokenizer, items, .{ .word_splitter = std.meta.stringToEnum(WordSplitter, case.object.get("word_splitter").?.string).? });
        defer batch.deinit();
        const expected = case.object.get("expected").?.object;
        for (batch.samples, 0..) |sample, row| {
            try std.testing.expectEqualStrings(items[row].text, sample.original_text);
            const expected_words = expected.get("text_tokens").?.array.items[row].array.items;
            try std.testing.expectEqual(expected_words.len, sample.words.len);
            for (sample.words, expected_words) |word, expected_word| try std.testing.expectEqualStrings(expected_word.string, word.text);
            const expected_groups = expected.get("schema_tokens_list").?.array.items[row].array.items;
            try std.testing.expectEqual(expected_groups.len, sample.groups.len);
            for (sample.groups, expected_groups) |group, expected_group| {
                try std.testing.expectEqual(expected_group.array.items.len, group.fragments.len);
                for (group.fragments, expected_group.array.items) |fragment, value| try std.testing.expectEqualStrings(value.string, fragment);
            }
            const expected_queries = expected.get("query_layouts").?.array.items[row].object.get("queries").?.array.items;
            try std.testing.expectEqual(expected_queries.len, sample.queries.len);
            for (sample.queries, expected_queries) |query, expected_query| {
                try std.testing.expectEqualStrings(expected_query.object.get("role_name").?.string, query.name);
                try std.testing.expectEqualStrings(expected_query.object.get("task_name").?.string, sample.groups[query.group_index].model_name);
                try std.testing.expectEqual(@as(usize, @intCast(expected_query.object.get("task_index").?.integer)), query.group_index);
                try std.testing.expectEqual(@as(usize, @intCast(expected_query.object.get("role_index").?.integer)), query.role_index);
            }
            const starts = expected.get("start_mappings").?.array.items[row].array.items;
            const ends = expected.get("end_mappings").?.array.items[row].array.items;
            try std.testing.expectEqual(starts.len, sample.body_word_count);
            for (sample.words[sample.prefix_word_count..], starts, ends) |word, start, end| {
                const begin = testCodepointToByte(sample.original_text, @intCast(start.integer));
                const finish = testCodepointToByte(sample.original_text, @intCast(end.integer));
                if (begin == sample.original_text.len) {
                    try std.testing.expectEqual(@as(?ByteRange, null), word.source);
                } else {
                    try std.testing.expectEqualDeep(ByteRange{ .start = begin, .end = finish }, word.source.?);
                }
            }
        }
        if (compare_token_ids) {
            const tensors = expected.get("tensors").?.object;
            inline for (.{
                .{ "input_ids", "input_ids" },                 .{ "attention_mask", "attention_mask" },
                .{ "text_word_indices", "text_word_indices" }, .{ "query_marker_indices", "query_marker_indices" },
                .{ "query_group_index", "query_group_index" }, .{ "cls_marker_indices", "cls_marker_indices" },
                .{ "cls_group_index", "cls_group_index" },
            }) |mapping| {
                const values = tensors.get(mapping[0]).?.object.get("values").?.array.items;
                const actual = @field(batch, mapping[1]);
                try std.testing.expectEqual(values.len, actual.len);
                for (values, actual) |value, number| try std.testing.expectEqual(value.integer, number);
            }
            inline for (.{ "text_word_mask", "query_marker_mask", "cls_marker_mask" }) |name| {
                const values = tensors.get(name).?.object.get("values").?.array.items;
                const actual = @field(batch, name);
                try std.testing.expectEqual(values.len, actual.len);
                for (values, actual) |value, enabled| try std.testing.expectEqual(value.bool, enabled);
            }
        }
        // Every padded route points only to index zero with a false mask.
        for (batch.samples, 0..) |sample, row| {
            try expectPadding(i64, batch.input_ids, batch.sequence_length, row, sample.input_ids.len);
            try expectPadding(i64, batch.attention_mask, batch.sequence_length, row, sample.input_ids.len);
            try expectPadding(i64, batch.text_word_indices, batch.word_width, row, sample.words.len);
            try expectPadding(bool, batch.text_word_mask, batch.word_width, row, sample.words.len);
            try expectPadding(i64, batch.query_marker_indices, batch.query_width, row, sample.queries.len);
            try expectPadding(bool, batch.query_marker_mask, batch.query_width, row, sample.queries.len);
            try expectPadding(i64, batch.cls_marker_indices, batch.classification_width, row, sample.classification_labels.len);
            try expectPadding(bool, batch.cls_marker_mask, batch.classification_width, row, sample.classification_labels.len);
        }
    }
}
fn expectPadding(comptime T: type, values: []const T, width: usize, row: usize, count: usize) !void {
    for (values[row * width + count .. (row + 1) * width]) |value| try std.testing.expectEqual(@as(T, if (T == bool) false else 0), value);
}
fn testCodepointToByte(text: []const u8, count: usize) usize {
    var pos: usize = 0;
    var n: usize = 0;
    while (pos < text.len and n < count) : (n += 1) pos = nextCodepoint(text, pos);
    return pos;
}

test "boundary processor prompts words and query layouts match pinned Python golden" {
    const bytes = try readGolden(std.testing.allocator);
    defer std.testing.allocator.free(bytes);
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, bytes, .{});
    defer parsed.deinit();
    var tokenizer = TestTokenizer{};
    try expectGolden(std.testing.allocator, tokenizer.tokenizer(), parsed.value, false);
}

test "boundary processor token IDs and padded routes match pinned small tokenizer" {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const path = try std.fs.path.join(allocator, &.{ directory, "tokenizer.json" });
    defer allocator.free(path);
    const bytes = try @import("../util/c_file.zig").readFile(allocator, path);
    defer allocator.free(bytes);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const actual_hash = std.fmt.bytesToHex(digest, .lower);
    try std.testing.expectEqualStrings("cbc8ae6037812709c9c26f2a160f8dc48b0440bcb79c8141804259ae2d6adac3", &actual_hash);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(allocator, bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const golden = try readGolden(allocator);
    defer allocator.free(golden);
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, golden, .{});
    defer parsed.deinit();
    try expectGolden(allocator, tokenizer.tokenizer(), parsed.value, true);
}

test "boundary processor limits are atomic and empty subwords never shift offsets" {
    const allocator = std.testing.allocator;
    var schema = try ir.compile(allocator, simple_test_schema, .{});
    defer schema.deinit();
    var tokenizer = TestTokenizer{};
    const items = [_]Item{.{ .text = "One two", .schema = &schema }};
    try std.testing.expectError(error.BoundaryTextLimitExceeded, prepare(allocator, tokenizer.tokenizer(), &items, .{ .max_text_words = 1 }));
    try std.testing.expectError(error.BoundarySequenceLimitExceeded, prepare(allocator, tokenizer.tokenizer(), &items, .{ .max_sequence_tokens = 1 }));
    try std.testing.expectError(error.BoundaryBatchLimitExceeded, prepare(allocator, tokenizer.tokenizer(), &items, .{ .max_batch_tokens = 1 }));
    try std.testing.expectError(error.BoundaryTextLimitExceeded, prepare(allocator, tokenizer.tokenizer(), &items, .{ .max_text_bytes = 1 }));
    tokenizer.drop_fragment = "two";
    try std.testing.expectError(error.UnencodableBoundaryFragment, prepare(allocator, tokenizer.tokenizer(), &items, .{}));
    tokenizer.drop_fragment = "[E]";
    try std.testing.expectError(error.InvalidGlinerBoundaryTokenizer, prepare(allocator, tokenizer.tokenizer(), &items, .{}));
    tokenizer.drop_fragment = null;
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, prepare(allocator, tokenizer.tokenizer(), &items, .{ .control = .{ .check_fn = Cancel.check } }));
    try std.testing.expectError(error.InvalidUtf8, prepare(allocator, tokenizer.tokenizer(), &.{.{ .text = "\xff", .schema = &schema }}, .{}));
}

test "boundary processor retains original URL offsets and synthetic suffix identity" {
    const allocator = std.testing.allocator;
    var schema = try ir.compile(allocator, simple_test_schema, .{});
    defer schema.deinit();
    var tokenizer = TestTokenizer{};
    const text = "https://example.com/A";
    var batch = try prepare(allocator, tokenizer.tokenizer(), &.{.{ .text = text, .schema = &schema }}, .{});
    defer batch.deinit();
    try std.testing.expectEqual(@as(usize, 1), batch.samples[0].words.len);
    const word = batch.samples[0].words[0];
    try std.testing.expectEqualStrings("https://example.com/a.", word.text);
    try std.testing.expectEqualDeep(ByteRange{ .start = 0, .end = text.len }, word.source.?);
    try std.testing.expect(word.has_synthetic_suffix);
}

test "boundary processor allocation failures release the complete batch" {
    var schema = try ir.compile(std.testing.allocator, rich_test_schema, .{});
    defer schema.deinit();
    const Check = struct {
        fn run(allocator: Allocator, compiled: *const ir.CompiledSchema) !void {
            var tokenizer = TestTokenizer{};
            var batch = try prepare(allocator, tokenizer.tokenizer(), &.{.{ .text = "John ΟΣ İpek", .schema = compiled }}, .{});
            defer batch.deinit();
            try std.testing.expectEqual(@as(usize, 4), batch.samples[0].groups.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{&schema});
}

test "gliner boundary original word ranges and synthetic terminal capacity share the processor grammar" {
    const a = std.testing.allocator;
    const text = " İ 😀 中 https://x.test";
    const ranges = try sourceWordRanges(a, text, .{});
    defer a.free(ranges);
    try std.testing.expectEqual(@as(usize, 4), ranges.len);
    for ([_][]const u8{ "İ", "😀", "中", "https://x.test" }, ranges) |word, range|
        try std.testing.expectEqualStrings(word, text[range.start..range.end]);
    try std.testing.expect(!try terminalWordAdded(a, text, .whitespace));
    try std.testing.expect(try terminalWordAdded(a, "foo", .whitespace));
    try std.testing.expect(!try terminalWordAdded(a, "foo", .char));
    try std.testing.expect(try terminalWordAdded(a, "https://x.test ", .whitespace));
    try std.testing.expect(try terminalWordAdded(a, "a@x.test", .whitespace));
    try std.testing.expect(!try terminalWordAdded(a, "done!", .whitespace));
    try std.testing.expect(try terminalWordAdded(a, "", .whitespace));
    try std.testing.expectError(error.BoundaryTextLimitExceeded, sourceWordRanges(a, text, .{ .max_words = 3 }));
    try std.testing.expectError(error.InvalidUtf8, sourceWordRanges(a, "\xff", .{}));
    const Check = struct {
        fn run(allocator: Allocator) !void {
            const words = try sourceWordRanges(allocator, "İ😀https://x.test x", .{});
            defer allocator.free(words);
            _ = try terminalWordAdded(allocator, "https://x.test", .whitespace);
        }
        fn cancel(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{});
    try std.testing.expectError(error.Cancelled, sourceWordRanges(a, text, .{ .control = .{ .check_fn = Check.cancel } }));
}
