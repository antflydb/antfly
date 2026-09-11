// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Versioned, named, explicit-offset JSONL training data. A bounded owned
//! snapshot binds the bytes actually consumed to resume identity. Schemas are
//! supplied independently of answers; no importer infers the label universe.
const std = @import("std");
const schema_mod = @import("../pipelines/extraction_schema.zig");
const target = @import("gliner_boundary_targets.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const decode = @import("../pipelines/gliner_boundary_decode.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Budget = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const Allocator = std.mem.Allocator;
const Hash = std.crypto.hash.sha2.Sha256;
const regex = @import("../pipelines/extraction_regex.zig");

pub const Attribute = struct { group: []const u8, labels: []const []const u8 };
pub const Entity = struct { id: []const u8, type: []const u8, span: target.Source, attributes: []const Attribute = &.{} };
pub const Value = struct { occurrences: ?[]const target.Source = null, choice: ?[]const u8 = null };
pub const Field = struct { name: []const u8, values: []const Value };
pub const Record = struct { type: []const u8, id: []const u8, fields: []const Field };
pub const Endpoint = struct { entity: ?[]const u8 = null, span: ?target.Source = null };
pub const Relation = struct { type: []const u8, head: Endpoint, tail: Endpoint };
pub const Classification = struct { task: []const u8, labels: []const []const u8 };
pub const Row = struct {
    version: u32,
    id: []const u8,
    text: []const u8,
    schema: std.json.Value,
    entities: []const Entity = &.{},
    records: []const Record = &.{},
    relations: []const Relation = &.{},
    classifications: []const Classification = &.{},
};
pub const Limits = struct {
    max_file_bytes: usize = 256 * 1024 * 1024,
    max_host_bytes: usize = 512 * 1024 * 1024,
    max_row_bytes: usize = 2 * 1024 * 1024,
    max_examples: usize = 1024 * 1024,
    max_id_bytes: usize = 256,
    max_text_bytes: usize = 1024 * 1024,
    max_annotations: usize = 65536,
    max_json_depth: usize = 64,
};
pub const Options = struct { limits: Limits = .{}, schema: schema_mod.Options = .{}, regex: regex.ContextOptions = .{} };
pub const Failure = struct {
    line: ?usize = null,
    stage: enum { snapshot, row, schema, annotations, tokenization, targets } = .snapshot,
    /// The final allocation failure for this operation. A failed resize/remap
    /// is not terminal and never changes declared/backing attribution.
    allocation: ?Budget.AllocationFailure = null,
};

/// Stable diagnostic state is charged to the existing dataset host budget.
/// Allocating operations serialize their terminal failure observation; returned
/// samples remain independent and can be released after an operation returns.
/// No caller control or failure pointer survives an operation.
const AllocationOwner = struct {
    budget: *Budget,
    operation: std.atomic.Mutex = .unlocked,
    terminal: ?Budget.AllocationFailure = null,

    fn create(backing: Allocator, limit: usize, failure: ?*Failure) !*AllocationOwner {
        const budget = try backing.create(Budget);
        errdefer backing.destroy(budget);
        budget.* = .{ .backing = backing, .limit = limit };
        // The temporary observer covers allocation of the stable state itself.
        // Install the stable callback before any owned data is allocated.
        var setup = AllocationOwner{ .budget = budget };
        setup.attach();
        const owner = budget.allocator().create(AllocationOwner) catch |err| return setup.translate(err, failure);
        owner.* = .{ .budget = budget };
        owner.attach();
        return owner;
    }

    fn attach(self: *AllocationOwner) void {
        self.budget.failure_context = self;
        self.budget.allocation_failed = observe;
    }

    fn observe(raw: ?*anyopaque, event: Budget.AllocationFailure) void {
        const self: *AllocationOwner = @ptrCast(@alignCast(raw.?));
        // BoundedAllocator calls this under its lock; free/resize/remap never
        // emit terminal failures. The operation lock prevents unrelated users
        // of Dataset's allocating methods from replacing this observation.
        self.terminal = event;
    }

    fn enter(self: *AllocationOwner, control: ?Control) !void {
        try (control orelse Control{}).lock(&self.operation);
        self.terminal = null;
    }

    fn leave(self: *AllocationOwner) void {
        self.operation.unlock();
    }

    fn translate(self: *AllocationOwner, err: anyerror, failure: ?*Failure) anyerror {
        if (err != error.OutOfMemory and err != error.WriteFailed) return err;
        const terminal = self.terminal orelse return err;
        if (failure) |details| details.allocation = terminal;
        return if (terminal.kind == .declared_limit) error.BoundaryTrainingDatasetMemoryLimitExceeded else error.OutOfMemory;
    }

    fn destroy(self: *AllocationOwner) void {
        const budget = self.budget;
        const backing = budget.backing;
        budget.failure_context = null;
        budget.allocation_failed = null;
        budget.allocator().destroy(self);
        std.debug.assert(budget.live == 0);
        backing.destroy(budget);
    }
};
const Index = struct { start: usize, end: usize, id: []const u8, text_sha256: [32]u8 };

/// Samples use the dataset's stable budget. Every sample must be released
/// before its dataset; prepared batches may borrow the sample's text/schema.
pub const Sample = struct {
    arena: *std.heap.ArenaAllocator,
    allocator: Allocator,
    row: Row,
    schema: schema_mod.CompiledSchema,
    annotations: target.Annotations,
    validators: *regex.Context,
    pub fn deinit(self: *Sample) void {
        self.schema.deinit();
        self.validators.deinit();
        self.arena.deinit();
        self.allocator.destroy(self.arena);
        self.* = undefined;
    }
};

pub const Dataset = struct {
    backing: Allocator,
    budget: *Budget,
    allocation_owner: *AllocationOwner,
    bytes: []u8,
    index: []Index,
    options: Options,
    sha256: [32]u8,
    schemas_sha256: [32]u8,

    pub fn open(a: Allocator, path: []const u8, options: Options, control: ?Control, failure: ?*Failure) !Dataset {
        try check(control);
        if (failure) |f| f.* = .{};
        const owner = try AllocationOwner.create(a, options.limits.max_host_bytes, failure);
        errdefer owner.destroy();
        return openOwned(a, owner, path, options, control, failure) catch |err| return owner.translate(err, failure);
    }

    fn openOwned(backing: Allocator, owner: *AllocationOwner, path: []const u8, options: Options, control: ?Control, failure: ?*Failure) !Dataset {
        const bytes = try readSnapshot(owner.budget.allocator(), path, options.limits.max_file_bytes, control);
        errdefer owner.budget.allocator().free(bytes);
        return adopt(backing, owner, bytes, options, control, failure);
    }

    pub fn fromBytes(a: Allocator, bytes: []const u8, options: Options, control: ?Control, failure: ?*Failure) !Dataset {
        try check(control);
        if (failure) |f| f.* = .{};
        if (bytes.len > options.limits.max_file_bytes) return error.BoundaryTrainingDatasetLimitExceeded;
        const owner = try AllocationOwner.create(a, options.limits.max_host_bytes, failure);
        errdefer owner.destroy();
        return fromBytesOwned(a, owner, bytes, options, control, failure) catch |err| return owner.translate(err, failure);
    }

    fn fromBytesOwned(backing: Allocator, owner: *AllocationOwner, bytes: []const u8, options: Options, control: ?Control, failure: ?*Failure) !Dataset {
        const owned = try owner.budget.allocator().dupe(u8, bytes);
        errdefer owner.budget.allocator().free(owned);
        return adopt(backing, owner, owned, options, control, failure);
    }

    fn adopt(backing: Allocator, owner: *AllocationOwner, bytes: []u8, options: Options, control: ?Control, failure: ?*Failure) !Dataset {
        const budget = owner.budget;
        const a = budget.allocator();
        if (bytes.len == 0) return error.EmptyBoundaryTrainingDataset;
        var rows = std.ArrayListUnmanaged(Index).empty;
        errdefer {
            for (rows.items) |row| a.free(row.id);
            rows.deinit(a);
        }
        var ids = std.StringHashMapUnmanaged(void).empty;
        defer ids.deinit(a);
        var schemas = Hash.init(.{});
        schemas.update("antfly.boundary-training.schemas.v1\x00");
        var offset: usize = 0;
        while (offset < bytes.len) {
            try check(control);
            const newline = std.mem.indexOfScalarPos(u8, bytes, offset, '\n') orelse bytes.len;
            const end = if (newline > offset and bytes[newline - 1] == '\r') newline - 1 else newline;
            if (failure) |f| f.* = .{ .line = rows.items.len + 1, .stage = .row };
            if (rows.items.len >= options.limits.max_examples or end - offset > options.limits.max_row_bytes) return error.BoundaryTrainingDatasetLimitExceeded;
            if (end == offset) return error.EmptyBoundaryTrainingRow;
            var parsed_row = try parse(a, bytes[offset..end], options, control, failure);
            defer parsed_row.deinit();
            const id = try a.dupe(u8, parsed_row.row.id);
            errdefer a.free(id);
            const entry = try ids.getOrPut(a, id);
            if (entry.found_existing) return error.DuplicateBoundaryTrainingExample;
            var text_hash: [32]u8 = undefined;
            Hash.hash(parsed_row.row.text, &text_hash, .{});
            try rows.append(a, .{ .start = offset, .end = end, .id = id, .text_sha256 = text_hash });
            schemas.update(&parsed_row.schema.fingerprint);
            offset = if (newline < bytes.len) newline + 1 else bytes.len;
        }
        try check(control);
        var hash = Hash.init(.{});
        var pos: usize = 0;
        while (pos < bytes.len) {
            try check(control);
            const end = @min(bytes.len, pos + 256 * 1024);
            hash.update(bytes[pos..end]);
            pos = end;
        }
        return .{ .backing = backing, .budget = budget, .allocation_owner = owner, .bytes = bytes, .index = try rows.toOwnedSlice(a), .options = options, .sha256 = hash.finalResult(), .schemas_sha256 = schemas.finalResult() };
    }

    pub fn deinit(self: *Dataset) void {
        const a = self.budget.allocator();
        for (self.index) |row| a.free(row.id);
        a.free(self.index);
        a.free(self.bytes);
        self.allocation_owner.destroy();
        self.* = undefined;
    }

    pub fn sample(self: *const Dataset, index: usize, control: ?Control, failure: ?*Failure) !Sample {
        if (failure) |f| f.* = .{};
        try self.allocation_owner.enter(control);
        defer self.allocation_owner.leave();
        return self.sampleOwned(index, control, failure) catch |err| return self.allocation_owner.translate(err, failure);
    }

    fn sampleOwned(self: *const Dataset, index: usize, control: ?Control, failure: ?*Failure) !Sample {
        if (index >= self.index.len) return error.InvalidBoundaryTrainingExample;
        if (failure) |f| f.* = .{ .line = index + 1, .stage = .row };
        const row = self.index[index];
        return parse(self.budget.allocator(), self.bytes[row.start..row.end], self.options, control, failure);
    }

    /// Verify every annotation against actual tokenizer boundaries and target
    /// routing before training starts. Failure retains the original row number;
    /// oversized or invalid examples are never removed from the denominator.
    pub fn preflight(self: *const Dataset, tokenizer: @import("inference_tokenizer").Tokenizer, supplied_processor_options: processor.Options, target_options: target.Options, control: ?Control, failure: ?*Failure) !void {
        if (failure) |f| f.* = .{};
        try self.allocation_owner.enter(control);
        defer self.allocation_owner.leave();
        return self.preflightOwned(tokenizer, supplied_processor_options, target_options, control, failure) catch |err| return self.allocation_owner.translate(err, failure);
    }

    fn preflightOwned(self: *const Dataset, tokenizer: @import("inference_tokenizer").Tokenizer, supplied_processor_options: processor.Options, target_options: target.Options, control: ?Control, failure: ?*Failure) !void {
        var processor_options = supplied_processor_options;
        processor_options.control = control;
        for (0..self.index.len) |i| {
            var row = try self.sampleOwned(i, control, failure);
            defer row.deinit();
            if (failure) |f| f.stage = .tokenization;
            var prepared = try processor.prepare(self.budget.allocator(), tokenizer, &.{.{ .text = row.row.text, .schema = &row.schema }}, processor_options);
            defer prepared.deinit();
            if (failure) |f| f.stage = .targets;
            var selected = target_options;
            selected.control = control;
            row.validators.options.compile_options.control = control;
            row.validators.options.match_options.control = control;
            selected.regex_context = row.validators;
            selected.validate_value_fn = regex.Context.validateValue;
            var compiled = try target.compileBatch(self.budget.allocator(), prepared.samples, &.{&row.schema}, &.{row.annotations}, selected);
            defer compiled.deinit();
        }
        try check(control);
    }

    /// Explicit split contamination check using exact document IDs and exact
    /// UTF-8 text. This is not a claim about near-duplicates or pretraining data.
    pub fn requireDisjoint(self: *const Dataset, other: *const Dataset, control: ?Control) !void {
        try self.allocation_owner.enter(control);
        defer self.allocation_owner.leave();
        return self.requireDisjointOwned(other, control) catch |err| return self.allocation_owner.translate(err, null);
    }

    fn requireDisjointOwned(self: *const Dataset, other: *const Dataset, control: ?Control) !void {
        const a = self.budget.allocator();
        var ids = std.StringHashMapUnmanaged(void).empty;
        defer ids.deinit(a);
        var texts = std.AutoHashMapUnmanaged([32]u8, void).empty;
        defer texts.deinit(a);
        for (self.index) |row| {
            try check(control);
            try ids.put(a, row.id, {});
            try texts.put(a, row.text_sha256, {});
        }
        for (other.index) |row| {
            try check(control);
            if (ids.contains(row.id) or texts.contains(row.text_sha256)) return error.BoundaryTrainingSplitOverlap;
        }
    }
};

fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}
fn findNamed(items: anytype, name: []const u8) !usize {
    for (items, 0..) |item, i| if (std.mem.eql(u8, item.name, name)) return i;
    return error.UnknownBoundaryTrainingLabel;
}
fn labels(a: Allocator, names: []const []const u8, declared: []const []const u8) ![]const usize {
    const result = try a.alloc(usize, names.len);
    for (names, result, 0..) |name, *index, i| {
        index.* = for (declared, 0..) |label, j| {
            if (std.mem.eql(u8, name, label)) break j;
        } else return error.UnknownBoundaryTrainingLabel;
        if (std.mem.indexOfScalar(usize, result[0..i], index.*) != null) return error.DuplicateBoundaryTrainingLabel;
    }
    return result;
}
fn validateSource(mapping: decode.OffsetMap, source: target.Source) !void {
    const bytes = try mapping.toBytes(.{ .start = source.start, .end = source.end }, source.unit);
    if (bytes.start >= bytes.end) return error.InvalidBoundaryTrainingTargets;
}
fn endpoint(mapping: decode.OffsetMap, entities: std.StringHashMapUnmanaged(usize), raw: Endpoint) !target.Endpoint {
    if ((raw.entity != null) == (raw.span != null)) return error.InvalidBoundaryTrainingEndpoint;
    if (raw.span) |span| {
        try validateSource(mapping, span);
        return .{ .document = span };
    }
    return .{ .entity = entities.get(raw.entity.?) orelse return error.UnknownBoundaryTrainingEntity };
}
fn identifier(value: []const u8, limits: Limits) !void {
    if (value.len == 0 or value.len > limits.max_id_bytes or !std.unicode.utf8ValidateSlice(value)) return error.InvalidBoundaryTrainingIdentifier;
    for (value) |c| if (c < 32 or c == 127) return error.InvalidBoundaryTrainingIdentifier;
}
fn charge(count: *usize, amount: usize, limits: Limits) !void {
    count.* = std.math.add(usize, count.*, amount) catch return error.BoundaryTrainingDatasetLimitExceeded;
    if (count.* > limits.max_annotations) return error.BoundaryTrainingDatasetLimitExceeded;
}

fn parse(backing: Allocator, bytes: []const u8, options: Options, control: ?Control, failure: ?*Failure) !Sample {
    try check(control);
    if (bytes.len > options.limits.max_row_bytes) return error.BoundaryTrainingDatasetLimitExceeded;
    try validateDepth(bytes, options.limits.max_json_depth, control);
    const arena = try backing.create(std.heap.ArenaAllocator);
    errdefer backing.destroy(arena);
    arena.* = std.heap.ArenaAllocator.init(backing);
    errdefer arena.deinit();
    const a = arena.allocator();
    const parsed = try std.json.parseFromSlice(Row, a, bytes, .{ .allocate = .alloc_always });
    const row = parsed.value;
    if (row.version != 1) return error.UnsupportedBoundaryTrainingDatasetVersion;
    try identifier(row.id, options.limits);
    if (row.text.len == 0 or row.text.len > options.limits.max_text_bytes) return error.InvalidBoundaryTrainingText;
    if (failure) |f| f.stage = .schema;
    const schema_json = try std.json.Stringify.valueAlloc(a, row.schema, .{});
    if (options.schema.regex_context != null or options.schema.validate_regex_fn != null or options.regex.compile_options.control != null or options.regex.match_options.control != null) return error.UnsupportedBoundaryTrainingValidatorOverride;
    const validators = try a.create(regex.Context);
    var regex_options = options.regex;
    regex_options.compile_options.control = control;
    regex_options.match_options.control = control;
    validators.* = regex.Context.init(a, regex_options);
    errdefer validators.deinit();
    var schema = try schema_mod.compile(a, schema_json, validators.compilerOptions(options.schema));
    errdefer schema.deinit();
    if (failure) |f| f.stage = .annotations;
    var mapping = try decode.OffsetMap.init(a, row.text, options.limits.max_text_bytes);
    defer mapping.deinit();
    var count: usize = 0;
    for ([_]usize{ row.entities.len, row.records.len, row.relations.len, row.classifications.len }) |size| try charge(&count, size, options.limits);
    const entities = try a.alloc(target.Entity, row.entities.len);
    var entity_ids = std.StringHashMapUnmanaged(usize).empty;
    for (row.entities, entities, 0..) |entity, *out, i| {
        try check(control);
        try identifier(entity.id, options.limits);
        const entry = try entity_ids.getOrPut(a, entity.id);
        if (entry.found_existing) return error.DuplicateBoundaryTrainingEntity;
        entry.value_ptr.* = i;
        try validateSource(mapping, entity.span);
        const entity_type = if (schema.schema.joint_ie) |joint| try findNamed(joint.entities, entity.type) else try findNamed(schema.schema.entities, entity.type);
        try charge(&count, entity.attributes.len, options.limits);
        const attributes = try a.alloc(target.Attribute, entity.attributes.len);
        for (entity.attributes, attributes) |attribute, *attr| {
            const group = try findNamed(schema.schema.entity_attributes, attribute.group);
            try charge(&count, attribute.labels.len, options.limits);
            attr.* = .{ .group = group, .labels = try labels(a, attribute.labels, schema.schema.entity_attributes[group].labels) };
        }
        out.* = .{ .entity_type = entity_type, .source = entity.span, .attributes = attributes };
    }
    const records = try a.alloc(target.Record, row.records.len);
    for (row.records, records) |record, *out| {
        try check(control);
        try identifier(record.id, options.limits);
        const structure = try findNamed(schema.schema.structures, record.type);
        try charge(&count, record.fields.len, options.limits);
        const fields = try a.alloc(target.Field, record.fields.len);
        for (record.fields, fields) |field, *f| {
            const field_index = try findNamed(schema.schema.structures[structure].fields, field.name);
            const definition = schema.schema.structures[structure].fields[field_index];
            try charge(&count, field.values.len, options.limits);
            const values = try a.alloc(target.Value, field.values.len);
            for (field.values, values) |value, *v| {
                if ((value.occurrences != null) == (value.choice != null)) return error.InvalidBoundaryTrainingValue;
                if (value.occurrences) |occurrences| {
                    if (occurrences.len == 0 or definition.choices.len != 0) return error.InvalidBoundaryTrainingValue;
                    try charge(&count, occurrences.len, options.limits);
                    for (occurrences) |source| try validateSource(mapping, source);
                    v.* = .{ .document = occurrences };
                } else {
                    const choices = try labels(a, &.{value.choice.?}, definition.choices);
                    v.* = .{ .choice = choices[0] };
                }
            }
            f.* = .{ .field = field_index, .values = values };
        }
        out.* = .{ .structure = structure, .id = record.id, .fields = fields };
    }
    const relations = try a.alloc(target.Relation, row.relations.len);
    for (row.relations, relations) |relation, *out| {
        try check(control);
        out.* = .{ .relation_type = if (schema.schema.joint_ie) |joint| try findNamed(joint.relations, relation.type) else try findNamed(schema.schema.relations, relation.type), .head = try endpoint(mapping, entity_ids, relation.head), .tail = try endpoint(mapping, entity_ids, relation.tail) };
    }
    const classifications = try a.alloc(target.Classification, row.classifications.len);
    for (row.classifications, classifications) |classification, *out| {
        try check(control);
        const task = for (schema.schema.classifications, 0..) |task, i| {
            if (std.mem.eql(u8, task.task.name, classification.task)) break i;
        } else return error.UnknownBoundaryTrainingLabel;
        try charge(&count, classification.labels.len, options.limits);
        out.* = .{ .task = task, .labels = try labels(a, classification.labels, schema.schema.classifications[task].task.labels) };
    }
    try check(control);
    validators.options.compile_options.control = null;
    validators.options.match_options.control = null;
    return .{ .allocator = backing, .arena = arena, .row = row, .schema = schema, .validators = validators, .annotations = .{ .schema_fingerprint = schema.fingerprint, .entities = entities, .records = records, .relations = relations, .classifications = classifications } };
}

/// Bound parser/stringifier nesting before either can allocate or recurse.
/// Full lexical and structural validity is still checked by std.json.
fn validateDepth(bytes: []const u8, maximum: usize, control: ?Control) !void {
    var depth: usize = 0;
    var quoted = false;
    var escaped = false;
    for (bytes, 0..) |byte, index| {
        if ((index & 4095) == 0) try check(control);
        if (quoted) {
            if (escaped) escaped = false else if (byte == '\\') escaped = true else if (byte == '"') quoted = false;
        } else switch (byte) {
            '"' => quoted = true,
            '{', '[' => {
                if (depth >= maximum) return error.BoundaryTrainingDatasetLimitExceeded;
                depth += 1;
            },
            '}', ']' => {
                if (depth == 0) return error.InvalidBoundaryTrainingJSON;
                depth -= 1;
            },
            else => {},
        }
    }
}

fn readSnapshot(a: Allocator, path: []const u8, max_bytes: usize, control: ?Control) ![]u8 {
    const compat = @import("../io/compat.zig");
    return @import("../runtime/file_snapshot.zig").read(a, compat.io(), compat.cwd(), path, max_bytes, control) catch |err| switch (err) {
        error.InvalidSnapshotFile => error.InvalidBoundaryTrainingDatasetFile,
        error.SnapshotLimitExceeded => error.BoundaryTrainingDatasetLimitExceeded,
        else => err,
    };
}

const rich_row =
    \\{"version":1,"id":"document-1","text":"é Ada 🚀 Acme","schema":{"entities":["person","company"],"entity_attributes":{"tone":{"labels":["positive","negative"],"applies_to":["person"]}},"classifications":[{"name":"topic","labels":["meeting","billing"],"mode":"multi"}],"structures":{"deal":{"mode":"anchorless","fields":{"party":{"dtype":"str"},"state":{"dtype":"str","choices":["paid","due"]}}}},"relations":[{"type":"met"}]},"entities":[{"id":"ada","type":"person","span":{"start":2,"end":5,"unit":"unicode_codepoints"},"attributes":[{"group":"tone","labels":["positive"]}]},{"id":"acme","type":"company","span":{"start":12,"end":16}}],"records":[{"type":"deal","id":"deal-1","fields":[{"name":"party","values":[{"occurrences":[{"start":3,"end":6}]}]},{"name":"state","values":[{"choice":"due"}]}]}],"relations":[{"type":"met","head":{"entity":"ada"},"tail":{"entity":"acme"}}],"classifications":[{"task":"topic","labels":[]}]}
;
const simple_row =
    \\{"version":1,"id":"document-2","text":"Ada Acme","schema":{"entities":["person","company"]},"entities":[{"id":"ada","type":"person","span":{"start":0,"end":3}}]}
;

fn exerciseDataset(a: Allocator) !void {
    var dataset = try Dataset.fromBytes(a, rich_row ++ "\r\n" ++ simple_row ++ "\n", .{}, null, null);
    defer dataset.deinit();
    try std.testing.expectEqual(@as(usize, 2), dataset.index.len);
    var first = try dataset.sample(0, null, null);
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, 2), first.schema.schema.entities.len);
    try std.testing.expectEqual(@as(usize, 1), first.annotations.entities[1].entity_type);
    try std.testing.expectEqual(@as(usize, 0), first.annotations.entities[0].attributes[0].labels[0]);
    try std.testing.expectEqual(@as(usize, 1), first.annotations.records[0].fields[1].values[0].choice);
    try std.testing.expectEqual(@as(usize, 0), first.annotations.relations[0].head.entity);
    try std.testing.expectEqual(@as(usize, 1), first.annotations.relations[0].tail.entity);
    try std.testing.expectEqual(@as(usize, 0), first.annotations.classifications[0].labels.len);
    var second = try dataset.sample(1, null, null);
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 0), second.annotations.classifications.len);
    var isolated = try Dataset.fromBytes(a, simple_row, .{}, null, null);
    defer isolated.deinit();
    if (dataset.requireDisjoint(&isolated, null)) |_| return error.TestExpectedError else |err| {
        if (err != error.BoundaryTrainingSplitOverlap) return err;
    }
}

test "boundary training dataset resolves named mixed tasks and keeps exact occurrence and missing-label semantics" {
    try exerciseDataset(std.testing.allocator);
}

test "boundary training dataset allocation failures release snapshots schemas samples and split indexes" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseDataset, .{});
}

test "boundary training dataset rejects invalid rows duplicates version schema omissions and byte boundaries" {
    const a = std.testing.allocator;
    var failure = Failure{};
    try std.testing.expectError(error.DuplicateBoundaryTrainingExample, Dataset.fromBytes(a, simple_row ++ "\n" ++ simple_row, .{}, null, &failure));
    try std.testing.expectEqual(@as(?usize, 2), failure.line);
    try std.testing.expectError(error.EmptyBoundaryTrainingRow, Dataset.fromBytes(a, simple_row ++ "\n\n", .{}, null, &failure));
    try std.testing.expectEqual(@as(?usize, 2), failure.line);
    try std.testing.expectError(error.BoundaryTrainingDatasetLimitExceeded, Dataset.fromBytes(a, simple_row, .{ .limits = .{ .max_row_bytes = 8 } }, null, null));
    try std.testing.expectError(error.UnsupportedBoundaryTrainingDatasetVersion, Dataset.fromBytes(a, "{\"version\":2,\"id\":\"x\",\"text\":\"Ada\",\"schema\":{\"entities\":[\"person\"]}}", .{}, null, null));
    try std.testing.expectError(error.UnknownBoundaryTrainingLabel, Dataset.fromBytes(a, "{\"version\":1,\"id\":\"x\",\"text\":\"Ada\",\"schema\":{\"entities\":[\"company\"]},\"entities\":[{\"id\":\"a\",\"type\":\"person\",\"span\":{\"start\":0,\"end\":3}}]}", .{}, null, null));
    const cancelled = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, Dataset.fromBytes(a, simple_row, .{}, .{ .check_fn = cancelled.check }, null));
    try std.testing.expectError(error.BoundaryTrainingDatasetLimitExceeded, Dataset.fromBytes(a, "[" ** 65 ++ "0" ++ "]" ** 65, .{}, null, null));
    try validateDepth("{\"text\":\"[\\\"{{]\"}", 1, null);
    try std.testing.expectError(error.InvalidUtf8Boundary, Dataset.fromBytes(a, "{\"version\":1,\"id\":\"x\",\"text\":\"é\",\"schema\":{\"entities\":[\"person\"]},\"entities\":[{\"id\":\"a\",\"type\":\"person\",\"span\":{\"start\":1,\"end\":2}}]}", .{}, null, null));
}

test "boundary training dataset owns immutable file bytes and preflights real tokenizer targets" {
    const a = std.testing.allocator;
    const compat = @import("../io/compat.zig");
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/train.jsonl", .{temporary.sub_path});
    defer a.free(path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = simple_row });
    var dataset = try Dataset.open(a, path, .{}, null, null);
    defer dataset.deinit();
    const digest = dataset.sha256;
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = "changed" });
    var sample = try dataset.sample(0, null, null);
    defer sample.deinit();
    try std.testing.expectEqualStrings("Ada Acme", sample.row.text);
    var expected: [32]u8 = undefined;
    Hash.hash(simple_row, &expected, .{});
    try std.testing.expectEqual(expected, digest);
    const tokenizer_bytes = try @import("../util/c_file.zig").readFileMax(a, "testdata/gliner25/training_step/tokenizer.json", 1024 * 1024);
    defer a.free(tokenizer_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    try dataset.preflight(tokenizer.tokenizer(), .{}, .{}, null, null);
}

test "boundary training dataset terminal allocation distinguishes declared cap backing failure and cached retry" {
    const a = std.testing.allocator;
    var failure = Failure{};
    try std.testing.expectError(error.BoundaryTrainingDatasetMemoryLimitExceeded, Dataset.fromBytes(a, simple_row, .{ .limits = .{ .max_host_bytes = 1 } }, null, &failure));
    try std.testing.expectEqual(.declared_limit, failure.allocation.?.kind);
    try std.testing.expectEqual(@as(usize, 1), failure.allocation.?.limit_bytes);

    var dataset = try Dataset.fromBytes(a, simple_row, .{}, null, null);
    defer dataset.deinit();
    const original_limit = dataset.budget.limit;
    dataset.budget.limit = dataset.budget.live;
    try std.testing.expectError(error.BoundaryTrainingDatasetMemoryLimitExceeded, dataset.sample(0, null, &failure));
    try std.testing.expectEqual(.declared_limit, failure.allocation.?.kind);
    try std.testing.expectEqual(@as(?usize, 1), failure.line);
    dataset.budget.limit = original_limit;
    const retained = dataset.budget.live;
    const bytes = try dataset.budget.allocator().alloc(u8, 8);
    defer dataset.budget.allocator().free(bytes);
    try std.testing.expect(!dataset.budget.allocator().resize(bytes, original_limit + 1));
    try std.testing.expect(dataset.budget.denied);
    var backing_failure = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    dataset.budget.backing = backing_failure.allocator();
    const failed_sample = dataset.sample(0, null, &failure);
    dataset.budget.backing = a;
    try std.testing.expectError(error.OutOfMemory, failed_sample);
    try std.testing.expectEqual(.backing_allocator, failure.allocation.?.kind);
    try std.testing.expectEqual(retained + 8, dataset.budget.live);
    var retry = try dataset.sample(0, null, &failure);
    defer retry.deinit();
    try std.testing.expect(failure.allocation == null);
    try std.testing.expectEqualStrings("Ada Acme", retry.row.text);
}

test "boundary training dataset terminal operation rejects current cancellation without retaining caller controls" {
    var dataset = try Dataset.fromBytes(std.testing.allocator, simple_row, .{}, null, null);
    defer dataset.deinit();
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    var failure = Failure{};
    const original_limit = dataset.budget.limit;
    dataset.budget.limit = dataset.budget.live;
    try std.testing.expectError(error.BoundaryTrainingDatasetMemoryLimitExceeded, dataset.sample(0, null, &failure));
    dataset.budget.limit = original_limit;
    try std.testing.expect(failure.allocation != null);
    try std.testing.expectError(error.Cancelled, dataset.sample(0, .{ .check_fn = Cancel.check }, &failure));
    try std.testing.expect(failure.allocation == null);
    var sample = try dataset.sample(0, null, null);
    defer sample.deinit();
    try std.testing.expectEqualStrings("document-2", sample.row.id);
}
