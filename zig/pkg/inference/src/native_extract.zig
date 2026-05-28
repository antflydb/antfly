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

const std = @import("std");
const build_options = @import("build_options");
const backends = @import("backends/backends.zig");
const model_manager_mod = @import("server/model_manager.zig");
const gliner_mod = @import("pipelines/gliner.zig");
const ner_mod = @import("pipelines/ner.zig");

const print = std.debug.print;

const BackendChoice = enum {
    auto,
    native,
    metal,
    mlx,
};

const Options = struct {
    model_dir: []const u8,
    text: []const u8,
    schema_json: []const u8,
    backend: BackendChoice = .auto,
    relation_labels: std.ArrayListUnmanaged([]const u8) = .empty,

    fn deinit(self: *Options, allocator: std.mem.Allocator) void {
        self.relation_labels.deinit(allocator);
    }
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var opts = try parseArgs(allocator, args);
    defer opts.deinit(allocator);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, opts.schema_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidSchemaJson;

    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    configureBackendPreference(&session_manager, opts.backend);

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const model = try model_manager.loadFromDir(opts.model_dir);
    const texts = [_][]const u8{opts.text};

    var schema_labels = std.ArrayListUnmanaged([]const u8).empty;
    defer schema_labels.deinit(allocator);
    var key_it = parsed.value.object.iterator();
    while (key_it.next()) |entry| {
        try schema_labels.append(allocator, entry.key_ptr.*);
    }

    if (model.isGlinerModel()) {
        var gliner = model.glinerPipeline(allocator);
        if (opts.relation_labels.items.len > 0) {
            const extracted = try gliner.extractRelationsBatch(&texts, schema_labels.items, opts.relation_labels.items);
            defer freeEntities(allocator, extracted.entities);
            defer freeRelations(allocator, extracted.relations);
            try writeExtractJson(allocator, opts.model_dir, parsed.value.object, extracted.entities, extracted.relations);
        } else {
            const entities = try gliner.recognizeBatch(&texts, schema_labels.items);
            defer freeEntities(allocator, entities);
            try writeExtractJson(allocator, opts.model_dir, parsed.value.object, entities, null);
        }
    } else {
        if (opts.relation_labels.items.len > 0) return error.RelationExtractionNotSupported;
        var pipeline = model.nerPipeline(allocator);
        const entities = try pipeline.recognizeBatch(&texts);
        defer freeEntities(allocator, entities);
        try writeExtractJson(allocator, opts.model_dir, parsed.value.object, entities, null);
    }
}

fn parseArgs(allocator: std.mem.Allocator, args: []const []const u8) !Options {
    if (args.len < 3) {
        printUsage();
        return error.InvalidArguments;
    }

    var opts = Options{
        .model_dir = args[0],
        .text = args[1],
        .schema_json = args[2],
    };
    errdefer opts.deinit(allocator);

    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= args.len) return error.MissingBackendValue;
            opts.backend = parseBackendChoice(args[i]) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--relation-label")) {
            i += 1;
            if (i >= args.len) return error.MissingRelationLabelValue;
            try opts.relation_labels.append(allocator, args[i]);
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }

    return opts;
}

fn writeExtractJson(
    allocator: std.mem.Allocator,
    model_name: []const u8,
    schema: std.json.ObjectMap,
    all_entities: []const []const ner_mod.Entity,
    all_relations: ?[]const []const gliner_mod.Relation,
) !void {
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    try buf.appendSlice(allocator, "{\"model\":");
    try jsonEncodeString(&buf, allocator, model_name);
    try buf.appendSlice(allocator, ",\"results\":[");
    for (all_entities, 0..) |entities, ti| {
        if (ti > 0) try buf.append(allocator, ',');
        try buf.append(allocator, '{');

        var first_label = true;
        var schema_it = schema.iterator();
        while (schema_it.next()) |schema_entry| {
            const schema_name = schema_entry.key_ptr.*;
            if (!first_label) try buf.append(allocator, ',');
            first_label = false;
            try jsonEncodeString(&buf, allocator, schema_name);
            try buf.append(allocator, ':');
            try buf.append(allocator, '[');

            var first_entity = true;
            for (entities) |e| {
                if (labelMatchesSchema(e.label, schema_name)) {
                    if (!first_entity) try buf.append(allocator, ',');
                    first_entity = false;
                    try buf.appendSlice(allocator, "{\"value\":");
                    try jsonEncodeString(&buf, allocator, e.text);
                    const meta = try std.fmt.allocPrint(
                        allocator,
                        ",\"score\":{d},\"start\":{d},\"end\":{d}}}",
                        .{ e.score, e.start, e.end },
                    );
                    defer allocator.free(meta);
                    try buf.appendSlice(allocator, meta);
                }
            }

            try buf.append(allocator, ']');
        }

        if (all_relations) |rels_by_text| {
            const relations = if (ti < rels_by_text.len) rels_by_text[ti] else &.{};
            try buf.appendSlice(allocator, ",\"relations\":[");
            for (relations, 0..) |relation, ri| {
                if (ri > 0) try buf.append(allocator, ',');
                try buf.appendSlice(allocator, "{\"head\":");
                try writeRelationEntityJson(&buf, allocator, relation.head);
                try buf.appendSlice(allocator, ",\"tail\":");
                try writeRelationEntityJson(&buf, allocator, relation.tail);
                try buf.appendSlice(allocator, ",\"label\":");
                try jsonEncodeString(&buf, allocator, relation.label);
                const meta = try std.fmt.allocPrint(allocator, ",\"score\":{d}}}", .{relation.score});
                defer allocator.free(meta);
                try buf.appendSlice(allocator, meta);
            }
            try buf.append(allocator, ']');
        }

        try buf.append(allocator, '}');
    }
    try buf.appendSlice(allocator, "]}\n");

    print("{s}", .{buf.items});
}

fn freeEntities(allocator: std.mem.Allocator, all_entities: [][]ner_mod.Entity) void {
    for (all_entities) |entities| {
        for (entities) |e| allocator.free(e.text);
        allocator.free(entities);
    }
    allocator.free(all_entities);
}

fn freeRelations(allocator: std.mem.Allocator, all_relations: [][]gliner_mod.Relation) void {
    for (all_relations) |relations| {
        for (relations) |*relation| relation.deinit(allocator);
        allocator.free(relations);
    }
    allocator.free(all_relations);
}

fn labelMatchesSchema(label: []const u8, schema_name: []const u8) bool {
    if (label.len == 0 or schema_name.len == 0) return false;
    if (std.ascii.eqlIgnoreCase(label, schema_name)) return true;
    if (label.len > schema_name.len and label.len >= 2 and label[1] == '-') {
        if (std.ascii.eqlIgnoreCase(label[2..], schema_name)) return true;
    }
    return false;
}

fn writeRelationEntityJson(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, entity: ner_mod.Entity) !void {
    try buf.appendSlice(allocator, "{\"text\":");
    try jsonEncodeString(buf, allocator, entity.text);
    try buf.appendSlice(allocator, ",\"label\":");
    try jsonEncodeString(buf, allocator, entity.label);
    const meta = try std.fmt.allocPrint(
        allocator,
        ",\"start\":{d},\"end\":{d},\"score\":{d}}}",
        .{ entity.start, entity.end, entity.score },
    );
    defer allocator.free(meta);
    try buf.appendSlice(allocator, meta);
}

fn jsonEncodeString(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    try buf.append(allocator, '"');
    for (s) |ch| {
        switch (ch) {
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => {
                if (ch < 0x20) {
                    const hex = try std.fmt.allocPrint(allocator, "\\u{x:0>4}", .{ch});
                    defer allocator.free(hex);
                    try buf.appendSlice(allocator, hex);
                } else {
                    try buf.append(allocator, ch);
                }
            },
        }
    }
    try buf.append(allocator, '"');
}

fn parseBackendChoice(value: []const u8) ?BackendChoice {
    if (std.mem.eql(u8, value, "auto")) return .auto;
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    if (std.mem.eql(u8, value, "mlx")) return .mlx;
    return null;
}

fn configureBackendPreference(session_manager: *backends.SessionManager, choice: BackendChoice) void {
    session_manager.preferred_backends = switch (choice) {
        .auto => if (build_options.enable_metal and build_options.enable_mlx)
            &.{ backends.BackendType.metal, backends.BackendType.mlx, backends.BackendType.native }
        else if (build_options.enable_metal)
            &.{ backends.BackendType.metal, backends.BackendType.native }
        else if (build_options.enable_mlx)
            &.{ backends.BackendType.mlx, backends.BackendType.native }
        else
            &.{backends.BackendType.native},
        .native => &.{backends.BackendType.native},
        .metal => if (build_options.enable_metal) &.{backends.BackendType.metal} else &.{backends.BackendType.native},
        .mlx => if (build_options.enable_mlx) &.{backends.BackendType.mlx} else &.{backends.BackendType.native},
    };
}

fn printUsage() void {
    print(
        \\usage: antfly inference extract <model-dir> <text> <schema-json> [--backend auto|native|metal|mlx] [--relation-label LABEL]...
        \\  Runs native local extraction and prints a JSON response to stdout.
        \\
    , .{});
}

test "parseArgs accepts schema json and backend" {
    var opts = try parseArgs(std.testing.allocator, &.{
        "/tmp/model",
        "John works at Google",
        "{\"person\":[\"name::str\"]}",
        "--backend",
        "native",
    });
    defer opts.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("/tmp/model", opts.model_dir);
    try std.testing.expectEqualStrings("John works at Google", opts.text);
    try std.testing.expectEqualStrings("{\"person\":[\"name::str\"]}", opts.schema_json);
    try std.testing.expectEqual(BackendChoice.native, opts.backend);
}

test "parseArgs accepts repeated relation labels" {
    var opts = try parseArgs(std.testing.allocator, &.{
        "/tmp/model",
        "John works at Google",
        "{\"person\":[\"name::str\"]}",
        "--relation-label",
        "works_for",
        "--relation-label",
        "located_in",
    });
    defer opts.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), opts.relation_labels.items.len);
    try std.testing.expectEqualStrings("works_for", opts.relation_labels.items[0]);
    try std.testing.expectEqualStrings("located_in", opts.relation_labels.items[1]);
}
