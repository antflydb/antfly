// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const db_mod = @import("../../storage/db/mod.zig");

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

pub fn encodeRemoteBatchRequest(alloc: std.mem.Allocator, req: db_mod.types.BatchRequest) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.appendSlice(alloc, "{\"inserts\":{");
    for (req.writes, 0..) |write, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, &out, write.key);
        try out.append(alloc, ':');
        try out.appendSlice(alloc, write.value);
    }
    try out.append(alloc, '}');
    if (req.deletes.len > 0) {
        try out.appendSlice(alloc, ",\"deletes\":[");
        for (req.deletes, 0..) |key, i| {
            if (i > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, &out, key);
        }
        try out.append(alloc, ']');
    }
    if (req.relational_identity_rewrites.len > 0) {
        try out.appendSlice(alloc, ",\"relational_identity_rewrites\":[");
        for (req.relational_identity_rewrites, 0..) |rewrite, i| {
            if (i > 0) try out.append(alloc, ',');
            try out.appendSlice(alloc, "{\"old_key\":");
            try appendJsonString(alloc, &out, rewrite.old_key);
            try out.appendSlice(alloc, ",\"new_key\":");
            try appendJsonString(alloc, &out, rewrite.new_key);
            try out.appendSlice(alloc, ",\"value\":");
            try out.appendSlice(alloc, rewrite.value);
            try out.append(alloc, '}');
        }
        try out.append(alloc, ']');
    }
    if (req.transforms.len > 0) {
        try out.appendSlice(alloc, ",\"transforms\":[");
        for (req.transforms, 0..) |transform, i| {
            if (i > 0) try out.append(alloc, ',');
            try out.appendSlice(alloc, "{\"key\":");
            try appendJsonString(alloc, &out, transform.key);
            try out.appendSlice(alloc, ",\"operations\":[");
            for (transform.operations, 0..) |op, op_index| {
                if (op_index > 0) try out.append(alloc, ',');
                try out.appendSlice(alloc, "{\"op\":");
                try appendJsonString(alloc, &out, db_mod.transform.transformOpText(op.op));
                try out.appendSlice(alloc, ",\"path\":");
                try appendJsonString(alloc, &out, op.path);
                if (op.value_json) |value_json| {
                    try out.appendSlice(alloc, ",\"value\":");
                    try out.appendSlice(alloc, value_json);
                }
                try out.append(alloc, '}');
            }
            try out.append(alloc, ']');
            if (transform.upsert) try out.appendSlice(alloc, ",\"upsert\":true");
            try out.append(alloc, '}');
        }
        try out.append(alloc, ']');
    }
    try out.appendSlice(alloc, ",\"sync_level\":");
    try appendJsonString(alloc, &out, db_mod.types.publicSyncLevelText(req.sync_level));
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

test "remote batch request encoder preserves writes deletes transforms and escaping" {
    const alloc = std.testing.allocator;

    const operations = [_]db_mod.types.TransformOp{
        .{ .op = .set, .path = "profile.name", .value_json = "\"Ada\"" },
        .{ .op = .add_to_set, .path = "tags", .value_json = "\"math\"" },
    };
    const transforms = [_]db_mod.types.DocumentTransform{.{
        .key = "doc:transform",
        .operations = &operations,
        .upsert = true,
    }};
    const body = try encodeRemoteBatchRequest(alloc, .{
        .writes = &.{
            .{ .key = "doc:\"quoted\"", .value = "{\"title\":\"alpha\"}" },
        },
        .deletes = &.{"doc:\nold"},
        .relational_identity_rewrites = &.{.{
            .old_key = "doc:old",
            .new_key = "doc:new",
            .value = "{\"title\":\"renamed\"}",
        }},
        .transforms = &transforms,
        .sync_level = .write,
    });
    defer alloc.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const root = parsed.value.object;
    try std.testing.expectEqualStrings("alpha", root.get("inserts").?.object.get("doc:\"quoted\"").?.object.get("title").?.string);
    try std.testing.expectEqualStrings("doc:\nold", root.get("deletes").?.array.items[0].string);
    try std.testing.expectEqualStrings("write", root.get("sync_level").?.string);
    const rewrite = root.get("relational_identity_rewrites").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:old", rewrite.get("old_key").?.string);
    try std.testing.expectEqualStrings("doc:new", rewrite.get("new_key").?.string);
    try std.testing.expectEqualStrings("renamed", rewrite.get("value").?.object.get("title").?.string);

    const transform = root.get("transforms").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:transform", transform.get("key").?.string);
    try std.testing.expect(transform.get("upsert").?.bool);
    const encoded_ops = transform.get("operations").?.array.items;
    try std.testing.expectEqualStrings("$set", encoded_ops[0].object.get("op").?.string);
    try std.testing.expectEqualStrings("profile.name", encoded_ops[0].object.get("path").?.string);
    try std.testing.expectEqualStrings("Ada", encoded_ops[0].object.get("value").?.string);
    try std.testing.expectEqualStrings("$addToSet", encoded_ops[1].object.get("op").?.string);
    try std.testing.expectEqualStrings("tags", encoded_ops[1].object.get("path").?.string);
    try std.testing.expectEqualStrings("math", encoded_ops[1].object.get("value").?.string);
}
