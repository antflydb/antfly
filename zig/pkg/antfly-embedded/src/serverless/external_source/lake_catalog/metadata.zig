// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Iceberg v2 table requirements and updates. All changes are staged in an
//! arena; no object is published until the complete resulting metadata validates.
const std = @import("std");
const types = @import("types.zig");
const A = std.mem.Allocator;
const V = std.json.Value;

pub fn parse(a: A, bytes: []const u8) !V {
    if (bytes.len > types.max_metadata_bytes) return error.LakeMetadataTooLarge;
    return (try std.json.parseFromSlice(V, a, bytes, .{ .allocate = .alloc_always, .max_value_len = types.max_metadata_bytes })).value;
}
pub fn get(v: V, key: []const u8) !V {
    if (v != .object) return error.InvalidLakeMetadata;
    return v.object.get(key) orelse error.InvalidLakeMetadata;
}
pub fn str(v: V) ![]const u8 {
    if (v != .string or v.string.len == 0) return error.InvalidLakeMetadata;
    return v.string;
}
pub fn int(v: V) !i64 {
    return switch (v) {
        .integer => v.integer,
        .number_string => std.fmt.parseInt(i64, v.number_string, 10) catch error.InvalidLakeMetadata,
        else => error.InvalidLakeMetadata,
    };
}
fn items(v: V) ![]V {
    if (v != .array or v.array.items.len > 100_000) return error.InvalidLakeMetadata;
    return v.array.items;
}
fn put(a: A, v: *V, key: []const u8, value: V) !void {
    if (v.* != .object) return error.InvalidLakeMetadata;
    try v.object.put(a, try a.dupe(u8, key), value);
}
fn object(a: A) V {
    _ = a;
    return .{ .object = .empty };
}
fn array(a: A) V {
    return .{ .array = std.array_list.Managed(V).init(a) };
}
fn append(a: A, v: *V, key: []const u8, value: V) !void {
    if (!v.object.contains(key)) try put(a, v, key, array(a));
    const list = v.object.getPtr(key).?;
    if (list.* != .array) return error.InvalidLakeMetadata;
    try list.array.append(value);
}
fn string(a: A, value: []const u8) !V {
    return .{ .string = try a.dupe(u8, value) };
}
fn find(root: V, list: []const u8, key: []const u8, id: i64) !?V {
    for (try items(try get(root, list))) |entry| if (try int(try get(entry, key)) == id) return entry;
    return null;
}
fn same(a: A, lhs: V, rhs: V) !bool {
    const l = try std.json.Stringify.valueAlloc(a, lhs, .{});
    const r = try std.json.Stringify.valueAlloc(a, rhs, .{});
    return std.mem.eql(u8, l, r);
}

pub fn requirements(a: A, current: ?V, envelope: V) !void {
    for (try items(try get(envelope, "requirements"))) |req| {
        const kind = try str(try get(req, "type"));
        if (std.mem.eql(u8, kind, "assert-create")) {
            if (current != null) return error.LakeCommitConflict;
            continue;
        }
        const table = current orelse return error.LakeCommitConflict;
        if (std.mem.eql(u8, kind, "assert-table-uuid")) {
            if (!std.mem.eql(u8, try str(try get(req, "uuid")), try str(try get(table, "table-uuid")))) return error.LakeCommitConflict;
        } else if (std.mem.eql(u8, kind, "assert-ref-snapshot-id")) {
            const name = try str(try get(req, "ref"));
            const refs = try get(table, "refs");
            if (refs != .object) return error.InvalidLakeMetadata;
            const expected = try get(req, "snapshot-id");
            const actual = if (refs.object.get(name)) |ref| try get(ref, "snapshot-id") else V.null;
            if (!try same(a, expected, actual)) return error.LakeCommitConflict;
        } else {
            var matched = false;
            inline for (.{
                .{ "assert-last-assigned-field-id", "last-assigned-field-id", "last-column-id" },
                .{ "assert-current-schema-id", "current-schema-id", "current-schema-id" },
                .{ "assert-last-assigned-partition-id", "last-assigned-partition-id", "last-partition-id" },
                .{ "assert-default-spec-id", "default-spec-id", "default-spec-id" },
                .{ "assert-default-sort-order-id", "default-sort-order-id", "default-sort-order-id" },
            }) |mapping| {
                if (std.mem.eql(u8, kind, mapping[0])) {
                    matched = true;
                    if (try int(try get(req, mapping[1])) != try int(try get(table, mapping[2]))) return error.LakeCommitConflict;
                }
            }
            if (!matched) return error.UnsupportedLakeRequirement;
        }
    }
}

fn maximumFieldId(v: V, depth: usize) !i64 {
    if (depth > 32) return error.InvalidLakeMetadata;
    var result: i64 = 0;
    switch (v) {
        .object => |map| {
            inline for (.{ "id", "element-id", "key-id", "value-id" }) |key| {
                if (map.get(key)) |id| {
                    const n = try int(id);
                    if (n <= 0) return error.InvalidLakeMetadata;
                    result = @max(result, n);
                }
            }
            var it = map.iterator();
            while (it.next()) |entry| result = @max(result, try maximumFieldId(entry.value_ptr.*, depth + 1));
        },
        .array => |list| for (list.items) |child| {
            result = @max(result, try maximumFieldId(child, depth + 1));
        },
        else => {},
    }
    return result;
}
fn validateIds(root: V, list: []const u8, key: []const u8) !void {
    var ids: std.AutoHashMapUnmanaged(i64, void) = .empty;
    defer ids.deinit(std.heap.page_allocator);
    for (try items(try get(root, list))) |entry| {
        const id = try int(try get(entry, key));
        if (id < 0) return error.InvalidLakeMetadata;
        const result = try ids.getOrPut(std.heap.page_allocator, id);
        if (result.found_existing) return error.InvalidLakeMetadata;
    }
}
fn fieldId(ids: *std.AutoHashMapUnmanaged(i64, void), value: V) !void {
    const id = try int(value);
    if (id <= 0 or (try ids.getOrPut(std.heap.page_allocator, id)).found_existing) return error.InvalidLakeMetadata;
}
fn boolean(value: V) !void {
    if (value != .bool) return error.InvalidLakeMetadata;
}
fn fieldType(ids: *std.AutoHashMapUnmanaged(i64, void), value: V, depth: usize) anyerror!void {
    if (depth > 64) return error.InvalidLakeMetadata;
    if (value == .string) {
        inline for (.{ "boolean", "int", "long", "float", "double", "date", "time", "timestamp", "timestamptz", "string", "uuid", "binary" }) |name| if (std.mem.eql(u8, value.string, name)) return;
        if (std.mem.startsWith(u8, value.string, "decimal(") or std.mem.startsWith(u8, value.string, "fixed[")) return;
        return error.InvalidLakeMetadata;
    }
    const kind = try str(try get(value, "type"));
    if (std.mem.eql(u8, kind, "struct")) {
        var names: std.StringHashMapUnmanaged(void) = .empty;
        defer names.deinit(std.heap.page_allocator);
        for (try items(try get(value, "fields"))) |field| {
            try fieldId(ids, try get(field, "id"));
            const name = try str(try get(field, "name"));
            if ((try names.getOrPut(std.heap.page_allocator, name)).found_existing) return error.InvalidLakeMetadata;
            try boolean(try get(field, "required"));
            try fieldType(ids, try get(field, "type"), depth + 1);
        }
    } else if (std.mem.eql(u8, kind, "list")) {
        try fieldId(ids, try get(value, "element-id"));
        try boolean(try get(value, "element-required"));
        try fieldType(ids, try get(value, "element"), depth + 1);
    } else if (std.mem.eql(u8, kind, "map")) {
        try fieldId(ids, try get(value, "key-id"));
        try fieldId(ids, try get(value, "value-id"));
        try boolean(try get(value, "value-required"));
        try fieldType(ids, try get(value, "key"), depth + 1);
        try fieldType(ids, try get(value, "value"), depth + 1);
    } else return error.InvalidLakeMetadata;
}

pub fn validate(root: V) !void {
    if (try int(try get(root, "format-version")) != 2) return error.UnsupportedLakeFormatVersion;
    _ = try str(try get(root, "table-uuid"));
    _ = try str(try get(root, "location"));
    if (try int(try get(root, "last-updated-ms")) < 0 or try int(try get(root, "last-sequence-number")) < 0) return error.InvalidLakeMetadata;
    try validateIds(root, "schemas", "schema-id");
    try validateIds(root, "partition-specs", "spec-id");
    try validateIds(root, "sort-orders", "order-id");
    try validateIds(root, "snapshots", "snapshot-id");
    if ((try find(root, "schemas", "schema-id", try int(try get(root, "current-schema-id")))) == null or
        (try find(root, "partition-specs", "spec-id", try int(try get(root, "default-spec-id")))) == null or
        (try find(root, "sort-orders", "order-id", try int(try get(root, "default-sort-order-id")))) == null) return error.InvalidLakeMetadata;
    const properties = try get(root, "properties");
    if (properties != .object) return error.InvalidLakeMetadata;
    var property_it = properties.object.iterator();
    while (property_it.next()) |entry| if (entry.value_ptr.* != .string) return error.InvalidLakeMetadata;
    if (properties.object.get("format-version")) |format| if (!std.mem.eql(u8, format.string, "2")) return error.UnsupportedLakeFormatVersion;
    for (try items(try get(root, "schemas"))) |schema| {
        var ids: std.AutoHashMapUnmanaged(i64, void) = .empty;
        defer ids.deinit(std.heap.page_allocator);
        try fieldType(&ids, schema, 0);
    }
    var max_field: i64 = 0;
    for (try items(try get(root, "schemas"))) |schema| max_field = @max(max_field, try maximumFieldId(try get(schema, "fields"), 0));
    if (try int(try get(root, "last-column-id")) < max_field) return error.InvalidLakeMetadata;
    var max_partition: i64 = 999;
    for (try items(try get(root, "partition-specs"))) |spec| for (try items(try get(spec, "fields"))) |field| {
        max_partition = @max(max_partition, try int(try get(field, "field-id")));
    };
    if (try int(try get(root, "last-partition-id")) < max_partition) return error.InvalidLakeMetadata;
    const last_seq = try int(try get(root, "last-sequence-number"));
    for (try items(try get(root, "snapshots"))) |snapshot| {
        _ = try str(try get(snapshot, "manifest-list"));
        if (try int(try get(snapshot, "timestamp-ms")) < 0 or try int(try get(snapshot, "sequence-number")) < 0 or try int(try get(snapshot, "sequence-number")) > last_seq) return error.InvalidLakeMetadata;
        if (snapshot.object.get("schema-id")) |id| if ((try find(root, "schemas", "schema-id", try int(id))) == null) return error.InvalidLakeMetadata;
    }
    const refs = try get(root, "refs");
    if (refs != .object) return error.InvalidLakeMetadata;
    var it = refs.object.iterator();
    while (it.next()) |entry| {
        const ref = entry.value_ptr.*;
        if ((try find(root, "snapshots", "snapshot-id", try int(try get(ref, "snapshot-id")))) == null) return error.InvalidLakeMetadata;
        const kind = try str(try get(ref, "type"));
        if (!std.mem.eql(u8, kind, "branch") and !std.mem.eql(u8, kind, "tag")) return error.InvalidLakeMetadata;
        if (std.mem.eql(u8, entry.key_ptr.*, "main") and !std.mem.eql(u8, kind, "branch")) return error.InvalidLakeMetadata;
    }
    const current = root.object.get("current-snapshot-id") orelse V.null;
    if (refs.object.get("main")) |main| {
        if (current == .null or try int(current) != try int(try get(main, "snapshot-id"))) return error.InvalidLakeMetadata;
    } else if (current != .null and try int(current) != -1) return error.InvalidLakeMetadata;
}

pub fn createAlloc(a: A, request_bytes: []const u8, uuid: []const u8, timestamp: i64, default_location: []const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const request = try parse(scratch, request_bytes);
    const schema = try get(request, "schema");
    const spec = request.object.get("partition-spec") orelse try parse(scratch, "{\"spec-id\":0,\"fields\":[]}");
    const sort = request.object.get("write-order") orelse try parse(scratch, "{\"order-id\":0,\"fields\":[]}");
    var root = object(scratch);
    try put(scratch, &root, "format-version", .{ .integer = 2 });
    try put(scratch, &root, "table-uuid", try string(scratch, uuid));
    try put(scratch, &root, "location", request.object.get("location") orelse try string(scratch, default_location));
    try put(scratch, &root, "last-updated-ms", .{ .integer = timestamp });
    try put(scratch, &root, "last-sequence-number", .{ .integer = 0 });
    try put(scratch, &root, "last-column-id", .{ .integer = try maximumFieldId(try get(schema, "fields"), 0) });
    try append(scratch, &root, "schemas", schema);
    try put(scratch, &root, "current-schema-id", try get(schema, "schema-id"));
    try append(scratch, &root, "partition-specs", spec);
    try put(scratch, &root, "default-spec-id", try get(spec, "spec-id"));
    var last_partition: i64 = 999;
    for (try items(try get(spec, "fields"))) |field| last_partition = @max(last_partition, try int(try get(field, "field-id")));
    try put(scratch, &root, "last-partition-id", .{ .integer = last_partition });
    try append(scratch, &root, "sort-orders", sort);
    try put(scratch, &root, "default-sort-order-id", try get(sort, "order-id"));
    try put(scratch, &root, "properties", request.object.get("properties") orelse object(scratch));
    try put(scratch, &root, "refs", object(scratch));
    try put(scratch, &root, "current-snapshot-id", .null);
    inline for (.{ "snapshots", "snapshot-log", "metadata-log" }) |key| try put(scratch, &root, key, array(scratch));
    try validate(root);
    return std.json.Stringify.valueAlloc(a, root, .{});
}

pub fn applyAlloc(a: A, metadata_bytes: []const u8, metadata_location: []const u8, commit: types.Commit) ![]u8 {
    try commit.validate();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var root = try parse(scratch, metadata_bytes);
    try validate(root);
    const envelope = try parse(scratch, commit.body);
    try requirements(scratch, root, envelope);
    var last_schema: ?i64 = null;
    var last_spec: ?i64 = null;
    var last_sort: ?i64 = null;
    const old_seq = try int(try get(root, "last-sequence-number"));
    const old_time = try int(try get(root, "last-updated-ms"));
    const updated_ms = @max(old_time, commit.timestamp_ms);
    for (try items(try get(envelope, "updates"))) |update| {
        const action = try str(try get(update, "action"));
        if (std.mem.eql(u8, action, "assign-uuid")) {
            if (!std.mem.eql(u8, try str(try get(update, "uuid")), try str(try get(root, "table-uuid")))) return error.InvalidLakeCommit;
        } else if (std.mem.eql(u8, action, "upgrade-format-version")) {
            if (try int(try get(update, "format-version")) != 2) return error.UnsupportedLakeFormatVersion;
        } else if (std.mem.eql(u8, action, "set-location")) {
            // Relocation needs its own credential/binding migration protocol.
            if (!std.mem.eql(u8, try str(try get(update, "location")), try str(try get(root, "location")))) return error.LakeRelocationRequired;
        } else if (std.mem.eql(u8, action, "add-schema")) {
            const schema = try get(update, "schema");
            last_schema = try int(try get(schema, "schema-id"));
            try appendUnique(scratch, &root, "schemas", "schema-id", schema);
            const max_id = @max(try int(try get(root, "last-column-id")), try maximumFieldId(try get(schema, "fields"), 0));
            if (update.object.get("last-column-id")) |id| if (try int(id) < max_id) return error.InvalidLakeCommit;
            try put(scratch, &root, "last-column-id", .{ .integer = max_id });
        } else if (std.mem.eql(u8, action, "add-spec")) {
            const spec = try get(update, "spec");
            last_spec = try int(try get(spec, "spec-id"));
            try appendUnique(scratch, &root, "partition-specs", "spec-id", spec);
            var max_id = try int(try get(root, "last-partition-id"));
            for (try items(try get(spec, "fields"))) |field| max_id = @max(max_id, try int(try get(field, "field-id")));
            try put(scratch, &root, "last-partition-id", .{ .integer = max_id });
        } else if (std.mem.eql(u8, action, "add-sort-order")) {
            const sort = try get(update, "sort-order");
            last_sort = try int(try get(sort, "order-id"));
            try appendUnique(scratch, &root, "sort-orders", "order-id", sort);
        } else if (std.mem.eql(u8, action, "set-current-schema")) {
            const id = try int(try get(update, "schema-id"));
            try put(scratch, &root, "current-schema-id", .{ .integer = if (id == -1) last_schema orelse return error.InvalidLakeCommit else id });
        } else if (std.mem.eql(u8, action, "set-default-spec")) {
            const id = try int(try get(update, "spec-id"));
            try put(scratch, &root, "default-spec-id", .{ .integer = if (id == -1) last_spec orelse return error.InvalidLakeCommit else id });
        } else if (std.mem.eql(u8, action, "set-default-sort-order")) {
            const id = try int(try get(update, "sort-order-id"));
            try put(scratch, &root, "default-sort-order-id", .{ .integer = if (id == -1) last_sort orelse return error.InvalidLakeCommit else id });
        } else if (std.mem.eql(u8, action, "add-snapshot")) {
            var snapshot = try get(update, "snapshot");
            const seq = try int(try get(snapshot, "sequence-number"));
            if (seq <= old_seq or seq <= try int(try get(root, "last-sequence-number"))) return error.InvalidLakeCommit;
            if (snapshot.object.get("parent-snapshot-id")) |id| if ((try find(root, "snapshots", "snapshot-id", try int(id))) == null) return error.InvalidLakeCommit;
            if (!snapshot.object.contains("schema-id")) try put(scratch, &snapshot, "schema-id", try get(root, "current-schema-id"));
            var summary = snapshot.object.get("summary") orelse object(scratch);
            try put(scratch, &summary, "antfly.commit-id", try string(scratch, commit.id));
            try put(scratch, &summary, "antfly.commit-hash", try string(scratch, &types.commitHash(commit)));
            try put(scratch, &snapshot, "summary", summary);
            try appendUnique(scratch, &root, "snapshots", "snapshot-id", snapshot);
            try put(scratch, &root, "last-sequence-number", .{ .integer = seq });
        } else if (std.mem.eql(u8, action, "set-snapshot-ref")) {
            const name = try str(try get(update, "ref-name"));
            var ref = update;
            _ = ref.object.swapRemove("action");
            _ = ref.object.swapRemove("ref-name");
            const refs = root.object.getPtr("refs").?;
            try put(scratch, refs, name, ref);
            if (std.mem.eql(u8, name, "main")) {
                try put(scratch, &root, "current-snapshot-id", try get(ref, "snapshot-id"));
                var log = object(scratch);
                try put(scratch, &log, "timestamp-ms", .{ .integer = commit.timestamp_ms });
                try put(scratch, &log, "snapshot-id", try get(ref, "snapshot-id"));
                try append(scratch, &root, "snapshot-log", log);
            }
        } else if (std.mem.eql(u8, action, "remove-snapshot-ref")) {
            const name = try str(try get(update, "ref-name"));
            if (std.mem.eql(u8, name, "main")) return error.InvalidLakeCommit;
            _ = root.object.getPtr("refs").?.object.swapRemove(name);
        } else if (std.mem.eql(u8, action, "remove-snapshots")) {
            const ids = try items(try get(update, "snapshot-ids"));
            try removeIds(&root, "snapshots", "snapshot-id", ids);
            try removeIds(&root, "snapshot-log", "snapshot-id", ids);
            inline for (.{ "statistics", "partition-statistics" }) |key| if (root.object.contains(key)) try removeIds(&root, key, "snapshot-id", ids);
        } else if (std.mem.eql(u8, action, "remove-schemas")) {
            try removeIds(&root, "schemas", "schema-id", try items(try get(update, "schema-ids")));
        } else if (std.mem.eql(u8, action, "remove-partition-specs")) {
            try removeIds(&root, "partition-specs", "spec-id", try items(try get(update, "spec-ids")));
        } else if (std.mem.eql(u8, action, "set-properties")) {
            const updates = try get(update, "updates");
            if (updates != .object) return error.InvalidLakeCommit;
            var it = updates.object.iterator();
            while (it.next()) |entry| {
                _ = try str(entry.value_ptr.*);
                if (std.mem.eql(u8, entry.key_ptr.*, "format-version")) return error.InvalidLakeCommit;
                try put(scratch, root.object.getPtr("properties").?, entry.key_ptr.*, entry.value_ptr.*);
            }
        } else if (std.mem.eql(u8, action, "remove-properties")) {
            for (try items(try get(update, "removals"))) |key| _ = root.object.getPtr("properties").?.object.swapRemove(try str(key));
        } else {
            var matched = false;
            inline for (.{ .{ "statistics", "statistics" }, .{ "partition-statistics", "partition-statistics" } }) |mapping| {
                if (std.mem.eql(u8, action, "set-" ++ mapping[0])) {
                    matched = true;
                    const stat = try get(update, mapping[1]);
                    const ids = [_]V{try get(stat, "snapshot-id")};
                    if (root.object.contains(mapping[0])) try removeIds(&root, mapping[0], "snapshot-id", &ids);
                    try append(scratch, &root, mapping[0], stat);
                } else if (std.mem.eql(u8, action, "remove-" ++ mapping[0])) {
                    matched = true;
                    const ids = [_]V{try get(update, "snapshot-id")};
                    if (root.object.contains(mapping[0])) try removeIds(&root, mapping[0], "snapshot-id", &ids);
                }
            }
            if (!matched) return error.UnsupportedLakeUpdate;
        }
    }
    try put(scratch, &root, "last-updated-ms", .{ .integer = updated_ms });
    var log = object(scratch);
    try put(scratch, &log, "timestamp-ms", .{ .integer = old_time });
    try put(scratch, &log, "metadata-file", try string(scratch, metadata_location));
    try append(scratch, &root, "metadata-log", log);
    try put(scratch, root.object.getPtr("properties").?, "antfly.commit-id", try string(scratch, commit.id));
    try put(scratch, root.object.getPtr("properties").?, "antfly.commit-hash", try string(scratch, &types.commitHash(commit)));
    try validate(root);
    const result = try std.json.Stringify.valueAlloc(a, root, .{});
    errdefer a.free(result);
    if (result.len > types.max_metadata_bytes) return error.LakeMetadataTooLarge;
    return result;
}
fn appendUnique(a: A, root: *V, list: []const u8, key: []const u8, value: V) !void {
    const id = try int(try get(value, key));
    if (try find(root.*, list, key, id)) |existing| {
        if (!try same(a, existing, value)) return error.InvalidLakeCommit;
        return;
    }
    try append(a, root, list, value);
}
fn removeIds(root: *V, list: []const u8, key: []const u8, ids: []const V) !void {
    const values = root.object.getPtr(list) orelse return;
    if (values.* != .array) return error.InvalidLakeMetadata;
    var kept: usize = 0;
    for (values.array.items) |entry| {
        const id = try int(try get(entry, key));
        var remove = false;
        for (ids) |target| if (try int(target) == id) {
            remove = true;
            break;
        };
        if (!remove) {
            values.array.items[kept] = entry;
            kept += 1;
        }
    }
    values.array.items.len = kept;
}
