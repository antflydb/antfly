// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Bounded change planning over an authenticated manifest. Only touched inline
//! roots enter the transform interpreter. Immutable payloads are read once, at
//! the explicit compatibility materialization boundary, never during planning.
const std = @import("std");
const payload = @import("../document_payload.zig");
const projection = @import("../document_projection.zig");
const transform = @import("transform.zig");
const types = @import("types.zig");
const A = std.mem.Allocator;

pub const Plan = struct {
    before: @import("../document_mutation_revision.zig").Content,
    before_manifest_digest: [32]u8,
    patch: payload.MetadataPatch,

    pub fn deinit(self: Plan, a: A) void {
        self.patch.deinit(a);
    }

    /// Ownership of the patch transfers only after successful materialization.
    /// The caller owns expected_json separately, as for other prepared writes.
    pub fn materialize(self: Plan, a: A, reader: anytype, key: []const u8) !payload.PreparedWrite {
        var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
        defer arena.deinit();
        const manifest = try payload.Manifest.decode(arena.allocator(), self.patch.manifest);
        const bytes = try manifest.readOwned(a, reader, key);
        return .{ .before = self.before, .before_manifest_digest = self.before_manifest_digest, .patch = self.patch, .expected_json = bytes };
    }
};

/// A null result asks the ordinary complete-source path to handle the operation.
/// Errors remain atomic: no store writes, blob fetches or external callbacks are
/// performed by this function. The source must come from the pinned read view.
pub fn prepare(a: A, source: payload.SourceMetadata, operations: []const types.TransformOp) !?Plan {
    return prepareAllocating(a, source, operations) catch |err| switch (err) {
        error.WriteFailed => error.OutOfMemory,
        else => err,
    };
}
fn prepareAllocating(a: A, source: payload.SourceMetadata, operations: []const types.TransformOp) !?Plan {
    if (operations.len == 0 or operations.len > 128) return null;
    var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
    defer arena.deinit();
    const temp = arena.allocator();
    var names: std.ArrayList([]const u8) = .empty;
    var operand_bytes: usize = 0;
    for (operations) |op| {
        // Rename has two dependency paths. Time-dependent operations retain the
        // original interpreter's per-transform evaluation boundary.
        if (op.op == .rename or op.op == .current_date) return null;
        const name = try transform.rootPath(op.path);
        if (name.len == 0 or name[0] == '_') return null;
        if (op.value_json) |value| operand_bytes = std.math.add(usize, operand_bytes, value.len) catch return null;
        if (operand_bytes > 32 * 1024) return null;
        const found = for (names.items) |prior| {
            if (std.mem.eql(u8, prior, name)) break true;
        } else false;
        if (!found) try names.append(temp, name);
    }
    const old = try payload.Manifest.decode(temp, source.manifest);
    const directory = (try projection.Directory.open(temp, source.directory, source.proof)) orelse return null;
    const roots = try temp.dupe(projection.Field, directory.fields[0..directory.root_count]);
    std.mem.sort(projection.Field, roots, {}, struct {
        fn less(_: void, left: projection.Field, right: projection.Field) bool {
            return left.offset < right.offset;
        }
    }.less);
    var partial: std.Io.Writer.Allocating = .init(temp);
    try partial.writer.writeByte('{');
    var count: usize = 0;
    var touched_bytes: usize = 0;
    for (roots) |field| {
        const touched = for (names.items) |name| {
            if (std.mem.eql(u8, name, field.name)) break true;
        } else false;
        if (!touched) continue;
        touched_bytes = std.math.add(usize, touched_bytes, @intCast(field.length)) catch return null;
        if (touched_bytes > 32 * 1024) return null;
        var position: usize = 0;
        for (old.parts) |part| {
            const end = position + part.length();
            if (part == .blob and field.offset < end and field.offset + field.length > position) return null;
            position = end;
        }
        // The reader intentionally cannot fetch a blob even if the overlap guard
        // above regresses. Inline ranges alone are a sufficient capability here.
        const InlineOnly = struct {
            pub fn getPhysical(_: *@This(), _: []const u8) ![]const u8 {
                return error.UnexpectedPayloadRead;
            }
        };
        var reader: InlineOnly = .{};
        const raw = try old.readRange(temp, &reader, "", @intCast(field.offset), @intCast(field.length));
        if (count != 0) try partial.writer.writeByte(',');
        try std.json.Stringify.value(field.name, .{}, &partial.writer);
        try partial.writer.writeByte(':');
        try partial.writer.writeAll(raw);
        count += 1;
    }
    try partial.writer.writeByte('}');
    const resolved = (try transform.resolveDocumentTransform(temp, partial.written(), .{ .key = "", .operations = operations })).?;
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, temp, resolved, .{ .allocate = .alloc_always });
    var updates: std.ArrayList(payload.MetadataUpdate) = .empty;
    for (names.items) |name| {
        const value = parsed.object.get(name);
        try updates.append(temp, .{ .name = name, .value_json = if (value) |v| try std.json.Stringify.valueAlloc(temp, v, .{}) else null });
    }
    var order: std.ArrayList([]const u8) = .empty;
    for (roots) |field| {
        const removed = for (updates.items) |update| {
            if (std.mem.eql(u8, field.name, update.name)) break update.value_json == null;
        } else false;
        if (!removed) try order.append(temp, field.name);
    }
    for (parsed.object.keys()) |name| {
        const existing = for (roots) |field| {
            if (std.mem.eql(u8, field.name, name)) break true;
        } else false;
        if (!existing) try order.append(temp, name);
    }
    const patch = payload.prepareMetadataPatchOrdered(a, source.manifest, source.proof, source.directory, source.certificate, updates.items, order.items) catch |err| switch (err) {
        error.Unsupported => return null,
        else => return err,
    };
    return .{ .before = source.proof, .before_manifest_digest = old.physical_commitment, .patch = patch };
}

const Fixture = struct {
    body: []u8,
    json: []u8,
    source: payload.SourceMetadata,
    fn init(a: A) !Fixture {
        const logical = @import("../document_commitment.zig");
        const identity = @import("../source_value_identity.zig");
        const keys = @import("../internal_keys.zig");
        const body = try a.alloc(u8, payload.minimum_value_bytes);
        errdefer a.free(body);
        @memset(body, 'x');
        body[0] = '"';
        body[body.len - 1] = '"';
        const json = try std.fmt.allocPrint(a, "{{\"body\":{s},\"metadata\":{{\"count\":1,\"tags\":[\"a\",\"b\"]}},\"removed\":true}}", .{body});
        errdefer a.free(json);
        var root = try logical.build(a, json);
        defer root.deinit(a);
        const record = try logical.encode(a, root);
        defer a.free(record);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
        const manifest = try payload.encodeLogical(a, &.{ .{ .inline_json = "{\"body\":" }, .{ .blob = .{ .length = body.len, .digest = digest } }, .{ .inline_json = json[8 + body.len ..] } }, root.digest, record);
        errdefer a.free(manifest);
        const directory = (try projection.build(a, json, keys.primary_kind, root.digest)).?;
        errdefer a.free(directory);
        const certificate = (try identity.build(a, json, keys.primary_kind, root.digest, directory)).?;
        return .{ .body = body, .json = json, .source = .{ .manifest = manifest, .directory = directory, .certificate = certificate, .proof = .{ .revision = 1, .bytes = json.len, .kind = keys.primary_kind, .digest = root.digest, .manifest = true, .algorithm = .logical_json_v1 } } };
    }
    fn deinit(self: Fixture, a: A) void {
        self.source.deinit(a);
        a.free(self.json);
        a.free(self.body);
    }
};

pub fn testNativeChangePlans() !void {
    const a = std.testing.allocator;
    const fixture = try Fixture.init(a);
    defer fixture.deinit(a);
    const operations = [_]types.TransformOp{
        .{ .op = .inc, .path = "$.metadata.count", .value_json = "2" },
        .{ .op = .pull, .path = "metadata.tags", .value_json = "\"a\"" },
        .{ .op = .add_to_set, .path = "metadata.tags", .value_json = "\"c\"" },
        .{ .op = .unset, .path = "removed" },
        .{ .op = .set, .path = "added.name", .value_json = "\"Ada\"" },
        .{ .op = .set_on_insert, .path = "ignored", .value_json = "1" },
        .{ .op = .min, .path = "metadata.count", .value_json = "2" },
        .{ .op = .max, .path = "metadata.count", .value_json = "4" },
        .{ .op = .unset, .path = "metadata" },
        .{ .op = .set, .path = "metadata", .value_json = "false" },
    };
    const Reader = struct {
        body: []const u8,
        calls: usize = 0,
        pub fn getPhysical(self: *@This(), _: []const u8) ![]const u8 {
            self.calls += 1;
            return self.body;
        }
    };
    const Case = struct {
        fn run(alloc: A, input: Fixture, ops: []const types.TransformOp) !void {
            const plan = (try prepare(alloc, input.source, ops)).?;
            var transferred = false;
            defer if (!transferred) plan.deinit(alloc);
            var reader: Reader = .{ .body = input.body };
            // Planning has no reader capability at all.
            try std.testing.expectEqual(@as(usize, 0), reader.calls);
            const change = try plan.materialize(alloc, &reader, "doc:d");
            transferred = true;
            defer change.deinit(alloc);
            defer alloc.free(change.expected_json);
            try std.testing.expectEqual(@as(usize, 1), reader.calls);
            const expected = (try transform.resolveDocumentTransform(alloc, input.json, .{ .key = "doc", .operations = ops })).?;
            defer alloc.free(expected);
            const logical = @import("../document_commitment.zig");
            const want = try logical.compute(alloc, expected);
            const actual = try logical.compute(alloc, change.expected_json);
            try std.testing.expectEqual(want, actual);
            try std.testing.expectEqual(change.patch.proof.digest, actual);
        }
    };
    for (1..operations.len + 1) |end| try Case.run(a, fixture, operations[0..end]);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Case.run, .{ fixture, &operations });
    try std.testing.expect((try prepare(a, fixture.source, &.{.{ .op = .set, .path = "body", .value_json = "null" }})) == null);
    try std.testing.expect((try prepare(a, fixture.source, &.{.{ .op = .rename, .path = "metadata", .value_json = "\"other\"" }})) == null);
    try std.testing.expectError(error.InvalidArgument, prepare(a, fixture.source, &.{.{ .op = .inc, .path = "metadata", .value_json = "\"bad\"" }}));
}

/// Compare incremental validation with the ordinary complete-source validator.
/// Fixture ownership and the expensive old body stay outside each change plan.
pub fn testNativeRootValidation() !void {
    const a = std.testing.allocator;
    const fixture = try Fixture.init(a);
    defer fixture.deinit(a);
    const schema_json =
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","minProperties":2,"required":["body"],"dependentRequired":{"removed":["metadata"]},"propertyNames":{"pattern":"^(body|metadata|removed)$"},"properties":{"body":{"type":"string","minLength":65534},"metadata":{"type":"object","required":["count","tags"],"properties":{"count":{"type":"integer","minimum":0},"tags":{"type":"array","items":{"type":"string"}}},"additionalProperties":false},"removed":{"type":"boolean"}},"additionalProperties":false}}}}
    ;
    var compiled = try @import("../../schema/mod.zig").CompiledTableValidator.init(a, schema_json);
    defer compiled.deinit(a);
    try compiled.validateWrites(a, &.{.{ .value = fixture.json }});
    try std.testing.expect(compiled.supportsRootValidationReuse());
    const cases = [_]types.TransformOp{
        .{ .op = .set, .path = "metadata.count", .value_json = "-1" },
        .{ .op = .set, .path = "metadata.count", .value_json = "2" },
        .{ .op = .unset, .path = "metadata.count" },
        .{ .op = .push, .path = "metadata.tags", .value_json = "2" },
        .{ .op = .push, .path = "metadata.tags", .value_json = "\"c\"" },
        .{ .op = .unset, .path = "metadata" },
        .{ .op = .unset, .path = "removed" },
        .{ .op = .set, .path = "unknown", .value_json = "true" },
        .{ .op = .set, .path = "metadata", .value_json = "\"wrong type\"" },
    };
    for (cases) |operation| {
        const plan = (try prepare(a, fixture.source, &.{operation})).?;
        defer plan.deinit(a);
        const Fake = struct {
            body: []const u8,
            pub fn getPhysical(self: *@This(), _: []const u8) ![]const u8 {
                return self.body;
            }
        };
        var reader: Fake = .{ .body = fixture.body };
        const after = try plan.materialize(a, &reader, "row");
        defer a.free(after.expected_json);
        var expected: ?anyerror = null;
        compiled.validateWrites(a, &.{.{ .value = after.expected_json }}) catch |err| {
            expected = err;
        };
        var bytes: [32 * 1024]u8 = undefined;
        var bounded = std.heap.FixedBufferAllocator.init(&bytes);
        var actual: ?anyerror = null;
        const reused = @import("document_validation_preparation.zig").validateChanged(bounded.allocator(), compiled, fixture.source, after) catch |err| blk: {
            actual = err;
            break :blk true;
        };
        try std.testing.expect(reused);
        try std.testing.expectEqual(expected, actual);
    }
    // Rules observing root values jointly must retain whole-source validation.
    const global_rules = [_][]const u8{
        \\{"if":{"required":["removed"]},"then":{"required":["metadata"]}}
        ,
        \\{"dependentSchemas":{"removed":{"required":["metadata"]}}}
        ,
        \\{"anyOf":[{"required":["metadata"]},{"required":["removed"]}]}
        ,
        \\{"allOf":[{"required":["metadata"]}]}
        ,
        \\{"oneOf":[{"required":["metadata"]},{"required":["removed"]}]}
        ,
        \\{"not":{"required":["unknown"]}}
        ,
    };
    for (global_rules) |rule| {
        const json = try std.fmt.allocPrint(a, "{{\"version\":1,\"default_type\":\"doc\",\"document_schemas\":{{\"doc\":{{\"schema\":{s}}}}}}}", .{rule});
        defer a.free(json);
        var global = try @import("../../schema/mod.zig").CompiledTableValidator.init(a, json);
        defer global.deinit(a);
        try std.testing.expect(!global.supportsRootValidationReuse());
    }
}
