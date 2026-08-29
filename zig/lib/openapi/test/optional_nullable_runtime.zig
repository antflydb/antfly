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
const antfly_json = @import("antfly-json");
const types = @import("types");

test "optional nullable properties round-trip all three wire states" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const alloc = arena_state.allocator();

    const absent = try std.json.parseFromSliceLeaky(
        types.Pet,
        alloc,
        \\{"id":1,"name":"Mochi"}
    ,
        .{},
    );
    try std.testing.expect(absent.tag == .absent);
    const absent_json = try std.json.Stringify.valueAlloc(alloc, absent, .{});
    try std.testing.expect(std.mem.indexOf(u8, absent_json, "\"tag\"") == null);

    const explicit_null = try std.json.parseFromSliceLeaky(
        types.Pet,
        alloc,
        \\{"id":1,"name":"Mochi","tag":null}
    ,
        .{},
    );
    try std.testing.expect(explicit_null.tag == .null_value);
    const null_json = try std.json.Stringify.valueAlloc(alloc, explicit_null, .{});
    try std.testing.expect(std.mem.indexOf(u8, null_json, "\"tag\":null") != null);

    const concrete = try std.json.parseFromSliceLeaky(
        types.Pet,
        alloc,
        \\{"id":1,"name":"Mochi","tag":"cat"}
    ,
        .{},
    );
    try std.testing.expectEqualStrings("cat", concrete.tag.valueOrNull().?);
    const value_json = try std.json.Stringify.valueAlloc(alloc, concrete, .{});
    try std.testing.expect(std.mem.indexOf(u8, value_json, "\"tag\":\"cat\"") != null);

    const value_tree = try std.json.parseFromSliceLeaky(
        std.json.Value,
        alloc,
        \\{"id":1,"name":"Mochi","tag":"cat"}
    ,
        .{},
    );
    const concrete_from_value = try std.json.parseFromValueLeaky(types.Pet, alloc, value_tree, .{});
    try std.testing.expectEqualStrings("cat", concrete_from_value.tag.valueOrNull().?);

    const null_tree = try std.json.parseFromSliceLeaky(
        std.json.Value,
        alloc,
        \\{"id":1,"name":"Mochi","tag":null}
    ,
        .{},
    );
    const null_from_value = try std.json.parseFromValueLeaky(types.Pet, alloc, null_tree, .{});
    try std.testing.expect(null_from_value.tag == .null_value);
}

test "optional non-nullable properties reject explicit null without losing omission ergonomics" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const alloc = arena_state.allocator();

    const omitted = try std.json.parseFromSliceLeaky(
        types.Pet,
        alloc,
        \\{"id":1,"name":"Mochi"}
    ,
        .{},
    );
    try std.testing.expect(omitted.status == null);
    try std.testing.expect(omitted.metadata == null);

    // Generated serializers preserve the schema's absence/null distinction
    // even when the caller requests null emission for ordinary Zig optionals.
    const omitted_json = try std.json.Stringify.valueAlloc(
        alloc,
        omitted,
        .{ .emit_null_optional_fields = true },
    );
    try std.testing.expect(std.mem.indexOf(u8, omitted_json, "\"status\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, omitted_json, "\"metadata\"") == null);

    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSliceLeaky(
        types.Pet,
        alloc,
        \\{"id":1,"name":"Mochi","status":null}
    ,
        .{},
    ));
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSliceLeaky(
        types.Pet,
        alloc,
        \\{"id":1,"name":"Mochi","metadata":null}
    ,
        .{},
    ));

    const null_tree = try std.json.parseFromSliceLeaky(
        std.json.Value,
        alloc,
        \\{"id":1,"name":"Mochi","status":null}
    ,
        .{},
    );
    try std.testing.expectError(
        error.UnexpectedToken,
        std.json.parseFromValueLeaky(types.Pet, alloc, null_tree, .{}),
    );
}

test "required nullable presence remains distinct from optional non-nullable omission" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const alloc = arena_state.allocator();

    try std.testing.expectError(error.MissingField, std.json.parseFromSliceLeaky(
        types.MixedPresence,
        alloc,
        \\{}
    ,
        .{},
    ));

    const explicit_null = try std.json.parseFromSliceLeaky(
        types.MixedPresence,
        alloc,
        \\{"required_nullable":null}
    ,
        .{},
    );
    try std.testing.expect(explicit_null.required_nullable == null);
    try std.testing.expect(explicit_null.optional_non_nullable == null);

    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSliceLeaky(
        types.MixedPresence,
        alloc,
        \\{"required_nullable":null,"optional_non_nullable":null}
    ,
        .{},
    ));
}

test "required nullable fields require wire presence on every parser backend" {
    const alloc = std.testing.allocator;
    const missing =
        \\{"error":"boom","message":"failed"}
    ;

    try std.testing.expectError(
        error.MissingField,
        std.json.parseFromSlice(types.Error, alloc, missing, .{}),
    );
    try std.testing.expectError(
        error.MissingField,
        antfly_json.parseFromSliceWithConfig(
            types.Error,
            alloc,
            missing,
            .{},
            .{ .preferred_backend = .simd, .simd_min_input_len = 0 },
        ),
    );

    const automatic_input = missing ++ (" " ** 256);
    try std.testing.expectError(
        error.MissingField,
        antfly_json.parseFromSlice(types.Error, alloc, automatic_input, .{}),
    );

    var explicit_null = try antfly_json.parseFromSliceWithConfig(
        types.Error,
        alloc,
        \\{"error":"boom","message":"failed","details":null}
    ,
        .{},
        .{ .preferred_backend = .simd, .simd_min_input_len = 0 },
    );
    defer explicit_null.deinit();
    try std.testing.expect(explicit_null.value.details == null);
}

test "OpenAPI wire names remain distinct from ergonomic Zig field names" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const alloc = arena_state.allocator();

    const parsed = try std.json.parseFromSliceLeaky(
        types.Pet,
        alloc,
        \\{"id":1,"name":"Mochi","audit.event":"created","type":"cat"}
    ,
        .{},
    );
    try std.testing.expectEqualStrings("created", parsed.audit_event.?);
    try std.testing.expectEqualStrings("cat", parsed.type.?);

    const tree = try std.json.parseFromSliceLeaky(
        std.json.Value,
        alloc,
        \\{"id":1,"name":"Mochi","audit.event":"updated"}
    ,
        .{},
    );
    const from_value = try std.json.parseFromValueLeaky(types.Pet, alloc, tree, .{});
    try std.testing.expectEqualStrings("updated", from_value.audit_event.?);

    const encoded = try std.json.Stringify.valueAlloc(alloc, from_value, .{});
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"audit.event\":\"updated\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"audit_event\"") == null);

    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSliceLeaky(
        types.Pet,
        alloc,
        \\{"id":1,"name":"Mochi","audit.event":null}
    ,
        .{},
    ));
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSliceLeaky(
        types.Pet,
        alloc,
        \\{"id":1,"name":"Mochi","type":null}
    ,
        .{},
    ));
}
