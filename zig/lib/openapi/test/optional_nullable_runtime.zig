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
}
