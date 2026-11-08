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

//! Shared text-query wire contract for HTTP, embedded bindings and SQL hosts.
const std = @import("std");
const compiler = @import("compiler.zig");
const runtime = @import("runtime.zig");
const values = @import("value.zig");
const catalog = @import("catalog.zig");
const backend = @import("backend.zig");
const Wire = struct { query: []const u8, parameters: ?std.json.Value = null, typed_parameters: ?std.json.Value = null, write_request_id: ?[]const u8 = null, limits: ?runtime.Limits = null };
pub const Request = struct {
    arena: std.heap.ArenaAllocator,
    query: []const u8,
    parameters: []const values.Entry,
    limits: runtime.Limits,
    write_request_id: ?[]const u8 = null,
    pub fn deinit(self: *Request) void {
        self.arena.deinit();
        self.* = undefined;
    }
    /// Clients can tighten each service ceiling, never increase it. Query
    /// text and parameters are request-owned and never enter durable catalog.
    pub fn parse(a: std.mem.Allocator, json: []const u8, ceiling: runtime.Limits) !Request {
        if (json.len > 1 << 20) return error.CypherRequestTooLarge;
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const wire = try std.json.parseFromSliceLeaky(Wire, owned, json, .{ .allocate = .alloc_always, .parse_numbers = false });
        if (wire.write_request_id) |id| {
            if (id.len != 32) return error.InvalidCypherWriteRequestId;
            for (id) |byte| if (!std.ascii.isHex(byte)) return error.InvalidCypherWriteRequestId;
        }
        if (wire.parameters != null and wire.typed_parameters != null) return error.InvalidCypherParameters;
        const parameters = if (wire.typed_parameters) |input| try @import("wire.zig").parameter(owned, input) else if (wire.parameters) |input| try values.Value.fromJson(owned, input) else values.Value{ .map = &.{} };
        if (parameters != .map) return error.InvalidCypherParameters;
        const requested = wire.limits orelse ceiling;
        var limits: runtime.Limits = undefined;
        inline for (@typeInfo(runtime.Limits).@"struct".field_names) |name| {
            @field(limits, name) = @min(@field(requested, name), @field(ceiling, name));
        }
        return .{ .arena = arena, .query = wire.query, .parameters = parameters.map, .limits = limits, .write_request_id = wire.write_request_id };
    }
    pub fn execute(self: Request, a: std.mem.Allocator, provider: backend.Backend, graph: catalog.Graph, diagnostic: *compiler.Diagnostic) !runtime.Result {
        var compiled = try compiler.compile(a, self.query, .{}, diagnostic);
        defer compiled.deinit();
        return runtime.execute(a, provider, graph, &compiled, self.parameters, self.limits);
    }
};

test "graph transport owns parameters and clamps client budgets to service ceilings" {
    var request = try Request.parse(std.testing.allocator, "{\"query\":\"RETURN $name\",\"parameters\":{\"name\":\"Ada\"},\"limits\":{\"memory_bytes\":10000000,\"explored_edges\":2}}", .{ .memory_bytes = 4096, .explored_edges = 100 });
    defer request.deinit();
    try std.testing.expectEqual(@as(usize, 4096), request.limits.memory_bytes);
    try std.testing.expectEqual(@as(usize, 2), request.limits.explored_edges);
    try std.testing.expectEqualStrings("Ada", request.parameters[0].value.string);
    try std.testing.expectError(error.InvalidCypherParameters, Request.parse(std.testing.allocator, "{\"query\":\"RETURN 1\",\"parameters\":[]}", .{}));
    try std.testing.expectError(error.UnknownField, Request.parse(std.testing.allocator, "{\"query\":\"RETURN 1\",\"_native_cut\":{}}", .{}));
}
