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

//! Small deterministic projections of public metadata; no independent schema
//! or capability inference engine. Field capabilities are server authoritative.
const std = @import("std");
const client_mod = @import("antfly-client");
const cli = @import("mod.zig");
const Value = std.json.Value;

pub fn capabilities(a: std.mem.Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    cli.rejectRemainingArgs(args, "capabilities");
    cli.output.context = .{ .scope = .discovery, .execution = .complete };
    try cli.writeJson(a, io, .{
        .agent_contract = @as(u32, 1),
        .commands = .{ "schema", "table", "index", "sql", "query", "lookup" },
        .sql = .{ .parameters = "typed positional JSON", .limit = "admission; overflow fails", .explain = "sql --explain --statement <SQL>", .explain_analyze = "read-only SELECT; sql --analyze" },
        .search = .{ .scope = "requested_top_k", .total_relations = .{ "eq", "gte" }, .projection = "--fields" },
        .display = .{ .unit = "whole rows or documents", .limits = cli.agent.Limits{}, .retrieve = "--output json", .mutation_receipts = "never sampled" },
        .execution = .{ .progress = "measured elapsed time and available query profiles", .deadline = "--deadline-ms (per server request)", .sql_work = .{ "--max-scan-rows", "--memory-bytes", "--spill-bytes" }, .estimates = "sql --estimate; metadata-based logical work units; unknowns explicit", .fingerprints = "--fingerprint ordered|multiset; complete received response; no cross-request snapshot claim" },
        .server_features = "Use schema --table NAME for authoritative field and index capabilities.",
    });
}

const Column = struct { document_type: []const u8, name: []const u8, type: Value };
const Index = struct { name: []const u8, type: []const u8 };
const Table = struct {
    database: []const u8,
    namespace: []const u8,
    name: []const u8,
    columns: []const Column,
    indexes: []const Index,
    field_capabilities: ?[]const client_mod.types.FieldCapability,
};

fn concise(a: std.mem.Allocator, client: *client_mod.AntflyClient, input: client_mod.types.TableStatus) !Table {
    var columns: std.ArrayList(Column) = .empty;
    if (input.schema) |table_schema| if (table_schema.document_schemas) |schemas| {
        for (schemas.map.keys(), schemas.map.values()) |document_type, document| {
            const json_schema = document.schema orelse continue;
            const properties = json_schema.map.get("properties") orelse continue;
            if (properties != .object) continue;
            for (properties.object.keys(), properties.object.values()) |name, definition| {
                if (definition != .object) continue;
                try columns.append(a, .{ .document_type = document_type, .name = name, .type = definition.object.get("type") orelse .null });
            }
        }
    };
    var indexes: std.ArrayList(Index) = .empty;
    // CreatedIndex is a generated union. Project only public name and type,
    // never provider configuration, credentials or model prompts.
    for (input.indexes.map.keys(), input.indexes.map.values()) |name, index| {
        const raw = try std.json.Stringify.valueAlloc(a, index, .{});
        const parsed = try std.json.parseFromSlice(Value, a, raw, .{});
        const kind = if (parsed.value == .object) parsed.value.object.get("type") else null;
        try indexes.append(a, .{ .name = name, .type = if (kind != null and kind.? == .string) kind.?.string else "unknown" });
    }
    // Metadata order is not a stable discovery contract.
    std.mem.sort(Column, columns.items, {}, struct {
        fn less(_: void, lhs: Column, rhs: Column) bool {
            const order = std.mem.order(u8, lhs.document_type, rhs.document_type);
            return if (order == .eq) std.mem.lessThan(u8, lhs.name, rhs.name) else order == .lt;
        }
    }.less);
    std.mem.sort(Index, indexes.items, {}, struct {
        fn less(_: void, lhs: Index, rhs: Index) bool {
            return std.mem.lessThan(u8, lhs.name, rhs.name);
        }
    }.less);
    return .{
        .database = if (client.catalog_scope) |scope| scope.database else "default",
        .namespace = if (client.catalog_scope) |scope| scope.namespace else "public",
        .name = input.name,
        .columns = columns.items,
        .indexes = indexes.items,
        .field_capabilities = input.field_capabilities,
    };
}

pub fn schema(allocator: std.mem.Allocator, io: std.Io, client: *client_mod.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table: ?[]const u8 = null;
    while (args.next()) |flag| {
        if (!std.mem.eql(u8, flag, "--table")) return error.UnknownSchemaOption;
        cli.takeUniqueValue(args, &table, flag);
    }
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var result: std.ArrayList(Table) = .empty;
    if (table) |name| {
        var response = try client.getTable(name);
        defer response.deinit();
        cli.expectHttpSuccess(response);
        const data = response.data orelse return error.InvalidApiResponse;
        try result.append(a, try concise(a, client, data.value));
        cli.output.context = .{ .scope = .discovery, .execution = .complete };
        // Serialize while the typed response still owns field capability strings.
        try cli.writeJson(allocator, io, result.items);
        return;
    }
    var response = try client.listTables();
    defer response.deinit();
    cli.expectHttpSuccess(response);
    const data = response.data orelse return error.InvalidApiResponse;
    for (data.value) |value| try result.append(a, try concise(a, client, value));
    std.mem.sort(Table, result.items, {}, struct {
        fn less(_: void, lhs: Table, rhs: Table) bool {
            return std.mem.lessThan(u8, lhs.name, rhs.name);
        }
    }.less);
    cli.output.context = .{ .scope = .discovery, .execution = .complete };
    try cli.writeJson(allocator, io, result.items);
}

test "discovery projection qualifies names and sorts JSON schema columns" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const metadata = try std.json.parseFromSlice(client_mod.types.TableStatus, a,
        \\{"name":"dots.are.literal","shards":{},"indexes":{},"storage_status":{},"schema":{"document_schemas":{"doc":{"schema":{"properties":{"z":{"type":"integer"},"a":{"type":"string"}}}}}}}
    , .{});
    var http = client_mod.httpx.Client.initWithConfig(a, std.testing.io, .{});
    defer http.deinit();
    var client = try client_mod.AntflyClient.init(a, &http, "http://localhost");
    defer client.deinit();
    client.catalog_scope = .{ .database = "tenant", .namespace = "app" };
    const value = try concise(a, &client, metadata.value);
    try std.testing.expectEqualStrings("tenant", value.database);
    try std.testing.expectEqualStrings("dots.are.literal", value.name);
    try std.testing.expectEqualStrings("a", value.columns[0].name);
}
