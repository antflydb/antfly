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

pub const inventory = @embedFile("fixtures/sql_parity_inventory.json");
pub const catalog_campaign = @embedFile("fixtures/sql_catalog_campaign.json");
pub const dispositions = @embedFile("fixtures/sql_parity_dispositions.json");
pub const read_rows = @embedFile("fixtures/sql_read_reference_rows.json");
pub const read_reference = @embedFile("fixtures/sql_read_reference.json");
pub const mutation_campaign = @embedFile("fixtures/sql_mutation_campaign.json");
pub const mutation_reference = @embedFile("fixtures/sql_mutation_reference.json");
pub const mutation_postgres_reference = @embedFile("fixtures/sql_mutation_postgres_reference.json");
pub const unique_mutation_postgres_reference = @embedFile("fixtures/sql_unique_mutation_postgres_reference.json");
pub const partial_mutation_postgres_reference = @embedFile("fixtures/sql_partial_mutation_postgres_reference.json");
pub const lower_mutation_postgres_reference = @embedFile("fixtures/sql_lower_mutation_postgres_reference.json");
pub const mixed_mutation_postgres_reference = @embedFile("fixtures/sql_mixed_mutation_postgres_reference.json");
pub const upper_mutation_postgres_reference = @embedFile("fixtures/sql_upper_mutation_postgres_reference.json");
pub const correlated_mutation_postgres_reference = @embedFile("fixtures/sql_correlated_mutation_postgres_reference.json");
pub const conditional_subquery_reference = @embedFile("fixtures/sql_conditional_subquery_reference.json");
pub const document_campaign = @embedFile("fixtures/sql_document_campaign.json");
pub const document_reference = @embedFile("fixtures/sql_document_reference.json");
pub const read_campaign_reference = @embedFile("fixtures/sql_read_campaign_reference.json");
pub const typed_array_read_reference = @embedFile("fixtures/sql_typed_array_read_reference.json");
pub const aggregate_read_reference = @embedFile("fixtures/sql_aggregate_read_reference.json");
pub const joined_returning_reference = @embedFile("fixtures/sql_joined_returning_reference.json");
pub const joined_returning_subquery_reference = @embedFile("fixtures/sql_joined_returning_subquery_reference.json");
pub const lateral_campaign_reference = @embedFile("fixtures/sql_lateral_campaign_reference.json");
pub const array_expression_reference = @embedFile("fixtures/sql_array_expression_reference.json");
pub const json_exists_reference = @embedFile("fixtures/sql_json_exists_reference.json");
pub const set_spill_reference = @embedFile("fixtures/sql_set_spill_reference.json");
pub const set_read_reference = @embedFile("fixtures/sql_set_read_campaign_reference.json");

const std = @import("std");

/// Owned exact-source cases. Tests look up stable IDs rather than copying SQL
/// into a second, potentially drifting fixture. No disposition is inferred.
pub const Corpus = struct {
    pub const Case = struct {
        id: []const u8,
        name: []const u8,
        family: []const u8,
        sql: []const u8,
        params: []const std.json.Value,
        source_expectation: []const u8,
    };
    const Document = struct { entries: []const Case };
    parsed: std.json.Parsed(Document),

    pub fn init(alloc: std.mem.Allocator) !Corpus {
        var parsed = try std.json.parseFromSlice(Document, alloc, inventory, .{ .ignore_unknown_fields = true, .parse_numbers = false });
        errdefer parsed.deinit();
        if (parsed.value.entries.len != 1586) return error.InvalidParityInventory;
        for (parsed.value.entries, 1..) |entry, index| {
            if (try ordinal(entry.id) != index) return error.InvalidParityInventory;
        }
        return .{ .parsed = parsed };
    }

    pub fn deinit(self: *Corpus) void {
        self.parsed.deinit();
        self.* = undefined;
    }

    pub fn get(self: *const Corpus, id: []const u8) !*const Case {
        const index = try ordinal(id);
        if (index == 0 or index > self.parsed.value.entries.len) return error.UnknownParityCase;
        return &self.parsed.value.entries[index - 1];
    }

    /// The original corpus stores tagged internal values, while public HTTP
    /// accepts JSON values. Preserve logical types without copying legacy wire
    /// envelopes into the new API or rounding integer tokens through f64.
    pub fn logicalParameters(alloc: std.mem.Allocator, case: *const Case) ![]const std.json.Value {
        const values = try alloc.alloc(std.json.Value, case.params.len);
        for (case.params, values) |parameter, *out| {
            if (parameter != .object or parameter.object.count() != 1) return error.InvalidParityParameters;
            var it = parameter.object.iterator();
            const entry = it.next().?;
            const kind = entry.key_ptr.*;
            const value = entry.value_ptr.*;
            if (std.mem.eql(u8, kind, "string") and value == .string) {
                out.* = value;
            } else if (std.mem.eql(u8, kind, "integer") and value == .number_string) {
                out.* = .{ .integer = try std.fmt.parseInt(i64, value.number_string, 10) };
            } else if (std.mem.eql(u8, kind, "json") and value == .string) {
                out.* = try std.json.parseFromSliceLeaky(std.json.Value, alloc, value.string, .{});
            } else return error.InvalidParityParameters;
        }
        return values;
    }

    fn ordinal(id: []const u8) !usize {
        if (id.len != 8 or !std.mem.startsWith(u8, id, "sql-")) return error.UnknownParityCase;
        for (id[4..]) |digit| if (digit < '0' or digit > '9') return error.UnknownParityCase;
        return std.fmt.parseInt(usize, id[4..], 10);
    }
};
