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
pub const role_defaults_reference = @embedFile("fixtures/sql_role_defaults_reference.json");
pub const generated_original_reference = @embedFile("fixtures/sql_generated_original_reference.json");
pub const catalog_campaign = @embedFile("fixtures/sql_catalog_campaign.json");
pub const dispositions = @embedFile("fixtures/sql_parity_dispositions.json");
pub const read_rows = @embedFile("fixtures/sql_read_reference_rows.json");
pub const read_reference = @embedFile("fixtures/sql_read_reference.json");
pub const view_read_reference = @embedFile("fixtures/sql_view_read_reference.json");
pub const mutation_campaign = @embedFile("fixtures/sql_mutation_campaign.json");
pub const mutation_reference = @embedFile("fixtures/sql_mutation_reference.json");
pub const mutation_postgres_reference = @embedFile("fixtures/sql_mutation_postgres_reference.json");
pub const array_mutation_reference = @embedFile("fixtures/sql_array_mutation_reference.json");
pub const mutation_rejection_reference = @embedFile("fixtures/sql_mutation_rejection_reference.json");
pub const unique_mutation_postgres_reference = @embedFile("fixtures/sql_unique_mutation_postgres_reference.json");
pub const partial_mutation_postgres_reference = @embedFile("fixtures/sql_partial_mutation_postgres_reference.json");
pub const lower_mutation_postgres_reference = @embedFile("fixtures/sql_lower_mutation_postgres_reference.json");
pub const mixed_mutation_postgres_reference = @embedFile("fixtures/sql_mixed_mutation_postgres_reference.json");
pub const upper_mutation_postgres_reference = @embedFile("fixtures/sql_upper_mutation_postgres_reference.json");
pub const correlated_mutation_postgres_reference = @embedFile("fixtures/sql_correlated_mutation_postgres_reference.json");
pub const conditional_subquery_reference = @embedFile("fixtures/sql_conditional_subquery_reference.json");
pub const document_campaign = @embedFile("fixtures/sql_document_campaign.json");
pub const document_patch_reference = @embedFile("fixtures/sql_document_patch_reference.json");
pub const document_envelope_reference = @embedFile("fixtures/sql_document_envelope_reference.json");
pub const document_truncate_reference = @embedFile("fixtures/sql_document_truncate_reference.json");
pub const document_identity_reference = @embedFile("fixtures/sql_document_identity_reference.json");
pub const source_mutation_reference = @embedFile("fixtures/sql_source_mutation_reference.json");
pub const prepared_source_reference = @embedFile("fixtures/sql_prepared_source_reference.json");
pub const table_comment_reference = @embedFile("fixtures/sql_table_comment_reference.json");
pub const resource_comment_reference = @embedFile("fixtures/sql_resource_comment_reference.json");
pub const column_comment_reference = @embedFile("fixtures/sql_column_comment_reference.json");
pub const identity_reference = @embedFile("fixtures/sql_identity_reference.json");
pub const sequence_declaration_reference = @embedFile("fixtures/sql_sequence_declaration_reference.json");
pub const document_boundary_reference = @embedFile("fixtures/sql_document_boundary_reference.json");
pub const document_root_extension_reference = @embedFile("fixtures/sql_document_root_extension_reference.json");
pub const document_reference = @embedFile("fixtures/sql_document_reference.json");
pub const document_read_reference = @embedFile("fixtures/sql_document_read_reference.json");
pub const schema_lifecycle_reference = @embedFile("fixtures/sql_schema_lifecycle_reference.json");
pub const table_contract_reference = @embedFile("fixtures/sql_table_contract_reference.json");
pub const policy_lifecycle_reference = @embedFile("fixtures/sql_policy_lifecycle_reference.json");
pub const extended_read_reference = @embedFile("fixtures/sql_extended_read_reference.json");
pub const remaining_read_reference = @embedFile("fixtures/sql_remaining_read_reference.json");
pub const session_control_reference = @embedFile("fixtures/sql_session_control_reference.json");
pub const binding_rejection_reference = @embedFile("fixtures/sql_binding_rejection_reference.json");
pub const document_numeric_rejection_reference = @embedFile("fixtures/sql_document_numeric_rejection_reference.json");
pub const indexed_mutation_lifecycle_reference = @embedFile("fixtures/sql_indexed_mutation_lifecycle_reference.json");
pub const named_conflict_reference = @embedFile("fixtures/sql_named_conflict_reference.json");
pub const users_read_reference = @embedFile("fixtures/sql_users_read_reference.json");
pub const document_alias_reference = @embedFile("fixtures/sql_document_alias_reference.json");
pub const document_projection_reference = @embedFile("fixtures/sql_document_projection_reference.json");
pub const read_campaign_reference = @embedFile("fixtures/sql_read_campaign_reference.json");
pub const read_campaign_profile = @embedFile("fixtures/sql_read_campaign_profile.json");
pub const name_resolution_campaign = @embedFile("fixtures/sql_name_resolution_campaign.json");
pub const digest_read_reference = @embedFile("fixtures/sql_digest_read_campaign_reference.json");
pub const typed_array_read_reference = @embedFile("fixtures/sql_typed_array_read_reference.json");
pub const aggregate_read_reference = @embedFile("fixtures/sql_aggregate_read_reference.json");
pub const join_read_reference = @embedFile("fixtures/sql_join_read_reference.json");
pub const text_read_reference = @embedFile("fixtures/sql_text_read_reference.json");
pub const numeric_window_reference = @embedFile("fixtures/sql_numeric_window_reference.json");
pub const array_expansion_reference = @embedFile("fixtures/sql_array_expansion_reference.json");
pub const joined_returning_reference = @embedFile("fixtures/sql_joined_returning_reference.json");
pub const joined_returning_subquery_reference = @embedFile("fixtures/sql_joined_returning_subquery_reference.json");
pub const lateral_campaign_reference = @embedFile("fixtures/sql_lateral_campaign_reference.json");
pub const array_expression_reference = @embedFile("fixtures/sql_array_expression_reference.json");
pub const scalar_kernel_reference = @embedFile("fixtures/sql_scalar_kernel_reference.json");
pub const json_exists_reference = @embedFile("fixtures/sql_json_exists_reference.json");
pub const set_spill_reference = @embedFile("fixtures/sql_set_spill_reference.json");
pub const set_read_reference = @embedFile("fixtures/sql_set_read_campaign_reference.json");

pub const constraint_timing_reference = @embedFile("fixtures/sql_constraint_timing_reference.json");
pub const transaction_characteristics_reference = @embedFile("fixtures/sql_transaction_characteristics_reference.json");
pub const tablespace_lifecycle_reference = @embedFile("fixtures/sql_tablespace_lifecycle_reference.json");

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

pub const fk_not_valid_reference = @embedFile("fixtures/sql_fk_not_valid_reference.json");

pub const temporal_arithmetic_reference = @embedFile("fixtures/sql_temporal_arithmetic_reference.json");

pub const range_reference = @embedFile("fixtures/sql_range_reference.json");
pub const temporal_key_reference = @embedFile("fixtures/sql_temporal_key_reference.json");

pub const fk_validation_reference = @embedFile("fixtures/sql_fk_validation_reference.json");

pub const document_generated_reference = @embedFile("fixtures/sql_document_generated_reference.json");

pub const temporal_bin_reference = @embedFile("fixtures/sql_temporal_bin_reference.json");

pub const view_domain_reference = @embedFile("fixtures/sql_view_domain_reference.json");

pub const collection_reference = @embedFile("fixtures/sql_collection_reference.json");
