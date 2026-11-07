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

//! Canonical physical dependencies, available only to source session backends.
//! Keep private session implementation files out of this shared namespace.
pub const mapper = @import("db/document_mapper.zig");
pub const row_policy_gate = @import("db/row_policy_gate.zig");
pub const table_catalog = @import("db/table_catalog.zig");
pub const store = @import("docstore.zig");
pub const schema_registry = @import("db/schema_registry.zig");
pub const internal = @import("internal_keys.zig");
pub const ttl = @import("ttl.zig");
pub const graph = @import("db/query/graph_exec.zig");
pub const document = @import("../sql/document_row.zig");
pub const catalog = @import("../sql/catalog.zig");
pub const scalar = @import("../sql/scalar.zig");
pub const typed_json = @import("typed_json.zig");
pub const platform = @import("antfly_platform");
pub const schema = @import("schema.zig");
pub const index_catalog = @import("db/relational_index_catalog.zig");
pub const index_plan = @import("db/relational_index_plan.zig");
pub const index_records = @import("db/relational_index_records.zig");
pub const index_jobs = @import("db/relational_index_jobs.zig");
pub const index_keys = @import("db/relational_index_keys.zig");
pub const row_codec = @import("db/algebraic/relational_row_codec.zig");
pub const range_state = @import("db/range_state.zig");
pub const predicates = @import("db/relational_predicate.zig");
pub const row_cursor = @import("db/relational_row_cursor.zig");
pub const sql_builtin_type = @import("../common/sql_builtin_type.zig");
pub const sql_memory_budget = @import("../sql/memory_budget.zig");
pub const sql_operators = @import("../sql/operators.zig");
pub const index_cover = @import("db/relational_index_cover.zig");

pub const document_revision = @import("document_mutation_revision.zig");
pub const backend_erased = @import("backend_erased.zig");
pub const document_projection = @import("document_projection.zig");
pub const value_ranges = @import("value_ranges.zig");

pub const snapshot_admission = @import("db/snapshot_admission.zig");

pub const privacy_read = @import("db/privacy_read.zig");
