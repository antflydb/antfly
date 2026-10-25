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

test {
    _ = @import("sql/test_root.zig");
    _ = @import("sql/subquery_shape_test.zig");
    _ = @import("sql/joined_mutation_test.zig");
    _ = @import("antfly_local_sources").system_catalog_policies;
    _ = @import("antfly_local_sources").storage_db_row_policy_bundle;
    _ = @import("antfly_local_sources").storage_db_table_catalog;
    _ = @import("antfly_local_sources").system_catalog_domain;
    // Keep SQL schema lowering and native uniqueness/conflict admission in
    // one qualification gate; the build selects the cross-layer contract.
    _ = @import("antfly_local_sources").api_relational_integrity;
    _ = @import("antfly_local_sources").api_relational_integrity_commit;
    // Preparation owns the schema compiler as well as SQL declaration
    // lowering. Discover its explicit cross-layer ownership contracts here.
    _ = @import("antfly_local_sources").schema_mod;
    _ = @import("antfly_local_sources").storage_schema;
    _ = @import("antfly_local_sources").schema_relational_expression;
    _ = @import("antfly_local_sources").storage_db_relational_nullability;
    _ = @import("antfly_local_sources").storage_db_relational_predicate;
    _ = @import("antfly_local_sources").storage_db_relational_predicate_implication;
    _ = @import("antfly_local_sources").storage_db_relational_index_predicate;
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
