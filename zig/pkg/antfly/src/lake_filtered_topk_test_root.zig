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

//! Focused native filtering and tie-pagination qualification without HTTP routes.
pub const antfly_sources = @import("source_owner_physical.zig");
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
test {
    _ = @import("api/lake_index_ordered_rows.zig");
    _ = @import("api/lake_index_text_query.zig");
    _ = @import("api/lake_index_text_statistics.zig");
    _ = @import("api/lake_index_term_directory.zig");
    _ = @import("api/lake_index_serving_selection.zig");
    _ = @import("serverless/artifacts/retained_closure.zig");
    _ = @import("antfly_local_sources").sql_dynamic_filter;
    _ = @import("antfly_local_sources").search_search;
    _ = @import("antfly_local_sources").section_inverted;
    _ = @import("antfly_local_sources").sparse_sparse;
}
