// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

test {
    _ = @import("parity_case_test.zig");
    _ = @import("window_test.zig");
    _ = @import("subquery_test.zig");
    _ = @import("recursive_test.zig");
    _ = @import("merge_test.zig");
    _ = @import("antfly_local_sources").sql_aggregate_binding;
    _ = @import("antfly_local_sources").sql_compiler;
    _ = @import("antfly_local_sources").sql_scalar;
    _ = @import("antfly_local_sources").sql_array_value;
    _ = @import("antfly_local_sources").sql_array_binary;
    _ = @import("antfly_local_sources").sql_decision_eval;
    _ = @import("antfly_local_sources").sql_describe;
    _ = @import("antfly_local_sources").sql_runtime;
    _ = @import("insert_test.zig");
    _ = @import("returning_test.zig");
    _ = @import("conflict_test.zig");
    _ = @import("antfly_local_sources").sql_catalog;
    _ = @import("antfly_local_sources").sql_document_row;
    _ = @import("antfly_local_sources").sql_read_stream;
    _ = @import("antfly_local_sources").sql_operators;
    _ = @import("plan_cache.zig");
    _ = @import("antfly_local_sources").sql_session;
    _ = @import("antfly_local_sources").sql_setting_catalog;
}
