// Copyright 2026 Antfly, Inc.
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
    _ = @import("sql/token.zig");
    _ = @import("sql/lexer.zig");
    _ = @import("sql/parser.zig");
    _ = @import("sql/generated_parser.zig");
    _ = @import("sql/tokenized.zig");
    _ = @import("sql/grammar.zig");
    _ = @import("sql/ddl_plan.zig");
    _ = @import("sql/plan.zig");
    _ = @import("sql/binder.zig");
    _ = @import("sql/lowering_context.zig");
    _ = @import("sql/lower_expr.zig");
    _ = @import("sql/lower_dml.zig");
    _ = @import("sql/document_plan.zig");
    _ = @import("sql/query_function.zig");
    _ = @import("sql/select_set.zig");
    _ = @import("sql/scalar_subquery_default.zig");
}
