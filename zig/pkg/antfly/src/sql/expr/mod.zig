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

pub const aggregate = @import("aggregate.zig");
pub const build = @import("build.zig");
pub const condition = @import("condition.zig");
pub const disjoint = @import("disjoint.zig");
pub const equal = @import("equal.zig");
pub const generated = @import("generated.zig");
pub const generated_validate = @import("generated_validate.zig");
pub const json_path = @import("json_path.zig");
pub const limits = @import("limits.zig");
pub const operator = @import("operator.zig");
pub const order = @import("order.zig");
pub const parse = @import("parse.zig");
pub const predicate = @import("predicate.zig");
pub const projection = @import("projection.zig");
pub const row_parse = @import("row_parse.zig");
pub const selector = @import("selector.zig");
pub const text = @import("text.zig");
pub const token = @import("token.zig");
pub const typing = @import("type.zig");
pub const where_condition = @import("where_condition.zig");
pub const window = @import("window.zig");

test {
    _ = aggregate;
    _ = condition;
    _ = generated_validate;
    _ = order;
    _ = projection;
    _ = row_parse;
    _ = selector;
    _ = text;
    _ = where_condition;
    _ = window;

    try generated_validate.testGeneratedValidationChecksPredicateAndRowExpressionIdentity();
    try order.testOrderNullPlacementAndModifiers();
    try order.testDistinctOnOrderValidation();
    try projection.testProjectionBuildsOwnedDefaultOutputs();
    try projection.testProjectionParsesFieldHelpers();
    try projection.testProjectionPeeksSimpleReturningFields();
    try row_parse.testRowParseResolvesFunctionBindings();
}
