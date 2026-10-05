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

const std = @import("std");
const index_mod = @import("../../index.zig");

pub const Status = index_mod.TypedDocValuesCoverageStatus;

pub fn statusName(status: Status) []const u8 {
    return switch (status) {
        .covered => "covered",
        .missing_doc_values_section => "missing_doc_values_section",
        .malformed_doc_values_section => "malformed_doc_values_section",
        .doc_values_kind_mismatch => "doc_values_kind_mismatch",
        .sparse_live_doc_values => "sparse_live_doc_values",
        .invalid_doc_value_doc_id => "invalid_doc_value_doc_id",
        .duplicate_doc_value_doc_id => "duplicate_doc_value_doc_id",
    };
}

test "typed doc values coverage status names are stable" {
    try std.testing.expectEqualStrings("covered", statusName(.covered));
    try std.testing.expectEqualStrings("missing_doc_values_section", statusName(.missing_doc_values_section));
    try std.testing.expectEqualStrings("malformed_doc_values_section", statusName(.malformed_doc_values_section));
    try std.testing.expectEqualStrings("doc_values_kind_mismatch", statusName(.doc_values_kind_mismatch));
    try std.testing.expectEqualStrings("sparse_live_doc_values", statusName(.sparse_live_doc_values));
    try std.testing.expectEqualStrings("invalid_doc_value_doc_id", statusName(.invalid_doc_value_doc_id));
    try std.testing.expectEqualStrings("duplicate_doc_value_doc_id", statusName(.duplicate_doc_value_doc_id));
}
