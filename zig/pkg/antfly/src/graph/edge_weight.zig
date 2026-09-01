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

/// Durable graph edges have one storage domain independent of the path mode
/// that may later consume them. Mode-specific algorithms can narrow that domain
/// further (for example max_weight requires [0, 1]).
pub fn validateStored(weight: f64) !void {
    if (!isStoredValid(weight)) return error.InvalidGraphEdges;
}

pub fn isStoredValid(weight: f64) bool {
    return std.math.isFinite(weight) and weight >= 0.0;
}

test "stored graph weights are finite and non-negative" {
    try validateStored(0.0);
    try validateStored(std.math.floatMax(f64));
    try std.testing.expectError(error.InvalidGraphEdges, validateStored(-0.1));
    try std.testing.expectError(error.InvalidGraphEdges, validateStored(std.math.inf(f64)));
    try std.testing.expectError(error.InvalidGraphEdges, validateStored(std.math.nan(f64)));
}
