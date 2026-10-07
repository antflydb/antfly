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
