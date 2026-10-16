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
const source = @import("source.zig");
pub const Region = struct { start: usize, end: usize, p: f64, label: []const u8, label_line: usize };
pub const Evidence = struct {
    file_score: ?f64 = null,
    index: source.Index,
    regions: std.ArrayList(Region) = .empty,
    line_scores: []f64,
    complete: bool = true,
};
