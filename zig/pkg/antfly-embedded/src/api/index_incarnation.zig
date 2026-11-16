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

/// Pure decoder for the persisted catalog identity. Keep this independent of
/// entropy/runtime ownership so query binding and storage use one contract.
pub fn get(value: std.json.Value) ?u64 {
    if (value != .object) return null;
    const raw = value.object.get("_index_incarnation") orelse value.object.get("_coverage_incarnation") orelse return null;
    if (raw != .integer or raw.integer <= 0) return null;
    return @intCast(raw.integer);
}
