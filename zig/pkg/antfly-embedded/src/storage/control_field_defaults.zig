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

//! Semantic defaults for an independently validated isolated batch control.
//! This predicate grants no control authority and never classifies row effects.
const std = @import("std");

pub fn isolatedFieldMatchesDefault(comptime field_name: []const u8, value: anytype, expected: @TypeOf(value)) bool {
    if (comptime std.mem.eql(u8, field_name, "document_version_scheme")) {
        // Local ingress and untagged replay are neutral for a control with no
        // document effects. A revision envelope remains an explicit nondefault.
        return switch (value) {
            .legacy, .admit_current => true,
            .revision_v1 => false,
        };
    }
    return std.meta.eql(value, expected);
}
