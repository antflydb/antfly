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

//! Observed dynamic-field capabilities shared across the API/storage boundary.

const std = @import("std");
const schema_mod = @import("../schema.zig");

pub const ObservedDynamicFieldCapabilitySet = struct {
    index_name: []u8,
    field_capabilities: []schema_mod.FieldCapability,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.index_name);
        schema_mod.freeOwnedFieldCapabilities(alloc, self.field_capabilities);
        self.* = undefined;
    }
};
