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

//! Exact private metadata keys allowed in native transaction intents.
const std = @import("std");

pub fn isRawMetadataKey(key: []const u8) bool {
    return @import("source_admission.zig").isKey(key) or
        @import("range_protection.zig").counterBucket(key) != null or
        @import("range_protection.zig").indexCounterDigest(key) != null or
        std.mem.startsWith(u8, key, "\x00\x00__metadata__:relational_integrity:") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:relational_integrity_activation") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:relational_integrity_retirement") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:relational_integrity_generation_retirement") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:restore_staging_owner") or
        @import("db/relational_index_maintenance_contract.zig").isControlKey(key);
}
