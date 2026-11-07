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

//! Facade for the shared native index metadata and storage-name contract.
const shared = @import("../system_catalog/index_metadata.zig");
pub const isReservedEntry = shared.isReservedEntry;
pub const isOpaqueEntry = shared.isOpaqueEntry;
pub const isValidIndexName = shared.isValidIndexName;

test "privacy index API facade delegates opacity and naming to catalog owner" {
    const testing = @import("std").testing;
    inline for (.{ "_privacy", "_resolver_record_store", "_curation_proposals", "indexes", "status_fts" }) |name| {
        try testing.expectEqual(shared.isOpaqueEntry(name), isOpaqueEntry(name));
        try testing.expectEqual(shared.isReservedEntry(name), isReservedEntry(name));
        try testing.expectEqual(shared.isValidIndexName(name), isValidIndexName(name));
    }
    try testing.expect(isOpaqueEntry("_privacy"));
}
