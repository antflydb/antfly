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

//! Focused storage-owned resolver replay regressions.
pub const antfly_sources = @import("source_owner_storage.zig");
test {
    _ = @import("storage/db/runtime_callbacks.zig");
    _ = @import("storage/db/resolver_curation.zig");
    _ = @import("storage/db/resolution_runtime.zig");
    _ = @import("storage/db/db.zig");
}
