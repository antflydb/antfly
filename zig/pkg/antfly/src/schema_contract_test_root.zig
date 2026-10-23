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

//! Schema parser and API validation contracts without unrelated runtime suites.
const fixture = @import("api_http_runtime_test_root.zig");
pub const storage_backend_erased = fixture.storage_backend_erased;
pub const lsm_backend = fixture.lsm_backend;
pub const antfly_sources = fixture.antfly_sources;
pub const local_test_sources = fixture.local_test_sources;
test {
    _ = fixture;
    _ = @import("antfly_local_sources").schema_table_schema_impl;
}
