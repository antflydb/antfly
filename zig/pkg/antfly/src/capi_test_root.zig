// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Server integration coverage uses the public local C API with server fixtures.
pub const antfly_sources = @import("source_owner_storage.zig");
pub const local_test_sources = @import("local_test_sources.zig");
pub const runtime_impl = @import("antfly_local_sources").capi_embedded_root;
pub const capi = @import("antfly_local_sources").capi_db;
test {
    _ = capi;
    _ = @import("capi/db_test.zig");
}
