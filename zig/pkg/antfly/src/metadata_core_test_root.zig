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

const state = @import("metadata/state.zig");
const runtime = @import("metadata/runtime.zig");
const authority = @import("metadata/authority.zig");
const incarnation = @import("antfly_local_sources").metadata_incarnation;
const reconcile_lease = @import("metadata/reconcile_lease.zig");
const store_observer = @import("metadata/store_observer.zig");

test {
    _ = state;
    _ = runtime;
    _ = authority;
    _ = incarnation;
    _ = reconcile_lease;
    _ = store_observer;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
