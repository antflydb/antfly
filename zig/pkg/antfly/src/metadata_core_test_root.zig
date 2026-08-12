// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const state = @import("metadata/state.zig");
const runtime = @import("metadata/runtime.zig");
const authority = @import("metadata/authority.zig");
const incarnation = @import("metadata/incarnation.zig");
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
