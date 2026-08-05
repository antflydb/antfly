// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const host = @import("raft/host.zig");
const managed_host = @import("raft/managed_host.zig");
const catalog = @import("raft/storage/catalog.zig");

test {
    _ = host;
    _ = managed_host;
    _ = catalog;
}
