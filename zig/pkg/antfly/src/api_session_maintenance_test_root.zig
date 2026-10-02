// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the ELv2 at https://www.antfly.io/licensing/ELv2-license.

const http_server = @import("api/http_server.zig");
const transactions = @import("api/transactions.zig");

test {
    _ = http_server;
    _ = transactions;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
