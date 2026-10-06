// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

test {
    _ = @import("common/http/mod.zig");
    _ = @import("common/http/io_http_executor.zig");
    _ = @import("common/http/std_http_executor.zig");
    _ = @import("antfly_local_sources").common_http_std_http_listener;
    _ = @import("common/runtime_lifecycle.zig");
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
