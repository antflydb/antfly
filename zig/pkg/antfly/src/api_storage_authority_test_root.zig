// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the ELv2 at https://www.antfly.io/licensing/ELv2-license.

const cluster_api_http = @import("api/cluster_api_http.zig");
const public_table_http = @import("api/public_table_http.zig");

test {
    _ = cluster_api_http;
    _ = public_table_http;
}
