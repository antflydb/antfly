// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const service = @import("metadata/service.zig");
const http_client = @import("metadata/http_client.zig");
const http_routes = @import("metadata/http_routes.zig");
const http_server = @import("metadata/http_server.zig");

test {
    _ = service;
    _ = http_client;
    _ = http_routes;
    _ = http_server;
}
