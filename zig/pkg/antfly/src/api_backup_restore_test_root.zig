// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the ELv2 at https://www.antfly.io/licensing/ELv2-license.

const api_e2e = @import("api/e2e.zig");
const backups = @import("api/backups.zig");
const http_server = @import("api/http_server.zig");
const db = @import("storage/db/db.zig");

test {
    _ = api_e2e;
    _ = backups;
    _ = http_server;
    _ = db;
}
