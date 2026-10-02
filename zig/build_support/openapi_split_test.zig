// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const public = @import("antfly_public_openapi");
const public_server = @import("antfly_public_server_openapi");
const metadata = @import("antfly_metadata_openapi");
const metadata_server = @import("antfly_metadata_server_openapi");
const usermgr = @import("antfly_usermgr_openapi");
const usermgr_server = @import("antfly_usermgr_server_openapi");

test "server parsers use the same schema types as embedded code" {
    const public_result = @TypeOf(public_server.server.parseQueryBuilderAgentBody(undefined, undefined));
    try std.testing.expect(@typeInfo(public_result).error_union.payload == std.json.Parsed(public.QueryBuilderRequest));
    try std.testing.expect(!@hasDecl(public, "server"));
    try std.testing.expect(!@hasDecl(metadata, "server"));
    try std.testing.expect(!@hasDecl(usermgr, "server"));
    const metadata_result = @TypeOf(metadata_server.server.parseQueryTableBody(undefined, undefined));
    const usermgr_result = @TypeOf(usermgr_server.server.parseCreateUserBody(undefined, undefined));
    try std.testing.expect(@typeInfo(metadata_result).error_union.payload == std.json.Parsed(metadata.StatefulQueryRequest));
    try std.testing.expect(@typeInfo(usermgr_result).error_union.payload == std.json.Parsed(usermgr.CreateUserRequest));
}
