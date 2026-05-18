// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
pub const openapi = @import("antfly_client_openapi");
pub const httpx = @import("httpx");

pub const AntflyClient = @import("client.zig").AntflyClient;
pub const ApiError = @import("client.zig").ApiError;

/// Re-export generated types for convenience.
pub const types = openapi.types;

test "antfly client pkg compiles" {
    _ = AntflyClient;
    _ = ApiError;
    _ = types;
    _ = @import("client.zig");
}
