// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
/// Configuration is a server capability, never a credential supplied by the
/// request. Transport authentication cannot grant user execution authority.
pub const Key = struct { secret: []const u8, issuer: []const u8 };
/// Dedicated preparation authority never toggles public authentication or
/// admits an externally supplied principal. Explicit trusted-principal keys
/// remain a compatible configured fallback for existing clustered deployments.
pub fn configured(cfg: anytype) !Key {
    const Config = @TypeOf(cfg);
    if (comptime @hasField(Config, "sql_preparation_secret") and @hasField(Config, "sql_preparation_issuer")) {
        if (cfg.sql_preparation_secret != null or cfg.sql_preparation_issuer != null) return checked(cfg.sql_preparation_secret, cfg.sql_preparation_issuer);
    }
    return checked(cfg.trusted_principal_secret, cfg.trusted_principal_issuer);
}
fn checked(secret: ?[]const u8, issuer: ?[]const u8) !Key {
    const key = secret orelse return error.SqlPreparationAuthorityNotConfigured;
    const identity = issuer orelse return error.SqlPreparationAuthorityNotConfigured;
    if (key.len == 0 or key.len > 4096 or identity.len == 0 or identity.len > 256 or std.mem.indexOfScalar(u8, identity, 0) != null or !std.unicode.utf8ValidateSlice(identity)) return error.SqlPreparationAuthorityNotConfigured;
    return .{ .secret = key, .issuer = identity };
}
pub fn requireConfiguration(cfg: anytype) !void {
    _ = try configured(cfg);
}
