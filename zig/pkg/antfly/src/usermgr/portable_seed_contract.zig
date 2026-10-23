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

pub const users_namespace = "usermgr_users";
pub const casbin_namespace = "usermgr_casbin";

// Capture validation and installation share the durable auth key vocabulary.
// User instance identities fence API keys across deletion and recreation.
pub fn validKey(namespace: []const u8, key: []const u8) bool {
    if (std.mem.eql(u8, namespace, users_namespace)) {
        return std.mem.startsWith(u8, key, "userpass:") or
            std.mem.startsWith(u8, key, "usermeta:") or
            std.mem.startsWith(u8, key, "userinstance:") or
            std.mem.startsWith(u8, key, "apikey:");
    }
    if (!std.mem.eql(u8, namespace, casbin_namespace)) return false;
    return std.mem.startsWith(u8, key, "p::") or
        std.mem.startsWith(u8, key, "p2::") or
        std.mem.startsWith(u8, key, "g::");
}
