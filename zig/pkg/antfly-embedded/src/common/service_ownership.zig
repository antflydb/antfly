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

//! Catalog-persisted ownership of native service storage. Public transports
//! cannot construct the request-local service capability.
pub const Owner = enum { memory };
pub fn requireAccess(owner: ?Owner, capability: ?Owner) !void {
    if (owner != capability) return error.Forbidden;
}

/// Preserve ordinary catalog encodings for peers without native services.
/// Owned records require the service-capable admission floor before creation.
pub fn jsonStringify(value: anytype, writer: anytype) !void {
    try writer.beginObject();
    inline for (@typeInfo(@TypeOf(value)).@"struct".field_names) |field| {
        if (!@import("std").mem.eql(u8, field, "native_service") or value.native_service != null) {
            try writer.objectField(field);
            try writer.write(@field(value, field));
        }
    }
    try writer.endObject();
}
