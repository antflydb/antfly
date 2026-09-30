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

//! Binary-safe native command JSON. Only integrity metadata uses byte arrays;
//! ordinary primary JSON documents keep their existing compact representation.
const std = @import("std");

pub fn write(value: anytype, stream: anytype) @TypeOf(stream.*).Error!void {
    const T = @TypeOf(value);
    // std.json.Value is a semantic JSON tree. Its object representation owns
    // hash-map pointers, which are not part of the wire format; delegate that
    // union to std.json's value encoder instead of recursively visiting its
    // implementation fields as if they were integrity command identities.
    if (comptime T == std.json.Value) return stream.write(value);
    switch (@typeInfo(T)) {
        .@"struct" => |info| {
            if (@hasDecl(T, "nativeJsonProjection")) return write(value.nativeJsonProjection(), stream);
            try stream.beginObject();
            inline for (info.fields) |field| {
                if (!@hasDecl(T, "nativeJsonSkipField") or !value.nativeJsonSkipField(field.name)) {
                    try stream.objectField(field.name);
                    try write(@field(value, field.name), stream);
                }
            }
            try stream.endObject();
        },
        .@"union" => |info| {
            if (info.tag_type == null) @compileError("integrity commands require tagged unions");
            try stream.beginObject();
            switch (value) {
                inline else => |payload, tag| {
                    try stream.objectField(@tagName(tag));
                    try write(payload, stream);
                },
            }
            try stream.endObject();
        },
        .pointer => |info| {
            if (info.size != .slice) @compileError("integrity commands cannot serialize pointer identities");
            try stream.beginArray();
            for (value) |element| try write(element, stream);
            try stream.endArray();
        },
        .array => {
            try stream.beginArray();
            for (value) |element| try write(element, stream);
            try stream.endArray();
        },
        .optional => if (value) |payload| try write(payload, stream) else try stream.write(null),
        .void => {
            // std.json's canonical tagged-union representation for void.
            try stream.beginObject();
            try stream.endObject();
        },
        .@"enum" => try stream.write(@tagName(value)),
        else => try stream.write(value),
    }
}
