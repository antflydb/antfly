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

//! Binary-safe native command JSON. Only integrity metadata uses byte arrays;
//! ordinary primary JSON documents keep their existing compact representation.
const std = @import("std");

pub fn write(value: anytype, stream: anytype) @TypeOf(stream.*).Error!void {
    const T = @TypeOf(value);
    switch (@typeInfo(T)) {
        .@"struct" => |info| {
            try stream.beginObject();
            inline for (info.fields) |field| {
                try stream.objectField(field.name);
                try write(@field(value, field.name), stream);
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
