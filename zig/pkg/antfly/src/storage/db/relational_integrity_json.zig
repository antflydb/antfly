// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at https://www.antfly.io/licensing/ELv2-license.

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
