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

//! ABI-compatible unavailable provider for the explicit embedded storage profile.
const std = @import("std");
const bridge = @import("antfly_inference_bridge");
const engine = @import("antfly_inference_engine");

fn unavailable(comptime R: type) R {
    if (R == void) {
        return;
    } else if (R == u8) {
        return 0;
    } else if (R == bridge.Status) {
        return bridge.statusFromError(error.UnsupportedOperation);
    } else {
        return .{ .failure = .init("UnsupportedOperation") };
    }
}
fn stub(comptime Pointer: type) Pointer {
    const info = @typeInfo(@typeInfo(Pointer).pointer.child).@"fn";
    const R = info.return_type.?;
    const cc = info.attrs.@"callconv";
    return switch (info.param_types.len) {
        1 => struct {
            fn call(_: info.param_types[0].?) callconv(cc) R {
                return unavailable(R);
            }
        }.call,
        2 => struct {
            fn call(_: info.param_types[0].?, _: info.param_types[1].?) callconv(cc) R {
                return unavailable(R);
            }
        }.call,
        3 => struct {
            fn call(_: info.param_types[0].?, _: info.param_types[1].?, _: info.param_types[2].?) callconv(cc) R {
                return unavailable(R);
            }
        }.call,
        4 => struct {
            fn call(_: info.param_types[0].?, _: info.param_types[1].?, _: info.param_types[2].?, _: info.param_types[3].?) callconv(cc) R {
                return unavailable(R);
            }
        }.call,
        5 => struct {
            fn call(_: info.param_types[0].?, _: info.param_types[1].?, _: info.param_types[2].?, _: info.param_types[3].?, _: info.param_types[4].?) callconv(cc) R {
                return unavailable(R);
            }
        }.call,
        6 => struct {
            fn call(_: info.param_types[0].?, _: info.param_types[1].?, _: info.param_types[2].?, _: info.param_types[3].?, _: info.param_types[4].?, _: info.param_types[5].?) callconv(cc) R {
                return unavailable(R);
            }
        }.call,
        7 => struct {
            fn call(_: info.param_types[0].?, _: info.param_types[1].?, _: info.param_types[2].?, _: info.param_types[3].?, _: info.param_types[4].?, _: info.param_types[5].?, _: info.param_types[6].?) callconv(cc) R {
                return unavailable(R);
            }
        }.call,
        8 => struct {
            fn call(_: info.param_types[0].?, _: info.param_types[1].?, _: info.param_types[2].?, _: info.param_types[3].?, _: info.param_types[4].?, _: info.param_types[5].?, _: info.param_types[6].?, _: info.param_types[7].?) callconv(cc) R {
                return unavailable(R);
            }
        }.call,
        else => @compileError("unsupported unavailable-provider signature"),
    };
}
const table: engine.VTable = blk: {
    var value: engine.VTable = undefined;
    for (@typeInfo(engine.VTable).@"struct".field_names, @typeInfo(engine.VTable).@"struct".field_types) |name, T| @field(value, name) = stub(T);
    break :blk value;
};
const descriptor: engine.Descriptor = .{ .layout = engine.layout, .dispatch = engine.Boundary.local_dispatch, .table = &table };
fn getEngine() callconv(.c) *const engine.Descriptor {
    return &descriptor;
}
const functions: bridge.FunctionTable = blk: {
    var value: bridge.FunctionTable = undefined;
    for (@typeInfo(bridge.FunctionTable).@"struct".field_names, @typeInfo(bridge.FunctionTable).@"struct".field_types) |name, T| {
        if (std.mem.eql(u8, name, "abi_version")) @field(value, name) = bridge.abi_version else if (std.mem.eql(u8, name, "struct_size")) @field(value, name) = @sizeOf(bridge.FunctionTable) else if (std.mem.eql(u8, name, "capabilities")) @field(value, name) = 0 else if (@typeInfo(T) == .optional) @field(value, name) = null else @field(value, name) = stub(T);
    }
    break :blk value;
};
fn getHost() callconv(.c) *const bridge.FunctionTable {
    return &functions;
}
comptime {
    @export(&getHost, .{ .name = "antfly_standalone_inference_get_function_table", .visibility = .hidden });
    @export(&getEngine, .{ .name = "antfly_inference_engine_get_function_table", .visibility = .hidden });
}
