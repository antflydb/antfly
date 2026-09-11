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

//! ONNX file formats and tensor data, independent of graph conversion.
const std = @import("std");
const message = @import("protobuf").message;
pub const proto = @import("proto.zig");
pub const tensor = @import("tensor_data.zig");
pub const ModelProto = proto.ModelProto;
pub const GraphProto = proto.GraphProto;
pub const TensorProto = proto.TensorProto;
pub const DataType = proto.DataType;

pub fn serializeModel(allocator: std.mem.Allocator, model: *const ModelProto) ![]u8 {
    return message.encode(ModelProto, allocator, model);
}

test {
    std.testing.refAllDecls(@This());
}
