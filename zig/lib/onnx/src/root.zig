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

// ONNX model parser and graph converter for termite.
//
// Parses ONNX protobuf models and converts them into termite's native
// Graph IR, enabling execution on any termite backend (BLAS, MLX,
// PJRT) without ONNX Runtime.
//
// Usage:
//   const onnx = @import("onnx");
//   var model = try onnx.parse(allocator, onnx_bytes);
//   defer model.deinit();
//   var result = try model.convertToGraph(allocator);
//   defer result.deinit(allocator);
//   // result.graph is a termite Graph ready for execution

pub const proto = @import("proto.zig");
pub const attrs = @import("attrs.zig");
pub const tensor = @import("tensor.zig");
pub const ops = @import("ops.zig");
pub const convert = @import("convert.zig");
pub const @"export" = @import("export.zig");

pub const Model = convert.Model;
pub const ConvertResult = convert.ConvertResult;
pub const InitializerData = convert.InitializerData;
pub const ConvertError = convert.ConvertError;
pub const DimOverrides = convert.DimOverrides;
pub const ExportOptions = @"export".ExportOptions;
pub const NodeNameOverride = @"export".NodeNameOverride;
pub const SemanticDecoderGqaBinding = @"export".SemanticDecoderGqaBinding;
pub const ExportResult = @"export".ExportResult;
pub const ExternalDataFile = @"export".ExternalDataFile;
pub const ParameterInitializer = @"export".ParameterInitializer;
pub const ParameterInitializerReference = @"export".ParameterInitializerReference;
pub const ParameterInitializerReferenceProvider = @"export".ParameterInitializerReferenceProvider;
pub const Q8_0BlockInitializerReference = @"export".Q8_0BlockInitializerReference;
pub const StreamedExportResult = @"export".StreamedExportResult;
pub const ByteSink = @"export".ByteSink;
pub const StreamedTensorData = @"export".StreamedTensorData;
pub const exportGraph = @"export".exportGraph;
pub const exportGraphWithExternalData = @"export".exportGraphWithExternalData;
pub const exportGraphWithExternalDataToPath = @"export".exportGraphWithExternalDataToPath;
pub const serializeModel = @"export".serializeModel;
pub const freeStreamedTensorData = @"export".freeStreamedTensorData;

// Re-export proto types for inspection
pub const ModelProto = proto.ModelProto;
pub const GraphProto = proto.GraphProto;
pub const NodeProto = proto.NodeProto;
pub const TensorProto = proto.TensorProto;
pub const DataType = proto.DataType;
pub const ExternalDataInfo = proto.ExternalDataInfo;
pub const LazyModelProto = proto.LazyModelProto;
pub const LazyGraphProto = proto.LazyGraphProto;

/// Parse an ONNX model from binary protobuf data.
pub fn parse(allocator: std.mem.Allocator, data: []const u8) !Model {
    const model_proto = try proto.parseModelProto(allocator, data);
    return Model.init(allocator, model_proto);
}

/// Parse an ONNX model from binary protobuf data and attach a base directory
/// so external-data initializers can be resolved relative to it.
/// `base_dir` is borrowed and must outlive the returned Model.
pub fn parseWithBaseDir(
    allocator: std.mem.Allocator,
    data: []const u8,
    base_dir: ?[]const u8,
) !Model {
    const model_proto = try proto.parseModelProto(allocator, data);
    return Model.initWithBaseDir(allocator, model_proto, base_dir);
}

/// Parse an ONNX model lazily — initializers are not parsed up front.
/// Use the LazyGraphProto.parseInitializer() method to parse individual
/// weight tensors on demand. Ideal for large models (100MB+).
pub fn parseLazy(allocator: std.mem.Allocator, data: []const u8) !LazyModelProto {
    return proto.parseLazyModelProto(allocator, data);
}

/// Parse an ONNX model lazily and wrap it in a Model ready for conversion.
/// Initializers are only parsed on demand (i.e. when the converter actually
/// needs to read them), which saves memory for large models with many
/// weights. The returned Model's lifetime owns the lazy state; call
/// `model.deinit()` to free.
pub fn parseLazyAsModel(allocator: std.mem.Allocator, data: []const u8) !Model {
    const lazy_model = try proto.parseLazyModelProto(allocator, data);
    return Model.initFromLazy(allocator, lazy_model, null);
}

/// Same as `parseLazyAsModel` but attaches a base directory so external-data
/// initializers can be resolved on demand. `base_dir` is borrowed and must
/// outlive the returned Model.
pub fn parseLazyAsModelWithBaseDir(
    allocator: std.mem.Allocator,
    data: []const u8,
    base_dir: ?[]const u8,
) !Model {
    const lazy_model = try proto.parseLazyModelProto(allocator, data);
    return Model.initFromLazy(allocator, lazy_model, base_dir);
}

/// Check if an ONNX op type is supported by the converter.
pub fn isOpSupported(op_type: []const u8) bool {
    return ops.isSupported(op_type);
}

const std = @import("std");

test {
    _ = proto;
    _ = attrs;
    _ = tensor;
    _ = ops;
    _ = convert;
    _ = @"export";
}
