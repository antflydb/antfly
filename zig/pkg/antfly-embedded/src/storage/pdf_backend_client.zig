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

//! Source-free complete PDF backend operations. Unknown PDF error names remain
//! in the failure envelope so template directives retain their exact diagnostic.
const std = @import("std");
const abi = @import("enrichment_compute_abi");
const identity = @import("kernel_error_identity");
pub const Operation = abi.PdfBackendOperation;
pub const Outcome = union(enum) { bytes: []u8, failure: abi.FailureIdentity };

pub fn compute(allocator: std.mem.Allocator, bytes: []const u8, operation: Operation) !Outcome {
    const table = abi.BackendAllocator.fromStd(&allocator);
    var result: abi.CallerOwnedBytes = .{};
    var delivered = false;
    defer if (!delivered) if (result.ptr) |ptr| allocator.free(ptr[0..@intCast(result.len)]);
    var failure: abi.FailureIdentity = .{};
    const status = abi.antfly_enrichment_pdf_backend(&.{
        .allocator = &table,
        .pdf_bytes = .fromSlice(bytes),
        .operation = operation,
    }, &result, &failure);
    try identity.validateFailureEnvelope(status, &failure, abi.abi_version);
    if (status != .ok) {
        if (failure.boundary != .enrichment_compute) return error.InvalidBoundaryFailureIdentity;
        return .{ .failure = failure };
    }
    if (result.len != 0 and result.ptr == null) return error.InvalidBoundaryFailureIdentity;
    delivered = true;
    return .{ .bytes = result.slice() };
}
