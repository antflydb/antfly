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

//! Stable source-write diagnostics for maintained retained datasets. A matching
//! typed rejection is definite; transport errors and unknown outcomes must be
//! handled before decoding these reasons.
const std = @import("std");
pub const Error = error{
    InvalidRetainedSeriesRow,
    RetainedSeriesBatchTooLarge,
    RetainedSeriesCorrectionWindow,
    RetainedSeriesCoverageMissing,
    RetainedSeriesSourceDraining,
    RetainedSeriesNumericOverflow,
    RetainedSeriesTimeOverflow,
};
pub const Spec = struct { status: u16, code: []const u8, message: []const u8, retryable: bool = false };
pub fn classify(err: anyerror) ?Error {
    inline for (@typeInfo(Error).error_set.error_names.?) |name| if (err == @field(Error, name)) return @field(Error, name);
    return null;
}
pub fn decode(name: []const u8) ?Error {
    inline for (@typeInfo(Error).error_set.error_names.?) |field| if (std.mem.eql(u8, field, name)) return @field(Error, field);
    return null;
}
pub fn describe(err: Error) Spec {
    return switch (err) {
        error.InvalidRetainedSeriesRow => .{ .status = 422, .code = "retained_series_invalid_row", .message = "Source fields do not satisfy a retained dataset definition." },
        error.RetainedSeriesBatchTooLarge => .{ .status = 413, .code = "retained_series_batch_too_large", .message = "Retained maintenance exceeds the write budget; reduce the batch or document size." },
        error.RetainedSeriesCorrectionWindow => .{ .status = 409, .code = "retained_series_correction_window", .message = "The mutation changes finalized retained history." },
        error.RetainedSeriesCoverageMissing => .{ .status = 409, .code = "retained_series_coverage_missing", .message = "The mutation addresses history outside retained coverage." },
        error.RetainedSeriesSourceDraining => .{ .status = 503, .code = "retained_series_source_draining", .message = "Retained dataset closure is draining prepared writes; retry shortly.", .retryable = true },
        error.RetainedSeriesNumericOverflow => .{ .status = 422, .code = "retained_series_numeric_overflow", .message = "A retained aggregate is outside its supported numeric range." },
        error.RetainedSeriesTimeOverflow => .{ .status = 422, .code = "retained_series_time_overflow", .message = "A timestamp or bucket boundary is outside its supported range." },
    };
}
test "retained source write diagnostics preserve exact reasons and bounded retry semantics" {
    inline for (@typeInfo(Error).error_set.error_names.?) |name| {
        const reason = @field(Error, name);
        try std.testing.expectEqual(reason, classify(reason).?);
        try std.testing.expectEqual(reason, decode(@errorName(reason)).?);
        try std.testing.expectEqual(reason == error.RetainedSeriesSourceDraining, describe(reason).retryable);
    }
    try std.testing.expectEqual(@as(u16, 413), describe(error.RetainedSeriesBatchTooLarge).status);
    try std.testing.expectEqual(@as(u16, 409), describe(error.RetainedSeriesCorrectionWindow).status);
    try std.testing.expect(classify(error.OutcomeUnknown) == null);
    try std.testing.expect(classify(error.OutOfMemory) == null);
    try std.testing.expect(decode("unknown") == null);
}
