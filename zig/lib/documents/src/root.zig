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

//! Checked document formats and native display compilation, independent of inference.
pub const types = @import("types.zig");
pub const ooxml = @import("ooxml.zig");
pub const slides = @import("slides.zig");
pub const drawingml = @import("drawingml.zig");
pub const drawingml_theme = @import("drawingml_theme.zig");
pub const drawingml_text = @import("drawingml_text.zig");
pub const drawingml_chart = @import("drawingml_chart.zig");
pub const output = @import("output.zig");
pub const fixtures = if (@import("builtin").is_test) @import("fixtures.zig") else struct {};

test {
    _ = types;
    _ = ooxml;
    _ = slides;
    _ = drawingml;
    _ = drawingml_theme;
    _ = drawingml_text;
    _ = drawingml_chart;
    _ = output;
}

pub const renderer_version = "antfly-native-documents-v1";
