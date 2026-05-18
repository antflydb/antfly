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

const std = @import("std");
const backends = @import("../backends/backends.zig");
const image = @import("image.zig");
const crop = @import("crop.zig");
const ctc_decode = @import("ctc_decode.zig");
const connected_components = @import("connected_components.zig");
const vision_reader = @import("../readers/vision_reader.zig");

pub const TextRegion = struct {
    bbox: [4]f64,
    polygon: []const [2]f64 = &.{},
    confidence: f64 = 0,
};

pub const RecognizedRegion = struct {
    bbox: [4]f64,
    polygon: []const [2]f64 = &.{},
    confidence: f64 = 0,
    text: []const u8,
    rec_confidence: f64 = 0,
    label: ?[]const u8 = null,

    pub fn deinit(self: *RecognizedRegion, allocator: std.mem.Allocator) void {
        allocator.free(self.text);
        if (self.label) |label| allocator.free(label);
    }
};

pub const LayoutRegion = struct {
    bbox: [4]f64,
    polygon: []const [2]f64 = &.{},
    confidence: f64 = 0,
    label: []const u8,
    order_idx: usize = 0,

    pub fn deinit(self: *LayoutRegion, allocator: std.mem.Allocator) void {
        allocator.free(self.label);
    }
};

pub const PreprocessConfig = struct {
    width: u32,
    height: u32,
    mean: [3]f32 = .{ 0.5, 0.5, 0.5 },
    std: [3]f32 = .{ 0.5, 0.5, 0.5 },
    rescale_factor: f32 = 1.0 / 255.0,
    resample: image.Resample = .bilinear,
    keep_aspect_ratio: bool = false,
    dynamic_width: bool = false,
    pad_value_rgb: [3]u8 = .{ 255, 255, 255 },
};

pub const RecognitionResult = struct {
    text: []u8,
    confidence: f64,
};

pub const MultiStageOCRResult = struct {
    allocator: std.mem.Allocator,
    full_text: []u8,
    regions: []RecognizedRegion,
    layout: []LayoutRegion = &.{},

    pub fn deinit(self: *MultiStageOCRResult) void {
        self.allocator.free(self.full_text);
        for (self.regions) |*region| region.deinit(self.allocator);
        if (self.regions.len > 0) self.allocator.free(self.regions);
        for (self.layout) |*region| region.deinit(self.allocator);
        if (self.layout.len > 0) self.allocator.free(self.layout);
    }
};

pub const DBPostProcessor = struct {
    threshold: f32,
    box_threshold: f32,
    unclip_ratio: f64,
    min_box_area: usize,
    max_candidates: usize = 1000,

    pub fn process(
        self: DBPostProcessor,
        allocator: std.mem.Allocator,
        output: []const f32,
        width: usize,
        height: usize,
        original_width: u32,
        original_height: u32,
    ) ![]TextRegion {
        if (output.len < width * height) return allocator.dupe(TextRegion, &.{});

        const mask = try allocator.alloc(bool, width * height);
        defer allocator.free(mask);
        for (0..width * height) |i| mask[i] = output[i] > self.threshold;

        const components = try connected_components.findConnectedComponents(allocator, mask, width, height, self.min_box_area);
        defer allocator.free(components);

        const scale_x = @as(f64, @floatFromInt(original_width)) / @as(f64, @floatFromInt(width));
        const scale_y = @as(f64, @floatFromInt(original_height)) / @as(f64, @floatFromInt(height));

        var regions = std.ArrayListUnmanaged(TextRegion).empty;
        errdefer regions.deinit(allocator);

        for (components, 0..) |comp, idx| {
            if (idx >= self.max_candidates) break;
            const mean_prob = computeBoxScore(output, comp, width);
            if (mean_prob < self.box_threshold) continue;

            try regions.append(allocator, .{
                .bbox = unclipBox(self.unclip_ratio, comp, scale_x, scale_y, original_width, original_height),
                .confidence = mean_prob,
            });
        }

        return try regions.toOwnedSlice(allocator);
    }
};

pub const HeatmapPostProcessor = struct {
    threshold: f32,
    min_area: usize,

    pub fn process(
        self: HeatmapPostProcessor,
        allocator: std.mem.Allocator,
        output: []const f32,
        width: usize,
        height: usize,
        original_width: u32,
        original_height: u32,
    ) ![]TextRegion {
        if (output.len < width * height) return allocator.dupe(TextRegion, &.{});

        const mask = try allocator.alloc(bool, width * height);
        defer allocator.free(mask);
        for (0..width * height) |i| mask[i] = output[i] > self.threshold;

        const components = try connected_components.findConnectedComponents(allocator, mask, width, height, self.min_area);
        defer allocator.free(components);

        const scale_x = @as(f64, @floatFromInt(original_width)) / @as(f64, @floatFromInt(width));
        const scale_y = @as(f64, @floatFromInt(original_height)) / @as(f64, @floatFromInt(height));
        const regions = try allocator.alloc(TextRegion, components.len);
        errdefer allocator.free(regions);

        for (components, 0..) |comp, i| {
            regions[i] = .{
                .bbox = .{
                    @as(f64, @floatFromInt(comp.min_x)) * scale_x,
                    @as(f64, @floatFromInt(comp.min_y)) * scale_y,
                    @as(f64, @floatFromInt(comp.max_x + 1)) * scale_x,
                    @as(f64, @floatFromInt(comp.max_y + 1)) * scale_y,
                },
                .confidence = 1.0,
            };
        }

        return regions;
    }
};

pub const DetectionPostProcessor = union(enum) {
    db: DBPostProcessor,
    heatmap: HeatmapPostProcessor,

    pub fn process(
        self: DetectionPostProcessor,
        allocator: std.mem.Allocator,
        output: []const f32,
        width: usize,
        height: usize,
        original_width: u32,
        original_height: u32,
    ) ![]TextRegion {
        return switch (self) {
            .db => |processor| processor.process(allocator, output, width, height, original_width, original_height),
            .heatmap => |processor| processor.process(allocator, output, width, height, original_width, original_height),
        };
    }
};

pub const CTCRecognizer = struct {
    allocator: std.mem.Allocator,
    session: backends.Session,
    char_dict: [][]u8,
    preprocess: PreprocessConfig,

    pub fn deinit(self: *CTCRecognizer) void {
        ctc_decode.freeCharDict(self.allocator, self.char_dict);
        self.session.close();
    }

    pub fn recognize(self: *CTCRecognizer, img: image.Image) !RecognitionResult {
        var input_width = self.preprocess.width;
        const pixel_values = if (self.preprocess.keep_aspect_ratio) blk: {
            if (self.preprocess.dynamic_width) {
                input_width = image.computeAspectFitWidth(img.width, img.height, self.preprocess.height, self.preprocess.width);
                break :blk try image.preprocessDecodedRectScaledWithResample(
                    self.allocator,
                    img,
                    input_width,
                    self.preprocess.height,
                    self.preprocess.mean,
                    self.preprocess.std,
                    self.preprocess.rescale_factor,
                    self.preprocess.resample,
                );
            }

            break :blk try image.preprocessDecodedRectKeepAspectPadRightScaledWithResample(
                self.allocator,
                img,
                self.preprocess.width,
                self.preprocess.height,
                self.preprocess.mean,
                self.preprocess.std,
                self.preprocess.rescale_factor,
                self.preprocess.resample,
                self.preprocess.pad_value_rgb,
            );
        } else try image.preprocessDecodedRectScaledWithResample(
            self.allocator,
            img,
            self.preprocess.width,
            self.preprocess.height,
            self.preprocess.mean,
            self.preprocess.std,
            self.preprocess.rescale_factor,
            self.preprocess.resample,
        );
        defer self.allocator.free(pixel_values);

        const input_name = if (self.session.inputInfo().len > 0) self.session.inputInfo()[0].name else "x";
        const shape = [_]i64{
            1,
            3,
            @intCast(self.preprocess.height),
            @intCast(input_width),
        };
        var input_tensor = try backends.Tensor.initFloat32(self.allocator, input_name, &shape, pixel_values);
        defer input_tensor.deinit();

        const outputs = try self.session.run(&.{input_tensor}, self.allocator);
        defer freeTensorSlice(self.allocator, outputs);
        if (outputs.len == 0) return error.NoRecognitionOutput;

        const decoded = try ctc_decode.decodeFromTensor(self.allocator, &outputs[0], self.char_dict);
        return .{
            .text = decoded.text,
            .confidence = decoded.confidence,
        };
    }
};

pub const Vision2SeqRecognizer = struct {
    allocator: std.mem.Allocator,
    reader: vision_reader.LoadedVisionReader,

    pub fn loadFromStagePaths(
        allocator: std.mem.Allocator,
        model_path: []const u8,
        encoder_file: []const u8,
        decoder_file: []const u8,
        session_manager: *backends.SessionManager,
    ) !Vision2SeqRecognizer {
        return .{
            .allocator = allocator,
            .reader = try vision_reader.LoadedVisionReader.loadFromStagePaths(
                allocator,
                model_path,
                encoder_file,
                decoder_file,
                session_manager,
            ),
        };
    }

    pub fn deinit(self: *Vision2SeqRecognizer) void {
        self.reader.deinit();
    }

    pub fn recognize(self: *Vision2SeqRecognizer, img: image.Image) !RecognitionResult {
        var result = try self.reader.readDecodedRaw(img, .{});
        defer result.deinit();

        const trimmed = std.mem.trim(u8, result.text, " \t\r\n");
        return .{
            .text = try self.allocator.dupe(u8, trimmed),
            .confidence = if (trimmed.len > 0) 1.0 else 0.0,
        };
    }
};

pub const Recognizer = union(enum) {
    ctc: CTCRecognizer,
    vision2seq: Vision2SeqRecognizer,

    pub fn deinit(self: *Recognizer) void {
        switch (self.*) {
            .ctc => |*recognizer| recognizer.deinit(),
            .vision2seq => |*recognizer| recognizer.deinit(),
        }
    }

    pub fn recognize(self: *Recognizer, img: image.Image) !RecognitionResult {
        return switch (self.*) {
            .ctc => |*recognizer| recognizer.recognize(img),
            .vision2seq => |*recognizer| recognizer.recognize(img),
        };
    }
};

pub const MultiStageOCRPipeline = struct {
    allocator: std.mem.Allocator,
    detector: backends.Session,
    detection_preprocess: PreprocessConfig,
    post_processor: DetectionPostProcessor,
    recognizer: ?Recognizer = null,
    layout: ?backends.Session = null,
    order: ?backends.Session = null,

    pub fn deinit(self: *MultiStageOCRPipeline) void {
        if (self.recognizer) |*recognizer| {
            recognizer.deinit();
            self.recognizer = null;
        }
        if (self.layout) |layout| {
            layout.close();
            self.layout = null;
        }
        if (self.order) |order| {
            order.close();
            self.order = null;
        }
        self.detector.close();
    }

    pub fn run(self: *MultiStageOCRPipeline, image_bytes: []const u8) !MultiStageOCRResult {
        const img = try image.decode(self.allocator, image_bytes);
        defer img.deinit(self.allocator);
        return self.runDecoded(img);
    }

    pub fn runDecoded(self: *MultiStageOCRPipeline, img: image.Image) !MultiStageOCRResult {
        const regions = try self.detect(img);
        defer if (regions.len > 0) self.allocator.free(regions);

        var layout_regions: []LayoutRegion = &.{};
        errdefer {
            for (layout_regions) |*region| region.deinit(self.allocator);
            if (layout_regions.len > 0) self.allocator.free(layout_regions);
        }

        if (self.layout != null) {
            layout_regions = try self.analyzeLayout(img);
        }

        if (self.order != null) {
            try self.determineOrder(regions);
        } else {
            sortRegionsByReadingOrder(TextRegion, regions);
        }

        var recognized = std.ArrayListUnmanaged(RecognizedRegion).empty;
        errdefer {
            for (recognized.items) |*region| region.deinit(self.allocator);
            recognized.deinit(self.allocator);
        }

        if (self.recognizer) |*recognizer| {
            for (regions) |region| {
                const cropped = try crop.cropBBox(self.allocator, img, region.bbox);
                defer cropped.deinit(self.allocator);

                const rec = recognizer.recognize(cropped) catch continue;
                if (rec.text.len == 0) {
                    self.allocator.free(rec.text);
                    continue;
                }

                try recognized.append(self.allocator, .{
                    .bbox = region.bbox,
                    .polygon = region.polygon,
                    .confidence = region.confidence,
                    .text = rec.text,
                    .rec_confidence = rec.confidence,
                    .label = null,
                });
            }
        } else {
            for (regions) |region| {
                try recognized.append(self.allocator, .{
                    .bbox = region.bbox,
                    .polygon = region.polygon,
                    .confidence = region.confidence,
                    .text = try self.allocator.dupe(u8, ""),
                    .rec_confidence = region.confidence,
                    .label = null,
                });
            }
        }

        if (layout_regions.len > 0) {
            try attachLayoutLabels(self.allocator, recognized.items, layout_regions);
        }

        const full_text = if (recognized.items.len > 0)
            try assembleFullText(self.allocator, recognized.items)
        else
            try self.allocator.dupe(u8, "");

        return .{
            .allocator = self.allocator,
            .full_text = full_text,
            .regions = try recognized.toOwnedSlice(self.allocator),
            .layout = layout_regions,
        };
    }

    fn detect(self: *MultiStageOCRPipeline, img: image.Image) ![]TextRegion {
        const pixel_values = try image.preprocessDecodedRectScaledWithResample(
            self.allocator,
            img,
            self.detection_preprocess.width,
            self.detection_preprocess.height,
            self.detection_preprocess.mean,
            self.detection_preprocess.std,
            self.detection_preprocess.rescale_factor,
            self.detection_preprocess.resample,
        );
        defer self.allocator.free(pixel_values);

        const input_name = if (self.detector.inputInfo().len > 0) self.detector.inputInfo()[0].name else "pixel_values";
        const shape = [_]i64{
            1,
            3,
            @intCast(self.detection_preprocess.height),
            @intCast(self.detection_preprocess.width),
        };
        var input_tensor = try backends.Tensor.initFloat32(self.allocator, input_name, &shape, pixel_values);
        defer input_tensor.deinit();

        const outputs = try self.detector.run(&.{input_tensor}, self.allocator);
        defer freeTensorSlice(self.allocator, outputs);
        if (outputs.len == 0) return self.allocator.dupe(TextRegion, &.{});

        const heatmap = try extractDetectionHeatmap(self.allocator, &outputs[0], self.detection_preprocess.width, self.detection_preprocess.height);
        defer self.allocator.free(heatmap.values);
        return self.post_processor.process(self.allocator, heatmap.values, heatmap.width, heatmap.height, img.width, img.height);
    }

    fn analyzeLayout(self: *MultiStageOCRPipeline, img: image.Image) ![]LayoutRegion {
        const layout = self.layout orelse return self.allocator.dupe(LayoutRegion, &.{});

        const pixel_values = try image.preprocessDecodedRectScaledWithResample(
            self.allocator,
            img,
            self.detection_preprocess.width,
            self.detection_preprocess.height,
            self.detection_preprocess.mean,
            self.detection_preprocess.std,
            self.detection_preprocess.rescale_factor,
            self.detection_preprocess.resample,
        );
        defer self.allocator.free(pixel_values);

        const input_name = if (layout.inputInfo().len > 0) layout.inputInfo()[0].name else "pixel_values";
        const shape = [_]i64{
            1,
            3,
            @intCast(self.detection_preprocess.height),
            @intCast(self.detection_preprocess.width),
        };
        var input_tensor = try backends.Tensor.initFloat32(self.allocator, input_name, &shape, pixel_values);
        defer input_tensor.deinit();

        const outputs = try layout.run(&.{input_tensor}, self.allocator);
        defer freeTensorSlice(self.allocator, outputs);
        if (outputs.len == 0) return self.allocator.dupe(LayoutRegion, &.{});

        return parseLayoutOutput(self.allocator, &outputs[0], img.width, img.height);
    }

    fn determineOrder(self: *MultiStageOCRPipeline, regions: []TextRegion) !void {
        if (regions.len <= 1) return;
        const order = self.order orelse {
            sortRegionsByReadingOrder(TextRegion, regions);
            return;
        };

        const bbox_data = try self.allocator.alloc(f32, regions.len * 4);
        defer self.allocator.free(bbox_data);
        for (regions, 0..) |region, idx| {
            bbox_data[idx * 4 + 0] = @floatCast(region.bbox[0]);
            bbox_data[idx * 4 + 1] = @floatCast(region.bbox[1]);
            bbox_data[idx * 4 + 2] = @floatCast(region.bbox[2]);
            bbox_data[idx * 4 + 3] = @floatCast(region.bbox[3]);
        }

        const input_name = if (order.inputInfo().len > 0) order.inputInfo()[0].name else "boxes";
        const shape = [_]i64{ 1, @intCast(regions.len), 4 };
        var input_tensor = try backends.Tensor.initFloat32(self.allocator, input_name, &shape, bbox_data);
        defer input_tensor.deinit();

        const outputs = order.run(&.{input_tensor}, self.allocator) catch {
            sortRegionsByReadingOrder(TextRegion, regions);
            return;
        };
        defer freeTensorSlice(self.allocator, outputs);
        if (outputs.len == 0) {
            sortRegionsByReadingOrder(TextRegion, regions);
            return;
        }

        const order_data = outputs[0].asFloat32IfAligned() orelse {
            sortRegionsByReadingOrder(TextRegion, regions);
            return;
        };

        try sortRegionsByPredictedOrder(self.allocator, regions, order_data);
    }
};

const DetectionHeatmap = struct {
    values: []f32,
    width: usize,
    height: usize,
};

const layout_class_labels = [_][]const u8{
    "Caption",
    "Footnote",
    "Formula",
    "ListItem",
    "PageFooter",
    "PageHeader",
    "Picture",
    "SectionHeader",
    "Table",
    "Text",
    "Title",
};

pub fn sortRegionsByReadingOrder(comptime T: type, regions: []T) void {
    if (regions.len <= 1) return;

    const tolerance = computeReadingOrderTolerance(T, regions);
    std.mem.sort(T, regions, tolerance, struct {
        fn lessThan(tol: f64, lhs: T, rhs: T) bool {
            const lhs_y = yCenter(lhs.bbox);
            const rhs_y = yCenter(rhs.bbox);
            if (abs(lhs_y - rhs_y) < tol) return lhs.bbox[0] < rhs.bbox[0];
            return lhs_y < rhs_y;
        }
    }.lessThan);
}

pub fn assembleFullText(allocator: std.mem.Allocator, regions: []const RecognizedRegion) ![]u8 {
    if (regions.len == 0) return allocator.dupe(u8, "");

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);

    try out.appendSlice(allocator, regions[0].text);
    if (regions.len == 1) return try out.toOwnedSlice(allocator);

    const tolerance = computeReadingOrderTolerance(RecognizedRegion, regions);
    for (regions[1..], 1..) |region, idx| {
        const prev_y = yCenter(regions[idx - 1].bbox);
        const cur_y = yCenter(region.bbox);
        try out.append(allocator, if (abs(cur_y - prev_y) < tolerance) ' ' else '\n');
        try out.appendSlice(allocator, region.text);
    }

    return try out.toOwnedSlice(allocator);
}

pub fn computeOverlap(a: [4]f64, b: [4]f64) f64 {
    const x1 = @max(a[0], b[0]);
    const y1 = @max(a[1], b[1]);
    const x2 = @min(a[2], b[2]);
    const y2 = @min(a[3], b[3]);

    if (x2 <= x1 or y2 <= y1) return 0;

    const intersection = (x2 - x1) * (y2 - y1);
    const area_a = (a[2] - a[0]) * (a[3] - a[1]);
    const area_b = (b[2] - b[0]) * (b[3] - b[1]);
    const union_area = area_a + area_b - intersection;
    if (union_area <= 0) return 0;
    return intersection / union_area;
}

fn computeReadingOrderTolerance(comptime T: type, regions: []const T) f64 {
    if (regions.len == 0) return 0;

    var avg_height: f64 = 0;
    for (regions) |region| avg_height += region.bbox[3] - region.bbox[1];
    avg_height /= @as(f64, @floatFromInt(regions.len));
    return avg_height * 0.5;
}

fn yCenter(bbox: [4]f64) f64 {
    return (bbox[1] + bbox[3]) / 2.0;
}

fn abs(v: f64) f64 {
    return if (v < 0) -v else v;
}

fn parseLayoutOutput(
    allocator: std.mem.Allocator,
    output: *const backends.Tensor,
    original_width: u32,
    original_height: u32,
) ![]LayoutRegion {
    if (output.dtype != .f32) return allocator.dupe(LayoutRegion, &.{});
    if (output.shape.len != 4) return allocator.dupe(LayoutRegion, &.{});
    const output_data = output.asFloat32IfAligned() orelse return allocator.dupe(LayoutRegion, &.{});

    const num_classes: usize = @intCast(output.shape[1]);
    const height: usize = @intCast(output.shape[2]);
    const width: usize = @intCast(output.shape[3]);
    const plane_size = width * height;
    const scale_x = @as(f64, @floatFromInt(original_width)) / @as(f64, @floatFromInt(width));
    const scale_y = @as(f64, @floatFromInt(original_height)) / @as(f64, @floatFromInt(height));

    var regions = std.ArrayListUnmanaged(LayoutRegion).empty;
    errdefer {
        for (regions.items) |*region| region.deinit(allocator);
        regions.deinit(allocator);
    }

    const class_count = @min(num_classes, layout_class_labels.len);
    for (0..class_count) |class_idx| {
        const heatmap = output_data[class_idx * plane_size .. (class_idx + 1) * plane_size];
        const mask = try allocator.alloc(bool, plane_size);
        defer allocator.free(mask);
        for (0..plane_size) |i| mask[i] = heatmap[i] > 0.5;

        const components = try connected_components.findConnectedComponents(allocator, mask, width, height, 100);
        defer allocator.free(components);

        for (components) |comp| {
            try regions.append(allocator, .{
                .bbox = .{
                    @as(f64, @floatFromInt(comp.min_x)) * scale_x,
                    @as(f64, @floatFromInt(comp.min_y)) * scale_y,
                    @as(f64, @floatFromInt(comp.max_x + 1)) * scale_x,
                    @as(f64, @floatFromInt(comp.max_y + 1)) * scale_y,
                },
                .confidence = 1.0,
                .label = try allocator.dupe(u8, layout_class_labels[class_idx]),
            });
        }
    }

    return try regions.toOwnedSlice(allocator);
}

fn attachLayoutLabels(
    allocator: std.mem.Allocator,
    regions: []RecognizedRegion,
    layout_regions: []const LayoutRegion,
) !void {
    for (regions) |*region| {
        var best_overlap: f64 = 0;
        var best_label: ?[]const u8 = null;
        for (layout_regions) |layout_region| {
            const overlap = computeOverlap(region.bbox, layout_region.bbox);
            if (overlap > best_overlap) {
                best_overlap = overlap;
                best_label = layout_region.label;
            }
        }

        if (best_overlap > 0.3 and best_label != null) {
            if (region.label) |label| allocator.free(label);
            region.label = try allocator.dupe(u8, best_label.?);
        }
    }
}

fn sortRegionsByPredictedOrder(
    allocator: std.mem.Allocator,
    regions: []TextRegion,
    order_data: []const f32,
) !void {
    const IndexedRegion = struct {
        region: TextRegion,
        order: f32,
    };

    const indexed = try allocator.alloc(IndexedRegion, regions.len);
    defer allocator.free(indexed);

    for (regions, 0..) |region, idx| {
        indexed[idx] = .{
            .region = region,
            .order = if (idx < order_data.len) order_data[idx] else 0,
        };
    }

    std.mem.sort(IndexedRegion, indexed, {}, struct {
        fn lessThan(_: void, lhs: IndexedRegion, rhs: IndexedRegion) bool {
            return lhs.order < rhs.order;
        }
    }.lessThan);

    for (indexed, 0..) |entry, idx| regions[idx] = entry.region;
}

fn extractDetectionHeatmap(
    allocator: std.mem.Allocator,
    output: *const backends.Tensor,
    fallback_width: u32,
    fallback_height: u32,
) !DetectionHeatmap {
    if (output.dtype != .f32) return error.UnsupportedTensorType;
    const output_data = output.asFloat32IfAligned() orelse return error.UnalignedTensorData;

    switch (output.shape.len) {
        4 => {
            const num_classes: usize = @intCast(output.shape[1]);
            const out_h: usize = @intCast(output.shape[2]);
            const out_w: usize = @intCast(output.shape[3]);
            const plane_size = out_h * out_w;
            const plane = if (num_classes >= 2)
                output_data[plane_size .. 2 * plane_size]
            else
                output_data[0..plane_size];
            return .{
                .values = try allocator.dupe(f32, plane),
                .width = out_w,
                .height = out_h,
            };
        },
        3 => {
            const out_h: usize = @intCast(output.shape[1]);
            const out_w: usize = @intCast(output.shape[2]);
            const plane_size = out_h * out_w;
            return .{
                .values = try allocator.dupe(f32, output_data[0..plane_size]),
                .width = out_w,
                .height = out_h,
            };
        },
        else => return .{
            .values = try allocator.dupe(f32, output_data),
            .width = fallback_width,
            .height = fallback_height,
        },
    }
}

fn computeBoxScore(prob_map: []const f32, comp: connected_components.ComponentRect, width: usize) f64 {
    var sum: f64 = 0;
    var count: usize = 0;
    for (comp.min_y..comp.max_y + 1) |y| {
        for (comp.min_x..comp.max_x + 1) |x| {
            const idx = y * width + x;
            if (idx < prob_map.len) {
                sum += prob_map[idx];
                count += 1;
            }
        }
    }
    if (count == 0) return 0;
    return sum / @as(f64, @floatFromInt(count));
}

fn unclipBox(
    ratio: f64,
    comp: connected_components.ComponentRect,
    scale_x: f64,
    scale_y: f64,
    original_width: u32,
    original_height: u32,
) [4]f64 {
    const box_w = @as(f64, @floatFromInt(comp.max_x - comp.min_x + 1)) * scale_x;
    const box_h = @as(f64, @floatFromInt(comp.max_y - comp.min_y + 1)) * scale_y;
    const perimeter = 2.0 * (box_w + box_h);
    const area = box_w * box_h;
    const distance = if (perimeter > 0) area * ratio / perimeter else 0;

    return .{
        std.math.clamp(@as(f64, @floatFromInt(comp.min_x)) * scale_x - distance, 0, @as(f64, @floatFromInt(original_width))),
        std.math.clamp(@as(f64, @floatFromInt(comp.min_y)) * scale_y - distance, 0, @as(f64, @floatFromInt(original_height))),
        std.math.clamp(@as(f64, @floatFromInt(comp.max_x + 1)) * scale_x + distance, 0, @as(f64, @floatFromInt(original_width))),
        std.math.clamp(@as(f64, @floatFromInt(comp.max_y + 1)) * scale_y + distance, 0, @as(f64, @floatFromInt(original_height))),
    };
}

fn freeTensorSlice(allocator: std.mem.Allocator, tensors: []backends.Tensor) void {
    for (tensors) |*tensor| {
        var mut = tensor.*;
        mut.deinit();
    }
    allocator.free(tensors);
}

test "sortRegionsByReadingOrder sorts by y band then x" {
    var regions = [_]TextRegion{
        .{ .bbox = .{ 90, 10, 120, 30 } },
        .{ .bbox = .{ 10, 60, 80, 80 } },
        .{ .bbox = .{ 10, 12, 70, 32 } },
    };

    sortRegionsByReadingOrder(TextRegion, regions[0..]);

    try std.testing.expectEqualDeep([4]f64{ 10, 12, 70, 32 }, regions[0].bbox);
    try std.testing.expectEqualDeep([4]f64{ 90, 10, 120, 30 }, regions[1].bbox);
    try std.testing.expectEqualDeep([4]f64{ 10, 60, 80, 80 }, regions[2].bbox);
}

test "assembleFullText matches reading order spacing behavior" {
    const allocator = std.testing.allocator;
    const regions = [_]RecognizedRegion{
        .{ .bbox = .{ 10, 10, 80, 30 }, .text = "Hello" },
        .{ .bbox = .{ 90, 10, 180, 30 }, .text = "world" },
        .{ .bbox = .{ 10, 60, 120, 80 }, .text = "Goodbye" },
    };

    const text = try assembleFullText(allocator, &regions);
    defer allocator.free(text);

    try std.testing.expectEqualStrings("Hello world\nGoodbye", text);
}

test "computeOverlap returns iou for overlapping boxes" {
    const overlap = computeOverlap(.{ 0, 0, 10, 10 }, .{ 5, 5, 15, 15 });
    try std.testing.expectApproxEqAbs(@as(f64, 25.0 / 175.0), overlap, 1e-9);
    try std.testing.expectEqual(@as(f64, 0), computeOverlap(.{ 0, 0, 1, 1 }, .{ 2, 2, 3, 3 }));
}

test "extractDetectionHeatmap selects foreground plane from multi-class output" {
    const allocator = std.testing.allocator;
    var tensor = try backends.Tensor.initFloat32(allocator, "logits", &.{ 1, 2, 2, 2 }, &.{
        0.1, 0.2, 0.3, 0.4,
        0.9, 0.8, 0.7, 0.6,
    });
    defer tensor.deinit();

    const heatmap = try extractDetectionHeatmap(allocator, &tensor, 2, 2);
    defer allocator.free(heatmap.values);

    try std.testing.expectEqual(@as(usize, 2), heatmap.width);
    try std.testing.expectEqual(@as(usize, 2), heatmap.height);
    try std.testing.expectEqualSlices(f32, &.{ 0.9, 0.8, 0.7, 0.6 }, heatmap.values);
}

test "heatmap post processor returns scaled regions" {
    const allocator = std.testing.allocator;
    const processor = HeatmapPostProcessor{
        .threshold = 0.5,
        .min_area = 1,
    };
    const regions = try processor.process(
        allocator,
        &.{
            0.9, 0.0,
            0.0, 0.8,
        },
        2,
        2,
        200,
        100,
    );
    defer allocator.free(regions);

    try std.testing.expectEqual(@as(usize, 2), regions.len);
}

test "db post processor thresholds and unclipped boxes" {
    const allocator = std.testing.allocator;
    const processor = DBPostProcessor{
        .threshold = 0.3,
        .box_threshold = 0.5,
        .unclip_ratio = 1.5,
        .min_box_area = 1,
    };
    const regions = try processor.process(
        allocator,
        &.{
            0.9, 0.9, 0.1,
            0.9, 0.9, 0.1,
            0.1, 0.1, 0.1,
        },
        3,
        3,
        300,
        300,
    );
    defer allocator.free(regions);

    try std.testing.expectEqual(@as(usize, 1), regions.len);
    try std.testing.expect(regions[0].bbox[2] > regions[0].bbox[0]);
    try std.testing.expect(regions[0].bbox[3] > regions[0].bbox[1]);
    try std.testing.expect(regions[0].confidence >= 0.5);
}

test "parseLayoutOutput returns labeled regions for active class heatmaps" {
    const allocator = std.testing.allocator;
    const height: usize = 10;
    const width: usize = 10;
    const plane_size = width * height;
    const data = try allocator.alloc(f32, layout_class_labels.len * plane_size);
    defer allocator.free(data);
    @memset(data, 0);
    for (0..plane_size) |idx| data[9 * plane_size + idx] = 0.9;

    var tensor = try backends.Tensor.initFloat32(allocator, "layout", &.{ 1, 11, height, width }, data);
    defer tensor.deinit();

    const regions = try parseLayoutOutput(allocator, &tensor, 200, 100);
    defer {
        for (regions) |*region| region.deinit(allocator);
        allocator.free(regions);
    }

    try std.testing.expectEqual(@as(usize, 1), regions.len);
    try std.testing.expectEqualStrings("Text", regions[0].label);
}

test "attachLayoutLabels assigns best overlapping label" {
    const allocator = std.testing.allocator;
    var regions = [_]RecognizedRegion{
        .{ .bbox = .{ 0, 0, 10, 10 }, .text = try allocator.dupe(u8, "hello") },
    };
    defer for (regions[0..]) |*region| region.deinit(allocator);

    var layout = [_]LayoutRegion{
        .{ .bbox = .{ 0, 0, 10, 10 }, .label = try allocator.dupe(u8, "Title") },
        .{ .bbox = .{ 20, 20, 30, 30 }, .label = try allocator.dupe(u8, "Text") },
    };
    defer {
        for (&layout) |*r| {
            allocator.free(r.label);
        }
    }

    try attachLayoutLabels(allocator, regions[0..], layout[0..]);
    try std.testing.expectEqualStrings("Title", regions[0].label.?);
}

test "sortRegionsByPredictedOrder reorders regions by model scores" {
    const allocator = std.testing.allocator;
    var regions = [_]TextRegion{
        .{ .bbox = .{ 0, 40, 10, 50 } },
        .{ .bbox = .{ 0, 10, 10, 20 } },
        .{ .bbox = .{ 0, 25, 10, 35 } },
    };

    try sortRegionsByPredictedOrder(allocator, regions[0..], &.{ 2.0, 0.0, 1.0 });
    try std.testing.expectEqualDeep([4]f64{ 0, 10, 10, 20 }, regions[0].bbox);
    try std.testing.expectEqualDeep([4]f64{ 0, 25, 10, 35 }, regions[1].bbox);
    try std.testing.expectEqualDeep([4]f64{ 0, 40, 10, 50 }, regions[2].bbox);
}
