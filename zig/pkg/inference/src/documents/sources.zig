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

//! Source adapters retain immutable encoded sources and parse each document
//! once. Page analysis/render buffers live in a resettable per-page arena.
const std = @import("std");
const pdf = @import("antfly_pdf");
const image = @import("antfly_image");
const c = @import("contract.zig");
const ooxml = @import("antfly_documents").ooxml;
const slide_parts = @import("antfly_documents").slides;
const drawingml = @import("antfly_documents").drawingml;
const A = std.mem.Allocator;
pub const Input = @import("antfly_readers").DocumentSource;
// Structural parsing is bounded independently of the selected-page work budget.
pub const max_source_pages = ooxml.max_presentation_slides;
fn selectedPage(selection: ?[]const usize, number: usize) bool {
    const pages = selection orelse return true;
    return std.sort.binarySearch(usize, pages, number, struct {
        fn order(key: usize, value: usize) std.math.Order {
            return std.math.order(key, value);
        }
    }.order) != null;
}
pub const Slide = struct { path: []const u8, view: ?ooxml.View = null, hidden: bool = false, notes: ?ooxml.View = null, inherited: []const c.InheritedPart = &.{}, theme: @import("antfly_documents").drawingml_theme.Theme = .{}, theme_supported: bool = true };
pub const Source = struct {
    input: Input,
    fingerprint: [64]u8,
    bytes: []const u8,
    page_selection: ?[]const usize = null,
    // Source-owned immutable fork metadata is reused across bounded page calls.
    render_template: ?pdf.reader.RenderForkTemplate = null,
    max_regions: usize = (c.Limits{}).max_regions,
    parsed: union(enum) { pdf: pdf.reader.Reader, image: void, slides: struct { archive: ooxml.Archive, slides: []const Slide, size: ?drawingml.Size, inheritance: slide_parts.Cache } },
    pub fn init(a: A, input: Input, bytes: []const u8, limits: c.Limits, control: c.Control) !Source {
        try control.check();
        if (input.id.len == 0 or input.id.len > 256 or bytes.len > limits.max_source_bytes) return error.InvalidDocumentSource;
        const essence = @import("antfly_readers").mediaTypeEssence(input.media_type) catch return error.InvalidDocumentSource;
        var normalized = input;
        normalized.media_type = if (std.ascii.eqlIgnoreCase(essence, "application/pdf")) "application/pdf" else if (std.ascii.eqlIgnoreCase(essence, "application/vnd.openxmlformats-officedocument.presentationml.presentation")) "application/vnd.openxmlformats-officedocument.presentationml.presentation" else if (image.inferenceFormatForMimeEssence(essence)) |format| image.inferenceMimeEssenceForFormat(format).? else return error.UnsupportedDocumentFormat;
        if (input.quadrilateral != null and !std.mem.startsWith(u8, normalized.media_type, "image/")) return error.InvalidDocumentQuadrilateral;
        const selection: ?[]const usize = if (input.pages) |pages| blk: {
            if (pages.len == 0) return error.InvalidDocumentSelection;
            if (pages.len > limits.max_pages) return error.DocumentPageLimitExceeded;
            const sorted = try a.dupe(usize, pages);
            std.mem.sort(usize, sorted, {}, std.sort.asc(usize));
            for (sorted, 0..) |number, i| {
                if (number == 0 or (i > 0 and sorted[i - 1] == number)) return error.InvalidDocumentSelection;
            }
            break :blk sorted;
        } else null;
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
        const fingerprint = std.fmt.bytesToHex(hash, .lower);
        var result: Source = undefined;
        if (std.mem.eql(u8, normalized.media_type, "application/pdf")) {
            if (!std.mem.startsWith(u8, bytes, "%PDF-")) return error.DocumentMediaTypeMismatch;
            const Probe = struct {
                fn cancelled(raw: ?*const anyopaque) bool {
                    const ctl: *const c.Control = @ptrCast(@alignCast(raw.?));
                    ctl.check() catch return true;
                    return false;
                }
            };
            const persistent_control = try a.create(c.Control);
            persistent_control.* = control;
            result = .{ .input = normalized, .bytes = bytes, .fingerprint = fingerprint, .parsed = .{ .pdf = try pdf.reader.Reader.initWithDecodeLimitsAndCancellation(a, bytes, .{}, .{ .context = persistent_control, .is_cancelled_fn = Probe.cancelled }) } };
        } else if (std.mem.eql(u8, normalized.media_type, "application/vnd.openxmlformats-officedocument.presentationml.presentation")) {
            const archive = try ooxml.Archive.initWithControl(a, bytes, limits.max_source_bytes, control);
            const presentation = try archive.get(a, "ppt/presentation.xml");
            const slide_size = drawingml.presentationSize(a, presentation, control) catch |err| switch (err) {
                error.DocumentRendererUnavailable => null,
                else => return err,
            };
            var slides = std.ArrayList(Slide).empty;
            var inheritance = slide_parts.Cache{ .max_regions = limits.max_regions };
            const relationships = (try inheritance.relationshipsFor(a, archive, "ppt/presentation.xml")) orelse return error.MissingDocumentPart;
            const ids = try ooxml.presentationSlideIds(a, presentation, control);
            for (ids) |id| {
                try control.check();
                const path = try normalizedPart(a, "ppt/", (try relationships.get(id, .slide)).target);
                if (slides.items.len >= max_source_pages) return error.DocumentPageLimitExceeded;
                const number = slides.items.len + 1;
                if (selection == null and number > limits.max_pages) return error.DocumentPageLimitExceeded;
                var slide = Slide{ .path = path };
                // Unselected slides keep their original indexes but do not
                // decompress XML, walk inheritance, or populate part caches.
                if (selectedPage(selection, number)) {
                    slide.view = try ooxml.View.initWithControl(a, try archive.get(a, path), control);
                    try slide.view.?.requirePart(.slide);
                    const xml = slide.view.?.bytes;
                    var slide_iter = ooxml.Xml{ .bytes = xml };
                    while (try slide_iter.next()) |part| {
                        if (part == .tag and !part.tag.closing and std.mem.eql(u8, part.tag.local(), "sld")) {
                            slide.hidden = !try part.tag.booleanAttr("show", true);
                            break;
                        }
                    }
                    slide.inherited = try inheritance.inherited(a, archive, path, xml);
                    for (0..slide.inherited.len) |index| {
                        const part = slide.inherited[slide.inherited.len - index - 1];
                        if (try inheritance.relatedPath(a, archive, part.path, .theme_override) != null) slide.theme_supported = false;
                        const part_theme = inheritance.themeForPart(a, archive, part.path) catch |err| switch (err) {
                            error.DocumentRendererUnavailable => unsupported: {
                                slide.theme_supported = false;
                                break :unsupported null;
                            },
                            else => return err,
                        };
                        if (part_theme) |theme| slide.theme = theme;
                        slide.theme = try slide.theme.withMapping(a, try inheritance.renderView(part.path), control);
                    }
                    const own_theme = inheritance.themeForPart(a, archive, path) catch |err| switch (err) {
                        error.DocumentRendererUnavailable => unsupported: {
                            slide.theme_supported = false;
                            break :unsupported null;
                        },
                        else => return err,
                    };
                    if (own_theme) |theme| slide.theme = theme;
                    if (try inheritance.relatedPath(a, archive, path, .theme_override) != null) slide.theme_supported = false;
                    slide.theme = try slide.theme.withMapping(a, slide.view.?, control);
                    if (input.include_notes and (!slide.hidden or input.include_hidden)) if (try inheritance.relatedPath(a, archive, path, .notes)) |notes_path| {
                        slide.notes = try ooxml.View.initWithControl(a, try archive.get(a, notes_path), control);
                        try slide.notes.?.requirePart(.notes);
                    };
                }
                try slides.append(a, slide);
            }
            if (slides.items.len == 0) return error.InvalidDocumentSource;
            result = .{ .input = normalized, .bytes = bytes, .fingerprint = fingerprint, .parsed = .{ .slides = .{ .archive = archive, .slides = try slides.toOwnedSlice(a), .size = slide_size, .inheritance = inheritance } } };
        } else if (std.mem.startsWith(u8, normalized.media_type, "image/")) {
            const expected = image.inferenceFormatForMimeEssence(normalized.media_type) orelse return error.UnsupportedDocumentFormat;
            if (image.detectFormat(bytes) != expected) return error.DocumentMediaTypeMismatch;
            const info = try image.inspectEncoded(bytes);
            if (info.width > limits.max_dimension or info.height > limits.max_dimension or try info.pixels() > limits.max_pixels) return error.DocumentPixelLimitExceeded;
            if (input.quadrilateral) |quad| _ = try @import("rectification.zig").plan(quad, info.width, info.height, limits);
            result = .{ .input = normalized, .bytes = bytes, .fingerprint = fingerprint, .parsed = .image };
        } else return error.UnsupportedDocumentFormat;
        errdefer result.deinit();
        result.page_selection = selection;
        result.max_regions = limits.max_regions;
        const count = try result.pageCount();
        if (count > max_source_pages) return error.DocumentPageLimitExceeded;
        if (selection) |pages| {
            if (pages[pages.len - 1] > count) return error.InvalidDocumentSelection;
        } else if (count > limits.max_pages) return error.DocumentPageLimitExceeded;
        return result;
    }
    pub fn deinit(self: *Source) void {
        if (self.render_template) |*template| template.deinit();
        if (self.parsed == .pdf) self.parsed.pdf.deinit();
    }
    pub fn pageCount(self: *Source) !usize {
        return switch (self.parsed) {
            .pdf => |*p| try p.pageCount(),
            .slides => |s| s.slides.len,
            .image => 1,
        };
    }
    pub fn selected(self: Source, number: usize) bool {
        return selectedPage(self.page_selection, number);
    }

    pub fn analyze(self: *Source, a: A, number: usize, control: c.Control) !c.Page {
        return self.analyzeWithRegionLimit(a, number, self.max_regions, control);
    }
    pub fn analyzeWithRegionLimit(self: *Source, a: A, number: usize, requested_limit: usize, control: c.Control) !c.Page {
        const limit = @min(requested_limit, self.max_regions);
        try control.check();
        var page = c.Page{ .source_id = self.input.id, .number = number, .kind = .image };
        switch (self.parsed) {
            .pdf => |*p| {
                var analysis = p.extractPageTextAnalysisWithRunLimitAlloc(number, limit) catch |err| switch (err) {
                    error.TextRunLimitExceeded => return error.DocumentRegionLimitExceeded,
                    else => return err,
                };
                defer analysis.deinit(p.alloc);
                page.kind = .page;
                page.coordinate_space = .pdf_points;
                page.native_text = try a.dupe(u8, analysis.text);
                const regions = try a.alloc(c.Region, analysis.runs.len);
                for (regions, analysis.runs, 0..) |*region, run, i| {
                    const box = run.bounds();
                    region.* = .{ .id = i, .text = try a.dupe(u8, run.text), .bbox = if (box) |b| .{ b.min_x, b.min_y, b.max_x, b.max_y } else null, .origin = .native };
                }
                page.regions = regions;
                if (analysis.has_unrendered_annotations) try @import("representation.zig").finding(a, &page, "pdf_annotation_rendering_unavailable", true);
                if (analysis.outline_fallback) try @import("representation.zig").finding(a, &page, "native_font_fallback", true);
                if (analysis.has_nontext_visual) try @import("representation.zig").finding(a, &page, "visual_content_requires_inspection", true);
            },
            .slides => |s| {
                if (number == 0 or number > s.slides.len) return error.InvalidDocumentSelection;
                const slide = s.slides[number - 1];
                page.kind = .slide;
                page.coordinate_space = .slide_emu;
                const view = slide.view orelse try ooxml.View.initWithControl(a, try s.archive.get(a, slide.path), control);
                try view.requirePart(.slide);
                page.native_text = try view.text(a);
                page.tables = try view.tables(a, limit);
                // Native extraction alone cannot assert visual completeness.
                if (try view.needsVisualInspection())
                    page.findings = &.{"slide_visual_inspection_unavailable"};
                page.inherited_parts = slide.inherited;
                const ir = @import("representation.zig");
                for (slide.inherited) |part| {
                    if ((part.shapes_visible and part.shape_visual) or (part.background_visible and part.background_visual)) try ir.finding(a, &page, "slide_inherited_visual_inspection_unavailable", true);
                    if (part.conditional_content or (part.shapes_visible and (std.mem.trim(u8, part.text, " \t\r\n").len > 0 or part.tables.len > 0))) try ir.finding(a, &page, "slide_inherited_native_content_requires_composition", true);
                }
                if (slide.hidden and !self.input.include_hidden) return error.HiddenDocumentPage;
                // Notes retain their own checked view, separately from visible OCR.
                if (slide.notes) |notes| page.notes = try notes.text(a);
            },
            .image => {},
        }
        try @import("representation.zig").validatePage(page, limit);
        return page;
    }
    pub fn render(self: *Source, a: A, number: usize, run: *c.Run) !struct { bytes: []const u8, mime_type: []const u8, width: u32, height: u32, effective_dpi: u16, raster_to_source: c.Transform } {
        try run.control.check();
        switch (self.parsed) {
            .pdf => |*p| {
                const request = pdf.PageRenderRequest{ .page_number = number, .max_pixels = run.limits.max_pixels, .max_dimension = @min(run.limits.max_dimension, 4096), .max_output_bytes = run.limits.max_source_bytes };
                const plan = try pdf.prepareParsedPageRenderPlan(p, request);
                const geometry = plan.geometry();
                // Charge the actual initial raster once, before painting. PNG
                // output fitting may shrink it, but cannot refund physical work.
                try run.pixels(geometry.pixels);
                if (self.render_template == null) self.render_template = try p.prepareRenderForkTemplate(p.alloc, p.cancellationProbe());
                const Probe = struct {
                    fn cancelled(raw: ?*const anyopaque) bool {
                        const control: *const c.Control = @ptrCast(@alignCast(raw.?));
                        control.check() catch return true;
                        return false;
                    }
                };
                var batch = try pdf.renderPreparedPagesBatchAlloc(a, p, &.{plan}, .{
                    .max_batch_pages = 1,
                    .max_parallel_pages = 1,
                    .max_inflight_pixels = geometry.pixels,
                    .profile = .exact,
                    .fork_template = &self.render_template.?,
                    .cancellation = .{ .context = &run.control, .is_cancelled_fn = Probe.cancelled },
                });
                defer batch.deinit(a);
                if (batch.results[0].failure) |err| switch (err) {
                    error.UnsupportedPdfAnnotationRendering => return error.DocumentRendererUnavailable,
                    else => return err,
                };
                const rendered = batch.results[0].rendered orelse return error.DocumentRendererUnavailable;
                // Retain the batch's ownership until all transform work succeeds.
                const page_box = try p.extractPageBox(number);
                const rotation = @mod((try p.extractPageRotation(number)) orelse 0, 360);
                const unit = 72.0 / @as(f64, @floatFromInt(geometry.effective_dpi));
                const width: f64 = @floatFromInt(if (rotation == 90 or rotation == 270) geometry.height else geometry.width);
                const height: f64 = @floatFromInt(if (rotation == 90 or rotation == 270) geometry.width else geometry.height);
                const transform: c.Transform = .{ .matrix = switch (rotation) {
                    0 => .{ unit, 0, page_box.min_x, 0, -unit, page_box.min_y + height * unit, 0, 0, 1 },
                    90 => .{ 0, unit, page_box.min_x, unit, 0, page_box.min_y, 0, 0, 1 },
                    180 => .{ -unit, 0, page_box.min_x + width * unit, 0, unit, page_box.min_y, 0, 0, 1 },
                    270 => .{ 0, -unit, page_box.min_x + width * unit, -unit, 0, page_box.min_y + height * unit, 0, 0, 1 },
                    else => return error.InvalidDocumentTransform,
                } };
                // Fitting can use nonintegral per-axis scales. Rounded DPI is
                // advisory; compose exact pixel ratios for crop/evidence geometry.
                const x_scale = @as(f64, @floatFromInt(geometry.width)) / @as(f64, @floatFromInt(rendered.width));
                const y_scale = @as(f64, @floatFromInt(geometry.height)) / @as(f64, @floatFromInt(rendered.height));
                var fitted = transform;
                for (0..3) |row| {
                    fitted.matrix[row * 3] *= x_scale;
                    fitted.matrix[row * 3 + 1] *= y_scale;
                }
                batch.results[0].rendered = null;
                return .{ .raster_to_source = fitted, .bytes = rendered.png, .mime_type = "image/png", .width = rendered.width, .height = rendered.height, .effective_dpi = rendered.effective_dpi };
            },
            .image => {
                const info = try image.inspectEncoded(self.bytes);
                try run.pixels(try info.pixels());
                if (self.input.quadrilateral) |quad| {
                    const rectification = @import("rectification.zig");
                    const target = try rectification.plan(quad, info.width, info.height, run.limits);
                    try run.pixels(@as(u64, target.width) * target.height);
                    const decoded = try @import("../pipelines/image.zig").decodeRgba(a, self.bytes);
                    defer decoded.deinit(a);
                    const rgba = try rectification.warp(a, .{ .data = decoded.data, .width = decoded.width, .height = decoded.height, .format = .rgba8 }, target, run.control);
                    defer a.free(rgba);
                    const Probe = struct {
                        fn cancelled(raw: ?*const anyopaque) bool {
                            const ctl: *const c.Control = @ptrCast(@alignCast(raw.?));
                            ctl.check() catch return true;
                            return false;
                        }
                    };
                    const png = image.png.encodeRgbaWithOptions(a, a, target.width, target.height, rgba, .{
                        .max_output_bytes = run.limits.max_source_bytes,
                        .cancellation = .{ .context = &run.control, .is_cancelled_fn = Probe.cancelled },
                    }) catch |err| {
                        try run.control.check();
                        return if (err == error.ImageTooLarge) error.DocumentSourceLimitExceeded else err;
                    };
                    return .{ .raster_to_source = target.raster_to_source, .bytes = png, .mime_type = "image/png", .width = target.width, .height = target.height, .effective_dpi = 0 };
                }
                return .{ .raster_to_source = .{}, .bytes = self.bytes, .mime_type = self.input.media_type, .width = info.width, .height = info.height, .effective_dpi = 0 };
            },
            .slides => |slides| {
                if (number == 0 or number > slides.slides.len) return error.InvalidDocumentSelection;
                const slide = slides.slides[number - 1];
                if (slide.hidden and !self.input.include_hidden) return error.HiddenDocumentPage;
                // Composition is only admitted when every visible layer has
                // native display support. Never OCR an incomplete slide image.
                if (!slide.theme_supported) return error.DocumentRendererUnavailable;
                const size = slides.size orelse return error.DocumentRendererUnavailable;
                var scratch = std.heap.ArenaAllocator.init(a);
                defer scratch.deinit();
                const work = scratch.allocator();
                var layers = std.ArrayList(drawingml.Layer).empty;
                // Back-to-front master/layout/slide order, retaining the same
                // placeholder and nearest-background decisions as extraction.
                for (0..slide.inherited.len) |index| {
                    const part = slide.inherited[slide.inherited.len - index - 1];
                    if (part.conditional_content) return error.DocumentRendererUnavailable;
                    if (!part.shapes_visible and !part.background_visible) continue;
                    try layers.append(work, .{ .path = part.path, .view = try slides.inheritance.renderView(part.path), .shapes = part.shapes_visible, .background = part.background_visible, .theme = slide.theme });
                }
                try layers.append(work, .{ .path = slide.path, .view = slide.view orelse return error.InvalidDocumentSelection, .background = true, .theme = slide.theme });
                const Formats = @import("antfly_documents").types;
                var rendering: Formats.RenderContext = .{ .control = run.control, .work_allocator = run.work_allocator, .accounting = .{ .ptr = run, .pixels_fn = struct {
                    fn charge(raw: *anyopaque, pixels: u64) !void {
                        const owner: *c.Run = @ptrCast(@alignCast(raw));
                        try owner.pixels(pixels);
                    }
                }.charge } };
                inline for (@typeInfo(Formats.RenderLimits).@"struct".field_names) |name| @field(rendering.limits, name) = @field(run.limits, name);
                const bytes = try drawingml.compile(work, slides.archive, size, layers.items, &rendering);
                var native = try Source.init(scratch.allocator(), .{ .id = self.input.id, .url = "", .media_type = "application/pdf" }, bytes, run.limits, run.control);
                defer native.deinit();
                var rendered = try native.render(a, 1, run);
                // The internal display list uses PDF points with Y up; public
                // slide evidence remains in top-left DrawingML EMU coordinates.
                for (0..3) |column| {
                    rendered.raster_to_source.matrix[column] *= 12700;
                    rendered.raster_to_source.matrix[3 + column] *= -12700;
                }
                rendered.raster_to_source.matrix[5] += size.height;
                return rendered;
            },
        }
    }
};

const normalizedPart = slide_parts.normalizedPart;

test "document sources normalize validated image MIME while enforcing byte identity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const bytes: []const u8 = @import("antfly_documents").fixtures.get("one-pixel.png");
    const readers = @import("antfly_readers");
    for ([_][]const u8{ "image/png", "IMAGE/PNG", "image/png; charset=binary", "Image/PNG; charset=\"binary\"" }) |mime| {
        try readers.validateEncodedRequest(.{ .images = &.{.{ .bytes = bytes, .mime_type = mime }} });
        var source = try Source.init(a, .{ .id = "image", .url = "", .media_type = mime }, bytes, .{}, .{});
        defer source.deinit();
        try std.testing.expectEqualStrings("image/png", source.input.media_type);
        var run = c.Run{ .limits = .{} };
        const rendered = try source.render(a, 1, &run);
        try std.testing.expectEqualStrings("image/png", rendered.mime_type);
        try std.testing.expectEqual(bytes.ptr, rendered.bytes.ptr);
    }
    try std.testing.expectError(error.DocumentMediaTypeMismatch, Source.init(a, .{ .id = "image", .url = "", .media_type = "IMAGE/JPEG; charset=binary" }, bytes, .{}, .{}));
    try std.testing.expectError(error.InvalidDocumentSource, Source.init(a, .{ .id = "image", .url = "", .media_type = "image/png;" }, bytes, .{}, .{}));
}

fn expectNativeRgb(expected: [3]u8, actual: []const u8) !void {
    // Bilinear sampling at rounded output pixels can differ from an exact
    // source-sample center by one or two channel levels.
    for (expected, actual) |left, right| try std.testing.expectApproxEqAbs(@as(f32, @floatFromInt(left)), @as(f32, @floatFromInt(right)), 2);
}

test "document native slide pictures preserve quadrants alpha and slide coordinates" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var source = try Source.init(a, .{ .id = "slide", .url = "", .media_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation" }, @import("antfly_documents").fixtures.get("native-pictures.pptx"), .{}, .{});
    defer source.deinit();
    var run = c.Run{ .limits = .{} };
    defer run.deinit();
    const raster = try source.render(a, 1, &run);
    const decoded = try @import("../pipelines/image.zig").decodeRgba(a, raster.bytes);
    defer decoded.deinit(a);
    const samples = [_]struct { x: f64, y: f64, color: [3]u8 }{
        .{ .x = 0.25, .y = 0.25, .color = .{ 255, 0, 0 } },
        .{ .x = 0.625, .y = 0.25, .color = .{ 0, 0, 255 } },
        .{ .x = 0.875, .y = 0.25, .color = .{ 0, 255, 0 } },
        .{ .x = 0.625, .y = 0.75, .color = .{ 255, 255, 255 } },
        .{ .x = 0.875, .y = 0.75, .color = .{ 255, 255, 0 } },
    };
    for (samples) |sample| {
        const x: usize = @intFromFloat(sample.x * @as(f64, @floatFromInt(decoded.width)));
        const y: usize = @intFromFloat(sample.y * @as(f64, @floatFromInt(decoded.height)));
        const pixel = decoded.data[(y * decoded.width + x) * 4 ..][0..3];
        try expectNativeRgb(sample.color, pixel);
    }
    const corner = try raster.raster_to_source.point(.{ @floatFromInt(raster.width), @floatFromInt(raster.height) });
    // Native PDF raster dimensions ceil fractional DPI extents. The last
    // column can extend beyond the slide by less than one physical pixel.
    const pixel_emu = 72.0 * 12700 / @as(f64, @floatFromInt(raster.effective_dpi));
    try std.testing.expect(corner[0] >= 2540000 and corner[0] - 2540000 < pixel_emu);
    try std.testing.expectApproxEqAbs(@as(f64, 1270000), corner[1], 0.0001);
    try std.testing.expectEqual(@as(usize, 0), run.usage.physical_model_calls);
    try std.testing.expect(run.usage.rendered_pixels > 4);
}

test "document native slide nearest background remains beneath inherited master shapes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var source = try Source.init(a, .{ .id = "slide", .url = "", .media_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation" }, @import("antfly_documents").fixtures.get("native-layers.pptx"), .{}, .{});
    defer source.deinit();
    var run = c.Run{ .limits = .{} };
    defer run.deinit();
    const raster = try source.render(a, 1, &run);
    const decoded = try @import("../pipelines/image.zig").decodeRgba(a, raster.bytes);
    defer decoded.deinit(a);
    const y: usize = decoded.height * 3 / 4;
    const decoration = decoded.data[(y * decoded.width + decoded.width / 4) * 4 ..][0..3];
    const background = decoded.data[(y * decoded.width + decoded.width * 5 / 8) * 4 ..][0..3];
    try std.testing.expectEqualSlices(u8, &.{ 255, 0, 0 }, decoration);
    try expectNativeRgb(.{ 0, 255, 0 }, background);
}

test "document native nested slide groups compose child extents without shifting source coordinates" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var source = try Source.init(a, .{ .id = "group", .url = "", .media_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation" }, @import("antfly_documents").fixtures.get("native-groups.pptx"), .{}, .{});
    defer source.deinit();
    var run = c.Run{ .limits = .{} };
    defer run.deinit();
    const raster = try source.render(a, 1, &run);
    const decoded = try @import("../pipelines/image.zig").decodeRgba(a, raster.bytes);
    defer decoded.deinit(a);
    const samples = [_]struct { x: f64, y: f64, color: [3]u8 }{
        .{ .x = 0.125, .y = 0.5, .color = .{ 255, 255, 255 } },
        .{ .x = 0.375, .y = 0.5, .color = .{ 255, 0, 0 } },
        .{ .x = 0.5625, .y = 0.375, .color = .{ 0, 0, 255 } },
        .{ .x = 0.6875, .y = 0.375, .color = .{ 0, 255, 0 } },
        .{ .x = 0.5625, .y = 0.625, .color = .{ 255, 255, 255 } },
        .{ .x = 0.6875, .y = 0.625, .color = .{ 255, 255, 0 } },
    };
    for (samples) |sample| {
        const x: usize = @intFromFloat(sample.x * @as(f64, @floatFromInt(decoded.width)));
        const y: usize = @intFromFloat(sample.y * @as(f64, @floatFromInt(decoded.height)));
        try expectNativeRgb(sample.color, decoded.data[(y * decoded.width + x) * 4 ..][0..3]);
    }
}

test "document native themed slide paints font outlines above inherited colors" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var source = try Source.init(a, .{ .id = "theme", .url = "", .media_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation" }, @import("antfly_documents").fixtures.get("native-themed-text.pptx"), .{}, .{});
    defer source.deinit();
    const page = try source.analyze(a, 1, .{});
    try std.testing.expect(std.mem.indexOf(u8, page.native_text, "Invoice €12.50") != null);
    var run = c.Run{ .limits = .{} };
    defer run.deinit();
    const raster = try source.render(a, 1, &run);
    const decoded = try @import("../pipelines/image.zig").decodeRgba(a, raster.bytes);
    defer decoded.deinit(a);
    const pixel = decoded.data[(@as(usize, decoded.height) * 3 / 4 * decoded.width + decoded.width / 4) * 4 ..][0..3];
    try expectNativeRgb(.{ 128, 192, 255 }, pixel);
    var ink: usize = 0;
    for (0..decoded.height / 3) |y| for (0..decoded.width / 2) |x| {
        const sample = decoded.data[(y * decoded.width + x) * 4 ..][0..3];
        if (sample[0] < 32 and sample[1] < 32 and sample[2] < 32) ink += 1;
    };
    try std.testing.expect(ink > 30 and ink < @as(usize, decoded.width) * decoded.height / 8);
}

test "document native cached chart values become correctly scaled colored bars" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var source = try Source.init(a, .{ .id = "chart", .url = "", .media_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation" }, @import("antfly_documents").fixtures.get("native-chart.pptx"), .{}, .{});
    defer source.deinit();
    var run = c.Run{ .limits = .{} };
    defer run.deinit();
    const raster = try source.render(a, 1, &run);
    const decoded = try @import("../pipelines/image.zig").decodeRgba(a, raster.bytes);
    defer decoded.deinit(a);
    const samples = [_]struct { x: f64, y: f64, rgb: [3]u8 }{
        .{ .x = 0.35, .y = 0.63, .rgb = .{ 0, 128, 255 } },
        .{ .x = 0.35, .y = 0.23, .rgb = .{ 255, 255, 255 } },
        .{ .x = 0.75, .y = 0.23, .rgb = .{ 0, 128, 255 } },
    };
    for (samples) |sample| {
        const x: usize = @intFromFloat(sample.x * @as(f64, @floatFromInt(decoded.width)));
        const y: usize = @intFromFloat(sample.y * @as(f64, @floatFromInt(decoded.height)));
        try expectNativeRgb(sample.rgb, decoded.data[(y * decoded.width + x) * 4 ..][0..3]);
    }
    try std.testing.expectEqual(@as(usize, 0), run.usage.physical_model_calls);
}

test "document native line and pie charts render their distinct cached data geometry" {
    inline for (.{ .{ @import("antfly_documents").fixtures.get("native-line-chart.pptx"), false }, .{ @import("antfly_documents").fixtures.get("native-pie-chart.pptx"), true } }) |fixture| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var source = try Source.init(a, .{ .id = "chart", .url = "", .media_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation" }, fixture[0], .{}, .{});
        defer source.deinit();
        var run = c.Run{ .limits = .{} };
        defer run.deinit();
        const raster = try source.render(a, 1, &run);
        const decoded = try @import("../pipelines/image.zig").decodeRgba(a, raster.bytes);
        defer decoded.deinit(a);
        if (fixture[1]) {
            const x = decoded.width * 65 / 100;
            const y = decoded.height / 4;
            try expectNativeRgb(.{ 0, 128, 255 }, decoded.data[(@as(usize, y) * decoded.width + x) * 4 ..][0..3]);
            const ox = decoded.width * 45 / 100;
            const oy = decoded.height * 55 / 100;
            try expectNativeRgb(.{ 255, 128, 0 }, decoded.data[(@as(usize, oy) * decoded.width + ox) * 4 ..][0..3]);
        } else {
            var blue: usize = 0;
            for (0..@as(usize, decoded.width) * decoded.height) |i| {
                const pixel = decoded.data[i * 4 ..][0..3];
                if (pixel[0] < 32 and pixel[1] > 96 and pixel[1] < 160 and pixel[2] > 224) blue += 1;
            }
            try std.testing.expect(blue > 50 and blue < 2000);
        }
    }
}

test "document selections spend page budget only on selected PDF pages and slides" {
    inline for (.{ .{ @import("antfly_documents").fixtures.get("two-pages.pdf"), "application/pdf" }, .{ @import("antfly_documents").fixtures.get("ordered-slides.pptx"), "application/vnd.openxmlformats-officedocument.presentationml.presentation" }, .{ @import("antfly_documents").fixtures.get("inherited-slides.pptx"), "application/vnd.openxmlformats-officedocument.presentationml.presentation" } }) |fixture| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var source = try Source.init(a, .{ .id = "source", .url = "", .media_type = fixture[1], .pages = &.{2} }, fixture[0], .{ .max_pages = 1 }, .{});
        defer source.deinit();
        try std.testing.expectEqual(@as(usize, 2), try source.pageCount());
        try std.testing.expect(!source.selected(1));
        try std.testing.expect(source.selected(2));
        const page = try source.analyze(a, 2, .{});
        try std.testing.expectEqual(@as(usize, 2), page.number);
        if (source.parsed == .slides) {
            try std.testing.expectEqual(@as(usize, 0), source.parsed.slides.slides[0].inherited.len);
            try std.testing.expect(source.parsed.slides.slides[0].view == null);
        }
        try std.testing.expectError(error.DocumentPageLimitExceeded, Source.init(a, .{ .id = "source", .url = "", .media_type = fixture[1] }, fixture[0], .{ .max_pages = 1 }, .{}));
        try std.testing.expectError(error.DocumentPageLimitExceeded, Source.init(a, .{ .id = "source", .url = "", .media_type = fixture[1], .pages = &.{ 1, 2 } }, fixture[0], .{ .max_pages = 1 }, .{}));
        try std.testing.expectError(error.InvalidDocumentSelection, Source.init(a, .{ .id = "source", .url = "", .media_type = fixture[1], .pages = &.{3} }, fixture[0], .{ .max_pages = 1 }, .{}));
        try std.testing.expectError(error.InvalidDocumentSelection, Source.init(a, .{ .id = "source", .url = "", .media_type = fixture[1], .pages = &.{ 1, 1 } }, fixture[0], .{}, .{}));
    }
}

test "document PDF analysis preserves transformed regions and enforces active limits" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var source = try Source.init(a, .{ .id = "geometry", .url = "", .media_type = "application/pdf" }, @import("antfly_documents").fixtures.get("transformed-text.pdf"), .{}, .{});
    defer source.deinit();
    const page = try source.analyze(a, 1, .{});
    try std.testing.expectEqual(@as(usize, 4), page.regions.len);
    for (page.regions) |region| {
        const box = region.bbox.?;
        if (std.mem.eql(u8, region.text, "Rotated text")) try std.testing.expect(box[3] - box[1] > 2 * (box[2] - box[0]));
        if (std.mem.eql(u8, region.text, "Scaled text")) try std.testing.expect(box[3] - box[1] > 30);
        if (std.mem.eql(u8, region.text, "Mirrored text")) try std.testing.expectApproxEqAbs(@as(f64, 200), box[2], 0.000001);
    }
    try std.testing.expectError(error.DocumentRegionLimitExceeded, source.analyzeWithRegionLimit(a, 1, 3, .{}));
    var excess = try Source.init(a, .{ .id = "excess", .url = "", .media_type = "application/pdf" }, @import("antfly_documents").fixtures.get("excess-text-runs.pdf"), .{}, .{});
    defer excess.deinit();
    try std.testing.expectError(error.DocumentRegionLimitExceeded, excess.analyze(a, 1, .{}));
}

test "document PDF encoding enforces byte admission reuses source forks and composes fitted geometry" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const bytes = @import("antfly_documents").fixtures.get("two-pages.pdf");
    var source = try Source.init(a, .{ .id = "pdf", .url = "", .media_type = "application/pdf" }, bytes, .{}, .{});
    defer source.deinit();
    var original_run = c.Run{ .limits = .{} };
    const original = try source.render(a, 1, &original_run);
    const frozen_entries = source.render_template.?.xref_entries.ptr;
    var fitted_run = c.Run{ .limits = .{ .max_source_bytes = bytes.len } };
    const fitted = try source.render(a, 1, &fitted_run);
    try std.testing.expect(original.bytes.len > bytes.len);
    try std.testing.expect(fitted.bytes.len <= bytes.len);
    try std.testing.expectEqual(original_run.usage.rendered_pixels, fitted_run.usage.rendered_pixels);
    try std.testing.expect(fitted_run.usage.rendered_pixels > @as(u64, fitted.width) * fitted.height);
    for ([_][2]f64{ .{ 0, 0 }, .{ 0.5, 0.5 }, .{ 1, 1 } }) |fraction| {
        const before = try original.raster_to_source.point(.{ fraction[0] * @as(f64, @floatFromInt(original.width)), fraction[1] * @as(f64, @floatFromInt(original.height)) });
        const after = try fitted.raster_to_source.point(.{ fraction[0] * @as(f64, @floatFromInt(fitted.width)), fraction[1] * @as(f64, @floatFromInt(fitted.height)) });
        try std.testing.expectApproxEqAbs(before[0], after[0], 0.000001);
        try std.testing.expectApproxEqAbs(before[1], after[1], 0.000001);
    }
    const second = try source.render(a, 2, &fitted_run);
    try std.testing.expect(second.bytes.len <= bytes.len);
    try std.testing.expectEqual(frozen_entries, source.render_template.?.xref_entries.ptr);
    try std.testing.expectEqual(@as(usize, 0), fitted_run.usage.physical_model_calls);
    var tiny = c.Run{ .limits = .{ .max_source_bytes = 1 } };
    try std.testing.expectError(error.RenderedPageOutputTooLarge, source.render(a, 1, &tiny));
    try std.testing.expectEqual(@as(usize, 0), tiny.usage.physical_model_calls);
    const Cancelled = struct {
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks >= 3) return error.Canceled;
        }
    };
    var cancelled = Cancelled{};
    var interrupted = c.Run{ .limits = .{}, .control = .{ .ptr = &cancelled, .check_fn = Cancelled.check } };
    try std.testing.expectError(error.Canceled, source.render(a, 1, &interrupted));
    try std.testing.expect(interrupted.usage.rendered_pixels > 0);
}

test "document visible slide views exclude hidden shapes across inheritance notes and extraction evidence" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]bool{ false, true }) |include_hidden| {
        var source = try Source.init(a, .{ .id = "slides", .url = "", .media_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation", .include_hidden = include_hidden, .include_notes = true }, @import("antfly_documents").fixtures.get("hidden-shapes.pptx"), .{ .max_regions = 1 }, .{});
        defer source.deinit();
        const cached = source.parsed.slides.slides[0].view.?.bytes.ptr;
        const native = try source.analyze(a, 1, .{});
        try std.testing.expectEqualStrings("Speaker note\n", native.notes);
        try std.testing.expect(std.mem.indexOf(u8, native.native_text, "12.50") != null);
        try std.testing.expect(std.mem.indexOf(u8, native.native_text, "Obsolete") == null);
        try std.testing.expectEqual(@as(usize, 1), native.tables.len);
        try std.testing.expectEqualStrings("Public cell\n", native.tables[0].cells[0].text);
        try std.testing.expectEqual(@as(usize, 0), native.findings.len);
        try std.testing.expectEqual(@as(usize, 2), native.inherited_parts.len);
        for (native.inherited_parts) |part| {
            try std.testing.expectEqualStrings("", part.text);
            try std.testing.expectEqual(@as(usize, 0), part.tables.len);
            try std.testing.expect(!part.shape_visual and !part.conditional_content);
        }
        _ = try source.analyze(a, 1, .{});
        try std.testing.expectEqual(cached, source.parsed.slides.slides[0].view.?.bytes.ptr);
        const ex = @import("executor.zig");
        const Fake = struct {
            fn read(_: *anyopaque, _: A, _: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) anyerror!ex.Candidate {
                return error.UnexpectedCall;
            }
        };
        var marker: u8 = 0;
        var run = c.Run{ .limits = .{} };
        const page = try ex.readPage(a, .{ .models = &.{.{ .name = "leaf", .model = "leaf", .operation = .read }}, .read = .{ .reader = "leaf" } }, .{ .ptr = &marker, .read_fn = Fake.read }, 0, native, &run);
        try std.testing.expectEqual(c.Status.complete, page.status);
        try std.testing.expectEqual(@as(usize, 0), run.usage.model_calls);
        const metadata = try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, .{ .source_id = page.source_id, .segments = .{.{ .number = page.number, .start = 0, .end = page.text.len, .origin = page.origin }} }, .{}), .{});
        const start = std.mem.indexOf(u8, page.text, "12.50").?;
        for ([_][]const u8{ "12.50", "999.99" }, [_]bool{ true, false }) |value, supported| {
            var item = try std.json.parseFromSliceLeaky(std.json.Value, a, try std.fmt.allocPrint(a, "{{\"structures\":{{\"record\":[{{\"amount\":{{\"value\":\"{s}\",\"start\":{d},\"end\":{d}}}}}]}}}}", .{ value, start, start + value.len }), .{});
            try std.testing.expectEqual(supported, try @import("evidence.zig").attach(a, &item, page.text, metadata, true, false));
        }
    }
}

test "document encoded hidden slide flags obey selection and include hidden" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]bool{ false, true }) |include_hidden| {
        var source = try Source.init(a, .{ .id = "slides", .url = "", .media_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation", .include_hidden = include_hidden }, @import("antfly_documents").fixtures.get("encoded-hidden-slides.pptx"), .{}, .{});
        defer source.deinit();
        if (include_hidden) {
            const hidden = try source.analyze(a, 1, .{});
            try std.testing.expectEqualStrings("Slide 2\n", hidden.native_text);
        } else try std.testing.expectError(error.HiddenDocumentPage, source.analyze(a, 1, .{}));
        const visible = try source.analyze(a, 2, .{});
        try std.testing.expectEqualStrings("Slide 1\n", visible.native_text);
    }
}
