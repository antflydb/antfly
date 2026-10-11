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

#import <Metal/Metal.h>
void *av_test_device_create(void) { return (__bridge_retained void *)MTLCreateSystemDefaultDevice(); }
void av_test_device_destroy(void *p) { if (p) { id object = (__bridge_transfer id)p; (void)object; } }
#import <CoreVideo/CoreVideo.h>
#include <stdint.h>
#include <string.h>
static void *surface_create(const uint8_t *bytes, uint32_t width, uint32_t height, int full, int depth) {
    @autoreleasepool {
        NSDictionary *attrs = @{
            (__bridge NSString *)kCVPixelBufferMetalCompatibilityKey: @YES,
            (__bridge NSString *)kCVPixelBufferIOSurfacePropertiesKey: @{}
        };
        CVPixelBufferRef pixel = NULL;
        if (CVPixelBufferCreate(kCFAllocatorDefault, width, height,
            depth == 10 ? (full ? kCVPixelFormatType_420YpCbCr10BiPlanarFullRange : kCVPixelFormatType_420YpCbCr10BiPlanarVideoRange) :
            (full ? kCVPixelFormatType_420YpCbCr8BiPlanarFullRange : kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange),
            (__bridge CFDictionaryRef)attrs, &pixel)) return NULL;
        if (CVPixelBufferLockBaseAddress(pixel, 0)) { CVPixelBufferRelease(pixel); return NULL; }
        for (size_t plane = 0; plane < 2; ++plane) {
            size_t h = CVPixelBufferGetHeightOfPlane(pixel, plane);
            size_t stride = CVPixelBufferGetBytesPerRowOfPlane(pixel, plane);
            uint8_t *out = CVPixelBufferGetBaseAddressOfPlane(pixel, plane);
            size_t row = (size_t)width * (depth == 10 ? 2 : 1);
            const uint8_t *in = bytes + (plane ? row * height : 0);
            for (size_t y = 0; y < h; ++y) memcpy(out + y * stride, in + y * row, row);
        }
        CVPixelBufferUnlockBaseAddress(pixel, 0); return pixel;
    }
}
void *av_test_surface_create(const uint8_t *bytes, uint32_t width, uint32_t height, int full) {
    return surface_create(bytes, width, height, full, 8);
}
void *av_test_p010_surface_create(const uint8_t *bytes, uint32_t width, uint32_t height, int full) {
    return surface_create(bytes, width, height, full, 10);
}
void av_test_surface_destroy(void *p) { if (p) CVPixelBufferRelease(p); }
// Test-only producer for direct integer texture imports; caller owns one retain.
void *av_test_native_texture(void *device, const uint8_t *bytes, uint32_t width,
    uint32_t height, size_t stride, uint32_t depth, uint32_t channels) {
    @autoreleasepool {
        id<MTLDevice> d = (__bridge id<MTLDevice>)device;
        MTLPixelFormat format = depth == 8 ? (channels == 1 ? MTLPixelFormatR8Uint : MTLPixelFormatRG8Uint)
            : (channels == 1 ? MTLPixelFormatR16Uint : MTLPixelFormatRG16Uint);
        MTLTextureDescriptor *desc = [MTLTextureDescriptor texture2DDescriptorWithPixelFormat:format width:width height:height mipmapped:NO];
        desc.storageMode = MTLStorageModeShared; desc.usage = MTLTextureUsageShaderRead;
        id<MTLTexture> texture = [d newTextureWithDescriptor:desc];
        if (!texture) return NULL;
        [texture replaceRegion:MTLRegionMake2D(0, 0, width, height) mipmapLevel:0 withBytes:bytes bytesPerRow:stride];
        return (__bridge_retained void *)texture;
    }
}
void av_test_native_texture_destroy(void *p) { if (p) { id object = (__bridge_transfer id)p; (void)object; } }
