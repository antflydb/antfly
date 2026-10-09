// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#import <Metal/Metal.h>
void *av_test_device_create(void) { return (__bridge_retained void *)MTLCreateSystemDefaultDevice(); }
void av_test_device_destroy(void *p) { if (p) { id object = (__bridge_transfer id)p; (void)object; } }
#import <CoreVideo/CoreVideo.h>
#include <stdint.h>
#include <string.h>
void *av_test_surface_create(const uint8_t *bytes, uint32_t width, uint32_t height, int full) {
    @autoreleasepool {
        NSDictionary *attrs = @{
            (__bridge NSString *)kCVPixelBufferMetalCompatibilityKey: @YES,
            (__bridge NSString *)kCVPixelBufferIOSurfacePropertiesKey: @{}
        };
        CVPixelBufferRef pixel = NULL;
        if (CVPixelBufferCreate(kCFAllocatorDefault, width, height,
            full ? kCVPixelFormatType_420YpCbCr8BiPlanarFullRange : kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange,
            (__bridge CFDictionaryRef)attrs, &pixel)) return NULL;
        if (CVPixelBufferLockBaseAddress(pixel, 0)) { CVPixelBufferRelease(pixel); return NULL; }
        for (size_t plane = 0; plane < 2; ++plane) {
            size_t h = CVPixelBufferGetHeightOfPlane(pixel, plane);
            size_t stride = CVPixelBufferGetBytesPerRowOfPlane(pixel, plane);
            uint8_t *out = CVPixelBufferGetBaseAddressOfPlane(pixel, plane);
            const uint8_t *in = bytes + (plane ? (size_t)width * height : 0);
            for (size_t y = 0; y < h; ++y) memcpy(out + y * stride, in + y * width, width);
        }
        CVPixelBufferUnlockBaseAddress(pixel, 0); return pixel;
    }
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
