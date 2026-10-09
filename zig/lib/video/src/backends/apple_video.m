// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#include "apple_video.h"
#import <Foundation/Foundation.h>
#import <VideoToolbox/VideoToolbox.h>
#import <CoreVideo/CoreVideo.h>
#import <Metal/Metal.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    VTDecompressionSessionRef session;
    CMVideoFormatDescriptionRef format;
    av_output output;
    void *context;
} AVDecoder;
static void output_frame(void *context, void *ref, OSStatus status,
                         VTDecodeInfoFlags flags, CVImageBufferRef image,
                         CMTime pts, CMTime duration) {
    (void)flags; (void)pts; (void)duration;
    AVDecoder *decoder = context;
    decoder->output(decoder->context, (size_t)(uintptr_t)ref - 1, status, image);
}
int32_t av_decoder_create(const uint8_t *config, size_t length, uint32_t width, uint32_t height, int hardware,
                         av_output output, void *context, void **result) {
    *result = NULL;
    if (length < 7 || config[0] != 1 || !output) return paramErr;
    const uint8_t *sets[286]; size_t sizes[286]; size_t count = 0, cursor = 6;
    unsigned sps = config[5] & 31;
    for (unsigned group = 0; group < 2; ++group) {
        unsigned n = sps;
        if (group) { if (cursor >= length) return paramErr; n = config[cursor++]; }
        for (unsigned i = 0; i < n; ++i) {
            if (length - cursor < 2) return paramErr;
            size_t size = ((size_t)config[cursor] << 8) | config[cursor + 1]; cursor += 2;
            if (!size || size > length - cursor || count >= 286) return paramErr;
            sets[count] = config + cursor; sizes[count++] = size; cursor += size;
        }
    }
    AVDecoder *d = calloc(1, sizeof(*d));
    if (!d) return memFullErr;
    OSStatus status = CMVideoFormatDescriptionCreateFromH264ParameterSets(
        kCFAllocatorDefault, count, sets, sizes, (config[4] & 3) + 1, &d->format);
    if (status) { free(d); return status; }
    CMVideoDimensions dimensions = CMVideoFormatDescriptionGetDimensions(d->format);
    if (dimensions.width <= 0 || dimensions.height <= 0 || (uint32_t)dimensions.width != width || (uint32_t)dimensions.height != height) {
        CFRelease(d->format); free(d); return paramErr;
    }
    d->output = output; d->context = context;
    @autoreleasepool {
        NSDictionary *spec = @{
            (__bridge NSString *)kVTVideoDecoderSpecification_RequireHardwareAcceleratedVideoDecoder: (hardware ? @YES : @NO)
        };
        NSDictionary *attrs = @{
            (__bridge NSString *)kCVPixelBufferPixelFormatTypeKey: @(kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange),
            (__bridge NSString *)kCVPixelBufferMetalCompatibilityKey: @YES,
            (__bridge NSString *)kCVPixelBufferIOSurfacePropertiesKey: @{}
        };
        VTDecompressionOutputCallbackRecord callback = { output_frame, d };
        status = VTDecompressionSessionCreate(kCFAllocatorDefault, d->format,
            (__bridge CFDictionaryRef)spec, (__bridge CFDictionaryRef)attrs, &callback, &d->session);
    }
    if (status) { CFRelease(d->format); free(d); return status; }
    *result = d;
    return 0;
}
int32_t av_decoder_submit(void *handle, const uint8_t *bytes, size_t length,
                         size_t index, int64_t pts, int64_t dts, uint32_t duration, uint32_t scale) {
    if (!handle || !length || !scale || scale > INT32_MAX || index == SIZE_MAX) return paramErr;
    AVDecoder *d = handle; CMBlockBufferRef block = NULL; CMSampleBufferRef sample = NULL;
    OSStatus status = CMBlockBufferCreateWithMemoryBlock(kCFAllocatorDefault, NULL, length,
        kCFAllocatorDefault, NULL, 0, length, 0, &block);
    if (status) return status;
    status = CMBlockBufferReplaceDataBytes(bytes, block, 0, length);
    CMSampleTimingInfo timing = { CMTimeMake(duration, scale), CMTimeMake(pts, scale), CMTimeMake(dts, scale) };
    if (!status) status = CMSampleBufferCreateReady(kCFAllocatorDefault, block, d->format,
        1, 1, &timing, 1, &length, &sample);
    if (!status) status = VTDecompressionSessionDecodeFrame(d->session, sample, 0,
        (void *)(uintptr_t)(index + 1), NULL);
    if (sample) CFRelease(sample);
    CFRelease(block);
    return status;
}
int32_t av_decoder_drain(void *handle) {
    return VTDecompressionSessionWaitForAsynchronousFrames(((AVDecoder *)handle)->session);
}
void av_decoder_destroy(void *handle) {
    if (!handle) return;
    AVDecoder *d = handle;
    VTDecompressionSessionWaitForAsynchronousFrames(d->session);
    VTDecompressionSessionInvalidate(d->session);
    CFRelease(d->session); CFRelease(d->format); free(d);
}
int av_decoder_hardware(void *handle) {
    CFTypeRef value = NULL;
    OSStatus status = VTSessionCopyProperty(((AVDecoder *)handle)->session,
        kVTDecompressionPropertyKey_UsingHardwareAcceleratedVideoDecoder, kCFAllocatorDefault, &value);
    int result = !status && value == kCFBooleanTrue;
    if (value) CFRelease(value);
    return result;
}
void av_surface_retain(void *p) { CVPixelBufferRetain(p); }
void av_surface_release(void *p) { CVPixelBufferRelease(p); }
uint32_t av_surface_width(void *p) { return (uint32_t)CVPixelBufferGetWidth(p); }
uint32_t av_surface_height(void *p) { return (uint32_t)CVPixelBufferGetHeight(p); }
uint32_t av_surface_format(void *p) { return CVPixelBufferGetPixelFormatType(p); }
uint64_t av_surface_storage_bytes(void *p) {
    uint64_t size = 0;
    for (size_t i = 0; i < CVPixelBufferGetPlaneCount(p); ++i)
        size += (uint64_t)CVPixelBufferGetBytesPerRowOfPlane(p, i) * CVPixelBufferGetHeightOfPlane(p, i);
    return size;
}
int32_t av_surface_lock(void *p) { return CVPixelBufferLockBaseAddress(p, kCVPixelBufferLock_ReadOnly); }
void av_surface_unlock(void *p) { CVPixelBufferUnlockBaseAddress(p, kCVPixelBufferLock_ReadOnly); }
const uint8_t *av_surface_plane(void *p, size_t plane, size_t *stride, size_t *width, size_t *height) {
    if (plane >= CVPixelBufferGetPlaneCount(p)) return NULL;
    *stride = CVPixelBufferGetBytesPerRowOfPlane(p, plane);
    *width = CVPixelBufferGetWidthOfPlane(p, plane);
    *height = CVPixelBufferGetHeightOfPlane(p, plane);
    return CVPixelBufferGetBaseAddressOfPlane(p, plane);
}
typedef struct { CVMetalTextureCacheRef cache; } AVMetal;
typedef struct { CVPixelBufferRef pixel; CVMetalTextureRef planes[2]; } AVImport;
int32_t av_metal_create(void *device, void **out) {
    *out = NULL;
    if (!device) return paramErr;
    AVMetal *m = calloc(1, sizeof(*m)); if (!m) return memFullErr;
    CVReturn status = CVMetalTextureCacheCreate(kCFAllocatorDefault, NULL,
        (__bridge id<MTLDevice>)device, NULL, &m->cache);
    if (status) { free(m); return status; }
    *out = m; return 0;
}
void av_metal_destroy(void *p) {
    if (p) { AVMetal *m = p; CFRelease(m->cache); free(m); }
}
int32_t av_metal_import(void *cache, void *pixel, void **out) {
    *out = NULL;
    if (!cache || !pixel || CVPixelBufferGetPlaneCount(pixel) != 2) return paramErr;
    OSType format = CVPixelBufferGetPixelFormatType(pixel);
    if (format != kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange &&
        format != kCVPixelFormatType_420YpCbCr8BiPlanarFullRange) return paramErr;
    AVImport *p = calloc(1, sizeof(*p)); if (!p) return memFullErr;
    p->pixel = CVPixelBufferRetain(pixel);
    for (size_t i = 0; i < 2; ++i) {
        CVReturn status = CVMetalTextureCacheCreateTextureFromImage(kCFAllocatorDefault,
            ((AVMetal *)cache)->cache, pixel, NULL, i ? MTLPixelFormatRG8Unorm : MTLPixelFormatR8Unorm,
            CVPixelBufferGetWidthOfPlane(pixel, i), CVPixelBufferGetHeightOfPlane(pixel, i), i, &p->planes[i]);
        if (status) { av_metal_import_release(p); return status; }
    }
    *out = p; return 0;
}
void *av_metal_plane_texture(void *p, size_t i) {
    if (!p || i > 1) return NULL;
    return (__bridge void *)CVMetalTextureGetTexture(((AVImport *)p)->planes[i]);
}
void av_metal_import_release(void *handle) {
    if (!handle) return;
    AVImport *p = handle;
    for (size_t i = 0; i < 2; ++i) if (p->planes[i]) CFRelease(p->planes[i]);
    if (p->pixel) CVPixelBufferRelease(p->pixel);
    free(p);
}
