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
static OSStatus create_session(AVDecoder *d, OSType pixel, int hardware) {
    @autoreleasepool {
        NSDictionary *spec = @{
            (__bridge NSString *)kVTVideoDecoderSpecification_EnableHardwareAcceleratedVideoDecoder: @YES,
            (__bridge NSString *)kVTVideoDecoderSpecification_RequireHardwareAcceleratedVideoDecoder: @(hardware != 0)
        };
        NSDictionary *attrs = @{
            (__bridge NSString *)kCVPixelBufferPixelFormatTypeKey: @(pixel),
            (__bridge NSString *)kCVPixelBufferMetalCompatibilityKey: @YES,
            (__bridge NSString *)kCVPixelBufferIOSurfacePropertiesKey: @{}
        };
        VTDecompressionOutputCallbackRecord callback = { output_frame, d };
        return VTDecompressionSessionCreate(kCFAllocatorDefault, d->format,
            (__bridge CFDictionaryRef)spec, (__bridge CFDictionaryRef)attrs, &callback, &d->session);
    }
}
int av_decoder_supports(uint32_t codec) {
    if (__builtin_available(macOS 10.13, *)) return VTIsHardwareDecodeSupported(codec);
    return 0;
}
// Codec IDs and configuration atoms are values, not references to symbols added
// by newer SDKs. Decoder creation establishes actual OS/profile availability.
int32_t av_decoder_create_coded(uint32_t codec, const uint8_t *config, size_t length,
                               uint32_t width, uint32_t height, uint32_t pixel, int hardware,
                               av_output output, void *context, void **result) {
    *result = NULL;
    if (!output || !config || !length || !width || !height || width > 16384 || height > 16384 ||
        (codec != 0x76703039 && codec != 0x61763031)) return paramErr;
    AVDecoder *d = calloc(1, sizeof(*d));
    if (!d) return memFullErr;
    d->output = output; d->context = context;
    @autoreleasepool {
        if (__builtin_available(macOS 11.0, *)) VTRegisterSupplementalVideoDecoderIfAvailable(codec);
        NSString *atom = codec == 0x76703039 ? @"vpcC" : @"av1C";
        NSData *data = [NSData dataWithBytes:config length:length];
        NSDictionary *extensions = @{
            (__bridge NSString *)kCMFormatDescriptionExtension_SampleDescriptionExtensionAtoms: @{atom: data}
        };
        OSStatus status = CMVideoFormatDescriptionCreate(kCFAllocatorDefault, codec,
            width, height, (__bridge CFDictionaryRef)extensions, &d->format);
        if (!status) status = create_session(d, pixel, hardware);
        if (status) {
            if (d->format) CFRelease(d->format);
            free(d); return status;
        }
    }
    *result = d; return 0;
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
    status = create_session(d, kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange, hardware);
    if (status) { CFRelease(d->format); free(d); return status; }
    *result = d;
    return 0;
}
static OSStatus submit_sample(void *handle, const uint8_t *bytes, size_t length,
                              size_t index, CMTime pts, CMTime dts, CMTime duration) {
    if (!handle || !bytes || !length || index == SIZE_MAX) return paramErr;
    AVDecoder *d = handle; CMBlockBufferRef block = NULL; CMSampleBufferRef sample = NULL;
    OSStatus status = CMBlockBufferCreateWithMemoryBlock(kCFAllocatorDefault, NULL, length,
        kCFAllocatorDefault, NULL, 0, length, 0, &block);
    if (status) return status;
    status = CMBlockBufferReplaceDataBytes(bytes, block, 0, length);
    CMSampleTimingInfo timing = { duration, pts, dts };
    if (!status) status = CMSampleBufferCreateReady(kCFAllocatorDefault, block, d->format,
        1, 1, &timing, 1, &length, &sample);
    if (!status) status = VTDecompressionSessionDecodeFrame(d->session, sample, 0,
        (void *)(uintptr_t)(index + 1), NULL);
    if (sample) CFRelease(sample);
    CFRelease(block);
    return status;
}
int32_t av_decoder_submit(void *handle, const uint8_t *bytes, size_t length,
                         size_t index, int64_t pts, int64_t dts, uint32_t duration, uint32_t scale) {
    if (!scale || scale > INT32_MAX) return paramErr;
    return submit_sample(handle, bytes, length, index, CMTimeMake(pts, scale),
                         CMTimeMake(dts, scale), CMTimeMake(duration, scale));
}
int32_t av_decoder_submit_coded(void *handle, const uint8_t *bytes, size_t length,
                               size_t index, int64_t pts, int64_t duration, uint32_t scale) {
    if (!scale || scale > INT32_MAX) return paramErr;
    // WebM supplies presentation time and optional duration, never a DTS.
    return submit_sample(handle, bytes, length, index, CMTimeMake(pts, scale),
                         kCMTimeInvalid, duration < 0 ? kCMTimeInvalid : CMTimeMake(duration, scale));
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
typedef struct { CVMetalTextureCacheRef cache; CVPixelBufferRef pixel; CVMetalTextureRef planes[2]; } AVImport;
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
        format != kCVPixelFormatType_420YpCbCr8BiPlanarFullRange &&
        format != kCVPixelFormatType_420YpCbCr10BiPlanarVideoRange &&
        format != kCVPixelFormatType_420YpCbCr10BiPlanarFullRange) return paramErr;
    const bool high = format == kCVPixelFormatType_420YpCbCr10BiPlanarVideoRange || format == kCVPixelFormatType_420YpCbCr10BiPlanarFullRange;
    AVImport *p = calloc(1, sizeof(*p)); if (!p) return memFullErr;
    // CoreVideo texture wrappers may call back into their originating cache on
    // release. Preserve it when imports outlive their creating cache/preparer.
    p->cache = (CVMetalTextureCacheRef)CFRetain(((AVMetal *)cache)->cache);
    p->pixel = CVPixelBufferRetain(pixel);
    for (size_t i = 0; i < 2; ++i) {
        CVReturn status = CVMetalTextureCacheCreateTextureFromImage(kCFAllocatorDefault,
            ((AVMetal *)cache)->cache, pixel, NULL, high ? (i ? MTLPixelFormatRG16Unorm : MTLPixelFormatR16Unorm) : (i ? MTLPixelFormatRG8Unorm : MTLPixelFormatR8Unorm),
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
    if (p->cache) CFRelease(p->cache);
    free(p);
}
