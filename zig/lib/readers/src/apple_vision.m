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

#import <Foundation/Foundation.h>
#import <Vision/Vision.h>
#import <ImageIO/ImageIO.h>
#import <CoreGraphics/CoreGraphics.h>
#include <pthread.h>
#import "apple_vision.h"

static int recognize(const AntflyVisionInput *input) {
    if (@available(macOS 10.15, *)) {
        if (input->should_cancel(input->context)) return 4;
        NSData *optionsData = [NSData dataWithBytesNoCopy:(void *)input->options
                                                  length:input->options_length freeWhenDone:NO];
        NSDictionary *options = [NSJSONSerialization JSONObjectWithData:optionsData options:0 error:NULL];
        if (![options isKindOfClass:[NSDictionary class]]) return 1;

        CGImageRef image = NULL;
        CGImagePropertyOrientation orientation = kCGImagePropertyOrientationUp;
        if (input->width) {
            if (!input->height || input->stride < (size_t)input->width * 4 ||
                input->stride > SIZE_MAX / input->height ||
                input->length != input->stride * input->height) return 1;
            if ((uint64_t)input->width * input->height > input->max_pixels) return 6;
            CGDataProviderRef provider = CGDataProviderCreateWithData(NULL, input->bytes, input->length, NULL);
            CGColorSpaceRef colors = CGColorSpaceCreateDeviceRGB();
            if (provider && colors) image = CGImageCreate(input->width, input->height, 8, 32,
                input->stride, colors, kCGImageAlphaLast | kCGBitmapByteOrderDefault,
                provider, NULL, false, kCGRenderingIntentDefault);
            if (colors) CGColorSpaceRelease(colors);
            if (provider) CGDataProviderRelease(provider);
        } else {
            NSData *data = [NSData dataWithBytesNoCopy:(void *)input->bytes length:input->length freeWhenDone:NO];
            CGImageSourceRef source = CGImageSourceCreateWithData((__bridge CFDataRef)data, NULL);
            if (!source) return 1;
            NSDictionary *properties = CFBridgingRelease(CGImageSourceCopyPropertiesAtIndex(source, 0, NULL));
            uint64_t width = [properties[(__bridge NSString *)kCGImagePropertyPixelWidth] unsignedLongLongValue];
            uint64_t height = [properties[(__bridge NSString *)kCGImagePropertyPixelHeight] unsignedLongLongValue];
            if (!width || !height || width > input->max_pixels / height) {
                CFRelease(source);
                return 6;
            }
            NSNumber *rotation = properties[(__bridge NSString *)kCGImagePropertyOrientation];
            if (rotation && rotation.unsignedIntValue >= 1 && rotation.unsignedIntValue <= 8)
                orientation = (CGImagePropertyOrientation)rotation.unsignedIntValue;
            image = CGImageSourceCreateImageAtIndex(source, 0, NULL);
            CFRelease(source);
        }
        if (!image) return 1;
        if ((uint64_t)CGImageGetWidth(image) > input->max_pixels / CGImageGetHeight(image)) {
            CGImageRelease(image);
            return 6;
        }
        double width = CGImageGetWidth(image), height = CGImageGetHeight(image);
        if (orientation >= kCGImagePropertyOrientationLeftMirrored) {
            double temp = width; width = height; height = temp;
        }

        VNRecognizeTextRequest *request = [[VNRecognizeTextRequest alloc] init];
        request.recognitionLevel = [options[@"recognition_level"] isEqual:@"fast"]
            ? VNRequestTextRecognitionLevelFast : VNRequestTextRecognitionLevelAccurate;
        request.usesLanguageCorrection = [options[@"uses_language_correction"] boolValue];
        NSArray *languages = options[@"recognition_languages"];
        NSError *error = nil;
        NSArray *supported = [request supportedRecognitionLanguagesAndReturnError:&error];
        if (error) { CGImageRelease(image); return 2; }
        if (![languages isKindOfClass:[NSArray class]] || !languages.count) {
            CGImageRelease(image); return 1;
        }
        for (NSString *language in languages) {
            if (![language isKindOfClass:[NSString class]] || ![supported containsObject:language]) {
                CGImageRelease(image); return 3;
            }
        }
        request.recognitionLanguages = languages;
        request.progressHandler = ^(VNRequest *active, double progress, NSError *progressError) {
            (void)progress; (void)progressError;
            if (input->should_cancel(input->context)) [active cancel];
        };
        VNImageRequestHandler *handler = [[VNImageRequestHandler alloc] initWithCGImage:image
            orientation:orientation options:@{}];
        BOOL success = [handler performRequests:@[request] error:&error];
        CGImageRelease(image);
        if (input->should_cancel(input->context)) return 4;
        if (!success || error) return 2;

        // Stable top-to-bottom order, then left-to-right for equal top edges.
        // Complex column/table reconstruction is outside this plain-text adapter.
        NSArray<VNRecognizedTextObservation *> *observations = [request.results
            sortedArrayUsingComparator:^NSComparisonResult(VNRecognizedTextObservation *a, VNRecognizedTextObservation *b) {
                double ay = CGRectGetMaxY(a.boundingBox), by = CGRectGetMaxY(b.boundingBox);
                if (ay != by) return ay > by ? NSOrderedAscending : NSOrderedDescending;
                double ax = CGRectGetMinX(a.boundingBox), bx = CGRectGetMinX(b.boundingBox);
                return ax < bx ? NSOrderedAscending : ax > bx ? NSOrderedDescending : NSOrderedSame;
            }];
        for (VNRecognizedTextObservation *observation in observations) {
            if (input->should_cancel(input->context)) return 4;
            VNRecognizedText *candidate = [observation topCandidates:1].firstObject;
            if (!candidate) continue;
            NSData *text = [candidate.string dataUsingEncoding:NSUTF8StringEncoding];
            CGRect box = observation.boundingBox;
            if (input->region(input->context, text.bytes, text.length,
                CGRectGetMinX(box) * width, (1 - CGRectGetMaxY(box)) * height,
                CGRectGetMaxX(box) * width, (1 - CGRectGetMinY(box)) * height,
                candidate.confidence)) return 4;
        }
        return 0;
    }
    return 7;
}

int antfly_vision_read(const AntflyVisionInput *input) {
    @autoreleasepool {
        static pthread_mutex_t gate = PTHREAD_MUTEX_INITIALIZER;
        // Bound concurrent Vision work without blocking shutdown on a queue.
        if (pthread_mutex_trylock(&gate)) return 5;
        @try {
            return recognize(input);
        } @finally {
            pthread_mutex_unlock(&gate);
        }
    }
}
