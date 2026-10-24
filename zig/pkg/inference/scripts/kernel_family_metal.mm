// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cmath>
#include <cstring>
#include <string>
#include <vector>

static void require(bool ok, NSString *message) {
    if (!ok) { fprintf(stderr, "%s\n", message.UTF8String); exit(2); }
}
static MTLSize size3(NSArray *values) {
    return MTLSizeMake([values[0] unsignedIntegerValue], [values[1] unsignedIntegerValue],
                       [values[2] unsignedIntegerValue]);
}
static double dispatch(id<MTLCommandQueue> queue, id<MTLComputePipelineState> pipeline,
                       NSDictionary *test, NSArray *buffers, NSArray *data, int repeats) {
    id<MTLCommandBuffer> command = [queue commandBuffer];
    id<MTLComputeCommandEncoder> encoder = [command computeCommandEncoder];
    [encoder setComputePipelineState:pipeline];
    NSUInteger i = 0;
    for (NSDictionary *binding in test[@"bindings"]) {
        NSUInteger index = [binding[@"index"] unsignedIntegerValue];
        if ([binding[@"binding"] isEqual:@"bytes"]) {
            NSData *value = data[i];
            [encoder setBytes:value.bytes length:value.length atIndex:index];
        } else [encoder setBuffer:buffers[i] offset:0 atIndex:index];
        ++i;
    }
    for (NSDictionary *scratch in test[@"scratch"])
        [encoder setThreadgroupMemoryLength:[scratch[@"bytes"] unsignedIntegerValue]
                                   atIndex:[scratch[@"index"] unsignedIntegerValue]];
    for (int iteration = 0; iteration < repeats; ++iteration) {
        if ([test[@"dispatch"] isEqual:@"threads"])
            [encoder dispatchThreads:size3(test[@"grid"]) threadsPerThreadgroup:size3(test[@"threads"])];
        else [encoder dispatchThreadgroups:size3(test[@"grid"]) threadsPerThreadgroup:size3(test[@"threads"])];
        [encoder memoryBarrierWithScope:MTLBarrierScopeBuffers];
    }
    [encoder endEncoding]; [command commit]; [command waitUntilCompleted];
    require(command.status == MTLCommandBufferStatusCompleted, command.error.description ?: @"GPU command failed");
    return (command.GPUEndTime - command.GPUStartTime) * 1e9 / repeats;
}
int main(int argc, char **argv) { @autoreleasepool {
    require(argc == 3, @"Usage: kernel-family-metal manifest.json receipt.json");
    NSString *manifestPath = @(argv[1]), *directory = manifestPath.stringByDeletingLastPathComponent;
    NSError *error = nil;
    NSDictionary *manifest = [NSJSONSerialization JSONObjectWithData:[NSData dataWithContentsOfFile:manifestPath]
                                                           options:0 error:&error];
    require(manifest != nil, error.description ?: @"Cannot read manifest");
    id<MTLDevice> device = MTLCreateSystemDefaultDevice(); require(device != nil, @"No Metal device");
    id<MTLCommandQueue> queue = [device newCommandQueue];
    NSMutableDictionary *libraries = [NSMutableDictionary dictionary];
    NSMutableArray *results = [NSMutableArray array]; bool passed = true;
    for (NSDictionary *test in manifest[@"cases"]) { @autoreleasepool {
        NSString *name = test[@"kernel"], *mode = test[@"math_mode"] ?: @"safe";
        id<MTLComputePipelineState> pipelines[2];
        for (int version = 0; version < 2; ++version) {
            NSString *key = [NSString stringWithFormat:@"%d-%@", version, mode];
            id<MTLLibrary> library = libraries[key];
            if (!library) {
                NSString *path = [directory stringByAppendingPathComponent:manifest[version ? @"candidate" : @"baseline"]];
                NSString *source = [NSString stringWithContentsOfFile:path encoding:NSUTF8StringEncoding error:&error];
                MTLCompileOptions *options = [MTLCompileOptions new];
                options.mathMode = [mode isEqual:@"safe"] ? MTLMathModeSafe : MTLMathModeFast;
                library = [device newLibraryWithSource:source options:options error:&error];
                require(library != nil, error.description ?: @"Shader compilation failed"); libraries[key] = library;
            }
            id<MTLFunction> function = [library newFunctionWithName:name]; require(function != nil, name);
            pipelines[version] = [device newComputePipelineStateWithFunction:function error:&error];
            require(pipelines[version] != nil, error.description ?: @"Pipeline creation failed");
        }
        NSMutableArray *buffers[2] = {[NSMutableArray array], [NSMutableArray array]};
        NSMutableArray *data = [NSMutableArray array];
        for (NSDictionary *binding in test[@"bindings"]) {
            NSData *initial = [NSData dataWithContentsOfFile:[directory stringByAppendingPathComponent:binding[@"file"]]];
            require(initial != nil && initial.length > 0, @"Missing/empty fixture data"); [data addObject:initial];
            if ([binding[@"binding"] isEqual:@"bytes"]) {
                [buffers[0] addObject:[NSNull null]]; [buffers[1] addObject:[NSNull null]]; continue;
            }
            id<MTLBuffer> buffer = [device newBufferWithBytes:initial.bytes length:initial.length options:MTLResourceStorageModeShared];
            require(buffer != nil, @"Buffer allocation failed"); [buffers[0] addObject:buffer];
            id<MTLBuffer> second = [binding[@"output"] boolValue]
                ? [device newBufferWithBytes:initial.bytes length:initial.length options:MTLResourceStorageModeShared] : buffer;
            require(second != nil, @"Buffer allocation failed"); [buffers[1] addObject:second];
        }
        for (int version = 0; version < 2; ++version)
            dispatch(queue, pipelines[version], test, buffers[version], data, 1);
        bool exact = true, guards = true, reference = true; NSUInteger i = 0, outputs = 0, reference_outputs = 0;
        for (NSDictionary *binding in test[@"bindings"]) {
            if ([binding[@"output"] boolValue]) {
                ++outputs; NSData *initial = data[i];
                exact &= memcmp([buffers[0][i] contents], [buffers[1][i] contents], initial.length) == 0;
                NSUInteger written = [binding[@"written_bytes"] unsignedIntegerValue];
                require(written <= initial.length, @"Output range exceeds fixture");
                for (int version = 0; version < 2; ++version)
                    guards &= memcmp((char *)[buffers[version][i] contents] + written,
                                     (const char *)initial.bytes + written, initial.length - written) == 0;
                if (binding[@"reference_file"]) {
                    ++reference_outputs;
                    NSData *expected = [NSData dataWithContentsOfFile:[directory stringByAppendingPathComponent:binding[@"reference_file"]]];
                    NSString *dtype = binding[@"output_dtype"] ?: @"f32";
                    require([dtype isEqual:@"f32"] || [dtype isEqual:@"f16"], @"Invalid reference dtype");
                    NSUInteger width = [dtype isEqual:@"f16"] ? 2 : 4;
                    require(written % width == 0 && expected != nil && expected.length == written / width * 4, @"Invalid reference extent");
                    double absolute = [binding[@"absolute_tolerance"] doubleValue];
                    double relative = [binding[@"relative_tolerance"] doubleValue];
                    require(std::isfinite(absolute) && std::isfinite(relative) && absolute >= 0 && relative >= 0, @"Invalid reference tolerance");
                    for (int version = 0; version < 2; ++version) {
                        const void *actual = [buffers[version][i] contents];
                        for (NSUInteger element = 0; element < written / width; ++element) {
                            float want = ((const float *)expected.bytes)[element];
                            float got = width == 2 ? (float)((const _Float16 *)actual)[element] : ((const float *)actual)[element];
                            reference &= std::isfinite(want) ? (std::isfinite(got) && std::abs((double)got - want) <= absolute + relative * std::abs(want))
                                       : (std::isnan(want) ? std::isnan(got) : got == want);
                        }
                    }
                }
            }
            ++i;
        }
        require(outputs > 0 && exact && guards && reference, [NSString stringWithFormat:@"Parity/guard/reference failure %@", test[@"name"]]);
        bool resources = pipelines[1].maxTotalThreadsPerThreadgroup >= pipelines[0].maxTotalThreadsPerThreadgroup
                      && pipelines[1].staticThreadgroupMemoryLength <= pipelines[0].staticThreadgroupMemoryLength;
        int warmup = [manifest[@"warmup"] intValue], repeats = [manifest[@"repeats"] intValue];
        require(warmup > 0 && repeats > 0, @"Warmup/repeats must be positive");
        double warmup_gpu_ns[2];
        unsigned long long warmup_dispatches[2] = {(unsigned long long)warmup, (unsigned long long)warmup};
        for (int version = 0; version < 2; ++version)
            warmup_gpu_ns[version] = dispatch(queue, pipelines[version], test, buffers[0], data, warmup) * warmup;
        const double warmup_ms = [manifest[@"warmup_ms"] doubleValue];
        require(std::isfinite(warmup_ms) && warmup_ms >= 0, @"Invalid warmup duration");
        if (warmup_ms > 0) {
            // Fixed-count warmups can finish before a tiny kernel reaches a
            // steady device clock. Alternate equal batches on both pipelines.
            const auto started = std::chrono::steady_clock::now();
            int version = 0;
            do {
                warmup_gpu_ns[version] += dispatch(queue, pipelines[version], test, buffers[0], data, repeats) * repeats;
                warmup_dispatches[version] += repeats;
                version ^= 1;
            } while (std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started).count() < warmup_ms);
        }
        NSString *order = manifest[@"arm_order"] ?: @"ABBABAABABBAABBA";
        std::vector<double> times[2]; NSMutableArray *arms = [NSMutableArray array];
        for (NSUInteger arm = 0; arm < order.length; ++arm) {
            unichar label = [order characterAtIndex:arm]; require(label == 'A' || label == 'B', @"Invalid arm label");
            int version = label == 'B'; double ns = dispatch(queue, pipelines[version], test, buffers[0], data, repeats);
            times[version].push_back(ns); [arms addObject:@{@"arm":@(version), @"ns":@(ns)}];
        }
        require(times[0].size() == times[1].size() && times[0].size() >= 2, @"Unbalanced/short timing protocol");
        double medians[2];
        for (int version = 0; version < 2; ++version) {
            auto &values = times[version]; std::sort(values.begin(), values.end());
            medians[version] = (values[(values.size()-1)/2] + values[values.size()/2]) / 2;
        }
        double ratio = medians[1] / medians[0]; bool ok = ratio <= 1.05 && resources; passed &= ok;
        [results addObject:@{@"name":test[@"name"], @"kernel":name, @"exact":@(exact), @"guards":@(guards),
            @"baseline_ns":@(medians[0]), @"candidate_ns":@(medians[1]), @"ratio":@(ratio), @"pass":@(ok),
            @"resources_pass":@(resources), @"arms":arms,
            @"warmup_ms":@(warmup_ms), @"warmup_dispatches":@[@(warmup_dispatches[0]), @(warmup_dispatches[1])],
            @"warmup_gpu_ns":@[@(warmup_gpu_ns[0]), @(warmup_gpu_ns[1])], @"launches_per_arm":@(repeats),
            @"reference_outputs":@(reference_outputs), @"reference_pass":reference_outputs ? @(reference) : [NSNull null],
            @"baseline_max_threads":@(pipelines[0].maxTotalThreadsPerThreadgroup),
            @"candidate_max_threads":@(pipelines[1].maxTotalThreadsPerThreadgroup),
            @"baseline_static_bytes":@(pipelines[0].staticThreadgroupMemoryLength),
            @"candidate_static_bytes":@(pipelines[1].staticThreadgroupMemoryLength)}];
        NSData *json = [NSJSONSerialization dataWithJSONObject:results options:NSJSONWritingPrettyPrinted error:&error];
        require([json writeToFile:@(argv[2]) atomically:YES], @"Cannot save receipt");
        printf("%s exact=%d guard=%d ratio=%.6f pass=%d\n", [test[@"name"] UTF8String], exact, guards, ratio, ok); fflush(stdout);
    }}
    printf("KERNEL_FAMILY_METAL %s %lu cases\n", passed ? "PASSED" : "FAILED", (unsigned long)results.count);
    return passed ? 0 : 1;
}}
