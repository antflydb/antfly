// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Test-only forwarding proxies: record the real command stream and optionally
// decline resources or encoders. Wall-clock timings use the unwrapped queue;
// GPU timings capture the real completed command buffer through the proxy.
@interface HostFfnTrace : NSObject
@property NSMutableArray<NSString *> *events;
@property NSMutableDictionary<NSValue *, NSNumber *> *buffers;
@property NSUInteger encoders;
@property NSUInteger failEncoder;
@property BOOL failCommand;
@property BOOL failCompletion;
@property void *output;
@property BOOL outputAliased;
@property BOOL forceCopiedOutput;
@property NSUInteger failAllocation;
@property NSUInteger allocations;
@property id<MTLCommandBuffer> command;
@end
@implementation HostFfnTrace
- (instancetype)init {
    if ((self = [super init])) { _events = [NSMutableArray new]; _buffers = [NSMutableDictionary new]; }
    return self;
}
- (NSNumber *)bufferId:(id<MTLBuffer>)buffer {
    if (buffer == nil) return @0;
    if (buffer.contents == _output) _outputAliased = YES;
    NSValue *key = [NSValue valueWithPointer:(__bridge void *)buffer];
    NSNumber *value = _buffers[key];
    if (value == nil) { value = @(_buffers.count + 1); _buffers[key] = value; }
    return value;
}
@end

@interface HostFfnTraceProxy : NSProxy {
    id _target;
    HostFfnTrace *_trace;
}
+ (id)wrap:(id)target trace:(HostFfnTrace *)trace;
@end
@implementation HostFfnTraceProxy
+ (id)wrap:(id)target trace:(HostFfnTrace *)trace {
    if (target == nil) return nil;
    HostFfnTraceProxy *proxy = [self alloc]; proxy->_target = target; proxy->_trace = trace; return proxy;
}
- (NSMethodSignature *)methodSignatureForSelector:(SEL)selector { return [_target methodSignatureForSelector:selector]; }
- (void)forwardInvocation:(NSInvocation *)invocation { [invocation invokeWithTarget:_target]; }
- (BOOL)respondsToSelector:(SEL)selector { return class_getInstanceMethod([HostFfnTraceProxy class], selector) != NULL || [_target respondsToSelector:selector]; }
- (id<MTLCommandBuffer>)commandBufferWithDescriptor:(MTLCommandBufferDescriptor *)descriptor {
    [_trace.events addObject:@"command-descriptor"];
    if (_trace.failCommand) return nil;
    _trace.command = [(id<MTLCommandQueue>)_target commandBufferWithDescriptor:descriptor];
    return [HostFfnTraceProxy wrap:_trace.command trace:_trace];
}
- (id<MTLCommandBuffer>)commandBuffer {
    [_trace.events addObject:@"command"];
    if (_trace.failCommand) return nil;
    _trace.command = [(id<MTLCommandQueue>)_target commandBuffer];
    return [HostFfnTraceProxy wrap:_trace.command trace:_trace];
}
- (id<MTLBuffer>)newBufferWithBytesNoCopy:(void *)bytes length:(NSUInteger)length options:(MTLResourceOptions)options deallocator:(void (^)(void *, NSUInteger))deallocator {
    [_trace.events addObject:[NSString stringWithFormat:@"alias %lu",(unsigned long)length]];
    if (_trace.failAllocation || (_trace.forceCopiedOutput && bytes == _trace.output)) return nil;
    return [(id<MTLDevice>)_target newBufferWithBytesNoCopy:bytes length:length options:options deallocator:deallocator];
}
- (id<MTLBuffer>)newBufferWithBytes:(const void *)bytes length:(NSUInteger)length options:(MTLResourceOptions)options {
    ++_trace.allocations; [_trace.events addObject:[NSString stringWithFormat:@"copy %lu",(unsigned long)length]];
    return _trace.failAllocation == _trace.allocations ? nil : [(id<MTLDevice>)_target newBufferWithBytes:bytes length:length options:options];
}
- (id<MTLBuffer>)newBufferWithLength:(NSUInteger)length options:(MTLResourceOptions)options {
    ++_trace.allocations; [_trace.events addObject:[NSString stringWithFormat:@"allocate %lu %lu",(unsigned long)length,(unsigned long)options]];
    return _trace.failAllocation == _trace.allocations ? nil : [(id<MTLDevice>)_target newBufferWithLength:length options:options];
}
- (void)setLabel:(NSString *)label { [_trace.events addObject:[@"label " stringByAppendingString:label]]; [_target setLabel:label]; }
- (id<MTLComputeCommandEncoder>)computeCommandEncoder {
    ++_trace.encoders; [_trace.events addObject:@"encoder"];
    if (_trace.failEncoder == _trace.encoders) return nil;
    return [HostFfnTraceProxy wrap:[_target computeCommandEncoder] trace:_trace];
}
- (id<MTLComputeCommandEncoder>)computeCommandEncoderWithDispatchType:(MTLDispatchType)type {
    ++_trace.encoders; [_trace.events addObject:[NSString stringWithFormat:@"encoder-type %lu",(unsigned long)type]];
    if (_trace.failEncoder == _trace.encoders) return nil;
    return [HostFfnTraceProxy wrap:[_target computeCommandEncoderWithDispatchType:type] trace:_trace];
}
- (void)setComputePipelineState:(id<MTLComputePipelineState>)pipeline {
    [_trace.events addObject:[NSString stringWithFormat:@"pipeline %p",(__bridge void *)pipeline]];
    [_target setComputePipelineState:pipeline];
}
- (void)setBuffer:(id<MTLBuffer>)buffer offset:(NSUInteger)offset atIndex:(NSUInteger)index {
    [_trace.events addObject:[NSString stringWithFormat:@"buffer %@ %lu %lu",[_trace bufferId:buffer],(unsigned long)offset,(unsigned long)index]];
    [_target setBuffer:buffer offset:offset atIndex:index];
}
- (void)setBuffers:(const id<MTLBuffer> *)buffers offsets:(const NSUInteger *)offsets withRange:(NSRange)range {
    [_trace.events addObject:[NSString stringWithFormat:@"buffers %lu %lu",(unsigned long)range.location,(unsigned long)range.length]];
    for (NSUInteger i=0;i<range.length;++i) [_trace.events addObject:[NSString stringWithFormat:@"binding %@ %lu",[_trace bufferId:buffers[i]],(unsigned long)offsets[i]]];
    [_target setBuffers:buffers offsets:offsets withRange:range];
}
- (void)setBytes:(const void *)bytes length:(NSUInteger)length atIndex:(NSUInteger)index {
    [_trace.events addObject:[NSString stringWithFormat:@"bytes %lu %@",(unsigned long)index,[NSData dataWithBytes:bytes length:length]]];
    [_target setBytes:bytes length:length atIndex:index];
}
- (void)setThreadgroupMemoryLength:(NSUInteger)length atIndex:(NSUInteger)index {
    [_trace.events addObject:[NSString stringWithFormat:@"memory %lu %lu",(unsigned long)length,(unsigned long)index]];
    [_target setThreadgroupMemoryLength:length atIndex:index];
}
- (void)dispatchThreads:(MTLSize)grid threadsPerThreadgroup:(MTLSize)threads {
    [_trace.events addObject:[NSString stringWithFormat:@"threads %zu %zu %zu / %zu %zu %zu",grid.width,grid.height,grid.depth,threads.width,threads.height,threads.depth]];
    [_target dispatchThreads:grid threadsPerThreadgroup:threads];
}
- (void)dispatchThreadgroups:(MTLSize)grid threadsPerThreadgroup:(MTLSize)threads {
    [_trace.events addObject:[NSString stringWithFormat:@"groups %zu %zu %zu / %zu %zu %zu",grid.width,grid.height,grid.depth,threads.width,threads.height,threads.depth]];
    [_target dispatchThreadgroups:grid threadsPerThreadgroup:threads];
}
- (void)endEncoding { [_trace.events addObject:@"end"]; [_target endEncoding]; }
- (void)commit { [_trace.events addObject:@"commit"]; [_target commit]; }
- (void)waitUntilCompleted { [_trace.events addObject:@"wait"]; [_target waitUntilCompleted]; }
- (MTLCommandBufferStatus)status { return _trace.failCompletion ? MTLCommandBufferStatusError : [(id<MTLCommandBuffer>)_target status]; }
@end
