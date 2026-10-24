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

#import <Foundation/Foundation.h>
#import <CoreFoundation/CoreFoundation.h>
#import <objc/runtime.h>
#import "metal_device_credit.h"

// The association follows the actual resource object through runtime aliases
// and command/frame retention. Clearing a logical slot never retires a credit.
@interface AntflyMetalDeviceCredit : NSObject {
@public
    void *credit;
    void (*release_credit)(void *);
}
@end
@implementation AntflyMetalDeviceCredit
- (void)dealloc {
    if (credit != NULL) release_credit(credit);
}
@end
static char termite_metal_device_credit_key;

id<MTLBuffer> termite_metal_device_budget_new_buffer(
    id<MTLDevice> device, size_t bytes, MTLResourceOptions options,
    const termite_metal_device_budget *budget)
{
    if (device == nil || bytes == 0) return nil;
    if (budget == NULL || budget->context == NULL)
        return [device newBufferWithLength:bytes options:options];
    if (budget->reserve == NULL || budget->release_credit == NULL) return nil;
    AntflyMetalDeviceCredit *owner = [AntflyMetalDeviceCredit new];
    if (owner == nil) return nil;
    void *credit = NULL;
    if (budget->reserve(budget->context, bytes, &credit) != 0 || credit == NULL) return nil;
    owner->credit = credit;
    owner->release_credit = budget->release_credit;
    id<MTLBuffer> buffer = [device newBufferWithLength:bytes options:options];
    if (buffer == nil) return nil; // ARC retires the unconsumed credit.
    objc_setAssociatedObject(buffer, &termite_metal_device_credit_key, owner, OBJC_ASSOCIATION_RETAIN_NONATOMIC);
    return buffer;
}

// CPU-only fixture: the factory selector and ARC ownership are identical to
// the production call. No Metal device, command queue or GPU work is created.
static size_t fixture_allocations;
static size_t fixture_frees;
@interface AntflyMetalCreditFixtureBuffer : NSObject
@end
@implementation AntflyMetalCreditFixtureBuffer
- (void)dealloc { fixture_frees += 1; }
@end
@interface AntflyMetalCreditFixtureDevice : NSObject
@property(nonatomic) BOOL fail;
@end
@implementation AntflyMetalCreditFixtureDevice
- (id<MTLBuffer>)newBufferWithLength:(NSUInteger)length options:(MTLResourceOptions)options {
    (void)length; (void)options;
    fixture_allocations += 1;
    if (self.fail) return nil;
    return (id<MTLBuffer>)[AntflyMetalCreditFixtureBuffer new];
}
@end
void *termite_metal_device_credit_fixture_buffer(const termite_metal_device_budget *budget, size_t bytes, int fail) {
    @autoreleasepool {
        AntflyMetalCreditFixtureDevice *device = [AntflyMetalCreditFixtureDevice new];
        device.fail = fail != 0;
        id<MTLBuffer> buffer = termite_metal_device_budget_new_buffer((id<MTLDevice>)device, bytes, MTLResourceStorageModePrivate, budget);
        return buffer == nil ? NULL : (void *)CFBridgingRetain(buffer);
    }
}
void termite_metal_device_credit_fixture_retain(void *buffer) { if (buffer != NULL) CFRetain(buffer); }
void termite_metal_device_credit_fixture_release(void *buffer) {
    @autoreleasepool { if (buffer != NULL) CFRelease(buffer); }
}
size_t termite_metal_device_credit_fixture_allocations(void) { return fixture_allocations; }
size_t termite_metal_device_credit_fixture_frees(void) { return fixture_frees; }
