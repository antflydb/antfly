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

#pragma once
#include <stddef.h>
#include <stdint.h>

// All inputs are borrowed until this synchronous call returns. Callbacks run
// synchronously; the region bytes are borrowed only for that callback.
typedef struct {
    const uint8_t *bytes;
    size_t length;
    uint32_t width; // Zero selects encoded image input.
    uint32_t height;
    size_t stride;
    const uint8_t *options;
    size_t options_length;
    uint64_t max_pixels;
    void *context;
    int (*should_cancel)(void *context);
    int (*region)(void *context, const uint8_t *text, size_t length,
                  double x1, double y1, double x2, double y2, float confidence);
} AntflyVisionInput;

// 0 success, 1 invalid image/options, 2 Vision failure, 3 unsupported language,
// 4 cancelled/callback stopped, 5 resource busy, 6 too many pixels, 7 unavailable.
int antfly_vision_read(const AntflyVisionInput *input);
