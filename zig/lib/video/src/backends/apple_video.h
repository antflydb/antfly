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

#pragma once
#include <stddef.h>
#include <stdint.h>
typedef void (*av_output)(void *, size_t, int32_t, void *);
int32_t av_decoder_create(const uint8_t *, size_t, uint32_t, uint32_t, int, av_output, void *, void **);
int32_t av_decoder_submit(void *, const uint8_t *, size_t, size_t, int64_t, int64_t, uint32_t, uint32_t);
int32_t av_decoder_submit_coded(void *, const uint8_t *, size_t, size_t, int64_t, int64_t, uint32_t);
int32_t av_decoder_drain(void *);
void av_decoder_destroy(void *);
int av_decoder_hardware(void *);
int av_decoder_supports(uint32_t);
int32_t av_decoder_create_coded(uint32_t, const uint8_t *, size_t, uint32_t, uint32_t, uint32_t, int, av_output, void *, void **);
void av_surface_retain(void *);
void av_surface_release(void *);
uint32_t av_surface_width(void *);
uint32_t av_surface_height(void *);
uint32_t av_surface_format(void *);
uint64_t av_surface_storage_bytes(void *);
int32_t av_surface_lock(void *);
void av_surface_unlock(void *);
const uint8_t *av_surface_plane(void *, size_t, size_t *, size_t *, size_t *);
// Metal imports retain both the pixel buffer and the plane textures until release.
int32_t av_metal_create(void *, void **);
void av_metal_destroy(void *);
int32_t av_metal_import(void *, void *, void **);
void *av_metal_plane_texture(void *, size_t);
void av_metal_import_release(void *);
int32_t av_preparer_create(void *, void **);
void av_preparer_destroy(void *);
int32_t av_coefficients_create(void *, const uint32_t *, size_t, const int32_t *, size_t, const uint32_t *, size_t, const int32_t *, size_t, void **);
void av_coefficients_destroy(void *);
int32_t av_chroma_create(void *, const void *, size_t, const void *, size_t, void **);
void av_chroma_destroy(void *);
int32_t av_prepare_submit(void *, void *, const uint32_t *, void *, void *, void **);
int32_t av_prepare_rgba_submit(void *, const uint8_t *, size_t, const uint32_t *, void *, void **);
// Native integer planes are retained until prepared output is released.
int32_t av_prepare_native_submit(void *, void *, void *, const uint32_t *, void *, void *, void **);
int32_t av_prepare_host_submit(void *, const uint8_t *, size_t, const uint8_t *, size_t, const uint32_t *, void *, void *, void **);
int av_prepared_poll(void *);
void *av_prepared_buffer(void *);
int32_t av_prepared_copy(void *, float *, size_t);
void av_prepared_destroy(void *);
