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

#include "loader.h"
#include <dlfcn.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sysctl.h>

// No Swift or newer framework symbols appear in this object. In particular,
// check the running OS before dlopen: Swift initializers run during loading.
static pthread_once_t once = PTHREAD_ONCE_INIT;
static pthread_once_t os_once = PTHREAD_ONCE_INIT;
static int os_status = 2;
static antfly_apple_invoke_fn invoke_bridge;
static int load_status = 12; // Missing/unloadable bridge.
static void *bridge_handle;

static int supported_os(void) {
    char version[64] = {0};
    size_t size = sizeof(version);
    if (sysctlbyname("kern.osproductversion", version, &size, NULL, 0) != 0 ||
        size == 0 || size > sizeof(version) || version[size - 1] != '\0') return 0;
    char *end;
    long major = strtol(version, &end, 10);
    return end != version && (*end == '.' || *end == '\0') && major >= 26;
}

static void check_os(void) { if (supported_os()) os_status = 0; }
int antfly_apple_loader_available(void) {
    pthread_once(&os_once, check_os);
    return os_status;
}

static void *open_relative(const char *directory, const char *suffix) {
    char path[PATH_MAX];
    int length = snprintf(path, sizeof(path), "%s/%s", directory, suffix);
    if (length < 0 || (size_t)length >= sizeof(path)) return NULL;
    return dlopen(path, RTLD_NOW | RTLD_LOCAL);
}

static void load_bridge(void) {
    if (antfly_apple_loader_available()) { load_status = 2; return; }
    const char *override = getenv("ANTFLY_APPLE_BRIDGE_PATH");
    if (override) {
        // Explicit opt-in for custom Lite layouts and tests. Never search CWD,
        // PATH, or a bare dylib name, and never silently ignore a bad override.
        if (override[0] != '/') return;
        bridge_handle = dlopen(override, RTLD_NOW | RTLD_LOCAL);
    } else {
        Dl_info owner;
        char directory[PATH_MAX];
        // A private anchor cannot be interposed by another loaded Antfly image.
        if (!dladdr((const void *)&load_bridge, &owner) ||
            !owner.dli_fname || !realpath(owner.dli_fname, directory)) return;
        char *slash = strrchr(directory, '/');
        if (!slash) return;
        *slash = '\0';
        // Lite: alongside libantfly. Installed CLI: bin/../lib.
        // Unpacked runtime archive: antfly alongside lib/.
        const char *locations[] = {"libantfly-apple.dylib", "../lib/libantfly-apple.dylib", "lib/libantfly-apple.dylib"};
        for (size_t i = 0; i < sizeof(locations) / sizeof(locations[0]); ++i) {
            bridge_handle = open_relative(directory, locations[i]);
            if (bridge_handle) break;
        }
    }
    if (!bridge_handle) return;
    uint32_t (*abi_version)(void) = (uint32_t (*)(void))dlsym(bridge_handle, "antfly_apple_bridge_abi_version");
    if (!abi_version || abi_version() != 1) { load_status = 15; return; }
    invoke_bridge = (antfly_apple_invoke_fn)dlsym(bridge_handle, "antfly_apple_invoke");
    load_status = invoke_bridge ? 0 : 15;
    // Intentionally never dlclose, even on ABI failure. The Swift runtime may
    // retain metadata/tasks. Failed loads are cached and require a restart.
}

int antfly_apple_loader_invoke(int operation, const uint8_t *json, size_t json_len,
    const uint8_t *audio, size_t audio_len, size_t limit, void *context,
    antfly_apple_cancel cancel, antfly_apple_output output) {
    pthread_once(&once, load_bridge);
    if (load_status) return load_status;
    return invoke_bridge(operation, json, json_len, audio, audio_len, limit, context, cancel, output);
}
