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

#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef int nvrtcResult;
typedef struct _nvrtcProgram *nvrtcProgram;

typedef nvrtcResult (*nvrtcCreateProgramFn)(nvrtcProgram *, const char *, const char *, int, const char *const *, const char *const *);
typedef nvrtcResult (*nvrtcCompileProgramFn)(nvrtcProgram, int, const char *const *);
typedef nvrtcResult (*nvrtcGetPTXSizeFn)(nvrtcProgram, size_t *);
typedef nvrtcResult (*nvrtcGetPTXFn)(nvrtcProgram, char *);
typedef nvrtcResult (*nvrtcGetProgramLogSizeFn)(nvrtcProgram, size_t *);
typedef nvrtcResult (*nvrtcGetProgramLogFn)(nvrtcProgram, char *);
typedef nvrtcResult (*nvrtcDestroyProgramFn)(nvrtcProgram *);
typedef const char *(*nvrtcGetErrorStringFn)(nvrtcResult);

struct Nvrtc {
    void *handle;
    nvrtcCreateProgramFn createProgram;
    nvrtcCompileProgramFn compileProgram;
    nvrtcGetPTXSizeFn getPTXSize;
    nvrtcGetPTXFn getPTX;
    nvrtcGetProgramLogSizeFn getProgramLogSize;
    nvrtcGetProgramLogFn getProgramLog;
    nvrtcDestroyProgramFn destroyProgram;
    nvrtcGetErrorStringFn getErrorString;
};

static void *load_symbol(void *handle, const char *name) {
    void *symbol = dlsym(handle, name);
    if (!symbol) {
        fprintf(stderr, "missing NVRTC symbol %s: %s\n", name, dlerror());
        exit(2);
    }
    return symbol;
}

static struct Nvrtc load_nvrtc(void) {
    struct Nvrtc nvrtc;
    memset(&nvrtc, 0, sizeof(nvrtc));
    nvrtc.handle = dlopen("libnvrtc.so.11.2", RTLD_NOW | RTLD_LOCAL);
    if (!nvrtc.handle) nvrtc.handle = dlopen("libnvrtc.so.11.8", RTLD_NOW | RTLD_LOCAL);
    if (!nvrtc.handle) {
        fprintf(stderr, "failed to load NVRTC: %s\n", dlerror());
        exit(2);
    }

    nvrtc.createProgram = (nvrtcCreateProgramFn)load_symbol(nvrtc.handle, "nvrtcCreateProgram");
    nvrtc.compileProgram = (nvrtcCompileProgramFn)load_symbol(nvrtc.handle, "nvrtcCompileProgram");
    nvrtc.getPTXSize = (nvrtcGetPTXSizeFn)load_symbol(nvrtc.handle, "nvrtcGetPTXSize");
    nvrtc.getPTX = (nvrtcGetPTXFn)load_symbol(nvrtc.handle, "nvrtcGetPTX");
    nvrtc.getProgramLogSize = (nvrtcGetProgramLogSizeFn)load_symbol(nvrtc.handle, "nvrtcGetProgramLogSize");
    nvrtc.getProgramLog = (nvrtcGetProgramLogFn)load_symbol(nvrtc.handle, "nvrtcGetProgramLog");
    nvrtc.destroyProgram = (nvrtcDestroyProgramFn)load_symbol(nvrtc.handle, "nvrtcDestroyProgram");
    nvrtc.getErrorString = (nvrtcGetErrorStringFn)load_symbol(nvrtc.handle, "nvrtcGetErrorString");
    return nvrtc;
}

static char *read_file(const char *path, size_t *len_out) {
    FILE *file = fopen(path, "rb");
    if (!file) {
        fprintf(stderr, "open %s failed: %s\n", path, strerror(errno));
        exit(2);
    }
    if (fseek(file, 0, SEEK_END) != 0) {
        fprintf(stderr, "seek %s failed: %s\n", path, strerror(errno));
        exit(2);
    }
    long size = ftell(file);
    if (size < 0) {
        fprintf(stderr, "tell %s failed: %s\n", path, strerror(errno));
        exit(2);
    }
    rewind(file);

    char *buffer = (char *)malloc((size_t)size + 1);
    if (!buffer) {
        fprintf(stderr, "out of memory reading %s\n", path);
        exit(2);
    }
    size_t read = fread(buffer, 1, (size_t)size, file);
    if (read != (size_t)size) {
        fprintf(stderr, "read %s failed\n", path);
        exit(2);
    }
    fclose(file);
    buffer[size] = '\0';
    *len_out = (size_t)size;
    return buffer;
}

static void write_file(const char *path, const char *data, size_t len) {
    FILE *file = fopen(path, "wb");
    if (!file) {
        fprintf(stderr, "open %s failed: %s\n", path, strerror(errno));
        exit(2);
    }
    if (fwrite(data, 1, len, file) != len) {
        fprintf(stderr, "write %s failed: %s\n", path, strerror(errno));
        exit(2);
    }
    fclose(file);
}

static void print_log(struct Nvrtc *nvrtc, nvrtcProgram program) {
    size_t log_size = 0;
    nvrtc->getProgramLogSize(program, &log_size);
    if (log_size <= 1) return;
    char *log = (char *)malloc(log_size);
    if (!log) return;
    if (nvrtc->getProgramLog(program, log) == 0) fputs(log, stderr);
    free(log);
}

int main(int argc, char **argv) {
    if (argc < 3 || argc > 4) {
        fprintf(stderr, "usage: %s input.cu output.ptx [compute_XX]\n", argv[0]);
        return 2;
    }

    const char *arch = argc == 4 ? argv[3] : "compute_75";
    char arch_option[64];
    snprintf(arch_option, sizeof(arch_option), "--gpu-architecture=%s", arch);

    size_t source_len = 0;
    char *source = read_file(argv[1], &source_len);
    (void)source_len;

    struct Nvrtc nvrtc = load_nvrtc();
    nvrtcProgram program = NULL;
    nvrtcResult result = nvrtc.createProgram(&program, source, argv[1], 0, NULL, NULL);
    if (result != 0) {
        fprintf(stderr, "nvrtcCreateProgram failed: %s\n", nvrtc.getErrorString(result));
        return 2;
    }

    const char *options[] = {
        arch_option,
        "--std=c++14",
        "--use_fast_math",
        "--device-as-default-execution-space",
    };
    result = nvrtc.compileProgram(program, (int)(sizeof(options) / sizeof(options[0])), options);
    print_log(&nvrtc, program);
    if (result != 0) {
        fprintf(stderr, "nvrtcCompileProgram failed: %s\n", nvrtc.getErrorString(result));
        nvrtc.destroyProgram(&program);
        return 1;
    }

    size_t ptx_size = 0;
    result = nvrtc.getPTXSize(program, &ptx_size);
    if (result != 0) {
        fprintf(stderr, "nvrtcGetPTXSize failed: %s\n", nvrtc.getErrorString(result));
        nvrtc.destroyProgram(&program);
        return 2;
    }
    char *ptx = (char *)malloc(ptx_size);
    if (!ptx) {
        fprintf(stderr, "out of memory allocating PTX\n");
        nvrtc.destroyProgram(&program);
        return 2;
    }
    result = nvrtc.getPTX(program, ptx);
    if (result != 0) {
        fprintf(stderr, "nvrtcGetPTX failed: %s\n", nvrtc.getErrorString(result));
        nvrtc.destroyProgram(&program);
        return 2;
    }

    write_file(argv[2], ptx, ptx_size - 1);
    fprintf(stderr, "wrote %zu bytes of PTX for %s\n", ptx_size - 1, arch);
    nvrtc.destroyProgram(&program);
    free(ptx);
    free(source);
    return 0;
}
