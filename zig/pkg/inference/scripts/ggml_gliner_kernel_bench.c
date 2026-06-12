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

#include <ggml.h>
#include <ggml-cpu.h>

#include <dlfcn.h>
#include <errno.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct bench_case {
    const char * label;
    enum ggml_type type;
    int64_t rows;
    int64_t in_dim;
    int64_t out_dim;
};

typedef enum ggml_status (*graph_compute_with_ctx_fn)(struct ggml_context *, struct ggml_cgraph *, int);
typedef const struct ggml_type_traits_cpu * (*get_type_traits_cpu_fn)(enum ggml_type);

static graph_compute_with_ctx_fn graph_compute_with_ctx = NULL;
static get_type_traits_cpu_fn get_type_traits_cpu = NULL;

static int try_load_ggml_cpu_plugin(const char * path) {
    void * handle = dlopen(path, RTLD_NOW | RTLD_GLOBAL);
    if (!handle) return 1;
    graph_compute_with_ctx = (graph_compute_with_ctx_fn) dlsym(handle, "ggml_graph_compute_with_ctx");
    get_type_traits_cpu = (get_type_traits_cpu_fn) dlsym(handle, "ggml_get_type_traits_cpu");
    return graph_compute_with_ctx && get_type_traits_cpu ? 0 : 1;
}

static int init_ggml_cpu_plugin(void) {
    const char * explicit_plugin = getenv("GGML_CPU_PLUGIN");
    if (explicit_plugin && explicit_plugin[0] != '\0') {
        if (try_load_ggml_cpu_plugin(explicit_plugin) == 0) return 0;
        fprintf(stderr, "failed to load GGML_CPU_PLUGIN=%s with ggml_graph_compute_with_ctx\n", explicit_plugin);
        return 1;
    }

    const char * ggml_prefix = getenv("GGML_PREFIX");
    if (ggml_prefix && ggml_prefix[0] != '\0') {
        const char * plugin_names[] = {
            "libggml-cpu-apple_m4.so",
            "libggml-cpu-apple_m2_m3.so",
            "libggml-cpu-apple_m1.so",
            "libggml-cpu.so",
        };
        char path[4096];
        for (size_t i = 0; i < sizeof(plugin_names) / sizeof(plugin_names[0]); ++i) {
            const int written = snprintf(path, sizeof(path), "%s/libexec/%s", ggml_prefix, plugin_names[i]);
            if (written <= 0 || (size_t) written >= sizeof(path)) continue;
            if (try_load_ggml_cpu_plugin(path) == 0) return 0;
        }
    }

    const char * candidates[] = {
        "/opt/homebrew/opt/ggml/libexec/libggml-cpu-apple_m4.so",
        "/opt/homebrew/opt/ggml/libexec/libggml-cpu-apple_m2_m3.so",
        "/opt/homebrew/opt/ggml/libexec/libggml-cpu-apple_m1.so",
        "/opt/homebrew/opt/ggml/libexec/libggml-cpu.so",
        "/usr/local/opt/ggml/libexec/libggml-cpu-apple_m4.so",
        "/usr/local/opt/ggml/libexec/libggml-cpu-apple_m2_m3.so",
        "/usr/local/opt/ggml/libexec/libggml-cpu-apple_m1.so",
        "/usr/local/opt/ggml/libexec/libggml-cpu.so",
    };
    for (size_t i = 0; i < sizeof(candidates) / sizeof(candidates[0]); ++i) {
        if (try_load_ggml_cpu_plugin(candidates[i]) == 0) return 0;
    }
    fprintf(stderr, "failed to load ggml CPU plugin with ggml_graph_compute_with_ctx\n");
    return 1;
}

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t) ts.tv_sec * 1000000000ull + (uint64_t) ts.tv_nsec;
}

static uint32_t xorshift32(uint32_t * state) {
    uint32_t x = *state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    return x;
}

static float rand_f32(uint32_t * state) {
    return ((float) (xorshift32(state) & 0x00ffffffu) / (float) 0x01000000u) * 2.0f - 1.0f;
}

static int parse_int_arg(int argc, char ** argv, const char * name, int fallback, int min_value, int * out) {
    *out = fallback;
    for (int i = 1; i + 1 < argc; ++i) {
        if (strcmp(argv[i], name) == 0) {
            char * end = NULL;
            errno = 0;
            const long parsed = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' || parsed < min_value || parsed > INT32_MAX) {
                fprintf(stderr, "invalid %s value: %s\n", name, argv[i + 1]);
                return 1;
            }
            *out = (int) parsed;
            return 0;
        }
    }
    return 0;
}

static const char * parse_string_arg(int argc, char ** argv, const char * name) {
    for (int i = 1; i + 1 < argc; ++i) {
        if (strcmp(argv[i], name) == 0) return argv[i + 1];
    }
    return NULL;
}

static const char * type_label(enum ggml_type type) {
    switch (type) {
        case GGML_TYPE_Q1_0: return "Q1_0";
        case GGML_TYPE_Q2_K: return "Q2_K";
        case GGML_TYPE_Q3_K: return "Q3_K";
        case GGML_TYPE_Q4_0: return "Q4_0";
        case GGML_TYPE_Q4_1: return "Q4_1";
        case GGML_TYPE_Q5_0: return "Q5_0";
        case GGML_TYPE_Q5_1: return "Q5_1";
        case GGML_TYPE_Q4_K: return "Q4_K";
        case GGML_TYPE_Q5_K: return "Q5_K";
        case GGML_TYPE_Q6_K: return "Q6_K";
        case GGML_TYPE_Q8_0: return "Q8_0";
        case GGML_TYPE_Q8_1: return "Q8_1";
        case GGML_TYPE_Q8_K: return "Q8_K";
        default: return ggml_type_name(type);
    }
}

static int case_matches_filters(
    const struct bench_case * c,
    const char * type_filters,
    const char * label_contains,
    int rows_filter,
    int in_dim_filter,
    int out_dim_filter
) {
    if (type_filters) {
        const char * label = type_label(c->type);
        const size_t label_len = strlen(label);
        int matched_type = 0;
        const char * start = type_filters;
        while (*start != '\0') {
            const char * end = strchr(start, ',');
            const size_t part_len = end ? (size_t) (end - start) : strlen(start);
            if (part_len == 0) return 0;
            if (part_len == label_len && strncmp(start, label, part_len) == 0) {
                matched_type = 1;
                break;
            }
            if (!end) break;
            start = end + 1;
        }
        if (!matched_type) return 0;
    }
    if (label_contains && strstr(c->label, label_contains) == NULL) return 0;
    if (rows_filter > 0 && c->rows != rows_filter) return 0;
    if (in_dim_filter > 0 && c->in_dim != in_dim_filter) return 0;
    if (out_dim_filter > 0 && c->out_dim != out_dim_filter) return 0;
    return 1;
}

static int supports_quant_weight_mul_mat(enum ggml_type type) {
    if (!ggml_is_quantized(type) || !get_type_traits_cpu) return 0;
    const struct ggml_type_traits_cpu * traits = get_type_traits_cpu(type);
    return traits && traits->from_float && traits->vec_dot;
}

static int bench_one_projections(const struct bench_case * c, int warmup_iters, int measure_iters, int threads, int projections) {
    if (projections < 1 || projections > 3) {
        fprintf(stderr, "invalid projections=%d for %s; expected 1..3\n", projections, c->label);
        return 1;
    }
    const int64_t weight_elems = c->out_dim * c->in_dim;
    const int64_t input_elems = c->rows * c->in_dim;
    if (!supports_quant_weight_mul_mat(c->type)) {
        printf("ggml_gliner_kernel label=%s type=%s rows=%lld in=%lld out=%lld projections=%d skipped=unsupported_weight_type\n",
               c->label,
               type_label(c->type),
               (long long) c->rows,
               (long long) c->in_dim,
               (long long) c->out_dim,
               projections);
        return 0;
    }
    const size_t weight_f32_bytes = (size_t) weight_elems * sizeof(float);
    const size_t input_bytes = (size_t) input_elems * sizeof(float);
    const size_t quant_bytes = ggml_row_size(c->type, c->in_dim) * (size_t) c->out_dim;

    float * weight_f32[3] = { NULL, NULL, NULL };
    float * input_f32 = (float *) malloc(input_bytes);
    void * weight_quant[3] = { NULL, NULL, NULL };
    for (int p = 0; p < projections; ++p) {
        weight_f32[p] = (float *) malloc(weight_f32_bytes);
        weight_quant[p] = malloc(quant_bytes);
    }
    if (!input_f32) {
        fprintf(stderr, "alloc failed for %s\n", c->label);
        free(input_f32);
        for (int p = 0; p < projections; ++p) {
            free(weight_f32[p]);
            free(weight_quant[p]);
        }
        return 1;
    }
    for (int p = 0; p < projections; ++p) {
        if (!weight_f32[p] || !weight_quant[p]) {
            fprintf(stderr, "alloc failed for %s projection=%d\n", c->label, p);
            free(input_f32);
            for (int q = 0; q < projections; ++q) {
                free(weight_f32[q]);
                free(weight_quant[q]);
            }
            return 1;
        }
    }

    uint32_t rng = 0xC11C0001u;
    const float scale = 1.0f / sqrtf((float) c->in_dim);
    for (int p = 0; p < projections; ++p) {
        for (int64_t i = 0; i < weight_elems; ++i) {
            weight_f32[p][i] = rand_f32(&rng) * scale;
        }
    }
    for (int64_t i = 0; i < input_elems; ++i) {
        input_f32[i] = rand_f32(&rng);
    }

    for (int p = 0; p < projections; ++p) {
        const size_t wrote = ggml_quantize_chunk(c->type, weight_f32[p], weight_quant[p], 0, c->out_dim, c->in_dim, NULL);
        if (wrote == 0 || wrote > quant_bytes) {
            fprintf(stderr, "quantization failed for %s projection=%d type=%s wrote=%zu quant_bytes=%zu\n", c->label, p, type_label(c->type), wrote, quant_bytes);
            free(input_f32);
            for (int q = 0; q < projections; ++q) {
                free(weight_f32[q]);
                free(weight_quant[q]);
            }
            return 1;
        }
    }

    const size_t ctx_bytes =
        quant_bytes * (size_t) projections +
        input_bytes +
        (size_t) projections * (size_t) c->rows * (size_t) c->out_dim * sizeof(float) +
        128u * 1024u * 1024u;
    struct ggml_init_params params = {
        .mem_size = ctx_bytes,
        .mem_buffer = NULL,
        .no_alloc = false,
    };
    struct ggml_context * ctx = ggml_init(params);
    if (!ctx) {
        fprintf(stderr, "ggml_init failed for %s\n", c->label);
        free(input_f32);
        for (int p = 0; p < projections; ++p) {
            free(weight_f32[p]);
            free(weight_quant[p]);
        }
        return 1;
    }

    struct ggml_tensor * weight[3] = { NULL, NULL, NULL };
    struct ggml_tensor * out[3] = { NULL, NULL, NULL };
    struct ggml_tensor * input = ggml_new_tensor_2d(ctx, GGML_TYPE_F32, c->in_dim, c->rows);
    for (int p = 0; p < projections; ++p) {
        weight[p] = ggml_new_tensor_2d(ctx, c->type, c->in_dim, c->out_dim);
        memcpy(weight[p]->data, weight_quant[p], quant_bytes);
    }
    memcpy(input->data, input_f32, input_bytes);

    struct ggml_cgraph * graph = ggml_new_graph(ctx);
    for (int p = 0; p < projections; ++p) {
        out[p] = ggml_mul_mat(ctx, weight[p], input);
        ggml_build_forward_expand(graph, out[p]);
    }

    uint64_t warmup_ns = 0;
    for (int i = 0; i < warmup_iters; ++i) {
        const uint64_t start = now_ns();
        enum ggml_status status = graph_compute_with_ctx(ctx, graph, threads);
        warmup_ns += now_ns() - start;
        if (status != GGML_STATUS_SUCCESS) {
            fprintf(stderr, "ggml compute failed for %s: %s\n", c->label, ggml_status_to_string(status));
            ggml_free(ctx);
            free(input_f32);
            for (int p = 0; p < projections; ++p) {
                free(weight_f32[p]);
                free(weight_quant[p]);
            }
            return 1;
        }
    }

    uint64_t total_ns = 0;
    uint64_t min_ns = UINT64_MAX;
    volatile float checksum = 0.0f;
    for (int i = 0; i < measure_iters; ++i) {
        const uint64_t start = now_ns();
        enum ggml_status status = graph_compute_with_ctx(ctx, graph, threads);
        const uint64_t elapsed = now_ns() - start;
        if (status != GGML_STATUS_SUCCESS) {
            fprintf(stderr, "ggml compute failed for %s: %s\n", c->label, ggml_status_to_string(status));
            ggml_free(ctx);
            free(input_f32);
            for (int p = 0; p < projections; ++p) {
                free(weight_f32[p]);
                free(weight_quant[p]);
            }
            return 1;
        }
        total_ns += elapsed;
        if (elapsed < min_ns) min_ns = elapsed;
        for (int p = 0; p < projections; ++p) {
            checksum += ((float *) out[p]->data)[0];
        }
    }

    const double warmup_ms = warmup_iters > 0 ? (double) warmup_ns / (double) warmup_iters / 1.0e6 : 0.0;
    const double avg_ms = (double) total_ns / (double) measure_iters / 1.0e6;
    const double min_ms = (double) min_ns / 1.0e6;
    printf("ggml_gliner_kernel label=%s type=%s rows=%lld in=%lld out=%lld projections=%d warmup_ms=%.3f avg_ms=%.3f min_ms=%.3f iters=%d checksum=%.6g\n",
           c->label,
           type_label(c->type),
           (long long) c->rows,
           (long long) c->in_dim,
           (long long) c->out_dim,
           projections,
           warmup_ms,
           avg_ms,
           min_ms,
           measure_iters,
           (double) checksum);

    ggml_free(ctx);
    free(input_f32);
    for (int p = 0; p < projections; ++p) {
        free(weight_f32[p]);
        free(weight_quant[p]);
    }
    return 0;
}

int main(int argc, char ** argv) {
    if (init_ggml_cpu_plugin() != 0) return 1;

    int warmup_iters = 1;
    int measure_iters = 5;
    int threads = 4;
    int projections = 1;
    int rows_filter = 0;
    int in_dim_filter = 0;
    int out_dim_filter = 0;
    if (parse_int_arg(argc, argv, "--warmup-iters", 1, 0, &warmup_iters) != 0) return 2;
    if (parse_int_arg(argc, argv, "--measure-iters", 5, 1, &measure_iters) != 0) return 2;
    if (parse_int_arg(argc, argv, "--threads", 4, 1, &threads) != 0) return 2;
    if (parse_int_arg(argc, argv, "--projections", 1, 1, &projections) != 0) return 2;
    if (projections > 3) {
        fprintf(stderr, "invalid --projections value: %d; expected 1..3\n", projections);
        return 2;
    }
    if (parse_int_arg(argc, argv, "--rows", 0, 0, &rows_filter) != 0) return 2;
    if (parse_int_arg(argc, argv, "--in-dim", 0, 0, &in_dim_filter) != 0) return 2;
    if (parse_int_arg(argc, argv, "--out-dim", 0, 0, &out_dim_filter) != 0) return 2;
    const char * type_filter = parse_string_arg(argc, argv, "--type");
    const char * type_filters = parse_string_arg(argc, argv, "--types");
    if (type_filters == NULL) type_filters = type_filter;
    const char * label_contains = parse_string_arg(argc, argv, "--label-contains");

    const struct bench_case cases[] = {
#define GLINER_CASES(TYPE) \
        { "gliner_qkv_128x768x768",       TYPE, 128,  768,  768 }, \
        { "gliner_ffn_up_128x768x3072",   TYPE, 128,  768, 3072 }, \
        { "gliner_ffn_down_128x3072x768", TYPE, 128, 3072,  768 }

#define CLIPCLAP_CASES(TYPE) \
        { "clip_text_qkv_77x768x768",   TYPE,  77,  768,  768 }, \
        { "clap_text_qkv_257x768x768",  TYPE, 257,  768,  768 }, \
        { "clipclap_proj_1x768x768",    TYPE,   1,  768,  768 }, \
        { "clipclap_mlp_up_1x768x3072", TYPE,   1,  768, 3072 }

        GLINER_CASES(GGML_TYPE_Q1_0),
        GLINER_CASES(GGML_TYPE_Q4_0),
        GLINER_CASES(GGML_TYPE_Q4_1),
        GLINER_CASES(GGML_TYPE_Q5_0),
        GLINER_CASES(GGML_TYPE_Q5_1),
        GLINER_CASES(GGML_TYPE_Q8_0),
        GLINER_CASES(GGML_TYPE_Q8_1),
        GLINER_CASES(GGML_TYPE_Q2_K),
        GLINER_CASES(GGML_TYPE_Q3_K),
        GLINER_CASES(GGML_TYPE_Q4_K),
        GLINER_CASES(GGML_TYPE_Q5_K),
        GLINER_CASES(GGML_TYPE_Q6_K),
        GLINER_CASES(GGML_TYPE_Q8_K),

        CLIPCLAP_CASES(GGML_TYPE_Q1_0),
        CLIPCLAP_CASES(GGML_TYPE_Q4_0),
        CLIPCLAP_CASES(GGML_TYPE_Q4_1),
        CLIPCLAP_CASES(GGML_TYPE_Q5_0),
        CLIPCLAP_CASES(GGML_TYPE_Q5_1),
        CLIPCLAP_CASES(GGML_TYPE_Q8_0),
        CLIPCLAP_CASES(GGML_TYPE_Q8_1),
        CLIPCLAP_CASES(GGML_TYPE_Q2_K),
        CLIPCLAP_CASES(GGML_TYPE_Q3_K),
        CLIPCLAP_CASES(GGML_TYPE_Q4_K),
        CLIPCLAP_CASES(GGML_TYPE_Q5_K),
        CLIPCLAP_CASES(GGML_TYPE_Q6_K),
        CLIPCLAP_CASES(GGML_TYPE_Q8_K),
#undef GLINER_CASES
#undef CLIPCLAP_CASES
    };

    size_t matched = 0;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        if (!case_matches_filters(&cases[i], type_filters, label_contains, rows_filter, in_dim_filter, out_dim_filter)) continue;
        ++matched;
        if (bench_one_projections(&cases[i], warmup_iters, measure_iters, threads, projections) != 0) {
            ggml_quantize_free();
            return 1;
        }
    }
    if (matched == 0) {
        fprintf(
            stderr,
            "no ggml_gliner_kernel cases matched filters type=%s label_contains=%s rows=%d in=%d out=%d projections=%d\n",
            type_filters ? type_filters : "*",
            label_contains ? label_contains : "*",
            rows_filter,
            in_dim_filter,
            out_dim_filter,
            projections
        );
        ggml_quantize_free();
        return 2;
    }
    ggml_quantize_free();
    return 0;
}
