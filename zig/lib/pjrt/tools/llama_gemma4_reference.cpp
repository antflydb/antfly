// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Thin oracle over the unmodified llama.cpp / libmtmd C APIs.
#include "llama.h"
#include "mtmd-helper.h"
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

static void require(bool ok, const char * message) {
    if (!ok) throw std::runtime_error(message);
}
static std::string read_text(const std::filesystem::path & path) {
    std::ifstream in(path, std::ios::binary);
    require(bool(in), "Cannot read oracle input");
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}
int main(int argc, char ** argv) {
    try {
        require(argc == 5, "Expected model projector input-directory output-directory");
        if (const char * path = std::getenv("GGML_BACKEND_PATH")) ggml_backend_load_all_from_path(path);
        else ggml_backend_load_all();
        require(ggml_backend_dev_by_type(GGML_BACKEND_DEVICE_TYPE_GPU) != nullptr, "CUDA backend unavailable");
        llama_backend_init();
        auto mp = llama_model_default_params();
        mp.n_gpu_layers = -1;
        auto * model = llama_model_load_from_file(argv[1], mp);
        require(model != nullptr, "Model loading failed");
        auto cp = llama_context_default_params();
        cp.n_ctx = 8192; cp.n_batch = 2048; cp.n_ubatch = 512;
        cp.n_threads = 2; cp.n_threads_batch = 2;
        cp.type_k = GGML_TYPE_F16; cp.type_v = GGML_TYPE_F16;
        cp.flash_attn_type = LLAMA_FLASH_ATTN_TYPE_ENABLED;
        auto * ctx = llama_init_from_model(model, cp);
        require(ctx != nullptr, "Context initialization failed");
        auto vp = mtmd_context_params_default();
        vp.use_gpu = true; vp.n_threads = 2;
        vp.image_min_tokens = 256; vp.image_max_tokens = 280;
        vp.media_marker = "[ANTFLY_REFERENCE_MEDIA]";
        auto * media = mtmd_init_from_file(argv[2], model, vp);
        require(media != nullptr, "Projector initialization failed");
        const auto * vocab = llama_model_get_vocab(model);
        const auto n_vocab = llama_vocab_n_tokens(vocab);
        require(n_vocab == 262144, "Expected E4B vocabulary");
        std::filesystem::path base(argv[3]), out(argv[4]);
        std::filesystem::create_directories(out);
        std::istringstream suite(read_text(base / "cases.tsv"));
        std::string line;
        while (std::getline(suite, line)) {
            std::istringstream fields(line);
            std::string id, kind, path; size_t expected; int budget;
            require(bool(fields >> id >> kind >> path >> expected >> budget), "Invalid case");
            require(kind == "text" || kind == "image" || kind == "audio", "Oracle supports text, image and audio inputs");
            require(budget > 0 && budget <= 8192 && expected + size_t(budget) <= 8192, "Invalid context/output budget");
            llama_memory_clear(llama_get_memory(ctx), true);
            const auto prompt = read_text(base / (id + ".prompt"));
            std::vector<const mtmd_bitmap *> bitmaps;
            mtmd_helper_bitmap_wrapper bitmap{};
            if (kind != "text") {
                auto opt = mtmd_helper_init_opt_default();
                opt.video_params.fps_target = 1;
                opt.video_params.timestamp_interval_ms = 1000;
                bitmap = mtmd_helper_bitmap_init_from_file(media, path.c_str(), false, opt);
                require(bitmap.bitmap != nullptr, "Media decoding failed");
                bitmaps.push_back(bitmap.bitmap);
            }
            mtmd_input_text input{prompt.data(), prompt.size(), false, true};
            auto * chunks = mtmd_input_chunks_init();
            require(mtmd_tokenize(media, chunks, &input, bitmaps.data(), bitmaps.size()) == 0, "Media tokenization failed");
            const auto count = mtmd_helper_get_n_tokens(chunks);
            std::ofstream prepared(out / (id + ".prompt-tokens"));
            for (size_t chunk_index = 0; chunk_index < mtmd_input_chunks_size(chunks); ++chunk_index) {
                const auto * chunk = mtmd_input_chunks_get(chunks, chunk_index);
                if (mtmd_input_chunk_get_type(chunk) == MTMD_INPUT_CHUNK_TYPE_TEXT) {
                    size_t length = 0;
                    const auto * text = mtmd_input_chunk_get_tokens_text(chunk, &length);
                    for (size_t i = 0; i < length; ++i) prepared << text[i] << '\n';
                } else {
                    for (size_t i = 0; i < mtmd_input_chunk_get_n_tokens(chunk); ++i) prepared << -1 << '\n';
                }
            }
            std::fprintf(stderr, "reference %s prompt=%zu expected=%zu\n", id.c_str(), count, expected);
            require(count == expected, "Reference/native prompt geometry differs");
            llama_pos pos = 0;
            require(mtmd_helper_eval_chunks(media, ctx, chunks, 0, 0, 512, true, &pos) == 0, "Reference prefill failed");
            if (kind == "audio") {
                size_t media_tokens = 0, media_chunks = 0;
                for (size_t i = 0; i < mtmd_input_chunks_size(chunks); ++i) {
                    const auto * chunk = mtmd_input_chunks_get(chunks, i);
                    if (mtmd_input_chunk_get_type(chunk) != MTMD_INPUT_CHUNK_TYPE_TEXT) {
                        media_tokens += mtmd_input_chunk_get_n_tokens(chunk);
                        ++media_chunks;
                    }
                }
                // The API exposes only the last encode pass, not all chunks.
                if (media_chunks == 1) {
                    const float * features = mtmd_get_output_embd(media);
                    require(features != nullptr && llama_model_n_embd_inp(model) == 2560, "Invalid audio feature geometry");
                    std::ofstream feature_file(out / (id + ".media-f32"), std::ios::binary);
                    feature_file.write(reinterpret_cast<const char *>(features), media_tokens * 2560 * sizeof(float));
                    require(bool(feature_file), "Audio feature output failed");
                }
            }
            auto * logits = llama_get_logits_ith(ctx, -1);
            require(logits != nullptr, "No logits");
            for (int i = 0; i < n_vocab; ++i) require(std::isfinite(logits[i]), "Nonfinite logits");
            std::ofstream logit_file(out / (id + ".f32"), std::ios::binary);
            logit_file.write(reinterpret_cast<const char *>(logits), n_vocab * sizeof(float));
            require(bool(logit_file), "Logit output failed");
            std::ofstream tokens(out / (id + ".tokens"));
            for (int step = 0; step < budget; ++step) {
                require(logits != nullptr, "Missing decode logits");
                for (int i = 0; i < n_vocab; ++i) require(std::isfinite(logits[i]), "Nonfinite decode logits");
                auto token = static_cast<llama_token>(std::max_element(logits, logits + n_vocab) - logits);
                tokens << token << '\n';
                if (step + 1 < budget) {
                    auto batch = llama_batch_get_one(&token, 1);
                    require(llama_decode(ctx, batch) == 0, "Reference decode failed");
                    logits = llama_get_logits_ith(ctx, -1);
                }
            }
            require(bool(tokens), "Token output failed");
            mtmd_input_chunks_free(chunks);
            if (bitmap.bitmap) mtmd_bitmap_free(bitmap.bitmap);
            if (bitmap.video_ctx) mtmd_helper_video_free(bitmap.video_ctx);
        }
        mtmd_free(media); llama_free(ctx); llama_model_free(model); llama_backend_free();
    } catch (const std::exception & error) {
        std::fprintf(stderr, "Reference failed: %s\n", error.what());
        return 1;
    }
}
