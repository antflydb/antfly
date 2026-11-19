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

// Oracle and fixed-budget benchmark over unmodified llama.cpp / libmtmd APIs.
#include "llama.h"
#include "mtmd-helper.h"
#include <algorithm>
#include <chrono>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#ifdef ANTFLY_REFERENCE_PYAV_VIDEO
#include "llama_gemma4_pyav.h"
#endif

static void require(bool ok, const char * message) {
    if (!ok) throw std::runtime_error(message);
}
static std::string read_text(const std::filesystem::path & path) {
    std::ifstream in(path, std::ios::binary);
    require(bool(in), "Cannot read oracle input");
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}
struct Case {
    std::string id, kind, path, prompt;
    size_t expected;
    int budget;
};
struct Row {
    std::string id;
    size_t prompt_tokens;
    double ms;
    double input_and_prefill_ms, decode_and_readback_ms;
    std::vector<llama_token> tokens;
};
int main(int argc, char ** argv) {
    try {
        require(argc == 5 || (argc == 8 && std::string(argv[5]) == "--measure"),
                "Expected model projector input-directory output-directory [--measure warmups repeats]");
        const bool measure = argc == 8;
        const int warmups = measure ? std::stoi(argv[6]) : 0;
        const int repeats = measure ? std::stoi(argv[7]) : 1;
        require(warmups >= 0 && warmups <= 100 && repeats >= 1 && repeats <= 100, "Invalid campaign bounds");
        if (const char * path = std::getenv("GGML_BACKEND_PATH")) ggml_backend_load_all_from_path(path);
        else ggml_backend_load_all();
        require(ggml_backend_dev_by_type(GGML_BACKEND_DEVICE_TYPE_GPU) != nullptr, "CUDA backend unavailable");
        llama_backend_init();
        auto mp = llama_model_default_params();
        mp.n_gpu_layers = -1;
        auto * model = llama_model_load_from_file(argv[1], mp);
        require(model != nullptr, "Model loading failed");
        llama_sampler * sampler = nullptr;
        llama_sampler_seq_config sampling{};
        if (measure) {
            sampler = llama_sampler_chain_init(llama_sampler_chain_default_params());
            require(sampler != nullptr, "Sampler initialization failed");
            llama_sampler_chain_add(sampler, llama_sampler_init_greedy());
            sampling = {0, sampler};
        }
        auto cp = llama_context_default_params();
        cp.n_ctx = 8192; cp.n_batch = 2048; cp.n_ubatch = 512;
        cp.n_threads = 2; cp.n_threads_batch = 2;
        cp.type_k = GGML_TYPE_F16; cp.type_v = GGML_TYPE_F16;
        cp.flash_attn_type = LLAMA_FLASH_ATTN_TYPE_ENABLED;
        if (measure) { cp.samplers = &sampling; cp.n_samplers = 1; }
        auto * ctx = llama_init_from_model(model, cp);
        require(ctx != nullptr, "Context initialization failed");
        auto * step_batch = llama_batch_ext_init(ctx);
        require(step_batch != nullptr, "Decode batch initialization failed");
        auto vp = mtmd_context_params_default();
        vp.use_gpu = true; vp.n_threads = 2;
        vp.image_min_tokens = 256; vp.image_max_tokens = 280;
        vp.media_marker = "[ANTFLY_REFERENCE_MEDIA]";
        auto * media = mtmd_init_from_file(argv[2], model, vp);
        require(media != nullptr, "Projector initialization failed");
        const auto n_vocab = llama_vocab_n_tokens(llama_model_get_vocab(model));
        require(n_vocab == 262144, "Expected E4B vocabulary");
        std::filesystem::path base(argv[3]), out(argv[4]);
        require(!std::filesystem::exists(out), "Output directory must be new");
        std::filesystem::create_directories(out);
        std::istringstream suite(read_text(base / "cases.tsv"));
        std::vector<Case> cases;
        std::string line;
        while (std::getline(suite, line)) {
            std::istringstream fields(line);
            Case c;
            require(bool(fields >> c.id >> c.kind >> c.path >> c.expected >> c.budget), "Invalid case");
            require(!c.id.empty() && std::all_of(c.id.begin(), c.id.end(), [](unsigned char v) {
                return std::isalnum(v) || v == '_' || v == '-';
            }), "Invalid case identifier");
            require(std::none_of(cases.begin(), cases.end(), [&](const Case & prior) { return prior.id == c.id; }), "Duplicate case");
            require(c.kind == "text" || c.kind == "image" || c.kind == "audio" || c.kind == "video", "Unsupported media kind");
            require(c.budget > 0 && c.budget <= 8192 && c.expected + size_t(c.budget) <= 8192, "Invalid context/output budget");
            c.prompt = read_text(base / (c.id + ".prompt"));
            cases.push_back(std::move(c));
        }
        require(!cases.empty(), "Empty suite");
        // Match the official processor's 70 soft tokens per video frame. Image
        // requests retain their separate 280-token projector configuration.
        mtmd_context * video_media = nullptr;
        if (std::any_of(cases.begin(), cases.end(), [](const Case & c) { return c.kind == "video"; })) {
#ifdef ANTFLY_REFERENCE_PYAV_VIDEO
            reference_pyav_init();
#endif
            auto video_params = vp;
            video_params.image_min_tokens = 70;
            video_params.image_max_tokens = 70;
            video_media = mtmd_init_from_file(argv[2], model, video_params);
            require(video_media != nullptr, "Video projector initialization failed");
        }
        auto run = [&](const Case & c, bool capture) {
            llama_memory_clear(llama_get_memory(ctx), true);
            if (sampler) llama_sampler_reset(sampler);
            // KV reset is outside timing; finish its device work before starting.
            llama_synchronize(ctx);
            const auto started = std::chrono::steady_clock::now();
            std::vector<const mtmd_bitmap *> bitmaps;
            mtmd_helper_bitmap_wrapper bitmap{};
            auto * request_media = c.kind == "video" ? video_media : media;
            std::string prompt = c.prompt;
            if (c.kind == "video") {
                auto params = mtmd_helper_video_init_params_default();
                params.fps_target = 1.0f;
                params.timestamp_interval_ms = 0;
                params.no_soundtrack = true;
#ifdef ANTFLY_REFERENCE_PYAV_VIDEO
                params.decoder = &reference_pyav_decoder;
#endif
                bitmap.video_ctx = mtmd_helper_video_init(request_media, c.path.c_str(), params);
                require(bitmap.video_ctx != nullptr, "Video decoding unavailable");
                const auto info = mtmd_helper_video_get_info(bitmap.video_ctx);
                require(info.fps == 1.0f, "Expected one-Hz video sampling");
                std::string frames;
                for (;;) {
                    mtmd_bitmap * frame = nullptr;
                    char * text = nullptr;
                    const auto status = mtmd_helper_video_read_next(bitmap.video_ctx, &frame, &text);
                    if (status == -1) break;
                    require(status == 0 && bool(frame) != bool(text), "Video decode failed");
                    // The generic helper's prose timestamps differ from Gemma's
                    // template. Use the same one-Hz timestamps as Transformers.
                    if (text) { std::free(text); continue; }
                    require(bitmaps.size() < 60, "Video exceeds qualification scope");
                    char timestamp[32];
                    std::snprintf(timestamp, sizeof(timestamp), "%s00:%02zu [ANTFLY_REFERENCE_MEDIA]",
                                  bitmaps.empty() ? "" : " ", bitmaps.size());
                    frames += timestamp;
                    bitmaps.push_back(frame);
                }
                require(!bitmaps.empty(), "Empty video");
                const std::string marker = "[ANTFLY_REFERENCE_MEDIA]";
                const auto offset = prompt.find(marker);
                require(offset != std::string::npos && prompt.find(marker, offset + marker.size()) == std::string::npos,
                        "Expected one video marker");
                prompt.replace(offset, marker.size(), frames + " ");
            } else if (c.kind != "text") {
                auto opt = mtmd_helper_init_opt_default();
                bitmap = mtmd_helper_bitmap_init_from_file(media, c.path.c_str(), false, opt);
                require(bitmap.bitmap != nullptr, "Media decoding failed");
                bitmaps.push_back(bitmap.bitmap);
            }
            mtmd_input_text input{prompt.data(), prompt.size(), false, true};
            auto * chunks = mtmd_input_chunks_init();
            require(mtmd_tokenize(request_media, chunks, &input, bitmaps.data(), bitmaps.size()) == 0, "Media tokenization failed");
            const auto count = mtmd_helper_get_n_tokens(chunks);
            require(count == c.expected, "Reference/native prompt geometry differs");
            llama_pos pos = 0;
            require(mtmd_helper_eval_chunks(request_media, ctx, chunks, 0, 0, 512, true, &pos) == 0, "Reference prefill failed");
            const auto prefilled = std::chrono::steady_clock::now();
            std::vector<llama_token> tokens;
            tokens.reserve(c.budget);
            float * logits = measure ? nullptr : llama_get_logits_ith(ctx, -1);
            // Validation alone downloads full logits and features. Measurement
            // obtains one backend-sampled token per step, as a serving client would.
            if (capture) {
#ifdef ANTFLY_REFERENCE_PYAV_VIDEO
                if (c.kind == "video") reference_pyav_capture(out / (c.id + ".frames.json"));
#endif
                require(logits != nullptr, "No logits");
                std::ofstream logit_file(out / (c.id + ".f32"), std::ios::binary);
                logit_file.write(reinterpret_cast<const char *>(logits), n_vocab * sizeof(float));
                require(bool(logit_file), "Logit output failed");
                std::ofstream prepared(out / (c.id + ".prompt-tokens"));
                size_t media_tokens = 0, media_chunks = 0;
                for (size_t j = 0; j < mtmd_input_chunks_size(chunks); ++j) {
                    const auto * chunk = mtmd_input_chunks_get(chunks, j);
                    if (mtmd_input_chunk_get_type(chunk) == MTMD_INPUT_CHUNK_TYPE_TEXT) {
                        size_t length = 0;
                        const auto * text = mtmd_input_chunk_get_tokens_text(chunk, &length);
                        for (size_t i = 0; i < length; ++i) prepared << text[i] << '\n';
                    } else {
                        const auto length = mtmd_input_chunk_get_n_tokens(chunk);
                        for (size_t i = 0; i < length; ++i) prepared << -1 << '\n';
                        media_tokens += length; ++media_chunks;
                    }
                }
                require(bool(prepared), "Prompt output failed");
                if (c.kind == "audio" && media_chunks == 1) {
                    const auto * features = mtmd_get_output_embd(media);
                    require(features != nullptr && llama_model_n_embd_inp(model) == 2560, "Invalid audio feature geometry");
                    std::ofstream file(out / (c.id + ".media-f32"), std::ios::binary);
                    file.write(reinterpret_cast<const char *>(features), media_tokens * 2560 * sizeof(float));
                    require(bool(file), "Audio feature output failed");
                }
            }
            for (int step = 0; step < c.budget; ++step) {
                llama_token token;
                if (measure) {
                    token = llama_get_sampled_token_ith(ctx, -1);
                    require(token >= 0 && token < n_vocab, "Missing backend sampled token");
                    llama_sampler_accept(sampler, token);
                } else {
                    require(logits != nullptr, "Missing decode logits");
                    for (int i = 0; i < n_vocab; ++i) require(std::isfinite(logits[i]), "Nonfinite decode logits");
                    token = static_cast<llama_token>(std::max_element(logits, logits + n_vocab) - logits);
                }
                tokens.push_back(token);
                if (step + 1 < c.budget) {
                    llama_batch_ext_clear(step_batch);
                    const auto entry = llama_batch_ext_add_token(step_batch, 0, token);
                    require(entry >= 0, "Decode batch append failed");
                    const llama_pos position = pos + step;
                    require(llama_batch_ext_set_pos(step_batch, entry, &position), "Decode position failed");
                    require(llama_batch_ext_set_output_logits(step_batch, entry, true), "Decode output failed");
                    require(llama_process(ctx, LLAMA_PROCESS_TYPE_DECODE, step_batch) == 0, "Reference decode failed");
                    if (!measure) logits = llama_get_logits_ith(ctx, -1);
                }
            }
            llama_synchronize(ctx);
            const auto finished = std::chrono::steady_clock::now();
            const double elapsed = std::chrono::duration<double, std::milli>(finished - started).count();
            mtmd_input_chunks_free(chunks);
            if (bitmap.bitmap) mtmd_bitmap_free(bitmap.bitmap);
            if (c.kind == "video") for (const auto * frame : bitmaps) mtmd_bitmap_free(const_cast<mtmd_bitmap *>(frame));
            if (bitmap.video_ctx) mtmd_helper_video_free(bitmap.video_ctx);
            if (capture) {
                std::ofstream file(out / (c.id + ".tokens"));
                for (auto token : tokens) file << token << '\n';
                require(bool(file), "Token output failed");
            }
            return Row{c.id, count, elapsed,
                std::chrono::duration<double, std::milli>(prefilled - started).count(),
                std::chrono::duration<double, std::milli>(finished - prefilled).count(),
                std::move(tokens)};
        };
        if (measure) for (const auto & c : cases) (void)run(c, false);
        std::ofstream report(out / "result.json");
        report << std::setprecision(12) << "{\"backend\":\"llama.cpp CUDA\",\"sampling\":\"" << (measure ? "backend greedy" : "CPU argmax full logits")
#ifdef ANTFLY_REFERENCE_PYAV_VIDEO
               << "\",\"video_decoder\":\"PyAV via unmodified libmtmd custom-decoder API"
#else
               << "\",\"video_decoder\":\"libmtmd ffmpeg helper"
#endif
               << "\",\"measurement\":" << (measure ? "true" : "false") << ",\"warmups\":" << warmups << ",\"repeats\":" << repeats
               << ",\"scope\":\"pre-rendered prompt; encoded media read, tokenization, towers, prefill, fixed-budget decode and synchronized token readback; excludes load, KV reset, template rendering, output text decoding and cleanup\",\"rows\":[";
        bool first = true;
        for (const auto & c : cases) for (int rep = 0; rep < warmups + repeats; ++rep) {
            auto row = run(c, !measure);
            if (rep < warmups) continue;
            if (!first) report << ',';
            first = false;
            report << "{\"id\":\"" << row.id << "\",\"repeat\":" << rep - warmups
                   << ",\"ms\":" << (measure ? std::to_string(row.ms) : "null")
                   << ",\"input_and_prefill_ms\":" << (measure ? std::to_string(row.input_and_prefill_ms) : "null")
                   << ",\"decode_and_readback_ms\":" << (measure ? std::to_string(row.decode_and_readback_ms) : "null")
                   << ",\"prompt_tokens\":" << row.prompt_tokens
                   << ",\"completion_tokens\":" << row.tokens.size() << ",\"tokens\":[";
            for (size_t i = 0; i < row.tokens.size(); ++i) { if (i) report << ','; report << row.tokens[i]; }
            report << "]}";
            std::fprintf(stderr, "llama-gemma4 %s rep=%d ms=%.3f prompt=%zu output=%zu\n", row.id.c_str(), rep - warmups, row.ms, row.prompt_tokens, row.tokens.size());
        }
        report << "]}\n";
        require(bool(report), "Report output failed");
        if (video_media) mtmd_free(video_media);
        mtmd_free(media); llama_batch_ext_free(step_batch); llama_free(ctx);
        if (sampler) llama_sampler_free(sampler);
        llama_model_free(model); llama_backend_free();
    } catch (const std::exception & error) {
        std::fprintf(stderr, "Reference failed: %s\n", error.what());
        return 1;
    }
}
