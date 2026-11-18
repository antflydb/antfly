// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
            require(c.kind == "text" || c.kind == "image" || c.kind == "audio", "Oracle supports text, image and audio inputs");
            require(c.budget > 0 && c.budget <= 8192 && c.expected + size_t(c.budget) <= 8192, "Invalid context/output budget");
            c.prompt = read_text(base / (c.id + ".prompt"));
            cases.push_back(std::move(c));
        }
        require(!cases.empty(), "Empty suite");
        auto run = [&](const Case & c, bool capture) {
            llama_memory_clear(llama_get_memory(ctx), true);
            if (sampler) llama_sampler_reset(sampler);
            // KV reset is outside timing; finish its device work before starting.
            llama_synchronize(ctx);
            const auto started = std::chrono::steady_clock::now();
            std::vector<const mtmd_bitmap *> bitmaps;
            mtmd_helper_bitmap_wrapper bitmap{};
            if (c.kind != "text") {
                auto opt = mtmd_helper_init_opt_default();
                bitmap = mtmd_helper_bitmap_init_from_file(media, c.path.c_str(), false, opt);
                require(bitmap.bitmap != nullptr, "Media decoding failed");
                bitmaps.push_back(bitmap.bitmap);
            }
            mtmd_input_text input{c.prompt.data(), c.prompt.size(), false, true};
            auto * chunks = mtmd_input_chunks_init();
            require(mtmd_tokenize(media, chunks, &input, bitmaps.data(), bitmaps.size()) == 0, "Media tokenization failed");
            const auto count = mtmd_helper_get_n_tokens(chunks);
            require(count == c.expected, "Reference/native prompt geometry differs");
            llama_pos pos = 0;
            require(mtmd_helper_eval_chunks(media, ctx, chunks, 0, 0, 512, true, &pos) == 0, "Reference prefill failed");
            const auto prefilled = std::chrono::steady_clock::now();
            std::vector<llama_token> tokens;
            tokens.reserve(c.budget);
            float * logits = measure ? nullptr : llama_get_logits_ith(ctx, -1);
            // Validation alone downloads full logits and features. Measurement
            // obtains one backend-sampled token per step, as a serving client would.
            if (capture) {
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
        mtmd_free(media); llama_batch_ext_free(step_batch); llama_free(ctx);
        if (sampler) llama_sampler_free(sampler);
        llama_model_free(model); llama_backend_free();
    } catch (const std::exception & error) {
        std::fprintf(stderr, "Reference failed: %s\n", error.what());
        return 1;
    }
}
