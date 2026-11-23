# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run Whisper through the generated GPU kernels on an OpenCL device.

The kernels are the OpenCL dialect of the same templates that render the
Metal and CUDA sources (src/tile/gpu_model.zig), so running this on PoCL
(OpenCL on the CPU) executes the GPU schedules - tiling, block reductions,
online softmax - without a GPU. The launch sequence mirrors
src/tile/whisper_model.zig and the launch contract in gpu_model.zig.

    zig build tile-kernel-codegen -- --emit-opencl /path/tile_whisper_model.cl
    pip install numpy pyopencl pocl-binary-distribution
    python run_whisper_opencl.py /path/tile_whisper_model.cl <hf-model-dir> <reference-dir>

`reference-dir` holds mel.f32 (input) and, optionally, encoder_out.f32,
step0_logits.f32 and reference.json to compare against.
"""

import json
import math
import os
import struct
import sys
import time

import numpy as np
import pyopencl as cl

GEMM_TILE, GEMM_THREADS = 64, 256
NORM_THREADS = 128
ATTENTION_THREADS = 32  # OpenCL dialect: one query per work-group
ELEMENT_THREADS = 256
HEAD_DIM = 64
PROMPT = [50258, 50259, 50359, 50363]  # <|startoftranscript|><|en|><|transcribe|><|notimestamps|>


def load_safetensors(path):
    with open(path, "rb") as f:
        n = struct.unpack("<Q", f.read(8))[0]
        header = json.loads(f.read(n))
        data = f.read()
    out = {}
    for name, info in header.items():
        if name == "__metadata__":
            continue
        assert info["dtype"] == "F32", name
        b, e = info["data_offsets"]
        out[name] = np.frombuffer(data[b:e], dtype=np.float32).reshape(info["shape"])
    return out


class Runner:
    def __init__(self, source):
        self.ctx = cl.Context(cl.get_platforms()[0].get_devices())
        self.queue = cl.CommandQueue(self.ctx)
        self.prg = cl.Program(self.ctx, source).build()
        self.kernels = {k.function_name: k for k in self.prg.all_kernels()}

    def buf(self, array):
        a = np.ascontiguousarray(array)
        return cl.Buffer(self.ctx, cl.mem_flags.READ_WRITE | cl.mem_flags.COPY_HOST_PTR, hostbuf=a)

    def empty(self, n):
        return cl.Buffer(self.ctx, cl.mem_flags.READ_WRITE, size=4 * max(n, 1))

    def read(self, b, n):
        out = np.empty(n, dtype=np.float32)
        cl.enqueue_copy(self.queue, out, b)
        return out

    def launch(self, name, groups, threads, *args):
        k = self.kernels[name]
        k(self.queue, (groups[0] * threads, groups[1]), (threads, 1), *args)

    @staticmethod
    def u32s(*values):
        return np.array(values, dtype=np.uint32)

    def gemm(self, name, x, w, out, m, n, k, bias=None, residual=None):
        args = [x, w] + ([bias] if bias is not None else []) + ([residual] if residual is not None else [])
        groups = (math.ceil(n / GEMM_TILE), math.ceil(m / GEMM_TILE))
        self.launch(name, groups, GEMM_THREADS, *args, out, self.u32s(m, n, k))

    def layer_norm(self, x, gamma, beta, out, rows, dim):
        self.launch("antfly_tile_layer_norm", (rows, 1), NORM_THREADS, x, gamma, beta, out, self.u32s(rows, dim))

    def attention(self, q, k, v, out, tq, tk, heads, ld, causal=False, q_offset=0, scale=1.0):
        params = np.zeros(1, dtype=[(f, "<u4") for f in ("tq", "tk", "heads", "ld_q", "ld_k", "ld_v", "ld_o", "causal", "q_offset")] + [("scale", "<f4")])
        params[0] = (tq, tk, heads, ld, ld, ld, ld, int(causal), q_offset, scale)
        self.launch("antfly_tile_attention_hd64", (tq, heads), ATTENTION_THREADS, q, k, v, out, params[0])

    def im2col(self, name, x, out, channels, t_in, t_out):
        total = t_out * channels * 3
        self.launch(name, (math.ceil(total / ELEMENT_THREADS), 1), ELEMENT_THREADS, x, out, self.u32s(channels, t_in, t_out))

    def embed(self, ids, table, pos, out, rows, dim, pos_offset):
        self.launch("antfly_tile_whisper_embed", (math.ceil(rows * dim / ELEMENT_THREADS), 1), ELEMENT_THREADS, ids, table, pos, out, self.u32s(rows, dim, pos_offset))


def report(label, want, got):
    d = np.abs(want - got)
    print(f"{label}: max |diff| {d.max():.2e} (values up to {np.abs(want).max():.2f}), rms diff {math.sqrt(float((d * d).mean())):.2e}")


def main():
    source_path, model_dir, ref_dir = sys.argv[1:4]
    cfg = json.load(open(os.path.join(model_dir, "config.json")))
    gen = json.load(open(os.path.join(model_dir, "generation_config.json")))
    w = load_safetensors(os.path.join(model_dir, "model.safetensors"))
    r = Runner(open(source_path).read())
    print("device:", r.ctx.devices[0].name)
    d, n_mels, ffn = cfg["d_model"], cfg["num_mel_bins"], cfg["encoder_ffn_dim"]
    t_enc, t_mel, vocab, heads = cfg["max_source_positions"], 2 * cfg["max_source_positions"], cfg["vocab_size"], cfg["encoder_attention_heads"]
    W = {name: r.buf(t) for name, t in w.items()}

    # ---- Encoder ----
    t0 = time.time()
    mel = r.buf(np.fromfile(os.path.join(ref_dir, "mel.f32"), dtype=np.float32))
    cols1, h1 = r.empty(t_mel * n_mels * 3), r.empty(t_mel * d)
    r.im2col("antfly_tile_whisper_im2col_mel", mel, cols1, n_mels, t_mel, t_mel)
    r.gemm("antfly_tile_gemm_bias_gelu", cols1, W["model.encoder.conv1.weight"], h1, t_mel, d, n_mels * 3, bias=W["model.encoder.conv1.bias"])
    cols2, h = r.empty(t_enc * d * 3), r.empty(t_enc * d)
    r.im2col("antfly_tile_whisper_im2col_hidden", h1, cols2, d, t_mel, t_enc)
    r.gemm("antfly_tile_gemm_bias_gelu_residual", cols2, W["model.encoder.conv2.weight"], h, t_enc, d, d * 3,
           bias=W["model.encoder.conv2.bias"], residual=W["model.encoder.embed_positions.weight"])
    xn, q, k, v, ctx = (r.empty(t_enc * d) for _ in range(5))
    ff = r.empty(t_enc * ffn)
    for i in range(cfg["encoder_layers"]):
        p = f"model.encoder.layers.{i}."
        r.layer_norm(h, W[p + "self_attn_layer_norm.weight"], W[p + "self_attn_layer_norm.bias"], xn, t_enc, d)
        r.gemm("antfly_tile_gemm_bias_scale", xn, W[p + "self_attn.q_proj.weight"], q, t_enc, d, d, bias=W[p + "self_attn.q_proj.bias"])
        r.gemm("antfly_tile_gemm", xn, W[p + "self_attn.k_proj.weight"], k, t_enc, d, d)
        r.gemm("antfly_tile_gemm_bias", xn, W[p + "self_attn.v_proj.weight"], v, t_enc, d, d, bias=W[p + "self_attn.v_proj.bias"])
        r.attention(q, k, v, ctx, t_enc, t_enc, heads, d)
        r.gemm("antfly_tile_gemm_bias_residual", ctx, W[p + "self_attn.out_proj.weight"], h, t_enc, d, d, bias=W[p + "self_attn.out_proj.bias"], residual=h)
        r.layer_norm(h, W[p + "final_layer_norm.weight"], W[p + "final_layer_norm.bias"], xn, t_enc, d)
        r.gemm("antfly_tile_gemm_bias_gelu", xn, W[p + "fc1.weight"], ff, t_enc, ffn, d, bias=W[p + "fc1.bias"])
        r.gemm("antfly_tile_gemm_bias_residual", ff, W[p + "fc2.weight"], h, t_enc, d, ffn, bias=W[p + "fc2.bias"], residual=h)
    enc = r.empty(t_enc * d)
    r.layer_norm(h, W["model.encoder.layer_norm.weight"], W["model.encoder.layer_norm.bias"], enc, t_enc, d)
    r.queue.finish()
    print(f"encoder: {time.time() - t0:.1f} s")
    ref_enc = os.path.join(ref_dir, "encoder_out.f32")
    if os.path.exists(ref_enc):
        report("encoder output vs reference", np.fromfile(ref_enc, dtype=np.float32), r.read(enc, t_enc * d))

    # ---- Decoder ----
    t0 = time.time()
    layers, text_ctx = cfg["decoder_layers"], cfg["max_target_positions"]
    cross_k, cross_v, self_k, self_v = [], [], [], []
    for i in range(layers):
        p = f"model.decoder.layers.{i}.encoder_attn."
        ck, cv = r.empty(t_enc * d), r.empty(t_enc * d)
        r.gemm("antfly_tile_gemm", enc, W[p + "k_proj.weight"], ck, t_enc, d, d)
        r.gemm("antfly_tile_gemm_bias", enc, W[p + "v_proj.weight"], cv, t_enc, d, d, bias=W[p + "v_proj.bias"])
        cross_k.append(ck), cross_v.append(cv)
        self_k.append(r.empty(text_ctx * d)), self_v.append(r.empty(text_ctx * d))
    tokens, length, pending = list(PROMPT), 0, list(PROMPT)
    logits = r.empty(vocab)
    suppress = np.array(gen["suppress_tokens"])
    begin_suppress = np.array(gen["begin_suppress_tokens"])
    first_logits = None
    while len(tokens) < text_ctx:
        n = len(pending)
        x, xn, qd, ctx_d, kn, vn = (r.empty(n * d) for _ in range(6))
        ffd = r.empty(n * ffn)
        r.embed(r.buf(np.array(pending, dtype=np.uint32)), W["model.decoder.embed_tokens.weight"], W["model.decoder.embed_positions.weight"], x, n, d, length)
        total = length + n
        for i in range(layers):
            p = f"model.decoder.layers.{i}."
            r.layer_norm(x, W[p + "self_attn_layer_norm.weight"], W[p + "self_attn_layer_norm.bias"], xn, n, d)
            r.gemm("antfly_tile_gemm_bias_scale", xn, W[p + "self_attn.q_proj.weight"], qd, n, d, d, bias=W[p + "self_attn.q_proj.bias"])
            r.gemm("antfly_tile_gemm", xn, W[p + "self_attn.k_proj.weight"], kn, n, d, d)
            r.gemm("antfly_tile_gemm_bias", xn, W[p + "self_attn.v_proj.weight"], vn, n, d, d, bias=W[p + "self_attn.v_proj.bias"])
            cl.enqueue_copy(r.queue, self_k[i], kn, dst_offset=length * d * 4, byte_count=n * d * 4)
            cl.enqueue_copy(r.queue, self_v[i], vn, dst_offset=length * d * 4, byte_count=n * d * 4)
            r.attention(qd, self_k[i], self_v[i], ctx_d, n, total, heads, d, causal=True, q_offset=length)
            r.gemm("antfly_tile_gemm_bias_residual", ctx_d, W[p + "self_attn.out_proj.weight"], x, n, d, d, bias=W[p + "self_attn.out_proj.bias"], residual=x)
            r.layer_norm(x, W[p + "encoder_attn_layer_norm.weight"], W[p + "encoder_attn_layer_norm.bias"], xn, n, d)
            r.gemm("antfly_tile_gemm_bias_scale", xn, W[p + "encoder_attn.q_proj.weight"], qd, n, d, d, bias=W[p + "encoder_attn.q_proj.bias"])
            r.attention(qd, cross_k[i], cross_v[i], ctx_d, n, t_enc, heads, d)
            r.gemm("antfly_tile_gemm_bias_residual", ctx_d, W[p + "encoder_attn.out_proj.weight"], x, n, d, d, bias=W[p + "encoder_attn.out_proj.bias"], residual=x)
            r.layer_norm(x, W[p + "final_layer_norm.weight"], W[p + "final_layer_norm.bias"], xn, n, d)
            r.gemm("antfly_tile_gemm_bias_gelu", xn, W[p + "fc1.weight"], ffd, n, ffn, d, bias=W[p + "fc1.bias"])
            r.gemm("antfly_tile_gemm_bias_residual", ffd, W[p + "fc2.weight"], x, n, d, ffn, bias=W[p + "fc2.bias"], residual=x)
        length = total
        last = r.empty(d)
        cl.enqueue_copy(r.queue, last, x, src_offset=(n - 1) * d * 4, byte_count=d * 4)
        r.layer_norm(last, W["model.decoder.layer_norm.weight"], W["model.decoder.layer_norm.bias"], xn, 1, d)
        r.gemm("antfly_tile_gemm", xn, W["model.decoder.embed_tokens.weight"], logits, 1, vocab, d)
        host_logits = r.read(logits, vocab)
        if first_logits is None:
            first_logits = host_logits.copy()
        host_logits[suppress] = -np.inf
        if len(tokens) == len(PROMPT):
            host_logits[begin_suppress] = -np.inf
        nxt = int(np.argmax(host_logits))
        tokens.append(nxt)
        if nxt == cfg["eos_token_id"]:
            break
        pending = [nxt]
    print(f"decoder: {len(tokens) - len(PROMPT)} tokens in {time.time() - t0:.1f} s")
    print("tokens:", tokens)
    ref_logits = os.path.join(ref_dir, "step0_logits.f32")
    if os.path.exists(ref_logits):
        report("first-step logits vs reference", np.fromfile(ref_logits, dtype=np.float32), first_logits)
    ref_json = os.path.join(ref_dir, "reference.json")
    if os.path.exists(ref_json):
        ref = json.load(open(ref_json))
        same = ref["tokens"] == tokens
        print("tokens vs reference:", "identical" if same else "DIFFERENT")
        print("reference text:", ref["text"])
        if not same:
            sys.exit(1)


if __name__ == "__main__":
    main()
