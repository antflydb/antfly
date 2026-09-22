# Copyright 2026 Antfly, Inc.
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

"""Independent BGE-M3 GGUF reference using gguf-py dequantization and ONNX Runtime.

The official graph supplies operators; every initializer is replaced with the
selected GGUF artifact's weights. This separates execution parity from lossy
conversion accuracy. No Antfly tensor loader or compute code is used.
"""

import gguf
import numpy as np
import onnx
import onnxruntime as ort


def add_gguf_initializers(options, model_path, gguf_path):
    reader = gguf.GGUFReader(str(gguf_path))
    model = onnx.load(model_path, load_external_data=False)
    embeddings = {
        "token_embd": "word_embeddings",
        "position_embd": "position_embeddings",
        "token_types": "token_type_embeddings",
        "token_embd_norm": "LayerNorm",
    }
    blocks = {
        "attn_q": "attention.self.query",
        "attn_k": "attention.self.key",
        "attn_v": "attention.self.value",
        "attn_output": "attention.output.dense",
        "attn_output_norm": "attention.output.LayerNorm",
        "ffn_up": "intermediate.dense",
        "ffn_down": "output.dense",
        "layer_output_norm": "output.LayerNorm",
    }
    weights = {}
    for tensor in reader.tensors:
        if tensor.name.startswith("blk."):
            _, layer, part, suffix = tensor.name.split(".")
            name = f"0.auto_model.encoder.layer.{layer}.{blocks[part]}.{suffix}"
        else:
            part, suffix = tensor.name.split(".")
            name = f"0.auto_model.embeddings.{embeddings[part]}.{suffix}"
        weights[name] = tensor
    matmuls = {
        node.input[1]: node.name.removesuffix("/MatMul").strip("/").replace("/", ".")
        + ".weight"
        for node in model.graph.node
        if node.op_type == "MatMul" and node.input[1].startswith("onnx::MatMul")
    }
    # ORT borrows these buffers. The caller retains them until session teardown.
    retained = []
    for initializer in model.graph.initializer:
        name = matmuls.get(initializer.name, initializer.name)
        tensor = weights[name]  # Fail closed if the official graph layout changes.
        data = gguf.quants.dequantize(tensor.data, tensor.tensor_type).reshape(
            tuple(int(dim) for dim in reversed(tensor.shape))
        )
        if initializer.name in matmuls:
            data = data.T
        if name.endswith("position_embeddings.weight"):
            # llama.cpp removes the two reserved XLM-R position rows. Restore
            # them for the official graph, which computes unshifted HF indices.
            if data.shape[0] + 2 != initializer.dims[0]:
                raise ValueError("unexpected BGE-M3 GGUF position-table layout")
            data = np.pad(data, ((2, 0), (0, 0)))
        data = np.ascontiguousarray(
            data.reshape(tuple(initializer.dims)), dtype=np.float32
        )
        value = ort.OrtValue.ortvalue_from_numpy(data)
        retained.append((value, data))
        options.add_initializer(initializer.name, value)
    return retained
