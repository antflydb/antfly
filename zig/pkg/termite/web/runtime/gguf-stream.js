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

const GGUF_MAGIC = 'GGUF';
const DEFAULT_ALIGNMENT = 32;
const DEFAULT_MAX_TENSOR_STAGING_BYTES = 512 * 1024 * 1024;

const KNOWN_TENSOR_TYPES = {
  F32: 0,
  F16: 1,
  Q4_0: 2,
  Q4_1: 3,
  Q5_0: 6,
  Q5_1: 7,
  Q8_0: 8,
  Q8_1: 9,
  Q2_K: 10,
  Q3_K: 11,
  Q4_K: 12,
  Q5_K: 13,
  Q6_K: 14,
  Q8_K: 15,
  IQ2_XXS: 16,
  IQ2_XS: 17,
  IQ3_XXS: 18,
  IQ1_S: 19,
  IQ4_NL: 20,
  IQ3_S: 21,
  IQ2_S: 22,
  IQ4_XS: 23,
  I8: 24,
  I16: 25,
  I32: 26,
  I64: 27,
  F64: 28,
  IQ1_M: 29,
  BF16: 30,
  TQ1_0: 34,
  TQ2_0: 35,
  I2_S: 36,
  I8_S: 37,
  TL1: 38,
  MXFP4: 39,
  NVFP4: 40,
  Q1_0: 41,
};

const metadataValueTypes = {
  u8: 0,
  i8: 1,
  u16: 2,
  i16: 3,
  u32: 4,
  i32: 5,
  f32: 6,
  bool_: 7,
  string: 8,
  array: 9,
  u64: 10,
  i64: 11,
  f64: 12,
};

const TOKENIZER_METADATA_KEYS = new Set([
  'tokenizer.ggml.model',
  'tokenizer.ggml.tokens',
  'tokenizer.ggml.merges',
  'tokenizer.ggml.scores',
  'tokenizer.ggml.token_type',
  'tokenizer.ggml.bos_token_id',
  'tokenizer.ggml.eos_token_id',
  'tokenizer.ggml.padding_token_id',
  'tokenizer.ggml.unknown_token_id',
  'tokenizer.ggml.add_space_prefix',
  'tokenizer.chat_template',
]);

const GPU_RESIDENT_QUANT_TYPES = new Set([
  KNOWN_TENSOR_TYPES.Q4_0,
  KNOWN_TENSOR_TYPES.Q4_1,
  KNOWN_TENSOR_TYPES.Q5_0,
  KNOWN_TENSOR_TYPES.Q5_1,
  KNOWN_TENSOR_TYPES.Q8_0,
  KNOWN_TENSOR_TYPES.Q8_1,
  KNOWN_TENSOR_TYPES.IQ4_NL,
  KNOWN_TENSOR_TYPES.IQ4_XS,
  KNOWN_TENSOR_TYPES.Q2_K,
  KNOWN_TENSOR_TYPES.Q3_K,
  KNOWN_TENSOR_TYPES.Q4_K,
  KNOWN_TENSOR_TYPES.Q5_K,
  KNOWN_TENSOR_TYPES.Q6_K,
  KNOWN_TENSOR_TYPES.Q8_K,
  KNOWN_TENSOR_TYPES.I2_S,
]);

function alignForward(value, alignment) {
  const remainder = value % alignment;
  return remainder === 0 ? value : value + alignment - remainder;
}

function formatByteSize(byteLen) {
  if (byteLen >= 1024 ** 3) return `${(byteLen / (1024 ** 3)).toFixed(2)} GiB`;
  if (byteLen >= 1024 ** 2) return `${(byteLen / (1024 ** 2)).toFixed(1)} MiB`;
  if (byteLen >= 1024) return `${(byteLen / 1024).toFixed(1)} KiB`;
  return `${byteLen} B`;
}

function addPtr(abi, ptr, delta) {
  return abi.isWasm64 ? ptr + BigInt(delta) : ptr + delta;
}

function valuesPerBlock(tensorTypeRaw) {
  switch (tensorTypeRaw) {
    case KNOWN_TENSOR_TYPES.F32:
    case KNOWN_TENSOR_TYPES.F16:
    case KNOWN_TENSOR_TYPES.BF16:
    case KNOWN_TENSOR_TYPES.I8:
    case KNOWN_TENSOR_TYPES.I16:
    case KNOWN_TENSOR_TYPES.I32:
    case KNOWN_TENSOR_TYPES.I64:
    case KNOWN_TENSOR_TYPES.F64:
    case KNOWN_TENSOR_TYPES.Q4_0:
    case KNOWN_TENSOR_TYPES.Q4_1:
    case KNOWN_TENSOR_TYPES.Q5_0:
    case KNOWN_TENSOR_TYPES.Q5_1:
    case KNOWN_TENSOR_TYPES.Q8_0:
    case KNOWN_TENSOR_TYPES.Q8_1:
    case KNOWN_TENSOR_TYPES.IQ4_NL:
    case KNOWN_TENSOR_TYPES.MXFP4:
      return 32;
    case KNOWN_TENSOR_TYPES.Q1_0:
    case KNOWN_TENSOR_TYPES.I2_S:
      return 128;
    case KNOWN_TENSOR_TYPES.I8_S:
    case KNOWN_TENSOR_TYPES.TL1:
      return 1;
    case KNOWN_TENSOR_TYPES.Q2_K:
    case KNOWN_TENSOR_TYPES.Q3_K:
    case KNOWN_TENSOR_TYPES.Q4_K:
    case KNOWN_TENSOR_TYPES.Q5_K:
    case KNOWN_TENSOR_TYPES.Q6_K:
    case KNOWN_TENSOR_TYPES.Q8_K:
    case KNOWN_TENSOR_TYPES.IQ2_XXS:
    case KNOWN_TENSOR_TYPES.IQ2_XS:
    case KNOWN_TENSOR_TYPES.IQ3_XXS:
    case KNOWN_TENSOR_TYPES.IQ1_S:
    case KNOWN_TENSOR_TYPES.IQ3_S:
    case KNOWN_TENSOR_TYPES.IQ2_S:
    case KNOWN_TENSOR_TYPES.IQ4_XS:
    case KNOWN_TENSOR_TYPES.IQ1_M:
    case KNOWN_TENSOR_TYPES.TQ1_0:
    case KNOWN_TENSOR_TYPES.TQ2_0:
      return 256;
    case KNOWN_TENSOR_TYPES.NVFP4:
      return 64;
    default:
      return null;
  }
}

function bytesPerBlock(tensorTypeRaw) {
  switch (tensorTypeRaw) {
    case KNOWN_TENSOR_TYPES.F32: return 128;
    case KNOWN_TENSOR_TYPES.F16:
    case KNOWN_TENSOR_TYPES.BF16:
    case KNOWN_TENSOR_TYPES.I16: return 64;
    case KNOWN_TENSOR_TYPES.I8: return 32;
    case KNOWN_TENSOR_TYPES.I32: return 128;
    case KNOWN_TENSOR_TYPES.I64:
    case KNOWN_TENSOR_TYPES.F64: return 256;
    case KNOWN_TENSOR_TYPES.Q4_0: return 18;
    case KNOWN_TENSOR_TYPES.Q4_1: return 20;
    case KNOWN_TENSOR_TYPES.Q1_0: return 18;
    case KNOWN_TENSOR_TYPES.I2_S: return 32;
    case KNOWN_TENSOR_TYPES.I8_S:
    case KNOWN_TENSOR_TYPES.TL1: return 1;
    case KNOWN_TENSOR_TYPES.Q5_0: return 22;
    case KNOWN_TENSOR_TYPES.Q5_1: return 24;
    case KNOWN_TENSOR_TYPES.Q8_0: return 34;
    case KNOWN_TENSOR_TYPES.Q8_1: return 36;
    case KNOWN_TENSOR_TYPES.Q2_K: return 84;
    case KNOWN_TENSOR_TYPES.Q3_K: return 110;
    case KNOWN_TENSOR_TYPES.Q4_K: return 144;
    case KNOWN_TENSOR_TYPES.Q5_K: return 176;
    case KNOWN_TENSOR_TYPES.Q6_K: return 210;
    case KNOWN_TENSOR_TYPES.Q8_K: return 292;
    case KNOWN_TENSOR_TYPES.IQ2_XXS: return 66;
    case KNOWN_TENSOR_TYPES.IQ2_XS: return 74;
    case KNOWN_TENSOR_TYPES.IQ3_XXS: return 98;
    case KNOWN_TENSOR_TYPES.IQ1_S: return 50;
    case KNOWN_TENSOR_TYPES.IQ4_NL: return 18;
    case KNOWN_TENSOR_TYPES.IQ3_S: return 110;
    case KNOWN_TENSOR_TYPES.IQ2_S: return 82;
    case KNOWN_TENSOR_TYPES.IQ4_XS: return 136;
    case KNOWN_TENSOR_TYPES.IQ1_M: return 56;
    case KNOWN_TENSOR_TYPES.TQ1_0: return 54;
    case KNOWN_TENSOR_TYPES.TQ2_0: return 66;
    case KNOWN_TENSOR_TYPES.MXFP4: return 17;
    case KNOWN_TENSOR_TYPES.NVFP4: return 36;
    default: return null;
  }
}

function elementCount(dimensions) {
  let total = 1;
  for (const dim of dimensions) {
    total *= dim;
  }
  return total;
}

function byteLenForTensor(tensorTypeRaw, dimensions) {
  const total = elementCount(dimensions);
  switch (tensorTypeRaw) {
    case KNOWN_TENSOR_TYPES.F32: return total * 4;
    case KNOWN_TENSOR_TYPES.F16:
    case KNOWN_TENSOR_TYPES.BF16:
    case KNOWN_TENSOR_TYPES.I16: return total * 2;
    case KNOWN_TENSOR_TYPES.I8: return total;
    case KNOWN_TENSOR_TYPES.I32: return total * 4;
    case KNOWN_TENSOR_TYPES.I64:
    case KNOWN_TENSOR_TYPES.F64: return total * 8;
    case KNOWN_TENSOR_TYPES.I2_S: return Math.ceil(total / 4) + 32;
    case KNOWN_TENSOR_TYPES.I8_S: return total;
    case KNOWN_TENSOR_TYPES.TL1: return 1;
    default: {
      const vpb = valuesPerBlock(tensorTypeRaw);
      const bpb = bytesPerBlock(tensorTypeRaw);
      if (vpb == null || bpb == null) return null;
      return Math.ceil(total / vpb) * bpb;
    }
  }
}

function appendSpecialTokensFromMetadata(addedTokens, metadata, tokens, tokenTypes = null) {
  const seen = new Set();
  const specialIdKeys = [
    'tokenizer.ggml.bos_token_id',
    'tokenizer.ggml.eos_token_id',
    'tokenizer.ggml.padding_token_id',
    'tokenizer.ggml.unknown_token_id',
  ];

  for (const key of specialIdKeys) {
    const tokenId = metadata[key];
    if (!Number.isInteger(tokenId) || tokenId < 0 || tokenId >= tokens.length) continue;
    if (seen.has(tokenId)) continue;
    addedTokens.push({ id: tokenId, content: tokens[tokenId], special: true });
    seen.add(tokenId);
  }

  if (!Array.isArray(tokenTypes)) return;
  for (let i = 0; i < tokens.length; i++) {
    const tokenType = tokenTypes[i];
    if (tokenType === 1 || tokenType === 6) continue;
    if (seen.has(i)) continue;
    addedTokens.push({ id: i, content: tokens[i], special: true });
    seen.add(i);
  }
}

export function deriveGgufTokenizerArtifacts(metadata) {
  const modelName = metadata['tokenizer.ggml.model'];
  const tokens = metadata['tokenizer.ggml.tokens'];
  if (typeof modelName !== 'string' || !Array.isArray(tokens)) return null;

  let tokenizerJson = null;
  const chatTemplate = typeof metadata['tokenizer.chat_template'] === 'string' &&
    metadata['tokenizer.chat_template'].trim().length > 0
    ? metadata['tokenizer.chat_template']
    : null;

  const merges = metadata['tokenizer.ggml.merges'];
  const scores = metadata['tokenizer.ggml.scores'];
  const tokenTypes = metadata['tokenizer.ggml.token_type'];

  if (Array.isArray(merges)) {
    const vocab = {};
    for (let i = 0; i < tokens.length; i++) {
      vocab[tokens[i]] = i;
    }
    const addedTokens = [];
    appendSpecialTokensFromMetadata(addedTokens, metadata, tokens, tokenTypes);
    tokenizerJson = JSON.stringify({
      model: {
        type: 'BPE',
        byte_fallback: false,
        vocab,
        merges,
      },
      pre_tokenizer: { type: 'ByteLevel' },
      added_tokens: addedTokens,
    });
  } else if (
    (modelName === 'llama' || modelName.startsWith('gemma')) &&
    Array.isArray(scores) &&
    Array.isArray(tokenTypes) &&
    scores.length === tokens.length &&
    tokenTypes.length === tokens.length
  ) {
    const unkId = Number.isInteger(metadata['tokenizer.ggml.unknown_token_id'])
      ? metadata['tokenizer.ggml.unknown_token_id']
      : 0;
    const prependScheme = metadata['tokenizer.ggml.add_space_prefix'] === false
      ? 'never'
      : 'always';
    const vocab = tokens.map((token, index) => [token, scores[index]]);
    const addedTokens = [];
    appendSpecialTokensFromMetadata(addedTokens, metadata, tokens, tokenTypes);
    tokenizerJson = JSON.stringify({
      model: {
        type: 'Unigram',
        unk_id: unkId,
        vocab,
      },
      pre_tokenizer: {
        type: 'Metaspace',
        replacement: '\u2581',
        prepend_scheme: prependScheme,
        split: true,
      },
      added_tokens: addedTokens,
    });
  }

  if (!tokenizerJson && !chatTemplate) return null;
  return {
    tokenizerModel: modelName,
    tokenizerJson,
    chatTemplate,
    metadata,
  };
}

export async function streamRegisterGguf({
  response,
  abi,
  wasm,
  handle,
  gpuResidency = null,
  signal,
  onProgress,
  onMetadata,
  maxTensorStagingBytes = DEFAULT_MAX_TENSOR_STAGING_BYTES,
}) {
  if (!response.ok) {
    throw new Error(`Failed to fetch model: ${response.status} ${response.statusText}`);
  }

  const totalBytes = parseInt(response.headers.get('content-length') || '0', 10);
  const textEncoder = new TextEncoder();
  const textDecoder = new TextDecoder();
  let loadedBytes = 0;

  const reportProgress = (currentWeight, weightsLoaded, weightsTotal) => {
    if (!onProgress) return;
    onProgress({
      loaded: loadedBytes,
      total: totalBytes,
      currentWeight,
      weightsLoaded,
      weightsTotal,
    });
  };

  const registerTensor = (entry, raw) => {
    const nameBytes = textEncoder.encode(entry.name);
    const namePtr = abi.alloc(nameBytes.length);
    const dataPtr = abi.alloc(raw.length);
    if (!namePtr || !dataPtr) {
      if (namePtr) abi.free(namePtr, nameBytes.length);
      if (dataPtr) abi.free(dataPtr, raw.length);
      throw new Error(`WASM allocation failed while registering ${entry.name}`);
    }

    abi.copyBytesIn(namePtr, nameBytes);
    abi.copyBytesIn(dataPtr, raw);
    const ok = wasm.register_weight_gguf(
      handle,
      namePtr, abi.size(nameBytes.length),
      dataPtr, abi.size(raw.length),
      entry.tensorTypeRaw,
      abi.size(entry.nElements),
    );
    abi.free(namePtr, nameBytes.length);
    abi.free(dataPtr, raw.length);
    return ok;
  };

  const registerTensorOwned = async (entry) => {
    const nameBytes = textEncoder.encode(entry.name);
    const namePtr = abi.alloc(nameBytes.length);
    if (!namePtr) {
      throw new Error(`WASM allocation failed while registering ${entry.name} metadata`);
    }

    const dataPtr = abi.alloc(entry.dataLen);
    if (!dataPtr) {
      abi.free(namePtr, nameBytes.length);
      throw new Error(
        `WASM allocation failed while reserving ${formatByteSize(entry.dataLen)} for ${entry.name}. ` +
        `The model likely exceeds current browser memory limits.`
      );
    }

    try {
      abi.copyBytesIn(namePtr, nameBytes);
      await readIntoWasm(dataPtr, entry.dataLen, entry.name);
      return wasm.register_weight_gguf_owned(
        handle,
        namePtr, abi.size(nameBytes.length),
        dataPtr, abi.size(entry.dataLen),
        entry.tensorTypeRaw,
        abi.size(entry.nElements),
      );
    } catch (err) {
      abi.free(dataPtr, entry.dataLen);
      throw err;
    } finally {
      abi.free(namePtr, nameBytes.length);
    }
  };

  const registerTensorGpuResident = async (entry) => {
    const nameBytes = textEncoder.encode(entry.name);
    const namePtr = abi.alloc(nameBytes.length);
    if (!namePtr) {
      throw new Error(`WASM allocation failed while registering ${entry.name} metadata`);
    }

    const gpuBufferId = gpuResidency.createBuffer(entry.dataLen);
    if (!gpuBufferId) {
      abi.free(namePtr, nameBytes.length);
      throw new Error(`GPU buffer allocation failed while reserving ${formatByteSize(entry.dataLen)} for ${entry.name}`);
    }

    try {
      abi.copyBytesIn(namePtr, nameBytes);
      await readIntoGpu(gpuBufferId, entry.dataLen, entry.name);
      return wasm.register_weight_gguf_gpu(
        handle,
        namePtr, abi.size(nameBytes.length),
        gpuBufferId,
        entry.tensorTypeRaw,
        abi.size(entry.nElements),
      );
    } catch (err) {
      gpuResidency.freeBuffer(gpuBufferId);
      throw err;
    } finally {
      abi.free(namePtr, nameBytes.length);
    }
  };

  const reader = response.body?.getReader?.();

  if (!reader) {
    throw new Error('GGUF streaming requires ReadableStream support');
  }

  let pending = new Uint8Array(0);
  let cursorPos = 0;

  const readChunk = async (currentWeight) => {
    if (signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    const { done, value } = await reader.read();
    if (done) throw new Error(`Unexpected EOF while reading ${currentWeight}`);
    loadedBytes += value.length;
    reportProgress(currentWeight, 0, 0);
    return value;
  };

  const readExact = async (byteLen, currentWeight) => {
    if (!currentWeight.startsWith('(') && byteLen > maxTensorStagingBytes) {
      throw new Error(
        `GGUF tensor ${currentWeight} requires ${formatByteSize(byteLen)} of contiguous staging. ` +
        `The current streamed loader still stages one full tensor at a time; chunked tensor registration is not implemented yet.`
      );
    }

    let remaining = byteLen;
    const parts = [];

    if (pending.length > 0) {
      const takeLen = Math.min(remaining, pending.length);
      parts.push(pending.subarray(0, takeLen));
      pending = pending.subarray(takeLen);
      remaining -= takeLen;
    }

    while (remaining > 0) {
      const chunk = await readChunk(currentWeight);
      if (chunk.length <= remaining) {
        parts.push(chunk);
        remaining -= chunk.length;
        continue;
      }
      parts.push(chunk.subarray(0, remaining));
      pending = chunk.subarray(remaining);
      remaining = 0;
    }

    const out = new Uint8Array(byteLen);
    let writeOffset = 0;
    for (const part of parts) {
      out.set(part, writeOffset);
      writeOffset += part.length;
    }
    cursorPos += byteLen;
    return out;
  };

  const skipExact = async (byteLen, currentWeight) => {
    let remaining = byteLen;

    if (pending.length > 0) {
      const takeLen = Math.min(remaining, pending.length);
      pending = pending.subarray(takeLen);
      remaining -= takeLen;
    }

    while (remaining > 0) {
      const chunk = await readChunk(currentWeight);
      if (chunk.length <= remaining) {
        remaining -= chunk.length;
        continue;
      }
      pending = chunk.subarray(remaining);
      remaining = 0;
    }

    cursorPos += byteLen;
  };

  const readIntoWasm = async (targetPtr, byteLen, currentWeight) => {
    if (byteLen > maxTensorStagingBytes) {
      // This path streams directly into WASM memory and does not require a JS-sized ArrayBuffer.
    }

    let remaining = byteLen;
    let written = 0;

    if (pending.length > 0) {
      const takeLen = Math.min(remaining, pending.length);
      abi.view(Uint8Array, addPtr(abi, targetPtr, written), takeLen).set(pending.subarray(0, takeLen));
      pending = pending.subarray(takeLen);
      remaining -= takeLen;
      written += takeLen;
    }

    while (remaining > 0) {
      const chunk = await readChunk(currentWeight);
      if (chunk.length <= remaining) {
        abi.view(Uint8Array, addPtr(abi, targetPtr, written), chunk.length).set(chunk);
        remaining -= chunk.length;
        written += chunk.length;
        continue;
      }

      abi.view(Uint8Array, addPtr(abi, targetPtr, written), remaining).set(chunk.subarray(0, remaining));
      pending = chunk.subarray(remaining);
      written += remaining;
      remaining = 0;
    }

    cursorPos += byteLen;
  };

  const readIntoGpu = async (gpuBufferId, byteLen, currentWeight) => {
    let remaining = byteLen;
    let written = 0;

    if (pending.length > 0) {
      const takeLen = Math.min(remaining, pending.length);
      gpuResidency.writeBufferAtOffset(gpuBufferId, written, pending.subarray(0, takeLen));
      pending = pending.subarray(takeLen);
      remaining -= takeLen;
      written += takeLen;
    }

    while (remaining > 0) {
      const chunk = await readChunk(currentWeight);
      if (chunk.length <= remaining) {
        gpuResidency.writeBufferAtOffset(gpuBufferId, written, chunk);
        remaining -= chunk.length;
        written += chunk.length;
        continue;
      }

      gpuResidency.writeBufferAtOffset(gpuBufferId, written, chunk.subarray(0, remaining));
      pending = chunk.subarray(remaining);
      written += remaining;
      remaining = 0;
    }

    cursorPos += byteLen;
  };

  const readU32 = async (currentWeight) => new DataView((await readExact(4, currentWeight)).buffer).getUint32(0, true);
  const readI32 = async (currentWeight) => new DataView((await readExact(4, currentWeight)).buffer).getInt32(0, true);
  const readU64 = async (currentWeight) => Number(new DataView((await readExact(8, currentWeight)).buffer).getBigUint64(0, true));
  const readI64 = async (currentWeight) => Number(new DataView((await readExact(8, currentWeight)).buffer).getBigInt64(0, true));
  const readF32 = async (currentWeight) => new DataView((await readExact(4, currentWeight)).buffer).getFloat32(0, true);
  const readF64 = async (currentWeight) => new DataView((await readExact(8, currentWeight)).buffer).getFloat64(0, true);
  const readBool = async (currentWeight) => (await readExact(1, currentWeight))[0] !== 0;

  const readString = async (currentWeight) => {
    const len = await readU64(currentWeight);
    const bytes = await readExact(len, currentWeight);
    return textDecoder.decode(bytes);
  };

  const readMetadataValue = async (valueType, currentWeight) => {
    switch (valueType) {
      case metadataValueTypes.u8:
        return (await readExact(1, currentWeight))[0];
      case metadataValueTypes.i8:
        return new DataView((await readExact(1, currentWeight)).buffer).getInt8(0);
      case metadataValueTypes.u16:
        return new DataView((await readExact(2, currentWeight)).buffer).getUint16(0, true);
      case metadataValueTypes.i16:
        return new DataView((await readExact(2, currentWeight)).buffer).getInt16(0, true);
      case metadataValueTypes.u32:
        return readU32(currentWeight);
      case metadataValueTypes.i32:
        return readI32(currentWeight);
      case metadataValueTypes.f32:
        return readF32(currentWeight);
      case metadataValueTypes.bool_:
        return readBool(currentWeight);
      case metadataValueTypes.string:
        return readString(currentWeight);
      case metadataValueTypes.u64:
        return readU64(currentWeight);
      case metadataValueTypes.i64:
        return readI64(currentWeight);
      case metadataValueTypes.f64:
        return readF64(currentWeight);
      case metadataValueTypes.array: {
        const elementType = await readU32(currentWeight);
        const count = await readU64(currentWeight);
        const items = new Array(count);
        for (let i = 0; i < count; i++) {
          items[i] = await readMetadataValue(elementType, currentWeight);
        }
        return items;
      }
      default:
        throw new Error(`Unsupported GGUF metadata value type: ${valueType}`);
    }
  };

  const skipMetadataValue = async (valueType, currentWeight) => {
    switch (valueType) {
      case metadataValueTypes.u8:
      case metadataValueTypes.i8:
      case metadataValueTypes.bool_:
        await skipExact(1, currentWeight);
        return;
      case metadataValueTypes.u16:
      case metadataValueTypes.i16:
        await skipExact(2, currentWeight);
        return;
      case metadataValueTypes.u32:
      case metadataValueTypes.i32:
      case metadataValueTypes.f32:
        await skipExact(4, currentWeight);
        return;
      case metadataValueTypes.u64:
      case metadataValueTypes.i64:
      case metadataValueTypes.f64:
        await skipExact(8, currentWeight);
        return;
      case metadataValueTypes.string:
        await readString(currentWeight);
        return;
      case metadataValueTypes.array: {
        const elementType = await readU32(currentWeight);
        const count = await readU64(currentWeight);
        for (let i = 0; i < count; i++) {
          await skipMetadataValue(elementType, currentWeight);
        }
        return;
      }
      default:
        throw new Error(`Unsupported GGUF metadata value type: ${valueType}`);
    }
  };

  const magic = textDecoder.decode(await readExact(4, '(header)'));
  if (magic !== GGUF_MAGIC) throw new Error('Invalid GGUF magic');

  const version = await readU32('(header)');
  if (version < 2 || version > 3) throw new Error(`Unsupported GGUF version: ${version}`);

  const tensorCount = await readU64('(header)');
  const metadataCount = await readU64('(header)');
  let alignment = DEFAULT_ALIGNMENT;
  const tokenizerMetadata = {};

  for (let i = 0; i < metadataCount; i++) {
    const key = await readString('(metadata)');
    const valueType = await readU32('(metadata)');
    if (key === 'general.alignment' && (valueType === metadataValueTypes.u32 || valueType === metadataValueTypes.u64)) {
      alignment = valueType === metadataValueTypes.u32
        ? await readU32('(metadata)')
        : await readU64('(metadata)');
      continue;
    }
    if (TOKENIZER_METADATA_KEYS.has(key)) {
      tokenizerMetadata[key] = await readMetadataValue(valueType, '(metadata)');
      continue;
    }
    await skipMetadataValue(valueType, '(metadata)');
  }

  if (onMetadata) {
    const artifacts = deriveGgufTokenizerArtifacts(tokenizerMetadata);
    if (artifacts) onMetadata(artifacts);
  }

  const tensorEntries = [];
  for (let i = 0; i < tensorCount; i++) {
    const name = await readString('(tensor-info)');
    const nDimensions = await readU32('(tensor-info)');
    const dimensions = [];
    for (let d = 0; d < nDimensions; d++) {
      dimensions.push(await readU64('(tensor-info)'));
    }
    const tensorTypeRaw = await readU32('(tensor-info)');
    const relOffset = await readU64('(tensor-info)');
    const dataLen = byteLenForTensor(tensorTypeRaw, dimensions);
    if (dataLen == null) {
      throw new Error(`Unsupported GGUF tensor type ${tensorTypeRaw} for ${name}`);
    }
    tensorEntries.push({
      name,
      tensorTypeRaw,
      dimensions,
      nElements: elementCount(dimensions),
      relOffset,
      dataLen,
    });
  }

  const dataRegionOffset = alignForward(cursorPos, alignment);
  let streamOffset = cursorPos;
  if (dataRegionOffset > streamOffset) {
    await skipExact(dataRegionOffset - streamOffset, '(align)');
    streamOffset = dataRegionOffset;
  }

  const entries = tensorEntries
    .map((entry) => ({
      ...entry,
      dataStart: dataRegionOffset + entry.relOffset,
      dataEnd: dataRegionOffset + entry.relOffset + entry.dataLen,
    }))
    .sort((a, b) => a.dataStart - b.dataStart);

  let registered = 0;
  for (const entry of entries) {
    if (signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    if (entry.dataStart < streamOffset) {
      throw new Error(`Out-of-order GGUF tensor: ${entry.name}`);
    }
    if (entry.dataStart > streamOffset) {
      await skipExact(entry.dataStart - streamOffset, entry.name);
      streamOffset = entry.dataStart;
    }
    const useGpuResidentPath =
      gpuResidency &&
      typeof wasm.register_weight_gguf_gpu === 'function' &&
      GPU_RESIDENT_QUANT_TYPES.has(entry.tensorTypeRaw);

    const ok = useGpuResidentPath
      ? await registerTensorGpuResident(entry)
      : await registerTensorOwned(entry);
    streamOffset = entry.dataEnd;
    if (!ok) {
      console.warn(`Failed to register GGUF tensor: ${entry.name}`);
      continue;
    }
    registered++;
    reportProgress(entry.name, registered, entries.length);
  }
}
