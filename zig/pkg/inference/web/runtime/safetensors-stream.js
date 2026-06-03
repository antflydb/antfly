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

export async function streamRegisterSafetensors({
  response,
  abi,
  wasm,
  handle,
  signal,
  onProgress,
  family,
}) {
  if (!response.ok) {
    throw new Error(`Failed to fetch model: ${response.status} ${response.statusText}`);
  }

  const totalBytes = parseInt(response.headers.get('content-length') || '0', 10);
  const dtypeMap = { F32: 0, F16: 1, BF16: 2 };
  const textEncoder = new TextEncoder();
  const textDecoder = new TextDecoder();
  const isGpt2 = family === 'gpt2';
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

  const buildTensorEntries = (header, dataOffset) => Object.entries(header)
    .filter(([name]) => name !== '__metadata__')
    .map(([name, meta]) => {
      const skip = name.endsWith('.position_ids') ||
        name.endsWith('.attn.bias') ||
        name.endsWith('.attn.masked_bias');
      const shape = meta.shape ?? [];
      const rows = shape.length >= 2 ? shape[0] : 1;
      const cols = shape.length >= 2 ? shape[1] : shape[0] || 1;

      let dtype = dtypeMap[meta.dtype] ?? 0;
      const needsTranspose = !skip &&
        isGpt2 &&
        shape.length === 2 &&
        name.startsWith('h.') &&
        name.endsWith('.weight') &&
        (name.includes('.attn.c_') || name.includes('.mlp.c_'));
      if (needsTranspose) {
        dtype = dtype === 1 ? 3 : 4;
      }

      return {
        name,
        skip,
        rows,
        cols,
        dtype,
        dataStart: dataOffset + meta.data_offsets[0],
        dataEnd: dataOffset + meta.data_offsets[1],
      };
    })
    .sort((a, b) => a.dataStart - b.dataStart);

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
    const ok = wasm.register_weight(
      handle,
      namePtr, abi.size(nameBytes.length),
      dataPtr, abi.size(raw.length),
      entry.rows, entry.cols, entry.dtype,
    );
    abi.free(namePtr, nameBytes.length);
    abi.free(dataPtr, raw.length);
    return ok;
  };

  const reader = response.body?.getReader?.();

  if (!reader) {
    const data = new Uint8Array(await response.arrayBuffer());
    loadedBytes = data.length;
    reportProgress('(downloaded)', 0, 0);

    const headerLen = Number(new DataView(data.buffer, data.byteOffset, 8).getBigUint64(0, true));
    const headerJson = textDecoder.decode(data.slice(8, 8 + headerLen));
    const header = JSON.parse(headerJson);
    const dataOffset = 8 + headerLen;
    const entries = buildTensorEntries(header, dataOffset);
    const loadableEntries = entries.filter((entry) => !entry.skip);
    let registered = 0;

    for (const entry of entries) {
      if (signal?.aborted) throw new DOMException('Aborted', 'AbortError');
      const raw = data.subarray(entry.dataStart, entry.dataEnd);
      if (entry.skip) continue;
      const ok = registerTensor(entry, raw);
      if (!ok) {
        console.warn(`Failed to register weight: ${entry.name}`);
        continue;
      }
      registered++;
      reportProgress(entry.name, registered, loadableEntries.length);
    }

    return;
  }

  let pending = new Uint8Array(0);

  const readChunk = async (currentWeight) => {
    if (signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    const { done, value } = await reader.read();
    if (done) throw new Error(`Unexpected EOF while reading ${currentWeight}`);
    loadedBytes += value.length;
    reportProgress(currentWeight, 0, 0);
    return value;
  };

  const readExact = async (byteLen, currentWeight) => {
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

    if (parts.length === 1 && parts[0].length === byteLen) {
      return new Uint8Array(parts[0]);
    }

    const out = new Uint8Array(byteLen);
    let writeOffset = 0;
    for (const part of parts) {
      out.set(part, writeOffset);
      writeOffset += part.length;
    }
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
  };

  const headerLenBytes = await readExact(8, '(header)');
  const headerLen = Number(new DataView(
    headerLenBytes.buffer,
    headerLenBytes.byteOffset,
    8,
  ).getBigUint64(0, true));
  const headerBytes = await readExact(headerLen, '(header)');
  const header = JSON.parse(textDecoder.decode(headerBytes));
  const dataOffset = 8 + headerLen;
  const entries = buildTensorEntries(header, dataOffset);
  const loadableEntries = entries.filter((entry) => !entry.skip);
  let streamOffset = dataOffset;
  let registered = 0;

  for (const entry of entries) {
    if (signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    if (entry.dataStart < streamOffset) {
      throw new Error(`Out-of-order SafeTensors entry: ${entry.name}`);
    }
    if (entry.dataStart > streamOffset) {
      await skipExact(entry.dataStart - streamOffset, entry.name);
      streamOffset = entry.dataStart;
    }

    const raw = await readExact(entry.dataEnd - entry.dataStart, entry.name);
    streamOffset = entry.dataEnd;

    if (entry.skip) continue;

    const ok = registerTensor(entry, raw);
    if (!ok) {
      console.warn(`Failed to register weight: ${entry.name}`);
      continue;
    }

    registered++;
    reportProgress(entry.name, registered, loadableEntries.length);
  }
}
