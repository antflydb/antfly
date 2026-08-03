/**
 * Binary codec for Inference embedding serialization
 *
 * Format (little-endian):
 * - 8 bytes: uint64 - number of vectors
 * - 8 bytes: uint64 - dimension of each vector (only if numVectors > 0)
 * - Rest: float32 values (4 bytes each)
 */

const MAX_DECODED_EMBEDDING_BYTES = 256n << 20n;
const APPROXIMATE_VECTOR_OVERHEAD_BYTES = 64n;
const JAVASCRIPT_NUMBER_BYTES = 8n;

/**
 * Serialize embedding vectors to binary format
 * @param embeddings - 2D array of float32 embeddings
 * @returns ArrayBuffer containing serialized embeddings
 */
export function serializeEmbeddings(embeddings: number[][]): ArrayBuffer {
  if (embeddings.length === 0) {
    // Just the count (0)
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    view.setBigUint64(0, BigInt(0), true); // little-endian
    return buffer;
  }

  const numVectors = embeddings.length;
  const dimension = embeddings[0]?.length ?? 0;
  if (dimension === 0) {
    throw new Error("Cannot serialize a non-empty embedding matrix with zero dimension");
  }

  // 8 bytes for numVectors + 8 bytes for dimension + 4 bytes per float
  const valueCount = numVectors * dimension;
  const totalSize = 16 + valueCount * 4;
  if (!Number.isSafeInteger(valueCount) || !Number.isSafeInteger(totalSize)) {
    throw new Error("Embedding matrix is too large to serialize safely");
  }
  for (const [index, vector] of embeddings.entries()) {
    if (vector.length !== dimension) {
      throw new Error(
        `Cannot serialize ragged embeddings: vector ${index} has dimension ${vector.length}, expected ${dimension}`
      );
    }
  }
  const buffer = new ArrayBuffer(totalSize);
  const view = new DataView(buffer);

  // Write header
  view.setBigUint64(0, BigInt(numVectors), true); // little-endian
  view.setBigUint64(8, BigInt(dimension), true); // little-endian

  // Write float32 values
  let offset = 16;
  for (const vector of embeddings) {
    for (const val of vector) {
      view.setFloat32(offset, val, true); // little-endian
      offset += 4;
    }
  }

  return buffer;
}

/**
 * Deserialize binary embedding data to 2D array
 * @param buffer - ArrayBuffer containing serialized embeddings
 * @returns 2D array of float32 embeddings
 */
export function deserializeEmbeddings(buffer: ArrayBuffer): number[][] {
  if (buffer.byteLength < 8) {
    throw new Error("Invalid binary embedding response: missing vector count");
  }
  const view = new DataView(buffer);

  // Read header
  const numVectorsRaw = view.getBigUint64(0, true); // little-endian
  if (numVectorsRaw === 0n) {
    if (buffer.byteLength !== 8) {
      throw new Error("Invalid binary embedding response: unexpected data after empty header");
    }
    return [];
  }

  if (buffer.byteLength < 16) {
    throw new Error("Invalid binary embedding response: missing vector dimension");
  }

  const dimensionRaw = view.getBigUint64(8, true); // little-endian
  if (dimensionRaw === 0n) {
    throw new Error("Invalid binary embedding response: non-empty response has zero dimension");
  }
  const expectedBytes = 16n + numVectorsRaw * dimensionRaw * 4n;
  if (expectedBytes !== BigInt(buffer.byteLength)) {
    throw new Error(
      `Invalid binary embedding response: header declares ${expectedBytes} bytes, received ${buffer.byteLength}`
    );
  }

  const decodedBytes =
    numVectorsRaw * APPROXIMATE_VECTOR_OVERHEAD_BYTES +
    numVectorsRaw * dimensionRaw * JAVASCRIPT_NUMBER_BYTES;
  if (decodedBytes > MAX_DECODED_EMBEDDING_BYTES) {
    throw new Error(
      `Binary embedding response exceeds decoded size limit of ${MAX_DECODED_EMBEDDING_BYTES} bytes`
    );
  }

  const numVectors = Number(numVectorsRaw);
  const dimension = Number(dimensionRaw);

  // Read float32 values
  const embeddings = new Array<number[]>(numVectors);
  let offset = 16;
  for (let i = 0; i < numVectors; i++) {
    const vector = new Array<number>(dimension);
    for (let j = 0; j < dimension; j++) {
      vector[j] = view.getFloat32(offset, true); // little-endian
      offset += 4;
    }
    embeddings[i] = vector;
  }

  return embeddings;
}
