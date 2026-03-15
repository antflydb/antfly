/**
 * Binary codec for Termite embedding serialization
 *
 * Format (little-endian):
 * - 8 bytes: uint64 - number of vectors
 * - 8 bytes: uint64 - dimension of each vector (only if numVectors > 0)
 * - Rest: float32 values (4 bytes each)
 */

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

  // 8 bytes for numVectors + 8 bytes for dimension + 4 bytes per float
  const totalSize = 8 + 8 + numVectors * dimension * 4;
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
  const view = new DataView(buffer);

  // Read header
  const numVectors = Number(view.getBigUint64(0, true)); // little-endian
  if (numVectors === 0) {
    return [];
  }

  const dimension = Number(view.getBigUint64(8, true)); // little-endian

  // Read float32 values
  const embeddings: number[][] = [];
  let offset = 16;
  for (let i = 0; i < numVectors; i++) {
    const vector: number[] = [];
    for (let j = 0; j < dimension; j++) {
      vector.push(view.getFloat32(offset, true)); // little-endian
      offset += 4;
    }
    embeddings.push(vector);
  }

  return embeddings;
}
