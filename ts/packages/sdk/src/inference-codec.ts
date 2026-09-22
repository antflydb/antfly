/**
 * Codec for the negotiated numeric response frame.
 *
 * The inference server returns this instead of JSON when a request's `Accept`
 * header names {@link NUMERIC_RESPONSE_MEDIA_TYPE}, which keeps every float out
 * of JSON text.
 *
 * Layout (little-endian): the magic `AFN1`, a uint32 kind, uint64 rows, uint64
 * columns, then `rows * columns` float32 values. Kind 1 is dense embeddings and
 * kind 2 reranking scores.
 */

/** The media type of the packed frame. */
export const NUMERIC_RESPONSE_MEDIA_TYPE = "application/vnd.antfly.numeric.v1";

/** Takes the frame, but settles for the JSON body an older server returns. */
export const NUMERIC_RESPONSE_ACCEPT = `${NUMERIC_RESPONSE_MEDIA_TYPE}, application/json`;

const NUMERIC_FRAME_HEADER_BYTES = 24;
const NUMERIC_FRAME_MAGIC = "AFN1";
const NUMERIC_FRAME_KIND_DENSE = 1;
const MAX_DECODED_EMBEDDING_BYTES = 256n << 20n;
const APPROXIMATE_VECTOR_OVERHEAD_BYTES = 64n;
const JAVASCRIPT_NUMBER_BYTES = 8n;

/**
 * Serialize embedding vectors as one numeric frame. Mirrors what the server
 * writes, which is what makes it useful for tests and fixtures.
 *
 * @param embeddings - 2D array of float32 embeddings
 * @returns ArrayBuffer containing the frame
 */
export function serializeNumericDenseFrame(embeddings: number[][]): ArrayBuffer {
  const dimension = embeddings.length === 0 ? 0 : (embeddings[0]?.length ?? 0);
  if (embeddings.length > 0 && dimension === 0) {
    throw new Error("Cannot serialize a non-empty embedding matrix with zero dimension");
  }
  // Size arithmetic comes before walking the rows, so a forged length cannot
  // send the loop off the end of the matrix.
  const valueCount = embeddings.length * dimension;
  const totalSize = NUMERIC_FRAME_HEADER_BYTES + valueCount * 4;
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
  for (const [index, character] of [...NUMERIC_FRAME_MAGIC].entries()) {
    view.setUint8(index, character.charCodeAt(0));
  }
  view.setUint32(4, NUMERIC_FRAME_KIND_DENSE, true);
  view.setBigUint64(8, BigInt(embeddings.length), true);
  view.setBigUint64(16, BigInt(dimension), true);

  let offset = NUMERIC_FRAME_HEADER_BYTES;
  for (const vector of embeddings) {
    for (const value of vector) {
      view.setFloat32(offset, value, true);
      offset += 4;
    }
  }
  return buffer;
}

/**
 * Read the dense vectors out of one numeric frame. The declared shape is
 * checked against the body before anything is allocated, so a forged header
 * cannot make the client reserve memory it never received.
 *
 * @param frame - the response body, as bytes or a buffer
 * @returns 2D array of embeddings
 */
export function decodeNumericDenseFrame(frame: ArrayBuffer | Uint8Array): number[][] {
  const bytes = frame instanceof Uint8Array ? frame : new Uint8Array(frame);
  if (bytes.byteLength < NUMERIC_FRAME_HEADER_BYTES) {
    throw new Error("Invalid numeric embedding response: truncated frame header");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);

  const magic = String.fromCharCode(...[0, 1, 2, 3].map((index) => view.getUint8(index)));
  if (magic !== NUMERIC_FRAME_MAGIC) {
    throw new Error(
      `Invalid numeric embedding response: not a ${NUMERIC_RESPONSE_MEDIA_TYPE} frame`
    );
  }
  const kind = view.getUint32(4, true);
  if (kind !== NUMERIC_FRAME_KIND_DENSE) {
    throw new Error(`Invalid numeric embedding response: kind ${kind} is not dense embeddings`);
  }

  const rows = view.getBigUint64(8, true);
  const columns = view.getBigUint64(16, true);
  if (rows > 0n && columns === 0n) {
    throw new Error("Invalid numeric embedding response: non-empty response has zero dimension");
  }
  const expectedBytes = BigInt(NUMERIC_FRAME_HEADER_BYTES) + rows * columns * 4n;
  if (expectedBytes !== BigInt(bytes.byteLength)) {
    throw new Error(
      `Invalid numeric embedding response: header declares ${expectedBytes} bytes, received ${bytes.byteLength}`
    );
  }

  const decodedBytes =
    rows * APPROXIMATE_VECTOR_OVERHEAD_BYTES + rows * columns * JAVASCRIPT_NUMBER_BYTES;
  if (decodedBytes > MAX_DECODED_EMBEDDING_BYTES) {
    throw new Error(
      `Numeric embedding response exceeds decoded size limit of ${MAX_DECODED_EMBEDDING_BYTES} bytes`
    );
  }

  const rowCount = Number(rows);
  const columnCount = Number(columns);
  const embeddings = new Array<number[]>(rowCount);
  let offset = NUMERIC_FRAME_HEADER_BYTES;
  for (let i = 0; i < rowCount; i++) {
    const vector = new Array<number>(columnCount);
    for (let j = 0; j < columnCount; j++) {
      vector[j] = view.getFloat32(offset, true);
      offset += 4;
    }
    embeddings[i] = vector;
  }
  return embeddings;
}
