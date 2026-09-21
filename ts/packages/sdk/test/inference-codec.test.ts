import { describe, expect, it } from "vitest";
import {
  decodeNumericDenseFrame,
  NUMERIC_RESPONSE_MEDIA_TYPE,
  serializeNumericDenseFrame,
} from "../src/inference-codec.js";

const HEADER_BYTES = 24;

/** Build a frame header directly, so a test can forge what a server sent. */
function frameHeader(kind: number, rows: bigint, columns: bigint, payloadBytes = 0): ArrayBuffer {
  const buffer = new ArrayBuffer(HEADER_BYTES + payloadBytes);
  const view = new DataView(buffer);
  for (const [index, character] of [..."AFN1"].entries()) {
    view.setUint8(index, character.charCodeAt(0));
  }
  view.setUint32(4, kind, true);
  view.setBigUint64(8, rows, true);
  view.setBigUint64(16, columns, true);
  return buffer;
}

describe("Numeric response codec", () => {
  describe("serializeNumericDenseFrame", () => {
    it("writes the header a dense frame declares", () => {
      const buffer = serializeNumericDenseFrame([[1.0, 2.0, 3.0]]);
      const view = new DataView(buffer);

      expect(buffer.byteLength).toBe(HEADER_BYTES + 3 * 4);
      expect(String.fromCharCode(...new Uint8Array(buffer, 0, 4))).toBe("AFN1");
      expect(view.getUint32(4, true)).toBe(1);
      expect(Number(view.getBigUint64(8, true))).toBe(1);
      expect(Number(view.getBigUint64(16, true))).toBe(3);
      expect(view.getFloat32(HEADER_BYTES, true)).toBeCloseTo(1.0);
      expect(view.getFloat32(HEADER_BYTES + 4, true)).toBeCloseTo(2.0);
      expect(view.getFloat32(HEADER_BYTES + 8, true)).toBeCloseTo(3.0);
    });

    it("writes every vector in request order", () => {
      const buffer = serializeNumericDenseFrame([
        [1.0, 2.0],
        [3.0, 4.0],
        [-5.0, 0.00001],
      ]);
      const view = new DataView(buffer);

      expect(buffer.byteLength).toBe(HEADER_BYTES + 6 * 4);
      expect(Number(view.getBigUint64(8, true))).toBe(3);
      expect(view.getFloat32(HEADER_BYTES, true)).toBeCloseTo(1.0);
      expect(view.getFloat32(HEADER_BYTES + 12, true)).toBeCloseTo(4.0);
      expect(view.getFloat32(HEADER_BYTES + 16, true)).toBeCloseTo(-5.0);
      expect(view.getFloat32(HEADER_BYTES + 20, true)).toBeCloseTo(0.00001, 6);
    });

    it("rejects zero-width and ragged matrices before allocation", () => {
      expect(() => serializeNumericDenseFrame([[]])).toThrow("zero dimension");
      expect(() => serializeNumericDenseFrame([[1, 2], [3]])).toThrow(
        "vector 1 has dimension 1, expected 2"
      );
    });

    it("rejects unsafe buffer-size arithmetic before allocation", () => {
      const forged = {
        length: Number.MAX_SAFE_INTEGER,
        0: [1, 2],
      } as unknown as number[][];
      expect(() => serializeNumericDenseFrame(forged)).toThrow("too large to serialize safely");
    });
  });

  describe("decodeNumericDenseFrame", () => {
    it("reads the vectors back out of a frame", () => {
      const frame = frameHeader(1, 3n, 2n, 6 * 4);
      const view = new DataView(frame);
      [1.0, 2.0, 3.0, 4.0, 5.0, 6.0].forEach((value, index) => {
        view.setFloat32(HEADER_BYTES + index * 4, value, true);
      });

      expect(decodeNumericDenseFrame(frame)).toEqual([
        [1.0, 2.0],
        [3.0, 4.0],
        [5.0, 6.0],
      ]);
    });

    it("reads a frame that is a view into a larger buffer", () => {
      // Response bodies commonly arrive as a slice of a pooled buffer.
      const frame = serializeNumericDenseFrame([[0.5, -0.5]]);
      const padded = new Uint8Array(frame.byteLength + 8);
      padded.set(new Uint8Array(frame), 8);

      expect(decodeNumericDenseFrame(padded.subarray(8))).toEqual([[0.5, -0.5]]);
    });

    it("rejects forged headers before allocating vectors", () => {
      expect(() => decodeNumericDenseFrame(new ArrayBuffer(HEADER_BYTES - 1))).toThrow(
        "truncated frame header"
      );

      const wrongMagic = frameHeader(1, 0n, 0n);
      new DataView(wrongMagic).setUint8(3, "2".charCodeAt(0));
      expect(() => decodeNumericDenseFrame(wrongMagic)).toThrow(NUMERIC_RESPONSE_MEDIA_TYPE);

      // Kind 2 is reranking scores, which is not what this decoder returns.
      expect(() => decodeNumericDenseFrame(frameHeader(2, 1n, 1n, 4))).toThrow(
        "kind 2 is not dense embeddings"
      );

      expect(() => decodeNumericDenseFrame(frameHeader(1, 0xffffffffffffffffn, 0n))).toThrow(
        "non-empty response has zero dimension"
      );

      expect(() => decodeNumericDenseFrame(frameHeader(1, 1n, 1n))).toThrow(
        `header declares ${HEADER_BYTES + 4} bytes, received ${HEADER_BYTES}`
      );
    });

    it("rejects payloads whose nested arrays would exceed the decoded memory budget", () => {
      const vectorCount = 4_000_000;
      expect(() =>
        decodeNumericDenseFrame(frameHeader(1, BigInt(vectorCount), 1n, vectorCount * 4))
      ).toThrow("exceeds decoded size limit");
    });
  });

  describe("roundtrip", () => {
    it("preserves empty, single and multiple vectors", () => {
      for (const original of [
        [],
        [[0.1, 0.2, 0.3, 0.4, 0.5]],
        [
          [0.1, -0.2],
          [-0.4, 0.5],
        ],
      ]) {
        const decoded = decodeNumericDenseFrame(serializeNumericDenseFrame(original));
        expect(decoded.length).toBe(original.length);
        original.forEach((vector, i) => {
          vector.forEach((value, j) => {
            expect(decoded[i][j]).toBeCloseTo(value);
          });
        });
      }
    });

    it("preserves high-dimensional embeddings (384 dimensions)", () => {
      const dimension = 384;
      const original = Array.from({ length: 3 }, () =>
        Array.from({ length: dimension }, () => (Math.random() - 0.5) * 2)
      );

      const serialized = serializeNumericDenseFrame(original);
      expect(serialized.byteLength).toBe(HEADER_BYTES + 3 * dimension * 4);

      const decoded = decodeNumericDenseFrame(serialized);
      expect(decoded.length).toBe(3);
      original.forEach((vector, i) => {
        expect(decoded[i].length).toBe(dimension);
        // float32 has ~7 significant digits of precision.
        vector.forEach((value, j) => {
          expect(decoded[i][j]).toBeCloseTo(value, 5);
        });
      });
    });
  });

  describe("wire order", () => {
    it("writes the header and floats little-endian", () => {
      const bytes = new Uint8Array(serializeNumericDenseFrame([[1.0]]));

      expect(bytes[4]).toBe(1); // kind
      expect(bytes[8]).toBe(1); // rows
      expect(bytes[15]).toBe(0);
      expect(bytes[16]).toBe(1); // columns
      expect(bytes[23]).toBe(0);
      // 1.0 as float32 little-endian is [0x00, 0x00, 0x80, 0x3F].
      expect([...bytes.slice(HEADER_BYTES, HEADER_BYTES + 4)]).toEqual([0x00, 0x00, 0x80, 0x3f]);
    });
  });
});
