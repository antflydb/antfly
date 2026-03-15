import { describe, expect, it } from "vitest";
import { deserializeEmbeddings, serializeEmbeddings } from "../src/codec.js";

describe("Binary Embedding Codec", () => {
  describe("serializeEmbeddings", () => {
    it("should serialize empty embeddings array", () => {
      const buffer = serializeEmbeddings([]);
      const view = new DataView(buffer);

      // Should only contain 8 bytes for numVectors = 0
      expect(buffer.byteLength).toBe(8);
      expect(Number(view.getBigUint64(0, true))).toBe(0);
    });

    it("should serialize single embedding vector", () => {
      const embeddings = [[1.0, 2.0, 3.0]];
      const buffer = serializeEmbeddings(embeddings);
      const view = new DataView(buffer);

      // Header: 8 (numVectors) + 8 (dimension) + 3 * 4 (floats) = 28 bytes
      expect(buffer.byteLength).toBe(28);
      expect(Number(view.getBigUint64(0, true))).toBe(1); // numVectors
      expect(Number(view.getBigUint64(8, true))).toBe(3); // dimension
      expect(view.getFloat32(16, true)).toBeCloseTo(1.0);
      expect(view.getFloat32(20, true)).toBeCloseTo(2.0);
      expect(view.getFloat32(24, true)).toBeCloseTo(3.0);
    });

    it("should serialize multiple embedding vectors", () => {
      const embeddings = [
        [1.0, 2.0],
        [3.0, 4.0],
        [5.0, 6.0],
      ];
      const buffer = serializeEmbeddings(embeddings);
      const view = new DataView(buffer);

      // Header: 8 + 8 + 6 * 4 = 40 bytes
      expect(buffer.byteLength).toBe(40);
      expect(Number(view.getBigUint64(0, true))).toBe(3); // numVectors
      expect(Number(view.getBigUint64(8, true))).toBe(2); // dimension

      // First vector
      expect(view.getFloat32(16, true)).toBeCloseTo(1.0);
      expect(view.getFloat32(20, true)).toBeCloseTo(2.0);

      // Second vector
      expect(view.getFloat32(24, true)).toBeCloseTo(3.0);
      expect(view.getFloat32(28, true)).toBeCloseTo(4.0);

      // Third vector
      expect(view.getFloat32(32, true)).toBeCloseTo(5.0);
      expect(view.getFloat32(36, true)).toBeCloseTo(6.0);
    });

    it("should handle negative values", () => {
      const embeddings = [[-1.5, 0.0, 2.5]];
      const buffer = serializeEmbeddings(embeddings);
      const view = new DataView(buffer);

      expect(view.getFloat32(16, true)).toBeCloseTo(-1.5);
      expect(view.getFloat32(20, true)).toBeCloseTo(0.0);
      expect(view.getFloat32(24, true)).toBeCloseTo(2.5);
    });

    it("should handle very small values", () => {
      const embeddings = [[0.00001, -0.00002, 0.00003]];
      const buffer = serializeEmbeddings(embeddings);
      const view = new DataView(buffer);

      expect(view.getFloat32(16, true)).toBeCloseTo(0.00001, 6);
      expect(view.getFloat32(20, true)).toBeCloseTo(-0.00002, 6);
      expect(view.getFloat32(24, true)).toBeCloseTo(0.00003, 6);
    });
  });

  describe("deserializeEmbeddings", () => {
    it("should deserialize empty embeddings", () => {
      const buffer = new ArrayBuffer(8);
      const view = new DataView(buffer);
      view.setBigUint64(0, BigInt(0), true);

      const result = deserializeEmbeddings(buffer);
      expect(result).toEqual([]);
    });

    it("should deserialize single embedding vector", () => {
      const buffer = new ArrayBuffer(28);
      const view = new DataView(buffer);
      view.setBigUint64(0, BigInt(1), true); // numVectors
      view.setBigUint64(8, BigInt(3), true); // dimension
      view.setFloat32(16, 1.0, true);
      view.setFloat32(20, 2.0, true);
      view.setFloat32(24, 3.0, true);

      const result = deserializeEmbeddings(buffer);
      expect(result.length).toBe(1);
      expect(result[0].length).toBe(3);
      expect(result[0][0]).toBeCloseTo(1.0);
      expect(result[0][1]).toBeCloseTo(2.0);
      expect(result[0][2]).toBeCloseTo(3.0);
    });

    it("should deserialize multiple embedding vectors", () => {
      const buffer = new ArrayBuffer(40);
      const view = new DataView(buffer);
      view.setBigUint64(0, BigInt(3), true); // numVectors
      view.setBigUint64(8, BigInt(2), true); // dimension
      view.setFloat32(16, 1.0, true);
      view.setFloat32(20, 2.0, true);
      view.setFloat32(24, 3.0, true);
      view.setFloat32(28, 4.0, true);
      view.setFloat32(32, 5.0, true);
      view.setFloat32(36, 6.0, true);

      const result = deserializeEmbeddings(buffer);
      expect(result.length).toBe(3);
      expect(result[0]).toEqual([1.0, 2.0]);
      expect(result[1]).toEqual([3.0, 4.0]);
      expect(result[2]).toEqual([5.0, 6.0]);
    });
  });

  describe("roundtrip serialization", () => {
    it("should roundtrip empty embeddings", () => {
      const original: number[][] = [];
      const serialized = serializeEmbeddings(original);
      const deserialized = deserializeEmbeddings(serialized);
      expect(deserialized).toEqual(original);
    });

    it("should roundtrip single vector", () => {
      const original = [[0.1, 0.2, 0.3, 0.4, 0.5]];
      const serialized = serializeEmbeddings(original);
      const deserialized = deserializeEmbeddings(serialized);

      expect(deserialized.length).toBe(original.length);
      for (let i = 0; i < original.length; i++) {
        for (let j = 0; j < original[i].length; j++) {
          expect(deserialized[i][j]).toBeCloseTo(original[i][j]);
        }
      }
    });

    it("should roundtrip multiple vectors", () => {
      const original = [
        [0.1, -0.2, 0.3],
        [-0.4, 0.5, -0.6],
        [0.7, -0.8, 0.9],
      ];
      const serialized = serializeEmbeddings(original);
      const deserialized = deserializeEmbeddings(serialized);

      expect(deserialized.length).toBe(original.length);
      for (let i = 0; i < original.length; i++) {
        for (let j = 0; j < original[i].length; j++) {
          expect(deserialized[i][j]).toBeCloseTo(original[i][j]);
        }
      }
    });

    it("should roundtrip high-dimensional embeddings (384 dimensions)", () => {
      // Simulate BGE-small embedding dimension
      const dimension = 384;
      const numVectors = 3;
      const original: number[][] = [];

      for (let i = 0; i < numVectors; i++) {
        const vector: number[] = [];
        for (let j = 0; j < dimension; j++) {
          // Generate varied values including negatives
          vector.push((Math.random() - 0.5) * 2);
        }
        original.push(vector);
      }

      const serialized = serializeEmbeddings(original);
      const deserialized = deserializeEmbeddings(serialized);

      // Verify buffer size: 8 + 8 + (3 * 384 * 4) = 4624 bytes
      expect(serialized.byteLength).toBe(8 + 8 + numVectors * dimension * 4);

      expect(deserialized.length).toBe(numVectors);
      for (let i = 0; i < numVectors; i++) {
        expect(deserialized[i].length).toBe(dimension);
        for (let j = 0; j < dimension; j++) {
          // float32 has ~7 significant digits of precision
          expect(deserialized[i][j]).toBeCloseTo(original[i][j], 5);
        }
      }
    });
  });

  describe("binary format compatibility", () => {
    it("should use little-endian byte order for header", () => {
      const embeddings = [[1.0]];
      const buffer = serializeEmbeddings(embeddings);
      const bytes = new Uint8Array(buffer);

      // numVectors = 1 in little-endian: [1, 0, 0, 0, 0, 0, 0, 0]
      expect(bytes[0]).toBe(1);
      expect(bytes[1]).toBe(0);
      expect(bytes[7]).toBe(0);

      // dimension = 1 in little-endian: [1, 0, 0, 0, 0, 0, 0, 0]
      expect(bytes[8]).toBe(1);
      expect(bytes[9]).toBe(0);
      expect(bytes[15]).toBe(0);
    });

    it("should use little-endian byte order for floats", () => {
      // 1.0 as float32 little-endian is: [0x00, 0x00, 0x80, 0x3F]
      const embeddings = [[1.0]];
      const buffer = serializeEmbeddings(embeddings);
      const bytes = new Uint8Array(buffer);

      expect(bytes[16]).toBe(0x00);
      expect(bytes[17]).toBe(0x00);
      expect(bytes[18]).toBe(0x80);
      expect(bytes[19]).toBe(0x3f);
    });
  });
});
