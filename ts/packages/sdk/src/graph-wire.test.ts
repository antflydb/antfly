import { describe, expect, it } from "vitest";
import { decodeGraphValue, encodeGraphParameter } from "./graph-wire.js";

describe("lossless graph wire parameters", () => {
  it("round trips integer boundaries and unreserved map keys", () => {
    const input = {
      type: "node",
      $type: "relationship",
      value: [-(1n << 63n), (1n << 63n) - 1n],
      __proto__: null,
    };
    expect(decodeGraphValue(encodeGraphParameter(input))).toEqual({
      type: "node",
      $type: "relationship",
      value: [-(1n << 63n), (1n << 63n) - 1n],
    });
  });
  it("rejects rounded integers and values JSON cannot represent", () => {
    expect(() => encodeGraphParameter(Number.MAX_SAFE_INTEGER + 1)).toThrow("bigint");
    expect(() => encodeGraphParameter(1n << 63n)).toThrow("64-bit");
    expect(() => encodeGraphParameter(NaN)).toThrow("finite");
    expect(() => encodeGraphParameter(new Array(1))).toThrow("plain data");
  });
});
