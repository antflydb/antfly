/**
 * Unit tests for ResultsToolbar formatQueryTime function
 */
import { describe, expect, it } from "vitest";
import { formatQueryTime } from "./ResultsToolbar";

describe("formatQueryTime", () => {
  it("should return '< 1ms' for sub-millisecond values", () => {
    expect(formatQueryTime(500_000)).toBe("< 1ms"); // 0.5ms
    expect(formatQueryTime(100_000)).toBe("< 1ms"); // 0.1ms
    expect(formatQueryTime(0)).toBe("< 1ms");
  });

  it("should format milliseconds correctly", () => {
    expect(formatQueryTime(1_000_000)).toBe("1ms");
    expect(formatQueryTime(17_613_349)).toBe("18ms"); // The bug case - 17.6ms rounded
    expect(formatQueryTime(500_000_000)).toBe("500ms");
    expect(formatQueryTime(999_000_000)).toBe("999ms");
  });

  it("should format seconds correctly for values >= 1000ms", () => {
    expect(formatQueryTime(1_000_000_000)).toBe("1.0s"); // 1 second
    expect(formatQueryTime(1_500_000_000)).toBe("1.5s"); // 1.5 seconds
    expect(formatQueryTime(10_000_000_000)).toBe("10.0s"); // 10 seconds
  });

  it("should handle the original bug case (17613349 nanoseconds)", () => {
    // The original bug: 17613349 was displayed as "17613349ms" instead of "18ms"
    const result = formatQueryTime(17_613_349);
    expect(result).toBe("18ms");
    expect(result).not.toBe("17613349ms");
  });
});
