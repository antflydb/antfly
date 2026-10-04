import { describe, expect, it } from "vitest";
import {
  emptyModelProfile,
  modelProfileSchema,
  profileAxes,
  readRuntimeProfile,
} from "./runtime-profile";

describe("runtime profiles", () => {
  it("does not treat missing measurements as zero scores", () => {
    expect(
      profileAxes({ name: "", chip: "" }, emptyModelProfile).every(
        (axis) => axis.score === undefined
      )
    ).toBe(true);
    const axes = profileAxes(
      { name: "", chip: "", memoryGB: 16 },
      { ...emptyModelProfile, qualityPercent: 0, peakMemoryGB: 8 }
    );
    expect(axes[0].score).toBe(0);
    expect(axes[3].score).toBe(0.5);
    expect(axes[1].score).toBeUndefined();
  });

  it("caps chart values without changing displayed measurements", () => {
    const axes = profileAxes(
      { name: "", chip: "", memoryGB: 16 },
      { ...emptyModelProfile, decodeTokensPerSecond: 150, peakMemoryGB: 20 }
    );
    expect(axes[1].score).toBe(1);
    expect(axes[1].value).toBe("150 tok/s");
    expect(axes[3].score).toBe(0);
  });

  it("requires configured RAM to compute headroom", () => {
    expect(
      profileAxes({ name: "", chip: "" }, { ...emptyModelProfile, peakMemoryGB: 8 })[3].score
    ).toBeUndefined();
  });

  it("rejects invalid measurements and recovers from corrupt browser storage", () => {
    for (const value of [-1, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(
        modelProfileSchema.safeParse({ ...emptyModelProfile, decodeTokensPerSecond: value }).success
      ).toBe(false);
    }
    expect(
      modelProfileSchema.safeParse({ ...emptyModelProfile, qualityPercent: 101 }).success
    ).toBe(false);
    for (const raw of [
      null,
      "bad json",
      '{"version":2}',
      '{"version":1,"device":{},"models":{}}',
    ]) {
      expect(readRuntimeProfile(raw).models).toEqual({});
    }
  });
});
