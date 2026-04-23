import { describe, expect, it } from "vitest";
import {
  defaultProduct,
  enabledProducts,
  getDefaultRoute,
  productForPath,
  showProductSwitcher,
} from "./products";

describe("products config", () => {
  it("enables both products by default", () => {
    expect(enabledProducts).toEqual(["antfly", "termite"]);
    expect(defaultProduct).toBe("antfly");
    expect(showProductSwitcher).toBe(true);
    expect(getDefaultRoute()).toBe("/");
  });

  it("maps antfly routes to the antfly product", () => {
    expect(productForPath("/")).toBe("antfly");
    expect(productForPath("/create")).toBe("antfly");
    expect(productForPath("/tables/example")).toBe("antfly");
    expect(productForPath("/playground/chat")).toBe("antfly");
    expect(productForPath("/playground/chunking")).toBe("antfly");
  });

  it("maps termite routes to the termite product", () => {
    expect(productForPath("/models")).toBe("termite");
    expect(productForPath("/playground/chunk")).toBe("termite");
    expect(productForPath("/playground/recognize")).toBe("termite");
    expect(productForPath("/playground/transcribe")).toBe("termite");
  });

  it("prefers the most specific route prefix", () => {
    expect(productForPath("/playground/chunk")).toBe("termite");
    expect(productForPath("/playground/chunking")).toBe("antfly");
  });

  it("returns undefined for unknown routes", () => {
    expect(productForPath("/does-not-exist")).toBeUndefined();
  });
});
