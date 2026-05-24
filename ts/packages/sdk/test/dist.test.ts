import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const cjsPath = resolve(__dirname, "../dist/index.cjs");
const hasBuild = existsSync(cjsPath);

describe.skipIf(!hasBuild)("CJS dist bundle", () => {
  it('does not emit require("openapi-fetch")', () => {
    const src = readFileSync(cjsPath, "utf8");
    expect(src).not.toMatch(/require\("openapi-fetch"\)/);
    expect(src).not.toMatch(/import_openapi_fetch/);
  });

  it("instantiates CJS clients without throwing", () => {
    const req = createRequire(import.meta.url);
    const { AntflyClient, TermiteClient } = req(cjsPath);

    expect(() => new AntflyClient({ baseUrl: "http://localhost" })).not.toThrow();
    expect(() => new TermiteClient({ baseUrl: "http://localhost" })).not.toThrow();
  });
});

describe.skipIf(hasBuild)("CJS dist bundle - skipped", () => {
  it("skipped: run `pnpm build` first to validate the CJS bundle", () => {
    expect(hasBuild).toBe(false);
  });
});
