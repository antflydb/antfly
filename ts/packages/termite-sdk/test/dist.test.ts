import { createRequire } from "node:module"
import { existsSync, readFileSync } from "node:fs"
import { resolve } from "node:path"
import { describe, it, expect } from "vitest"

// Guards the CJS/ESM interop fix in tsup.config.ts (noExternal: ["openapi-fetch"]).
// openapi-fetch is ESM-first; if tsup emits `__toESM(require("openapi-fetch"), 1)`
// into the CJS bundle, the default export gets double-nested and
// `new TermiteClient(...)` throws "is not a function" in any CJS consumer.
// These tests fail loudly if noExternal is removed or openapi-fetch stops being inlined.

const cjsPath = resolve(__dirname, "../dist/index.cjs")
const hasBuild = existsSync(cjsPath)

describe.skipIf(!hasBuild)("CJS dist bundle", () => {
  it("does not emit require(\"openapi-fetch\") (openapi-fetch must be inlined)", () => {
    const src = readFileSync(cjsPath, "utf8")
    expect(src).not.toMatch(/require\("openapi-fetch"\)/)
    expect(src).not.toMatch(/import_openapi_fetch/)
  })

  it("instantiates TermiteClient from the CJS bundle without throwing", () => {
    const req = createRequire(import.meta.url)
    const { TermiteClient } = req(cjsPath)
    expect(() => new TermiteClient({ baseUrl: "http://localhost" })).not.toThrow()
  })
})

describe.skipIf(hasBuild)("CJS dist bundle — skipped", () => {
  it("skipped: run `pnpm build` first to validate the CJS bundle", () => {
    expect(hasBuild).toBe(false)
  })
})
