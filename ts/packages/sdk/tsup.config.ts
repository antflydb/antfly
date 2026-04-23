import { defineConfig } from "tsup"

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["cjs", "esm"],
  dts: true,
  clean: true,
  // Bundle openapi-fetch into the output to avoid a CJS/ESM interop crash.
  // openapi-fetch is ESM-first; when tsup emits CJS that does
  // `__toESM(require("openapi-fetch"), 1)`, the default export gets
  // double-nested (`.default` becomes an object, not the createClient
  // function), so `(0, import_openapi_fetch.default)(...)` throws
  // "is not a function" under tsx/ts-node in any CJS consumer project.
  // Inlining sidesteps the interop entirely.
  noExternal: ["openapi-fetch"],
})
