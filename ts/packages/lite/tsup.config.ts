import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["cjs", "esm"],
  dts: true,
  clean: true,
  platform: "node",
  target: "node24",
  // koffi ships prebuilt native bindings and its own loader; never bundle it.
  external: ["koffi"],
});
