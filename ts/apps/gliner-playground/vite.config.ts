import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
const headers = {
  "Cross-Origin-Opener-Policy": "same-origin",
  "Cross-Origin-Embedder-Policy": "require-corp",
};
export default defineConfig({
  plugins: [react()],
  server: { headers, port: 3101 },
  preview: { headers, port: 3101 },
  build: { target: "es2022", minify: "esbuild" },
});
