import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["test/**/*.{test,spec}.ts", "src/**/*.{test,spec}.ts"],
    exclude: ["node_modules", "dist", "examples"],
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      exclude: [
        "node_modules",
        "dist",
        "examples",
        "**/*.d.ts",
        "src/antfly-api.d.ts",
        "src/bleve-query.d.ts",
        "src/antfly.ts",
        "**/*.config.*",
      ],
    },
  },
});
