import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["test/**/*.{test,spec}.ts", "src/**/*.{test,spec}.ts"],
    exclude: ["node_modules", "dist"],
    // The concurrency and busy-timeout tests open real handles and sleep
    // across close/reopen cycles; give them room without slowing down CI on
    // every run.
    testTimeout: 20_000,
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      exclude: ["node_modules", "dist", "**/*.d.ts", "**/*.config.*"],
    },
  },
});
