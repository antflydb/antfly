// https://vite.dev/config/
import path from "node:path";
import { fileURLToPath } from "node:url";
import { storybookTest } from "@storybook/addon-vitest/vitest-plugin";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig, type ViteUserConfig } from "vitest/config";

const dirname =
  typeof __dirname !== "undefined" ? __dirname : path.dirname(fileURLToPath(import.meta.url));

// More info at: https://storybook.js.org/docs/next/writing-tests/integrations/vitest-addon
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:8080",
        changeOrigin: true,
      },
      "/registry": {
        target: "https://registry.antfly.io/v1",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/registry/, ""),
      },
      "/termite": {
        target: "http://localhost:11433",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/termite/, ""),
      },
    },
  },
  // optimizeDeps: {
  //   esbuildOptions: {
  //     loader: {
  //       ".js": "jsx",
  //     },
  //   },
  //   include: ["@antfly/components"],
  // },
  build: {
    // Produce a single JS + CSS bundle. The dashboard is embedded into the Go
    // binary via go:embed so code-splitting has no benefit — it only inflates
    // the binary and git history (shiki alone adds ~390 chunk files).
    rollupOptions: {
      output: {
        inlineDynamicImports: true,
      },
    },
  },
  /* shadcn */
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  test: {
    projects: [
      // Unit tests for hooks and utilities
      {
        extends: true,
        test: {
          name: "unit",
          include: ["src/**/*.test.ts", "src/**/*.test.tsx"],
          environment: "jsdom",
        },
      },
      // Storybook component tests
      {
        extends: true,
        plugins: [
          // The plugin will run tests for the stories defined in your Storybook config
          // See options at: https://storybook.js.org/docs/next/writing-tests/integrations/vitest-addon#storybooktest
          storybookTest({
            configDir: path.join(dirname, ".storybook"),
          }),
        ],
        test: {
          name: "storybook",
          browser: {
            enabled: true,
            headless: true,
          },
          setupFiles: [".storybook/vitest.setup.ts"],
        },
      },
    ],
  },
} as ViteUserConfig);
