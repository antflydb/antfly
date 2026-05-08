import { resolve } from "node:path";
import react from "@vitejs/plugin-react";
import preserveDirectives from "rollup-plugin-preserve-directives";
import { defineConfig } from "vite";
import dts from "vite-plugin-dts";
import tsconfigPaths from "vite-tsconfig-paths";

export default defineConfig({
  plugins: [
    react(),
    tsconfigPaths(),
    dts({
      insertTypesEntry: true,
      exclude: ["**/*.test.tsx", "**/*.test.ts", "**/*.stories.tsx"],
    }),
  ],
  build: {
    lib: {
      entry: {
        index: resolve(__dirname, "src/index.ts"),
        "primitives/index": resolve(__dirname, "src/primitives/index.ts"),
        "brand/index": resolve(__dirname, "src/brand/index.ts"),
        "components/index": resolve(__dirname, "src/components/index.ts"),
        "charts/index": resolve(__dirname, "src/charts/index.ts"),
        "components/compound/dashboard-page": resolve(
          __dirname,
          "src/components/compound/dashboard-page.tsx"
        ),
        "components/compound/status-screen": resolve(
          __dirname,
          "src/components/compound/status-screen.tsx"
        ),
        "templates/index": resolve(__dirname, "src/templates/index.ts"),
        "examples/index": resolve(__dirname, "src/examples/index.ts"),
      },
      formats: ["es"],
      fileName: (_format, entryName) => `${entryName}.js`,
    },
    rollupOptions: {
      external: [
        "react",
        "react-dom",
        "react/jsx-runtime",
        "next-themes",
        "lucide-react",
        "recharts",
        "date-fns",
        "react-hook-form",
        "react-day-picker",
        "react-resizable-panels",
        "sonner",
        /^@radix-ui\//,
        "radix-ui",
        /^@tanstack\//,
        "class-variance-authority",
        "clsx",
        "cmdk",
        "embla-carousel-react",
        "gsap",
        "input-otp",
        "tailwind-merge",
        "vaul",
      ],
      output: {
        preserveModules: true,
        preserveModulesRoot: "src",
        entryFileNames: "[name].js",
      },
      plugins: [preserveDirectives()],
    },
    sourcemap: true,
    outDir: "dist",
  },
});
