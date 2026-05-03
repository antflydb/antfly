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
