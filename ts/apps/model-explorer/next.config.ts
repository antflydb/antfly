import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "export",
  reactStrictMode: true,
  transpilePackages: ["@antfly/design-system"],
  devIndicators: false,
  images: { unoptimized: true },
};

export default nextConfig;
