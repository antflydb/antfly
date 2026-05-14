import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  transpilePackages: ["@antfly/design-system"],
  devIndicators: false,
};

export default nextConfig;
