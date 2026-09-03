export function detectLinuxLibc(report = process.report) {
  try {
    // Node 24's supported GNU/Linux floor is glibc 2.28, matching the GNU
    // Antfly archive. The shell installer separately handles older glibc by
    // selecting the portable musl build.
    return report?.getReport()?.header?.glibcVersionRuntime ? "glibc" : "musl";
  } catch {
    return "musl";
  }
}

export function resolvePlatformPackage(platform, arch, report = process.report) {
  if (platform === "darwin" && arch === "arm64") {
    return "@antfly/cli-darwin-arm64";
  }
  if (platform !== "linux" || (arch !== "arm64" && arch !== "x64")) {
    return undefined;
  }

  if (detectLinuxLibc(report) !== "glibc") {
    return undefined;
  }
  return `@antfly/cli-linux-${arch}`;
}
