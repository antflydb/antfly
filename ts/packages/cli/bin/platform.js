export function detectLinuxLibc(report = process.report) {
  try {
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

  const libc = detectLinuxLibc(report);
  return libc === "glibc" ? `@antfly/cli-linux-${arch}-gnu` : `@antfly/cli-linux-${arch}`;
}
