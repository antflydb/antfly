#!/usr/bin/env python3
"""Render the Homebrew formula for the native Zig Antfly runtime."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path


ARCHIVES = {
    "darwin_arm64": ("Darwin", "arm64", None),
    "linux_arm64": ("Linux", "arm64", "gnu"),
    "linux_x86_64": ("Linux", "x86_64", "gnu"),
}


def archive_name(version: str, os_name: str, arch: str, variant: str | None) -> str:
    suffix = f"_{variant}" if variant else ""
    return f"antfly_{version}_{os_name}_{arch}{suffix}.tar.gz"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True, help="Version without v prefix")
    parser.add_argument("--tag", required=True, help="Release tag with v prefix")
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    values: dict[str, str] = {}
    for key, (os_name, arch, variant) in ARCHIVES.items():
        name = archive_name(args.version, os_name, arch, variant)
        path = args.archive_dir / name
        if not path.exists():
            raise SystemExit(f"missing archive for Homebrew formula: {path}")
        values[f"{key}_archive"] = name
        values[f"{key}_sha256"] = sha256(path)

    tag = args.tag
    base_url = f"https://releases.antfly.io/antfly/{tag}"
    formula = f'''# typed: false
# frozen_string_literal: true

class Antfly < Formula
  desc "Native Zig AntflyDB runtime"
  homepage "https://docs.antfly.io"
  license "Elastic-2.0"

  if OS.mac?
    if Hardware::CPU.arm?
      url "{base_url}/{values["darwin_arm64_archive"]}"
      sha256 "{values["darwin_arm64_sha256"]}"
    else
      odie "antfly supports Apple Silicon macOS only"
    end
  elsif OS.linux?
    if Hardware::CPU.arm?
      url "{base_url}/{values["linux_arm64_archive"]}"
      sha256 "{values["linux_arm64_sha256"]}"
    else
      url "{base_url}/{values["linux_x86_64_archive"]}"
      sha256 "{values["linux_x86_64_sha256"]}"
    end
  end

  def install
    bin.install "antfly"
    include.install Dir["include/*"] if Dir.exist?("include")
    lib.install Dir["lib/*"] if Dir.exist?("lib")
    (share/"antfly").install Dir["share/antfly/*"] if Dir.exist?("share/antfly")
    bash_completion.install "completions/antfly.bash" => "antfly"
    zsh_completion.install "completions/antfly.zsh" => "_antfly"
    fish_completion.install "completions/antfly.fish"
  end

  service do
    run [opt_bin/"antfly", "standalone", "--data-dir", var/"lib/antfly"]
    keep_alive true
    working_dir var/"lib/antfly"
    log_path var/"log/antfly.log"
    error_log_path var/"log/antfly.err.log"
  end

  def post_install
    (var/"lib/antfly").mkpath
  end

  test do
    system "#{{bin}}/antfly", "--help"
  end

  def caveats
    <<~EOS
      antfly is now the native Zig runtime.

      Create and verify a portable backup before upgrading across storage-format
      changes, and restore into a fresh data directory when rollback is needed.

      Start the local single-node service with:
        brew services start antflydb/taps/antfly
    EOS
  end
end
'''
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(formula)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
