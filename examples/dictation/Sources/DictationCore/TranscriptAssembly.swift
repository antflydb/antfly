// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

/// Preserve explicit recognizer whitespace. Infer a boundary only when neither
/// side supplies one; unspaced writing systems must not acquire ASCII spaces.
public enum TranscriptAssembly {
  public static func join(_ parts: [String], language: String = "") -> String {
    func unspacedScript(_ character: Character) -> Bool {
      character.unicodeScalars.contains {
        (0x3000...0x30FF).contains($0.value) || (0x3400...0x9FFF).contains($0.value)
          || (0x0E00...0x0EFF).contains($0.value) || (0x1000...0x109F).contains($0.value)
          || (0x1780...0x17FF).contains($0.value)
          || (0xF900...0xFAFF).contains($0.value) || (0x20000...0x323AF).contains($0.value)
      }
    }
    return parts.filter { !$0.isEmpty }.reduce(into: "") { result, next in
      if let last = result.last, let first = next.first,
        !last.isWhitespace, !first.isWhitespace,
        !unspacedScript(last), !unspacedScript(first),
        !",.!?:;、。！？：；)]}»”’".contains(first), !"([{«“‘".contains(last)
      {
        result.append(" ")
      }
      result.append(next)
    }
  }
}
