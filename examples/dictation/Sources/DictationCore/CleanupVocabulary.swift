// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

public enum CleanupVocabulary {
  /// Small dictionaries remain intact. Large dictionaries prioritize plausible
  /// spelling hints for this bounded cleanup batch, with a conservative fallback
  /// when recognition supplied no usable match. Never changes saved preferences.
  public static func relevant(_ terms: [String], to text: String) -> [String] {
    let unique = terms.reduce(into: [String]()) { if !$0.contains($1) { $0.append($1) } }
    guard unique.count > 32 else { return unique }
    func normalized(_ value: String) -> String {
      value.folding(
        options: [.caseInsensitive, .diacriticInsensitive],
        locale: Locale(identifier: "en_US_POSIX")
      )
      .filter { $0.isLetter || $0.isNumber }
    }
    let compact = normalized(text)
    let words = Set(
      text.split(whereSeparator: { !$0.isLetter && !$0.isNumber }).map { normalized(String($0)) })
    let matches = unique.filter { term in
      let key = normalized(term)
      guard key.count >= 3 else { return compact.contains(key) }
      if compact.contains(key) { return true }
      guard key.count <= 32 else { return false }
      return words.contains { close(key, $0) }
    }
    return matches.isEmpty ? unique : matches
  }
  private static func close(_ a: String, _ b: String) -> Bool {
    let bound = a.count <= 6 ? 2 : 3
    guard b.count >= 3, abs(a.count - b.count) <= bound else { return false }
    let left = Array(a)
    let right = Array(b)
    var previous = Array(0...right.count)
    for (i, char) in left.enumerated() {
      var row = [i + 1]
      for (j, other) in right.enumerated() {
        row.append(min(row[j] + 1, previous[j + 1] + 1, previous[j] + (char == other ? 0 : 1)))
      }
      if row.min()! > bound { return false }
      previous = row
    }
    return previous.last! <= bound
  }
}
