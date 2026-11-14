// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

public enum CleanupVocabulary {
  /// Recognition errors need not resemble the intended spelling. Preserve every
  /// user hint, deduplicating in linear time without changing preference order.
  public static func relevant(_ terms: [String], to text: String) -> [String] {
    var seen = Set<String>()
    return terms.filter { seen.insert($0).inserted }
  }
}
