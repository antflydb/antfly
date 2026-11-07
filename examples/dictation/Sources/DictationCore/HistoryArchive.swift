// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

/// Local retention must remain usable even when the optional upload queue is
/// damaged or full. All archive mutations share one revision-fenced writer.
public actor HistoryArchive {
  private let file: URL
  private var revision: UInt64 = 0
  public init(file: URL) { self.file = file }
  public func store(_ entries: [HistoryEntry], revision next: UInt64) throws {
    guard next >= revision else { return }
    let data = try JSONEncoder().encode(entries)
    try data.write(to: file, options: .atomic)
    try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
    revision = next
  }
}
