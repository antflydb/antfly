// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

/// Local retention must remain usable even when the optional upload queue is
/// damaged or full. All archive mutations share one revision-fenced writer.
public actor HistoryArchive {
  private let file: URL
  public static let maxBytes = 16 * 1024 * 1024
  public static let maxEntries = 500
  /// Conservative JSON size: every UTF-8 byte may need a six-byte escape.
  /// This bounds both retained memory and disk without serializing on the UI actor.
  public static func bounded(_ entries: [HistoryEntry]) -> [HistoryEntry] {
    var bytes = 2
    var result: [HistoryEntry] = []
    for entry in entries.prefix(maxEntries) {
      let cost = estimatedBytes(entry)
      if cost > maxBytes - 2 { continue }
      guard cost <= maxBytes - bytes else { break }
      bytes += cost
      result.append(entry)
    }
    return result
  }
  public static func estimatedBytes(_ entry: HistoryEntry) -> Int {
    let strings = [
      entry.id, entry.preset, entry.model, entry.application, entry.raw,
      entry.text, entry.status, entry.destination?.server ?? "", entry.destination?.table ?? "",
    ]
    return 1024 + strings.reduce(0) { $0 + $1.utf8.count * 6 }
  }
  /// Only used after the outbox failed to initialize, so no uploader owns it.
  public func retireOutbox(at directory: URL) throws {
    guard FileManager.default.fileExists(atPath: directory.path) else { return }
    let retired = directory.deletingLastPathComponent().appendingPathComponent(
      "retired-outbox-" + UUID().uuidString)
    try FileManager.default.moveItem(at: directory, to: retired)
    try FileManager.default.removeItem(at: retired)
  }
  private var revision: UInt64 = 0
  private var requestedRevision: UInt64 = 0
  private let beforeWrite: @Sendable () async throws -> Void
  public init(file: URL, beforeWrite: @escaping @Sendable () async throws -> Void = {}) {
    self.file = file
    self.beforeWrite = beforeWrite
  }
  public func store(_ entries: [HistoryEntry], revision next: UInt64) async throws {
    guard next > revision, next >= requestedRevision else { return }
    requestedRevision = next
    try await beforeWrite()
    guard next == requestedRevision else { return }
    let data = try JSONEncoder().encode(Self.bounded(entries))
    guard data.count <= Self.maxBytes else {
      throw DictationError.invalid("History archive exceeds its byte limit.")
    }
    try data.write(to: file, options: .atomic)
    try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
    revision = next
  }
}
