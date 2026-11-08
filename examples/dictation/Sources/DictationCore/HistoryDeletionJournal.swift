// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

/// Durable deletion intent, independent of transcript retention and upload jobs.
/// Keep every original destination until remote acknowledgments AND local cleanup
/// are committed. A crash may repeat an idempotent delete, never forget a target.
public actor HistoryDeletionJournal {
  private struct Record: Codable {
    var destinations: Set<HistoryDestination>
    var pending: Set<HistoryDestination>
  }
  private let file: URL
  private var records: [String: Record]
  public nonisolated let recoveredIDs: Set<String>
  private static let maxBytes = 4 * 1024 * 1024
  public init(file: URL) throws {
    self.file = file
    var loaded: [String: Record] = [:]
    if FileManager.default.fileExists(atPath: file.path) {
      guard (try file.resourceValues(forKeys: [.fileSizeKey]).fileSize ?? 0) <= Self.maxBytes else {
        throw DictationError.invalid(
          "History deletion journal exceeds its byte limit. Uploads remain paused.")
      }
      let data = try Data(contentsOf: file)
      guard data.count <= Self.maxBytes else {
        throw DictationError.invalid("History deletion journal is too large.")
      }
      loaded = try JSONDecoder().decode([String: Record].self, from: data)
    }
    try Self.validate(loaded)
    records = loaded
    recoveredIDs = Set(loaded.keys)
  }
  private static func validate(_ records: [String: Record]) throws {
    guard records.count <= 1000, records.values.reduce(0, { $0 + $1.destinations.count }) <= 2000
    else {
      throw DictationError.invalid(
        "History deletion journal is full. Retry existing deletions first.")
    }
    for (id, record) in records {
      guard !id.isEmpty, id.utf8.count <= 256, !record.destinations.isEmpty,
        record.pending.isSubset(of: record.destinations)
      else {
        throw DictationError.invalid("Invalid history deletion journal.")
      }
      for destination in record.destinations {
        var config = Configuration()
        config.server = destination.server
        config.table = destination.table
        _ = try config.validated()
      }
    }
  }
  private func commit(_ next: [String: Record]) throws {
    try Self.validate(next)
    let data = try JSONEncoder().encode(next)
    guard data.count <= Self.maxBytes else {
      throw DictationError.invalid("History deletion journal exceeds its byte limit.")
    }
    if next.isEmpty {
      if FileManager.default.fileExists(atPath: file.path) {
        try FileManager.default.removeItem(at: file)
      }
    } else {
      try data.write(to: file, options: .atomic)
      try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
    }
    records = next
  }
  public func ids() -> Set<String> { Set(records.keys) }
  public func begin(_ groups: [HistoryDestination: Set<String>]) throws {
    var next = records
    for (destination, ids) in groups {
      for id in ids {
        var record = next[id] ?? Record(destinations: [], pending: [])
        if record.destinations.insert(destination).inserted { record.pending.insert(destination) }
        next[id] = record
      }
    }
    try commit(next)
  }
  public func pending() -> [HistoryDestination: Set<String>] {
    var groups: [HistoryDestination: Set<String>] = [:]
    for (id, record) in records {
      for destination in record.pending { groups[destination, default: []].insert(id) }
    }
    return groups
  }
  public func acknowledge(_ destination: HistoryDestination, ids: Set<String>) throws {
    var next = records
    for id in ids { next[id]?.pending.remove(destination) }
    try commit(next)
  }
  public func complete(_ ids: Set<String>) throws {
    guard ids.allSatisfy({ records[$0]?.pending.isEmpty == true }) else {
      throw DictationError.invalid("History deletion still has unacknowledged destinations.")
    }
    var next = records
    for id in ids { next.removeValue(forKey: id) }
    try commit(next)
  }
}
