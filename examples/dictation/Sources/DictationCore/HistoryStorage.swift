// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

/// Table preparation is shared by startup, retention enablement, and the outbox.
/// Cache existence, never credentials, and recover if the table is later removed.
public actor HistoryStorage {
  private var ready: Set<HistoryDestination> = []
  private struct Preparation {
    let id = UUID()
    let task: Task<Void, Error>
  }
  private var pending: [HistoryDestination: Preparation] = [:]
  public init() {}

  public func cancelPreparations() {
    let tasks = pending.values.map(\.task)
    pending.removeAll()
    for task in tasks { task.cancel() }
  }

  public func prepare(_ destination: HistoryDestination, client: AntflyClient) async throws {
    try Task.checkCancellation()
    if ready.contains(destination) { return }
    let preparation: Preparation
    if let existing = pending[destination] {
      preparation = existing
    } else {
      preparation = Preparation(task: Task { try await client.setup(table: destination.table) })
      pending[destination] = preparation
    }
    do {
      try await preparation.task.value
      if pending[destination]?.id == preparation.id { pending[destination] = nil }
      if ready.count >= 256, let old = ready.first { ready.remove(old) }
      ready.insert(destination)
    } catch {
      if pending[destination]?.id == preparation.id { pending[destination] = nil }
      throw error
    }
    try Task.checkCancellation()
  }

  public func upload(
    _ entry: HistoryEntry, destination: HistoryDestination, client: AntflyClient
  ) async throws {
    try await prepare(destination, client: client)
    do {
      try await client.saveHistory(entry, table: destination.table)
    } catch let failure as ServerFailure where failure.status == 404 {
      ready.remove(destination)
      try await prepare(destination, client: client)
      try await client.saveHistory(entry, table: destination.table)
    }
  }
}
