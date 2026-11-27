// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

/// Share table preparation by destination without sharing callers' cancellation.
/// A preparation is owned by its waiters; the last departing waiter stops it.
public actor HistoryStorage {
  private var ready: Set<HistoryDestination> = []
  private struct Preparation {
    let id: UUID
    let task: Task<Void, Never>
    var waiters: [UUID: CheckedContinuation<Void, Error>]
  }
  private var pending: [HistoryDestination: Preparation] = [:]
  public init() {}

  // Internal read-only diagnostics also allow deterministic concurrency tests.
  func pendingWaiterCount(for destination: HistoryDestination) -> Int {
    pending[destination]?.waiters.count ?? 0
  }

  public func cancelPreparations() {
    let preparations = Array(pending.values)
    pending.removeAll()
    for preparation in preparations {
      preparation.task.cancel()
      for waiter in preparation.waiters.values { waiter.resume(throwing: CancellationError()) }
    }
  }

  private func cancelWaiter(_ waiter: UUID, destination: HistoryDestination) {
    guard var preparation = pending[destination],
      let continuation = preparation.waiters.removeValue(forKey: waiter)
    else { return }
    if preparation.waiters.isEmpty {
      pending[destination] = nil
      preparation.task.cancel()
    } else {
      pending[destination] = preparation
    }
    continuation.resume(throwing: CancellationError())
  }

  private func finish(_ destination: HistoryDestination, id: UUID, result: Result<Void, Error>) {
    guard let preparation = pending[destination], preparation.id == id else { return }
    pending[destination] = nil
    if case .success = result {
      if ready.count >= 256, let old = ready.first { ready.remove(old) }
      ready.insert(destination)
    }
    for waiter in preparation.waiters.values { waiter.resume(with: result) }
  }

  public func prepare(_ destination: HistoryDestination, client: AntflyClient) async throws {
    try Task.checkCancellation()
    if ready.contains(destination) { return }
    let waiter = UUID()
    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation {
        (continuation: CheckedContinuation<Void, Error>) in
        guard !Task.isCancelled else {
          continuation.resume(throwing: CancellationError())
          return
        }
        if var preparation = pending[destination] {
          preparation.waiters[waiter] = continuation
          pending[destination] = preparation
        } else {
          let id = UUID()
          let task = Task {
            let result: Result<Void, Error>
            do {
              try await client.setup(table: destination.table)
              result = .success(())
            } catch { result = .failure(error) }
            finish(destination, id: id, result: result)
          }
          pending[destination] = Preparation(id: id, task: task, waiters: [waiter: continuation])
        }
      }
    } onCancel: {
      Task { await self.cancelWaiter(waiter, destination: destination) }
    }
    try Task.checkCancellation()
  }

  public func upload(
    _ entry: HistoryEntry, destination: HistoryDestination, client: AntflyClient
  ) async throws {
    try Task.checkCancellation()
    // Existing tables need only write permission. Metadata/create permission is
    // required only when the write reports a genuinely missing destination.
    do {
      try await client.saveHistory(entry, table: destination.table)
    } catch let failure as ServerFailure where failure.status == 404 {
      ready.remove(destination)
      try await prepare(destination, client: client)
      try await client.saveHistory(entry, table: destination.table)
    }
  }
}
