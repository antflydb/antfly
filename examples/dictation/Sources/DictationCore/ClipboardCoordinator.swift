// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

/// Owns overlapping temporary clipboard writes. A later paste extends the same
/// lease instead of saving an earlier transcript as the user's original clipboard.
@MainActor public final class ClipboardCoordinator {
  public struct Representation: Sendable {
    public var type: String
    public var data: Data
    public init(type: String, data: Data) {
      self.type = type
      self.data = data
    }
  }
  public struct Snapshot: Sendable {
    public var count: Int
    public var items: [[Representation]]
    public init(count: Int, items: [[Representation]]) {
      self.count = count
      self.items = items
    }
  }
  private let read: @Sendable () async throws -> Snapshot
  private let count: @MainActor () -> Int
  private let writeText: @MainActor (String) -> Int
  private let restore: @MainActor ([[Representation]]) -> Void
  private let sleep: @Sendable () async throws -> Void
  private let preservationTimeout: Duration
  private var saved: Snapshot?
  private var ownedCount: Int?
  private var restoration: Task<Void, Never>?
  private var generation = UUID()
  private var pendingRead: ClipboardSnapshotRead?
  private var snapshotWaiters: [UUID: @Sendable () -> Void] = [:]
  private var active = 0
  private var idleWaiters: [CheckedContinuation<Void, Never>] = []
  public init(
    read: @escaping @Sendable () async throws -> Snapshot,
    count: @escaping @MainActor () -> Int,
    writeText: @escaping @MainActor (String) -> Int,
    restore: @escaping @MainActor ([[Representation]]) -> Void,
    preservationTimeout: Duration = .seconds(5),
    sleep: @escaping @Sendable () async throws -> Void = {
      try await Task.sleep(for: .milliseconds(750))
    }
  ) {
    self.read = read
    self.count = count
    self.writeText = writeText
    self.restore = restore
    self.preservationTimeout = preservationTimeout
    self.sleep = sleep
  }
  public func paste(_ text: String, send: @MainActor () throws -> Void) async throws {
    try await withPaste(text) { validateOwnership in
      try validateOwnership()
      try send()
    }
  }
  /// Holds the lease across asynchronous confirmation. An overlapping paste or
  /// a user copy invalidates the old operation without restoring over new data.
  public func withPaste(
    _ text: String, restoreAfterOperation: Bool = false,
    prepare: @MainActor (String) async throws -> String = { $0 },
    operation: @MainActor (_ validateOwnership: @MainActor () throws -> Void) async throws -> Void
  ) async throws {
    // Reserve intent before any suspension. Older confirmations must not act
    // or restore over a newer operation that is still preserving/preparing.
    let operationID = UUID()
    generation = operationID
    cancelRestoration()
    let superseded = Array(snapshotWaiters.values)
    snapshotWaiters.removeAll()
    for cancel in superseded { cancel() }
    active += 1
    defer {
      active -= 1
      if active == 0 {
        let waiting = idleWaiters
        idleWaiters = []
        for continuation in waiting { continuation.resume() }
      }
    }
    if ownedCount != count() {
      saved = nil
      ownedCount = nil
      cancelRestoration()
    }
    if saved == nil {
      let snapshot = try await snapshot(operationID)
      try validateGeneration(operationID)
      guard snapshot.count == count() else {
        throw DictationError.invalid(
          "Clipboard changed. Copy the transcript to insert it manually.")
      }
      // Another paste may have acquired ownership while snapshotting.
      if saved == nil { saved = snapshot }
    }
    let observedCount = count()
    let previousLease = ownedCount
    cancelRestoration()
    let prepared: String
    do {
      prepared = try await prepare(text)
      try validateGeneration(operationID)
      guard observedCount == count() else {
        throw DictationError.invalid("Clipboard changed during insertion preparation.")
      }
    } catch {
      if generation != operationID { throw error }
      if let previousLease {
        // A preceding operation may already have posted Paste against this
        // shared lease, even though the newer preparation failed before write.
        scheduleRestoration(previousLease, generation: operationID)
      } else if ownedCount == nil {
        saved = nil
      }
      throw error
    }
    cancelRestoration()
    let lease = writeText(prepared)
    ownedCount = lease
    do {
      try await operation {
        try self.validateGeneration(operationID)
        guard self.ownedCount == lease, self.count() == lease else {
          throw DictationError.invalid("Clipboard changed; the pending insertion was canceled.")
        }
      }
    } catch {
      // A posted paste is asynchronous. Even a failed/canceled confirmation
      // must leave the payload available for the normal delivery grace period.
      scheduleRestoration(lease, generation: operationID)
      throw error
    }
    guard generation == operationID, ownedCount == lease else { return }
    if restoreAfterOperation {
      restoreIfOwned(lease, generation: operationID)
      return
    }
    scheduleRestoration(lease, generation: operationID)
  }
  private func scheduleRestoration(_ lease: Int, generation operationID: UUID) {
    guard generation == operationID, ownedCount == lease else { return }
    cancelRestoration()
    let sleep = self.sleep
    restoration = Task { [weak self] in
      do {
        try await sleep()
        try Task.checkCancellation()
      } catch { return }
      self?.restoreIfOwned(lease, generation: operationID)
    }
  }
  /// Only shutdown waits for the lease; normal insertion remains nonblocking.
  /// A newer paste may extend it while we await an older timer.
  public func finishRestoration() async {
    while true {
      if active > 0 {
        await withCheckedContinuation { idleWaiters.append($0) }
      } else if let pending = restoration {
        await pending.value
      } else {
        return
      }
    }
  }
  private func cancelRestoration() {
    restoration?.cancel()
    restoration = nil
  }
  private func validateGeneration(_ id: UUID) throws {
    try Task.checkCancellation()
    guard generation == id else { throw CancellationError() }
  }
  private func snapshot(_ operationID: UUID) async throws -> Snapshot {
    if pendingRead == nil || pendingRead?.isComplete == true {
      let id = UUID()
      pendingRead = ClipboardSnapshotRead(
        id: id, read: read,
        onComplete: { [weak self] in
          Task { @MainActor [weak self] in
            if self?.pendingRead?.id == id { self?.pendingRead = nil }
          }
        })
    }
    let provider = pendingRead!
    let waiter = ClipboardSnapshotWaiter()
    snapshotWaiters[operationID] = { waiter.complete(.failure(CancellationError())) }
    provider.attach(waiter, id: operationID)
    let duration = preservationTimeout
    let timeout = Task { [weak waiter] in
      do { try await Task.sleep(for: duration) } catch { return }
      waiter?.complete(
        .failure(
          DictationError.invalid(
            "Clipboard provider did not respond. Copy the transcript manually.")))
    }
    defer {
      timeout.cancel()
      provider.detach(operationID)
      snapshotWaiters.removeValue(forKey: operationID)
    }
    return try await withTaskCancellationHandler {
      try Task.checkCancellation()
      return try await withCheckedThrowingContinuation { waiter.install($0) }
    } onCancel: {
      waiter.complete(.failure(CancellationError()))
    }
  }
  private func restoreIfOwned(_ lease: Int, generation operationID: UUID) {
    guard generation == operationID, ownedCount == lease else { return }
    if count() == lease, let saved { restore(saved.items) }
    saved = nil
    ownedCount = nil
    restoration = nil
  }
}
