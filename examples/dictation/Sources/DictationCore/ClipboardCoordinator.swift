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
  private var saved: Snapshot?
  private var ownedCount: Int?
  private var restoration: Task<Void, Never>?
  private var active = 0
  private var idleWaiters: [CheckedContinuation<Void, Never>] = []
  public init(
    read: @escaping @Sendable () async throws -> Snapshot,
    count: @escaping @MainActor () -> Int,
    writeText: @escaping @MainActor (String) -> Int,
    restore: @escaping @MainActor ([[Representation]]) -> Void,
    sleep: @escaping @Sendable () async throws -> Void = {
      try await Task.sleep(for: .milliseconds(750))
    }
  ) {
    self.read = read
    self.count = count
    self.writeText = writeText
    self.restore = restore
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
    active += 1
    defer {
      active -= 1
      if active == 0 {
        let waiting = idleWaiters
        idleWaiters = []
        waiting.forEach { $0.resume() }
      }
    }
    if ownedCount != count() {
      saved = nil
      ownedCount = nil
      cancelRestoration()
    }
    if saved == nil {
      let snapshot = try await read()
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
      try Task.checkCancellation()
      guard observedCount == count() else {
        throw DictationError.invalid("Clipboard changed during insertion preparation.")
      }
    } catch {
      if let previousLease {
        restoreIfOwned(previousLease)
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
        try Task.checkCancellation()
        guard self.ownedCount == lease, self.count() == lease else {
          throw DictationError.invalid("Clipboard changed; the pending insertion was canceled.")
        }
      }
    } catch {
      restoreIfOwned(lease)
      throw error
    }
    guard ownedCount == lease else { return }
    if restoreAfterOperation {
      restoreIfOwned(lease)
      return
    }
    let sleep = self.sleep
    restoration = Task { [weak self] in
      do {
        try await sleep()
        try Task.checkCancellation()
      } catch { return }
      self?.restoreIfOwned(lease)
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
  private func restoreIfOwned(_ lease: Int) {
    guard ownedCount == lease else { return }
    if count() == lease, let saved { restore(saved.items) }
    saved = nil
    ownedCount = nil
    restoration = nil
  }
}
