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
    if ownedCount != count() {
      saved = nil
      ownedCount = nil
      restoration?.cancel()
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
    try Task.checkCancellation()
    restoration?.cancel()
    let lease = writeText(text)
    ownedCount = lease
    do { try send() } catch {
      restoreIfOwned(lease)
      throw error
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
  private func restoreIfOwned(_ lease: Int) {
    guard ownedCount == lease else { return }
    if count() == lease, let saved { restore(saved.items) }
    saved = nil
    ownedCount = nil
    restoration = nil
  }
}
