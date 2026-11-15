// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

/// A lazy pasteboard provider cannot be forcibly interrupted safely. Keep at
/// most one provider read in flight, while canceling/timing out its waiters.
final class ClipboardSnapshotRead: @unchecked Sendable {
  typealias Snapshot = ClipboardCoordinator.Snapshot
  let id: UUID
  private let lock = NSLock()
  private var result: Result<Snapshot, Error>?
  private var waiters: [UUID: ClipboardSnapshotWaiter] = [:]
  init(
    id: UUID, read: @escaping @Sendable () async throws -> Snapshot,
    onComplete: @escaping @Sendable () -> Void
  ) {
    self.id = id
    Task.detached { [self] in
      do { complete(.success(try await read())) } catch { complete(.failure(error)) }
      onComplete()
    }
  }
  var isComplete: Bool { lock.withLock { result != nil } }
  func attach(_ waiter: ClipboardSnapshotWaiter, id: UUID) {
    let completed = lock.withLock { () -> Result<Snapshot, Error>? in
      if let result { return result }
      waiters[id] = waiter
      return nil
    }
    if let completed { waiter.complete(completed) }
  }
  func detach(_ id: UUID) { _ = lock.withLock { waiters.removeValue(forKey: id) } }
  private func complete(_ value: Result<Snapshot, Error>) {
    let waiting = lock.withLock { () -> [ClipboardSnapshotWaiter] in
      result = value
      let values = Array(waiters.values)
      waiters.removeAll()
      return values
    }
    waiting.forEach { $0.complete(value) }
  }
}

final class ClipboardSnapshotWaiter: @unchecked Sendable {
  private let lock = NSLock()
  private var result: Result<ClipboardSnapshotRead.Snapshot, Error>?
  private var continuation: CheckedContinuation<ClipboardSnapshotRead.Snapshot, Error>?
  func install(_ value: CheckedContinuation<ClipboardSnapshotRead.Snapshot, Error>) {
    let completed = lock.withLock { () -> Result<ClipboardSnapshotRead.Snapshot, Error>? in
      if let result { return result }
      continuation = value
      return nil
    }
    if let completed { value.resume(with: completed) }
  }
  func complete(_ value: Result<ClipboardSnapshotRead.Snapshot, Error>) {
    let pending = lock.withLock {
      () -> CheckedContinuation<ClipboardSnapshotRead.Snapshot, Error>? in
      guard result == nil else { return nil }
      result = value
      let pending = continuation
      continuation = nil
      return pending
    }
    pending?.resume(with: value)
  }
}
