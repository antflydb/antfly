// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
import Foundation
import Testing

@testable import DictationCore

private actor ClipboardClock {
  private var waiters: [CheckedContinuation<Void, Never>] = []
  func sleep() async { await withCheckedContinuation { waiters.append($0) } }
  func pending() -> Int { waiters.count }
  func advance() {
    let waiting = waiters
    waiters = []
    waiting.forEach { $0.resume() }
  }
}
@MainActor private final class FakeClipboard {
  var count = 0
  var text = "original"
  var restores = 0
  func snapshot() -> ClipboardCoordinator.Snapshot {
    .init(count: count, items: [[.init(type: "text", data: Data(text.utf8))]])
  }
  func write(_ text: String) -> Int {
    self.text = text
    count += 1
    return count
  }
  func restore(_ items: [[ClipboardCoordinator.Representation]]) {
    text = String(decoding: items[0][0].data, as: UTF8.self)
    count += 1
    restores += 1
  }
}
@Test @MainActor func overlappingPastesPreserveOriginalAndDoNotWaitForRestoration() async throws {
  let board = FakeClipboard()
  let clock = ClipboardClock()
  let coordinator = ClipboardCoordinator(
    read: { await board.snapshot() }, count: { board.count }, writeText: { board.write($0) },
    restore: { board.restore($0) }, sleep: { await clock.sleep() })
  var sent = 0
  try await coordinator.paste("first") { sent += 1 }
  while await clock.pending() < 1 { await Task.yield() }
  try await coordinator.paste("second") { sent += 1 }
  while await clock.pending() < 2 { await Task.yield() }
  #expect(sent == 2 && board.text == "second" && board.restores == 0)
  await clock.advance()
  while board.restores == 0 { await Task.yield() }
  #expect(board.text == "original" && board.restores == 1)
}
@Test @MainActor func userCopyDuringTemporaryPasteOwnsClipboard() async throws {
  let board = FakeClipboard()
  let clock = ClipboardClock()
  let coordinator = ClipboardCoordinator(
    read: { await board.snapshot() }, count: { board.count }, writeText: { board.write($0) },
    restore: { board.restore($0) }, sleep: { await clock.sleep() })
  try await coordinator.paste("transcript") {}
  while await clock.pending() < 1 { await Task.yield() }
  _ = board.write("user copy")
  await clock.advance()
  await Task.yield()
  try await coordinator.paste("next transcript") {}
  while await clock.pending() < 1 { await Task.yield() }
  await clock.advance()
  while board.restores == 0 { await Task.yield() }
  #expect(board.text == "user copy" && board.restores == 1)
}
