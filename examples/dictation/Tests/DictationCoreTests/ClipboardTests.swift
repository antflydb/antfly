// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

private actor ClipboardClock {
  private var waiters: [CheckedContinuation<Void, Never>] = []
  func sleep() async { await withCheckedContinuation { waiters.append($0) } }
  func pending() -> Int { waiters.count }
  func advanceFirst() { if !waiters.isEmpty { waiters.removeFirst().resume() } }
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

@Test(arguments: [false, true]) @MainActor func shutdownFinishesOnlyAnOwnedClipboardLease(
  userCopied: Bool
) async throws {
  let board = FakeClipboard()
  let clock = ClipboardClock()
  let coordinator = ClipboardCoordinator(
    read: { await board.snapshot() }, count: { board.count }, writeText: { board.write($0) },
    restore: { board.restore($0) }, sleep: { await clock.sleep() })
  try await coordinator.paste("dictation") {}
  while await clock.pending() < 1 { await Task.yield() }
  if userCopied { _ = board.write("user copy") }
  var finished = false
  let shutdown = Task {
    await coordinator.finishRestoration()
    finished = true
  }
  await Task.yield()
  #expect(!finished)
  await clock.advance()
  await shutdown.value
  #expect(board.text == (userCopied ? "user copy" : "original"))
  #expect(board.restores == (userCopied ? 0 : 1))
}

@Test @MainActor func applicationQuitWaitsForClipboardRestoration() async throws {
  let path = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  try FileManager.default.createDirectory(at: path, withIntermediateDirectories: true)
  defer { try? FileManager.default.removeItem(at: path) }
  let board = FakeClipboard()
  let clock = ClipboardClock()
  let coordinator = ClipboardCoordinator(
    read: { await board.snapshot() }, count: { board.count }, writeText: { board.write($0) },
    restore: { board.restore($0) }, sleep: { await clock.sleep() })
  let store = Store(
    directory: path, registerHotkey: false,
    originalPaster: { text, validate in try await coordinator.paste(text) { try validate() } },
    clipboardRestoration: { await coordinator.finishRestoration() },
    credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  store.preview = "recovered"
  store.canRecoverRaw = true
  store.insertOriginal()
  await store.requestTask?.value
  while await clock.pending() < 1 { await Task.yield() }
  var quitReady = false
  let quit = Task {
    await store.prepareToQuit()
    quitReady = true
  }
  await Task.yield()
  #expect(!quitReady)
  await clock.advance()
  await quit.value
  #expect(board.text == "original")
}

@Test @MainActor func shutdownFollowsTheNewestOverlappingRestoration() async throws {
  let board = FakeClipboard()
  let clock = ClipboardClock()
  let coordinator = ClipboardCoordinator(
    read: { await board.snapshot() }, count: { board.count }, writeText: { board.write($0) },
    restore: { board.restore($0) }, sleep: { await clock.sleep() })
  try await coordinator.paste("first") {}
  while await clock.pending() < 1 { await Task.yield() }
  var finished = false
  let shutdown = Task {
    await coordinator.finishRestoration()
    finished = true
  }
  await Task.yield()
  try await coordinator.paste("second") {}
  while await clock.pending() < 2 { await Task.yield() }
  await clock.advanceFirst()
  for _ in 0..<10 { await Task.yield() }
  #expect(!finished && board.text == "second")
  await clock.advance()
  await shutdown.value
  #expect(board.text == "original" && board.restores == 1)
}
private actor ReadFailure {
  var enabled = false
  func enable() { enabled = true }
}
@Test @MainActor func failedSnapshotAfterUserCopyCannotLeaveAnUndrainableTimer() async throws {
  let board = FakeClipboard()
  let clock = ClipboardClock()
  let failure = ReadFailure()
  let coordinator = ClipboardCoordinator(
    read: {
      if await failure.enabled { throw DictationError.invalid("Read failed") }
      return await board.snapshot()
    }, count: { board.count }, writeText: { board.write($0) },
    restore: { board.restore($0) }, sleep: { await clock.sleep() })
  try await coordinator.paste("first") {}
  while await clock.pending() < 1 { await Task.yield() }
  _ = board.write("user copy")
  await failure.enable()
  do {
    try await coordinator.paste("second") {}
    Issue.record("Expected snapshot failure")
  } catch is DictationError {}
  await coordinator.finishRestoration()
  #expect(board.text == "user copy" && board.restores == 0)
  await clock.advance()
}
