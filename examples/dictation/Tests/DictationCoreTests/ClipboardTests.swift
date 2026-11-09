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

@Test @MainActor func confirmedPasteKeepsClipboardUntilDelayedAcknowledgement() async throws {
  let board = FakeClipboard()
  let clock = ClipboardClock()
  let coordinator = ClipboardCoordinator(
    read: { await board.snapshot() }, count: { board.count },
    writeText: { board.write($0) }, restore: { board.restore($0) }, sleep: { await clock.sleep() })
  var sent = false
  let operation = Task { @MainActor in
    try await coordinator.withPaste("dictation", restoreAfterOperation: true) { ownership in
      sent = true
      await clock.sleep()  // Simulates a target consuming paste after the old 750 ms lease.
      try ownership()
      #expect(board.text == "dictation" && board.restores == 0)
    }
  }
  while await clock.pending() == 0 { await Task.yield() }
  #expect(sent)
  #expect(board.text == "dictation" && board.restores == 0)
  var drained = false
  let shutdown = Task {
    await coordinator.finishRestoration()
    drained = true
  }
  await Task.yield()
  #expect(!drained)
  await clock.advance()
  try await operation.value
  await shutdown.value
  #expect(board.text == "original" && board.restores == 1 && drained)
}
@Test @MainActor func changedClipboardInvalidatesPendingConfirmedAction() async throws {
  let board = FakeClipboard()
  let clock = ClipboardClock()
  let coordinator = ClipboardCoordinator(
    read: { await board.snapshot() }, count: { board.count },
    writeText: { board.write($0) }, restore: { board.restore($0) })
  var sent = false
  var entered = false
  let operation = Task { @MainActor in
    try await coordinator.withPaste("dictation", restoreAfterOperation: true) { ownership in
      sent = true
      await clock.sleep()
      try ownership()
      entered = true
    }
  }
  while await clock.pending() == 0 { await Task.yield() }
  #expect(sent)
  _ = board.write("user copied this")
  await clock.advance()
  do {
    try await operation.value
    Issue.record("Lost lease accepted")
  } catch {}
  #expect(!entered && board.text == "user copied this" && board.restores == 0)
}

@Test @MainActor func newerPasteInvalidatesOlderConfirmationWithoutLosingItsRestore() async throws {
  let board = FakeClipboard()
  let clock = ClipboardClock()
  let coordinator = ClipboardCoordinator(
    read: { await board.snapshot() }, count: { board.count },
    writeText: { board.write($0) }, restore: { board.restore($0) }, sleep: { await clock.sleep() })
  var entered = false
  let first = Task { @MainActor in
    try await coordinator.withPaste("first", restoreAfterOperation: true) { ownership in
      await clock.sleep()
      try ownership()
      entered = true
    }
  }
  while await clock.pending() < 1 { await Task.yield() }
  try await coordinator.paste("second") {}
  while await clock.pending() < 2 { await Task.yield() }
  await clock.advanceFirst()
  do {
    try await first.value
    Issue.record("Older action retained ownership")
  } catch {}
  #expect(!entered && board.text == "second" && board.restores == 0)
  await clock.advance()
  await coordinator.finishRestoration()
  #expect(board.text == "original" && board.restores == 1)
}
@Test @MainActor func preparationCannotOverwriteAnExternalClipboardChange() async throws {
  let board = FakeClipboard()
  let coordinator = ClipboardCoordinator(
    read: { await board.snapshot() }, count: { board.count },
    writeText: { board.write($0) }, restore: { board.restore($0) })
  do {
    try await coordinator.withPaste(
      "dictation",
      prepare: { text in
        _ = board.write("external")
        return text
      }
    ) { _ in Issue.record("Paste sent after clipboard changed") }
    Issue.record("Preparation accepted a changed clipboard")
  } catch {}
  #expect(board.text == "external" && board.restores == 0)
}

@Test @MainActor func newerPreparationImmediatelyInvalidatesOldActionAndRetainsSnapshot()
  async throws
{
  let board = FakeClipboard()
  let firstGate = ClipboardClock()
  let preparing = ClipboardClock()
  let coordinator = ClipboardCoordinator(
    read: { await board.snapshot() }, count: { board.count }, writeText: { board.write($0) },
    restore: { board.restore($0) })
  var oldAction = false
  var newAction = false
  let first = Task { @MainActor in
    try await coordinator.withPaste("first", restoreAfterOperation: true) { ownership in
      await firstGate.sleep()
      try ownership()
      oldAction = true
    }
  }
  while await firstGate.pending() == 0 { await Task.yield() }
  let second = Task { @MainActor in
    try await coordinator.withPaste(
      "second", restoreAfterOperation: true,
      prepare: { text in
        await preparing.sleep()
        return text
      }
    ) { ownership in
      try ownership()
      newAction = true
    }
  }
  while await preparing.pending() == 0 { await Task.yield() }
  await firstGate.advance()
  do {
    try await first.value
    Issue.record("Old action survived newer intent")
  } catch is CancellationError {}
  #expect(!oldAction && board.text == "first" && board.restores == 0)
  await preparing.advance()
  try await second.value
  #expect(newAction && board.text == "original" && board.restores == 1)
}
@Test @MainActor func canceledProviderReadDoesNotBlockShutdownOrSpawnAnotherProvider() async throws
{
  let board = FakeClipboard()
  let provider = ClipboardClock()
  let coordinator = ClipboardCoordinator(
    read: {
      await provider.sleep()
      return await board.snapshot()
    }, count: { board.count }, writeText: { board.write($0) }, restore: { board.restore($0) })
  let first = Task { @MainActor in
    try await coordinator.withPaste("first") { _ in Issue.record("Canceled read pasted") }
  }
  while await provider.pending() == 0 { await Task.yield() }
  first.cancel()
  do {
    try await first.value
    Issue.record("Canceled provider read accepted")
  } catch is CancellationError {}
  await coordinator.finishRestoration()  // Provider is deliberately still blocked.
  #expect(board.text == "original" && board.restores == 0)
  var secondDone = false
  let second = Task { @MainActor in
    try await coordinator.withPaste("second", restoreAfterOperation: true) { ownership in
      try ownership()
      secondDone = true
    }
  }
  for _ in 0..<20 { await Task.yield() }
  #expect(await provider.pending() == 1)  // Reuses the single still-running provider.
  await provider.advance()
  try await second.value
  #expect(secondDone && board.text == "original" && board.restores == 1)
}

@Test @MainActor func nonrespondingProviderTimesOutBeforeAnyClipboardWrite() async throws {
  let board = FakeClipboard()
  let provider = ClipboardClock()
  let coordinator = ClipboardCoordinator(
    read: {
      await provider.sleep()
      return await board.snapshot()
    }, count: { board.count }, writeText: { board.write($0) }, restore: { board.restore($0) },
    preservationTimeout: .milliseconds(10))
  do {
    try await coordinator.withPaste("dictation") { _ in Issue.record("Timed out provider pasted") }
    Issue.record("Provider never timed out")
  } catch let failure as DictationError {
    #expect(failure.localizedDescription.contains("provider"))
  }
  await coordinator.finishRestoration()
  #expect(board.text == "original" && board.count == 0)
  while await provider.pending() == 0 { await Task.yield() }
  await provider.advance()
}
