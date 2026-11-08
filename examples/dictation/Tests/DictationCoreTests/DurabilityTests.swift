// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

private actor Signal {
  private var sent = false
  private var waiting: [CheckedContinuation<Void, Never>] = []
  func send() {
    sent = true
    let pending = waiting
    waiting = []
    pending.forEach { $0.resume() }
  }
  func wait() async { if !sent { await withCheckedContinuation { waiting.append($0) } } }
}
private func directory() throws -> URL {
  let path = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  try FileManager.default.createDirectory(at: path, withIntermediateDirectories: true)
  var config = Configuration()
  config.retainHistory = true
  config.retainAudio = true
  try JSONEncoder().encode(config).write(to: path.appendingPathComponent("settings.json"))
  return path
}
private func entry(_ text: String) -> HistoryEntry {
  HistoryEntry(
    preset: "p", model: "speech", application: "", raw: text, text: text, status: "ready",
    destination: .init(server: "https://fixture.example", table: "dictation"))
}
private actor Writes {
  var calls = 0
  var failNewer: Bool
  let began = Signal()
  let release = Signal()
  init(failNewer: Bool) { self.failNewer = failNewer }
  func beforeWrite() async throws {
    calls += 1
    let step = calls
    if step == 1 {
      await began.send()
      await release.wait()
    }
    if step == 2 && failNewer { throw DictationError.invalid("Storage unavailable") }
  }
}
@Test(arguments: [false, true]) func archiveSupersessionRequiresAnActualCommit(failNewer: Bool)
  async throws
{
  let path = try directory()
  defer { try? FileManager.default.removeItem(at: path) }
  let file = path.appendingPathComponent("history.json")
  let writes = Writes(failNewer: failNewer)
  let archive = HistoryArchive(file: file, beforeWrite: { try await writes.beforeWrite() })
  let old = entry("cleanup")
  let newer = entry("newer")
  let first = Task { try await archive.store([old], revision: 1) }
  await writes.began.wait()
  do { try await archive.store([newer], revision: 2) } catch {
    #expect(failNewer)
  }
  await writes.release.send()
  try await first.value
  let saved = try JSONDecoder().decode([HistoryEntry].self, from: Data(contentsOf: file))
  #expect(saved == [failNewer ? old : newer])
}

private final class SuccessProtocol: URLProtocol, @unchecked Sendable {
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    let response = HTTPURLResponse(
      url: request.url!, statusCode: 200, httpVersion: nil,
      headerFields: ["Content-Type": "application/json"])!
    client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
    client?.urlProtocol(self, didLoad: Data("{}".utf8))
    client?.urlProtocolDidFinishLoading(self)
  }
  override func stopLoading() {}
}
private actor OrderedWrites {
  var calls = 0
  let cleanupBegan = Signal()
  let cleanupRelease = Signal()
  func beforeWrite() async throws {
    calls += 1
    let step = calls
    if step == 2 {
      await cleanupBegan.send()
      await cleanupRelease.wait()
    }
    if step == 3 { throw DictationError.invalid("New snapshot failed") }
  }
}
@Test @MainActor func deletionCleanupCommitsBeforeAQueuedFailingSnapshot() async throws {
  let path = try directory()
  defer { try? FileManager.default.removeItem(at: path) }
  let old = entry("old")
  let newer = entry("newer")
  let file = path.appendingPathComponent("history.json")
  try JSONEncoder().encode([old]).write(to: file)
  let writes = OrderedWrites()
  let options = URLSessionConfiguration.ephemeral
  options.protocolClasses = [SuccessProtocol.self]
  let session = URLSession(configuration: options)
  let store = Store(
    directory: path, registerHotkey: false, historyUpload: { _, _ in },
    historyArchiveBeforeWrite: { try await writes.beforeWrite() },
    clientFactory: { try AntflyClient(configuration: $0, session: session) },
    credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  await store.historyPersistenceTask?.value
  let deletion = Task { try await store.deleteRetainedHistory() }
  await writes.cleanupBegan.wait()
  let producer = Task { await store.recordHistory(newer, snapshot: store.config) }
  for _ in 0..<50 { await Task.yield() }
  #expect(await writes.calls == 2)
  #expect(store.pendingHistoryDeletions == 1)
  #expect(
    FileManager.default.fileExists(
      atPath: path.appendingPathComponent("history-deletions.json").path))
  await writes.cleanupRelease.send()
  try await deletion.value
  await producer.value
  let saved = try JSONDecoder().decode([HistoryEntry].self, from: Data(contentsOf: file))
  #expect(!saved.contains { $0.id == old.id })
  #expect(store.pendingHistoryDeletions == 0)
}

private actor FailCleanup {
  var calls = 0
  func beforeWrite() throws {
    calls += 1
    if calls == 2 { throw DictationError.invalid("Cleanup failed") }
  }
}
@Test @MainActor func failedLocalDeletionCommitRetainsItsJournalUntilRestartRetry() async throws {
  let path = try directory()
  defer { try? FileManager.default.removeItem(at: path) }
  let old = entry("old")
  let file = path.appendingPathComponent("history.json")
  try JSONEncoder().encode([old]).write(to: file)
  let writes = FailCleanup()
  let options = URLSessionConfiguration.ephemeral
  options.protocolClasses = [SuccessProtocol.self]
  let session = URLSession(configuration: options)
  let store = Store(
    directory: path, registerHotkey: false, historyUpload: { _, _ in },
    historyArchiveBeforeWrite: { try await writes.beforeWrite() },
    clientFactory: { try AntflyClient(configuration: $0, session: session) },
    credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  await store.historyPersistenceTask?.value
  do {
    try await store.deleteRetainedHistory()
    Issue.record("Reported an uncommitted deletion")
  } catch is DictationError {}
  #expect(store.pendingHistoryDeletions == 1)
  let journal = try HistoryDeletionJournal(
    file: path.appendingPathComponent("history-deletions.json"))
  #expect(journal.recoveredIDs == [old.id])
  #expect(await journal.pending().isEmpty)  // Remote deletion already acknowledged.
  let restarted = Store(
    directory: path, registerHotkey: false,
    historyUpload: { _, _ in Issue.record("Deleted entry uploaded") },
    clientFactory: { try AntflyClient(configuration: $0, session: session) },
    credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  await restarted.historyPersistenceTask?.value
  try await restarted.deleteRetainedHistory(retryOnly: true)
  #expect(restarted.pendingHistoryDeletions == 0)
  #expect(try JSONDecoder().decode([HistoryEntry].self, from: Data(contentsOf: file)).isEmpty)
}

private actor SlowAudio {
  var active = 0
  var maximum = 0
  var clips: [UInt8] = []
  let began = Signal()
  let release = Signal()
  func stage(_ data: Data, directory: URL) async throws -> URL {
    active += 1
    maximum = max(maximum, active)
    clips.append(data.first!)
    await began.send()
    await release.wait()  // A kernel write cannot observe Task cancellation.
    active -= 1
    let file = directory.appendingPathComponent(".recording-" + UUID().uuidString + ".wav")
    try data.write(to: file)
    return file
  }
}
@Test @MainActor func slowAudioWriterKeepsOnlyOneActiveAndTheNewestPendingClip() async throws {
  let path = try directory()
  defer { try? FileManager.default.removeItem(at: path) }
  let audio = SlowAudio()
  let store = Store(
    directory: path, registerHotkey: false,
    audioArchiveStage: { try await audio.stage($0, directory: $1) },
    credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  store.archiveAudio(Task { Data([1]) }, run: store.generation)
  await audio.began.wait()
  for index in 2...20 {
    store.cancel()
    store.archiveAudio(Task { Data([UInt8(index)]) }, run: store.generation)
    await Task.yield()
  }
  #expect(await audio.clips == [1])
  #expect(await audio.maximum == 1)
  await audio.release.send()
  await store.audioArchiveTask?.value
  #expect(await audio.clips == [1, 20])
  #expect(await audio.maximum == 1)
  #expect(try Data(contentsOf: path.appendingPathComponent("last-recording.wav")) == Data([20]))
  await store.prepareToQuit()
}
@Test @MainActor func cancelingPendingProcessingPreservesTheOlderCompletedArchive() async throws {
  let path = try directory()
  defer { try? FileManager.default.removeItem(at: path) }
  let audio = SlowAudio()
  let store = Store(
    directory: path, registerHotkey: false,
    audioArchiveStage: { try await audio.stage($0, directory: $1) },
    credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  store.archiveAudio(Task { Data([1]) }, run: store.generation)
  await audio.began.wait()
  store.cancel()
  store.archiveAudio(Task { Data([2]) }, run: store.generation)
  store.state = .processing
  store.cancel()
  await audio.release.send()
  await store.audioArchiveTask?.value
  #expect(await audio.clips == [1])
  #expect(try Data(contentsOf: path.appendingPathComponent("last-recording.wav")) == Data([1]))
}

@Test(arguments: [false, true], [false, true]) @MainActor
func repeatedCancellationCannotResurrectAnAudioArchive(existingArchive: Bool, quit: Bool)
  async throws
{
  let path = try directory()
  defer { try? FileManager.default.removeItem(at: path) }
  let file = path.appendingPathComponent("last-recording.wav")
  if existingArchive { try Data([99]).write(to: file) }
  let audio = SlowAudio()
  let store = Store(
    directory: path, registerHotkey: false,
    audioArchiveStage: { try await audio.stage($0, directory: $1) },
    credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  store.archiveAudio(Task { Data([1]) }, run: store.generation)
  await audio.began.wait()
  store.state = .processing
  store.cancel()
  for clip in UInt8(2)...3 {
    store.archiveAudio(Task { Data([clip]) }, run: store.generation)
    store.state = .processing
    store.cancel()
  }
  await audio.release.send()
  if quit { await store.prepareToQuit() } else { await store.audioArchiveTask?.value }
  #expect(await audio.clips == [1])
  #expect(await audio.maximum == 1)
  if existingArchive {
    #expect(try Data(contentsOf: file) == Data([99]))
  } else {
    #expect(!FileManager.default.fileExists(atPath: file.path))
  }
  #expect(
    try FileManager.default.contentsOfDirectory(atPath: path.path).allSatisfy {
      !$0.hasPrefix(".recording-")
    })
}

@Test @MainActor func canceledEncodingCannotRegainArchiveEligibility() async throws {
  let path = try directory()
  defer { try? FileManager.default.removeItem(at: path) }
  let began = Signal()
  let release = Signal()
  let store = Store(
    directory: path, registerHotkey: false,
    audioArchiveStage: { _, _ in
      Issue.record("Canceled encoding reached disk staging")
      throw CancellationError()
    }, credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  store.archiveAudio(
    Task {
      await began.send()
      await release.wait()
      return Data([1])
    }, run: store.generation)
  await began.wait()
  // Let the archive worker acquire the encoding before replacing its request.
  for _ in 0..<50 { await Task.yield() }
  store.state = .processing
  store.cancel()
  store.archiveAudio(Task { Data([2]) }, run: store.generation)
  store.state = .processing
  store.cancel()
  await release.send()
  await store.prepareToQuit()
  #expect(
    !FileManager.default.fileExists(atPath: path.appendingPathComponent("last-recording.wav").path))
}
