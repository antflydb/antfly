// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

actor HistoryProbe {
  var destinations: [HistoryDestination] = []
  var uploads: [String] = []
  var blocked = true
  var wasCanceled = false
  var pending: [CheckedContinuation<Void, Never>] = []
  func upload(_ entry: HistoryEntry, to destination: HistoryDestination) async {
    destinations.append(destination)
    uploads.append(entry.id)
    if blocked { await withCheckedContinuation { pending.append($0) } }
    wasCanceled = Task.isCancelled
  }
  func release() {
    blocked = false
    let waiting = pending
    pending = []
    for continuation in waiting { continuation.resume() }
  }
}
private func historyEntry() -> HistoryEntry {
  HistoryEntry(
    preset: "Slack", model: "transcribers/speech", application: "test", raw: "raw", text: "Clean.",
    status: "ready")
}
private func testDirectory() throws -> URL {
  let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
  return directory
}

@Test func historyOutboxEnqueueDoesNotWaitForNetworkAndUsesOneWorker() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let probe = HistoryProbe()
  let destination = HistoryDestination(server: "https://first.example", table: "dictation")
  let queue = try HistoryOutbox(
    directory: directory.appendingPathComponent("outbox"),
    upload: { await probe.upload($0, to: $1) })
  try await queue.enqueue(historyEntry(), destination: destination)
  try await queue.enqueue(historyEntry(), destination: destination)
  let status = await queue.status()
  #expect(status.pending == 2)
  let count = await probe.uploads.count
  #expect(count <= 1)
  await probe.release()
  await queue.waitUntilIdle()
  let uploaded = await probe.uploads
  #expect(uploaded.count == 2)
  let targets = await probe.destinations
  #expect(targets == [destination, destination])
}

@Test func historyOutboxSurvivesRestartAndRetainsOriginalDestination() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let outbox = directory.appendingPathComponent("outbox")
  let original = HistoryDestination(server: "https://original.example", table: "original")
  let first = try HistoryOutbox(
    directory: outbox, paused: true,
    upload: { _, _ in Issue.record("Paused queue dispatched work") })
  let entry = historyEntry()
  try await first.enqueue(entry, destination: original)
  let probe = HistoryProbe()
  await probe.release()
  let restarted = try HistoryOutbox(
    directory: outbox, paused: true, upload: { await probe.upload($0, to: $1) })
  await restarted.resume()
  await restarted.waitUntilIdle()
  let targets = await probe.destinations
  #expect(targets == [original])
  let uploaded = await probe.uploads
  #expect(uploaded == [entry.id])
  #expect(try FileManager.default.contentsOfDirectory(atPath: outbox.path).isEmpty)
}

actor FailureProbe {
  var attempts = 0
  func upload() throws {
    attempts += 1
    if attempts == 1 { throw DictationError.invalid("temporary outage") }
  }
}
@Test func historyOutboxRetriesAndFencesDelayedArchiveWrites() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let probe = FailureProbe()
  let queue = try HistoryOutbox(
    directory: directory.appendingPathComponent("outbox"),
    upload: { _, _ in try await probe.upload() }, sleep: { _ in })
  let entry = historyEntry()
  try await queue.enqueue(
    entry, destination: HistoryDestination(server: "https://example.com", table: "dictation"))
  await queue.waitUntilIdle()
  let attempts = await probe.attempts
  #expect(attempts == 2)
  try await queue.storeArchive([], revision: 2)
  try await queue.storeArchive([entry], revision: 1)
  let saved = try JSONDecoder().decode(
    [HistoryEntry].self, from: Data(contentsOf: directory.appendingPathComponent("history.json")))
  #expect(saved.isEmpty)
}

@Test func deletingPendingHistoryDoesNotResurrectItAfterRestart() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let outbox = directory.appendingPathComponent("outbox")
  let queue = try HistoryOutbox(
    directory: outbox, paused: true, upload: { _, _ in Issue.record("Deleted job uploaded") })
  let entry = historyEntry()
  try await queue.enqueue(
    entry, destination: HistoryDestination(server: "https://example.com", table: "dictation"))
  try await queue.discard(ids: [entry.id])
  let restarted = try HistoryOutbox(
    directory: outbox, paused: true, upload: { _, _ in Issue.record("Deleted job restored") })
  let status = await restarted.status()
  #expect(status.pending == 0)
}

final class StoreProtocol: URLProtocol, @unchecked Sendable {
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    let response = HTTPURLResponse(
      url: request.url!, statusCode: 200, httpVersion: "HTTP/1.1",
      headerFields: ["Content-Type": "text/event-stream"])!
    client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
    let body =
      "data: {\"type\":\"dictation.transcript\",\"transcript\":{\"text\":\"raw\"}}\n\ndata: {\"type\":\"dictation.completed\",\"text\":\"Clean.\"}\n\ndata: [DONE]\n\n"
    client?.urlProtocol(self, didLoad: Data(body.utf8))
    client?.urlProtocolDidFinishLoading(self)
  }
  override func stopLoading() {}
}

@Test @MainActor func completedDictationBecomesIdleWhileHistoryUploadIsBlocked() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let settings = URLSessionConfiguration.ephemeral
  settings.protocolClasses = [StoreProtocol.self]
  let connection = try AntflyClient(
    server: "https://example.com", session: URLSession(configuration: settings))
  let probe = HistoryProbe()
  var initial = Configuration()
  initial.retainHistory = true
  initial.autoPaste = false
  try JSONEncoder().encode(initial).write(to: directory.appendingPathComponent("settings.json"))
  let store = Store(
    directory: directory, registerHotkey: false, historyUpload: { await probe.upload($0, to: $1) },
    clientFactory: { _ in connection })
  store.config.retainHistory = true
  store.config.autoPaste = false
  store.submit(
    audio: Data([1]), preset: store.config.presets[0], snapshot: store.config, destination: nil)
  await store.requestTask?.value
  #expect(store.state == .idle)
  #expect(store.result == "Clean.")
  #expect(store.history.count == 1)
  await store.historyPersistenceTask?.value
  while await probe.uploads.isEmpty { await Task.yield() }
  let dispatched = await probe.uploads.count
  #expect(dispatched == 1)
  await probe.release()
  await store.outbox?.waitUntilIdle()
}

@Test func enablingSyncCannotBypassAnExplicitDeletionBarrier() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let queue = try HistoryOutbox(
    directory: directory.appendingPathComponent("outbox"), paused: true,
    upload: { _, _ in Issue.record("Deletion barrier bypassed") })
  let token = await queue.pause()
  await queue.setEnabled(true)
  let entry = historyEntry()
  try await queue.enqueue(
    entry, destination: HistoryDestination(server: "https://example.com", table: "dictation"))
  try await queue.discard(ids: [entry.id])
  await queue.resume(token)
  await queue.waitUntilIdle()
}

@Suite(.serialized) struct HistoryDestinationTests {
  @Test @MainActor func deleteFollowsOriginalDestinationAndCancelsEveryQueuedCopy() async throws {
    let directory = try testDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    HistoryDeleteProtocol.destinations = []
    HistoryDeleteProtocol.failure = false
    let options = URLSessionConfiguration.ephemeral
    options.protocolClasses = [HistoryDeleteProtocol.self]
    let session = URLSession(configuration: options)
    let store = Store(
      directory: directory, registerHotkey: false,
      historyUpload: { _, _ in Issue.record("Deleted history uploaded") },
      clientFactory: { try AntflyClient(configuration: $0, session: session) },
      credentialReader: { _ in "" }, credentialWriter: { _, _ in })
    store.config.retainHistory = true
    store.config.connectionMode = .external
    store.config.server = "https://original.example"
    await store.recordHistory(historyEntry(), snapshot: store.config)
    let id = store.history[0].id
    try await store.outbox?.enqueue(
      store.history[0],
      destination: HistoryDestination(server: "https://copy.example", table: "second"))
    store.config.server = "https://new.example"
    store.config.table = "new"
    try await store.deleteRetainedHistory()
    #expect(store.history.isEmpty)
    #expect(
      Set(HistoryDeleteProtocol.destinations) == [
        "https://original.example/db/v1/tables/dictation/batch",
        "https://copy.example/db/v1/tables/second/batch",
      ])
    #expect(await store.outbox?.status().pending == 0)
    let restarted = try HistoryOutbox(
      directory: directory.appendingPathComponent("outbox"), paused: true,
      upload: { _, _ in Issue.record("Deleted job resurrected") })
    #expect(await restarted.destinations(for: [id]).isEmpty == true)
  }
  @Test @MainActor func failedRemoteDeletionKeepsLocalRecoveryButCannotResumeUploads() async throws
  {
    let directory = try testDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    HistoryDeleteProtocol.failure = true
    defer { HistoryDeleteProtocol.failure = false }
    let options = URLSessionConfiguration.ephemeral
    options.protocolClasses = [HistoryDeleteProtocol.self]
    let session = URLSession(configuration: options)
    let store = Store(
      directory: directory, registerHotkey: false,
      historyUpload: { _, _ in Issue.record("Deletion failure resumed an upload") },
      clientFactory: { try AntflyClient(configuration: $0, session: session) },
      credentialReader: { _ in "" }, credentialWriter: { _, _ in })
    store.config.retainHistory = true
    await store.recordHistory(historyEntry(), snapshot: store.config)
    do {
      try await store.deleteRetainedHistory()
      Issue.record("Expected remote failure")
    } catch is ServerFailure {}
    #expect(store.history.count == 1)
    #expect(await store.outbox?.status().pending == 0)
    await store.outbox?.resume()
    await store.outbox?.waitUntilIdle()
  }
  @Test @MainActor func damagedOutboxRetainsLocalHistoryAndFailsRemoteDeletionClosed() async throws
  {
    let directory = try testDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let queueDirectory = directory.appendingPathComponent("outbox")
    var configuration = Configuration()
    configuration.retainHistory = true
    try JSONEncoder().encode(configuration).write(
      to: directory.appendingPathComponent("settings.json"))
    let pending = try HistoryOutbox(
      directory: queueDirectory, paused: true,
      upload: { _, _ in Issue.record("Paused queue dispatched") })
    try await pending.enqueue(
      historyEntry(), destination: .init(server: "https://original.example", table: "dictation"))
    try Data(count: 8 * 1024 * 1024 + 1).write(
      to: queueDirectory.appendingPathComponent("oversized.json"))
    HistoryDeleteProtocol.failure = false
    let options = URLSessionConfiguration.ephemeral
    options.protocolClasses = [HistoryDeleteProtocol.self]
    let session = URLSession(configuration: options)
    let store = Store(
      directory: directory, registerHotkey: false,
      clientFactory: { try AntflyClient(configuration: $0, session: session) },
      credentialReader: { _ in "" }, credentialWriter: { _, _ in })
    #expect(store.outbox == nil)
    store.config.retainHistory = true
    await store.recordHistory(historyEntry(), snapshot: store.config)
    let file = directory.appendingPathComponent("history.json")
    #expect(try JSONDecoder().decode([HistoryEntry].self, from: Data(contentsOf: file)).count == 1)
    do {
      try await store.deleteRetainedHistory()
      Issue.record("Deletion must not report success while pending jobs cannot be canceled")
    } catch {}
    #expect(try JSONDecoder().decode([HistoryEntry].self, from: Data(contentsOf: file)).count == 1)
    store.config.retainHistory = false
    store.save()
    await store.historySettingsTask?.value
    #expect(!FileManager.default.fileExists(atPath: queueDirectory.path))
    let restarted = Store(directory: directory, registerHotkey: false)
    #expect(await restarted.outbox?.status().pending == 0)
  }

}
private final class HistoryDeleteProtocol: URLProtocol, @unchecked Sendable {
  nonisolated(unsafe) static var destinations: [String] = []
  nonisolated(unsafe) static var failure = false
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    Self.destinations.append(request.url!.absoluteString)
    let response = HTTPURLResponse(
      url: request.url!, statusCode: Self.failure ? 503 : 200,
      httpVersion: nil, headerFields: ["Content-Type": "application/json"])!
    client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
    client?.urlProtocol(self, didLoad: Data("{}".utf8))
    client?.urlProtocolDidFinishLoading(self)
  }
  override func stopLoading() {}
}

@Test @MainActor func legacyArchiveRecoversQueuedDestinationBeforeUploadsResume() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  var config = Configuration()
  config.retainHistory = true
  config.server = "https://new.example"
  try JSONEncoder().encode(config).write(to: directory.appendingPathComponent("settings.json"))
  let entry = historyEntry()
  try JSONEncoder().encode([entry]).write(to: directory.appendingPathComponent("history.json"))
  let original = HistoryDestination(server: "https://original.example", table: "original")
  let queue = try HistoryOutbox(
    directory: directory.appendingPathComponent("outbox"), paused: true,
    upload: { _, _ in Issue.record("Preparation queue dispatched") })
  try await queue.enqueue(entry, destination: original)
  let probe = HistoryProbe()
  await probe.release()
  let store = Store(
    directory: directory, registerHotkey: false, historyUpload: { await probe.upload($0, to: $1) })
  await store.outbox?.waitUntilIdle()
  #expect(store.history.first?.destination == original)
  #expect(await probe.destinations == [original])
  let saved = try JSONDecoder().decode(
    [HistoryEntry].self,
    from: Data(contentsOf: directory.appendingPathComponent("history.json")))
  #expect(saved.first?.destination == original)
}

@Test(.timeLimit(.minutes(1)))
func deletionBarrierWaitsForDispatchedUploadInsteadOfCancelingItsResponse() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let probe = HistoryProbe()
  let queue = try HistoryOutbox(
    directory: directory.appendingPathComponent("outbox"),
    upload: { await probe.upload($0, to: $1) })
  try await queue.enqueue(
    historyEntry(),
    destination: HistoryDestination(server: "https://example.com", table: "dictation"))
  while await probe.uploads.isEmpty { await Task.yield() }
  let barrier = Task { await queue.pause() }
  while await !queue.status().paused { await Task.yield() }
  await probe.release()
  let token = await barrier.value
  #expect(await probe.wasCanceled == false)
  #expect(await queue.status().pending == 0)
  await queue.resume(token)
}

@Test(.timeLimit(.minutes(1))) @MainActor
func manualHistoryRetryDoesNotRetargetOldTranscriptsAfterConnectionChanges() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let probe = HistoryProbe()
  await probe.release()
  let store = Store(
    directory: directory, registerHotkey: false,
    historyUpload: { await probe.upload($0, to: $1) })
  store.config.retainHistory = true
  store.config.connectionMode = .external
  store.config.server = "https://original.example"
  store.config.table = "original"
  await store.recordHistory(historyEntry(), snapshot: store.config)
  store.config.server = "https://new.example"
  store.config.table = "new"
  store.syncHistory()
  while store.historyWorking { await Task.yield() }
  await store.outbox?.waitUntilIdle()
  #expect(
    await probe.destinations == [
      HistoryDestination(server: "https://original.example", table: "original")
    ])
}

@Test func disablingRetentionCannotCancelADispatchedWriteDuringDeletion() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let probe = HistoryProbe()
  let queue = try HistoryOutbox(
    directory: directory.appendingPathComponent("outbox"),
    upload: { await probe.upload($0, to: $1) })
  let entry = historyEntry()
  try await queue.enqueue(
    entry, destination: .init(server: "https://example.com", table: "dictation"))
  while await probe.uploads.isEmpty { await Task.yield() }
  let deletion = Task { await queue.pause() }
  while !(await queue.status()).paused { await Task.yield() }
  let disable = Task { await queue.setEnabled(false) }
  await Task.yield()
  await probe.release()
  let hold = await deletion.value
  await disable.value
  #expect(await probe.wasCanceled == false)
  try await queue.discard(ids: [entry.id])
  await queue.resume(hold)
  #expect((await queue.status()).paused)
}

private actor HistorySignal {
  var sent = false
  var waiters: [CheckedContinuation<Void, Never>] = []
  func send() {
    sent = true
    let pending = waiters
    waiters = []
    pending.forEach { $0.resume() }
  }
  func wait() async { if !sent { await withCheckedContinuation { waiters.append($0) } } }
}

@Test @MainActor func retentionDisableFencesAnActiveManualHistoryUpload() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  var config = Configuration()
  config.retainHistory = true
  try JSONEncoder().encode(config).write(to: directory.appendingPathComponent("settings.json"))
  let probe = HistoryProbe()
  await probe.release()
  let store = Store(
    directory: directory, registerHotkey: false,
    historyUpload: { await probe.upload($0, to: $1) },
    credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  let queue = try #require(store.outbox)
  await store.historyPersistenceTask?.value
  store.history = (0..<200).map { _ in historyEntry() }
  store.syncHistory()
  while await probe.uploads.isEmpty { await Task.yield() }
  store.config.retainHistory = false
  store.save()
  await store.historySettingsTask?.value
  let completedAtDisable = await probe.uploads.count
  while store.historyWorking { await Task.yield() }
  #expect(await probe.uploads.count == completedAtDisable)
  #expect((await queue.status()).paused)
  #expect((await queue.status()).pending == 0)
  #expect(store.history.isEmpty)
}

@Test @MainActor func unreadableSettingsPreserveQueuedHistoryAndAudioUntilExplicitRecovery()
  async throws
{
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let queueDirectory = directory.appendingPathComponent("outbox")
  let queue = try HistoryOutbox(
    directory: queueDirectory, paused: true,
    upload: { _, _ in Issue.record("Untrusted settings resumed uploads") })
  let entry = historyEntry()
  try await queue.enqueue(
    entry, destination: .init(server: "https://original.example", table: "dictation"))
  let historyFile = directory.appendingPathComponent("history.json")
  let audioFile = directory.appendingPathComponent("last-recording.wav")
  let archived = try JSONEncoder().encode([entry])
  try archived.write(to: historyFile)
  try Data([1, 2, 3]).write(to: audioFile)
  try Data("{".utf8).write(to: directory.appendingPathComponent("settings.json"))
  let probe = HistoryProbe()
  await probe.release()
  let store = Store(
    directory: directory, registerHotkey: false,
    historyUpload: { await probe.upload($0, to: $1) },
    credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  #expect(store.settingsLoadError != nil)
  #expect(await store.outbox?.status().pending == 1)
  #expect(await store.outbox?.status().paused == true)
  #expect(try Data(contentsOf: historyFile) == archived)
  #expect(try Data(contentsOf: audioFile) == Data([1, 2, 3]))
  store.config.retainHistory = true
  store.config.retainAudio = true
  store.save()
  await store.historySettingsTask?.value
  await store.outbox?.waitUntilIdle()
  #expect(store.settingsLoadError == nil && store.history.map(\.id) == [entry.id])
  #expect(await probe.uploads == [entry.id])
}

@Test @MainActor func historyDiskWriteDoesNotGateResultOrNextRecording() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  var config = Configuration()
  config.retainHistory = true
  config.autoPaste = false
  try JSONEncoder().encode(config).write(to: directory.appendingPathComponent("settings.json"))
  let began = HistorySignal()
  let release = HistorySignal()
  let settings = URLSessionConfiguration.ephemeral
  settings.protocolClasses = [StoreProtocol.self]
  let connection = try AntflyClient(
    server: "https://fixture.example", session: URLSession(configuration: settings))
  let store = Store(
    directory: directory, registerHotkey: false, historyUpload: { _, _ in },
    historyArchiveBeforeWrite: {
      await began.send()
      await release.wait()
    },
    clientFactory: { _ in connection }, credentialReader: { _ in "" }, credentialWriter: { _, _ in }
  )
  await store.historyPersistenceTask?.value
  store.submit(audio: Data([1]), preset: config.presets[0], snapshot: config, destination: nil)
  await store.requestTask?.value
  await began.wait()
  #expect(store.result == "Clean." && store.state == .idle && store.phase == .ready)
  store.config.retainHistory = false
  store.save()
  await release.send()
  await store.historySettingsTask?.value
  #expect(await store.outbox?.status().pending == 0)
  let saved = try JSONDecoder().decode(
    [HistoryEntry].self, from: Data(contentsOf: directory.appendingPathComponent("history.json")))
  #expect(saved.isEmpty)
}

@Test func historyArchiveBoundsBytesAndCountBeforeEncoding() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  var large = historyEntry()
  large.raw = String(repeating: "\u{0001}", count: 65536)
  large.text = String(repeating: "t", count: 65536)
  let entries = Array(repeating: large, count: 500)
  let bounded = HistoryArchive.bounded(entries)
  #expect(!bounded.isEmpty && bounded.count < 500)
  let file = directory.appendingPathComponent("history.json")
  let archive = HistoryArchive(file: file)
  try await archive.store(entries, revision: 1)
  let data = try Data(contentsOf: file)
  #expect(data.count <= HistoryArchive.maxBytes)
  #expect(try JSONDecoder().decode([HistoryEntry].self, from: data) == bounded)
}

@Test @MainActor func missingSettingsCannotTurnShortcutSetupIntoRetentionDeletion() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let queueDirectory = directory.appendingPathComponent("outbox")
  let queue = try HistoryOutbox(directory: queueDirectory, paused: true, upload: { _, _ in })
  let entry = historyEntry()
  try await queue.enqueue(
    entry, destination: .init(server: "https://original.example", table: "dictation"))
  let store = Store(
    directory: directory, registerHotkey: false,
    credentialReader: { _ in "" }, credentialWriter: { _, _ in }, keyboardAccess: { true })
  #expect(store.settingsLoadError != nil)
  store.setUpShortcut()
  #expect(store.shortcutSetupStatus.contains("Settings are missing"))
  #expect(
    !FileManager.default.fileExists(atPath: directory.appendingPathComponent("settings.json").path))
  #expect(await store.outbox?.status().pending == 1)
  #expect(await store.outbox?.status().paused == true)
}

@Test @MainActor func anEmptyFirstLaunchOutboxDoesNotRequireSettingsRecovery() throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let first = Store(directory: directory, registerHotkey: false)
  #expect(first.settingsLoadError == nil)
  let restarted = Store(directory: directory, registerHotkey: false)
  #expect(restarted.settingsLoadError == nil)
}

@Test @MainActor func normalQuitFinishesLocalHistoryWithoutWaitingForRemoteUpload() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  var config = Configuration()
  config.retainHistory = true
  config.autoPaste = false
  try JSONEncoder().encode(config).write(to: directory.appendingPathComponent("settings.json"))
  let began = HistorySignal()
  let release = HistorySignal()
  let probe = HistoryProbe()
  let options = URLSessionConfiguration.ephemeral
  options.protocolClasses = [StoreProtocol.self]
  let connection = try AntflyClient(
    server: "https://fixture.example", session: URLSession(configuration: options))
  let store = Store(
    directory: directory, registerHotkey: false,
    historyUpload: { await probe.upload($0, to: $1) },
    historyArchiveBeforeWrite: {
      await began.send()
      await release.wait()
    },
    clientFactory: { _ in connection }, credentialReader: { _ in "" }, credentialWriter: { _, _ in }
  )
  await store.historyPersistenceTask?.value
  store.submit(audio: Data([1]), preset: config.presets[0], snapshot: config, destination: nil)
  await store.requestTask?.value
  await began.wait()
  var quitReady = false
  let quit = Task {
    await store.prepareToQuit()
    quitReady = true
  }
  await Task.yield()
  #expect(!quitReady)
  await release.send()
  await quit.value
  let saved = try JSONDecoder().decode(
    [HistoryEntry].self, from: Data(contentsOf: directory.appendingPathComponent("history.json")))
  #expect(saved.count == 1 && saved[0].text == "Clean.")
  #expect(await store.outbox?.status().pending == 1)
  await probe.release()
  await store.outbox?.waitUntilIdle()
}

@Test @MainActor func suspendedStorageBoundsPendingWorkAndCoalescesTheLatestArchive() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  var config = Configuration()
  config.retainHistory = true
  try JSONEncoder().encode(config).write(to: directory.appendingPathComponent("settings.json"))
  let began = HistorySignal()
  let release = HistorySignal()
  let store = Store(
    directory: directory, registerHotkey: false, historyUpload: { _, _ in },
    historyArchiveBeforeWrite: {
      await began.send()
      await release.wait()
    },
    credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  await store.historyPersistenceTask?.value
  let tasks = (0..<100).map { _ in
    var entry = historyEntry()
    entry.raw = String(repeating: "r", count: 65536)
    entry.text = String(repeating: "t", count: 65536)
    return Task { await store.recordHistory(entry, snapshot: config) }
  }
  await began.wait()
  while store.pendingHistoryEntries < 80 { await Task.yield() }
  for _ in 0..<100 { await Task.yield() }
  #expect(store.pendingHistoryEntries < 100)
  await release.send()
  for task in tasks { await task.value }
  await store.historyPersistenceTask?.value
  await store.outbox?.waitUntilIdle()
  #expect(store.pendingHistoryEntries == 0)
  #expect(store.historySyncStatus.contains("backlog is full"))
  let file = directory.appendingPathComponent("history.json")
  let data = try Data(contentsOf: file)
  #expect(data.count <= HistoryArchive.maxBytes)
  let saved = try JSONDecoder().decode([HistoryEntry].self, from: data)
  #expect(saved.map(\.id) == store.history.map(\.id))
}

private final class DeletionRetryProtocol: URLProtocol, @unchecked Sendable {
  nonisolated(unsafe) static var failB = true
  nonisolated(unsafe) static var hosts: [String] = []
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    let host = request.url!.host!
    Self.hosts.append(host)
    let response = HTTPURLResponse(
      url: request.url!, statusCode: host == "b.example" && Self.failB ? 503 : 200,
      httpVersion: nil, headerFields: ["Content-Type": "application/json"])!
    client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
    client?.urlProtocol(self, didLoad: Data("{}".utf8))
    client?.urlProtocolDidFinishLoading(self)
  }
  override func stopLoading() {}
}
@Suite(.serialized) struct DurableDeletionTests {
  @Test @MainActor func retryAfterRestartAndLocalClearStillDeletesEveryOriginalDestination()
    async throws
  {
    let directory = try testDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    var config = Configuration()
    config.retainHistory = true
    try JSONEncoder().encode(config).write(to: directory.appendingPathComponent("settings.json"))
    let entry = historyEntry()
    try JSONEncoder().encode([entry]).write(to: directory.appendingPathComponent("history.json"))
    let pending = try HistoryOutbox(
      directory: directory.appendingPathComponent("outbox"), paused: true, upload: { _, _ in })
    for host in ["a", "b"] {
      try await pending.enqueue(
        entry, destination: .init(server: "https://\(host).example", table: "dictation"))
    }
    let began = HistorySignal()
    let release = HistorySignal()
    let options = URLSessionConfiguration.ephemeral
    options.protocolClasses = [DeletionRetryProtocol.self]
    let session = URLSession(configuration: options)
    DeletionRetryProtocol.failB = true
    DeletionRetryProtocol.hosts = []
    let store = Store(
      directory: directory, registerHotkey: false,
      historyUpload: { _, _ in Issue.record("Deletion dispatched a canceled upload") },
      historyArchiveBeforeWrite: {
        await began.send()
        await release.wait()
      },
      clientFactory: { try AntflyClient(configuration: $0, session: session) },
      credentialReader: { _ in "" }, credentialWriter: { _, _ in })
    await began.wait()
    let queue = try #require(store.outbox)
    let hold = await queue.pause()
    await release.send()
    await store.historyPersistenceTask?.value
    do {
      try await store.deleteRetainedHistory()
      Issue.record("Expected partial failure")
    } catch is ServerFailure {}
    #expect(store.history.count == 1 && store.pendingHistoryDeletions == 1)
    #expect(await queue.status().pending == 0)
    // Dropping local transcripts must not discard the separate deletion intent.
    store.clearLocalHistory()
    await store.prepareToQuit()
    DeletionRetryProtocol.failB = false
    let restarted = Store(
      directory: directory, registerHotkey: false,
      historyUpload: { uploaded, _ in #expect(uploaded.id != entry.id) },
      clientFactory: { try AntflyClient(configuration: $0, session: session) },
      credentialReader: { _ in "" }, credentialWriter: { _, _ in })
    await restarted.historyPersistenceTask?.value
    #expect(restarted.history.isEmpty && restarted.pendingHistoryDeletions == 1)
    let newer = historyEntry()
    await restarted.recordHistory(newer, snapshot: config)
    await restarted.outbox?.waitUntilIdle()
    try await restarted.deleteRetainedHistory(retryOnly: true)
    #expect(restarted.history.map(\.id) == [newer.id])
    #expect(DeletionRetryProtocol.hosts.filter { $0 == "b.example" }.count == 2)
    #expect(restarted.pendingHistoryDeletions == 0)
    #expect(
      !FileManager.default.fileExists(
        atPath: directory.appendingPathComponent("history-deletions.json").path))
    await queue.resume(hold)
  }

  @Test @MainActor func completeDeletionPlanIsDurableBeforeAnActiveUploadFinishes() async throws {
    let directory = try testDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    var config = Configuration()
    config.retainHistory = true
    try JSONEncoder().encode(config).write(to: directory.appendingPathComponent("settings.json"))
    var entry = historyEntry()
    entry.destination = .init(server: "https://a.example", table: "dictation")
    try JSONEncoder().encode([entry]).write(to: directory.appendingPathComponent("history.json"))
    let queue = try HistoryOutbox(
      directory: directory.appendingPathComponent("outbox"), paused: true, upload: { _, _ in })
    let extra = HistoryDestination(server: "https://b.example", table: "dictation")
    try await queue.enqueue(entry, destination: extra)
    let probe = HistoryProbe()
    let options = URLSessionConfiguration.ephemeral
    options.protocolClasses = [DeletionRetryProtocol.self]
    let session = URLSession(configuration: options)
    DeletionRetryProtocol.failB = false
    DeletionRetryProtocol.hosts = []
    let store = Store(
      directory: directory, registerHotkey: false,
      historyUpload: { await probe.upload($0, to: $1) },
      clientFactory: { try AntflyClient(configuration: $0, session: session) },
      credentialReader: { _ in "" }, credentialWriter: { _, _ in })
    while await probe.uploads.isEmpty { await Task.yield() }
    let deletion = Task { try await store.deleteRetainedHistory() }
    while store.pendingHistoryDeletions == 0 { await Task.yield() }
    let recovered = try HistoryDeletionJournal(
      file: directory.appendingPathComponent("history-deletions.json"))
    #expect(await recovered.pending()[extra] == [entry.id])
    #expect(DeletionRetryProtocol.hosts.isEmpty)
    await probe.release()
    try await deletion.value
    #expect(Set(DeletionRetryProtocol.hosts) == ["a.example", "b.example"])
    #expect(store.pendingHistoryDeletions == 0)
    #expect(await probe.wasCanceled == false)
  }
}

@Test @MainActor func restartCancelsJobsWhoseDeletionIntentWasCommittedBeforeQueueDiscard()
  async throws
{
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  var config = Configuration()
  config.retainHistory = true
  try JSONEncoder().encode(config).write(to: directory.appendingPathComponent("settings.json"))
  let entry = historyEntry()
  let destination = HistoryDestination(server: "https://original.example", table: "dictation")
  let queue = try HistoryOutbox(
    directory: directory.appendingPathComponent("outbox"), paused: true, upload: { _, _ in })
  try await queue.enqueue(entry, destination: destination)
  let journal = try HistoryDeletionJournal(
    file: directory.appendingPathComponent("history-deletions.json"))
  try await journal.begin([destination: [entry.id]])
  let store = Store(
    directory: directory, registerHotkey: false,
    historyUpload: { _, _ in Issue.record("Pending deletion was uploaded after restart") },
    credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  await store.historyPersistenceTask?.value
  #expect(await store.outbox?.status().pending == 0)
  #expect(store.pendingHistoryDeletions == 1)
}

@Test @MainActor func corruptDeletionJournalPausesUploadsWithoutDisablingLocalRetention()
  async throws
{
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  var config = Configuration()
  config.retainHistory = true
  try JSONEncoder().encode(config).write(to: directory.appendingPathComponent("settings.json"))
  try Data("{".utf8).write(to: directory.appendingPathComponent("history-deletions.json"))
  let store = Store(
    directory: directory, registerHotkey: false,
    historyUpload: { _, _ in Issue.record("Corrupt journal permitted upload") },
    credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  await store.historyPersistenceTask?.value
  let entry = historyEntry()
  await store.recordHistory(entry, snapshot: config)
  #expect(store.history.map(\.id) == [entry.id])
  #expect(await store.outbox?.status().paused == true)
  let archived = try JSONDecoder().decode(
    [HistoryEntry].self, from: Data(contentsOf: directory.appendingPathComponent("history.json")))
  #expect(archived.map(\.id) == [entry.id])
  do {
    try await store.deleteRetainedHistory()
    Issue.record("Corrupt journal allowed deletion")
  } catch is DictationError {}
}

@Test @MainActor func normalQuitWaitsForExplicitLocalHistoryClear() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  var config = Configuration()
  config.retainHistory = true
  try JSONEncoder().encode(config).write(to: directory.appendingPathComponent("settings.json"))
  var entry = historyEntry()
  entry.destination = .init(server: config.server, table: config.table)
  let file = directory.appendingPathComponent("history.json")
  try JSONEncoder().encode([entry]).write(to: file)
  let began = HistorySignal()
  let release = HistorySignal()
  let store = Store(
    directory: directory, registerHotkey: false,
    historyArchiveBeforeWrite: {
      await began.send()
      await release.wait()
    },
    credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  await store.historyPersistenceTask?.value
  store.clearLocalHistory()
  await began.wait()
  var quitReady = false
  let quit = Task {
    await store.prepareToQuit()
    quitReady = true
  }
  await Task.yield()
  #expect(!quitReady)
  await release.send()
  await quit.value
  #expect(try JSONDecoder().decode([HistoryEntry].self, from: Data(contentsOf: file)).isEmpty)
}

@Test func deletionBarrierPreservesDestinationOfAnUploadThatFinishesWhilePausing() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let probe = HistoryProbe()
  let queue = try HistoryOutbox(
    directory: directory.appendingPathComponent("outbox"),
    upload: { await probe.upload($0, to: $1) })
  let entry = historyEntry()
  let destination = HistoryDestination(server: "https://extra.example", table: "dictation")
  try await queue.enqueue(entry, destination: destination)
  while await probe.uploads.isEmpty { await Task.yield() }
  let barrier = Task { await queue.pauseForDeletion(ids: [entry.id]) }
  while !(await queue.status()).paused { await Task.yield() }
  await probe.release()
  let held = await barrier.value
  #expect(held.destinations == [entry.id: [destination]])
  #expect(await probe.wasCanceled == false)
  await queue.resume(held.token)
}

@Test func discardingAnAlreadyUnlinkedJobIsIdempotent() async throws {
  let directory = try testDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let queue = try HistoryOutbox(directory: directory, paused: true, upload: { _, _ in })
  let entry = historyEntry()
  try await queue.enqueue(
    entry, destination: .init(server: "https://original.example", table: "dictation"))
  let file = try #require(
    FileManager.default.contentsOfDirectory(at: directory, includingPropertiesForKeys: nil).first)
  try FileManager.default.removeItem(at: file)
  try await queue.discard(ids: [entry.id])
  #expect(await queue.status().pending == 0)
}
