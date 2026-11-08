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
