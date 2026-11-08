// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

@MainActor private final class TestShortcut: ShortcutManaging {
  var changed: ((Bool) -> Void)?
  var interrupted: (() -> Void)?
  var isAvailable = false
  var registrations: [KeyboardShortcut] = []
  var reject = false
  func register(_ shortcut: KeyboardShortcut) throws {
    if reject { throw DictationError.invalid("Accessibility denied") }
    if isAvailable && registrations.last == shortcut {
      throw DictationError.invalid("Duplicate Carbon registration")
    }
    registrations.append(shortcut)
    isAvailable = true
  }
  func unregister() { isAvailable = false }
  func setSuspended(_ value: Bool) {}
}
private func lifecycleDirectory(_ config: Configuration = Configuration()) throws -> URL {
  let file = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  try FileManager.default.createDirectory(at: file, withIntermediateDirectories: true)
  try JSONEncoder().encode(config).write(to: file.appendingPathComponent("settings.json"))
  return file
}
@Test @MainActor func reapplyingActiveCarbonShortcutDoesNotRegisterItTwice() throws {
  var config = Configuration()
  config.effectiveShortcut = .controlOptionSpace
  let directory = try lifecycleDirectory(config)
  defer { try? FileManager.default.removeItem(at: directory) }
  let shortcut = TestShortcut()
  let store = Store(
    directory: directory, registerHotkey: false, shortcutManager: shortcut, keyboardAccess: { true }
  )
  store.setUpShortcut()
  store.setUpShortcut()
  #expect(shortcut.registrations == [.controlOptionSpace])
  #expect(store.shortcutSetupStatus.contains("enabled and saved"))
}
@Test @MainActor func settingsAndCredentialsSaveWhileShortcutPermissionIsDenied() throws {
  let directory = try lifecycleDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let shortcut = TestShortcut()
  shortcut.reject = true
  var credentials: [(String, String)] = []
  let store = Store(
    directory: directory, registerHotkey: false, shortcutManager: shortcut,
    credentialWriter: { credentials.append(($0, $1)) })
  store.config.server = "https://saved.example"
  store.config.dictionary = ["Persisted"]
  store.token = "test-only-secret"
  store.save()
  let data = try Data(contentsOf: directory.appendingPathComponent("settings.json"))
  let saved = try Configuration.decode(data)
  #expect(saved.server == store.config.server && saved.dictionary == ["Persisted"])
  #expect(credentials.count == 1 && credentials[0].0 == store.token)
  #expect(!String(decoding: data, as: UTF8.self).contains(store.token))
  #expect(
    store.message.contains("Settings saved")
      && store.shortcutStatus.contains("Accessibility denied"))
}
private actor LifecycleSignal {
  private var sent = false
  private var waiters: [CheckedContinuation<Void, Never>] = []
  func send() {
    sent = true
    let pending = waiters
    waiters = []
    pending.forEach { $0.resume() }
  }
  func wait() async { if !sent { await withCheckedContinuation { waiters.append($0) } } }
}
@Test(.timeLimit(.minutes(1))) @MainActor func closingOldLiveSessionCannotMutateNewRecording()
  async throws
{
  let directory = try lifecycleDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let began = LifecycleSignal()
  let release = LifecycleSignal()
  let pipeline = LiveDictation(
    incremental: false, needsCleanup: false,
    recognize: { _, _ in [] }, clean: { DictationResult(raw: $0, text: $0) },
    close: {
      await began.send()
      await release.wait()
    })
  let store = Store(directory: directory, registerHotkey: false)
  let run = store.generation
  let failure = Task {
    await store.recoverLiveFailure(
      "old request failed", pipeline: pipeline, run: run, preset: store.config.presets[0])
  }
  await began.wait()
  store.cancel()
  store.state = .recording
  store.phase = .listening
  store.message = "New recording"
  store.processingWarning = ""
  await release.send()
  #expect(await failure.value == false)
  #expect(store.state == .recording && store.phase == .listening)
  #expect(store.message == "New recording" && store.processingWarning.isEmpty)
}
@Test @MainActor func connectionEditsDebounceCredentialReadsAndIgnoreCanceledDiscovery()
  async throws
{
  let directory = try lifecycleDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  var reads: [String] = []
  let settings = URLSessionConfiguration.ephemeral
  settings.protocolClasses = [DiscoveryProtocol.self]
  let connection = try AntflyClient(
    server: "https://example.com", session: URLSession(configuration: settings))
  let store = Store(
    directory: directory, registerHotkey: false,
    clientFactory: { _ in connection },
    credentialReader: {
      reads.append($0)
      return ""
    })
  for host in ["https://o.example", "https://ol.example", "https://old.example"] {
    store.config.server = host
    store.connectionChanged()
  }
  await store.discoveryTask?.value
  #expect(reads == ["https://old.example"])
  #expect(store.models.map(\.id) == ["transcribers/discovered"])
}
private final class DiscoveryProtocol: URLProtocol, @unchecked Sendable {
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    let response = HTTPURLResponse(
      url: request.url!, statusCode: 200, httpVersion: nil,
      headerFields: ["Content-Type": "application/json"])!
    client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
    client?.urlProtocol(
      self, didLoad: Data("{\"data\":[{\"id\":\"transcribers/discovered\"}]}".utf8))
    client?.urlProtocolDidFinishLoading(self)
  }
  override func stopLoading() {}
}

@Test @MainActor func savingBeforeDiscoveryPreservesUnloadedCredentialAndHonorsExplicitClear()
  async throws
{
  let directory = try lifecycleDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  var credentials = ["https://saved.example": "secret"]
  let store = Store(
    directory: directory, registerHotkey: false,
    credentialReader: { credentials[$0] ?? "" },
    credentialWriter: { credentials[$1] = $0 })
  store.config.server = "https://saved.example"
  store.connectionChanged()
  store.save()
  #expect(credentials["https://saved.example"] == "secret")
  store.token = ""
  store.save()
  #expect(credentials["https://saved.example"] == "")
  store.discoveryTask?.cancel()
}

@Test @MainActor func retentionDisableFencesAnArchiveAlreadyWriting() async throws {
  let directory = try lifecycleDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let began = LifecycleSignal()
  let release = LifecycleSignal()
  let store = Store(
    directory: directory, registerHotkey: false,
    audioArchiveStage: { data, directory in
      await began.send()
      await release.wait()
      // Deliberately ignore cancellation to model a completed kernel write.
      let file = directory.appendingPathComponent(".recording-" + UUID().uuidString + ".wav")
      try data.write(to: file)
      return file
    }, credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  store.config.retainAudio = true
  store.archiveAudio(Task { Data([1, 2, 3]) }, run: store.generation)
  let archive = store.audioArchiveTask
  await began.wait()
  #expect(store.state == .idle)
  store.config.retainAudio = false
  store.save()
  await release.send()
  await archive?.value
  #expect(
    !FileManager.default.fileExists(
      atPath: directory.appendingPathComponent("last-recording.wav").path))
  #expect(
    try FileManager.default.contentsOfDirectory(atPath: directory.path).allSatisfy {
      !$0.hasPrefix(".recording-")
    })
  await store.historySettingsTask?.value
}

@Test @MainActor func slowFailingAudioArchiveDoesNotDelayOrFailDictation() async throws {
  let directory = try lifecycleDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let began = LifecycleSignal()
  let release = LifecycleSignal()
  let options = URLSessionConfiguration.ephemeral
  options.protocolClasses = [StoreProtocol.self]
  let client = try AntflyClient(
    server: "https://fixture.example", session: URLSession(configuration: options))
  let store = Store(
    directory: directory, registerHotkey: false,
    audioArchiveStage: { _, _ in
      await began.send()
      await release.wait()
      throw DictationError.invalid("Disk unavailable")
    }, clientFactory: { _ in client }, credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  store.config.retainAudio = true
  store.config.autoPaste = false
  store.archiveAudio(Task { Data([1]) }, run: store.generation)
  await began.wait()
  store.submit(
    audio: Data([1]), preset: store.config.presets[0], snapshot: store.config, destination: nil)
  await store.requestTask?.value
  #expect(store.state == .idle && store.result == "Clean.")
  await release.send()
  await store.audioArchiveTask?.value
  #expect(store.state == .idle && store.result == "Clean.")
  #expect(store.processingWarning.contains("Audio archive unavailable"))
}

@Test @MainActor func cancelingOriginalInsertionBeforeClipboardReadFinishesCannotPaste()
  async throws
{
  let directory = try lifecycleDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let began = LifecycleSignal()
  let release = LifecycleSignal()
  var sends = 0
  let store = Store(
    directory: directory, registerHotkey: false,
    originalPaster: { _, validate in
      await began.send()
      await release.wait()
      try validate()
      sends += 1
    }, credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  store.preview = "Recovered transcript"
  store.canRecoverRaw = true
  store.insertOriginal()
  let insertion = store.requestTask
  await began.wait()
  store.cancel()
  await release.send()
  await insertion?.value
  #expect(sends == 0)
  #expect(store.state == .idle && store.phase == .canceled)
}

@Test @MainActor func oversizedSettingsCannotReplaceSavedPreferencesOrCredentials() throws {
  let directory = try lifecycleDirectory()
  defer { try? FileManager.default.removeItem(at: directory) }
  let file = directory.appendingPathComponent("settings.json")
  let saved = try Data(contentsOf: file)
  var writes = 0
  let store = Store(
    directory: directory, registerHotkey: false,
    credentialReader: { _ in "" }, credentialWriter: { _, _ in writes += 1 })
  store.config.presets[0].appIDs = [String(repeating: "a", count: Configuration.maxEncodedBytes)]
  store.save()
  #expect(store.message.contains("Settings exceed"))
  #expect(try Data(contentsOf: file) == saved)
  #expect(writes == 0)
}
