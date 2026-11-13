// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

private final class SetupDiscoveryProtocol: URLProtocol, @unchecked Sendable {
  private static let responseLock = NSLock()
  nonisolated(unsafe) private static var storedResponse = (
    status: 503, body: #"{"error":"ModelInventoryUnavailable"}"#
  )
  static var response: (status: Int, body: String) {
    get { responseLock.withLock { storedResponse } }
    set { responseLock.withLock { storedResponse = newValue } }
  }
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    let response = Self.response
    client?.urlProtocol(
      self,
      didReceive: HTTPURLResponse(
        url: request.url!, statusCode: response.status,
        httpVersion: nil,
        headerFields: [
          "Content-Type": request.url?.path.hasSuffix("/dictate") == true
            ? "text/event-stream" : "application/json"
        ])!,
      cacheStoragePolicy: .notAllowed)
    client?.urlProtocol(self, didLoad: Data(response.body.utf8))
    client?.urlProtocolDidFinishLoading(self)
  }
  override func stopLoading() {}
}

@Suite(.serialized) struct SetupLifecycleTests {
  private func directory() throws -> URL {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
    return root
  }
  @Test @MainActor func explicitCheckShowsFailureClearsStaleModelsAndRecovers() async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let settings = URLSessionConfiguration.ephemeral
    settings.protocolClasses = [SetupDiscoveryProtocol.self]
    let connection = try AntflyClient(
      server: "http://localhost:8080", session: URLSession(configuration: settings))
    let store = Store(
      directory: root, registerHotkey: false, clientFactory: { _ in connection },
      credentialReader: { _ in "" })
    store.config.connectionMode = .external
    let catalog =
      #"{"data":[{"id":"transcribers/whisper","task":"transcribe","availability":"ready"}]}"#
    store.models = try JSONDecoder().decode(ModelCatalog.self, from: Data(catalog.utf8)).data
    SetupDiscoveryProtocol.response = (503, #"{"error":"ModelInventoryUnavailable"}"#)
    store.checkEngine()
    #expect(store.discoveryState == .checking && !store.engineReadiness.isReady)
    await store.discoveryTask?.value
    #expect(store.models.isEmpty)
    #expect(store.message.contains("ModelInventoryUnavailable"))
    if case .failed = store.discoveryState {
    } else {
      Issue.record("Missing discovery failure state")
    }
    #expect(!store.engineReadiness.isReady)
    SetupDiscoveryProtocol.response = (200, catalog)
    store.checkEngine()
    await store.discoveryTask?.value
    #expect(store.discoveryState == .current && store.engineReadiness.isReady)
    await store.prepareToQuit()
  }
  @Test @MainActor func setupStatusFollowsTheOperationAndSuccessfulSettingsCommit() async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let store = Store(directory: root, registerHotkey: false)
    store.runModelSetup(.appleSpeech) { throw DictationError.invalid("Test failure") }
    await store.modelSetupTask?.value
    #expect(store.modelSetup == .failed(.appleSpeech, "Test failure"))
    store.runModelSetup(.whisper) {
      var candidate = store.config
      candidate.dictionary.append("PersistedWord")
      return PreparedModelSetup(
        configuration: candidate, result: ModelSetupResult(operation: .whisper))
    }
    #expect(store.modelSetup.isRunning && store.modelSetup.operation == .whisper)
    await store.modelSetupTask?.value
    #expect(store.modelSetup == .ready(ModelSetupResult(operation: .whisper)))
    #expect(!store.message.contains("Apple"))
    #expect(!store.hasUnsavedSettings)
    #expect(
      try Configuration.decode(Data(contentsOf: root.appendingPathComponent("settings.json")))
        .dictionary.contains("PersistedWord"))
    await store.prepareToQuit()
  }
  @Test @MainActor func cancelingWhisperSetupKeepsOperationAndPreferences() async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let store = Store(directory: root, registerHotkey: false)
    let original = store.config
    store.runModelSetup(.whisper) {
      try await Task.sleep(for: .seconds(60))
      return PreparedModelSetup(
        configuration: store.config, result: ModelSetupResult(operation: .whisper))
    }
    store.cancelModelSetup()
    await store.modelSetupTask?.value
    #expect(store.modelSetup == .canceled(.whisper))
    #expect(store.config == original && !store.message.contains("Apple"))
    await store.prepareToQuit()
  }
  @Test @MainActor func processorConflictBlocksSetupWithoutChangingOrSavingPresets() async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let store = Store(directory: root, registerHotkey: false)
    store.config.presets[0].cleanupModel = "Qwen/raw-model"
    let original = store.config
    store.runModelSetup(.appleSpeech) {
      let candidate = try store.config.selectingManagedTranscriber(
        .appleSpeech, catalog: ModelCatalog(data: []))
      return PreparedModelSetup(
        configuration: candidate,
        result: ModelSetupResult(operation: .appleSpeech, language: "en-US"))
    }
    await store.modelSetupTask?.value
    if case .blocked(.appleSpeech, let conflict) = store.modelSetup {
      #expect(!conflict.canUseBundledProcessor)
    } else {
      Issue.record("Conflict did not produce a recoverable setup state")
    }
    #expect(store.config == original)
    #expect(
      !FileManager.default.fileExists(atPath: root.appendingPathComponent("settings.json").path))
    await store.prepareToQuit()
  }
  @Test @MainActor func invalidProcessorOnResubmissionPreservesOriginalRecovery() async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let store = Store(
      directory: root, registerHotkey: false,
      clientFactory: { _ in
        Issue.record("Invalid processor reached the client factory")
        throw DictationError.invalid("Unexpected network")
      })
    store.preview = "Keep the original words"
    store.canRecoverRaw = true
    var preset = store.config.presets[0]
    preset.outputMode = .rewrite
    store.submit(audio: Data([0]), preset: preset, snapshot: store.config, destination: nil)
    #expect(store.state == .idle && store.requestTask == nil)
    #expect(store.canRecoverRaw && store.preview == "Keep the original words")
    #expect(store.message.contains("text processor"))
    await store.prepareToQuit()
  }
}

extension SetupLifecycleTests {
  @Test @MainActor func cleanupRetryDoesNotFalselyVerifyMissingSpeechAssets() async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let settings = URLSessionConfiguration.ephemeral
    settings.protocolClasses = [SetupDiscoveryProtocol.self]
    let connection = try AntflyClient(
      server: "http://localhost:8080", session: URLSession(configuration: settings))
    let store = Store(
      directory: root, registerHotkey: false, clientFactory: { _ in connection },
      credentialReader: { _ in "" })
    store.config.connectionMode = .external
    store.config.presets[0].model = "transcribers/apple-speech"
    store.config.presets[0].language = "fr-FR"
    store.config.presets[0].cleanupModel = "generators/cleanup"
    store.config.presets[0].outputMode = .clean
    SetupDiscoveryProtocol.response = (
      200,
      #"{"data":[{"id":"transcribers/apple-speech","dictation":{"live_preview":false,"recognition_hints":false,"native_status":{"speech_available":true,"speech_installed_locales":[]}}}]}"#
    )
    store.checkEngine()
    await store.discoveryTask?.value
    #expect(store.engineReadiness == .assetsNeeded("fr-FR"))
    store.preview = "raw"
    store.canRecoverRaw = true
    SetupDiscoveryProtocol.response = (
      200,
      "data: {\"type\":\"dictation.transcript\",\"transcript\":{\"text\":\"raw\"}}\n\ndata: {\"type\":\"dictation.completed\",\"text\":\"Clean.\"}\n\ndata: [DONE]\n\n"
    )
    store.retryCleanup()
    await store.requestTask?.value
    #expect(store.result == "Clean.")
    #expect(store.engineReadiness == .assetsNeeded("fr-FR"))
    #expect(store.appleSpeechLanguage == "fr-FR")
    await store.prepareToQuit()
  }
}

extension SetupLifecycleTests {
  @Test(arguments: [ModelSetupOperation.appleSpeech, .whisper])
  @MainActor func failedSetupKeepsPreferencesAndNamesTheFailingEngine(
    operation: ModelSetupOperation
  ) async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let store = Store(directory: root, registerHotkey: false)
    let original = store.config
    store.runModelSetup(operation) { throw DictationError.invalid("Download unavailable") }
    await store.modelSetupTask?.value
    #expect(store.modelSetup == .failed(operation, "Download unavailable"))
    #expect(store.message.contains(operation.label))
    #expect(store.config == original)
    #expect(
      !FileManager.default.fileExists(atPath: root.appendingPathComponent("settings.json").path))
    await store.prepareToQuit()
  }
}

extension SetupLifecycleTests {
  @Test(arguments: ["ready", "failed", "canceled"])
  @MainActor func explicitAutomaticSelectionSupersedesTerminalSetupState(terminal: String)
    async throws
  {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let settings = URLSessionConfiguration.ephemeral
    settings.protocolClasses = [SetupDiscoveryProtocol.self]
    let connection = try AntflyClient(
      server: "http://localhost:8080", session: URLSession(configuration: settings))
    let store = Store(
      directory: root, registerHotkey: false, clientFactory: { _ in connection },
      managedAppleSupport: { true })
    store.config.presets[0].automaticallySelectLocalTranscriber = false
    store.runModelSetup(.whisper) {
      if terminal == "failed" { throw DictationError.invalid("Synthetic failure") }
      if terminal == "canceled" { try await Task.sleep(for: .seconds(60)) }
      return PreparedModelSetup(
        configuration: store.config, result: ModelSetupResult(operation: .whisper))
    }
    if terminal == "canceled" { store.cancelModelSetup() }
    await store.modelSetupTask?.value
    #expect(store.modelSetup != .idle)
    SetupDiscoveryProtocol.response = (
      200,
      #"{"data":[{"id":"transcribers/apple-speech","task":"transcribe","dictation":{"live_preview":false,"recognition_hints":false,"native_status":{"speech_available":true,"speech_installed_locales":["en_US"]}}}]}"#
    )
    store.selectAutomaticTranscriber(presetID: store.config.presets[0].id)
    #expect(store.modelSetup == .idle)
    await store.discoveryTask?.value
    #expect(store.config.presets[0].automaticallySelectLocalTranscriber == true)
    #expect(store.config.presets[0].model == "transcribers/apple-speech")
    await store.prepareToQuit()
  }

  @Test(arguments: [true, false])
  @MainActor func failedSetupCommitKeepsOldSelectionAndCanRetrySavingWithoutReinstalling(
    retrySave: Bool
  ) async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let original = Configuration()
    let file = root.appendingPathComponent("settings.json")
    let backup = root.appendingPathComponent("original.json")
    try JSONEncoder().encode(original).write(to: file)
    let store = Store(directory: root, registerHotkey: false)
    var candidate = original
    candidate.presets[0].model = "transcribers/apple-speech"
    candidate.presets[0].language = "fr-FR"
    candidate.presets[0].automaticallySelectLocalTranscriber = false
    // Force a real rename failure after staging, without relying on UID/permission behavior.
    try FileManager.default.moveItem(at: file, to: backup)
    try FileManager.default.createDirectory(at: file, withIntermediateDirectories: false)
    var installs = 0
    let result = ModelSetupResult(operation: .appleSpeech, language: "fr-FR")
    store.runModelSetup(.appleSpeech) {
      installs += 1
      return PreparedModelSetup(configuration: candidate, result: result)
    }
    await store.modelSetupTask?.value
    #expect(store.hasPendingSetupCommit && store.settingsLocked)
    #expect(store.config == original)
    #expect(try Configuration.decode(Data(contentsOf: backup)) == original)
    #expect(
      try FileManager.default.contentsOfDirectory(atPath: root.path).allSatisfy {
        !$0.hasPrefix(".dictation-settings-")
      })
    if case .installedNotSaved(let installed, _) = store.modelSetup {
      #expect(installed == result)
    } else {
      Issue.record("Save failure lost the completed installation result")
    }
    try FileManager.default.removeItem(at: file)
    try FileManager.default.moveItem(at: backup, to: file)
    if retrySave {
      store.savePendingSetup()
      await store.settingsTask?.value
      #expect(store.config == candidate)
      #expect(try Configuration.decode(Data(contentsOf: file)) == candidate)
      #expect(store.modelSetup == .ready(result))
      let permissions =
        try FileManager.default.attributesOfItem(atPath: file.path)[.posixPermissions] as? NSNumber
      #expect(permissions?.intValue == 0o600)
    } else {
      store.discardPendingSetup()
      #expect(store.config == original)
      #expect(try Configuration.decode(Data(contentsOf: file)) == original)
      #expect(store.modelSetup == .installedNotSelected(result))
    }
    #expect(installs == 1 && !store.hasPendingSetupCommit && !store.settingsLocked)
    await store.prepareToQuit()
  }

  @Test @MainActor func setupResultKeepsTheCheckedLanguageAfterTheActivePresetChanges() async throws
  {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let store = Store(directory: root, registerHotkey: false)
    let checked = ModelSetupResult(operation: .appleSpeech, language: "en-US")
    store.runModelSetup(.appleSpeech) {
      return PreparedModelSetup(configuration: store.config, result: checked)
    }
    await store.modelSetupTask?.value
    store.config.presets[1].language = "fr-FR"
    store.config.selectedPreset = store.config.presets[1].id
    #expect(store.modelSetup == .ready(checked))
    #expect(store.modelSetup.message.contains("en-US"))
    #expect(!store.modelSetup.message.contains("fr-FR"))
    #expect(store.appleSpeechLanguage == "fr-FR")
    await store.prepareToQuit()
  }
}

@MainActor private final class RefusingSetupShortcut: ShortcutManaging {
  var changed: ((Bool) -> Void)?
  var interrupted: (() -> Void)?
  var isAvailable: Bool { false }
  func register(_ shortcut: KeyboardShortcut) throws {
    throw DictationError.invalid("Accessibility denied")
  }
  func unregister() {}
  func setSuspended(_ value: Bool) {}
}

extension SetupLifecycleTests {
  @Test @MainActor func postCommitShortcutFailureReportsSavedSettingsWithAWarning() async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let store = Store(
      directory: root, registerHotkey: false, shortcutManager: RefusingSetupShortcut())
    var candidate = store.config
    candidate.dictionary.append("CommittedWord")
    store.runModelSetup(.whisper) {
      return PreparedModelSetup(
        configuration: candidate, result: ModelSetupResult(operation: .whisper))
    }
    await store.modelSetupTask?.value
    #expect(!store.hasPendingSetupCommit && store.config == candidate)
    #expect(
      try Configuration.decode(Data(contentsOf: root.appendingPathComponent("settings.json")))
        == candidate)
    if case .ready(let result) = store.modelSetup {
      #expect(result.warning?.contains("Accessibility denied") == true)
    } else {
      Issue.record("A post-commit warning was mistaken for a failed settings write")
    }
    await store.prepareToQuit()
  }

  @Test @MainActor func pendingCommitCannotOverwriteInterveningConfigurationChanges() async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let store = Store(directory: root, registerHotkey: false)
    let file = root.appendingPathComponent("settings.json")
    try FileManager.default.createDirectory(at: file, withIntermediateDirectories: false)
    var candidate = store.config
    candidate.dictionary = ["Candidate"]
    store.runModelSetup(.whisper) {
      return PreparedModelSetup(
        configuration: candidate, result: ModelSetupResult(operation: .whisper))
    }
    await store.modelSetupTask?.value
    #expect(store.hasPendingSetupCommit)
    // UI edits are locked; also defend against changes from other application callbacks.
    store.config.dictionary = ["New user edit"]
    try FileManager.default.removeItem(at: file)
    store.savePendingSetup()
    await store.settingsTask?.value
    #expect(store.config.dictionary == ["New user edit"])
    #expect(!FileManager.default.fileExists(atPath: file.path))
    #expect(store.hasPendingSetupCommit && store.modelSetup.message.contains("Settings changed"))
    store.discardPendingSetup()
    #expect(!store.settingsLocked && store.config.dictionary == ["New user edit"])
    await store.prepareToQuit()
  }
}
