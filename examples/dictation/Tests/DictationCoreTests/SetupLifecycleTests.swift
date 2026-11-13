// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

private final class SetupDiscoveryProtocol: URLProtocol, @unchecked Sendable {
  nonisolated(unsafe) static var response = (
    status: 503, body: #"{"error":"ModelInventoryUnavailable"}"#
  )
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    client?.urlProtocol(
      self,
      didReceive: HTTPURLResponse(
        url: request.url!, statusCode: Self.response.status,
        httpVersion: nil,
        headerFields: [
          "Content-Type": request.url?.path.hasSuffix("/dictate") == true
            ? "text/event-stream" : "application/json"
        ])!,
      cacheStoragePolicy: .notAllowed)
    client?.urlProtocol(self, didLoad: Data(Self.response.body.utf8))
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
    store.runModelSetup(.whisper) { store.config.dictionary.append("PersistedWord") }
    #expect(store.modelSetup.isRunning && store.modelSetup.operation == .whisper)
    await store.modelSetupTask?.value
    #expect(store.modelSetup == .ready(.whisper))
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
    store.runModelSetup(.whisper) { try await Task.sleep(for: .seconds(60)) }
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
      store.config = try store.config.selectingManagedTranscriber(
        .appleSpeech, catalog: ModelCatalog(data: []))
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
