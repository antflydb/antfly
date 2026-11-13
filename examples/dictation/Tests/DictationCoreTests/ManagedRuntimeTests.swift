import Foundation
import Testing

@testable import DictationApp
// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
@testable import DictationCore

@Test func newInstallUsesBundledRuntimeButLegacyServerSettingsStayExternal() throws {
  let fresh = Configuration()
  #expect(fresh.usesManagedRuntime)
  var encoded =
    try JSONSerialization.jsonObject(with: JSONEncoder().encode(fresh)) as! [String: Any]
  encoded.removeValue(forKey: "connectionMode")
  encoded["server"] = "https://my-antfly.example"
  let legacy = try Configuration.decode(JSONSerialization.data(withJSONObject: encoded))
  #expect(!legacy.usesManagedRuntime)
  #expect(legacy.server == "https://my-antfly.example")
  #expect(try Configuration.decode(JSONEncoder().encode(fresh)).usesManagedRuntime)
}

@MainActor @Test func missingManagedCredentialNeverResetsExistingData() async throws {
  let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  let data = root.appendingPathComponent("Runtime/data")
  try FileManager.default.createDirectory(at: data, withIntermediateDirectories: true)
  let sentinel = data.appendingPathComponent("preserved")
  try Data("existing history".utf8).write(to: sentinel)
  defer { try? FileManager.default.removeItem(at: root) }
  var writes = 0
  let runtime = ManagedRuntime(
    directory: root, runtime: URL(fileURLWithPath: "/usr/bin/false"),
    launcher: URL(fileURLWithPath: "/usr/bin/false"), credentialRead: { "" },
    credentialWrite: { _ in writes += 1 })
  runtime.start()
  let deadline = ContinuousClock.now.advanced(by: .seconds(3))
  while runtime.busy && ContinuousClock.now < deadline {
    try await Task.sleep(for: .milliseconds(10))
  }
  #expect(!runtime.ready)
  #expect(runtime.status.contains("Keychain credential is unavailable"))
  #expect(writes == 0)
  #expect(try Data(contentsOf: sentinel) == Data("existing history".utf8))
  await runtime.stop()
}

@MainActor @Test func managedCredentialStorageFailureStopsBeforeLaunchingAnEngine() async throws {
  let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  defer { try? FileManager.default.removeItem(at: root) }
  let runtime = ManagedRuntime(
    directory: root, runtime: URL(fileURLWithPath: "/usr/bin/false"),
    launcher: URL(fileURLWithPath: "/usr/bin/false"), credentialRead: { "" },
    credentialWrite: { secret in
      #expect(Data(base64Encoded: secret)?.count == 32)
      throw DictationError.invalid("Synthetic locked Keychain")
    })
  runtime.start()
  let deadline = ContinuousClock.now.advanced(by: .seconds(3))
  while runtime.busy && ContinuousClock.now < deadline {
    try await Task.sleep(for: .milliseconds(10))
  }
  #expect(!runtime.ready)
  #expect(runtime.status == "Synthetic locked Keychain")
  #expect(
    !FileManager.default.fileExists(atPath: root.appendingPathComponent("Runtime/antfly.json").path)
  )
  await runtime.stop()
}

@MainActor @Test func quitDrainsModelSetupAndKeepsInstallationQuiesced() async throws {
  let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  defer { try? FileManager.default.removeItem(at: root) }
  let store = Store(
    directory: root, registerHotkey: false, credentialReader: { _ in "" },
    credentialWriter: { _, _ in })
  store.config.connectionMode = .managed
  let original = store.config.presets
  store.installLocalWhisper()
  #expect(store.settingUpModel && store.working)
  await store.prepareToQuit()
  #expect(!store.settingUpModel)
  #expect(store.working)
  #expect(store.config.presets == original)
  #expect(!store.runtime.ready)
}

@Test func managedValidationUsesLocalOriginAndPreservesTheInactiveExternalAddress() throws {
  for address in ["", "localhost", "not a server", "https://previous.example"] {
    var config = Configuration()
    config.server = address
    let validated = try config.validated()
    #expect(validated.server == address)
    #expect(validated.effectiveServer == Configuration.managedServerOrigin)
    #expect(
      try Configuration.decode(JSONEncoder().encode(config)).effectiveServer
        == Configuration.managedServerOrigin)
    if address != "https://previous.example" {
      config.connectionMode = .external
      #expect(throws: DictationError.self) { try config.validated() }
    }
  }
}

private func localCatalog(error: String? = nil) throws -> ModelCatalog {
  var capabilities: [String: Any] = [
    "live_preview": false, "recognition_hints": false,
    "native_status": ["speech_available": true, "speech_installed_locales": ["en_US"]],
  ]
  if let error { capabilities["readiness_error"] = error }
  return try JSONDecoder().decode(
    ModelCatalog.self,
    from: JSONSerialization.data(withJSONObject: [
      "data": [
        ["id": "transcribers/apple-speech", "task": "transcribe", "dictation": capabilities],
        ["id": "generators/custom-cleanup", "task": "generate"],
      ]
    ]))
}

@Test func automaticLocalSpeechPrefersReadyAppleAndFallsBackWithoutDownloading() throws {
  var config = Configuration()
  config.presets[0].cleanupModel = "generators/custom-cleanup"
  #expect(config.selectAvailableManagedTranscriber(from: try localCatalog(), supportsApple: true))
  #expect(config.presets.allSatisfy { $0.model == "transcribers/apple-speech" })
  #expect(config.presets[0].cleanupModel == "generators/custom-cleanup")
  #expect(
    config.selectAvailableManagedTranscriber(
      from: try localCatalog(error: "assets missing"), supportsApple: true))
  #expect(config.presets.allSatisfy { $0.model == "transcribers/whisper" })
  #expect(!config.selectAvailableManagedTranscriber(from: try localCatalog(), supportsApple: false))
}

@Test func automaticLocalSpeechPreservesExplicitCustomAndExternalSelections() throws {
  var config = Configuration()
  config.presets[0].automaticallySelectLocalTranscriber = false
  config.presets[1].model = "transcribers/custom"
  config.selectAvailableManagedTranscriber(from: try localCatalog(), supportsApple: true)
  #expect(config.presets[0].model == "transcribers/whisper")
  #expect(config.presets[1].model == "transcribers/custom")
  #expect(config.presets[2].model == "transcribers/apple-speech")
  config.connectionMode = .external
  let previous = config.presets
  #expect(!config.selectAvailableManagedTranscriber(from: try localCatalog(), supportsApple: false))
  #expect(config.presets == previous)
}

@MainActor @Test func savingManagedSettingsDoesNotReadOrOverwriteExternalCredentials() throws {
  let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  defer { try? FileManager.default.removeItem(at: root) }
  let store = Store(
    directory: root, registerHotkey: false,
    credentialReader: { _ in
      Issue.record("Read inactive external credentials")
      return ""
    },
    credentialWriter: { _, _ in Issue.record("Overwrote inactive external credentials") })
  store.config.connectionMode = .managed
  store.config.server = "not a server"
  store.save()
  let saved = try Configuration.decode(
    Data(contentsOf: root.appendingPathComponent("settings.json")))
  #expect(saved.usesManagedRuntime && saved.server == "not a server")
}

@Test func managedClientRoutesToLoopbackWithoutAValidExternalServer() async throws {
  var config = Configuration()
  config.server = "not a server"
  let client = try AntflyClient(configuration: config)
  #expect(await client.origin.absoluteString == Configuration.managedServerOrigin)
  let explicit = try AntflyClient(server: "https://external.example")
  #expect(await explicit.origin.absoluteString == "https://external.example")
}

@MainActor @Test func setupWaitReportsStartupFailureInsteadOfWaitingTheFullDeadline() async throws {
  let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  defer { try? FileManager.default.removeItem(at: root) }
  let runtime = ManagedRuntime(
    directory: root, runtime: URL(fileURLWithPath: "/does-not-exist"), credentialRead: { "" },
    credentialWrite: { _ in })
  runtime.start()
  do {
    try await runtime.waitUntilReady(timeout: .seconds(3))
    Issue.record("Missing engine became ready")
  } catch { #expect(error.localizedDescription.contains("Bundled Antfly is missing")) }
  await runtime.stop()
}
