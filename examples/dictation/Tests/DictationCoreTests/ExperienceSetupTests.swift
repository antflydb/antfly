// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationCore

@Test func outputModesRequireAnExplicitTextProcessor() throws {
  try DictationMode.verbatim.validateProcessor("")
  for mode in [DictationMode.clean, .rewrite] {
    #expect(throws: (any Error).self) { try mode.validateProcessor(" \n") }
    try mode.validateProcessor("generators/cleanup")
  }
}

@Test func freshPresetsDoNotPromiseUnconfiguredCleanup() {
  for preset in Configuration().presets {
    #expect(preset.outputMode == .verbatim)
    #expect(!preset.runsCleanup)
  }
}

@Test func missingAppleAssetsExplainsInstallationOnTheEngineHost() throws {
  let response = HTTPURLResponse(
    url: URL(string: "http://localhost:8080/ai/v1/dictate")!,
    statusCode: 503, httpVersion: nil, headerFields: nil)!
  let failure = ServerFailure(
    response: response,
    body: Data(
      #"{"error":{"code":"AppleSpeechAssetsMissing","message":"model unavailable"}}"#.utf8))
  #expect(failure.localizedDescription.contains("language assets"))
  #expect(failure.localizedDescription.contains("external server"))
  #expect(failure.localizedDescription.contains("download_assets"))
}

private func speechCatalog(
  available: Bool? = true, installed: [String]? = ["en_US"],
  error: String? = nil
) throws -> ModelCatalog {
  var native: [String: Any] = [:]
  if let available { native["speech_available"] = available }
  if let installed { native["speech_installed_locales"] = installed }
  var capabilities: [String: Any] = [
    "live_preview": false, "recognition_hints": false, "native_status": native,
  ]
  if let error { capabilities["readiness_error"] = error }
  return try JSONDecoder().decode(
    ModelCatalog.self,
    from: JSONSerialization.data(withJSONObject: [
      "data": [
        ["id": "transcribers/apple-speech", "task": "transcribe", "dictation": capabilities],
        ["id": "generators/cleanup", "task": "generate"],
        [
          "id": "transcribers/whisper", "task": "transcribe",
          "dictation": [
            "live_preview": true, "recognition_hints": true, "stable_segments": true,
            "text_cleanup": true,
          ],
        ],
      ]
    ]))
}

@Test func speechReadinessRequiresAvailableSpeechAndTheRequestedLanguage() throws {
  let ready = try speechCatalog().data[0]
  #expect(ready.speechReadiness(language: "en-US") == .ready)
  #expect(ready.speechReadiness(language: "en_us") == .ready)
  #expect(ready.speechReadiness(language: "en") == .unverified)
  #expect(ready.speechReadiness(language: "en-GB") == .unverified)
  #expect(ready.speechReadiness(language: "", defaultLanguage: "en-US") == .ready)
  #expect(ready.speechReadiness(language: "") == .unverified)  // Unknown external model default.
  #expect(ready.speechReadiness(language: "fr-FR") == .assetsNeeded("fr-FR"))
  #expect(
    try speechCatalog(installed: []).data[0].speechReadiness(language: "en-US")
      == .assetsNeeded("en-US"))
  #expect(
    try speechCatalog(available: nil).data[0].speechReadiness(language: "en-US") == .unverified)
  #expect(
    try speechCatalog(installed: nil).data[0].speechReadiness(language: "en-US") == .unverified)
  if case .unavailable = try speechCatalog(available: false).data[0].speechReadiness(
    language: "en-US")
  {
  } else {
    Issue.record("Unavailable Apple speech was marked ready")
  }
  if case .unavailable = try speechCatalog(error: "bridge unavailable").data[0].speechReadiness(
    language: "en-US")
  {
  } else {
    Issue.record("Bridge readiness error was ignored")
  }
}

@Test func automaticAppleSelectionRespectsEachPresetsLanguageAndMissingAssets() throws {
  var config = Configuration()
  config.presets[1].language = "fr-FR"
  config.selectAvailableManagedTranscriber(from: try speechCatalog(), supportsApple: true)
  #expect(config.presets[0].model == "transcribers/apple-speech")
  #expect(config.presets[1].model == "transcribers/whisper")
  config.selectAvailableManagedTranscriber(
    from: try speechCatalog(installed: []), supportsApple: true)
  #expect(config.presets.allSatisfy { $0.model == "transcribers/whisper" })
  config.selectAvailableManagedTranscriber(
    from: try speechCatalog(available: false), supportsApple: true)
  #expect(config.presets.allSatisfy { $0.model == "transcribers/whisper" })
}

@Test func managedSpeechSelectionRequiresAnExplicitResolutionForIncompatibleProcessors() throws {
  var config = Configuration()
  config.presets[0].model = "openai/whisper-tiny"
  config.presets[0].transcriberRoute = .runtime
  config.presets[0].cleanupModel = "Qwen/raw-model"
  config.presets[0].outputMode = .rewrite
  config.presets[0].instructions = "Keep a friendly tone"
  config.presets[1].cleanupModel = "generators/cleanup"
  config.presets[1].outputMode = .clean
  let original = config
  let catalog = try speechCatalog()
  for operation in [ModelSetupOperation.appleSpeech, .whisper] {
    #expect(throws: ManagedCleanupConflict.self) {
      try config.selectingManagedTranscriber(operation, catalog: catalog)
    }
    #expect(config == original)
    let verbatim = try config.selectingManagedTranscriber(
      operation, catalog: catalog, resolution: .verbatim)
    #expect(verbatim.presets[0].outputMode == .verbatim && verbatim.presets[0].cleanupModel.isEmpty)
    #expect(
      verbatim.presets[1].cleanupModel == "generators/cleanup"
        && verbatim.presets[1].outputMode == .clean)
    let bundled = try config.selectingManagedTranscriber(
      operation, catalog: catalog, resolution: .bundled)
    #expect(bundled.presets[0].cleanupModel == "generators/cleanup")
    #expect(
      bundled.presets[0].outputMode == .rewrite
        && bundled.presets[0].instructions == original.presets[0].instructions)
    #expect(
      bundled.presets.allSatisfy { $0.model == operation.transcriber && $0.usesNamedTranscriber })
  }
  #expect(throws: ManagedCleanupConflict.self) {
    try config.selectingManagedTranscriber(
      .appleSpeech, catalog: ModelCatalog(data: []), resolution: .bundled)
  }
}

@Test func automaticSelectionDoesNotMoveRawCleanupToTheAppleGateway() throws {
  var config = Configuration()
  config.presets[0].cleanupModel = "Qwen/raw-model"
  let before = config.presets[0]
  config.selectAvailableManagedTranscriber(from: try speechCatalog(), supportsApple: true)
  #expect(config.presets[0].model == "transcribers/whisper")
  #expect(config.presets[0].cleanupModel == before.cleanupModel)
  #expect(config.presets[1].model == "transcribers/apple-speech")
}

@Test func changingSpeechEngineRequiresResolutionForUnsupportedIncrementalProcessing() throws {
  var config = Configuration()
  config.presets[0].processingStrategy = .incremental
  config.presets[0].cleanupModel = "generators/cleanup"
  config.presets[0].outputMode = .clean
  config.presets[1].processingStrategy = .wholeUtterance
  let original = config
  let catalog = try speechCatalog()
  #expect(throws: ManagedProcessingConflict.self) {
    try config.selectingManagedTranscriber(.appleSpeech, catalog: catalog)
  }
  let candidate = try config.selectingManagedTranscriber(
    .appleSpeech, catalog: catalog, processing: .automatic)
  #expect(config == original)
  #expect(candidate.presets[0].processingStrategy == .automatic)
  #expect(
    candidate.presets[0].outputMode == .clean
      && candidate.presets[0].cleanupModel == "generators/cleanup")
  #expect(candidate.presets[1].processingStrategy == .wholeUtterance)
  for preset in candidate.presets {
    _ = try ProcessingPlan(preset: preset, model: catalog.transcriber(preset.model))
  }
  let whisper = try config.selectingManagedTranscriber(.whisper, catalog: catalog)
  #expect(whisper.presets[0].processingStrategy == .incremental)
}

@Test func automaticSelectionKeepsACompatibleIncrementalWhisperPreset() throws {
  var config = Configuration()
  config.presets[0].processingStrategy = .incremental
  config.selectAvailableManagedTranscriber(from: try speechCatalog(), supportsApple: true)
  #expect(config.presets[0].model == "transcribers/whisper")
  #expect(config.presets[0].processingStrategy == .incremental)
  #expect(config.presets[1].model == "transcribers/apple-speech")
}

@Test func cleanupAndProcessingResolutionsComposeWithoutDroppingUserInstructions() throws {
  var config = Configuration()
  config.presets[0].cleanupModel = "Qwen/raw"
  config.presets[0].processingStrategy = .incremental
  config.presets[0].outputMode = .clean
  config.presets[0].instructions = "Preserve my vocabulary"
  let catalog = try speechCatalog()
  #expect(throws: ManagedProcessingConflict.self) {
    try config.selectingManagedTranscriber(.appleSpeech, catalog: catalog, resolution: .bundled)
  }
  let candidate = try config.selectingManagedTranscriber(
    .appleSpeech, catalog: catalog,
    resolution: .bundled, processing: .automatic)
  #expect(candidate.presets[0].cleanupModel == "generators/cleanup")
  #expect(candidate.presets[0].processingStrategy == .automatic)
  #expect(candidate.presets[0].instructions == config.presets[0].instructions)
}

@Test func speechRecoveryGuidanceTargetsTheProcessingHost() {
  let assets = SpeechReadiness.assetsNeeded("fr-FR")
  let local = assets.message(for: .onThisMac)
  #expect(local.contains("this Mac") && local.contains("Settings → Speech & text"))
  let remote = assets.message(for: .externalServer("https://speech.example:8443"))
  #expect(remote.contains("https://speech.example:8443") && remote.contains("administrator"))
  #expect(remote.contains("download_assets") && !remote.contains("Settings →"))
  #expect(!remote.contains("this Mac"))
  let unavailable = SpeechReadiness.unavailable("Apple speech is unavailable.")
    .message(for: .externalServer("https://speech.example"))
  #expect(unavailable.contains("speech.example") && !unavailable.contains("this Mac"))
  let sanitized = assets.message(
    for: .externalServer("https://user:secret@speech.example/path?token=private"))
  #expect(
    sanitized.contains("speech.example") && !sanitized.contains("secret")
      && !sanitized.contains("private"))
}
