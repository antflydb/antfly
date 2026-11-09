// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationCore

@Test func appPresetSelectionAndFallback() throws {
  var config = Configuration()
  config.presets[1].appIDs = ["com.apple.mail"]
  #expect(config.preset(for: "com.apple.mail").id == "email")
  #expect(config.preset(for: "unknown").id == "slack")
  #expect(config.preset(for: nil).id == "slack")
  #expect(try Configuration.decode(JSONEncoder().encode(config)) == config)
}
@Test func importedSettingsRejectCredentialsAndInvalidVersions() throws {
  var config = Configuration()
  config.connectionMode = .external
  config.server = "https://user:password@example.com"
  #expect(throws: DictationError.self) { try config.validated() }
  config = Configuration()
  config.version = 2
  #expect(throws: DictationError.self) { try Configuration.decode(JSONEncoder().encode(config)) }
  config = Configuration()
  config.presets = []
  #expect(throws: DictationError.self) { try config.validated() }
  config = Configuration()
  config.presets.append(config.presets[0])
  #expect(throws: DictationError.self) { try config.validated() }
  config = Configuration()
  config.dictionary = ["new\nline"]
  #expect(throws: DictationError.self) { try config.validated() }
  config = Configuration()
  config.table = "../escape"
  #expect(throws: DictationError.self) { try config.validated() }
  config = Configuration()
  config.profile = "bad/profile"
  #expect(throws: DictationError.self) { try config.validated() }
}
@Test func recordingTransitionsIgnoreRepeatedEventsAndProcessingPresses() {
  var state = RecordingState.idle
  var changed = state.press(toggle: false)
  #expect(changed)
  #expect(state == .recording)
  changed = state.press(toggle: false)
  #expect(!changed)
  changed = state.release(toggle: false)
  #expect(changed)
  #expect(state == .processing)
  changed = state.release(toggle: false)
  #expect(!changed)
  changed = state.press(toggle: true)
  #expect(!changed)
  state = .idle
  changed = state.press(toggle: true)
  #expect(changed)
  changed = state.release(toggle: true)
  #expect(!changed)
  changed = state.press(toggle: true)
  #expect(changed)
  #expect(state == .processing)
}
@Test func retentionIsOptInAndSettingsHaveNoCredentialFields() throws {
  let config = Configuration()
  #expect(!config.retainHistory && !config.retainAudio)
  let text = String(decoding: try JSONEncoder().encode(config), as: UTF8.self)
  #expect(!text.contains("token"))
  #expect(!text.contains("password"))
  #expect(try config.validated().shortcutModifiers == 6144)
}

@Test func eventDecoderPreservesBoundariesAndMultilineData() throws {
  var decoder = SSEDecoder()
  var events: [String] = []
  for byte in Data(": ping\r\ndata: one\r\ndata: two\r\n\r\ndata: [DONE]\n\n".utf8) {
    if let event = try decoder.push(byte) { events.append(event) }
  }
  #expect(events == ["one\ntwo", "[DONE]"])
}

@Test func cleanupPickerOnlyOffersSupportedNamedGenerators() throws {
  let catalog = try JSONDecoder().decode(
    ModelCatalog.self,
    from: Data(
      """
      {"data":[{"id":"generators/cleanup","task":"generate"},
      {"id":"connections/vendor/models/model","task":"generate"},
      {"id":"chains/cleanup","task":"generate"},{"id":"local/model","task":"generate"}]}
      """.utf8))
  var preset = Preset(id: "test", name: "Test")
  #expect(preset.cleanupChoices(from: catalog.data).map(\.id) == ["generators/cleanup"])
  preset.model = "local/openai/whisper-small"
  #expect(preset.cleanupChoices(from: catalog.data).isEmpty)
  var config = Configuration()
  config.presets[0].cleanupModel = "connections/vendor/models/model"
  #expect(throws: DictationError.self) { try config.validated() }
  config.presets[0].model = "openai/whisper-small"
  config.presets[0].cleanupModel = "generators/cleanup"
  #expect(throws: DictationError.self) { try config.validated() }
}

@Test func serverOriginValidationAcceptsLocalhostAndTrimsPastedWhitespace() throws {
  for address in ["http://localhost:8080", "http://127.0.0.1:8080", "https://example.com/"] {
    var config = Configuration()
    config.connectionMode = .external
    config.server = " \n" + address + "\t "
    #expect(try config.validated().server == address)
  }
  for (address, explanation) in [
    (" ", "Enter a server address"), ("localhost:8080", "must start"),
    ("http://user:pass@localhost:8080", "Remove credentials"),
    ("http://localhost:8080/api", "without a path"),
  ] {
    var config = Configuration()
    config.connectionMode = .external
    config.server = address
    do {
      _ = try config.validated()
      Issue.record("Accepted invalid server address")
    } catch {
      #expect(error.localizedDescription.contains(explanation))
    }
  }
}

@Test func hostedDiscoveryPreservesSharedCleanupAndRuntimeDiscoveryClearsIt() throws {
  let catalog = try JSONDecoder().decode(
    ModelCatalog.self,
    from: Data(
      """
      {"data":[
        {"id":"connections/openai/models/whisper-1","task":"transcribe","dictation":{"live_preview":false,"recognition_hints":false,"text_cleanup":true,"executor":"openai"}},
        {"id":"connections/local-inference/models/openai/whisper-tiny","task":"transcribe","dictation":{"live_preview":false,"recognition_hints":false,"text_cleanup":false}},
        {"id":"generators/cleanup","task":"generate"},
        {"id":"transcribers/whisper","task":"transcribe"}
      ]}
      """.utf8))
  var preset = Preset(id: "test", name: "Test")
  preset.cleanupModel = "generators/cleanup"
  preset.selectTranscriber(catalog.data[0])
  #expect(catalog.data[0].executorName == "openai")
  #expect(preset.usesNamedTranscriber && preset.cleanupModel == "generators/cleanup")
  #expect(preset.cleanupChoices(from: catalog.data).map(\.id) == ["generators/cleanup"])
  var config = Configuration()
  config.presets[0] = preset
  config.selectedPreset = preset.id
  #expect(try Configuration.decode(JSONEncoder().encode(config)).presets[0].usesNamedTranscriber)
  preset.selectTranscriber(catalog.data[1])
  #expect(!preset.usesNamedTranscriber && preset.cleanupModel.isEmpty)
  #expect(catalog.transcriber("whisper")?.id == "transcribers/whisper")
}

@Test func validatedSettingsAlwaysRoundTripWithinTheFileLimit() throws {
  var configuration = Configuration()
  configuration.dictionary = []
  configuration.presets = (0..<64).map { index in
    var preset = Preset(id: "preset-\(index)", name: "Preset \(index)")
    preset.dictionary = Array(repeating: String(repeating: "a", count: 128), count: 256)
    return preset
  }
  configuration.selectedPreset = "preset-0"
  #expect(throws: DictationError.self) { try configuration.validated() }
  configuration.presets = Array(configuration.presets.prefix(8))
  let valid = try configuration.validated()
  let encoded = try JSONEncoder().encode(valid)
  #expect(encoded.count <= Configuration.maxEncodedBytes)
  #expect(try Configuration.decode(encoded) == valid)
}
