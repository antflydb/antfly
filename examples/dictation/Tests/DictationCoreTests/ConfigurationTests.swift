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
