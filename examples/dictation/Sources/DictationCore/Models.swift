// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

public enum DictationError: Error, LocalizedError {
  case invalid(String)
  public var errorDescription: String? {
    if case .invalid(let message) = self { return message }
    return nil
  }
}

public struct Preset: Codable, Identifiable, Equatable, Sendable {
  public var id: String
  public var name: String
  public var model: String
  public var cleanupModel: String
  public var language: String
  public var style: String
  public var context: String
  public var instructions: String
  public var dictionary: [String]
  public var appIDs: [String]
  public var livePreview: Bool
  public init(id: String, name: String, style: String = "clean", context: String = "") {
    self.id = id
    self.name = name
    self.style = style
    self.context = context
    model = "transcribers/whisper"
    cleanupModel = ""
    language = ""
    instructions = ""
    dictionary = []
    appIDs = []
    livePreview = false
  }
}

extension Preset {
  /// Shared cleanup is a gateway feature of named transcribers. Runtime model
  /// requests retain their existing raw-model cleanup contract.
  public var usesNamedTranscriber: Bool { model.hasPrefix("transcribers/") || !model.contains("/") }
  public func cleanupChoices(from catalog: [DiscoveredModel]) -> [DiscoveredModel] {
    usesNamedTranscriber
      ? catalog.filter { $0.task == "generate" && $0.id.hasPrefix("generators/") } : []
  }
  public mutating func selectTranscriber(_ selection: DiscoveredModel) {
    model = selection.id
    if selection.dictation?.live_preview == false { livePreview = false }
    if !usesNamedTranscriber && cleanupModel.hasPrefix("generators/") { cleanupModel = "" }
  }
}

public struct Configuration: Codable, Equatable, Sendable {
  public var version = 1
  public var server = "http://localhost:8080"
  public var table = "dictation"
  public var profile = "default"
  public var shortcutKey: UInt32 = 49  // Space
  public var shortcutModifiers: UInt32 = 6144  // Control + Option (Carbon)
  public var toggle = false
  public var autoPaste = true
  public var retainHistory = false
  public var retainAudio = false
  public var microphoneID = ""
  public var selectedPreset = "slack"
  public var dictionary = ["Antfly", "Colony"]
  public var presets: [Preset] = [
    Preset(id: "slack", name: "Slack", context: "a Slack message"),
    Preset(id: "email", name: "Email", style: "formal", context: "an email"),
    Preset(id: "verbatim", name: "Verbatim", style: "verbatim"),
  ]
  public init() {}
  public func preset(for appID: String?) -> Preset {
    if let appID, let match = presets.first(where: { $0.appIDs.contains(appID) }) { return match }
    return presets.first(where: { $0.id == selectedPreset }) ?? presets[0]
  }
  public func validated() throws -> Configuration {
    guard version == 1 else { throw DictationError.invalid("Unsupported settings version.") }
    guard let url = URL(string: server), ["http", "https"].contains(url.scheme), url.host != nil,
      url.user == nil, url.password == nil, url.query == nil, url.fragment == nil,
      url.path.isEmpty || url.path == "/"
    else {
      throw DictationError.invalid(
        "Server must be an HTTP(S) origin without credentials or a path.")
    }
    guard !presets.isEmpty, presets.count <= 64, Set(presets.map(\.id)).count == presets.count,
      presets.contains(where: { $0.id == selectedPreset })
    else { throw DictationError.invalid("Choose a valid preset; preset IDs must be unique.") }
    for value in [table, profile] {
      guard !value.isEmpty, value.count <= 64,
        value.allSatisfy({ $0.isASCII && ($0.isLetter || $0.isNumber || $0 == "_" || $0 == "-") })
      else {
        throw DictationError.invalid(
          "Table and profile must use letters, numbers, underscores, or hyphens.")
      }
    }
    guard shortcutKey < 128, shortcutModifiers & 0x1b00 != 0,
      shortcutModifiers & ~UInt32(0x1b00) == 0
    else { throw DictationError.invalid("Shortcut needs a modifier and a valid macOS key code.") }
    for preset in presets {
      guard !preset.model.isEmpty,
        ["clean", "formal", "casual", "verbatim"].contains(preset.style),
        preset.context.utf8.count <= 4096, preset.instructions.utf8.count <= 4096
      else { throw DictationError.invalid("Invalid preset model, style, or instructions.") }
      if preset.cleanupModel.hasPrefix("connections/") || preset.cleanupModel.hasPrefix("chains/")
        || preset.cleanupModel.hasPrefix("local/")
      {
        throw DictationError.invalid(
          "Cleanup must use a configured generator name, or a raw generator model for a runtime transcriber."
        )
      }
      if !preset.usesNamedTranscriber && preset.cleanupModel.hasPrefix("generators/") {
        throw DictationError.invalid(
          "Choose a named transcriber to use shared named cleanup, or enter a raw cleanup model for the runtime transcriber."
        )
      }
      let terms = dictionary + preset.dictionary
      guard terms.count <= 256,
        terms.allSatisfy({
          !$0.isEmpty && $0.utf8.count <= 128 && !$0.contains("\n") && !$0.contains("\r")
        })
      else {
        throw DictationError.invalid(
          "Dictionary allows 256 single-line entries of up to 128 bytes.")
      }
    }
    return self
  }
  public static func decode(_ data: Data) throws -> Configuration {
    guard data.count <= 1_048_576 else {
      throw DictationError.invalid("Settings file exceeds 1 MiB.")
    }
    return try JSONDecoder().decode(Configuration.self, from: data).validated()
  }
}

public struct HistoryEntry: Codable, Identifiable, Equatable, Sendable {
  public var id: String = UUID().uuidString
  public var created: Date = Date()
  public var preset: String
  public var model: String
  public var application: String
  public var raw: String
  public var text: String
  public var status: String
  public init(
    preset: String, model: String, application: String, raw: String, text: String, status: String
  ) {
    self.preset = preset
    self.model = model
    self.application = application
    self.raw = raw
    self.text = text
    self.status = status
  }
}

public enum RecordingState: Equatable, Sendable {
  case idle, recording, processing
  public mutating func press(toggle: Bool) -> Bool {
    if self == .idle {
      self = .recording
      return true
    }
    if self == .recording && toggle {
      self = .processing
      return true
    }
    return false
  }
  public mutating func release(toggle: Bool) -> Bool {
    guard self == .recording && !toggle else { return false }
    self = .processing
    return true
  }
}

public struct DictationResult: Sendable {
  public var raw: String
  public var text: String
  public init(raw: String, text: String) {
    self.raw = raw
    self.text = text
  }
}

public struct DiscoveredModel: Decodable, Identifiable, Sendable {
  public struct DictationCapabilities: Decodable, Sendable {
    public var live_preview: Bool
    public var recognition_hints: Bool
    public var readiness_error: String?
  }
  public var id: String
  public var task: String?
  public var executor: String?
  public var dictation: DictationCapabilities?
}
public struct ModelCatalog: Decodable, Sendable {
  public var data: [DiscoveredModel]
}
