// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

public enum ConnectionMode: String, Codable, Sendable { case managed, external }

public enum TranscriberRoute: String, Codable, Sendable { case gateway, runtime }

/// The complete recording is intact; only the bounded live queue overflowed.
public struct LiveAudioOverflow: Error, LocalizedError, Sendable {
  public init() {}
  public var errorDescription: String? {
    "Live audio processing fell behind. The complete recording remains available for clip processing."
  }
}

public enum DictationError: Error, LocalizedError {
  case invalid(String)
  public var errorDescription: String? {
    if case .invalid(let message) = self { return message }
    if case .cleanupFailed(_, let message) = self { return message }
    return nil
  }
  case cleanupFailed(raw: String, message: String)
  public var rawTranscript: String? {
    if case .cleanupFailed(let raw, _) = self { return raw }
    return nil
  }
}

public struct Preset: Codable, Identifiable, Equatable, Sendable {
  public var id: String
  public var name: String
  public var model: String
  public var cleanupModel: String
  public var language: String
  public var languageChoice: SpeechLanguageChoice?
  public var fasterAppleSpeech: Bool?
  public var style: String
  public var context: String
  public var instructions: String
  public var dictionary: [String]
  public var appIDs: [String]
  public var mode: DictationMode?
  public var processing: ProcessingStrategy?
  public var livePreview: Bool
  public var automaticallySelectLocalTranscriber: Bool? = true
  public var transcriberRoute: TranscriberRoute?
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
  public var usesNamedTranscriber: Bool {
    if let transcriberRoute { return transcriberRoute == .gateway }
    return model.hasPrefix("transcribers/") || model.hasPrefix("connections/")
      || !model.contains("/")
  }
  public func cleanupChoices(from catalog: [DiscoveredModel]) -> [DiscoveredModel] {
    usesNamedTranscriber
      ? catalog.filter { $0.task == "generate" && $0.id.hasPrefix("generators/") } : []
  }
  public mutating func selectTranscriber(_ selection: DiscoveredModel) {
    automaticallySelectLocalTranscriber = false
    model = selection.id
    transcriberRoute =
      selection.dictation?.text_cleanup == true || model.hasPrefix("transcribers/")
      ? .gateway : .runtime
    if selection.dictation?.live_preview == false { livePreview = false }
    if !usesNamedTranscriber && cleanupModel.hasPrefix("generators/") { cleanupModel = "" }
  }
}

public struct Configuration: Codable, Equatable, Sendable {
  public static let maxEncodedBytes = 1_048_576
  public var version = 1
  /// Missing in older settings: preserve the user’s existing external server.
  public var connectionMode: ConnectionMode? = .managed
  public var usesManagedRuntime: Bool { connectionMode == .managed }
  public static let managedServerOrigin = "http://127.0.0.1:47831"
  public var effectiveServer: String { usesManagedRuntime ? Self.managedServerOrigin : server }
  public var server = "http://localhost:8080"
  public var table = "dictation"
  public var profile = "default"
  public var shortcut: KeyboardShortcut? = .fn
  public var effectiveShortcut: KeyboardShortcut {
    get { shortcut ?? KeyboardShortcut(legacyKey: shortcutKey, legacyModifiers: shortcutModifiers) }
    set { shortcut = newValue }
  }
  public var shortcutKey: UInt32 = 49  // Space
  public var shortcutModifiers: UInt32 = 6144  // Control + Option (Carbon)
  public var toggle = false
  public var autoPaste = true
  public var voiceFormatting: Bool?
  public var smartSpacing: Bool?
  public var pressEnterCommand: Bool?
  public var usesVoiceFormatting: Bool { voiceFormatting ?? true }
  public var usesSmartSpacing: Bool { smartSpacing ?? true }
  public var usesPressEnterCommand: Bool { pressEnterCommand ?? false }
  public var retainHistory = false
  public var retainAudio = false
  public var concurrentRequests: Int?
  public var prewarmCleanup: Bool?
  public var usesCleanupPrewarming: Bool { prewarmCleanup ?? true }
  public var microphoneID = ""
  public var selectedPreset = "slack"
  public var dictionary = ["Antfly", "Colony"]
  public var presets: [Preset] = [
    Preset(id: "slack", name: "Messages", context: "a message"),
    Preset(id: "email", name: "Writing", style: "formal", context: "an email"),
    Preset(id: "verbatim", name: "Verbatim", style: "verbatim"),
  ]
  public init() {}
  public func preset(for appID: String?) -> Preset {
    if let appID, let match = presets.first(where: { $0.appIDs.contains(appID) }) { return match }
    return presets.first(where: { $0.id == selectedPreset }) ?? presets[0]
  }
  public func validated() throws -> Configuration {
    guard version == 1 else { throw DictationError.invalid("Unsupported settings version.") }
    let origin = server.trimmingCharacters(in: .whitespacesAndNewlines)
    if !usesManagedRuntime {
      guard !origin.isEmpty else {
        throw DictationError.invalid("Enter a server address, such as http://localhost:8080.")
      }
      guard let url = URL(string: origin), ["http", "https"].contains(url.scheme),
        let host = url.host, !host.isEmpty
      else {
        throw DictationError.invalid(
          "Server address must start with http:// or https:// and include a hostname.")
      }
      guard url.user == nil, url.password == nil else {
        throw DictationError.invalid(
          "Remove credentials from the server address. Use the Bearer token field for authentication."
        )
      }
      guard url.query == nil, url.fragment == nil, url.path.isEmpty || url.path == "/" else {
        throw DictationError.invalid(
          "Use only the server origin, such as http://localhost:8080, without a path, query, or fragment."
        )
      }
    }
    guard (1...2).contains(concurrentRequests ?? 1) else {
      throw DictationError.invalid("Inference concurrency must be one or two.")
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
    _ = try effectiveShortcut.validated()
    if shortcut == nil && (shortcutModifiers & ~UInt32(0x1b00) != 0 || shortcutKey >= 128) {
      throw DictationError.invalid("Invalid legacy shortcut.")
    }
    for preset in presets {
      if preset.outputMode == .rewrite && preset.processingStrategy == .incremental {
        throw DictationError.invalid("Rewrite requires Automatic or Whole utterance processing.")
      }
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
    var normalized = self
    normalized.server = origin
    guard try JSONEncoder().encode(normalized).count <= Self.maxEncodedBytes else {
      throw DictationError.invalid(
        "Settings exceed 1 MiB. Reduce presets, dictionaries, or instructions before saving.")
    }
    return normalized
  }
  /// Only automatic local defaults follow readiness; explicit/custom selections survive discovery.
  @discardableResult public mutating func selectAvailableManagedTranscriber(
    from catalog: ModelCatalog, supportsApple: Bool,
    supportedAppleLanguages: [String]? = nil, nativeAppleStreaming: Bool = false
  ) -> Bool {
    guard usesManagedRuntime else { return false }
    let apple = catalog.transcriber("transcribers/apple-speech")
    var changed = false
    for index in presets.indices {
      guard presets[index].automaticallySelectLocalTranscriber != false,
        ["transcribers/whisper", "transcribers/apple-speech"].contains(presets[index].model)
      else { continue }
      let language = try? presets[index].resolvingLanguage(
        supported: supportedAppleLanguages,
        detectsLanguage: false
      ).language
      let ready =
        language.map { !($0.isEmpty) && apple?.speechReadiness(language: $0).isReady == true }
        ?? false
      var appleCandidate = presets[index]
      appleCandidate.model = "transcribers/apple-speech"
      appleCandidate.transcriberRoute = .gateway
      let selected =
        supportsApple && ready && appleCandidate.hasCompatibleSharedProcessor(in: catalog)
          && appleCandidate.supportsProcessing(
            with: apple, nativeAppleStreaming: nativeAppleStreaming)
        ? "transcribers/apple-speech" : "transcribers/whisper"
      var selectedCandidate = presets[index]
      selectedCandidate.model = selected
      selectedCandidate.transcriberRoute = .gateway
      guard
        selectedCandidate.supportsProcessing(
          with: catalog.transcriber(selected),
          nativeAppleStreaming: selected == "transcribers/apple-speech" && nativeAppleStreaming)
      else {
        continue
      }
      if presets[index].model != selected || presets[index].transcriberRoute != .gateway {
        presets[index].model = selected
        presets[index].transcriberRoute = .gateway
        changed = true
      }
    }
    return changed
  }

  public static func decode(_ data: Data) throws -> Configuration {
    guard data.count <= maxEncodedBytes else {
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
  /// Absent only for archives written before destinations were retained.
  public var destination: HistoryDestination?
  public init(
    preset: String, model: String, application: String, raw: String, text: String, status: String,
    destination: HistoryDestination? = nil
  ) {
    self.preset = preset
    self.model = model
    self.application = application
    self.raw = raw
    self.text = text
    self.status = status
    self.destination = destination
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
  public var formatting: VoiceFormatting.Output?
  public var raw: String
  public var text: String
  public var timing: DictationTiming?
  public init(raw: String, text: String, timing: DictationTiming? = nil) {
    self.raw = raw
    self.text = text
    self.timing = timing
  }
}

public struct DiscoveredModel: Decodable, Identifiable, Sendable {
  public struct NativeSpeechStatus: Decodable, Sendable {
    public var speech_available: Bool?
    public var speech_installed_locales: [String]?
  }
  public struct DictationCapabilities: Decodable, Sendable {
    public var stable_segments: Bool?
    public var text_cleanup: Bool?
    public var live_preview: Bool
    public var recognition_hints: Bool
    public var language_detection: Bool?
    public var readiness_error: String?
    public var native_status: NativeSpeechStatus?
    public var executor: String?
  }
  public var id: String
  public var task: String?
  public var executor: String?
  public var dictation: DictationCapabilities?
  public var availability: String?
  public var executorName: String { executor ?? dictation?.executor ?? "runtime" }
}
public struct ModelCatalog: Decodable, Sendable {
  public var data: [DiscoveredModel]
  public init(data: [DiscoveredModel]) { self.data = data }
  public func transcriber(_ identifier: String) -> DiscoveredModel? {
    data.first { $0.id == identifier || $0.id == "transcribers/" + identifier }
  }
}
