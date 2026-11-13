// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

public enum SpeechRecoveryContext: Equatable, Sendable {
  case onThisMac
  case externalServer(String)
  var serverLabel: String {
    guard case .externalServer(let origin) = self,
      var parts = URLComponents(string: origin),
      ["http", "https"].contains(parts.scheme), parts.host != nil
    else { return "the external Antfly server" }
    parts.user = nil
    parts.password = nil
    parts.path = ""
    parts.query = nil
    parts.fragment = nil
    return parts.string ?? "the external Antfly server"
  }
}

public enum SpeechReadiness: Equatable, Sendable {
  case ready, checking, unverified
  case assetsNeeded(String)
  case unavailable(String)
  public var isReady: Bool { self == .ready }
  public var message: String {
    switch self {
    case .ready: return "Speech is ready."
    case .checking: return "Checking the speech engine…"
    case .unverified: return "Speech readiness is not verified. Check the engine or test dictation."
    case .assetsNeeded(let language):
      return
        "Speech assets for \(language) are not installed on the processing server."
    case .unavailable(let reason): return reason
    }
  }
  public func message(for context: SpeechRecoveryContext) -> String {
    switch (self, context) {
    case (.assetsNeeded(let language), .onThisMac):
      return "Speech assets for " + language
        + " are not installed on this Mac. Use Install / check speech in Settings → Speech & text."
    case (.assetsNeeded(let language), .externalServer):
      return "Speech assets for " + language + " are not installed on " + context.serverLabel
        + ". Ask its administrator to install/check that language or enable download_assets on the server’s Mac."
    case (.unavailable(let reason), .externalServer): return context.serverLabel + ": " + reason
    default: return message
    }
  }
}

public enum ModelDiscoveryState: Equatable, Sendable {
  case unchecked, checking, current
  case failed(String)
}

extension DiscoveredModel {
  public var isAppleSpeech: Bool {
    executorName == "apple" || dictation?.native_status != nil || id == "transcribers/apple-speech"
  }
  public func speechReadiness(language: String, defaultLanguage: String? = nil) -> SpeechReadiness {
    if let error = dictation?.readiness_error {
      return .unavailable("Speech needs attention: " + error)
    }
    guard isAppleSpeech else {
      return availability == "ready" || availability == "loaded" ? .ready : .unverified
    }
    guard let native = dictation?.native_status, let available = native.speech_available else {
      return .unverified
    }
    guard available else {
      return .unavailable("Apple speech is unavailable. Choose another speech engine.")
    }
    let requested = language.isEmpty ? defaultLanguage : language
    guard let requested, let installed = native.speech_installed_locales else { return .unverified }
    if installed.contains(where: { Self.localeKey($0) == Self.localeKey(requested) }) {
      return .ready
    }
    // The released inventory does not report supportedLocale(equivalentTo:).
    // A different regional asset may satisfy this request; verify rather than guess.
    let language = Self.localeKey(requested).split(separator: "-").first
    if installed.contains(where: { Self.localeKey($0).split(separator: "-").first == language }) {
      return .unverified
    }
    return .assetsNeeded(requested)
  }
  public static func localeKey(_ locale: String) -> String {
    locale.replacingOccurrences(of: "_", with: "-").lowercased()
  }
}

public enum ModelSetupOperation: String, Equatable, Sendable {
  case appleSpeech, whisper
  public var label: String { self == .appleSpeech ? "Apple speech" : "Whisper" }
  public var transcriber: String {
    self == .appleSpeech ? "transcribers/apple-speech" : "transcribers/whisper"
  }
}

public enum ProcessingResolution: Equatable, Sendable { case requireCompatible, automatic }
public struct ManagedProcessingConflict: LocalizedError, Equatable, Sendable {
  public let presets: [String]
  public var errorDescription: String? {
    "The selected speech engine cannot use these presets’ processing strategies: "
      + presets.joined(separator: ", ")
      + ". Choose Automatic processing for these presets to continue."
  }
}
public struct ModelSetupResult: Equatable, Sendable {
  public let operation: ModelSetupOperation
  public let language: String?
  public var warning: String?
  public init(operation: ModelSetupOperation, language: String? = nil, warning: String? = nil) {
    self.operation = operation
    self.language = language
    self.warning = warning
  }
  public var installationMessage: String {
    if operation == .appleSpeech, let language {
      return "Apple speech is installed and checked for " + language + "."
    }
    return operation.label + " is installed."
  }
}

public enum CleanupResolution: Equatable, Sendable { case requireCompatible, verbatim, bundled }
public struct ManagedCleanupConflict: LocalizedError, Equatable, Sendable {
  public let presets: [String]
  public let canUseBundledProcessor: Bool
  public var errorDescription: String? {
    "These presets use text processors unavailable on the bundled engine: "
      + presets.joined(separator: ", ")
      + ". Choose a compatible processor, or use Verbatim for these presets."
  }
}

public enum ModelSetupState: Equatable, Sendable {
  case idle
  case running(ModelSetupOperation, String)
  case ready(ModelSetupResult)
  case installedNotSaved(ModelSetupResult, String)
  case installedNotSelected(ModelSetupResult)
  case canceled(ModelSetupOperation)
  case failed(ModelSetupOperation, String)
  case blocked(ModelSetupOperation, ManagedCleanupConflict)
  case blockedProcessing(ModelSetupOperation, ManagedProcessingConflict)
  public var operation: ModelSetupOperation? {
    switch self {
    case .idle: return nil
    case .running(let op, _), .canceled(let op), .failed(let op, _), .blocked(let op, _),
      .blockedProcessing(let op, _):
      return op
    case .ready(let result), .installedNotSaved(let result, _), .installedNotSelected(let result):
      return result.operation
    }
  }
  public var isRunning: Bool {
    if case .running = self { return true }
    return false
  }
  public var message: String {
    switch self {
    case .idle: return ""
    case .running(_, let stage): return stage
    case .ready(let result):
      return result.installationMessage + " Settings saved. Try a test dictation."
        + (result.warning.map { " Needs attention: " + $0 } ?? "")
    case .installedNotSaved(let result, let reason):
      return result.installationMessage
        + " Settings could not be saved; previous preset selections remain active. " + reason
    case .installedNotSelected(let result):
      return result.installationMessage + " Previous preset selections were kept."
    case .canceled(let op): return op.label + " setup canceled. Retry when ready."
    case .failed(let op, let error): return op.label + " setup failed: " + error
    case .blocked(_, let conflict): return conflict.localizedDescription
    case .blockedProcessing(_, let conflict): return conflict.localizedDescription
    }
  }
}

extension Preset {
  public func supportsProcessing(with model: DiscoveredModel?) -> Bool {
    (try? ProcessingPlan(preset: self, model: model)) != nil
  }
  public func hasCompatibleSharedProcessor(in catalog: ModelCatalog) -> Bool {
    let name = cleanupModel.trimmingCharacters(in: .whitespacesAndNewlines)
    if name.isEmpty { return true }
    let identifier = name.hasPrefix("generators/") ? name : "generators/" + name
    return catalog.data.contains { $0.task == "generate" && $0.id == identifier }
  }
}

extension Configuration {
  /// Construct a candidate without modifying any presets until setup succeeds.
  public func selectingManagedTranscriber(
    _ operation: ModelSetupOperation, catalog: ModelCatalog,
    resolution: CleanupResolution = .requireCompatible,
    processing: ProcessingResolution = .requireCompatible
  ) throws -> Configuration {
    let generators = Set(
      catalog.data.filter { $0.task == "generate" && $0.id.hasPrefix("generators/") }.map(\.id))
    let incompatible = presets.filter { !$0.hasCompatibleSharedProcessor(in: catalog) }
    let bundled = generators.contains("generators/cleanup")
    if !incompatible.isEmpty
      && (resolution == .requireCompatible || (resolution == .bundled && !bundled))
    {
      throw ManagedCleanupConflict(
        presets: incompatible.map { $0.name + " (" + $0.cleanupModel + ")" },
        canUseBundledProcessor: bundled)
    }
    var candidate = self
    for index in candidate.presets.indices {
      if !candidate.presets[index].hasCompatibleSharedProcessor(in: catalog) {
        switch resolution {
        case .requireCompatible: break  // Rejected above.
        case .verbatim:
          candidate.presets[index].cleanupModel = ""
          candidate.presets[index].outputMode = .verbatim
        case .bundled: candidate.presets[index].cleanupModel = "generators/cleanup"
        }
      }
      candidate.presets[index].automaticallySelectLocalTranscriber = false
      candidate.presets[index].model = operation.transcriber
      candidate.presets[index].transcriberRoute = .gateway
      if candidate.presets[index].cleanupModel.isEmpty {
        candidate.presets[index].outputMode = .verbatim
      }
    }
    let model = catalog.transcriber(operation.transcriber)
    let unsupported = candidate.presets.indices.filter {
      !candidate.presets[$0].supportsProcessing(with: model)
    }
    if !unsupported.isEmpty && processing == .requireCompatible {
      throw ManagedProcessingConflict(presets: unsupported.map { candidate.presets[$0].name })
    }
    for index in unsupported { candidate.presets[index].processingStrategy = .automatic }
    return try candidate.validated()
  }
}
