// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

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
        "Speech assets for \(language) are not installed. Install/check speech in Speech & text."
    case .unavailable(let reason): return reason
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
      return .unavailable("Apple speech is unavailable on this Mac. Choose another speech engine.")
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
  case ready(ModelSetupOperation)
  case canceled(ModelSetupOperation)
  case failed(ModelSetupOperation, String)
  case blocked(ModelSetupOperation, ManagedCleanupConflict)
  public var operation: ModelSetupOperation? {
    switch self {
    case .idle: return nil
    case .running(let op, _), .ready(let op), .canceled(let op), .failed(let op, _),
      .blocked(let op, _):
      return op
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
    case .ready(let op):
      return op == .appleSpeech
        ? "Apple speech is installed and checked for the selected language. Try a test dictation."
        : "Whisper is installed. Test dictation to verify recognition."
    case .canceled(let op): return op.label + " setup canceled. Retry when ready."
    case .failed(let op, let error): return op.label + " setup failed: " + error
    case .blocked(_, let conflict): return conflict.localizedDescription
    }
  }
}

extension Preset {
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
    resolution: CleanupResolution = .requireCompatible
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
    return try candidate.validated()
  }
}
