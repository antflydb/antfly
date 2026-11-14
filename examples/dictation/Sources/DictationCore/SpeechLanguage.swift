// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

public enum SpeechLanguageChoice: String, Codable, CaseIterable, Sendable {
  case system, detect, custom, server
  public var label: String {
    switch self {
    case .system: return "Use Mac language"
    case .detect: return "Detect spoken language"
    case .custom: return "Choose a language"
    case .server: return "Use server default"
    }
  }
}

public enum SpeechLanguages {
  public static let common = [
    "en-US", "en-GB", "es-ES", "fr-FR", "de-DE", "it-IT", "pt-BR", "ja-JP", "ko-KR", "zh-CN",
  ]
  public static func normalized(_ value: String) -> String {
    value.replacingOccurrences(of: "_", with: "-").split(separator: "@").first.map(String.init)
      ?? value
  }
  public static func primary(_ value: String) -> String {
    normalized(value).split(separator: "-").first.map { $0.lowercased() } ?? ""
  }
  public static func system(
    preferred: [String] = Locale.preferredLanguages, supported: [String]? = nil
  ) throws -> String {
    let preferences = preferred.filter { !primary($0).isEmpty }
    guard !preferences.isEmpty else {
      throw DictationError.invalid(
        "Choose a speech language in Presets; the Mac has no preferred language.")
    }
    guard let supported else { return normalized(preferences[0]) }
    for candidate in preferences {
      if let exact = supported.first(where: {
        normalized($0).lowercased() == normalized(candidate).lowercased()
      }) {
        return normalized(exact)
      }
      let script = Locale.Language(identifier: normalized(candidate)).maximalIdentifier
      let candidateScript = Locale.Language(identifier: script).script?.identifier
      if let equivalent = supported.first(where: {
        guard primary($0) == primary(candidate) else { return false }
        let maximal = Locale.Language(identifier: normalized($0)).maximalIdentifier
        return Locale.Language(identifier: maximal).script?.identifier == candidateScript
      }) {
        return normalized(equivalent)
      }
    }
    throw DictationError.invalid(
      "Your Mac’s preferred languages are not supported by this speech engine. Choose a speech language in Presets."
    )
  }
  public static func name(_ code: String) -> String {
    Locale.current.localizedString(forIdentifier: code) ?? code
  }
}

extension Preset {
  public var speechLanguageChoice: SpeechLanguageChoice {
    get {
      languageChoice
        ?? (language.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? .system : .custom)
    }
    set { languageChoice = newValue }
  }
  public func resolvingLanguage(
    preferred: [String] = Locale.preferredLanguages,
    supported: [String]? = nil, detectsLanguage: Bool = false, whisper: Bool = false
  ) throws -> Preset {
    var result = self
    switch speechLanguageChoice {
    case .system:
      result.language = try SpeechLanguages.system(preferred: preferred, supported: supported)
    case .custom:
      result.language = SpeechLanguages.normalized(
        language.trimmingCharacters(in: .whitespacesAndNewlines))
      guard !result.language.isEmpty else {
        throw DictationError.invalid("Choose a speech language in Presets.")
      }
    case .detect:
      guard detectsLanguage else {
        throw DictationError.invalid(
          "This speech engine does not advertise language detection. Use Mac language or choose a language in Presets."
        )
      }
      result.language = ""
    case .server: result.language = ""
    }
    if whisper && !result.language.isEmpty {
      result.language = SpeechLanguages.primary(result.language)
    }
    // Request snapshots carry the resolved value, independent of later system changes.
    result.languageChoice = result.language.isEmpty ? .server : .custom
    return result
  }
}

extension Preset {
  /// Whisper installs independently of Apple locale support and detects language
  /// when requested. Apple setup alone needs a concrete supported asset locale.
  public func setupLanguage(
    for operation: ModelSetupOperation, supportedAppleLanguages: [String]? = nil
  ) throws -> String {
    guard operation == .appleSpeech else { return "" }
    let resolved = try resolvingLanguage(supported: supportedAppleLanguages)
    guard !resolved.language.isEmpty else {
      throw DictationError.invalid(
        "Apple speech needs a selected language. Choose Use Mac language or a language in Presets.")
    }
    return resolved.language
  }
}

/// Language contracts follow the resolved backend, never the spelling of a
/// user-defined alias. Optional advertised capabilities override known-model
/// compatibility rules, including explicit false for detection.
public struct SpeechLanguageContract: Sendable {
  public enum Format: String, Sendable {
    case languageCode = "language_code"
    case bcp47
  }
  public let format: Format
  public let detectsLanguage: Bool

  public init(model: DiscoveredModel?, requested: String) {
    let backend = model?.backend_model?.trimmingCharacters(in: .whitespacesAndNewlines)
    let identifier = backend.flatMap { $0.isEmpty ? nil : $0 } ?? model?.id ?? requested
    // A real catalog backend takes precedence over the public alias, even if
    // that alias happens to be named whisper. Unknown backends stay unchanged.
    let knownWhisper: Bool
    if backend?.isEmpty == false {
      knownWhisper = Self.isWhisper(identifier)
    } else if model?.isAppleSpeech == true {
      knownWhisper = false
    } else {
      knownWhisper = Self.isWhisper(identifier) || identifier == "transcribers/whisper"
    }
    if let advertised = model?.dictation?.language_format {
      format = Format(rawValue: advertised) ?? .bcp47
    } else {
      format = knownWhisper ? .languageCode : .bcp47
    }
    detectsLanguage =
      model?.dictation?.language_detection
      ?? (knownWhisper && !identifier.hasSuffix(".en"))
  }
  private static func isWhisper(_ identifier: String) -> Bool {
    let name = identifier.hasPrefix("openai/") ? String(identifier.dropFirst(7)) : identifier
    return name == "whisper" || name.hasPrefix("whisper-")
  }
}
