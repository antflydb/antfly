// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

/// Local formatting is independent of recognition and generative cleanup.
public enum VoiceFormatting {
  public struct Output: Equatable, Sendable {
    public var text: String
    public var pressEnter: Bool
  }
  private static let trailingEnter = try! NSRegularExpression(
    pattern: #"(?i)\bpress\s+enter[.!?]?\s*$"#)
  private static let breaks = try! NSRegularExpression(
    pattern:
      #"(?i)\b(start a new paragraph|new paragraph|new line|next line|line break|skip a line)\b"#)
  private static let quotes = try! NSRegularExpression(pattern: #"\"[^\"]*\"|“[^”]*”"#)

  public static func format(_ text: String, commands: Bool = true, pressEnter: Bool = false)
    -> Output
  {
    var result = text
    var enter = false
    if pressEnter,
      let match = trailingEnter.firstMatch(
        in: result, range: NSRange(result.startIndex..., in: result))
    {
      let quoted = quotes.matches(in: result, range: NSRange(result.startIndex..., in: result))
      if !quoted.contains(where: { NSIntersectionRange($0.range, match.range).length > 0 }) {
        result = (result as NSString).substring(to: match.range.location)
          .trimmingCharacters(in: .whitespaces)
        enter = true
      }
    }
    if commands {
      let quoted = quotes.matches(in: result, range: NSRange(result.startIndex..., in: result)).map(
        \.range)
      let source = result as NSString
      var formatted = ""
      formatted.reserveCapacity(result.utf8.count)
      var cursor = 0
      var quoteIndex = 0
      for match in breaks.matches(in: result, range: NSRange(location: 0, length: source.length)) {
        while quoteIndex < quoted.count, NSMaxRange(quoted[quoteIndex]) <= match.range.location {
          quoteIndex += 1
        }
        if quoteIndex < quoted.count,
          NSIntersectionRange(quoted[quoteIndex], match.range).length > 0
        {
          continue
        }
        var start = match.range.location
        var end = NSMaxRange(match.range)
        while start > cursor, [32, 9].contains(source.character(at: start - 1)) { start -= 1 }
        while end < source.length, [32, 9, 44, 46, 58, 59].contains(source.character(at: end)) {
          end += 1
        }
        start = max(cursor, start)
        formatted += source.substring(with: NSRange(location: cursor, length: start - cursor))
        let phrase = source.substring(with: match.range).lowercased()
        formatted += phrase.contains("paragraph") ? "\n\n" : "\n"
        cursor = end
      }
      formatted += source.substring(from: cursor)
      result = formatted
    }
    return Output(text: result, pressEnter: enter)
  }

  public static func finalize(raw: String, text: String, commands: Bool, pressEnter: Bool) -> Output
  {
    let recognized = format(raw, commands: commands, pressEnter: pressEnter)
    let action = recognized.pressEnter
    var output = format(text, commands: commands, pressEnter: action)
    if commands {
      // Cleanup often trims edge whitespace. Preserve deliberate spoken breaks
      // even when the generator drops them, including a line-break-only utterance.
      let leading = recognized.text.prefix(while: { $0 == "\n" }).count
      let trailing = recognized.text.reversed().prefix(while: { $0 == "\n" }).count
      if recognized.text.allSatisfy({ $0 == "\n" }), !recognized.text.isEmpty {
        output.text = recognized.text
      } else {
        let existingLeading = output.text.prefix(while: { $0 == "\n" }).count
        let existingTrailing = output.text.reversed().prefix(while: { $0 == "\n" }).count
        output.text =
          String(repeating: "\n", count: max(0, leading - existingLeading))
          + output.text + String(repeating: "\n", count: max(0, trailing - existingTrailing))
      }
    }
    output.pressEnter = action && !output.text.isEmpty
    return output
  }

  /// Adds only boundary spaces. Existing whitespace, punctuation, and selected
  /// text replacements are preserved; URLs and decimal punctuation are untouched.
  public static func spaced(_ text: String, before: String, after: String) -> String {
    guard !text.isEmpty else { return text }
    func needsSpace(_ left: Character?, _ right: Character?) -> Bool {
      guard let left, let right, !left.isWhitespace, !right.isWhitespace else { return false }
      if ",.;:!?)]}’'\"".contains(right) || "([{‘'\"".contains(left) { return false }
      let cjk: (Character) -> Bool = { char in
        char.unicodeScalars.contains {
          (0x3000...0x9FFF).contains($0.value) || (0xAC00...0xD7AF).contains($0.value)
        }
      }
      if cjk(left) || cjk(right) { return false }
      return (left.isLetter || left.isNumber || ".!?)]}".contains(left))
        && (right.isLetter || right.isNumber || "([{".contains(right))
    }
    return (needsSpace(before.last, text.first) ? " " : "") + text
      + (needsSpace(text.last, after.first) ? " " : "")
  }
}

/// Sending a key requires observed insertion, not merely posting a paste event.
@MainActor public enum PasteConfirmation {
  public static func wait(
    validate: () throws -> Void, confirmed: () -> Bool,
    sleep: () async throws -> Void = { try await Task.sleep(for: .milliseconds(50)) }
  ) async throws {
    for _ in 0..<20 {
      try Task.checkCancellation()
      try validate()
      if confirmed() { return }
      try await sleep()
    }
    throw DictationError.invalid(
      "Paste sent; Enter was not sent because insertion could not be confirmed.")
  }
}
