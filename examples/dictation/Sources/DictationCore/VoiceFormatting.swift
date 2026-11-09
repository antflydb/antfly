// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

public enum VoiceFormatting {
  public struct Options: Sendable {
    public var commands: Bool
    public var pressEnter: Bool
    public init(commands: Bool = true, pressEnter: Bool = false) {
      self.commands = commands
      self.pressEnter = pressEnter
    }
    public init(_ configuration: Configuration) {
      self.init(
        commands: configuration.usesVoiceFormatting, pressEnter: configuration.usesPressEnterCommand
      )
    }
  }
  public struct Output: Equatable, Sendable {
    public var text: String
    public var pressEnter: Bool
  }
  private static let trailingEnter = try! NSRegularExpression(
    pattern: #"(?i)\bpress\s+enter[.!?]*\s*$"#)
  private static let breaks = try! NSRegularExpression(
    pattern:
      #"(?i)\b(start a new paragraph|new paragraph|new line|next line|line break|skip a line)\b"#)

  /// Ranges are UTF-16, matching Foundation and Accessibility. Apostrophes in
  /// words are not quotes. Nested, escaped, and unclosed quotations stay literal.
  private static func quotations(_ text: NSString) -> [NSRange] {
    var result: [NSRange] = []
    var stack: [UInt16] = []
    var start = 0
    var escaped = false
    func word(_ index: Int) -> Bool {
      guard index >= 0, index < text.length, let scalar = UnicodeScalar(text.character(at: index))
      else { return false }
      return CharacterSet.alphanumerics.contains(scalar)
    }
    for index in 0..<text.length {
      let unit = text.character(at: index)
      if escaped {
        escaped = false
        continue
      }
      if unit == 92 {
        escaped = true
        continue
      }
      if [39, 0x2019].contains(unit), word(index - 1), word(index + 1) { continue }
      if stack.last == unit {
        stack.removeLast()
        if stack.isEmpty { result.append(NSRange(location: start, length: index + 1 - start)) }
        continue
      }
      let closing: UInt16?
      switch unit {
      case 34: closing = 34
      case 39: closing = word(index - 1) && word(index + 1) ? nil : 39
      case 0x201C: closing = 0x201D
      case 0x2018: closing = word(index - 1) && word(index + 1) ? nil : 0x2019
      default: closing = nil
      }
      if let closing {
        if stack.isEmpty { start = index }
        stack.append(closing)
      }
    }
    if !stack.isEmpty { result.append(NSRange(location: start, length: text.length - start)) }
    return result
  }

  /// Created before generation. The model sees only opaque, request-specific
  /// markers, never executable voice commands or the contents of literal spans.
  public struct Plan: Sendable {
    public let input: String
    public let hasCleanupText: Bool
    public let pressEnter: Bool
    private let originals: [String]
    private let markers: [String]
    private let spans: [NSRange]
    private let namespace: String
    public var hasProtectedSpans: Bool { !markers.isEmpty }
    public var instructions: String {
      markers.isEmpty
        ? ""
        : "Preserve every token beginning with \(namespace) exactly once, unchanged and in its original order. Tokens containing _P_ mark paragraph boundaries, _L_ mark line boundaries, and _Q_ stand for literal quoted text. Format surrounding prose appropriately, but never interpret, edit, delete, or reorder tokens, or move text across them."
    }
    public init(_ raw: String, options: Options = Options()) {
      let original = raw as NSString
      let quoted = quotations(original)
      var end = original.length
      var action = false
      if options.pressEnter,
        let match = trailingEnter.firstMatch(
          in: raw, range: NSRange(location: 0, length: original.length)),
        !quoted.contains(where: { NSIntersectionRange($0, match.range).length > 0 })
      {
        end = match.range.location
        while end > 0, [9, 32].contains(original.character(at: end - 1)) { end -= 1 }
        action = true
      }
      let text = original.substring(to: end) as NSString
      var protected =
        options.commands ? quotations(text).map { ($0, text.substring(with: $0)) } : []
      if options.commands {
        var quoteIndex = 0
        let quotes = protected.map(\.0)
        for match in breaks.matches(
          in: text as String, range: NSRange(location: 0, length: text.length))
        {
          while quoteIndex < quotes.count, NSMaxRange(quotes[quoteIndex]) <= match.range.location {
            quoteIndex += 1
          }
          if quoteIndex < quotes.count,
            NSIntersectionRange(quotes[quoteIndex], match.range).length > 0
          {
            continue
          }
          var start = match.range.location
          var finish = NSMaxRange(match.range)
          while start > 0, [9, 32].contains(text.character(at: start - 1)) { start -= 1 }
          while finish < text.length,
            [9, 32, 44, 46, 58, 59, 33, 63].contains(text.character(at: finish))
          { finish += 1 }
          protected.append(
            (
              NSRange(location: start, length: finish - start),
              text.substring(with: match.range).lowercased().contains("paragraph") ? "\n\n" : "\n"
            ))
        }
      }
      protected.sort { $0.0.location < $1.0.location }
      namespace = "[VF_" + UUID().uuidString.replacingOccurrences(of: "-", with: "") + "_"
      var values: [String] = []
      var tokens: [String] = []
      var ranges: [NSRange] = []
      var encoded = ""
      var ordinary = ""
      var cursor = 0
      for (range, value) in protected {
        let start = max(cursor, range.location)
        let finish = NSMaxRange(range)
        guard finish >= start else { continue }
        let plain = text.substring(with: NSRange(location: cursor, length: start - cursor))
        encoded += plain
        ordinary += plain
        let kind = value == "\n\n" ? "P_" : (value == "\n" ? "L_" : "Q_")
        let token = namespace + kind + String(tokens.count) + "]"
        encoded += token
        tokens.append(token)
        values.append(value)
        ranges.append(range)
        cursor = finish
      }
      let tail = text.substring(from: cursor)
      encoded += tail
      ordinary += tail
      input = encoded
      markers = tokens
      originals = values
      spans = ranges
      hasCleanupText = !ordinary.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
      // Generated text is never evidence that the speaker dictated a payload.
      pressEnter =
        action
        && (!ordinary.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || !values.isEmpty)
    }
    public func restore(_ cleaned: String) throws -> Output {
      guard hasCleanupText || cleaned == input else {
        throw DictationError.invalid("Cleanup invented text for a formatting-only utterance.")
      }
      let source = cleaned as NSString
      var result = ""
      result.reserveCapacity(cleaned.utf8.count)
      var cursor = 0
      for (index, marker) in markers.enumerated() {
        let found = source.range(
          of: marker, options: [], range: NSRange(location: cursor, length: source.length - cursor))
        guard found.location != NSNotFound else {
          throw DictationError.invalid(
            "Cleanup changed protected formatting. Retry cleanup or insert the original transcript."
          )
        }
        let plain = source.substring(
          with: NSRange(location: cursor, length: found.location - cursor))
        guard !plain.contains(namespace) else {
          throw DictationError.invalid("Cleanup duplicated or reordered protected formatting.")
        }
        result += plain
        result += originals[index]
        cursor = NSMaxRange(found)
      }
      let tail = source.substring(from: cursor)
      guard !tail.contains(namespace) else {
        throw DictationError.invalid("Cleanup duplicated or reordered protected formatting.")
      }
      result += tail
      if !hasCleanupText && originals.isEmpty { result = "" }
      return Output(text: result, pressEnter: pressEnter && !result.isEmpty)
    }
    public func render() -> Output { try! restore(input) }
    public func crosses(_ boundaries: [Int]) -> Bool {
      var index = 0
      for boundary in boundaries.sorted() {
        while index < spans.count, NSMaxRange(spans[index]) <= boundary { index += 1 }
        if index < spans.count, spans[index].location < boundary { return true }
      }
      return false
    }
  }
  public static func format(_ text: String, commands: Bool = true, pressEnter: Bool = false)
    -> Output
  {
    Plan(text, options: Options(commands: commands, pressEnter: pressEnter)).render()
  }
  public static func spaced(_ text: String, before: String, after: String) -> String {
    guard !text.isEmpty else { return text }
    func cjk(_ character: Character) -> Bool {
      character.unicodeScalars.contains {
        (0x3000...0x9FFF).contains($0.value) || (0xAC00...0xD7AF).contains($0.value)
      }
    }
    func needsSpace(_ leftText: String, _ rightText: String) -> Bool {
      guard let left = leftText.last, let right = rightText.first, !left.isWhitespace,
        !right.isWhitespace, !cjk(left), !cjk(right)
      else { return false }
      if ",.;:!?)]}’”".contains(right) || "([{‘“".contains(left) { return false }
      if "\"'".contains(left),
        leftText.dropLast().last?.isLetter != true && leftText.dropLast().last?.isNumber != true
      {
        return false
      }
      if "\"'".contains(right), rightText.dropFirst().first?.isLetter != true { return false }
      if left == ".", leftText.dropLast().last?.isNumber == true, right.isNumber { return false }
      let token = leftText.split(whereSeparator: \.isWhitespace).last.map(String.init) ?? ""
      if token.contains("://") || token.contains("@") || token.hasPrefix("www."),
        "./@".contains(left)
      {
        return false
      }
      return (left.isLetter || left.isNumber || ".,;:!?)]}’”\"'".contains(left))
        && (right.isLetter || right.isNumber || "([{‘“\"'".contains(right))
    }
    if before.last == ".", before.dropLast().last?.isNumber == true,
      text.allSatisfy({ $0.isNumber || $0 == "." }), after.first?.isNumber == true
    {
      return text
    }
    return (needsSpace(before, text) ? " " : "") + text + (needsSpace(text, after) ? " " : "")
  }
}

@MainActor public enum PasteConfirmation {
  public static func wait(
    timeout: TimeInterval = 8,
    now: () -> TimeInterval = { ProcessInfo.processInfo.systemUptime },
    validate: () async throws -> Void, confirmed: () async throws -> Bool,
    sleep: () async throws -> Void = { try await Task.sleep(for: .milliseconds(50)) }
  ) async throws {
    let deadline = now() + timeout
    while now() < deadline {
      try Task.checkCancellation()
      try await validate()
      guard now() < deadline else { break }
      let matches = try await confirmed()
      try Task.checkCancellation()
      guard now() < deadline else { break }
      if matches { return }
      try await sleep()
    }
    throw DictationError.invalid(
      "Paste sent; Enter was not sent because insertion could not be confirmed.")
  }
}
