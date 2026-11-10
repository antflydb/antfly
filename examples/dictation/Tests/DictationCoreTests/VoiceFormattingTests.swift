// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
import Foundation
import Testing

@testable import DictationCore

@Test func spokenBreaksAndQuotationsAreParsedBeforeCleanup() throws {
  for command in ["new line", "next line", "line break", "skip a line"] {
    #expect(VoiceFormatting.format("Hello. \(command)! Next.").text == "Hello.\nNext.")
    #expect(VoiceFormatting.format(command).text == "\n")
  }
  for quoted in [
    "\"new paragraph\"", "‘new paragraph’", "'new paragraph'", "“new paragraph”",
    "\"say \\\"new line\\\"\"", "‘say “new line”’", "\"don’t say new line\"",
    "'don't say new line'", "\"new paragraph",
  ] {
    let raw = "Say \(quoted) please"
    #expect(VoiceFormatting.format(raw).text == raw)
  }
  #expect(VoiceFormatting.format("Don't stop new line next").text == "Don't stop\nnext")
  #expect(VoiceFormatting.format("New paragraph?").text == "\n\n")
  let plan = VoiceFormatting.Plan("First new paragraph second ‘new line’ please")
  #expect(!plan.input.contains("new paragraph") && !plan.input.contains("new line"))
  let cleaned = plan.input.replacingOccurrences(of: "First", with: "First.")
  #expect(try plan.restore(cleaned).text == "First.\n\nsecond ‘new line’ please")
  #expect(throws: DictationError.self) { try plan.restore("First second please.") }
  #expect(throws: DictationError.self) { try plan.restore(plan.input + plan.input) }
  #expect(VoiceFormatting.format("A new line", commands: false).text == "A new line")
}
@Test func enterRequiresOriginalPayloadNotModelGeneratedText() throws {
  let options = VoiceFormatting.Options(pressEnter: true)
  let empty = VoiceFormatting.Plan("press enter", options: options)
  #expect(empty.render().text.isEmpty && !empty.render().pressEnter && !empty.hasCleanupText)
  #expect(throws: DictationError.self) { try empty.restore("Press Enter to continue.") }
  let payload = VoiceFormatting.Plan("Hello. Press enter!", options: options)
  #expect(try payload.restore("Hello!").pressEnter)
  let invented = VoiceFormatting.Plan("Hello", options: options)
  let generated = try invented.restore("Hello press enter")
  #expect(!generated.pressEnter && generated.text == "Hello press enter")
  #expect(!VoiceFormatting.format("Press enter to continue", pressEnter: true).pressEnter)
  #expect(!VoiceFormatting.format("Say ‘press enter’", pressEnter: true).pressEnter)
  #expect(!VoiceFormatting.format("Say \"press enter", pressEnter: true).pressEnter)
  #expect(!VoiceFormatting.format("Hello press enter").pressEnter)
}
@Test func spacingUsesPunctuationQuotesDecimalsAndCursorBoundaries() {
  for before in ["Hello.", "Hello,", "Note:", "First;", "He said “yes”", "He said \"yes\""] {
    #expect(VoiceFormatting.spaced("Next", before: before, after: "") == " Next")
  }
  #expect(VoiceFormatting.spaced("middle", before: "before", after: "after") == " middle ")
  #expect(VoiceFormatting.spaced("word", before: "(", after: ")") == "word")
  #expect(VoiceFormatting.spaced("word", before: "\"", after: "\"") == "word")
  #expect(VoiceFormatting.spaced("text", before: " ", after: " ") == "text")
  #expect(VoiceFormatting.spaced("\nNext", before: "Hello.", after: "") == "\nNext")
  #expect(VoiceFormatting.spaced("word", before: "", after: ",") == "word")
  #expect(VoiceFormatting.spaced("你好", before: "世界", after: "！") == "你好")
  #expect(VoiceFormatting.spaced("5", before: "3.", after: "") == "5")
  #expect(VoiceFormatting.spaced("5", before: "3.", after: "0") == "5")
  #expect(VoiceFormatting.spaced("com", before: "https://example.", after: "") == "com")
}
@Test @MainActor func confirmationHasAWallClockDeadlineIncludingSlowReads() async throws {
  var time: TimeInterval = 0
  var sleeps = 0
  try await PasteConfirmation.wait(
    timeout: 8, now: { time }, validate: {}, confirmed: { time >= 1 },
    sleep: {
      time += 0.1
      sleeps += 1
    })
  #expect(sleeps >= 10)
  do {
    try await PasteConfirmation.wait(
      timeout: 1, now: { time }, validate: { time += 2 },
      confirmed: {
        Issue.record("Read after deadline")
        return true
      })
    Issue.record("Late validation accepted")
  } catch {}
  do {
    try await PasteConfirmation.wait(
      timeout: 1, now: { time }, validate: {},
      confirmed: {
        time += 2
        return true
      })
    Issue.record("Late confirmation accepted")
  } catch {}
}
@Test @MainActor func confirmationRejectsFocusChangesTimeoutAndCancellation() async throws {
  var time: TimeInterval = 0
  do {
    try await PasteConfirmation.wait(
      now: { time }, validate: { if time > 0 { throw DictationError.invalid("Focus changed") } },
      confirmed: { false }, sleep: { time += 1 })
    Issue.record("Focus change accepted")
  } catch {}
  #expect(time == 1)
  time = 0
  do {
    try await PasteConfirmation.wait(
      timeout: 2, now: { time }, validate: {}, confirmed: { false }, sleep: { time += 1 })
    Issue.record("Timeout accepted")
  } catch {}
  #expect(time == 2)
  let task = Task { @MainActor in
    withUnsafeCurrentTask { $0?.cancel() }
    try await PasteConfirmation.wait(
      validate: { Issue.record("Canceled insertion checked") }, confirmed: { true })
  }
  do {
    try await task.value
    Issue.record("Cancellation ignored")
  } catch is CancellationError {}
}
@Test func oldSettingsDefaultToFormattingWithoutSendingEnter() throws {
  let config = try Configuration.decode(JSONEncoder().encode(Configuration()))
  #expect(config.usesVoiceFormatting && config.usesSmartSpacing && !config.usesPressEnterCommand)
}
@Test func manyCommandsKeepUnicodeAndLiteralsAndDetectSplitControls() throws {
  let phrase = "Résumé new line ‘new paragraph’ new paragraph "
  #expect(
    VoiceFormatting.format(String(repeating: phrase, count: 1000)).text
      == String(repeating: "Résumé\n‘new paragraph’\n\n", count: 1000))
  let plan = VoiceFormatting.Plan("First new paragraph second")
  #expect(plan.crosses([9]))
  #expect(!plan.crosses([5]))
}

@Test func structuralMarkersOwnTheirWhitespaceWithoutChangingLiterals() throws {
  let plan = VoiceFormatting.Plan("Hello new paragraph ‘ new line ’ please")
  let paragraph = try NSRegularExpression(pattern: #"\[VF_[A-F0-9]+_P_[0-9]+\]"#)
  let padded = paragraph.stringByReplacingMatches(
    in: plan.input, range: NSRange(plan.input.startIndex..., in: plan.input), withTemplate: "  $0  "
  )
  #expect(try plan.restore(padded).text == "Hello\n\n‘ new line ’ please")
  #expect(VoiceFormatting.format("Hello new line next line world").text == "Hello\n\nworld")
}
@Test func encodedBatchesPreserveMarkersWordsAndUTF8() throws {
  let plan = VoiceFormatting.Plan(
    String(repeating: "Résumé new paragraph ‘new line’ ", count: 2000))
  let batches = try plan.batches()
  #expect(
    batches.count > 1
      && batches.allSatisfy { $0.text.utf8.count <= VoiceFormatting.Plan.maxCleanupBatchBytes })
  #expect(batches.map(\.text).joined() == plan.input)
  #expect(try plan.restore(batches.map(\.text).joined()).text == plan.render().text)
}

@Test func possessiveApostrophesDoNotSwallowFormattingOrEnterCommands() {
  for possessive in ["users'", "users’", "James'", "James’"] {
    let raw = "The \(possessive) accounts new line Next item press enter"
    let result = VoiceFormatting.format(raw, pressEnter: true)
    #expect(result.text == "The \(possessive) accounts\nNext item")
    #expect(result.pressEnter)
  }
  for quotation in ["'new line'", "'press enter'", "'users'", "'yes'"] {
    let raw = "Say \(quotation) new line Next item press enter"
    let result = VoiceFormatting.format(raw, pressEnter: true)
    #expect(result.text == "Say \(quotation)\nNext item")
    #expect(result.pressEnter)
  }
}

@Test func batchCapacityIncludesExactBoundaryAndMarkerExpansion() throws {
  let limit = VoiceFormatting.Plan.maxCleanupBatchBytes
  let exact = VoiceFormatting.Plan(String(repeating: "x", count: limit))
  #expect(try exact.batches().count == 1)
  let overflow = VoiceFormatting.Plan(String(repeating: "x", count: limit) + " y")
  #expect(try overflow.batches().count == 2)
  let markers = VoiceFormatting.Plan(String(repeating: "a new line ", count: 1000))
  let batches = try markers.batches()
  #expect(batches.count > 1)
  #expect(
    batches.allSatisfy { $0.text.utf8.count + 1024 <= VoiceFormatting.Plan.cleanupOutputTokens })
  #expect(batches.map(\.text).joined() == markers.input)
  #expect(try markers.restore(batches.map(\.text).joined()) == markers.render())
}
