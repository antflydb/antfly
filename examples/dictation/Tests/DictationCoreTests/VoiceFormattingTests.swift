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
