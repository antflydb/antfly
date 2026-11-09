// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
import Foundation
import Testing

@testable import DictationCore

@Test func spokenBreaksWorkWithoutGenerativeCleanup() {
  for command in ["new line", "next line", "line break", "skip a line"] {
    #expect(
      VoiceFormatting.format("Hello. \(command). Next sentence.").text == "Hello.\nNext sentence.")
    #expect(VoiceFormatting.format(command).text == "\n")
  }
  for command in ["new paragraph", "start a new paragraph"] {
    #expect(VoiceFormatting.format("Hello \(command) world").text == "Hello\n\nworld")
  }
  #expect(VoiceFormatting.format("Hello new line next line world").text == "Hello\n\nworld")
  #expect(
    VoiceFormatting.finalize(raw: "new paragraph", text: "", commands: true, pressEnter: false).text
      == "\n\n")
  #expect(
    VoiceFormatting.finalize(
      raw: "Hello new line", text: "Hello.", commands: true, pressEnter: false
    ).text == "Hello.\n")
  #expect(VoiceFormatting.format("A newline character").text == "A newline character")
  #expect(VoiceFormatting.format("A new line", commands: false).text == "A new line")
  #expect(VoiceFormatting.format("Say \"new line\" please").text == "Say \"new line\" please")
  #expect(VoiceFormatting.format("Say “new paragraph” please").text == "Say “new paragraph” please")
}
@Test func enterRequiresOptInAndATerminalUnquotedRecognitionCommand() {
  #expect(!VoiceFormatting.format("Hello press enter").pressEnter)
  let output = VoiceFormatting.format("Hello. Press enter!", pressEnter: true)
  #expect(output.text == "Hello." && output.pressEnter)
  #expect(!VoiceFormatting.format("Press enter to continue", pressEnter: true).pressEnter)
  #expect(!VoiceFormatting.format("Say \"press enter\"", pressEnter: true).pressEnter)
  #expect(
    !VoiceFormatting.finalize(
      raw: "Hello", text: "Hello press enter", commands: true, pressEnter: true
    ).pressEnter)
  #expect(
    VoiceFormatting.finalize(
      raw: "Hello press enter", text: "Hello.", commands: true, pressEnter: true
    ).pressEnter)
  #expect(
    !VoiceFormatting.finalize(
      raw: "press enter", text: "press enter", commands: true, pressEnter: true
    ).pressEnter)
}
@Test func spacingUsesBothCursorBoundariesWithoutChangingExistingWhitespace() {
  #expect(
    VoiceFormatting.spaced("Next sentence.", before: "Hello.", after: "") == " Next sentence.")
  #expect(VoiceFormatting.spaced("middle", before: "before", after: "after") == " middle ")
  #expect(VoiceFormatting.spaced("word", before: "(", after: ")") == "word")
  #expect(VoiceFormatting.spaced("text", before: " ", after: " ") == "text")
  #expect(VoiceFormatting.spaced("\nNext", before: "Hello.", after: "") == "\nNext")
  #expect(VoiceFormatting.spaced("word", before: "", after: ",") == "word")
  #expect(VoiceFormatting.spaced("你好", before: "世界", after: "！") == "你好")
  #expect(VoiceFormatting.spaced("", before: "word", after: "word") == "")
}
@Test @MainActor func enterConfirmationWaitsForObservedInsertion() async throws {
  var checks = 0
  var sleeps = 0
  try await PasteConfirmation.wait(
    validate: { checks += 1 }, confirmed: { sleeps == 2 }, sleep: { sleeps += 1 })
  #expect(checks == 3 && sleeps == 2)
}
@Test @MainActor func enterConfirmationRejectsFocusChangesTimeoutAndCancellation() async throws {
  var sleeps = 0
  do {
    try await PasteConfirmation.wait(
      validate: {
        if sleeps > 0 { throw DictationError.invalid("Focus changed") }
      }, confirmed: { false }, sleep: { sleeps += 1 })
    Issue.record("Focus change was accepted")
  } catch {}
  #expect(sleeps == 1)
  sleeps = 0
  do {
    try await PasteConfirmation.wait(validate: {}, confirmed: { false }, sleep: { sleeps += 1 })
    Issue.record("Unconfirmed insertion was accepted")
  } catch {}
  #expect(sleeps == 20)
  let task = Task { @MainActor in
    withUnsafeCurrentTask { $0?.cancel() }
    try await PasteConfirmation.wait(
      validate: { Issue.record("Canceled insertion checked") }, confirmed: { true })
  }
  do {
    try await task.value
    Issue.record("Canceled insertion accepted")
  } catch is CancellationError {}
}
@Test func oldSettingsDefaultToFormattingWithoutSendingEnter() throws {
  var object =
    try JSONSerialization.jsonObject(with: JSONEncoder().encode(Configuration())) as! [String: Any]
  object.removeValue(forKey: "voiceFormatting")
  object.removeValue(forKey: "smartSpacing")
  object.removeValue(forKey: "pressEnterCommand")
  let config = try Configuration.decode(JSONSerialization.data(withJSONObject: object))
  #expect(config.usesVoiceFormatting && config.usesSmartSpacing && !config.usesPressEnterCommand)
}

@Test func manyCommandsKeepUnicodeAndQuotedLiteralsIntact() {
  let phrase = "Résumé new line “new paragraph” new paragraph "
  let text = String(repeating: phrase, count: 1000)
  let formatted = VoiceFormatting.format(text).text
  #expect(formatted == String(repeating: "Résumé\n“new paragraph”\n\n", count: 1000))
}
