// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
import Foundation
import Testing

@testable import DictationCore

private actor WindowLog {
  var inputs: [String] = []
  func clean(_ input: String) -> String {
    inputs.append(input)
    return input.replacingOccurrences(of: "staging no I mean production", with: "production")
  }
}

@Test func cleanupWindowCanRetractThePreviousBatchsCorrectionTarget() async throws {
  let context = CleanupContext()
  let prefix = String(repeating: "x ", count: 1020)
  let suffix = String(repeating: " z", count: 800)
  let raw = prefix + "staging no I mean production" + suffix
  let plan = VoiceFormatting.Plan(raw, options: .init(), namespace: context.namespace)
  let log = WindowLog()
  let output = try await context.process(plan, clean: { await log.clean($0) })
  #expect(output == prefix + "production" + suffix)
  #expect(await log.inputs.count > 1)
  #expect(
    await log.inputs.allSatisfy { $0.utf8.count <= VoiceFormatting.Plan.maxCleanupBatchBytes })
}

@Test func growingLivePrefixesReuseCommittedOutputAndKeepOnlyABoundedEditableTail() async throws {
  let context = CleanupContext()
  let log = WindowLog()
  let prefix = String(repeating: "one ", count: 2000)
  let first = prefix + "staging"
  func plan(_ raw: String) -> VoiceFormatting.Plan {
    VoiceFormatting.Plan(raw, options: .init(), namespace: context.namespace)
  }
  #expect(try await context.process(plan(first), clean: { await log.clean($0) }) == first)
  let initial = await log.inputs.count
  let revised = try await context.process(
    plan(first + " no I mean production"), clean: { await log.clean($0) })
  #expect(revised == prefix + "production")
  #expect(await log.inputs.count == initial + 1)
  #expect(await log.inputs.last!.utf8.count <= VoiceFormatting.Plan.maxCleanupBatchBytes)
  // A final flush of an unchanged prefix does not invoke the model again.
  #expect(
    try await context.process(plan(first + " no I mean production"), clean: { await log.clean($0) })
      == revised)
  #expect(await log.inputs.count == initial + 1)
}

@Test func contextReplansSplitCommandsAndExtendingLiteralQuotesWithoutReexecutingGeneratedWords()
  async throws
{
  let context = CleanupContext()
  let log = WindowLog()
  func plan(_ raw: String) -> VoiceFormatting.Plan {
    VoiceFormatting.Plan(raw, options: .init(), namespace: context.namespace)
  }
  let first = plan("First new")
  _ = try await context.process(first, clean: { await log.clean($0) })
  let second = plan("First new paragraph say ‘new line")
  let encoded = try await context.process(second, clean: { await log.clean($0) })
  #expect(try second.restore(encoded).text == "First\n\nsay ‘new line")
  let count = await log.inputs.count
  let third = plan("First new paragraph say ‘new line please’")
  let completed = try await context.process(third, clean: { await log.clean($0) })
  #expect(try third.restore(completed).text == "First\n\nsay ‘new line please’")
  #expect(await log.inputs.count == count)  // Only the opaque literal value changed.
}

@Test func failedOrCanceledContextDoesNotPoisonARetry() async throws {
  let context = CleanupContext()
  let raw = "Hello new paragraph world"
  let plan = VoiceFormatting.Plan(raw, options: .init(), namespace: context.namespace)
  do {
    _ = try await context.process(plan, clean: { _ in "Missing protected tokens" })
    Issue.record("Accepted lost markers")
  } catch is DictationError {}
  do {
    _ = try await context.process(plan, clean: { _ in throw CancellationError() })
    Issue.record("Accepted canceled cleanup")
  } catch is CancellationError {}
  let retried = try await context.process(plan, clean: { $0 })
  #expect(try plan.restore(retried).text == "Hello\n\nworld")
}

@Test func unicodePartitionsAreLosslessAndNeverSplitMarkersOrGraphemes() throws {
  for raw in [
    String(repeating: "这是一个中文句子。", count: 200),
    String(repeating: "これは日本語の文章です。", count: 200),
    String(repeating: "ภาษาไทย", count: 1000),
    String(repeating: "longword", count: 1000),
    String(repeating: "👩🏽‍💻e\u{301}", count: 1000) + " new paragraph next",
  ] {
    let plan = VoiceFormatting.Plan(raw)
    let batches = try plan.batches()
    #expect(batches.count > 1)
    #expect(batches.map(\.text).joined() == plan.input)
    #expect(batches.allSatisfy { $0.text.utf8.count <= VoiceFormatting.Plan.maxCleanupBatchBytes })
    #expect(try plan.restore(batches.map(\.text).joined()).text == plan.render().text)
    #expect(batches.flatMap { Array($0.text) } == Array(plan.input))
  }
}

@Test func editableTailIsSizedFromTheEndInsteadOfBeingAOneByteRemainder() async throws {
  let context = CleanupContext()
  let prefix = String(repeating: "x ", count: 1020)
  let suffix = String(repeating: " z", count: 800)
  let plan = VoiceFormatting.Plan(
    prefix + "staging no I mean production" + suffix, options: .init(), namespace: context.namespace
  )
  let output = try await context.process(plan) { input in
    if input.hasSuffix("staging ") { return String(input.dropLast()) + ". " }
    return input.replacingOccurrences(of: "staging. no I mean production", with: "production")
  }
  #expect(output == prefix + "production" + suffix)
  let text = String(repeating: "x ", count: 1024) + "z"
  let partitions = try plan.partition(text, maxBytes: 1024, fromEnd: true)
  #expect(partitions.map(\.text).joined() == text)
  #expect(partitions.last!.text.utf8.count == 1024)
}
