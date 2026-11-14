// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

final class FixtureProtocol: URLProtocol, @unchecked Sendable {
  nonisolated(unsafe) static var reply: (@Sendable (URLRequest) throws -> (Int, String, String))?
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    do {
      let (status, type, body) = try Self.reply!(request)
      let response = HTTPURLResponse(
        url: request.url!, statusCode: status, httpVersion: "HTTP/1.1",
        headerFields: ["Content-Type": type])!
      client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
      client?.urlProtocol(self, didLoad: Data(body.utf8))
      client?.urlProtocolDidFinishLoading(self)
    } catch { client?.urlProtocol(self, didFailWithError: error) }
  }
  override func stopLoading() {}
}

@Suite(.serialized) struct ClientTests {
  @Test func appleAssetCheckUsesGeneratedSilenceWithoutCleanup() async throws {
    FixtureProtocol.reply = { request in
      #expect(request.value(forHTTPHeaderField: "X-Antfly-Inference-Timeout-Ms") == "1800000")
      #expect(request.timeoutInterval >= 1800)
      let body = try fixtureBody(request)
      let length = body.subdata(in: 8..<16).withUnsafeBytes {
        $0.loadUnaligned(as: UInt64.self).littleEndian
      }
      let metadata =
        try JSONSerialization.jsonObject(with: body.subdata(in: 40..<(40 + Int(length))))
        as! [String: Any]
      #expect(metadata["model"] as? String == "transcribers/apple-speech")
      #expect(metadata["style"] as? String == "verbatim")
      #expect(metadata["language"] as? String == "en-US")
      #expect(metadata["cleanup_model"] == nil)
      #expect(body.suffix(32000).allSatisfy { $0 == 0 })
      return (
        200, "text/event-stream",
        "data: {\"type\":\"dictation.transcript\",\"transcript\":{\"text\":\"\"}}\n\ndata: {\"type\":\"dictation.completed\",\"text\":\"\"}\n\ndata: [DONE]\n\n"
      )
    }
    try await client().checkAppleSpeechAssets()
  }
  @Test func appleAssetCheckDoesNotHideInstallationFailure() async throws {
    FixtureProtocol.reply = { _ in
      (503, "application/json", "{\"error\":\"AppleSpeechAssetsMissing\"}")
    }
    await #expect(throws: (any Error).self) { try await client().checkAppleSpeechAssets() }
  }

  @Test func managedPasswordUsesBasicAuthWithoutForwardingExternalBearer() async throws {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [FixtureProtocol.self]
    FixtureProtocol.reply = { request in
      #expect(
        request.value(forHTTPHeaderField: "Authorization") == "Basic "
          + Data("admin:local-private-password".utf8).base64EncodedString())
      return (200, "application/json", "{\"data\":[]}")
    }
    let connection = try AntflyClient(
      server: "http://127.0.0.1:47831", token: "external-token",
      basicPassword: "local-private-password", session: URLSession(configuration: configuration))
    _ = try await connection.models()
  }
  func client() throws -> AntflyClient {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [FixtureProtocol.self]
    return try AntflyClient(
      server: "https://antfly.example", token: "test-token",
      session: URLSession(configuration: configuration))
  }
  @Test func jsonSettingsPersistenceUsesAuthenticatedExpectedEndpoint() async throws {
    FixtureProtocol.reply = { request in
      #expect(request.url?.path == "/db/v1/tables/dictation/batch")
      #expect(request.value(forHTTPHeaderField: "Authorization") == "Bearer test-token")
      return (200, "application/json", "{}")
    }
    try await client().saveSettings(Configuration())
  }
  @Test(arguments: [
    "data: {\"type\":\"dictation.delta\",\"delta\":\"late\"}\n\n",
    "data: {\"type\":\"dictation.transcript\",\"transcript\":{\"text\":\"replacement\"}}\n\n",
    "data: {\"type\":\"dictation.completed\",\"text\":\"duplicate\"}\n\n",
  ]) func completedTextCannotBeChangedByLaterEvents(late: String) async throws {
    FixtureProtocol.reply = { _ in
      (
        200, "text/event-stream",
        "data: {\"type\":\"dictation.transcript\",\"transcript\":{\"text\":\"raw\"}}\n\ndata: {\"type\":\"dictation.completed\",\"text\":\"Clean.\"}\n\n"
          + late + "data: [DONE]\n\n"
      )
    }
    var preset = Configuration().presets[0]
    preset.cleanupModel = "generators/cleanup"
    do {
      _ = try await client().cleanupText("raw", preset: preset, dictionary: [])
      Issue.record("Accepted a post-completion event")
    } catch let error as DictationError {
      #expect(error.rawTranscript == "raw")
    }
  }

  @Test func serverErrorsAreNotSilentlyAccepted() async throws {
    FixtureProtocol.reply = { _ in (403, "application/json", "{\"error\":\"denied\"}") }
    do {
      _ = try await client().models()
      Issue.record("Expected HTTP failure")
    } catch let error as ServerFailure {
      #expect(error.status == 403 && error.message == "denied")
    }
  }
  @Test func dictationConsumesTranscriptAndCompletedEvents() async throws {
    FixtureProtocol.reply = { request in
      #expect(request.url?.path == "/ai/v1/dictate")
      #expect(request.value(forHTTPHeaderField: "Content-Type") == AttachmentEnvelope.contentType)
      return (
        200, "text/event-stream",
        "data: {\"type\":\"dictation.transcript\",\"transcript\":{\"text\":\"um hello\"}}\n\ndata: {\"type\":\"dictation.delta\",\"delta\":\"Hello.\"}\n\ndata: {\"type\":\"dictation.completed\",\"text\":\"Hello.\"}\n\ndata: [DONE]\n\n"
      )
    }
    let result = try await client().dictate(
      audio: Data([1]), preset: Configuration().presets[0], dictionary: []
    ) { _ in }
    #expect(result.raw == "um hello")
    #expect(result.text == "Hello.")
  }
  @Test func truncatedStreamCannotProduceInsertableResult() async throws {
    FixtureProtocol.reply = { _ in
      (200, "text/event-stream", "data: {\"type\":\"dictation.delta\",\"delta\":\"partial\"}\n\n")
    }
    do {
      _ = try await client().dictate(
        audio: Data([1]), preset: Configuration().presets[0], dictionary: []
      ) { _ in }
      Issue.record("Expected incomplete stream failure")
    } catch is DictationError {}
  }
  @Test func sessionAppendAndDeleteUseTheSamePublicHandle() async throws {
    FixtureProtocol.reply = { request in
      switch request.url!.path {
      case "/ai/v1/transcription/sessions":
        return (200, "application/json", "{\"id\":\"session_123\"}")
      case "/ai/v1/transcription/sessions/session_123/audio":
        return (
          200, "application/json",
          "{\"data\":[{\"text\":\"hello\",\"type\":\"final\",\"sequence\":1,\"start_ms\":0,\"end_ms\":1000}]}"
        )
      default:
        #expect(request.httpMethod == "DELETE")
        #expect(request.url!.path == "/ai/v1/transcription/sessions/session_123")
        return (200, "application/json", "{}")
      }
    }
    let client = try client()
    let id = try await client.createSession(preset: Configuration().presets[0], dictionary: [])
    let text = try await client.appendSession(id, audio: Data(), sampleRate: 16000, commit: true)
    #expect(text == "hello")
    await client.closeSession(id)
  }
  @Test func presetModesControlCleanupAndInstructionsOnTheWire() async throws {
    FixtureProtocol.reply = { request in
      let bytes = try fixtureBody(request)
      let size = bytes.withUnsafeBytes {
        $0.loadUnaligned(fromByteOffset: 8, as: UInt64.self).littleEndian
      }
      let body =
        try JSONSerialization.jsonObject(with: bytes.subdata(in: 40..<(40 + Int(size))))
        as! [String: Any]
      #expect(body["style"] as? String == "verbatim")
      #expect(body["instructions"] as? String == "")
      #expect(body["cleanup_model"] == nil)
      return (
        200, "text/event-stream",
        "data: {\"type\":\"dictation.transcript\",\"transcript\":{\"text\":\"um hello\"}}\n\ndata: {\"type\":\"dictation.completed\",\"text\":\"um hello\"}\n\ndata: [DONE]\n\n"
      )
    }
    var preset = Preset(id: "v", name: "Verbatim", style: "formal")
    preset.outputMode = .verbatim
    preset.cleanupModel = "generators/cleanup"
    preset.instructions = "Rewrite everything"
    let result = try await client().dictate(
      audio: Data([1]), preset: preset, dictionary: [], progress: { _ in })
    #expect(result.text == "um hello")
  }
  @Test func textCleanupAndLiveAudioUseTheirDeclaredTransports() async throws {
    FixtureProtocol.reply = { request in
      let body = try JSONSerialization.jsonObject(with: fixtureBody(request)) as! [String: Any]
      #expect(body["audio"] as? String == "")
      #expect(body["transcript_text"] as? String == "um hello")
      #expect(body["style"] as? String == "clean")
      #expect(body["instructions"] as? String == "")
      return (
        200, "text/event-stream",
        "data: {\"type\":\"dictation.transcript\",\"transcript\":{\"text\":\"um hello\"}}\n\ndata: {\"type\":\"dictation.completed\",\"text\":\"Hello.\"}\n\ndata: [DONE]\n\n"
      )
    }
    var preset = Preset(id: "c", name: "Clean", style: "formal")
    preset.outputMode = .clean
    preset.instructions = "Custom rewrite"
    preset.cleanupModel = "generators/cleanup"
    let result = try await client().cleanupText("um hello", preset: preset, dictionary: [])
    #expect(result.text == "Hello.")
    FixtureProtocol.reply = { request in
      #expect(request.value(forHTTPHeaderField: "Content-Type") == AttachmentEnvelope.contentType)
      let bytes = try fixtureBody(request)
      #expect(String(decoding: bytes.prefix(8), as: UTF8.self) == "AFATT001")
      let size = bytes.withUnsafeBytes {
        $0.loadUnaligned(fromByteOffset: 8, as: UInt64.self).littleEndian
      }
      let body =
        try JSONSerialization.jsonObject(with: bytes.subdata(in: 40..<(40 + Int(size))))
        as! [String: Any]
      #expect(body["audio"] as? String == "attachment:0")
      #expect(body["format"] as? String == "pcm_f32")
      return (200, "application/json", "{\"data\":[]}")
    }
    _ = try await client().appendSessionEvents(
      "session", audio: Data([0, 0, 0, 0]), sampleRate: 16000)
  }
  @Test func dictationPreservesStructuredHTTPFailureAndRetryAdvice() async throws {
    FixtureProtocol.reply = { _ in
      (
        429, "application/json",
        "{\"error\":{\"code\":\"InferenceCapacityUnavailable\",\"message\":\"No free inference slot\"}}"
      )
    }
    do {
      _ = try await client().dictate(
        audio: Data([1]), preset: Configuration().presets[0], dictionary: []
      ) { _ in }
      Issue.record("Expected overload")
    } catch let error as ServerFailure {
      #expect(error.status == 429 && error.code == "InferenceCapacityUnavailable")
      #expect(error.message == "No free inference slot")
      #expect(error.localizedDescription.contains("retry"))
    }
  }
  @Test func dictationBoundsUnstructuredHTTPErrorPages() async throws {
    FixtureProtocol.reply = { _ in (400, "text/html", String(repeating: "x", count: 100_000)) }
    do {
      _ = try await client().dictate(
        audio: Data([1]), preset: Configuration().presets[0], dictionary: []
      ) { _ in }
      Issue.record("Expected bad request")
    } catch let error as ServerFailure {
      #expect(error.status == 400 && error.message.count == 2048)
    }
  }

}

@Test func binaryAudioEnvelopeHasExactLittleEndianLayout() throws {
  let metadata = Data("{\"audio\":\"attachment:0\"}".utf8)
  let audio = Data([0, 1, 2, 255])
  let frame = try AttachmentEnvelope.audio(metadata: metadata, recording: audio)
  #expect(String(decoding: frame.prefix(8), as: UTF8.self) == "AFATT001")
  #expect(
    frame.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: 8, as: UInt64.self).littleEndian }
      == metadata.count)
  #expect(
    frame.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: 16, as: UInt32.self).littleEndian }
      == 1)
  #expect(
    frame.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: 24, as: UInt32.self).littleEndian }
      == 9)
  #expect(
    frame.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: 32, as: UInt64.self).littleEndian }
      == 4)
  #expect(frame.subdata(in: 40..<(40 + metadata.count)) == metadata)
  #expect(frame.suffix(4) == audio)
}

@Test func tenMinuteClipRemainsBinaryWithSmallMetadata() throws {
  let audio = Data(repeating: 0, count: 16000 * 2 * 600 + 44)
  let metadata = Data("{\"model\":\"transcribers/whisper\",\"audio\":\"attachment:0\"}".utf8)
  let frame = try AttachmentEnvelope.audio(metadata: metadata, recording: audio)
  #expect(frame.count == audio.count + metadata.count + 49)
  #expect(frame.count - audio.count < 1024)
}

private func fixtureBody(_ request: URLRequest) throws -> Data {
  if let body = request.httpBody { return body }
  guard let stream = request.httpBodyStream else {
    throw DictationError.invalid("Missing request body")
  }
  stream.open()
  defer { stream.close() }
  var body = Data()
  var buffer = [UInt8](repeating: 0, count: 4096)
  while true {
    let count = stream.read(&buffer, maxLength: buffer.count)
    if count < 0 { throw DictationError.invalid("Failed reading fixture body") }
    if count == 0 { return body }
    body.append(contentsOf: buffer.prefix(count))
  }
}

private func formattingEvents(raw: String, text: String) throws -> String {
  let transcript = try JSONSerialization.data(withJSONObject: [
    "type": "dictation.transcript", "transcript": ["text": raw],
  ])
  let complete = try JSONSerialization.data(withJSONObject: [
    "type": "dictation.completed", "text": text,
  ])
  return "data: " + String(decoding: transcript, as: UTF8.self) + "\n\ndata: "
    + String(decoding: complete, as: UTF8.self) + "\n\ndata: [DONE]\n\n"
}

extension ClientTests {
  @Test func protectedCleanupRestoresInternalBreaksAndLiteralQuotes() async throws {
    let raw = "First new paragraph second ‘new line’ please"
    FixtureProtocol.reply = { request in
      let body = try JSONSerialization.jsonObject(with: fixtureBody(request)) as! [String: Any]
      let encoded = body["transcript_text"] as! String
      #expect(!encoded.contains("new paragraph") && !encoded.contains("new line"))
      #expect((body["instructions"] as? String)?.contains("Preserve every token") == true)
      return (
        200, "text/event-stream",
        try formattingEvents(
          raw: encoded, text: encoded.replacingOccurrences(of: "First", with: "First."))
      )
    }
    var preset = Configuration().presets[0]
    preset.cleanupModel = "generators/cleanup"
    let result = try await client().cleanupText(
      raw, preset: preset, dictionary: [], formatting: .init())
    #expect(result.raw == raw && result.text == "First.\n\nsecond ‘new line’ please")
  }
  @Test func malformedCleanupCannotEraseProtectedCommandsOrInventAnEnterPayload() async throws {
    FixtureProtocol.reply = { request in
      let body = try JSONSerialization.jsonObject(with: fixtureBody(request)) as! [String: Any]
      return (
        200, "text/event-stream",
        try formattingEvents(raw: body["transcript_text"] as! String, text: "First second.")
      )
    }
    var preset = Configuration().presets[0]
    preset.cleanupModel = "generators/cleanup"
    do {
      _ = try await client().cleanupText(
        "First new paragraph second", preset: preset, dictionary: [], formatting: .init())
      Issue.record("Accepted missing protected tokens")
    } catch let error as DictationError {
      #expect(error.rawTranscript == "First new paragraph second")
    }
    FixtureProtocol.reply = { _ in
      Issue.record("Command-only speech invoked cleanup")
      return (500, "application/json", "{}")
    }
    let result = try await client().cleanupText(
      "press enter", preset: preset, dictionary: [], formatting: .init(pressEnter: true))
    #expect(result.text.isEmpty && result.formatting?.pressEnter == false)
  }
  @Test func recordedAudioRecognizesBeforeProtectedCleanup() async throws {
    let raw = "First new paragraph second press enter"
    FixtureProtocol.reply = { request in
      let data = try fixtureBody(request)
      if data.prefix(8) == Data("AFATT001".utf8) {
        let size = data.withUnsafeBytes {
          $0.loadUnaligned(fromByteOffset: 8, as: UInt64.self).littleEndian
        }
        let metadata =
          try JSONSerialization.jsonObject(with: data.subdata(in: 40..<(40 + Int(size))))
          as! [String: Any]
        #expect(metadata["cleanup_model"] == nil)
        return (200, "text/event-stream", try formattingEvents(raw: raw, text: raw))
      }
      let body = try JSONSerialization.jsonObject(with: data) as! [String: Any]
      let encoded = body["transcript_text"] as! String
      #expect(!encoded.contains("press enter") && !encoded.contains("new paragraph"))
      return (
        200, "text/event-stream",
        try formattingEvents(
          raw: encoded, text: encoded.replacingOccurrences(of: "First", with: "First."))
      )
    }
    var preset = Configuration().presets[0]
    preset.cleanupModel = "generators/cleanup"
    let result = try await client().dictate(
      audio: Data([0, 1]), preset: preset, dictionary: [], formatting: .init(pressEnter: true),
      progress: { _ in })
    #expect(
      result.raw == raw && result.text == "First.\n\nsecond"
        && result.formatting?.pressEnter == true)
  }
}

private func completionToken(_ body: [String: Any]) throws -> String {
  let messages = body["messages"] as! [[String: String]]
  let rules = messages[0]["content"]!
  let regex = try NSRegularExpression(pattern: #"\[DC_[A-F0-9]{32}\]"#)
  let match = regex.firstMatch(in: rules, range: NSRange(rules.startIndex..., in: rules))!
  return (rules as NSString).substring(with: match.range)
}
private func portableReply(_ text: String, finish: String = "stop", body: [String: Any]? = nil)
  throws -> String
{
  let content = try body.map { text + "\n" + (try completionToken($0)) } ?? text
  return String(
    decoding: try JSONSerialization.data(withJSONObject: [
      "choices": [["finish_reason": finish, "message": ["role": "assistant", "content": content]]]
    ]), as: UTF8.self)
}
extension ClientTests {
  @Test func cleanOutputDoesNotReviveAStoredRewriteTone() async throws {
    FixtureProtocol.reply = { request in
      let body = try JSONSerialization.jsonObject(with: fixtureBody(request)) as! [String: Any]
      #expect(request.url?.path == "/ai/v1/dictate")
      #expect(body["style"] as? String == "clean")
      let text = body["transcript_text"] as! String
      return (200, "text/event-stream", try formattingEvents(raw: text, text: text))
    }
    var preset = Configuration().presets[0]
    preset.mode = .clean
    preset.style = "formal"
    preset.cleanupModel = "generators/cleanup"
    let result = try await client().cleanupText(
      "Hello new paragraph world", preset: preset, dictionary: [], formatting: .init())
    #expect(result.text == "Hello\n\nworld")
  }
  @Test func rawRuntimeModelsUsePortableCleanupAfterRecognition() async throws {
    let raw = "Hello new paragraph world press enter"
    FixtureProtocol.reply = { request in
      let data = try fixtureBody(request)
      if data.prefix(8) == Data("AFATT001".utf8) {
        return (200, "text/event-stream", try formattingEvents(raw: raw, text: raw))
      }
      #expect(request.url?.path == "/ai/v1/generate")
      let body = try JSONSerialization.jsonObject(with: data) as! [String: Any]
      #expect(body["model"] as? String == "google/gemma-3-1b-it")
      let messages = body["messages"] as! [[String: String]]
      #expect(messages[0]["content"]!.contains("Antfly"))
      let text = String(messages[1]["content"]!.dropFirst("Transcript:\n".count))
      #expect(!text.contains("new paragraph") && !text.contains("press enter"))
      return (
        200, "application/json",
        try portableReply(
          "```text\nCleaned text: \"" + text + "\"\n" + (try completionToken(body)) + "\n```")
      )
    }
    var preset = Configuration().presets[0]
    preset.model = "openai/whisper-tiny"
    preset.transcriberRoute = .runtime
    preset.cleanupModel = "google/gemma-3-1b-it"
    let phases = FormattingPhaseLog()
    let result = try await client().dictate(
      audio: Data([1]), preset: preset, dictionary: ["Antfly"], formatting: .init(pressEnter: true),
      events: { await phases.add($0.phase) }, progress: { _ in })
    #expect(await phases.values.contains(.cleaning))
    #expect(
      result.raw == raw && result.text == "Hello\n\nworld" && result.formatting?.pressEnter == true)
  }
  @Test func maximumRewriteInstructionsArePreservedOutsideDictateBudget() async throws {
    let instructions = String(repeating: "x", count: 4096)
    FixtureProtocol.reply = { request in
      #expect(request.url?.path == "/ai/v1/generate")
      let body = try JSONSerialization.jsonObject(with: fixtureBody(request)) as! [String: Any]
      let messages = body["messages"] as! [[String: String]]
      #expect(messages[0]["content"]!.contains(instructions))
      #expect(messages[0]["content"]!.contains("Preserve every token"))
      return (
        200, "application/json",
        try portableReply(
          String(messages[1]["content"]!.dropFirst("Transcript:\n".count)), body: body)
      )
    }
    var preset = Configuration().presets[0]
    preset.mode = .rewrite
    preset.instructions = instructions
    preset.cleanupModel = "generators/cleanup"
    let result = try await client().cleanupText(
      "Hello new paragraph world", preset: preset, dictionary: [], formatting: .init())
    #expect(result.text == "Hello\n\nworld")
  }
  @Test func expandedFormattingUsesBoundedRequestsWithoutSplittingMarkers() async throws {
    FixtureProtocol.reply = { request in
      #expect(request.url?.path == "/ai/v1/dictate")
      let body = try JSONSerialization.jsonObject(with: fixtureBody(request)) as! [String: Any]
      let text = body["transcript_text"] as! String
      #expect(text.utf8.count <= VoiceFormatting.Plan.maxCleanupBatchBytes)
      #expect(body["max_tokens"] as? Int == VoiceFormatting.Plan.cleanupTokenBudget(for: text))
      return (200, "text/event-stream", try formattingEvents(raw: text, text: text))
    }
    var preset = Configuration().presets[0]
    preset.cleanupModel = "generators/cleanup"
    let raw = String(repeating: "word new paragraph ", count: 1400)
    #expect(VoiceFormatting.Plan(raw).input.utf8.count > 65536)
    let result = try await client().cleanupText(
      raw, preset: preset, dictionary: [], formatting: .init())
    #expect(result.raw == raw && result.text == String(repeating: "word\n\n", count: 1400))
  }
  @Test func portableCleanupRejectsTruncatedAndOversizedResults() async throws {
    var preset = Configuration().presets[0]
    preset.model = "openai/whisper-tiny"
    preset.transcriberRoute = .runtime
    preset.cleanupModel = "google/gemma-3-1b-it"
    for finish in ["length", "tool_calls"] {
      FixtureProtocol.reply = { _ in
        (200, "application/json", try portableReply("partial", finish: finish))
      }
      do {
        _ = try await client().cleanupText(
          "hello", preset: preset, dictionary: [], formatting: .init())
        Issue.record("Accepted incomplete generation")
      } catch let failure as DictationError { #expect(failure.rawTranscript == "hello") }
    }
    FixtureProtocol.reply = { _ in
      (200, "application/json", String(repeating: "x", count: 1_048_577))
    }
    do {
      _ = try await client().cleanupText(
        "hello", preset: preset, dictionary: [], formatting: .init())
      Issue.record("Accepted oversized generation")
    } catch let failure as DictationError { #expect(failure.rawTranscript == "hello") }
  }
}

extension ClientTests {
  @Test(arguments: [DictationMode.rewrite, .clean])
  func oversizedWholeUtteranceFailsBeforeGenerationWithRawRecovery(mode: DictationMode) async throws
  {
    FixtureProtocol.reply = { _ in
      Issue.record("An oversized whole utterance reached generation")
      return (500, "application/json", "{}")
    }
    var preset = Configuration().presets[0]
    preset.mode = mode
    if mode == .clean { preset.processing = .wholeUtterance }
    preset.instructions = "Preserve the whole argument"
    preset.cleanupModel = "generators/cleanup"
    let raw = String(repeating: "word new paragraph ", count: 1400)
    do {
      _ = try await client().cleanupText(raw, preset: preset, dictionary: [], formatting: .init())
      Issue.record("Accepted an utterance larger than the output budget")
    } catch let error as DictationError {
      #expect(error.rawTranscript == raw)
      #expect(error.localizedDescription.contains("whole-utterance cleanup capacity"))
    }
  }

  @Test(arguments: [false, true])
  func markerHeavyCleanupFitsOutputBudgetOnBothBackends(runtime: Bool) async throws {
    FixtureProtocol.reply = { request in
      let body = try JSONSerialization.jsonObject(with: fixtureBody(request)) as! [String: Any]
      let text: String
      if runtime {
        #expect(request.url?.path == "/ai/v1/generate")
        let messages = body["messages"] as! [[String: String]]
        text = String(messages[1]["content"]!.dropFirst("Transcript:\n".count))
      } else {
        #expect(request.url?.path == "/ai/v1/dictate")
        text = body["transcript_text"] as! String
      }
      let outputBudget = body["max_tokens"] as! Int
      // Simulate a byte-fallback tokenizer, rather than letting the echo
      // fixture return unlimited marker output regardless of max_tokens.
      #expect(text.utf8.count + 1024 <= outputBudget)
      guard text.utf8.count <= outputBudget else {
        return (200, "application/json", try portableReply("partial", finish: "length"))
      }
      return runtime
        ? (200, "application/json", try portableReply(text, body: body))
        : (200, "text/event-stream", try formattingEvents(raw: text, text: text))
    }
    var preset = Configuration().presets[0]
    preset.cleanupModel = "generators/cleanup"
    if runtime {
      preset.model = "openai/whisper-tiny"
      preset.transcriberRoute = .runtime
      preset.cleanupModel = "google/gemma-3-1b-it"
    }
    let raw = String(repeating: "a new line ", count: 1000)
    let result = try await client().cleanupText(
      raw, preset: preset, dictionary: [], formatting: .init())
    #expect(result.text == String(repeating: "a\n", count: 1000))
    #expect(result.raw == raw)
  }

}

private actor FormattingPhaseLog {
  var values: [DictationPhase] = []
  func add(_ phase: DictationPhase) { values.append(phase) }
}

extension ClientTests {
  @Test @MainActor func rawRuntimeCleanupCanRetryWithoutRecordingAgain() async throws {
    FixtureProtocol.reply = { request in
      #expect(request.url?.path == "/ai/v1/generate")
      let body = try JSONSerialization.jsonObject(with: fixtureBody(request)) as! [String: Any]
      let messages = body["messages"] as! [[String: String]]
      return (
        200, "application/json",
        try portableReply(
          String(messages[1]["content"]!.dropFirst("Transcript:\n".count)), body: body)
      )
    }
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: directory) }
    let connection = try client()
    let store = Store(
      directory: directory, registerHotkey: false, clientFactory: { _ in connection })
    store.config.presets[0].model = "openai/whisper-tiny"
    store.config.presets[0].transcriberRoute = .runtime
    store.config.presets[0].cleanupModel = "google/gemma-3-1b-it"
    store.preview = "Hello new paragraph world"
    store.canRecoverRaw = true
    store.retryCleanup()
    await store.requestTask?.value
    #expect(store.result == "Hello\n\nworld" && store.phase == .ready && !store.canRecoverRaw)
  }
}

extension ClientTests {
  @Test(arguments: [false, true])
  func contextualBatchesPreserveCorrectionsAndUnicodeOnBothBackends(runtime: Bool) async throws {
    FixtureProtocol.reply = { request in
      let body = try JSONSerialization.jsonObject(with: fixtureBody(request)) as! [String: Any]
      let text: String
      if runtime {
        let messages = body["messages"] as! [[String: String]]
        text = String(messages[1]["content"]!.dropFirst("Transcript:\n".count))
      } else {
        text = body["transcript_text"] as! String
      }
      #expect(text.utf8.count <= VoiceFormatting.Plan.maxCleanupBatchBytes)
      let cleaned = text.replacingOccurrences(
        of: "staging no I mean production", with: "production")
      return runtime
        ? (200, "application/json", try portableReply(cleaned, body: body))
        : (200, "text/event-stream", try formattingEvents(raw: text, text: cleaned))
    }
    var preset = Configuration().presets[0]
    preset.cleanupModel = "generators/cleanup"
    if runtime {
      preset.model = "openai/whisper-tiny"
      preset.transcriberRoute = .runtime
      preset.cleanupModel = "google/gemma-3-1b-it"
    }
    let prefix = String(repeating: "x ", count: 1532)
    let raw = prefix + "staging no I mean production"
    let result = try await client().cleanupText(
      raw, preset: preset, dictionary: [], formatting: .init())
    #expect(result.text == prefix + "production")
    let chinese = String(repeating: "这是一个中文句子。", count: 200)
    let unicode = try await client().cleanupText(
      chinese, preset: preset, dictionary: [], formatting: .init())
    #expect(unicode.text == chinese && unicode.raw == chinese)
  }

  @Test func liveCumulativeCleanupRevisesPreviousSegmentWithoutDuplicatingOutput() async throws {
    FixtureProtocol.reply = { request in
      let body = try JSONSerialization.jsonObject(with: fixtureBody(request)) as! [String: Any]
      let raw = body["transcript_text"] as! String
      let cleaned = raw.replacingOccurrences(of: "staging no I mean production", with: "production")
      return (200, "text/event-stream", try formattingEvents(raw: raw, text: cleaned))
    }
    let connection = try client()
    let context = CleanupContext()
    let phases = FormattingPhaseLog()
    var config = Configuration()
    config.presets[0].cleanupModel = "generators/cleanup"
    let preset = config.presets[0]
    let pipeline = LiveDictation(
      incremental: true, needsCleanup: true, formatting: .init(), cumulativeCleanup: true,
      recognize: { data, commit in
        if commit { return [] }
        let first = data.first == 1
        return [
          RecognitionEvent(
            type: "final", sequence: first ? 1 : 2,
            text: first ? "We deploy to staging" : "no I mean production",
            start: first ? 0 : 100, end: first ? 100 : 200)
        ]
      },
      clean: { raw in
        let result = try await connection.cleanupText(
          raw, preset: preset, dictionary: [], formatting: .init(), context: context)
        await phases.add(.cleaning)
        return result
      })
    try await pipeline.append(Data([1]))
    while await phases.values.isEmpty { await Task.yield() }
    try await pipeline.append(Data([2]))
    let result = try await pipeline.finish(Data())
    #expect(result.raw == "We deploy to staging no I mean production")
    #expect(result.text == "We deploy to production")
  }
}

extension ClientTests {
  @Test(arguments: ["missing", "wrong", "duplicate", "trailing", "empty"])
  func portableCleanupRejectsUnverifiedStopsBeforeEnterCanBeProduced(kind: String) async throws {
    FixtureProtocol.reply = { request in
      #expect(request.url?.path == "/ai/v1/generate")
      let body = try JSONSerialization.jsonObject(with: fixtureBody(request)) as! [String: Any]
      let messages = body["messages"] as! [[String: String]]
      let input = String(messages[1]["content"]!.dropFirst("Transcript:\n".count))
      let token = try completionToken(body)
      let response: String
      switch kind {
      case "wrong": response = input + "\n[DC_00000000000000000000000000000000]"
      case "duplicate": response = input + "\n" + token + "\n" + token
      case "trailing": response = input + "\n" + token + "extra text"
      case "empty": response = token
      default: response = input  // Legacy Apple token cap with synthesized stop.
      }
      return (200, "application/json", try portableReply(response))
    }
    var preset = Configuration().presets[0]
    preset.mode = .rewrite
    preset.instructions = String(repeating: "x", count: 4096)
    preset.cleanupModel = "generators/apple"
    let raw = "Hello new line this is the rest of the sentence press enter"
    do {
      _ = try await client().cleanupText(
        raw, preset: preset, dictionary: [], formatting: .init(pressEnter: true))
      Issue.record("Unverified portable output became eligible for insertion")
    } catch let error as DictationError { #expect(error.rawTranscript == raw) }
  }
}

extension ClientTests {
  @Test func explicitCleanupContextWorksWithoutEnablingVoiceCommands() async throws {
    FixtureProtocol.reply = { request in
      let body = try JSONSerialization.jsonObject(with: fixtureBody(request)) as! [String: Any]
      let raw = body["transcript_text"] as! String
      let text = raw.replacingOccurrences(of: "staging no I mean production", with: "production")
      return (200, "text/event-stream", try formattingEvents(raw: raw, text: text))
    }
    let context = CleanupContext()
    let connection = try client()
    var preset = Configuration().presets[0]
    preset.cleanupModel = "generators/cleanup"
    _ = try await connection.cleanupText(
      "We deploy to staging", preset: preset, dictionary: [], context: context)
    let result = try await connection.cleanupText(
      "We deploy to staging no I mean production new line", preset: preset, dictionary: [],
      context: context)
    #expect(result.text == "We deploy to production new line")
    #expect(result.formatting?.pressEnter == false)
  }
}

extension ClientTests {
  @Test func processorValidationPrecedesNetworkOnEveryPublicDictationPath() async throws {
    FixtureProtocol.reply = { _ in
      Issue.record("Invalid output settings reached the server")
      return (500, "application/json", "{}")
    }
    var preset = Configuration().presets[0]
    preset.outputMode = .clean
    let connection = try client()
    await #expect(throws: DictationError.self) {
      try await connection.dictate(
        audio: Data([0]), preset: preset, dictionary: [], progress: { _ in })
    }
    do {
      _ = try await connection.cleanupText(
        "Keep my raw words", preset: preset, dictionary: [], formatting: .init())
      Issue.record("Missing processor was accepted")
    } catch let error as DictationError {
      #expect(error.rawTranscript == "Keep my raw words")
    }
  }
  @Test func ordinaryRequestsKeepTheirBudgetsAfterAssetSetup() async throws {
    FixtureProtocol.reply = { request in
      if request.url?.path.hasSuffix("dictate") == true {
        #expect(request.value(forHTTPHeaderField: "X-Antfly-Inference-Timeout-Ms") == "1800000")
        return (
          200, "text/event-stream",
          "data: {\"type\":\"dictation.transcript\",\"transcript\":{\"text\":\"\"}}\n\ndata: {\"type\":\"dictation.completed\",\"text\":\"\"}\n\ndata: [DONE]\n\n"
        )
      }
      #expect(request.value(forHTTPHeaderField: "X-Antfly-Inference-Timeout-Ms") == nil)
      #expect(request.timeoutInterval < AntflyClient.speechAssetTimeout)
      return (200, "application/json", "{\"data\":[]}")
    }
    let connection = try client()
    try await connection.checkAppleSpeechAssets()
    _ = try await connection.models()
  }
}

private actor AssetRequestSignal {
  var signaled = false
  var waiting: [CheckedContinuation<Void, Never>] = []
  func send() {
    signaled = true
    let pending = waiting
    waiting = []
    pending.forEach { $0.resume() }
  }
  func wait() async { if !signaled { await withCheckedContinuation { waiting.append($0) } } }
}
private final class PendingAssetProtocol: URLProtocol, @unchecked Sendable {
  static let started = AssetRequestSignal()
  static let stopped = AssetRequestSignal()
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() { Task { await Self.started.send() } }
  override func stopLoading() { Task { await Self.stopped.send() } }
}
extension ClientTests {
  @Test(.timeLimit(.minutes(1))) func assetInstallationCancellationCancelsTheUnderlyingRequest()
    async throws
  {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [PendingAssetProtocol.self]
    let connection = try AntflyClient(
      server: "http://localhost:8080", session: URLSession(configuration: configuration))
    let pending = Task { try await connection.checkAppleSpeechAssets() }
    await PendingAssetProtocol.started.wait()
    pending.cancel()
    await #expect(throws: CancellationError.self) { try await pending.value }
    await PendingAssetProtocol.stopped.wait()
  }
}

extension ClientTests {
  @Test func cleanupWarmupContainsNoAudioOrUserTranscriptAndUsesSmallBudget() async throws {
    FixtureProtocol.reply = { request in
      #expect(request.url?.path == "/ai/v1/generate")
      let body = try JSONSerialization.jsonObject(with: fixtureBody(request)) as! [String: Any]
      #expect(body["model"] as? String == "generators/cleanup")
      #expect(body["max_tokens"] as? Int == 8)
      #expect(body["audio"] == nil && body["transcript_text"] == nil && body["dictionary"] == nil)
      return (
        200, "application/json",
        #"{"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"Ready."}}]}"#
      )
    }
    try await client().prewarmCleanup(model: "generators/cleanup")
  }
  @Test func formattedCleanupKeepsFirstEventAndFirstDraftTiming() async throws {
    FixtureProtocol.reply = { request in
      let body = try JSONSerialization.jsonObject(with: fixtureBody(request)) as! [String: Any]
      #expect(
        body["dictionary"] as? [String] == (0..<64).map { "UnrelatedEntry" + String($0) } + [
          "Antfly"
        ])
      let raw = body["transcript_text"] as! String
      let event = try JSONSerialization.data(withJSONObject: [
        "type": "dictation.delta", "delta": raw,
      ])
      let base = try formattingEvents(raw: raw, text: raw)
      // SSE lifecycle must keep transcript, delta, and completion in order.
      let pieces = base.components(separatedBy: "\n\n")
      let response =
        pieces[0] + "\n\ndata: " + String(decoding: event, as: UTF8.self) + "\n\n"
        + pieces.dropFirst().joined(separator: "\n\n")
      return (200, "text/event-stream", response)
    }
    var preset = Configuration().presets[0]
    preset.outputMode = .clean
    preset.cleanupModel = "generators/cleanup"
    let result = try await client().cleanupText(
      "What about Antfly now?", preset: preset,
      dictionary: (0..<64).map { "UnrelatedEntry" + String($0) } + ["Antfly"],
      formatting: .init(commands: true, pressEnter: false))
    #expect(result.text == "What about Antfly now?")
    #expect(result.timing?.firstEvent != nil && result.timing?.firstDraft != nil)
  }
}
