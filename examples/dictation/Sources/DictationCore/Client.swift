// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

private final class CleanupMeasurements: @unchecked Sendable {
  private let lock = NSLock()
  private let began: TimeInterval
  private var event: TimeInterval?
  private var draft: TimeInterval?
  init(began: TimeInterval) { self.began = began }
  func record(_ timing: DictationTiming?) {
    guard let timing else { return }
    let offset = max(0, ProcessInfo.processInfo.systemUptime - began - timing.total)
    lock.withLock {
      if let first = timing.firstEvent { event = min(event ?? .infinity, offset + first) }
      if let first = timing.firstDraft { draft = min(draft ?? .infinity, offset + first) }
    }
  }
  var firsts: (TimeInterval?, TimeInterval?) { lock.withLock { (event, draft) } }
}

public actor AntflyClient {
  let origin: URL
  let token: String
  private let basicPassword: String?
  let inferenceGate: InferenceGate
  let session: URLSession
  public init(
    server: String, token: String = "", basicPassword: String? = nil, session: URLSession? = nil,
    maxConcurrentRequests: Int = 1
  ) throws {
    var config = Configuration()
    config.connectionMode = .external
    config.server = server
    config.concurrentRequests = maxConcurrentRequests
    try self.init(
      configuration: config, token: token, basicPassword: basicPassword, session: session)
  }
  public init(
    configuration: Configuration, token: String = "", basicPassword: String? = nil,
    session: URLSession? = nil
  ) throws {
    let config = try configuration.validated()
    inferenceGate = InferenceGate(capacity: config.concurrentRequests ?? 1)
    origin = URL(string: config.effectiveServer)!
    self.token = token
    self.basicPassword = basicPassword
    let settings = URLSessionConfiguration.ephemeral
    settings.timeoutIntervalForRequest = 120
    settings.timeoutIntervalForResource = 300
    self.session = session ?? URLSession(configuration: settings)
  }
  func request(_ path: String, method: String = "POST", body: Data? = nil) -> URLRequest {
    var request = URLRequest(url: origin.appendingPathComponent(path))
    request.httpMethod = method
    request.httpBody = body
    request.setValue("application/json", forHTTPHeaderField: "Content-Type")
    if let basicPassword {
      let encoded = Data(("admin:" + basicPassword).utf8).base64EncodedString()
      request.setValue("Basic " + encoded, forHTTPHeaderField: "Authorization")
    } else if !token.isEmpty {
      request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
    }
    return request
  }
  private func json(
    _ path: String, method: String = "POST", body: Data? = nil, allowed: [Int] = [200, 201, 204]
  ) async throws -> Data {
    let (data, response) = try await session.data(for: request(path, method: method, body: body))
    try Task.checkCancellation()
    guard let http = response as? HTTPURLResponse, allowed.contains(http.statusCode) else {
      let code = (response as? HTTPURLResponse)?.statusCode ?? 0
      throw ServerFailure(response: response, status: code, body: data)
    }
    return data
  }
  private func encode(_ value: [String: Any]) throws -> Data {
    try JSONSerialization.data(withJSONObject: value)
  }
  public func models() async throws -> String {
    String(decoding: try await json("ai/v1/models", method: "GET"), as: UTF8.self)
  }
  public static let speechAssetTimeout: TimeInterval = 30 * 60
  /// Exercise the released engine's asset installation path without microphone audio.
  /// Call only after the user opts into downloads in the managed engine configuration.
  public func checkAppleSpeechAssets(language: String = "en-US") async throws {
    try Task.checkCancellation()
    var probe = Preset(id: "asset-check", name: "Language setup", style: "verbatim")
    probe.model = "transcribers/apple-speech"
    probe.language = language
    let silence = try PCMRecording(samples: Data(count: 16000 * 4), sampleRate: 16000).wav()
    let setupConfiguration = session.configuration
    setupConfiguration.timeoutIntervalForRequest = Self.speechAssetTimeout + 30
    setupConfiguration.timeoutIntervalForResource = Self.speechAssetTimeout + 30
    let setupSession = URLSession(configuration: setupConfiguration)
    defer { setupSession.invalidateAndCancel() }
    var body = dictationBody(preset: probe, dictionary: [])
    body["audio"] = "attachment:0"
    var upload = request(
      "ai/v1/dictate",
      body: try AttachmentEnvelope.audio(metadata: encode(body), recording: silence))
    upload.setValue(AttachmentEnvelope.contentType, forHTTPHeaderField: "Content-Type")
    upload.setValue(
      String(Int(Self.speechAssetTimeout * 1000)),
      forHTTPHeaderField: "X-Antfly-Inference-Timeout-Ms")
    upload.timeoutInterval = Self.speechAssetTimeout + 30
    _ = try await consumeDictation(
      upload, cleanup: false, transport: setupSession, events: { _ in }, progress: { _ in })
    try Task.checkCancellation()
  }
  public func prewarmCleanup(model: String) async throws {
    let body: [String: Any] = [
      "model": model, "stream": false, "max_tokens": 8, "temperature": 0,
      "messages": [
        ["role": "system", "content": "Reply with exactly Ready."],
        ["role": "user", "content": "Warm up."],
      ],
    ]
    var upload = request("ai/v1/generate", body: try encode(body))
    upload.timeoutInterval = 5
    upload.setValue("3000", forHTTPHeaderField: "X-Antfly-Inference-Timeout-Ms")
    let data = try await session.data(for: upload)
    try Task.checkCancellation()
    guard (data.1 as? HTTPURLResponse)?.statusCode == 200 else {
      throw ServerFailure(response: data.1, body: Data(data.0.prefix(ServerFailure.maxBodyBytes)))
    }
    guard data.0.count <= 65536,
      let value = try JSONSerialization.jsonObject(with: data.0) as? [String: Any],
      let choices = value["choices"] as? [[String: Any]], choices.count == 1,
      choices[0]["finish_reason"] as? String == "stop"
    else {
      throw DictationError.invalid("Text processor warmup did not complete.")
    }
  }
  private func dictationBody(preset: Preset, dictionary: [String]) -> [String: Any] {
    var body: [String: Any] = [
      "model": preset.model, "style": preset.requestStyle,
      "dictionary": dictionary + preset.dictionary, "context": preset.context,
      "instructions": preset.outputMode == .rewrite ? preset.instructions : "", "stream": true,
    ]
    if !preset.language.isEmpty { body["language"] = preset.language }
    if preset.runsCleanup { body["cleanup_model"] = preset.cleanupModel }
    return body
  }
  public func dictate(
    audio: Data, preset: Preset, dictionary: [String],
    formatting: VoiceFormatting.Options? = nil,
    events: @Sendable @escaping (DictationProgress) async -> Void = { _ in },
    progress: @Sendable @escaping (String) async -> Void
  ) async throws -> DictationResult {
    try preset.outputMode.validateProcessor(preset.cleanupModel)
    if let formatting, formatting.commands || formatting.pressEnter {
      let began = ProcessInfo.processInfo.systemUptime
      var recognition = preset
      recognition.cleanupModel = ""
      recognition.outputMode = .verbatim
      var raw = try await dictate(
        audio: audio, preset: recognition, dictionary: dictionary,
        events: events, progress: progress)
      if !preset.runsCleanup {
        let plan = VoiceFormatting.Plan(raw.raw, options: formatting)
        let restored =
          raw.text == raw.raw || plan.hasProtectedSpans || plan.pressEnter
          ? plan.render() : try plan.restore(raw.text)
        raw.text = restored.text
        raw.formatting = restored
        return raw
      }
      let cleanupBegan = ProcessInfo.processInfo.systemUptime
      var result = try await cleanupText(
        raw.raw, preset: preset, dictionary: dictionary,
        formatting: formatting, events: events)
      let firstDraft = result.timing?.firstDraft.map { cleanupBegan - began + $0 }
      result.timing = DictationTiming(
        recognition: raw.timing?.recognition ?? cleanupBegan - began,
        cleanup: preset.runsCleanup ? ProcessInfo.processInfo.systemUptime - cleanupBegan : 0,
        total: ProcessInfo.processInfo.systemUptime - began,
        firstEvent: raw.timing?.firstEvent, firstDraft: firstDraft)
      return result
    }
    var body = dictationBody(preset: preset, dictionary: dictionary)
    body["audio"] = "attachment:0"
    var upload = request(
      "ai/v1/dictate", body: try AttachmentEnvelope.audio(metadata: encode(body), recording: audio))
    upload.setValue(AttachmentEnvelope.contentType, forHTTPHeaderField: "Content-Type")
    return try await consumeDictation(
      upload, cleanup: preset.runsCleanup, events: events, progress: progress)
  }
  public func cleanupText(
    _ text: String, preset: Preset, dictionary: [String],
    formatting: VoiceFormatting.Options? = nil, context: CleanupContext? = nil,
    events: @Sendable @escaping (DictationProgress) async -> Void = { _ in }
  ) async throws -> DictationResult {
    do { try preset.outputMode.validateProcessor(preset.cleanupModel) } catch {
      throw DictationError.cleanupFailed(raw: text, message: error.localizedDescription)
    }
    guard text.utf8.count <= 65536 else {
      throw DictationError.invalid("Transcript exceeds the cleanup limit.")
    }
    if let formatting = formatting
      ?? context.map({ _ in VoiceFormatting.Options(commands: false, pressEnter: false) })
    {
      let context = context ?? CleanupContext()
      let plan = VoiceFormatting.Plan(text, options: formatting, namespace: context.namespace)
      var result: DictationResult
      if !preset.runsCleanup || !plan.hasCleanupText {
        result = DictationResult(raw: text, text: plan.render().text)
        result.formatting = plan.render()
        return result
      }
      do {
        let cleanupStarted = ProcessInfo.processInfo.systemUptime
        let measurements = CleanupMeasurements(began: cleanupStarted)
        await events(DictationProgress(phase: .cleaning, raw: text))
        var protectedPreset = preset
        var combinedInstructions = preset.outputMode == .rewrite ? preset.instructions : ""
        if !plan.instructions.isEmpty {
          if !combinedInstructions.isEmpty { combinedInstructions += "\n" }
          combinedInstructions += plan.instructions
        }
        let portable = !preset.usesNamedTranscriber || combinedInstructions.utf8.count > 4096
        if !plan.instructions.isEmpty && !portable {
          protectedPreset.style = preset.requestStyle
          protectedPreset.instructions = combinedInstructions
          protectedPreset.mode = .rewrite
        }
        let requiresWhole =
          preset.outputMode == .rewrite || preset.processingStrategy == .wholeUtterance
        guard !requiresWhole || plan.input.utf8.count <= VoiceFormatting.Plan.maxCleanupBatchBytes
        else {
          throw DictationError.invalid(
            "This transcript exceeds the whole-utterance cleanup capacity. Use Clean with Automatic processing, shorten the recording, or insert the original transcript."
          )
        }
        let effectivePreset = protectedPreset
        let cleaned = try await context.process(plan) { window in
          let leading = String(window.prefix(while: { $0.isWhitespace }))
          let trailing = String(window.reversed().prefix(while: { $0.isWhitespace }).reversed())
          let content = window.trimmingCharacters(in: .whitespacesAndNewlines)
          let chunk: String
          if portable {
            chunk = try await self.portableCleanup(
              content, preset: preset, dictionary: dictionary,
              protectedInstructions: plan.instructions)
          } else {
            let response = try await self.cleanupText(
              content, preset: effectivePreset, dictionary: dictionary,
              events: { event in
                await events(
                  DictationProgress(
                    phase: event.phase, raw: text,
                    draft: plan.hasProtectedSpans ? "" : event.draft))
              })
            measurements.record(response.timing)
            chunk = response.text
          }
          return leading + chunk + trailing
        }
        result = DictationResult(raw: text, text: cleaned)
        let restored = try plan.restore(result.text)
        result.raw = text
        result.text = restored.text
        result.formatting = restored
        let elapsed = ProcessInfo.processInfo.systemUptime - cleanupStarted
        let firsts = measurements.firsts
        result.timing = DictationTiming(
          cleanup: elapsed, total: elapsed,
          firstEvent: firsts.0, firstDraft: firsts.1)
        return result
      } catch is CancellationError { throw CancellationError() } catch {
        throw DictationError.cleanupFailed(raw: text, message: error.localizedDescription)
      }
    }
    if !preset.runsCleanup || text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
      return DictationResult(raw: text, text: text)
    }
    var effective = preset
    effective.dictionary = []
    var body = dictationBody(
      preset: effective,
      dictionary: CleanupVocabulary.relevant(dictionary + preset.dictionary, to: text))
    body["audio"] = ""
    body["transcript_text"] = text
    // Do not let the server's prose-based heuristic under-budget opaque
    // formatting markers; the caller sizes batches against this output cap.
    body["max_tokens"] = VoiceFormatting.Plan.cleanupTokenBudget(for: text)
    do {
      let result = try await consumeDictation(
        request("ai/v1/dictate", body: encode(body)), cleanup: true, events: events,
        progress: { _ in })
      guard result.raw == text else {
        throw DictationError.invalid("Cleanup response does not match the submitted transcript.")
      }
      return result
    } catch is CancellationError { throw CancellationError() } catch {
      throw DictationError.cleanupFailed(raw: text, message: error.localizedDescription)
    }
  }
  private func consumeDictation(
    _ upload: URLRequest, cleanup: Bool, transport: URLSession? = nil,
    events: @Sendable @escaping (DictationProgress) async -> Void,
    progress: @Sendable @escaping (String) async -> Void
  ) async throws -> DictationResult {
    try await inferenceGate.enter()
    defer { Task { await inferenceGate.leave() } }
    try Task.checkCancellation()
    let started = ProcessInfo.processInfo.systemUptime
    var recognized: TimeInterval?
    var firstEvent: TimeInterval?
    var firstDraft: TimeInterval?
    var raw = ""
    var text = ""
    var complete = false
    var done = false
    do {
      let (bytes, response) = try await (transport ?? session).bytes(for: upload)
      defer { bytes.task.cancel() }
      guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
        var body = Data()
        for try await byte in bytes {
          try Task.checkCancellation()
          body.append(byte)
          if body.count >= ServerFailure.maxBodyBytes { break }
        }
        bytes.task.cancel()
        throw ServerFailure(response: response, body: body)
      }
      guard http.value(forHTTPHeaderField: "Content-Type")?.contains("text/event-stream") == true
      else {
        throw DictationError.invalid("Server did not return dictation events.")
      }
      var decoder = SSEDecoder()
      for try await byte in bytes {
        try Task.checkCancellation()
        guard let payload = try decoder.push(byte) else { continue }
        if payload == "[DONE]" {
          done = true
          break
        }
        guard
          let value = try JSONSerialization.jsonObject(with: Data(payload.utf8)) as? [String: Any]
        else { continue }
        if firstEvent == nil { firstEvent = ProcessInfo.processInfo.systemUptime - started }
        switch value["type"] as? String {
        case "dictation.transcript":
          guard recognized == nil, !complete,
            let transcript = (value["transcript"] as? [String: Any])?["text"] as? String
          else {
            throw DictationError.invalid("Invalid dictation transcript lifecycle.")
          }
          raw = transcript
          recognized = ProcessInfo.processInfo.systemUptime
          await progress(raw)
          await events(
            DictationProgress(phase: cleanup && !raw.isEmpty ? .cleaning : .transcribing, raw: raw))
        case "dictation.delta":
          guard recognized != nil, !complete, let delta = value["delta"] as? String else {
            throw DictationError.invalid("Invalid dictation delta lifecycle.")
          }
          if cleanup && firstDraft == nil {
            firstDraft = ProcessInfo.processInfo.systemUptime - started
          }
          text += delta
          await progress(text)
          await events(
            DictationProgress(phase: cleanup ? .cleaning : .transcribing, raw: raw, draft: text))
        case "dictation.completed":
          guard recognized != nil, !complete, let final = value["text"] as? String else {
            throw DictationError.invalid("Invalid dictation completion lifecycle.")
          }
          text = final
          complete = true
        case "error":
          throw DictationError.invalid(value["message"] as? String ?? "Dictation failed.")
        default: break
        }
      }
      if raw.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        && !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
      {
        throw DictationError.invalid("Dictation returned text without recognized speech.")
      }
      guard complete && done else {
        throw DictationError.invalid("Dictation stream ended before completion.")
      }
      if cleanup && !raw.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        && text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
      {
        throw DictationError.invalid("Cleanup returned empty text for speech.")
      }
    } catch is CancellationError { throw CancellationError() } catch {
      try Task.checkCancellation()
      if recognized != nil {
        throw DictationError.cleanupFailed(raw: raw, message: error.localizedDescription)
      }
      throw error
    }
    let ended = ProcessInfo.processInfo.systemUptime
    return DictationResult(
      raw: raw, text: text,
      timing: DictationTiming(
        recognition: (recognized ?? ended) - started,
        cleanup: cleanup ? ended - (recognized ?? ended) : 0, total: ended - started,
        firstEvent: firstEvent, firstDraft: firstDraft))
  }
  public func createSession(preset: Preset, dictionary: [String]) async throws -> String {
    var body: [String: Any] = [
      "model": preset.model, "dictionary": dictionary + preset.dictionary, "emit_partials": true,
      "ttl_seconds": 300, "max_segment_ms": 10000, "partial_interval_ms": 2000,
    ]
    if !preset.language.isEmpty { body["language"] = preset.language }
    let data = try await json("ai/v1/transcription/sessions", body: encode(body))
    guard let value = try JSONSerialization.jsonObject(with: data) as? [String: Any],
      let id = value["id"] as? String,
      !id.isEmpty, id.allSatisfy({ $0.isLetter || $0.isNumber || $0 == "-" || $0 == "_" })
    else { throw DictationError.invalid("Invalid session response.") }
    return id
  }
  public func appendSession(_ id: String, audio: Data, sampleRate: Double, commit: Bool = false)
    async throws -> String
  {
    let events = try await appendSessionEvents(
      id, audio: audio, sampleRate: sampleRate, commit: commit)
    return events.map(\.text).joined(separator: " ")
  }
  public func appendSessionEvents(
    _ id: String, audio: Data, sampleRate: Double, commit: Bool = false
  ) async throws -> [RecognitionEvent] {
    try await inferenceGate.enter()
    defer { Task { await inferenceGate.leave() } }
    try Task.checkCancellation()
    var body: [String: Any] = [
      "format": "pcm_f32", "sample_rate": Int(sampleRate), "commit": commit,
    ]
    let path = "ai/v1/transcription/sessions/\(id)/audio"
    let data: Data
    if audio.isEmpty {
      data = try await json(path, body: encode(body))
    } else {
      body["audio"] = "attachment:0"
      var upload = request(
        path,
        body: try AttachmentEnvelope.audio(
          metadata: encode(body), recording: audio, mime: "audio/pcm"))
      upload.setValue(AttachmentEnvelope.contentType, forHTTPHeaderField: "Content-Type")
      let (bytes, response) = try await session.data(for: upload)
      guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
        throw ServerFailure(response: response, body: bytes)
      }
      data = bytes
    }
    struct Response: Decodable { var data: [RecognitionEvent] }
    return try JSONDecoder().decode(Response.self, from: data).data
  }
  public func closeSession(_ id: String) async {
    var disposal = request("ai/v1/transcription/sessions/\(id)", method: "DELETE")
    // The server TTL is the fallback if disposal cannot reach the server.
    disposal.timeoutInterval = 3
    let request = disposal
    let session = self.session
    let operation = Task { try await session.data(for: request) }
    let deadline = Task {
      do { try await Task.sleep(for: .seconds(3)) } catch { return }
      operation.cancel()
    }
    defer { deadline.cancel() }
    _ = try? await withTaskCancellationHandler {
      try await operation.value
    } onCancel: {
      operation.cancel()
    }
  }
  public func setup(table: String) async throws {
    // GET distinguishes an existing table from a genuine create failure.
    do {
      _ = try await json("db/v1/tables/\(table)", method: "GET")
      return
    } catch {
      // Creation below surfaces auth, connection, and schema errors.
    }
    _ = try await json(
      "db/v1/tables/\(table)", method: "POST",
      body: encode([
        "num_shards": 1, "indexes": ["search": ["type": "full_text", "field": "text"]],
      ]))
  }
  public func saveSettings(_ config: Configuration) async throws {
    let data = try JSONEncoder().encode(config.validated())
    let document: [String: Any] = [
      "kind": "settings", "configuration": try JSONSerialization.jsonObject(with: data),
    ]
    _ = try await json(
      "db/v1/tables/\(config.table)/batch",
      body: encode(["inserts": ["settings-\(config.profile)": document]]))
  }
  public func loadSettings(table: String, profile: String) async throws -> Configuration {
    let data = try await json("db/v1/tables/\(table)/documents/settings-\(profile)", method: "GET")
    guard let doc = try JSONSerialization.jsonObject(with: data) as? [String: Any],
      let config = doc["configuration"]
    else { throw DictationError.invalid("No settings snapshot found.") }
    return try Configuration.decode(encodeObject(config))
  }
  private func encodeObject(_ object: Any) throws -> Data {
    try JSONSerialization.data(withJSONObject: object)
  }
  public func saveHistory(_ entry: HistoryEntry, table: String) async throws {
    let encoder = JSONEncoder()
    encoder.dateEncodingStrategy = .iso8601
    let data = try encoder.encode(entry)
    var doc = try JSONSerialization.jsonObject(with: data) as! [String: Any]
    doc["kind"] = "transcript"
    _ = try await json(
      "db/v1/tables/\(table)/batch",
      body: encode(["inserts": [entry.id: doc], "sync_level": "full_index"]))
  }
  public func deleteHistory(ids: [String], table: String) async throws {
    _ = try await json(
      "db/v1/tables/\(table)/batch", body: encode(["deletes": ids, "sync_level": "full_index"]))
  }
  public func search(_ text: String, table: String) async throws -> String {
    let data = try await json(
      "db/v1/tables/\(table)/query",
      body: encode([
        "full_text_search": ["query": text], "limit": 50,
      ]))
    return String(decoding: data, as: UTF8.self)
  }
}
