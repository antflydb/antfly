// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

public actor AntflyClient {
  let origin: URL
  let token: String
  private let inferenceGate: InferenceGate
  let session: URLSession
  public init(
    server: String, token: String = "", session: URLSession? = nil, maxConcurrentRequests: Int = 1
  ) throws {
    guard (1...2).contains(maxConcurrentRequests) else {
      throw DictationError.invalid("Choose one or two concurrent inference requests.")
    }
    inferenceGate = InferenceGate(capacity: maxConcurrentRequests)
    var config = Configuration()
    config.server = server
    _ = try config.validated()
    origin = URL(string: server)!
    self.token = token
    let settings = URLSessionConfiguration.ephemeral
    settings.timeoutIntervalForRequest = 120
    settings.timeoutIntervalForResource = 300
    self.session = session ?? URLSession(configuration: settings)
  }
  private func request(_ path: String, method: String = "POST", body: Data? = nil) -> URLRequest {
    var request = URLRequest(url: origin.appendingPathComponent(path))
    request.httpMethod = method
    request.httpBody = body
    request.setValue("application/json", forHTTPHeaderField: "Content-Type")
    if !token.isEmpty { request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization") }
    return request
  }
  private func json(
    _ path: String, method: String = "POST", body: Data? = nil, allowed: [Int] = [200, 201, 204]
  ) async throws -> Data {
    let (data, response) = try await session.data(for: request(path, method: method, body: body))
    try Task.checkCancellation()
    guard let http = response as? HTTPURLResponse, allowed.contains(http.statusCode) else {
      let code = (response as? HTTPURLResponse)?.statusCode ?? 0
      throw DictationError.invalid(
        "Antfly HTTP \(code): \(String(decoding: data.prefix(2048), as: UTF8.self))")
    }
    return data
  }
  private func encode(_ value: [String: Any]) throws -> Data {
    try JSONSerialization.data(withJSONObject: value)
  }
  public func models() async throws -> String {
    String(decoding: try await json("ai/v1/models", method: "GET"), as: UTF8.self)
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
    events: @Sendable @escaping (DictationProgress) async -> Void = { _ in },
    progress: @Sendable @escaping (String) async -> Void
  ) async throws -> DictationResult {
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
    events: @Sendable @escaping (DictationProgress) async -> Void = { _ in }
  ) async throws -> DictationResult {
    guard text.utf8.count <= 65536 else {
      throw DictationError.invalid("Transcript exceeds the cleanup limit.")
    }
    if !preset.runsCleanup || text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
      return DictationResult(raw: text, text: text)
    }
    var body = dictationBody(preset: preset, dictionary: dictionary)
    body["audio"] = ""
    body["transcript_text"] = text
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
    _ upload: URLRequest, cleanup: Bool,
    events: @Sendable @escaping (DictationProgress) async -> Void,
    progress: @Sendable @escaping (String) async -> Void
  ) async throws -> DictationResult {
    try await inferenceGate.enter()
    defer { Task { await inferenceGate.leave() } }
    try Task.checkCancellation()
    let started = ProcessInfo.processInfo.systemUptime
    var recognized: TimeInterval?
    var raw = ""
    var text = ""
    var complete = false
    var done = false
    do {
      let (bytes, response) = try await session.bytes(for: upload)
      guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
        throw DictationError.invalid(
          "Dictation request failed (HTTP \((response as? HTTPURLResponse)?.statusCode ?? 0)). Check model readiness and connection."
        )
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
        switch value["type"] as? String {
        case "dictation.transcript":
          raw = (value["transcript"] as? [String: Any])?["text"] as? String ?? ""
          recognized = ProcessInfo.processInfo.systemUptime
          await progress(raw)
          await events(
            DictationProgress(phase: cleanup && !raw.isEmpty ? .cleaning : .transcribing, raw: raw))
        case "dictation.delta":
          text += value["delta"] as? String ?? ""
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
        cleanup: cleanup ? ended - (recognized ?? ended) : 0, total: ended - started))
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
        throw DictationError.invalid(
          "Transcription append failed (HTTP \((response as? HTTPURLResponse)?.statusCode ?? 0)).")
      }
      data = bytes
    }
    struct Response: Decodable { var data: [RecognitionEvent] }
    return try JSONDecoder().decode(Response.self, from: data).data
  }
  public func closeSession(_ id: String) async {
    _ = try? await json("ai/v1/transcription/sessions/\(id)", method: "DELETE")
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
