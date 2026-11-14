// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import CoreFoundation
import FoundationModels
import Speech
import AVFoundation

private enum BridgeError: Error { case status(Int32) }
private typealias Cancel = @convention(c) (UnsafeMutableRawPointer) -> Int32
private typealias Output = @convention(c) (UnsafeMutableRawPointer, UnsafePointer<UInt8>, Int) -> Int32

// The synchronous C boundary retains every borrowed pointer until the task has
// completed. Only the worker writes status; the semaphore publishes its write.
private final class Invocation: @unchecked Sendable {
    let context: UnsafeMutableRawPointer
    let cancel: Cancel
    let output: Output
    let limit: Int
    let finished = DispatchSemaphore(value: 0)
    var status: Int32 = 13
    init(_ context: UnsafeMutableRawPointer, _ cancel: @escaping Cancel, _ output: @escaping Output, _ limit: Int) {
        self.context = context; self.cancel = cancel; self.output = output; self.limit = limit
    }
    func check() throws {
        if Task.isCancelled || cancel(context) != 0 { throw BridgeError.status(7) }
    }
    func emit(_ data: Data) throws {
        try check()
        guard data.count <= limit else { throw BridgeError.status(9) }
        var empty: UInt8 = 0
        let result: Int32
        if data.isEmpty {
            result = withUnsafePointer(to: &empty) { output(context, $0, 0) }
        } else {
            result = data.withUnsafeBytes { bytes in
                output(context, bytes.bindMemory(to: UInt8.self).baseAddress!, data.count)
            }
        }
        if result != 0 { throw BridgeError.status(9) }
    }
}
private let gate = NSLock()
// Prefix caching is opt-in until an operator has measured its workload. Small
// dictations can spend more on session preparation than they save on prefill.
private let generationPrewarmingEnabled = ProcessInfo.processInfo.environment["ANTFLY_APPLE_PREFIX_PREWARM"] == "1"

@_cdecl("antfly_apple_bridge_abi_version")
public func bridgeABIVersion() -> UInt32 { 2 }

@available(macOS 26.0, *)
private func modelStatus() -> Int32 {
    switch SystemLanguageModel.default.availability {
    case .available: return 0
    case .unavailable(.appleIntelligenceNotEnabled): return 3
    case .unavailable(.modelNotReady): return 4
    case .unavailable: return 2
    }
}

// Validate even direct ABI callers before querying model readiness. Keep the
// generation API's signed 32-bit range, independently of the host Int width.
private func generationTokenBudget(_ request: [String: Any]) throws -> Int? {
    guard let raw = request["max_tokens"] else { return nil }
    guard let number = raw as? NSNumber, CFGetTypeID(number) != CFBooleanGetTypeID(),
          let budget = raw as? Int, budget > 0, budget <= Int(Int32.max)
    else { throw BridgeError.status(1) }
    return budget
}

private func generationByteLimit(_ budget: Int?, _ responseLimit: Int) throws -> Int {
    guard responseLimit >= 0 else { throw BridgeError.status(1) }
    let (bytes, overflow) = (budget ?? 1024).multipliedReportingOverflow(by: 8)
    return overflow ? responseLimit : min(responseLimit, bytes)
}

// Keep one unused, instruction-only session warm. Never return a session after
// respond/streamResponse has appended a user's prompt or generated transcript.
// Exact prefix matching, a byte ceiling, and expiry bound both memory and scope.
@available(macOS 26.0, *)
private actor PreparedGeneration {
    static let shared = PreparedGeneration()
    private var idle: (key: String, session: LanguageModelSession, expires: ContinuousClock.Instant)?
    private var eviction: Task<Void, Never>?
    private var revision = UUID()
    private var hits = 0
    private var preparations = 0
    func statistics() -> (Bool, Int, Int) {
        (idle.map { $0.expires > .now } ?? false, hits, preparations)
    }
    func take(key: String, entries: [Transcript.Entry], reusable: Bool) -> LanguageModelSession {
        if reusable, let cached = idle, cached.key == key, cached.expires > .now {
            idle = nil
            eviction?.cancel()
            hits += 1
            return cached.session
        }
        return LanguageModelSession(model: .default, transcript: Transcript(entries: entries))
    }
    func prepare(key: String, entries: [Transcript.Entry]) {
        guard key.utf8.count <= 16384 else { return }
        eviction?.cancel()
        let session = LanguageModelSession(model: .default, transcript: Transcript(entries: entries))
        session.prewarm(promptPrefix: Prompt("Transcript:\n"))
        preparations += 1
        let token = UUID()
        revision = token
        idle = (key, session, .now.advanced(by: .seconds(60)))
        eviction = Task { [weak self] in
            do { try await Task.sleep(for: .seconds(60)) } catch { return }
            await self?.expire(token)
        }
    }
    private func expire(_ token: UUID) {
        guard revision == token else { return }
        idle = nil
        eviction = nil
    }
}

@available(macOS 26.0, *)
private func generate(_ request: [String: Any], _ invocation: Invocation) async throws {
    let tokenBudget = try generationTokenBudget(request)
    let requireCompletion = request["require_completion"] as? Bool ?? false
    let outputLimit = requireCompletion
        ? try generationByteLimit(tokenBudget, invocation.limit) : invocation.limit
    let status = modelStatus()
    guard status == 0 else { throw BridgeError.status(status) }
    guard let messages = request["messages"] as? [[String: String]],
          let last = messages.last, last["role"] == "user",
          let prompt = last["text"], !prompt.isEmpty else { throw BridgeError.status(1) }
    var entries: [Transcript.Entry] = []
    var instructions: [String] = []
    var history: [[String: String]] = []
    for message in messages.dropLast() {
        guard let role = message["role"], let text = message["text"] else { throw BridgeError.status(1) }
        if role == "system" {
            guard history.isEmpty else { throw BridgeError.status(1) }
            instructions.append(text)
        } else { history.append(message) }
    }
    if !instructions.isEmpty {
        entries.append(.instructions(.init(segments: [.text(.init(content: instructions.joined(separator: "\n")))], toolDefinitions: [])))
    }
    for (index, message) in history.enumerated() {
        let role = message["role"]!
        guard role == (index % 2 == 0 ? "user" : "assistant") else { throw BridgeError.status(1) }
        let segments: [Transcript.Segment] = [.text(.init(content: message["text"]!))]
        if role == "user" { entries.append(.prompt(.init(segments: segments))) }
        else { entries.append(.response(.init(assetIDs: [], segments: segments))) }
    }
    guard history.count % 2 == 0 else { throw BridgeError.status(1) }
    let key = instructions.joined(separator: "\n")
    let reusable = generationPrewarmingEnabled && history.isEmpty && prompt.hasPrefix("Transcript:\n")
    let session = reusable
        ? await PreparedGeneration.shared.take(key: key, entries: entries, reusable: true)
        : LanguageModelSession(model: .default, transcript: Transcript(entries: entries))
    // Apple silently truncates at maximumResponseTokens. Strict text consumers
    // must instead let the model stop naturally, rejecting an over-budget result.
    // Streaming bounds retained bytes even on macOS 26, which has no usage API.
    let options = GenerationOptions(temperature: request["temperature"] as? Double,
                                    maximumResponseTokens: requireCompletion ? nil : tokenBudget)
    let stream = session.streamResponse(to: prompt, options: options)
    var content = ""
    for try await snapshot in stream {
        try invocation.check()
        guard snapshot.content.utf8.count <= outputLimit else { throw BridgeError.status(requireCompletion ? 16 : 9) }
        content = snapshot.content
    }
    if requireCompletion {
        let response = try await stream.collect()
        guard response.rawContent.isComplete else { throw BridgeError.status(16) }
        if #available(macOS 27.0, *), let tokenBudget,
           response.usage.output.totalTokenCount > tokenBudget { throw BridgeError.status(16) }
    }
    try invocation.emit(Data(content.utf8))
    if reusable {
        await PreparedGeneration.shared.prepare(key: key, entries: entries)
    }
}

@available(macOS 26.0, *)
private func transcribe(_ request: [String: Any], _ audio: Data, _ invocation: Invocation) async throws {
    guard SpeechTranscriber.isAvailable else { throw BridgeError.status(2) }
    let language = request["language"] as? String ?? "en-US"
    guard let locale = await SpeechTranscriber.supportedLocale(equivalentTo: Locale(identifier: language)) else {
        throw BridgeError.status(5)
    }
    let timestamps = request["timestamps"] as? Bool ?? true
    let transcriber = SpeechTranscriber(locale: locale, transcriptionOptions: [], reportingOptions: [],
                                       attributeOptions: timestamps ? [.audioTimeRange] : [])
    if let installation = try await AssetInventory.assetInstallationRequest(supporting: [transcriber]) {
        guard request["download_assets"] as? Bool == true else { throw BridgeError.status(6) }
        try await installation.downloadAndInstall()
    }
    try invocation.check()
    let path = FileManager.default.temporaryDirectory.appendingPathComponent("antfly-speech-\(UUID().uuidString).audio")
    try audio.write(to: path, options: [.atomic, .completeFileProtectionUnlessOpen])
    defer { try? FileManager.default.removeItem(at: path) }
    let file = try AVAudioFile(forReading: path)
    let duration = Double(file.length) / file.processingFormat.sampleRate
    guard duration.isFinite, duration >= 0, duration <= 3600 else { throw BridgeError.status(14) }
    let analyzer = SpeechAnalyzer(modules: [transcriber])
    let analysis = Task { try await analyzer.start(inputAudioFile: file, finishAfterFile: true) }
    do {
        var segments: [[String: Any]] = []
        var texts: [String] = []
        var charged = 1024
        for try await result in transcriber.results {
            try invocation.check()
            let text = String(result.text.characters)
            charged += text.utf8.count * 12 + 256
            guard charged <= invocation.limit, segments.count < 16384 else { throw BridgeError.status(9) }
            var segment: [String: Any] = ["text": text,
                "start_ms": Int64((result.range.start.seconds * 1000).rounded()),
                "end_ms": Int64((CMTimeRangeGetEnd(result.range).seconds * 1000).rounded())]
            if timestamps {
                var words: [[String: Any]] = []
                for run in result.text.runs {
                    guard let range = run.audioTimeRange else { continue }
                    let word = String(result.text[run.range].characters)
                    charged += word.utf8.count * 6 + 128
                    guard charged <= invocation.limit else { throw BridgeError.status(9) }
                    words.append(["word": word,
                        "start_ms": Int64((range.start.seconds * 1000).rounded()),
                        "end_ms": Int64((CMTimeRangeGetEnd(range).seconds * 1000).rounded())])
                }
                segment["words"] = words
            }
            texts.append(text); segments.append(segment)
        }
        try await analysis.value
        let response: [String: Any] = ["text": texts.joined(separator: " "), "language": locale.identifier,
            "duration_ms": Int64((duration * 1000).rounded()), "segments": timestamps ? segments : []]
        try invocation.emit(JSONSerialization.data(withJSONObject: response))
    } catch {
        analysis.cancel()
        await analyzer.cancelAndFinishNow()
        _ = try? await analysis.value
        throw error
    }
}

@_cdecl("antfly_apple_invoke")
public func invoke(_ operation: Int32, _ json: UnsafePointer<UInt8>, _ jsonLength: Int,
                   _ audio: UnsafePointer<UInt8>?, _ audioLength: Int, _ limit: Int,
                   _ context: UnsafeMutableRawPointer, _ cancel: @escaping @convention(c) (UnsafeMutableRawPointer) -> Int32,
                   _ output: @escaping @convention(c) (UnsafeMutableRawPointer, UnsafePointer<UInt8>, Int) -> Int32) -> Int32 {
    guard #available(macOS 26.0, *) else { return 2 }
    guard gate.try() else { return 8 }
    defer { gate.unlock() }
    guard cancel(context) == 0 else { return 7 }
    guard jsonLength > 0, jsonLength <= 1024 * 1024, audioLength <= 128 * 1024 * 1024,
          let request = try? JSONSerialization.jsonObject(with: Data(bytes: json, count: jsonLength)) as? [String: Any]
    else { return 1 }
    let audioData = audio.map { Data(bytesNoCopy: UnsafeMutableRawPointer(mutating: $0), count: audioLength, deallocator: .none) } ?? Data()
    let invocation = Invocation(context, cancel, output, limit)
    let task = Task.detached {
        defer { invocation.finished.signal() }
        do {
            switch operation {
            case 1: try await generate(request, invocation)
            case 2: try await transcribe(request, audioData, invocation)
            case 3:
                let (prepared, hits, preparations) = await PreparedGeneration.shared.statistics()
                try invocation.emit(JSONSerialization.data(withJSONObject: ["generation_status": modelStatus(),
                    "generation_prewarm": ["enabled": generationPrewarmingEnabled, "ready": prepared, "hits": hits, "preparations": preparations],
                    "speech_available": SpeechTranscriber.isAvailable,
                    "speech_installed_locales": await SpeechTranscriber.installedLocales.map { $0.identifier }]))
            default: throw BridgeError.status(1)
            }
            invocation.status = 0
        } catch BridgeError.status(let status) { invocation.status = status }
        catch is CancellationError { invocation.status = 7 }
        catch {
            if #available(macOS 27.0, *), let modelError = error as? LanguageModelError {
                switch modelError {
                case .contextSizeExceeded: invocation.status = 10
                case .guardrailViolation, .refusal: invocation.status = 11
                default: invocation.status = 13
                }
            } else if let modelError = error as? LanguageModelSession.GenerationError {
                switch modelError {
                case .exceededContextWindowSize: invocation.status = 10
                case .guardrailViolation, .refusal: invocation.status = 11
                case .assetsUnavailable: invocation.status = 4
                default: invocation.status = 13
                }
            } else { invocation.status = 13 }
        }
    }
    while invocation.finished.wait(timeout: .now() + .milliseconds(50)) == .timedOut {
        if cancel(context) != 0 { task.cancel() }
    }
    return invocation.status
}
