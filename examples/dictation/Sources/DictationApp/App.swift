// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AppKit
import DictationCore
import SwiftUI

@main struct DictationApp: App {
  @StateObject private var store = Store()
  var body: some Scene {
    WindowGroup("Antfly Dictation", id: "settings") {
      SettingsView(store: store).frame(minWidth: 760, minHeight: 640)
    }
    MenuBarExtra {
      MenuContent(store: store)
    } label: {
      Image(
        systemName: store.state == .recording
          ? "mic.fill" : store.state == .processing ? "ellipsis.circle" : "waveform")
    }
    .menuBarExtraStyle(.window)
  }
}

struct MenuContent: View {
  @ObservedObject var store: Store
  @Environment(\.openWindow) var openWindow
  var body: some View {
    VStack(alignment: .leading, spacing: 12) {
      Text("Antfly Dictation").font(.headline)
      Text(store.phase.label).font(.headline)
      Text(store.message).font(.caption).textSelection(.enabled)
      if !store.processingWarning.isEmpty {
        Text(store.processingWarning).font(.caption).foregroundStyle(.secondary)
      }
      if !store.preview.isEmpty { Text(store.preview).lineLimit(8).textSelection(.enabled) }
      HStack {
        Button(store.state == .recording ? "Finish" : "Record") {
          if store.state == .recording { store.finish() } else { store.start() }
        }.disabled(store.state == .processing)
        Button("Cancel") { store.cancel() }.disabled(store.state == .idle)
        Button("Copy") { store.copy(store.result.isEmpty ? store.preview : store.result) }.disabled(
          store.result.isEmpty && store.preview.isEmpty)
      }
      if store.canRecoverRaw {
        HStack {
          Button("Insert original") { store.insertOriginal() }
          Button("Retry cleanup") { store.retryCleanup() }
        }
      }
      Button("Settings & history") {
        openWindow(id: "settings")
        NSApp.activate(ignoringOtherApps: true)
      }
      Button("Quit") {
        store.cancel()
        NSApp.terminate(nil)
      }
    }.padding().frame(width: 350)
  }
}

struct SettingsView: View {
  @ObservedObject var store: Store
  var body: some View {
    VStack(alignment: .leading) {
      HStack {
        Image(nsImage: NSApplication.shared.applicationIconImage).resizable().scaledToFit().frame(
          width: 48, height: 48
        ).accessibilityLabel("Antfly Dictation")
        VStack(alignment: .leading) {
          Text("Antfly Dictation").font(.title)
          Text("Push to talk, with your vocabulary.").foregroundStyle(.secondary)
        }
        Spacer()
        Text(store.phase.label)
      }
      TabView {
        settings.tabItem { Text("Settings") }.tag(0)
        presets.tabItem { Text("Presets & dictionary") }.tag(1)
        transcript.tabItem { Text("Transcript") }.tag(2)
        history.tabItem { Text("History") }.tag(3)
      }
      Text(store.message).font(.callout).textSelection(.enabled).frame(
        maxWidth: .infinity, alignment: .leading)
      HStack {
        Button("Save settings") { store.save() }.disabled(
          store.state != .idle || store.working || store.capturingShortcut)
        Button("Import JSON…") { store.importSettings() }.disabled(
          store.state != .idle || store.working || store.capturingShortcut)
        Button("Export JSON…") { store.exportSettings() }
        Spacer()
        Button("Cancel recording / request") { store.cancel() }.disabled(store.state == .idle)
      }
    }.padding(20)
  }
  var settings: some View {
    ScrollView {
      Form {
        Section("Connection — audio is processed on this server") {
          TextField("Server origin", text: $store.config.server).onChange(of: store.config.server) {
            _, _ in store.connectionChanged()
          }
          SecureField("Bearer token (Keychain only)", text: $store.token)
          HStack {
            Button("Discover models") { store.discover() }
            Button("Create storage table") { store.setup() }
          }
          Toggle(
            "Allow parallel recognition and cleanup",
            isOn: Binding(
              get: { store.config.concurrentRequests == 2 },
              set: { store.config.concurrentRequests = $0 ? 2 : 1 }))
          Text(
            "Requires at least two inference slots on the server. Leave off for a single-slot local server."
          ).font(.caption).foregroundStyle(.secondary)
          TextField("Antfly table", text: $store.config.table)
          TextField("Settings profile", text: $store.config.profile)
          HStack {
            Button("Publish settings snapshot") { store.publish() }
            Button("Replace settings from Antfly") { store.download() }
          }
          Text(
            "Snapshots use explicit publish/replace. Local changes win until you replace them; publishing overwrites the selected remote profile. Credentials are never exported."
          ).font(.caption).foregroundStyle(.secondary)
        }
        Section("Microphone & shortcut") {
          Picker("Microphone", selection: $store.config.microphoneID) {
            Text("System default").tag("")
            ForEach(Microphone.devices, id: \.uniqueID) { Text($0.localizedName).tag($0.uniqueID) }
          }
          HStack {
            Button("Allow microphone") { store.requestMicrophone() }
          }
          ShortcutSettings(store: store)
          Toggle("Press to toggle recording instead of hold to talk", isOn: $store.config.toggle)
          Toggle("Paste into the original focused text field", isOn: $store.config.autoPaste)
        }
        Section("Retention") {
          Toggle("Save transcript history locally and in Antfly", isOn: $store.config.retainHistory)
          Toggle("Keep last audio recording locally", isOn: $store.config.retainAudio)
          Text(
            "History is off by default. Audio is never stored in Antfly. Turning retention off removes local retained data when settings are saved; remote history requires explicit deletion."
          ).font(.caption)
        }
        if !store.discovery.isEmpty {
          Section("Server model discovery") {
            Text(store.discovery).font(.system(.caption, design: .monospaced)).textSelection(
              .enabled)
          }
        }
      }.formStyle(.grouped)
    }.disabled(store.state != .idle || store.working)
  }
  var presets: some View {
    ScrollView {
      Form {
        Picker("Default preset", selection: $store.config.selectedPreset) {
          ForEach(store.config.presets) { Text($0.name).tag($0.id) }
        }
        TextField(
          "Global dictionary (comma-separated)",
          text: Binding(
            get: { store.config.dictionary.joined(separator: ", ") },
            set: { store.config.dictionary = terms($0) }))
        ForEach($store.config.presets) { $preset in
          Section(preset.name) {
            TextField("Name", text: $preset.name)
            Picker(
              "Output",
              selection: Binding(get: { preset.outputMode }, set: { preset.outputMode = $0 })
            ) {
              ForEach(DictationMode.allCases, id: \.self) { Text($0.label).tag($0) }
            }.pickerStyle(.segmented)
            Text(store.processingDescription(preset)).font(.caption).foregroundStyle(.secondary)
            if preset.outputMode == .rewrite {
              Picker(
                "Tone",
                selection: Binding(
                  get: { preset.style == "verbatim" ? "clean" : preset.style },
                  set: { preset.style = $0 })
              ) {
                Text("Natural").tag("clean")
                Text("Formal").tag("formal")
                Text("Casual").tag("casual")
              }
              TextField("Rewrite instructions", text: $preset.instructions, axis: .vertical)
            }
            TextField("Destination context", text: $preset.context)
            TextField("Language (empty = model default)", text: $preset.language)
            TextField(
              "Dictionary",
              text: Binding(
                get: { preset.dictionary.joined(separator: ", ") },
                set: { preset.dictionary = terms($0) }))
            TextField(
              "App bundle IDs (automatic selection)",
              text: Binding(
                get: { preset.appIDs.joined(separator: ", ") }, set: { preset.appIDs = terms($0) }))
            DisclosureGroup("Processing & models") {
              Picker(
                "Processing strategy",
                selection: Binding(
                  get: { preset.processingStrategy }, set: { preset.processingStrategy = $0 })
              ) {
                ForEach(ProcessingStrategy.allCases, id: \.self) { Text($0.label).tag($0) }
              }
              Text(
                "Automatic uses only advertised capabilities. Incremental requires final recognition segments; Rewrite needs the complete utterance. The selected engine never changes automatically."
              ).font(.caption)
              TextField(
                "Transcriber model or alias",
                text: Binding(
                  get: { preset.model },
                  set: {
                    preset.model = $0
                    preset.transcriberRoute = nil
                  }))
              Menu("Choose discovered transcriber") {
                ForEach(store.models.filter { $0.task == "transcribe" }) { model in
                  Button("\(model.id) · \(model.executorName)") {
                    preset.selectTranscriber(model)
                  }
                }
              }.disabled(store.models.isEmpty)
              TextField("Cleanup generator (empty = recognition only)", text: $preset.cleanupModel)
              Menu("Choose discovered cleanup generator") {
                Button("No cleanup") { preset.cleanupModel = "" }
                ForEach(preset.cleanupChoices(from: store.models)) { model in
                  Button(model.id) { preset.cleanupModel = model.id }
                }
              }.disabled(preset.outputMode == .verbatim)
              if !preset.usesNamedTranscriber {
                Text(
                  "Raw runtime transcribers use raw cleanup models and recorded-clip processing. Choose a named transcriber for shared incremental cleanup."
                ).font(.caption)
              }
              Toggle(
                "Legacy live preview when stable sessions are unavailable",
                isOn: $preset.livePreview)
            }
            Button("Delete preset") {
              store.config.presets.removeAll { $0.id == preset.id }
              if store.config.selectedPreset == preset.id {
                store.config.selectedPreset = store.config.presets.first?.id ?? ""
              }
            }.disabled(store.config.presets.count <= 1)
          }
        }
        Button("Add preset") {
          store.config.presets.append(Preset(id: UUID().uuidString, name: "New preset"))
        }
      }.formStyle(.grouped)
    }.disabled(store.state != .idle || store.working)
  }
  var transcript: some View {
    VStack(alignment: .leading) {
      Text("Raw transcript / live preview").font(.headline)
      ScrollView {
        Text(store.preview.isEmpty ? "Your transcript will appear here." : store.preview)
          .textSelection(.enabled).frame(maxWidth: .infinity, alignment: .leading)
      }.frame(maxHeight: 160)
      if !store.draft.isEmpty {
        Text("Cleanup preview — not inserted").font(.headline)
        Text(store.draft).lineLimit(5).textSelection(.enabled)
      }
      Text("Final text").font(.headline)
      TextEditor(text: $store.result).disabled(store.state != .idle)
      HStack {
        Button("Copy") { store.copy(store.result) }
        Button("Export text…") { store.exportResult() }
        Button("Retry recording (copy only)") { store.retry() }
        if store.canRecoverRaw {
          Button("Insert original") { store.insertOriginal() }
          Button("Retry cleanup (copy only)") { store.retryCleanup() }
          Button("Copy original") { store.copy(store.preview) }
        }
      }
      DisclosureGroup("Processing details") {
        Text(store.processingNote).font(.caption)
        if let timing = store.timing {
          Text(
            String(
              format:
                "Recognition requests: %.2f s · Cleanup requests: %.2f s · Session: %.2f s · Release to result: %.2f s",
              timing.recognition, timing.cleanup, timing.total, timing.afterRelease)
          ).font(.caption)
          Text(
            "Request durations include transport and queueing; overlapping stages do not add up to session duration."
          ).font(.caption).foregroundStyle(.secondary)
        }
      }
    }.padding()
  }
  var history: some View {
    VStack(alignment: .leading) {
      Text(store.historySyncStatus).font(.caption).foregroundStyle(.secondary).textSelection(
        .enabled)
      HStack {
        TextField("Search Antfly history", text: $store.searchText)
        Button("Search") { store.search() }
        Button("Upload local history") { store.syncHistory() }
      }
      if !store.searchResult.isEmpty {
        ScrollView {
          Text(store.searchResult).font(.system(.caption, design: .monospaced)).textSelection(
            .enabled)
        }.frame(maxHeight: 150)
      }
      List(store.history) { entry in
        VStack(alignment: .leading) {
          HStack {
            Text(entry.created, style: .date)
            Text(entry.preset)
            Text(entry.status).foregroundStyle(.secondary)
            Spacer()
            Button("Copy") { store.copy(entry.text) }
          }
          Text(entry.text).textSelection(.enabled)
        }
      }
      HStack {
        Button("Clear local history") { store.clearLocalHistory() }
        Button("Delete local history and matching Antfly documents") { store.clearHistory() }
      }
    }.padding()
  }
  private func terms(_ text: String) -> [String] {
    text.split(separator: ",").map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }.filter {
      !$0.isEmpty
    }
  }
}
