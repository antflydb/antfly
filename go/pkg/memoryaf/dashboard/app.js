(function () {
  const state = {
    identity: loadIdentity(),
    serverInfo: null,
    health: null,
    stats: null,
    memoryResults: [],
    selectedMemory: null,
    relatedMemories: [],
    entities: [],
    selectedEntity: null,
    sessions: [],
  };

  const els = {
    navLinks: [...document.querySelectorAll(".nav-link")],
    views: [...document.querySelectorAll(".view")],
    statusBanner: document.getElementById("status-banner"),
    refreshAll: document.getElementById("refresh-all"),
    newMemory: document.getElementById("new-memory"),
    metricTotal: document.getElementById("metric-total"),
    metricNamespace: document.getElementById("metric-namespace"),
    metricHealth: document.getElementById("metric-health"),
    metricExtractor: document.getElementById("metric-extractor"),
    chartTypes: document.getElementById("chart-types"),
    chartProjects: document.getElementById("chart-projects"),
    chartTags: document.getElementById("chart-tags"),
    chartSources: document.getElementById("chart-sources"),
    overviewRecent: document.getElementById("overview-recent"),
    memoryQuery: document.getElementById("memory-query"),
    memoryProject: document.getElementById("memory-project"),
    memorySourceBackend: document.getElementById("memory-source-backend"),
    memoryType: document.getElementById("memory-type"),
    memoryVisibility: document.getElementById("memory-visibility"),
    memoryEphemeral: document.getElementById("memory-ephemeral"),
    memoryExpandGraph: document.getElementById("memory-expand-graph"),
    applyMemoryFilters: document.getElementById("apply-memory-filters"),
    memoryList: document.getElementById("memory-list"),
    memoryDetail: document.getElementById("memory-detail"),
    memoryResultSummary: document.getElementById("memory-result-summary"),
    editMemory: document.getElementById("edit-memory"),
    deleteMemory: document.getElementById("delete-memory"),
    entityLabel: document.getElementById("entity-label"),
    entityEphemeral: document.getElementById("entity-ephemeral"),
    loadEntities: document.getElementById("load-entities"),
    entityList: document.getElementById("entity-list"),
    entityDetail: document.getElementById("entity-detail"),
    entityDetailHeading: document.getElementById("entity-detail-heading"),
    identityUser: document.getElementById("identity-user"),
    identityNamespace: document.getElementById("identity-namespace"),
    identityRole: document.getElementById("identity-role"),
    identityForm: document.getElementById("identity-form"),
    settingsUserID: document.getElementById("settings-user-id"),
    settingsNamespace: document.getElementById("settings-namespace"),
    settingsRole: document.getElementById("settings-role"),
    settingsAgentID: document.getElementById("settings-agent-id"),
    settingsDeviceID: document.getElementById("settings-device-id"),
    settingsSessionID: document.getElementById("settings-session-id"),
    namespaceForm: document.getElementById("namespace-form"),
    settingsInitNamespace: document.getElementById("settings-init-namespace"),
    serverInfo: document.getElementById("server-info"),
    refreshSessions: document.getElementById("refresh-sessions"),
    endCurrentSession: document.getElementById("end-current-session"),
    sessionList: document.getElementById("session-list"),
    dialog: document.getElementById("memory-dialog"),
    dialogTitle: document.getElementById("memory-dialog-title"),
    closeDialog: document.getElementById("close-memory-dialog"),
    memoryForm: document.getElementById("memory-form"),
    sourceReadonlyNote: document.getElementById("source-readonly-note"),
  };

  const formFields = {
    content: document.getElementById("form-content"),
    memoryType: document.getElementById("form-memory-type"),
    visibility: document.getElementById("form-visibility"),
    project: document.getElementById("form-project"),
    source: document.getElementById("form-source"),
    tags: document.getElementById("form-tags"),
    ephemeral: document.getElementById("form-ephemeral"),
    sessionID: document.getElementById("form-session-id"),
    agentID: document.getElementById("form-agent-id"),
    deviceID: document.getElementById("form-device-id"),
    eventTime: document.getElementById("form-event-time"),
    context: document.getElementById("form-context"),
    confidence: document.getElementById("form-confidence"),
    supersedes: document.getElementById("form-supersedes"),
    trigger: document.getElementById("form-trigger"),
    steps: document.getElementById("form-steps"),
    outcome: document.getElementById("form-outcome"),
    sourceBackend: document.getElementById("form-source-backend"),
    sourceID: document.getElementById("form-source-id"),
    sourcePath: document.getElementById("form-source-path"),
    sourceURL: document.getElementById("form-source-url"),
    sourceVersion: document.getElementById("form-source-version"),
    sectionPath: document.getElementById("form-section-path"),
  };

  let editingMemoryID = null;

  bindEvents();
  syncIdentityUI();
  refreshAll();

  function bindEvents() {
    els.navLinks.forEach((button) => {
      button.addEventListener("click", () => setView(button.dataset.view));
    });

    els.refreshAll.addEventListener("click", refreshAll);
    els.newMemory.addEventListener("click", () => openMemoryDialog(null));
    els.applyMemoryFilters.addEventListener("click", loadMemories);
    els.loadEntities.addEventListener("click", loadEntities);
    els.refreshSessions.addEventListener("click", loadSessions);
    els.endCurrentSession.addEventListener("click", endCurrentSession);
    els.editMemory.addEventListener("click", () => {
      if (state.selectedMemory) openMemoryDialog(state.selectedMemory);
    });
    els.deleteMemory.addEventListener("click", deleteSelectedMemory);

    els.identityForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      state.identity = {
        userId: clean(els.settingsUserID.value) || "dashboard",
        namespace: clean(els.settingsNamespace.value) || "default",
        role: clean(els.settingsRole.value) || "member",
        agentId: clean(els.settingsAgentID.value),
        deviceId: clean(els.settingsDeviceID.value),
        sessionId: clean(els.settingsSessionID.value),
      };
      saveIdentity(state.identity);
      syncIdentityUI();
      await refreshAll();
      showBanner("Identity updated.", false);
    });

    els.namespaceForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const namespace = clean(els.settingsInitNamespace.value);
      if (!namespace) {
        showBanner("Namespace is required.", true);
        return;
      }
      try {
        await api(`/api/v1/namespaces/${encodeURIComponent(namespace)}/init`, {
          method: "POST",
        });
        showBanner(`Namespace ${namespace} initialized.`, false);
      } catch (error) {
        showBanner(error.message, true);
      }
    });

    els.closeDialog.addEventListener("click", closeMemoryDialog);
    els.dialog.addEventListener("cancel", (event) => {
      event.preventDefault();
      closeMemoryDialog();
    });
    els.memoryForm.addEventListener("submit", submitMemoryForm);
  }

  async function refreshAll() {
    syncIdentityUI();
    await Promise.all([loadOverview(), loadMemories(), loadEntities(), loadServerInfo(), loadSessions()]);
  }

  async function loadOverview() {
    try {
      const [stats, recent, health] = await Promise.all([
        api(`/api/v1/stats${toQueryString({ ephemeral: false })}`),
        api(`/api/v1/memories${toQueryString({ limit: 8 })}`),
        api("/health"),
      ]);
      state.stats = stats;
      state.health = health;

      els.metricTotal.textContent = String(stats.total_memories || 0);
      els.metricNamespace.textContent = state.identity.namespace || "default";
      els.metricHealth.textContent = health.status || "unknown";
      els.metricExtractor.textContent = health.termite ? "Configured" : "Disabled";

      renderBars(els.chartTypes, stats.by_type || {});
      renderBars(els.chartProjects, stats.by_project || {});
      renderBars(els.chartTags, stats.by_tag || {});
      renderBars(els.chartSources, stats.by_source_backend || {});
      renderRecentMemories(els.overviewRecent, normalizeMemories(recent));
    } catch (error) {
      showBanner(error.message, true);
    }
  }

  async function loadServerInfo() {
    try {
      const [info, health] = await Promise.all([api("/api/v1/info"), api("/health")]);
      state.serverInfo = info;
      state.health = health;
      els.serverInfo.innerHTML = [
        serverRow("Name", info.name),
        serverRow("Version", info.version),
        serverRow("Description", info.description),
        serverRow("Health", health.status),
        serverRow("Antfly", health.antfly ? "reachable by configuration" : "unavailable"),
        serverRow("Extractor", health.termite ? "configured" : "disabled"),
      ].join("");
    } catch (error) {
      els.serverInfo.innerHTML = emptyState(error.message);
    }
  }

  async function loadMemories() {
    try {
      const query = clean(els.memoryQuery.value);
      const params = {
        project: clean(els.memoryProject.value),
        source_backend: clean(els.memorySourceBackend.value),
        memory_type: clean(els.memoryType.value),
        visibility: clean(els.memoryVisibility.value),
        ephemeral: els.memoryEphemeral.checked,
        limit: 50,
      };

      let results;
      if (query) {
        const searchResults = await api("/api/v1/memories/search", {
          method: "POST",
          body: JSON.stringify({
            query,
            project: params.project || undefined,
            source_backend: params.source_backend || undefined,
            memory_type: params.memory_type || undefined,
            visibility: params.visibility || undefined,
            expand_graph: els.memoryExpandGraph.checked,
            ephemeral: params.ephemeral,
            limit: params.limit,
          }),
        });
        results = normalizeSearchResults(searchResults);
      } else {
        const memories = await api(`/api/v1/memories${toQueryString(params)}`);
        results = normalizeMemories(memories).map((memory) => ({ memory, score: null }));
      }

      state.memoryResults = results;
      els.memoryResultSummary.textContent = `${results.length} result${results.length === 1 ? "" : "s"}`;
      renderMemoryList();

      if (state.selectedMemory) {
        const next = results.find((item) => item.memory.id === state.selectedMemory.id);
        state.selectedMemory = next ? next.memory : null;
      }
      if (!state.selectedMemory && results.length > 0) {
        state.selectedMemory = results[0].memory;
      }
      await renderSelectedMemory();
    } catch (error) {
      showBanner(error.message, true);
      els.memoryList.innerHTML = emptyState(error.message);
    }
  }

  function renderMemoryList() {
    if (state.memoryResults.length === 0) {
      els.memoryList.innerHTML = emptyState("No memories matched the current filters.");
      return;
    }

    els.memoryList.innerHTML = state.memoryResults
      .map(({ memory, score }) => {
        const active = state.selectedMemory && state.selectedMemory.id === memory.id ? " active" : "";
        return `
          <article class="item-card${active}" data-memory-id="${escapeHTML(memory.id)}">
            <p class="item-title">${escapeHTML(titleForMemory(memory))}</p>
            <p class="item-meta">${escapeHTML(memory.project || "No project")} · ${escapeHTML(memory.memory_type)} · ${escapeHTML(memory.visibility)}${memory.ephemeral ? " · ephemeral" : ""}${score !== null && score !== undefined ? ` · score ${Number(score).toFixed(2)}` : ""}</p>
            <div class="pill-row">
              ${renderPills(memory.tags || [])}
              ${memory.source_backend ? `<span class="pill">${escapeHTML(memory.source_backend)}</span>` : ""}
            </div>
          </article>
        `;
      })
      .join("");

    document.querySelectorAll("[data-memory-id]").forEach((node) => {
      node.addEventListener("click", async () => {
        const result = state.memoryResults.find((item) => item.memory.id === node.dataset.memoryId);
        if (!result) return;
        state.selectedMemory = result.memory;
        renderMemoryList();
        await renderSelectedMemory();
      });
    });
  }

  async function renderSelectedMemory() {
    els.editMemory.disabled = !state.selectedMemory;
    els.deleteMemory.disabled = !state.selectedMemory;

    if (!state.selectedMemory) {
      els.memoryDetail.innerHTML = `<div class="detail-empty">Select a memory to inspect its metadata, entities, and source references.</div>`;
      return;
    }

    const memory = normalizeMemory(state.selectedMemory);
    let relatedHTML = `<p class="subtle">Loading related memories…</p>`;
    let siblingHTML = memory.source_id ? `<p class="subtle">Loading sibling sections…</p>` : `<p class="subtle">No external source reference on this memory.</p>`;
    els.memoryDetail.innerHTML = renderMemoryDetail(memory, relatedHTML, siblingHTML);

    try {
      const requests = [
        api(
          `/api/v1/memories/${encodeURIComponent(memory.id)}/related${toQueryString({
            limit: 8,
            ephemeral: memory.ephemeral,
          })}`
        ),
      ];
      if (memory.source_id) {
        requests.push(
          api(
            `/api/v1/memories${toQueryString({
              source_id: memory.source_id,
              ephemeral: memory.ephemeral,
              limit: 25,
            })}`
          )
        );
      }
      const [related, siblings] = await Promise.all(requests);
      state.relatedMemories = normalizeSearchResults(related);
      relatedHTML = state.relatedMemories.length
        ? `<div class="stack-list">${state.relatedMemories
            .map((item) => `<div class="item-card"><p class="item-title">${escapeHTML(titleForMemory(item.memory))}</p><p class="item-meta">${escapeHTML(item.memory.project || "No project")} · score ${Number(item.score).toFixed(2)}</p></div>`)
            .join("")}</div>`
        : `<p class="subtle">No related memories found.</p>`;

      if (memory.source_id) {
        const normalizedSiblings = normalizeMemories(siblings || []).filter((item) => item.id !== memory.id);
        siblingHTML = normalizedSiblings.length
          ? `<div class="stack-list">${normalizedSiblings
              .map((item) => `<div class="item-card"><p class="item-title">${escapeHTML(titleForMemory(item))}</p><p class="item-meta">${escapeHTML((item.section_path || []).join(" > ") || item.source_path || "No section path")}</p></div>`)
              .join("")}</div>`
          : `<p class="subtle">No sibling sections found for this source document.</p>`;
      }
    } catch (error) {
      relatedHTML = `<p class="subtle">${escapeHTML(error.message)}</p>`;
      siblingHTML = `<p class="subtle">${escapeHTML(error.message)}</p>`;
    }

    els.memoryDetail.innerHTML = renderMemoryDetail(memory, relatedHTML, siblingHTML);
  }

  function renderMemoryDetail(memory, relatedHTML, siblingHTML) {
    return `
      <div class="detail-block">
        <div>
          <p class="item-title">${escapeHTML(titleForMemory(memory))}</p>
          <p class="item-meta">${escapeHTML(memory.project || "No project")} · ${escapeHTML(memory.memory_type)} · ${escapeHTML(memory.visibility)}${memory.ephemeral ? " · ephemeral" : ""}</p>
        </div>

        <div class="detail-grid">
          ${detailCell("ID", `<span class="mono">${escapeHTML(memory.id)}</span>`)}
          ${detailCell("Created By", escapeHTML(memory.created_by || "unknown"))}
          ${detailCell("Created At", escapeHTML(memory.created_at || "n/a"))}
          ${detailCell("Updated At", escapeHTML(memory.updated_at || "n/a"))}
          ${detailCell("Session", escapeHTML(memory.session_id || "n/a"))}
          ${detailCell("Agent", escapeHTML(memory.agent_id || "n/a"))}
          ${detailCell("Device", escapeHTML(memory.device_id || "n/a"))}
          ${detailCell("Source", escapeHTML(memory.source || "n/a"))}
        </div>

        <div>
          <p class="eyebrow">Content</p>
          <p>${escapeHTML(memory.content)}</p>
        </div>

        ${memory.tags.length ? `<div><p class="eyebrow">Tags</p><div class="pill-row">${renderPills(memory.tags)}</div></div>` : ""}

        ${(memory.entities || []).length ? `<div><p class="eyebrow">Entities</p><div class="pill-row">${memory.entities.map((entity) => `<span class="pill">${escapeHTML(entity.label)} · ${escapeHTML(entity.text)}</span>`).join("")}</div></div>` : ""}

        ${(memory.source_backend || memory.source_id || memory.source_path || memory.source_url) ? `
          <div>
            <p class="eyebrow">External Source Reference</p>
            <div class="detail-grid">
              ${detailCell("Backend", escapeHTML(memory.source_backend || "n/a"))}
              ${detailCell("Source ID", `<span class="mono">${escapeHTML(memory.source_id || "n/a")}</span>`)}
              ${detailCell("Source Path", `<span class="mono">${escapeHTML(memory.source_path || "n/a")}</span>`)}
              ${detailCell("Source Version", `<span class="mono">${escapeHTML(memory.source_version || "n/a")}</span>`)}
              ${detailCell("Section Path", escapeHTML((memory.section_path || []).join(" > ") || "n/a"))}
              ${detailCell("Source URL", memory.source_url ? `<a href="${escapeAttribute(memory.source_url)}" target="_blank" rel="noreferrer">${escapeHTML(memory.source_url)}</a>` : "n/a")}
            </div>
          </div>
        ` : ""}

        <div>
          <p class="eyebrow">Related Memories</p>
          ${relatedHTML}
        </div>

        <div>
          <p class="eyebrow">Sibling Sections</p>
          ${siblingHTML}
        </div>
      </div>
    `;
  }

  async function loadEntities() {
    try {
      const entities = await api(
        `/api/v1/entities${toQueryString({
          label: clean(els.entityLabel.value),
          limit: 60,
          ephemeral: els.entityEphemeral.checked,
        })}`
      );
      state.entities = Array.isArray(entities) ? entities : [];
      renderEntityList();
      if (state.selectedEntity) {
        const next = state.entities.find((entity) => matchEntity(entity, state.selectedEntity));
        state.selectedEntity = next || null;
      }
      if (!state.selectedEntity && state.entities.length > 0) {
        state.selectedEntity = state.entities[0];
      }
      await renderSelectedEntity();
    } catch (error) {
      els.entityList.innerHTML = emptyState(error.message);
    }
  }

  async function loadSessions() {
    try {
      const sessions = await api(`/api/v1/sessions${toQueryString({ limit: 30, agent_id: state.identity.agentId || undefined })}`);
      state.sessions = Array.isArray(sessions) ? sessions : [];
      renderSessions();
    } catch (error) {
      els.sessionList.innerHTML = emptyState(error.message);
    }
  }

  function renderSessions() {
    if (!state.sessions.length) {
      els.sessionList.innerHTML = emptyState("No active ephemeral sessions.");
      return;
    }
    els.sessionList.innerHTML = state.sessions
      .map((session) => `
        <article class="item-card">
          <p class="item-title">${escapeHTML(session.session_id)}</p>
          <p class="item-meta">${Number(session.memory_count || 0)} memories</p>
          <div class="inline-actions">
            <button class="button button-danger end-session-button" data-session-id="${escapeAttribute(session.session_id)}" type="button">End Session</button>
          </div>
        </article>
      `)
      .join("");

    document.querySelectorAll(".end-session-button").forEach((button) => {
      button.addEventListener("click", async () => {
        await endSession(button.dataset.sessionId);
      });
    });
  }

  function renderEntityList() {
    if (!state.entities.length) {
      els.entityList.innerHTML = emptyState("No entities found for this filter.");
      return;
    }
    els.entityList.innerHTML = state.entities
      .map((entity) => {
        const active = state.selectedEntity && matchEntity(entity, state.selectedEntity) ? " active" : "";
        return `
          <article class="item-card${active}" data-entity-label="${escapeHTML(entity.label)}" data-entity-text="${escapeHTML(entity.text)}">
            <p class="item-title">${escapeHTML(entity.text)}</p>
            <p class="item-meta">${escapeHTML(entity.label)} · mentions ${Number(entity.mention_count || 0)}</p>
          </article>
        `;
      })
      .join("");

    document.querySelectorAll("[data-entity-label]").forEach((node) => {
      node.addEventListener("click", async () => {
        state.selectedEntity = {
          label: node.dataset.entityLabel,
          text: node.dataset.entityText,
        };
        renderEntityList();
        await renderSelectedEntity();
      });
    });
  }

  async function renderSelectedEntity() {
    if (!state.selectedEntity) {
      els.entityDetailHeading.textContent = "Pick an entity to inspect linked memories";
      els.entityDetail.innerHTML = `<div class="detail-empty">No entity selected.</div>`;
      return;
    }

    els.entityDetailHeading.textContent = `${state.selectedEntity.label}:${state.selectedEntity.text}`;
    els.entityDetail.innerHTML = `<p class="subtle">Loading linked memories…</p>`;
    try {
      const results = await api(
        `/api/v1/entities/${encodeURIComponent(`${state.selectedEntity.label}:${state.selectedEntity.text}`)}/memories${toQueryString({
          limit: 25,
          ephemeral: els.entityEphemeral.checked,
        })}`
      );
      const normalized = normalizeSearchResults(results);
      if (!normalized.length) {
        els.entityDetail.innerHTML = emptyState("No memories linked to this entity.");
        return;
      }
      els.entityDetail.innerHTML = `
        <div class="stack-list">
          ${normalized
            .map((item) => `
              <div class="item-card">
                <p class="item-title">${escapeHTML(titleForMemory(item.memory))}</p>
                <p class="item-meta">${escapeHTML(item.memory.project || "No project")} · score ${Number(item.score).toFixed(2)}</p>
              </div>
            `)
            .join("")}
        </div>
      `;
    } catch (error) {
      els.entityDetail.innerHTML = emptyState(error.message);
    }
  }

  function openMemoryDialog(memory) {
    editingMemoryID = memory ? memory.id : null;
    els.dialogTitle.textContent = memory ? "Edit Memory" : "New Memory";
    els.sourceReadonlyNote.classList.toggle("hidden", !memory);
    toggleSourceInputs(!memory);

    const normalized = memory ? normalizeMemory(memory) : null;
    formFields.content.value = normalized?.content || "";
    formFields.memoryType.value = normalized?.memory_type || "semantic";
    formFields.visibility.value = normalized?.visibility || "team";
    formFields.project.value = normalized?.project || "";
    formFields.source.value = normalized?.source || "";
    formFields.tags.value = (normalized?.tags || []).join(", ");
    formFields.ephemeral.checked = Boolean(normalized?.ephemeral);
    formFields.sessionID.value = normalized?.session_id || state.identity.sessionId || "";
    formFields.agentID.value = normalized?.agent_id || state.identity.agentId || "";
    formFields.deviceID.value = normalized?.device_id || state.identity.deviceId || "";
    formFields.eventTime.value = normalized?.event_time ? toLocalInput(normalized.event_time) : "";
    formFields.context.value = normalized?.context || "";
    formFields.confidence.value = normalized?.confidence ?? "";
    formFields.supersedes.value = normalized?.supersedes || "";
    formFields.trigger.value = normalized?.trigger || "";
    formFields.steps.value = (normalized?.steps || []).join("\n");
    formFields.outcome.value = normalized?.outcome || "";
    formFields.sourceBackend.value = normalized?.source_backend || "";
    formFields.sourceID.value = normalized?.source_id || "";
    formFields.sourcePath.value = normalized?.source_path || "";
    formFields.sourceURL.value = normalized?.source_url || "";
    formFields.sourceVersion.value = normalized?.source_version || "";
    formFields.sectionPath.value = (normalized?.section_path || []).join(", ");

    els.dialog.showModal();
  }

  function closeMemoryDialog() {
    els.dialog.close();
    editingMemoryID = null;
    els.memoryForm.reset();
    toggleSourceInputs(true);
    els.sourceReadonlyNote.classList.add("hidden");
  }

  async function submitMemoryForm(event) {
    event.preventDefault();
    const base = {
      content: clean(formFields.content.value),
      memory_type: clean(formFields.memoryType.value) || "semantic",
      visibility: clean(formFields.visibility.value) || "team",
      project: clean(formFields.project.value),
      source: clean(formFields.source.value),
      tags: splitCommaField(formFields.tags.value),
      ephemeral: formFields.ephemeral.checked,
      session_id: clean(formFields.sessionID.value),
      agent_id: clean(formFields.agentID.value),
      device_id: clean(formFields.deviceID.value),
      event_time: clean(formFields.eventTime.value),
      context: clean(formFields.context.value),
      confidence: clean(formFields.confidence.value) ? Number(formFields.confidence.value) : undefined,
      supersedes: clean(formFields.supersedes.value),
      trigger: clean(formFields.trigger.value),
      steps: splitLines(formFields.steps.value),
      outcome: clean(formFields.outcome.value),
    };

    if (!base.content) {
      showBanner("Content is required.", true);
      return;
    }

    try {
      if (editingMemoryID) {
        await api(`/api/v1/memories/${encodeURIComponent(editingMemoryID)}`, {
          method: "PUT",
          body: JSON.stringify(base),
        });
        showBanner("Memory updated.", false);
      } else {
        await api("/api/v1/memories", {
          method: "POST",
          body: JSON.stringify({
            ...base,
            source_backend: clean(formFields.sourceBackend.value),
            source_id: clean(formFields.sourceID.value),
            source_path: clean(formFields.sourcePath.value),
            source_url: clean(formFields.sourceURL.value),
            source_version: clean(formFields.sourceVersion.value),
            section_path: splitCommaField(formFields.sectionPath.value),
          }),
        });
        showBanner("Memory created.", false);
      }
      closeMemoryDialog();
      await Promise.all([loadOverview(), loadMemories(), loadEntities(), loadSessions()]);
    } catch (error) {
      showBanner(error.message, true);
    }
  }

  async function deleteSelectedMemory() {
    if (!state.selectedMemory) return;
    if (!window.confirm(`Delete memory ${state.selectedMemory.id}?`)) return;
    try {
      await api(`/api/v1/memories/${encodeURIComponent(state.selectedMemory.id)}`, {
        method: "DELETE",
      });
      state.selectedMemory = null;
      showBanner("Memory deleted.", false);
      await Promise.all([loadOverview(), loadMemories(), loadEntities(), loadSessions()]);
    } catch (error) {
      showBanner(error.message, true);
    }
  }

  async function endCurrentSession() {
    if (!state.identity.sessionId) {
      showBanner("Set a session ID in Settings before ending the current session.", true);
      return;
    }
    await endSession(state.identity.sessionId);
  }

  async function endSession(sessionID) {
    if (!sessionID) return;
    if (!window.confirm(`End session ${sessionID}? This deletes its ephemeral memories.`)) return;
    try {
      await api(`/api/v1/sessions/${encodeURIComponent(sessionID)}/end`, {
        method: "POST",
      });
      if (state.identity.sessionId === sessionID) {
        state.identity.sessionId = "";
        saveIdentity(state.identity);
        syncIdentityUI();
      }
      showBanner(`Session ${sessionID} ended.`, false);
      await Promise.all([loadOverview(), loadMemories(), loadEntities(), loadSessions()]);
    } catch (error) {
      showBanner(error.message, true);
    }
  }

  async function api(path, options = {}) {
    const headers = {
      "Content-Type": "application/json",
      "X-User-ID": state.identity.userId || "dashboard",
      "X-Namespace": state.identity.namespace || "default",
      "X-Role": state.identity.role || "member",
    };
    if (state.identity.agentId) headers["X-Agent-ID"] = state.identity.agentId;
    if (state.identity.deviceId) headers["X-Device-ID"] = state.identity.deviceId;
    if (state.identity.sessionId) headers["X-Session-ID"] = state.identity.sessionId;

    const response = await fetch(path, {
      ...options,
      headers: { ...headers, ...(options.headers || {}) },
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({ error: response.statusText }));
      throw new Error(payload.error || `HTTP ${response.status}`);
    }
    return response.json();
  }

  function syncIdentityUI() {
    els.identityUser.textContent = state.identity.userId || "dashboard";
    els.identityNamespace.textContent = state.identity.namespace || "default";
    els.identityRole.textContent = state.identity.role || "member";

    els.settingsUserID.value = state.identity.userId || "dashboard";
    els.settingsNamespace.value = state.identity.namespace || "default";
    els.settingsRole.value = state.identity.role || "member";
    els.settingsAgentID.value = state.identity.agentId || "";
    els.settingsDeviceID.value = state.identity.deviceId || "";
    els.settingsSessionID.value = state.identity.sessionId || "";
    els.metricNamespace.textContent = state.identity.namespace || "default";
  }

  function setView(view) {
    els.navLinks.forEach((button) => button.classList.toggle("active", button.dataset.view === view));
    els.views.forEach((section) => section.classList.toggle("active", section.id === `view-${view}`));
  }

  function renderBars(container, values) {
    const entries = Object.entries(values || {}).sort((a, b) => b[1] - a[1]).slice(0, 7);
    if (!entries.length) {
      container.innerHTML = emptyState("No data available.");
      return;
    }
    const max = Math.max(...entries.map((entry) => entry[1]), 1);
    container.innerHTML = entries
      .map(([label, value]) => `
        <div class="bar-row">
          <div class="bar-label"><span>${escapeHTML(label || "unassigned")}</span><strong>${Number(value)}</strong></div>
          <div class="bar-track"><div class="bar-fill" style="width:${Math.max(8, (Number(value) / max) * 100)}%"></div></div>
        </div>
      `)
      .join("");
  }

  function renderRecentMemories(container, memories) {
    if (!memories.length) {
      container.innerHTML = emptyState("No memories found.");
      return;
    }
    container.innerHTML = memories
      .slice(0, 6)
      .map((memory) => `
        <div class="item-card">
          <p class="item-title">${escapeHTML(titleForMemory(memory))}</p>
          <p class="item-meta">${escapeHTML(memory.project || "No project")} · ${escapeHTML(memory.memory_type)}</p>
        </div>
      `)
      .join("");
  }

  function renderPills(values) {
    return values.map((value) => `<span class="pill">${escapeHTML(value)}</span>`).join("");
  }

  function detailCell(label, value) {
    return `<div><span>${label}</span><div>${value}</div></div>`;
  }

  function serverRow(label, value) {
    return `<div class="server-row"><span>${escapeHTML(label)}</span><strong>${escapeHTML(value)}</strong></div>`;
  }

  function emptyState(message) {
    return `<div class="empty-state">${escapeHTML(message)}</div>`;
  }

  function titleForMemory(memory) {
    return summarize(memory.content || "Untitled memory", 88);
  }

  function summarize(value, length) {
    const cleanValue = clean(value);
    return cleanValue.length > length ? `${cleanValue.slice(0, length - 1)}…` : cleanValue;
  }

  function normalizeSearchResults(items) {
    return (Array.isArray(items) ? items : []).map((item) => ({
      ...item,
      memory: normalizeMemory(item.memory || {}),
    }));
  }

  function normalizeMemories(items) {
    return (Array.isArray(items) ? items : []).map(normalizeMemory);
  }

  function normalizeMemory(memory) {
    return {
      ...memory,
      tags: ensureArray(memory.tags),
      entities: Array.isArray(memory.entities) ? memory.entities : [],
      steps: ensureArray(memory.steps),
      section_path: ensureArray(memory.section_path),
      ephemeral: Boolean(memory.ephemeral),
    };
  }

  function ensureArray(value) {
    if (Array.isArray(value)) return value.filter(Boolean);
    if (typeof value === "string" && value.trim()) {
      if (value.trim().startsWith("[")) {
        try {
          const parsed = JSON.parse(value);
          return Array.isArray(parsed) ? parsed.filter(Boolean) : [];
        } catch (_) {
          return [];
        }
      }
      return value.split(",").map((item) => item.trim()).filter(Boolean);
    }
    return [];
  }

  function splitCommaField(value) {
    return value
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
  }

  function splitLines(value) {
    return value
      .split("\n")
      .map((item) => item.trim())
      .filter(Boolean);
  }

  function toQueryString(params) {
    const search = new URLSearchParams();
    Object.entries(params || {}).forEach(([key, value]) => {
      if (value === undefined || value === null || value === "" || value === false) return;
      search.set(key, String(value));
    });
    const out = search.toString();
    return out ? `?${out}` : "";
  }

  function clean(value) {
    return typeof value === "string" ? value.trim() : "";
  }

  function showBanner(message, isError) {
    if (!message) {
      els.statusBanner.classList.add("hidden");
      els.statusBanner.textContent = "";
      return;
    }
    els.statusBanner.textContent = message;
    els.statusBanner.classList.remove("hidden");
    els.statusBanner.style.background = isError
      ? "rgba(166, 59, 48, 0.14)"
      : "rgba(91, 138, 93, 0.14)";
    els.statusBanner.style.borderColor = isError
      ? "rgba(166, 59, 48, 0.22)"
      : "rgba(91, 138, 93, 0.22)";
  }

  function toggleSourceInputs(enabled) {
    [
      formFields.sourceBackend,
      formFields.sourceID,
      formFields.sourcePath,
      formFields.sourceURL,
      formFields.sourceVersion,
      formFields.sectionPath,
    ].forEach((input) => {
      input.disabled = !enabled;
    });
  }

  function matchEntity(left, right) {
    return left && right && left.label === right.label && left.text === right.text;
  }

  function toLocalInput(value) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return "";
    const pad = (n) => String(n).padStart(2, "0");
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
  }

  function loadIdentity() {
    try {
      return {
        userId: "dashboard",
        namespace: "default",
        role: "member",
        agentId: "",
        deviceId: "",
        sessionId: "",
        ...JSON.parse(localStorage.getItem("memoryaf-identity") || "{}"),
      };
    } catch (_) {
      return {
        userId: "dashboard",
        namespace: "default",
        role: "member",
        agentId: "",
        deviceId: "",
        sessionId: "",
      };
    }
  }

  function saveIdentity(identity) {
    localStorage.setItem("memoryaf-identity", JSON.stringify(identity));
  }

  function escapeHTML(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function escapeAttribute(value) {
    return escapeHTML(value).replaceAll("`", "&#96;");
  }
})();
