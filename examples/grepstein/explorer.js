// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const status = document.querySelector("#status"),
  results = document.querySelector("#results");
let request;
document.querySelector("#search").onsubmit = async (e) => {
  e.preventDefault();
  if (request) request.abort();
  request = new AbortController();
  status.textContent = "Searching…";
  results.replaceChildren();
  try {
    let response = await fetch(
      "/api/search?" +
        new URLSearchParams({
          q: document.querySelector("#query").value,
          mode: document.querySelector("#mode").value,
        }),
      { signal: request.signal },
    );
    if (!response.ok) throw Error(await response.text());
    let data = await response.json();
    let groups = data.responses || [data];
    let total = 0;
    for (let group of groups) {
      if (group.error) throw Error(group.error);
      for (let hit of group.hits?.hits || group.hits || []) {
        total++;
        let card = document.createElement("article"),
          title = document.createElement("h3");
        title.textContent =
          hit._source?.title || hit.source?.title || hit._id || hit.id;
        card.append(title);
        let link = document.createElement("a");
        link.textContent = "Open original PDF";
        link.href = "/pdf?" + new URLSearchParams({ id: hit._id || hit.id });
        link.target = "_blank";
        link.rel = "noopener";
        card.append(link);
        let source = hit._source || hit.source || {};
        let matches = hit.hierarchy?.matches || hit.hierarchy?.chunks || [];
        let first = matches[0]?.hierarchy?.ancestors?.unit?.document;
        let page =
          first?.provenance?.page_number || first?.["provenance.page_number"];
        if (!page && source.metadata?.page_number) {
          link.href += "#page=" + source.metadata.page_number;
          link.textContent += " · page " + source.metadata.page_number;
        }
        if (page) {
          page += Math.max(1, source.metadata?.page_start || 1) - 1;
          link.href += "#page=" + page;
          link.textContent += " · page " + page;
        }
        for (let match of matches) {
          let p = document.createElement("p");
          p.textContent = (match._source || match.source || {}).text || "";
          card.append(p);
        }
        let evidence = document.createElement("details"),
          summary = document.createElement("summary"),
          pre = document.createElement("pre");
        summary.textContent = "Source and page evidence";
        pre.textContent = JSON.stringify(hit, null, 2);
        evidence.append(summary, pre);
        card.append(evidence);
        results.append(card);
      }
      if (group.graph_results) {
        let graph = document.createElement("details");
        graph.open = true;
        let title = document.createElement("summary");
        title.textContent = "Relationship neighborhoods and paths";
        let pre = document.createElement("pre");
        pre.textContent = JSON.stringify(group.graph_results, null, 2);
        graph.append(title);
        drawGraph(graph, group.graph_results.relations);
        graph.append(pre);
        results.append(graph);
      }
    }
    status.textContent =
      total +
      " results. Semantic and graph modes require their enrichment stages to finish.";
  } catch (error) {
    if (error.name !== "AbortError") status.textContent = error.message;
  }
};

function drawGraph(container, graph) {
  if (graph?.kind !== "nodes") return;
  const ns = "http://www.w3.org/2000/svg",
    svg = document.createElementNS(ns, "svg");
  svg.setAttribute("viewBox", "0 0 900 560");
  svg.style.width = "100%";
  svg.setAttribute("role", "group");
  svg.setAttribute("aria-label", "Extracted relationship neighborhood");
  const nodes = new Map(),
    edges = [],
    identity = (n) => (n.table ? n.table + "/" : "") + n.key;
  for (const node of graph.nodes || []) {
    nodes.set(identity(node), node);
    for (const edge of node.path_edges || []) {
      edges.push(edge);
      for (const endpoint of [edge.from, edge.to])
        if (!nodes.has(identity(endpoint)))
          nodes.set(identity(endpoint), endpoint);
    }
  }
  const positions = new Map();
  let i = 0;
  for (const [key, node] of nodes) {
    const angle = (2 * Math.PI * i++) / nodes.size;
    positions.set(key, {
      x: 450 + 250 * Math.cos(angle),
      y: 280 + 180 * Math.sin(angle),
    });
  }
  for (const edge of edges) {
    const from = positions.get(identity(edge.from)),
      to = positions.get(identity(edge.to));
    if (!from || !to) continue;
    const line = document.createElementNS(ns, "line");
    for (const [name, value] of Object.entries({
      x1: from.x,
      y1: from.y,
      x2: to.x,
      y2: to.y,
      stroke: "#a7bcb2",
    }))
      line.setAttribute(name, value);
    const title = document.createElementNS(ns, "title");
    title.textContent =
      edge.type + " · " + edge.direction + " · weight " + edge.weight;
    line.append(title);
    svg.append(line);
  }
  const evidence = document.createElement("pre");
  evidence.textContent = "Select a node to inspect its evidence.";
  for (const [key, node] of nodes) {
    const point = positions.get(key),
      group = document.createElementNS(ns, "g"),
      circle = document.createElementNS(ns, "circle"),
      label = document.createElementNS(ns, "text");
    circle.setAttribute("cx", point.x);
    circle.setAttribute("cy", point.y);
    circle.setAttribute("r", 8);
    circle.setAttribute("fill", "#28634f");
    label.setAttribute("x", point.x + 12);
    label.setAttribute("y", point.y + 4);
    label.setAttribute("font-size", 11);
    if (nodes.size > 24) label.style.visibility = "hidden";
    const name = node.document?.title || node.document?.name || node.key;
    label.textContent = name.length > 30 ? name.slice(0, 27) + "…" : name;
    group.append(circle, label);
    group.setAttribute("tabindex", "0");
    group.setAttribute("role", "button");
    group.setAttribute("aria-label", "Inspect " + name);
    const select = () => {
      evidence.textContent = JSON.stringify(node, null, 2);
    };
    group.onmouseenter = () => {
      label.style.visibility = "visible";
    };
    group.onmouseleave = () => {
      if (nodes.size > 24) label.style.visibility = "hidden";
    };
    group.onfocus = () => {
      label.style.visibility = "visible";
    };
    group.onblur = () => {
      if (nodes.size > 24) label.style.visibility = "hidden";
    };
    group.onclick = select;
    group.onkeydown = (e) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        select();
      }
    };
    svg.append(group);
  }
  container.append(svg, evidence);
}
