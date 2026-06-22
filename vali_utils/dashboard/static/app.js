/**
 * Validator Evaluation Dashboard — client-side logic.
 * Real-time score updates via SSE + Chart.js visualizations.
 */

const API = "";

let eventSource = null;
const scoreState = new Map();
const failureReports = [];
const MAX_FAILURE_REPORTS = 100;
function emptyPathStats(path) {
  return {
    path,
    jobs_total: 0,
    jobs_checked: 0,
    jobs_passed: 0,
    jobs_failed: 0,
    jobs_skipped: 0,
    entities_total: 0,
    entities_checked: 0,
    entities_passed: 0,
    entities_failed: 0,
    last_timestamp: null,
    last_status: "pending",
    detail: {},
  };
}

function emptyValidationStatsSession() {
  return {
    p2p: emptyPathStats("p2p"),
    s3: emptyPathStats("s3"),
    od: emptyPathStats("od"),
  };
}

const validationStatsState = { miners: [], session: emptyValidationStatsSession() };
const scoreHistory = new Map();
const prevValues = new Map();
let disableSetWeights = false;
let evaluationPaused = false;
let charts = {};
const MAX_HISTORY = 120;
const FEED_COLLAPSED_KEY = "dashboard_feed_collapsed";
const FEED_FILTER_KEY = "dashboard_feed_filter";
const feedEvents = [];
const MAX_FEED_EVENTS = 200;
let feedIssuesOnly = false;

const CHART_COLORS = {
  score: "#4f8cff",
  capped: "#64d2ff",
  localIncentive: "#34c759",
  chainIncentive: "#ffd60a",
  p2p: "#4f8cff",
  s3: "#ffd60a",
  od: "#bf5af2",
  credibility: "#30d158",
  s3Boost: "#ff9f0a",
  odBoost: "#bf5af2",
};

async function api(path, options = {}) {
  const resp = await fetch(`${API}${path}`, {
    headers: { "Content-Type": "application/json", ...options.headers },
    ...options,
  });
  if (!resp.ok) {
    const err = await resp.text();
    throw new Error(`${resp.status}: ${err}`);
  }
  return resp.json();
}

function formatTime(iso) {
  if (!iso) return "-";
  return new Date(iso).toLocaleTimeString();
}

function formatPct(v) {
  return (v * 100).toFixed(2) + "%";
}

function formatDelta(oldVal, newVal, decimals = 2) {
  const diff = newVal - oldVal;
  if (Math.abs(diff) < Math.pow(10, -decimals)) return "";
  const cls = diff > 0 ? "delta-up" : "delta-down";
  const sign = diff > 0 ? "+" : "";
  return `<span class="${cls}">${sign}${diff.toFixed(decimals)}</span>`;
}

function eventClass(type) {
  if (type.includes("fail") || type.includes("error")) return "failed";
  if (type.includes("score")) return "complete";
  if (type.includes("s3")) return "s3";
  if (type.includes("od")) return "od";
  if (type.includes("complete") || type.includes("passed")) return "complete";
  return "started";
}

function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function formatJsonBlock(obj) {
  if (obj === undefined || obj === null) return "—";
  try {
    return JSON.stringify(obj, null, 2);
  } catch (_) {
    return String(obj);
  }
}

function failureTypeLabel(type) {
  if (type === "od") return "On-Demand";
  if (type === "p2p") return "P2P";
  if (type === "s3") return "S3 / Parquet";
  return type || "?";
}

const FAILURE_PHASE_ORDER = [
  "index",
  "response",
  "download",
  "parse",
  "empty",
  "empty_submission",
  "schema",
  "basic",
  "uniqueness",
  "job_match",
  "request_fields",
  "metadata",
  "scraper",
  "s3_validation",
  "error",
  "failed",
];

function failurePhaseLabel(phase) {
  const labels = {
    scraper: "Content comparison (scraper)",
    job_match: "Job field match",
    request_fields: "Request field match",
    metadata: "Metadata completeness",
    schema: "Schema / format",
    basic: "Bucket metadata",
    uniqueness: "Duplicate entities",
    empty_submission: "Empty submission",
    empty: "Empty submission",
    download: "Download",
    parse: "Parse error",
    s3_validation: "S3 validation",
    index: "Miner index",
    response: "Miner response",
  };
  return labels[phase] || phase || "Validation failed";
}

function failurePhaseSortIndex(phase) {
  const idx = FAILURE_PHASE_ORDER.indexOf(phase);
  return idx >= 0 ? idx : FAILURE_PHASE_ORDER.length + 1;
}

function collectFailureMessageChain(report) {
  const seen = new Set();
  const entries = [];

  const add = (phase, message) => {
    const msg = String(message || "").trim();
    if (!msg || seen.has(msg)) return;
    seen.add(msg);
    entries.push({
      phase: phase || report.failure_phase || "failed",
      message: msg,
    });
  };

  const trail = report.validation_trail || [];
  if (trail.length) {
    trail.forEach((step) => {
      add(step.phase, step.message);
    });
    return entries;
  }

  (report.failures || []).forEach((f) => {
    const phase = f.phase || f.type || report.failure_phase;
    add(phase, f.validator_message || f.detail || f.reason);
  });
  (report.issues || []).forEach((issue) => {
    add(report.failure_phase || "s3_validation", issue);
  });
  add(report.failure_phase, report.reason);

  return entries.sort(
    (a, b) => failurePhaseSortIndex(a.phase) - failurePhaseSortIndex(b.phase)
  );
}

function renderPrimaryFailureMessages(report) {
  const chain = collectFailureMessageChain(report);
  if (!chain.length) {
    return `<div class="failure-primary-text">Validation failed</div>`;
  }
  return chain
    .map(
      (step, index) => `
      <div class="failure-primary-step">
        <div class="failure-primary-step-head">
          <span class="failure-primary-step-num">${index + 1}</span>
          <span class="failure-primary-step-phase">${escapeHtml(failurePhaseLabel(step.phase))}</span>
        </div>
        <div class="failure-primary-step-msg">${escapeHtml(step.message)}</div>
      </div>`
    )
    .join("");
}

function renderEntityPreviewBlock(entity, title) {
  if (!entity) {
    return `<div class="failure-compare-col"><h5>${escapeHtml(title)}</h5><p class="muted-text">No data</p></div>`;
  }
  const body = entity.content_json
    ? formatJsonBlock(entity.content_json)
    : entity.content_preview || "—";
  const meta = [
    entity.uri ? `URI: ${entity.uri}` : "",
    entity.label ? `Label: ${entity.label}` : "",
    entity.datetime ? `Time: ${entity.datetime}` : "",
  ]
    .filter(Boolean)
    .join(" · ");
  const sizeLine =
    entity.content_size_bytes !== undefined
      ? `<div class="failure-entity-size"><strong>Content size:</strong> ${escapeHtml(String(entity.content_size_bytes))} B</div>`
      : "";
  return `
    <div class="failure-compare-col">
      <h5>${escapeHtml(title)}</h5>
      ${meta ? `<div class="failure-entity-meta">${escapeHtml(meta)}</div>` : ""}
      ${sizeLine}
      <pre class="failure-entity-body">${escapeHtml(body)}</pre>
    </div>
  `;
}

function renderSizeComparison(comparison) {
  const sc = comparison.size_comparison;
  if (!sc) return "";
  const miner = sc.miner_content_size_bytes;
  const validator = sc.validator_content_size_bytes;
  if (miner === undefined && validator === undefined) return "";
  let delta = "";
  if (typeof sc.delta_bytes === "number") {
    const sign = sc.delta_bytes > 0 ? "+" : "";
    delta = ` · <span class="failure-size-delta">Δ ${sign}${sc.delta_bytes} B</span>`;
  }
  return `
    <div class="failure-size-compare">
      <strong>Content size comparison</strong>
      <span>Miner: <strong>${miner ?? "—"}</strong> B</span>
      <span>Validator: <strong>${validator ?? "—"}</strong> B</span>${delta}
    </div>
  `;
}

function renderFieldDiffTable(comparison) {
  const fieldDiffs = comparison.field_diffs || [];
  if (!fieldDiffs.length) return "";

  const focus = comparison.failure_focus;
  const mismatches = fieldDiffs.filter((d) => !d.match);
  const rows = mismatches.length ? mismatches : fieldDiffs;
  const title = mismatches.length
    ? `Field mismatches (${mismatches.length})`
    : "Compared fields";

  const body = rows
    .map((row) => {
      const focusClass =
        focus && (row.field === focus || (focus === "_content_size" && row.field === "body"))
          ? " failure-diff-focus"
          : "";
      const matchBadge = row.match
        ? `<span class="failure-diff-badge ok">match</span>`
        : `<span class="failure-diff-badge bad">mismatch</span>`;
      return `
        <tr class="${focusClass}">
          <td class="failure-diff-field">${escapeHtml(row.field)} ${matchBadge}</td>
          <td class="failure-diff-miner"><pre>${escapeHtml(formatJsonBlock(row.miner))}</pre></td>
          <td class="failure-diff-validator"><pre>${escapeHtml(formatJsonBlock(row.validator))}</pre></td>
        </tr>`;
    })
    .join("");

  return `
    <div class="failure-field-diffs">
      <h5>${escapeHtml(title)}${focus && focus !== "_content_size" ? ` · focus: <code>${escapeHtml(focus)}</code>` : ""}</h5>
      <table class="failure-diff-table">
        <thead>
          <tr><th>Field</th><th>Miner</th><th>Validator</th></tr>
        </thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

function extractComparisons(report) {
  const comparisons = [];
  const seenUris = new Set();
  const addComparison = (comparison) => {
    const uri = comparison?.uri || "";
    const key = uri || JSON.stringify(comparison);
    if (seenUris.has(key)) return;
    seenUris.add(key);
    comparisons.push(comparison);
  };

  (report.failures || []).forEach((f) => {
    if (f.content_comparison) {
      addComparison(f.content_comparison);
      return;
    }
    const miner = f.miner_submission || f.entity;
    const msg = f.validator_message || f.detail || f.reason;
    if (miner || msg) {
      addComparison({
        uri: f.uri,
        miner_submission: miner,
        validator_message: msg,
        validator_fetched: f.validator_fetched || null,
        job_requirements: f.job_requirements || null,
        phase: f.phase,
      });
    }
  });

  const entityResults =
    report.submission?.entity_validation_results
    || report.entity_validation_results
    || [];
  entityResults.forEach((result) => {
    if (result.passed) return;
    if (result.content_comparison) {
      addComparison(result.content_comparison);
      return;
    }
    addComparison({
      uri: result.uri,
      miner_submission: result.miner_submission || null,
      validator_message: result.validator_message || "Validation failed",
      phase: result.phase,
    });
  });

  if (!comparisons.length && report.submission?.failed_entity) {
    addComparison({
      uri: report.submission.failed_entity.uri,
      miner_submission: report.submission.failed_entity,
      validator_message: report.reason,
    });
  }
  return comparisons;
}

function renderEntityValidationResults(report) {
  const results =
    report.submission?.entity_validation_results
    || report.entity_validation_results
    || [];
  if (!results.length) return "";

  const passedCount = results.filter((r) => r.passed).length;
  const failedCount = results.length - passedCount;
  const rows = results
    .map((result) => {
      const statusClass = result.passed ? "entity-pass" : "entity-fail";
      const statusLabel = result.passed ? "PASSED" : "FAILED";
      const phase = result.phase ? failurePhaseLabel(result.phase) : "—";
      const message = result.validator_message || (result.passed ? "OK" : "Validation failed");
      return `
        <tr class="${statusClass}">
          <td><span class="entity-status-badge ${statusClass}">${statusLabel}</span></td>
          <td class="entity-result-uri">${escapeHtml(result.uri || result.post_id || "—")}</td>
          <td>${escapeHtml(phase)}</td>
          <td class="entity-result-message">${escapeHtml(message)}</td>
        </tr>`;
    })
    .join("");

  return `
    <div class="failure-section">
      <h4 class="failure-section-title">Sampled entity validation results (${passedCount} passed, ${failedCount} failed)</h4>
      <table class="entity-validation-table">
        <thead>
          <tr>
            <th>Status</th>
            <th>URI</th>
            <th>Phase</th>
            <th>Message</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

function renderComparisonSection(comparison, index) {
  const uri = comparison.uri || "";
  const validatorMsg =
    comparison.validator_message || "Validation failed for this entity.";
  const jobReq = comparison.job_requirements;
  const hasFetched = !!comparison.validator_fetched;
  const sizeHtml = renderSizeComparison(comparison);
  const diffHtml = renderFieldDiffTable(comparison);

  let compareHtml = "";
  if (hasFetched || comparison.miner_submission) {
    compareHtml = `
      <div class="failure-compare-grid">
        ${renderEntityPreviewBlock(comparison.miner_submission, "Miner submitted")}
        ${hasFetched
          ? renderEntityPreviewBlock(comparison.validator_fetched, "Validator re-scraped")
          : `<div class="failure-compare-col failure-compare-message">
              <h5>Validator result</h5>
              <div class="failure-validator-msg">${escapeHtml(validatorMsg)}</div>
              <p class="muted-text">Live re-scrape preview unavailable. See failure message and field table below.</p>
            </div>`}
      </div>
    `;
  }

  const jobReqHtml = jobReq
    ? `<div class="failure-job-req">
        <h5>Job requirements</h5>
        <pre>${escapeHtml(formatJsonBlock(jobReq))}</pre>
      </div>`
    : "";

  return `
    <div class="failure-comparison-block">
      <div class="failure-comparison-head">
        <span class="failure-comparison-label">Content check #${index + 1}</span>
        ${uri ? `<span class="failure-comparison-uri">${escapeHtml(uri)}</span>` : ""}
      </div>
      <div class="failure-validator-banner">${escapeHtml(validatorMsg)}</div>
      ${sizeHtml}
      ${compareHtml}
      ${diffHtml}
      ${jobReqHtml}
    </div>
  `;
}

function renderFailureContext(report) {
  if (report.validation_type === "od") {
    const jobId = report.submission?.job_id || report.job?.id || "—";
    const expected = report.expected || {};
    return `
      <details class="failure-context-details">
        <summary>Job context</summary>
        <div class="failure-context-body">
          <div><strong>Job ID:</strong> ${escapeHtml(jobId)}</div>
          <div><strong>Platform:</strong> ${escapeHtml(expected.platform || report.job?.job?.platform || "—")}</div>
          <div><strong>Keywords:</strong> ${escapeHtml(JSON.stringify(expected.keywords ?? report.job?.job?.keywords ?? null))}</div>
          <div><strong>Subreddit:</strong> ${escapeHtml(expected.subreddit ?? report.job?.job?.subreddit ?? "—")}</div>
          <div><strong>Usernames:</strong> ${escapeHtml(JSON.stringify(expected.usernames ?? report.job?.job?.usernames ?? null))}</div>
          <div><strong>Date range:</strong> ${escapeHtml(expected.start_date || "—")} → ${escapeHtml(expected.end_date || "—")}</div>
          <div><strong>Limit / mode:</strong> ${escapeHtml(String(expected.limit ?? "—"))} / ${escapeHtml(expected.keyword_mode || "—")}</div>
          <div><strong>Entities submitted:</strong> ${escapeHtml(String(report.submission?.entity_count ?? "—"))}</div>
        </div>
      </details>
    `;
  }
  if (report.validation_type === "p2p") {
    return `
      <details class="failure-context-details">
        <summary>P2P bucket context</summary>
        <div class="failure-context-body">
          <div><strong>Bucket:</strong> ${escapeHtml(report.job?.bucket_id || "—")}</div>
          <div><strong>Entities in sample:</strong> ${escapeHtml(String(report.submission?.entity_count ?? "—"))}</div>
        </div>
      </details>
    `;
  }
  if (report.validation_type === "s3") {
    return `
      <details class="failure-context-details">
        <summary>S3 / parquet context</summary>
        <div class="failure-context-body">
          <pre>${escapeHtml(formatJsonBlock(report.job))}</pre>
          ${report.submission ? `<pre>${escapeHtml(formatJsonBlock(report.submission))}</pre>` : ""}
        </div>
      </details>
    `;
  }
  return "";
}

function renderFailureCard(report) {
  const card = document.createElement("div");
  card.className = "failure-card";
  card.dataset.id = report.id || "";

  const comparisons = extractComparisons(report);
  const comparisonHtml = comparisons
    .map((c, i) => renderComparisonSection(c, i))
    .join("");
  const entityResultsHtml = renderEntityValidationResults(report);

  const issues = (report.issues || [])
    .map((i) => `<li>${escapeHtml(i)}</li>`)
    .join("");
  const hints = (report.hints || [])
    .map((h) => `<li>${escapeHtml(h)}</li>`)
    .join("");

  const showCompare = comparisons.length > 0;

  card.innerHTML = `
    <div class="failure-card-header">
      <span class="failure-badge ${escapeHtml(report.validation_type)}">${failureTypeLabel(report.validation_type)}</span>
      <span class="failure-phase">${escapeHtml(failurePhaseLabel(report.failure_phase))}</span>
      <span class="failure-time">${formatTime(report.timestamp)}</span>
      <span class="failure-miner">UID <strong>${report.uid}</strong> · ${escapeHtml((report.hotkey || "").slice(0, 18))}</span>
    </div>

    <div class="failure-primary-message">
      <div class="failure-primary-label">Validation failure messages</div>
      ${renderPrimaryFailureMessages(report)}
    </div>

    ${entityResultsHtml}

    ${showCompare || comparisonHtml
      ? `<div class="failure-section"><h4 class="failure-section-title">Miner vs validator comparison</h4>${comparisonHtml}</div>`
      : ""}

    ${issues
      ? `<div class="failure-section"><h4 class="failure-section-title">Issues</h4><ul class="failure-issues">${issues}</ul></div>`
      : ""}

    ${renderFailureContext(report)}

    ${hints
      ? `<div class="failure-section"><h4 class="failure-section-title">Hints</h4><ul class="failure-hints">${hints}</ul></div>`
      : ""}
  `;
  return card;
}

function upsertFailureReport(report) {
  if (!report || !report.id) return;
  const idx = failureReports.findIndex((r) => r.id === report.id);
  if (idx >= 0) failureReports[idx] = report;
  else failureReports.unshift(report);
  while (failureReports.length > MAX_FAILURE_REPORTS) failureReports.pop();
}

function updateFailuresTabBadge(count) {
  const tab = document.querySelector('.tab[data-tab="tab-failures"]');
  if (!tab) return;
  tab.textContent = count > 0 ? `Validation Failures (${count})` : "Validation Failures";
}

function renderFailuresList() {
  const list = document.getElementById("failures-list");
  const countEl = document.getElementById("failures-count");
  if (!list) return;

  const typeFilter = document.getElementById("failure-type-filter")?.value || "";
  const uidFilter = document.getElementById("failure-uid-filter")?.value || "";
  let items = [...failureReports];
  if (typeFilter) items = items.filter((r) => r.validation_type === typeFilter);
  if (uidFilter) items = items.filter((r) => String(r.uid) === uidFilter);
  items.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

  list.innerHTML = "";
  if (!items.length) {
    list.innerHTML = '<p class="muted-text">No validation failures match the current filters.</p>';
  } else {
    items.forEach((r) => list.appendChild(renderFailureCard(r)));
  }
  if (countEl) countEl.textContent = `${items.length} report(s)`;
  updateFailuresTabBadge(items.length);
}

function updateFailureUidOptions(rows) {
  const select = document.getElementById("failure-uid-filter");
  if (!select) return;
  const current = select.value;
  const uids = new Set(rows.map((r) => r.uid));
  failureReports.forEach((r) => uids.add(r.uid));
  select.innerHTML = '<option value="">All miners</option>';
  [...uids].sort((a, b) => a - b).forEach((uid) => {
    const opt = document.createElement("option");
    opt.value = uid;
    opt.textContent = `UID ${uid}`;
    select.appendChild(opt);
  });
  if (current && [...select.options].some((o) => o.value === current)) {
    select.value = current;
  }
}

function pctBar(passed, total) {
  if (!total) return 0;
  return Math.min(100, Math.round((passed / total) * 100));
}

function formatRatio(passed, total) {
  return `${passed} / ${total}`;
}

function pathLabel(path) {
  if (path === "p2p") return "P2P";
  if (path === "s3") return "S3 / Parquet";
  if (path === "od") return "On-Demand";
  return path;
}

function renderPathCard(path, stats, cumulative = false) {
  const s = stats || {};
  const jobsTotal = s.jobs_total || 0;
  const jobsPassed = s.jobs_passed || 0;
  const entTotal = s.entities_total || 0;
  const entChecked = s.entities_checked || entTotal;
  const entPassed = s.entities_passed || 0;
  const status = s.last_status || "pending";
  const suffix = cumulative ? " (cumulative)" : "";

  const card = document.createElement("div");
  card.className = `stats-path-card ${path}`;
  card.innerHTML = `
    <div class="stats-path-header">
      <span class="stats-path-name">${pathLabel(path)}${suffix}</span>
      <span class="stats-status ${status}">${status}</span>
    </div>
    <div class="stats-metric-row">
      <div class="stats-metric-label">Jobs passed / total</div>
      <div class="stats-metric-value">${formatRatio(jobsPassed, jobsTotal)}</div>
      <div class="stats-bar"><div class="stats-bar-fill pass" style="width:${pctBar(jobsPassed, jobsTotal)}%"></div></div>
    </div>
    <div class="stats-metric-row">
      <div class="stats-metric-label">Entities passed / checked</div>
      <div class="stats-metric-value">${formatRatio(entPassed, entChecked)}</div>
      <div class="stats-bar"><div class="stats-bar-fill pass" style="width:${pctBar(entPassed, entChecked)}%"></div></div>
    </div>
    ${entTotal !== entChecked ? `<div class="stats-metric-row" style="color:var(--muted);font-size:0.72rem">In bucket/submission: ${entTotal}</div>` : ""}
    ${path === "s3" && s.detail?.files_checked !== undefined ? `<div class="stats-metric-row" style="color:var(--muted);font-size:0.72rem">Files checked: ${s.detail.files_checked}</div>` : ""}
    ${path === "od" && s.detail?.credibility_bumped ? `<div class="stats-metric-row" style="color:var(--muted);font-size:0.72rem">Credibility-bumped jobs: ${s.detail.credibility_bumped}</div>` : ""}
    ${!cumulative && s.last_timestamp ? `<div style="font-size:0.7rem;color:var(--muted);margin-top:0.35rem">${formatTime(s.last_timestamp)}</div>` : ""}
  `;
  return card;
}

function renderRatioCell(passed, total) {
  const cls = passed === total && total > 0 ? "pass-cell" : passed < total ? "fail-cell" : "";
  return `<span class="${cls}">${formatRatio(passed, total)}</span>`;
}

function renderValidationStats() {
  const sessionGrid = document.getElementById("stats-session-grid");
  const tbody = document.getElementById("stats-miners-body");
  const detail = document.getElementById("stats-miner-detail");
  if (!sessionGrid || !tbody) return;

  sessionGrid.innerHTML = "";
  ["p2p", "s3", "od"].forEach((path) => {
    sessionGrid.appendChild(
      renderPathCard(path, validationStatsState.session[path], true)
    );
  });

  tbody.innerHTML = "";
  const miners = [...validationStatsState.miners].sort((a, b) => a.uid - b.uid);
  if (!miners.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="muted-text">No validation stats yet — waiting for evaluator cycle.</td></tr>';
  } else {
    miners.forEach((m) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${m.uid}</td>
        <td>${renderRatioCell(m.p2p?.jobs_passed || 0, m.p2p?.jobs_total || 0)}</td>
        <td>${renderRatioCell(m.p2p?.entities_passed || 0, m.p2p?.entities_checked || 0)}</td>
        <td>${renderRatioCell(m.s3?.jobs_passed || 0, m.s3?.jobs_total || 0)}</td>
        <td>${renderRatioCell(m.s3?.entities_passed || 0, m.s3?.entities_checked || 0)}</td>
        <td>${renderRatioCell(m.od?.jobs_passed || 0, m.od?.jobs_total || 0)}</td>
        <td>${renderRatioCell(m.od?.entities_passed || 0, m.od?.entities_checked || 0)}</td>
        <td>${formatTime(m.last_updated)}</td>
      `;
      tr.style.cursor = "pointer";
      tr.addEventListener("click", () => renderMinerStatsDetail(m));
      tbody.appendChild(tr);
    });
  }

  if (detail) {
    if (!miners.length) {
      detail.innerHTML = "";
    } else if (miners.length === 1) {
      renderMinerStatsDetail(miners[0]);
    }
  }

  const updatedEl = document.getElementById("stats-updated");
  if (updatedEl) {
    updatedEl.textContent = `Last update: ${formatTime(new Date().toISOString())}`;
  }
}

function renderMinerStatsDetail(miner) {
  const detail = document.getElementById("stats-miner-detail");
  if (!detail || !miner) return;
  detail.innerHTML = `<strong>UID ${miner.uid}</strong> — ${(miner.hotkey || "").slice(0, 20)}…`;
  const grid = document.createElement("div");
  grid.className = "stats-path-grid";
  grid.style.marginTop = "0.75rem";
  ["p2p", "s3", "od"].forEach((path) => {
    grid.appendChild(renderPathCard(path, miner[path], false));
  });
  detail.appendChild(grid);
}

async function loadValidationStats() {
  try {
    const data = await api("/dashboard/api/validation-stats");
    validationStatsState.miners = data.miners || [];
    validationStatsState.session = data.session || emptyValidationStatsSession();
    renderValidationStats();
  } catch (e) {
    console.error("Validation stats load failed:", e);
  }
}

let failureReportsOffset = 0;
const FAILURE_REPORTS_PAGE = 100;

async function loadValidationFailures(reset = true) {
  try {
    if (reset) failureReportsOffset = 0;
    const type = document.getElementById("failure-type-filter")?.value || "";
    const uid = document.getElementById("failure-uid-filter")?.value || "";
    const params = new URLSearchParams({
      limit: String(FAILURE_REPORTS_PAGE),
      offset: String(failureReportsOffset),
    });
    if (type) params.set("validation_type", type);
    if (uid) params.set("uid", uid);
    const data = await api(`/dashboard/api/validation-failures?${params}`);
    if (reset) failureReports.length = 0;
    (data.failures || [])
      .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
      .forEach((r) => upsertFailureReport(r));
    renderFailuresList();
    const loadMoreBtn = document.getElementById("load-more-failures");
    if (loadMoreBtn) {
      const total = data.total ?? failureReports.length;
      const loaded = failureReportsOffset + (data.failures || []).length;
      loadMoreBtn.style.display = loaded < total ? "inline-block" : "none";
      loadMoreBtn.textContent = `Load more (${loaded}/${total})`;
    }
  } catch (e) {
    console.error("Validation failures load failed:", e);
  }
}

async function loadMoreValidationFailures() {
  failureReportsOffset += FAILURE_REPORTS_PAGE;
  await loadValidationFailures(false);
}

async function clearValidationFailures() {
  const confirmed = window.confirm(
    "Delete all validation failure history?\n\n" +
      "This removes P2P, S3, and on-demand failure reports from the dashboard. " +
      "Scores and validation stats are not reset."
  );
  if (!confirmed) return;

  try {
    const result = await api("/dashboard/api/validation-failures/clear", {
      method: "POST",
      body: JSON.stringify({}),
    });
    failureReports.length = 0;
    renderFailuresList();
    showToast(result.message || "Validation failure history cleared.");
  } catch (e) {
    showToast("Clear failed: " + e.message, true);
  }
}

function recordHistory(uid, row) {
  if (!scoreHistory.has(uid)) scoreHistory.set(uid, []);
  const hist = scoreHistory.get(uid);
  hist.push({
    timestamp: row.timestamp || new Date().toISOString(),
    score: row.score,
    capped_score: row.capped_score,
    local_incentive: row.local_incentive,
    chain_incentive: row.chain_incentive,
    credibility: row.credibility,
    s3_boost: row.s3_boost,
    od_boost: row.od_boost,
    scorable_bytes: row.scorable_bytes,
    p2p_component: row.p2p_component,
    s3_component: row.s3_component,
    od_component: row.od_component,
  });
  while (hist.length > MAX_HISTORY) hist.shift();
}

function renderScoreRow(row) {
  const uid = row.uid;
  const prev = prevValues.get(uid) || {};
  const tr = document.createElement("tr");
  tr.dataset.uid = uid;
  tr.innerHTML = `
    <td>${uid}</td>
    <td title="${row.hotkey}">${row.hotkey.slice(0, 16)}…</td>
    <td class="metric-cell" data-metric="score">${row.score.toFixed(2)} ${formatDelta(prev.score ?? row.score, row.score)}</td>
    <td class="metric-cell" data-metric="capped">${row.capped_score.toFixed(2)} ${formatDelta(prev.capped_score ?? row.capped_score, row.capped_score)}</td>
    <td class="metric-cell" data-metric="local_incentive">${formatPct(row.local_incentive)} ${formatDelta((prev.local_incentive ?? row.local_incentive) * 100, row.local_incentive * 100, 2)}</td>
    <td class="metric-cell" data-metric="chain_incentive">${row.chain_incentive.toFixed(6)} ${formatDelta(prev.chain_incentive ?? row.chain_incentive, row.chain_incentive, 6)}</td>
    <td class="metric-cell" data-metric="credibility">${row.credibility.toFixed(3)} ${formatDelta(prev.credibility ?? row.credibility, row.credibility, 3)}</td>
    <td class="metric-cell" data-metric="s3_boost">${row.s3_boost.toFixed(0)} ${formatDelta(prev.s3_boost ?? row.s3_boost, row.s3_boost, 0)}</td>
    <td class="metric-cell" data-metric="od_boost">${row.od_boost.toFixed(0)} ${formatDelta(prev.od_boost ?? row.od_boost, row.od_boost, 0)}</td>
    <td class="metric-cell" data-metric="scorable_bytes">${(row.scorable_bytes / 1e6).toFixed(2)}M ${formatDelta((prev.scorable_bytes ?? row.scorable_bytes) / 1e6, row.scorable_bytes / 1e6, 2)}</td>
  `;
  prevValues.set(uid, { ...row });
  scoreState.set(uid, row);
  recordHistory(uid, row);
  return tr;
}

function updateScoresTable(rows) {
  const tbody = document.getElementById("scores-body");
  const sorted = [...rows].sort((a, b) => b.capped_score - a.capped_score);
  tbody.innerHTML = "";
  sorted.forEach((row) => tbody.appendChild(renderScoreRow(row)));
  updateChartUidOptions(sorted);
  updateFailureUidOptions(sorted);
  if (charts.timeline) refreshCharts();
  document.getElementById("last-update").textContent =
    `Last update: ${formatTime(new Date().toISOString())}`;
  const scoresUpdated = document.getElementById("scores-updated");
  if (scoresUpdated) {
    scoresUpdated.textContent = `Last update: ${formatTime(new Date().toISOString())}`;
  }
  if (document.getElementById("tab-charts")?.classList.contains("active")) {
    requestAnimationFrame(() => resizeCharts());
  }
}

function applyScoreSnapshot(snapshot) {
  if (!snapshot || snapshot.uid === undefined) return;
  const rows = [...scoreState.values()];
  const idx = rows.findIndex((r) => r.uid === snapshot.uid);
  if (idx >= 0) rows[idx] = { ...rows[idx], ...snapshot };
  else rows.push(snapshot);
  updateScoresTable(rows);
}

function feedEventSummary(event) {
  let summary = event.data?.message || event.data?.reason || "";
  if (event.data?.detail) summary += ` | ${event.data.detail}`;
  if (!summary && event.event_type === "score_updated") {
    summary = `score=${(event.data.score || 0).toFixed(2)} incentive=${formatPct(event.data.local_incentive || 0)}`;
  }
  if (event.event_type === "od_job_created") {
    summary = event.data?.message || `job ${(event.data?.job_id || "").slice(0, 8)}… labels=${event.data?.labels || ""}`;
  }
  if (!summary) summary = JSON.stringify(event.data || {}).slice(0, 120);
  return summary;
}

function feedEventSeverity(event) {
  const type = event.event_type || "";
  if (type.includes("fail") || type === "validation_failure") return "error";
  if (isFeedWarningOrError(event)) return "warning";
  return "";
}

function isFeedWarningOrError(event) {
  const type = event.event_type || "";
  const data = event.data || {};
  const text = [
    data.message,
    data.reason,
    data.detail,
    data.status,
    data.phase,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  if (type.includes("fail") || type === "validation_failure") return true;
  if (type === "eval_failed") return true;
  if (
    type === "eval_od_complete" &&
    ((data.validated_fail || 0) > 0 || (data.failures && data.failures.length) || (data.jobs_failed || 0) > 0)
  ) {
    return true;
  }
  if (type === "eval_p2p_complete" && !data.skipped) {
    if ((data.jobs_failed || 0) > 0) return true;
    if (data.passed !== undefined && data.total !== undefined && data.passed < data.total) {
      return true;
    }
    if (data.status === "failed" || data.status === "partial") return true;
  }
  if (type === "eval_s3_complete" && data.is_valid === false) return true;
  if (type === "od_jobs_batch_created" && (data.failed_count || 0) > 0) return true;
  if (data.severity === "warning" || data.severity === "error") return true;
  if (
    text.includes("warning") ||
    text.includes("error") ||
    text.includes("failed") ||
    text.includes("invalid") ||
    text.includes("mismatch")
  ) {
    return true;
  }
  return false;
}

function createFeedEventElement(event) {
  const div = document.createElement("div");
  div.className = "event-item";
  const severity = feedEventSeverity(event);
  if (severity) div.classList.add(`feed-${severity}`);
  div.innerHTML = `
    <span class="event-time">${formatTime(event.timestamp)}</span>
    <span class="event-type ${eventClass(event.event_type)}">${escapeHtml(event.event_type)}</span>
    <span>UID ${event.uid}</span>
    <span>${escapeHtml(feedEventSummary(event))}</span>
  `;
  return div;
}

function updateFeedCount() {
  const countEl = document.getElementById("feed-count");
  if (!countEl) return;
  const visible = feedIssuesOnly
    ? feedEvents.filter(isFeedWarningOrError).length
    : feedEvents.length;
  countEl.textContent = feedIssuesOnly
    ? `${visible} issue(s) / ${feedEvents.length} total`
    : `${visible} event(s)`;
}

function renderFeed() {
  const feed = document.getElementById("event-feed");
  if (!feed) return;
  const items = feedIssuesOnly
    ? feedEvents.filter(isFeedWarningOrError)
    : feedEvents;
  feed.innerHTML = "";
  if (!items.length) {
    feed.innerHTML = `<p class="muted-text feed-empty">${
      feedIssuesOnly
        ? "No errors or warnings in the feed."
        : "No events yet."
    }</p>`;
  } else {
    items.forEach((event) => feed.appendChild(createFeedEventElement(event)));
  }
  updateFeedCount();
}

function applyFeedEventSideEffects(event) {
  if (event.event_type === "validation_failure" && event.data) {
    upsertFailureReport(event.data);
    updateFailuresTabBadge(failureReports.length);
  }

  if (event.event_type === "scores_reset") {
    handleScoresReset(event.data, { silent: true });
  }

  if (event.event_type === "evaluation_state" && event.data) {
    if (event.data.evaluation_paused !== undefined) {
      evaluationPaused = !!event.data.evaluation_paused;
      const pausedEl = document.getElementById("eval-paused");
      if (pausedEl) pausedEl.checked = evaluationPaused;
      updateEvalControlsUI();
    }
  }
}

function ingestFeedEvent(event) {
  if (!event) return;
  feedEvents.unshift(event);
  while (feedEvents.length > MAX_FEED_EVENTS) feedEvents.pop();
  applyFeedEventSideEffects(event);
  renderFeed();
}

function renderEvent(event) {
  ingestFeedEvent(event);
}

async function clearFeedHistory() {
  const confirmed = window.confirm(
    "Delete all Live Evaluation Feed history?\n\n" +
      "This clears buffered feed events on the validator. New events will still appear as evaluation runs."
  );
  if (!confirmed) return;

  try {
    const result = await api("/dashboard/api/events/clear", {
      method: "POST",
      body: JSON.stringify({}),
    });
    feedEvents.length = 0;
    renderFeed();
    showToast(result.message || "Evaluation feed cleared.");
  } catch (e) {
    showToast("Clear feed failed: " + e.message, true);
  }
}

function initFeedFilter() {
  const select = document.getElementById("feed-filter");
  if (!select) return;
  const saved = localStorage.getItem(FEED_FILTER_KEY);
  if (saved === "issues") {
    feedIssuesOnly = true;
    select.value = "issues";
  }
  select.addEventListener("change", () => {
    feedIssuesOnly = select.value === "issues";
    localStorage.setItem(FEED_FILTER_KEY, feedIssuesOnly ? "issues" : "all");
    renderFeed();
  });
}

function connectSSE() {
  if (eventSource) eventSource.close();
  eventSource = new EventSource(`${API}/dashboard/api/events/stream`);
  eventSource.onmessage = (e) => {
    try {
      renderEvent(JSON.parse(e.data));
    } catch (err) {
      console.error("SSE parse error:", err, e.data);
    }
  };
  eventSource.onerror = () => {
    setTimeout(connectSSE, 5000);
  };
}

function updateIncentiveNote() {
  const el = document.getElementById("incentive-note");
  if (!el) return;
  if (disableSetWeights) {
    el.innerHTML =
      "<strong>Local Incentive</strong> = this validator's normalized weight share (from capped scores). " +
      "<strong>Chain Incentive</strong> = on-chain <code>metagraph.I</code> (won't reflect local eval on testnet when set_weights is disabled).";
  } else {
    el.innerHTML =
      "<strong>Local Incentive</strong> = expected weight share from this validator's scores. " +
      "<strong>Chain Incentive</strong> = Yuma consensus incentive from <code>metagraph.I</code>.";
  }
}

async function loadStatus() {
  try {
    const data = await api("/dashboard/api/status");
    const badge = document.getElementById("health-badge");
    badge.className = `status-badge ${data.healthy ? "healthy" : "unhealthy"}`;
    badge.innerHTML = `<span class="dot"></span>${data.healthy ? "Healthy" : "Unhealthy"}`;

    if (data.settings?.evaluation_paused !== undefined) {
      evaluationPaused = !!data.settings.evaluation_paused;
      const pausedEl = document.getElementById("eval-paused");
      if (pausedEl) pausedEl.checked = evaluationPaused;
      updateEvalControlsUI();
    }

    const evalLabel = evaluationPaused ? "Paused" : "Running";
    document.getElementById("meta-info").innerHTML = `
      <span>NetUID: <strong>${data.netuid}</strong></span>
      <span>Block: <strong>${data.block}</strong></span>
      <span>Validator UID: <strong>${data.validator_uid}</strong></span>
      <span>Eval cycles: <strong>${data.evaluation_cycles}</strong></span>
      <span>Evaluation: <strong>${evalLabel}</strong></span>
    `;
  } catch (e) {
    console.error("Status load failed:", e);
  }
}

function updateEvalControlsUI() {
  const badge = document.getElementById("eval-status-badge");
  const startBtn = document.getElementById("eval-start-btn");
  const stopBtn = document.getElementById("eval-stop-btn");
  const onceBtn = document.getElementById("eval-once-btn");
  if (!badge) return;

  if (evaluationPaused) {
    badge.textContent = "Paused";
    badge.className = "eval-status-badge paused";
    if (startBtn) startBtn.disabled = false;
    if (stopBtn) stopBtn.disabled = true;
    if (onceBtn) onceBtn.disabled = false;
  } else {
    badge.textContent = "Running";
    badge.className = "eval-status-badge running";
    if (startBtn) startBtn.disabled = true;
    if (stopBtn) stopBtn.disabled = false;
    if (onceBtn) onceBtn.disabled = false;
  }
}

function applyScoreRows(rows) {
  prevValues.clear();
  scoreState.clear();
  (rows || []).forEach((row) => {
    prevValues.set(row.uid, { ...row });
    scoreState.set(row.uid, row);
    recordHistory(row.uid, row);
  });
  updateScoresTable(rows || []);
}

function destroyCharts() {
  Object.values(charts).forEach((chart) => {
    if (chart && typeof chart.destroy === "function") chart.destroy();
  });
  charts = {};
}

function applyValidationStatsReset(data) {
  if (data?.validation_stats) {
    validationStatsState.miners = data.validation_stats.miners || [];
    validationStatsState.session =
      data.validation_stats.session || emptyValidationStatsSession();
  } else {
    validationStatsState.miners = [];
    validationStatsState.session = emptyValidationStatsSession();
  }
  renderValidationStats();
}

function handleScoresReset(data, options = {}) {
  scoreState.clear();
  scoreHistory.clear();
  prevValues.clear();
  failureReports.length = 0;
  applyValidationStatsReset(data);
  destroyCharts();
  if (data?.scores?.length) {
    applyScoreRows(data.scores);
  } else {
    loadScores();
  }
  loadValidationFailures();
  if (!data?.validation_stats) {
    loadValidationStats();
  }
  renderFailuresList();
  ensureCharts();
  refreshCharts();
  if (!options.silent && data?.message) showToast(data.message);
}

function setFeedCollapsed(collapsed) {
  const body = document.getElementById("feed-collapsible");
  const btn = document.getElementById("feed-toggle");
  const icon = document.getElementById("feed-toggle-icon");
  const label = document.getElementById("feed-toggle-label");
  if (!body || !btn) return;

  body.classList.toggle("collapsed", collapsed);
  btn.setAttribute("aria-expanded", String(!collapsed));
  if (icon) icon.textContent = collapsed ? "▸" : "▾";
  if (label) {
    label.textContent = collapsed
      ? "Show Live Evaluation Feed"
      : "Hide Live Evaluation Feed";
  }
  try {
    localStorage.setItem(FEED_COLLAPSED_KEY, collapsed ? "1" : "0");
  } catch (_) {}
}

function initFeedToggle() {
  const btn = document.getElementById("feed-toggle");
  if (!btn) return;
  let collapsed = true;
  try {
    const saved = localStorage.getItem(FEED_COLLAPSED_KEY);
    if (saved === "0") collapsed = false;
  } catch (_) {}
  setFeedCollapsed(collapsed);
  btn.addEventListener("click", () => {
    const body = document.getElementById("feed-collapsible");
    setFeedCollapsed(!body?.classList.contains("collapsed"));
  });
}

async function startEvaluation() {
  try {
    const result = await api("/dashboard/api/evaluate/resume", { method: "POST" });
    evaluationPaused = false;
    const pausedEl = document.getElementById("eval-paused");
    if (pausedEl) pausedEl.checked = false;
    updateEvalControlsUI();
    loadStatus();
    showToast(result.message || "Evaluation started");
  } catch (e) {
    showToast("Start failed: " + e.message, true);
  }
}

async function stopEvaluation() {
  try {
    const result = await api("/dashboard/api/evaluate/pause", { method: "POST" });
    evaluationPaused = true;
    const pausedEl = document.getElementById("eval-paused");
    if (pausedEl) pausedEl.checked = true;
    updateEvalControlsUI();
    loadStatus();
    showToast("Evaluation stopped");
  } catch (e) {
    showToast("Stop failed: " + e.message, true);
  }
}

async function loadMiners() {
  try {
    const data = await api("/dashboard/api/miners");
    const select = document.getElementById("target-miner");
    const current = [...select.selectedOptions].map((o) => parseInt(o.value, 10));
    select.innerHTML = "";
    data.miners.forEach((m) => {
      const opt = document.createElement("option");
      opt.value = m.uid;
      opt.textContent = `UID ${m.uid} — ${m.hotkey.slice(0, 12)}…`;
      if (current.includes(m.uid)) opt.selected = true;
      select.appendChild(opt);
    });
  } catch (e) {
    console.error("Miners load failed:", e);
  }
}

async function loadScores() {
  try {
    const data = await api("/dashboard/api/scores");
    disableSetWeights = !!data.disable_set_weights;
    updateIncentiveNote();
    updateScoresTable(data.scores || []);

    const histData = await api("/dashboard/api/scores/history?limit=60");
    if (histData.history) {
      if (histData.uid !== undefined) {
        scoreHistory.set(histData.uid, histData.history);
      } else {
        Object.entries(histData.history).forEach(([uid, points]) => {
          scoreHistory.set(parseInt(uid, 10), points);
        });
      }
      if (charts.timeline) refreshCharts();
    }
  } catch (e) {
    console.error("Scores load failed:", e);
  }
}

function updateChartUidOptions(rows) {
  const select = document.getElementById("chart-uid");
  if (!select) return;
  const current = select.value;
  select.innerHTML = "";
  rows.forEach((row) => {
    const opt = document.createElement("option");
    opt.value = row.uid;
    opt.textContent = `UID ${row.uid}`;
    select.appendChild(opt);
  });
  if (current && [...select.options].some((o) => o.value === current)) {
    select.value = current;
  }
}

function chartDefaults() {
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 400 },
    plugins: {
      legend: { labels: { color: "#8b92a5", boxWidth: 12 } },
    },
    scales: {
      x: { ticks: { color: "#8b92a5", maxRotation: 0 }, grid: { color: "rgba(42,47,61,0.5)" } },
      y: { ticks: { color: "#8b92a5" }, grid: { color: "rgba(42,47,61,0.5)" }, beginAtZero: true },
    },
  };
}

function formatBarTooltip(label, raw) {
  const v = Number(raw) || 0;
  if (label === "Credibility") return `Credibility: ${v.toFixed(4)}`;
  if (label === "Scorable MB") return `Scorable: ${v.toFixed(3)} MB`;
  if (label === "P2P" || label === "S3" || label === "OD") {
    return `${label}: ${v.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  }
  return `${label}: ${v.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
}

function barChartOptions() {
  return {
    ...chartDefaults(),
    plugins: {
      legend: { display: false },
      tooltip: {
        callbacks: {
          label: (ctx) => formatBarTooltip(ctx.label, ctx.raw),
        },
      },
    },
    scales: {
      x: { ticks: { color: "#8b92a5", maxRotation: 0 }, grid: { color: "rgba(42,47,61,0.5)" } },
      y: {
        type: "logarithmic",
        min: 0.01,
        ticks: {
          color: "#8b92a5",
          callback: (v) => (v >= 1000 ? v.toLocaleString() : v),
        },
        grid: { color: "rgba(42,47,61,0.5)" },
      },
    },
  };
}

function barValueForLog(raw) {
  const v = Number(raw) || 0;
  return Math.max(v, 0.01);
}

function ensureCharts() {
  if (charts.timeline || typeof Chart === "undefined") return;

  const timelineCtx = document.getElementById("chart-timeline");
  const componentsCtx = document.getElementById("chart-components");
  const boostsCtx = document.getElementById("chart-boosts");
  const pieCtx = document.getElementById("chart-incentive-pie");
  if (!timelineCtx || !componentsCtx || !boostsCtx || !pieCtx) return;

  charts.timeline = new Chart(timelineCtx, {
    type: "line",
    data: { labels: [], datasets: [] },
    options: chartDefaults(),
  });

  charts.components = new Chart(componentsCtx, {
    type: "bar",
    data: {
      labels: ["P2P", "S3", "OD"],
      datasets: [{
        data: [0.01, 0.01, 0.01],
        backgroundColor: [CHART_COLORS.p2p, CHART_COLORS.s3, CHART_COLORS.od],
      }],
    },
    options: barChartOptions(),
  });

  charts.boosts = new Chart(boostsCtx, {
    type: "bar",
    data: {
      labels: ["Credibility", "S3 Boost", "OD Boost", "Scorable MB"],
      datasets: [{
        data: [0.01, 0.01, 0.01, 0.01],
        backgroundColor: [CHART_COLORS.credibility, CHART_COLORS.s3Boost, CHART_COLORS.odBoost, CHART_COLORS.score],
      }],
    },
    options: barChartOptions(),
  });

  charts.incentivePie = new Chart(pieCtx, {
    type: "doughnut",
    data: { labels: [], datasets: [{ data: [], backgroundColor: ["#4f8cff", "#34c759", "#ffd60a", "#bf5af2", "#ff453a", "#64d2ff"] }] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: "right", labels: { color: "#8b92a5" } } },
    },
  });
}

function resizeCharts() {
  Object.values(charts).forEach((chart) => {
    if (chart && typeof chart.resize === "function") chart.resize();
  });
}

function getSelectedChartUid() {
  const select = document.getElementById("chart-uid");
  let uid = parseInt(select?.value, 10);
  if (!Number.isFinite(uid)) {
    const rows = [...scoreState.values()];
    if (rows.length) uid = rows[0].uid;
  }
  return Number.isFinite(uid) ? uid : null;
}

function refreshCharts() {
  if (!charts.timeline) return;

  const uid = getSelectedChartUid();
  if (uid === null) return;

  const hist = scoreHistory.get(uid) || [];
  const labels = hist.map((p) => formatTime(p.timestamp));

  charts.timeline.data.labels = labels;
  charts.timeline.data.datasets = [
    { label: "Score", data: hist.map((p) => p.score), borderColor: CHART_COLORS.score, tension: 0.3 },
    { label: "Capped Score", data: hist.map((p) => p.capped_score), borderColor: CHART_COLORS.capped, tension: 0.3 },
    { label: "Local Incentive %", data: hist.map((p) => p.local_incentive * 100), borderColor: CHART_COLORS.localIncentive, tension: 0.3, yAxisID: "y1" },
    { label: "Chain Incentive", data: hist.map((p) => p.chain_incentive), borderColor: CHART_COLORS.chainIncentive, tension: 0.3 },
  ];
  charts.timeline.options.scales.y1 = {
    position: "right",
    ticks: { color: "#8b92a5", callback: (v) => v + "%" },
    grid: { drawOnChartArea: false },
    beginAtZero: true,
  };
  charts.timeline.update("none");

  const latest = hist[hist.length - 1] || scoreState.get(uid) || {};
  charts.components.data.datasets[0].data = [
    barValueForLog(latest.p2p_component || 0),
    barValueForLog(latest.s3_component || 0),
    barValueForLog(latest.od_component || 0),
  ];
  charts.components.update("none");

  charts.boosts.data.datasets[0].data = [
    barValueForLog(latest.credibility || 0),
    barValueForLog(latest.s3_boost || 0),
    barValueForLog(latest.od_boost || 0),
    barValueForLog((latest.scorable_bytes || 0) / 1e6),
  ];
  charts.boosts.update("none");

  const allRows = [...scoreState.values()].filter((r) => r.local_incentive > 0);
  const pieRows = allRows.length ? allRows : [...scoreState.values()];
  charts.incentivePie.data.labels = pieRows.map((r) => `UID ${r.uid}`);
  charts.incentivePie.data.datasets[0].data = pieRows.map((r) => r.local_incentive * 100);
  charts.incentivePie.update("none");
}

function onChartsTabShown() {
  ensureCharts();
  refreshCharts();
  requestAnimationFrame(() => resizeCharts());
}

async function loadSchedulerStatus() {
  try {
    const data = await api("/dashboard/api/od-jobs/scheduler");
    const el = document.getElementById("scheduler-status");
    if (!el) return;
    if (data.enabled) {
      const src = data.keyword_source === "file"
        ? `file (${data.label_count || 0} labels)`
        : "manual";
      el.textContent = `Scheduler ON — every ${data.interval_minutes} min, source: ${src}, created ${data.jobs_created} jobs` +
        (data.last_label_used ? `, last: ${data.last_label_used}` : "") +
        (data.last_run ? `, run: ${formatTime(data.last_run)}` : "") +
        (data.next_run ? `, next: ${formatTime(data.next_run)}` : "") +
        (data.last_error ? ` | ERROR: ${data.last_error}` : "");
      el.style.color = data.last_error ? "var(--danger)" : "var(--success)";
    } else {
      el.textContent = "Scheduler OFF — enable automatic OD jobs in this tab and Save OD Settings";
      el.style.color = "var(--muted)";
    }
  } catch (e) {
    console.error("Scheduler status failed:", e);
  }
}

function toggleKeywordSourceUI() {
  const source = document.getElementById("auto-od-keyword-source")?.value || "manual";
  const isFile = source === "file";
  const manualPanel = document.getElementById("auto-manual-source-panel");
  const filePanel = document.getElementById("auto-file-source-panel");
  if (manualPanel) manualPanel.style.display = isFile ? "none" : "block";
  if (filePanel) filePanel.style.display = isFile ? "block" : "none";
}

function formatJobPreviewLine(entry) {
  const parts = [`#${entry.source_index}`, entry.summary || `[${entry.platform}]`];
  if (entry.keyword_mode) parts.push(`mode=${entry.keyword_mode}`);
  if (entry.ttl_minutes) parts.push(`ttl=${entry.ttl_minutes}m`);
  if (entry.limit) parts.push(`limit=${entry.limit}`);
  if (entry.start_date) parts.push(`start=${entry.start_date}`);
  if (entry.end_date) parts.push(`end=${entry.end_date}`);
  return parts.join(" | ");
}

function collectFileLabelSettings() {
  return {
    auto_od_label_file_path: document.getElementById("od-label-file")?.value.trim() || "",
    label_file_platform_filter: document.getElementById("od-label-platform")?.value || "all",
  };
}

function isoToDatetimeLocal(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  const pad = (n) => String(n).padStart(2, "0");
  // Display stored UTC timestamps as UTC wall-clock in datetime-local fields.
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
}

function datetimeLocalToIso(value) {
  if (!value || !String(value).trim()) return null;
  const match = String(value).trim().match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/);
  if (!match) return null;
  const [, y, m, d, h, min] = match;
  const utc = new Date(Date.UTC(Number(y), Number(m) - 1, Number(d), Number(h), Number(min)));
  if (Number.isNaN(utc.getTime())) return null;
  return utc.toISOString();
}

function formatOdDateTimeRange(startIso, endIso) {
  if (!startIso && !endIso) return "";
  const fmt = (iso) => {
    if (!iso) return "—";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    return d.toISOString().replace("T", " ").replace(/\.\d{3}Z$/, " UTC");
  };
  return `${fmt(startIso)} → ${fmt(endIso)}`;
}

function readOdDateFields(startId, endId) {
  const start = datetimeLocalToIso(document.getElementById(startId)?.value);
  const end = datetimeLocalToIso(document.getElementById(endId)?.value);
  const payload = {};
  if (start) payload.start_date = start;
  if (end) payload.end_date = end;
  return payload;
}

function setOdDateFields(startId, endId, startIso, endIso) {
  const startEl = document.getElementById(startId);
  const endEl = document.getElementById(endId);
  if (startEl) startEl.value = isoToDatetimeLocal(startIso);
  if (endEl) endEl.value = isoToDatetimeLocal(endIso);
}

function toggleOdPlatformFields(platformSelectId, subredditWrapId, usernamesWrapId) {
  const platform = document.getElementById(platformSelectId)?.value;
  const isReddit = platform === "reddit";
  const subWrap = document.getElementById(subredditWrapId);
  const userWrap = document.getElementById(usernamesWrapId);
  if (subWrap) subWrap.classList.toggle("od-hidden", !isReddit);
  if (userWrap) userWrap.classList.toggle("od-hidden", isReddit);
}

function syncOdPlatformFields() {
  toggleOdPlatformFields(
    "manual-od-platform",
    "manual-od-subreddit-wrap",
    "manual-od-usernames-wrap"
  );
  toggleOdPlatformFields(
    "auto-od-platform",
    "auto-od-subreddit-wrap",
    "auto-od-usernames-wrap"
  );
}

function collectOdSettingsPayload() {
  const autoOdEnabled = document.getElementById("auto-od-enabled").checked;
  let intervalVal = parseInt(document.getElementById("auto-od-interval").value, 10);
  if (autoOdEnabled && (isNaN(intervalVal) || intervalVal < 1)) {
    intervalVal = 5;
    document.getElementById("auto-od-interval").value = 5;
  }
  if (isNaN(intervalVal) || intervalVal < 1) intervalVal = 5;

  const dateFields = readOdDateFields("auto-od-start-date", "auto-od-end-date");
  return {
    auto_od_enabled: autoOdEnabled,
    auto_od_interval_minutes: intervalVal,
    auto_od_platform: document.getElementById("auto-od-platform").value,
    auto_od_keyword_source: document.getElementById("auto-od-keyword-source").value,
    auto_od_label_rotation: document.getElementById("auto-od-label-rotation").value,
    ...collectFileLabelSettings(),
    auto_od_keywords: document.getElementById("auto-od-keywords").value
      .split(",")
      .map((k) => k.trim())
      .filter(Boolean),
    auto_od_subreddit: document.getElementById("auto-od-subreddit").value.trim(),
    auto_od_usernames: document.getElementById("auto-od-usernames").value
      .split(",")
      .map((u) => u.trim())
      .filter(Boolean),
    auto_od_keyword_mode: document.getElementById("auto-od-keyword-mode").value,
    auto_od_limit: parseInt(document.getElementById("auto-od-limit").value, 10),
    auto_od_ttl_minutes: parseInt(document.getElementById("auto-od-ttl").value, 10),
    auto_od_start_date: dateFields.start_date || "",
    auto_od_end_date: dateFields.end_date || "",
  };
}

async function loadSettings() {
  try {
    const data = await api("/dashboard/api/settings");
    const s = data.settings;
    document.getElementById("skip-s3").checked = s.skip_s3_validation;
    document.getElementById("skip-p2p").checked = !!s.skip_p2p_validation;
    document.getElementById("p2p-validation-mode").value =
      s.p2p_scraper_validation_mode || "sample";
    document.getElementById("s3-validation-mode").value =
      s.s3_validation_mode || "sample";
    document.getElementById("od-validation-mode").value =
      s.od_validation_mode || "sample";
    document.getElementById("s3-validation-interval").value =
      s.s3_validation_interval_minutes || 120;
    document.getElementById("relax-weight-caps").checked = s.relax_weight_caps !== false;
    evaluationPaused = !!s.evaluation_paused;
    document.getElementById("eval-paused").checked = evaluationPaused;
    updateEvalControlsUI();
    document.getElementById("eval-batch-size").value = s.eval_batch_size;
    const evalInterval = document.getElementById("target-eval-interval");
    if (evalInterval) evalInterval.value = s.target_eval_interval_seconds || 300;
    document.getElementById("local-api-url").value = s.local_api_url;
    const dataDirEl = document.getElementById("local-api-data-dir");
    if (dataDirEl) dataDirEl.value = s.local_api_data_dir || "";
    document.getElementById("auto-od-enabled").checked = !!s.auto_od_enabled;
    document.getElementById("auto-od-interval").value = s.auto_od_interval_minutes || 5;
    document.getElementById("auto-od-interval").disabled = !s.auto_od_enabled;
    document.getElementById("auto-od-platform").value = s.auto_od_platform;
    document.getElementById("auto-od-keyword-source").value = s.auto_od_keyword_source || "manual";
    document.getElementById("od-label-file").value = s.auto_od_label_file_path || "";
    document.getElementById("od-label-platform").value = s.label_file_platform_filter || "all";
    document.getElementById("auto-od-label-rotation").value = s.auto_od_label_rotation || "sequential";
    document.getElementById("auto-od-keywords").value = (s.auto_od_keywords || []).join(", ");
    document.getElementById("auto-od-keyword-mode").value = s.auto_od_keyword_mode || "any";
    document.getElementById("auto-od-limit").value = s.auto_od_limit;
    document.getElementById("auto-od-ttl").value = s.auto_od_ttl_minutes;
    document.getElementById("auto-od-subreddit").value = s.auto_od_subreddit || "";
    document.getElementById("auto-od-usernames").value = (s.auto_od_usernames || []).join(", ");
    setOdDateFields("auto-od-start-date", "auto-od-end-date", s.auto_od_start_date, s.auto_od_end_date);
    document.getElementById("manual-od-keyword-mode").value = s.auto_od_keyword_mode || "any";
    document.getElementById("manual-od-ttl").value = s.auto_od_ttl_minutes || 30;
    setOdDateFields("manual-od-start-date", "manual-od-end-date", s.auto_od_start_date, s.auto_od_end_date);
    toggleKeywordSourceUI();
    syncOdPlatformFields();
  } catch (e) {
    console.error("Settings load failed:", e);
  }
}

async function previewLabelFile() {
  const filePath = document.getElementById("od-label-file").value.trim();
  const platform = document.getElementById("od-label-platform").value;
  const previewEl = document.getElementById("od-label-preview");
  if (!filePath) {
    showToast("Enter a job file path first", true);
    return;
  }
  try {
    const q = new URLSearchParams({ file_path: filePath, platform });
    const data = await api(`/dashboard/api/label-file/preview?${q}`);
    const lines = (data.preview || []).map(formatJobPreviewLine);
    const filterLabel = platform === "all" ? "all platforms" : platform;
    const truncated = data.total > lines.length ? `\n… and ${data.total - lines.length} more` : "";
    if (previewEl) {
      previewEl.textContent =
        `File: ${data.file_path}\n` +
        `Format: od_jobs | Platform filter: ${filterLabel} | Total jobs: ${data.total}\n` +
        lines.join("\n") +
        truncated;
    }
    showToast(`Loaded ${data.total} job(s)`);
  } catch (e) {
    if (previewEl) previewEl.textContent = e.message;
    showToast("Preview failed: " + e.message, true);
  }
}

async function uploadLabelFile(file) {
  if (!file) return;
  const form = new FormData();
  form.append("file", file);
  const resp = await fetch("/dashboard/api/label-file/upload", { method: "POST", body: form });
  if (!resp.ok) throw new Error(await resp.text());
  const data = await resp.json();
  document.getElementById("od-label-file").value = data.file_path;
  showToast(`Uploaded: ${data.filename}`);
  await previewLabelFile();
}

async function useOdExampleFile() {
  try {
    const data = await api("/dashboard/api/label-file/defaults");
    const match = (data.files || []).find((f) => f.name === "example_od_jobs.json");
    if (!match) {
      showToast("example_od_jobs.json not found", true);
      return;
    }
    document.getElementById("od-label-file").value = match.path;
    document.getElementById("od-label-platform").value = "all";
    await previewLabelFile();
    showToast(`Loaded ${match.name}`);
  } catch (e) {
    showToast("Failed: " + e.message, true);
  }
}

async function saveOdSettings() {
  try {
    await api("/dashboard/api/settings", {
      method: "PUT",
      body: JSON.stringify(collectOdSettingsPayload()),
    });
    showToast("OD settings saved");
    loadSchedulerStatus();
    loadOdJobs();
  } catch (e) {
    showToast("Save failed: " + e.message, true);
  }
}

async function createOdJobsFromFile() {
  const filePath = document.getElementById("od-label-file").value.trim();
  if (!filePath) {
    showToast("Enter a job file path first", true);
    return;
  }

  const startIndex = parseInt(document.getElementById("file-od-start-index").value, 10) || 0;
  const count = parseInt(document.getElementById("file-od-create-count").value, 10) || 1;

  const confirmed = window.confirm(
    `Create up to ${count} OD job(s) from file?\n\n` +
      `File: ${filePath}\n` +
      `Start index: ${startIndex}\n` +
      `Platform filter: ${document.getElementById("od-label-platform").value}`
  );
  if (!confirmed) return;

  try {
    const result = await api("/dashboard/api/od-jobs/create-from-file", {
      method: "POST",
      body: JSON.stringify({
        file_path: filePath,
        platform: document.getElementById("od-label-platform").value,
        start_index: startIndex,
        count,
      }),
    });
    const msg =
      `Created ${result.created_count}/${result.requested_count} job(s)` +
      (result.failed_count ? ` (${result.failed_count} failed)` : "") +
      ` — ${result.total_in_file} jobs in file`;
    showToast(msg, result.created_count === 0);
    await api("/dashboard/api/settings", {
      method: "PUT",
      body: JSON.stringify(collectFileLabelSettings()),
    });
    loadOdJobs();
  } catch (e) {
    showToast("Batch create failed: " + e.message, true);
  }
}

async function saveSettings() {
  const select = document.getElementById("target-miner");
  const targetUids = [...select.selectedOptions].map((o) => parseInt(o.value, 10));

  const payload = {
    target_miner_uids: targetUids,
    skip_s3_validation: document.getElementById("skip-s3").checked,
    skip_p2p_validation: document.getElementById("skip-p2p").checked,
    p2p_scraper_validation_mode: document.getElementById("p2p-validation-mode").value,
    s3_validation_mode: document.getElementById("s3-validation-mode").value,
    od_validation_mode: document.getElementById("od-validation-mode").value,
    s3_validation_interval_minutes: parseInt(
      document.getElementById("s3-validation-interval").value,
      10
    ),
    relax_weight_caps: document.getElementById("relax-weight-caps").checked,
    evaluation_paused: document.getElementById("eval-paused").checked,
    eval_batch_size: parseInt(document.getElementById("eval-batch-size").value, 10),
    target_eval_interval_seconds: parseInt(
      document.getElementById("target-eval-interval")?.value || "300",
      10
    ),
    local_api_url: document.getElementById("local-api-url").value,
  };

  try {
    await api("/dashboard/api/settings", { method: "PUT", body: JSON.stringify(payload) });
    evaluationPaused = payload.evaluation_paused;
    updateEvalControlsUI();
    showToast("Settings saved");
    loadStatus();
    loadScores();
  } catch (e) {
    showToast("Save failed: " + e.message, true);
  }
}

async function triggerEval() {
  try {
    await api("/dashboard/api/evaluate/trigger", { method: "POST" });
    showToast("Evaluation triggered");
  } catch (e) {
    showToast("Trigger failed: " + e.message, true);
  }
}

async function resetAllScores() {
  const confirmed = window.confirm(
    "Reset ALL local miner scores?\n\n" +
      "This clears score, boosts, credibility, chart history, and validation failure reports. " +
      "On-chain metagraph.I is NOT affected.\n\n" +
      "Evaluation will continue if it is currently running."
  );
  if (!confirmed) return;

  try {
    const result = await api("/dashboard/api/scores/reset", {
      method: "POST",
      body: JSON.stringify({
        uids: null,
        clear_history: true,
        clear_validation_reports: true,
      }),
    });
    handleScoresReset(
      { scores: result.scores, validation_stats: result.validation_stats },
      { silent: true }
    );
    if (!evaluationPaused) {
      await api("/dashboard/api/evaluate/trigger", { method: "POST" });
      showToast(
        `${result.message || "Scores reset"} — evaluation triggered`
      );
    } else {
      showToast(
        `${result.message || "Scores reset"} — press Start to resume evaluation`
      );
    }
  } catch (e) {
    showToast("Reset failed: " + e.message, true);
  }
}

async function createOdJob() {
  const platform = document.getElementById("manual-od-platform").value;
  const keywords = document.getElementById("manual-od-keywords").value
    .split(",")
    .map((k) => k.trim())
    .filter(Boolean);
  const limit = parseInt(document.getElementById("manual-od-limit").value, 10);
  const ttl = parseInt(document.getElementById("manual-od-ttl").value, 10) || 30;
  const keywordMode = document.getElementById("manual-od-keyword-mode").value;
  const dateFields = readOdDateFields("manual-od-start-date", "manual-od-end-date");
  const payload = {
    platform,
    keywords,
    limit,
    ttl_minutes: ttl,
    keyword_mode: keywordMode,
    ...dateFields,
  };
  if (platform === "reddit") {
    payload.subreddit = document.getElementById("manual-od-subreddit").value.trim();
  } else {
    const usernames = document.getElementById("manual-od-usernames").value
      .split(",")
      .map((u) => u.trim())
      .filter(Boolean);
    if (usernames.length) payload.usernames = usernames;
  }

  try {
    const result = await api("/dashboard/api/od-jobs/create", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    showToast(`OD job created: ${result.id || "ok"}`);
    loadOdJobs();
    loadOdSubmissions();
    loadOdTemplates();
  } catch (e) {
    showToast("OD job failed: " + e.message, true);
  }
}

async function loadOdTemplates() {
  const list = document.getElementById("od-templates-list");
  if (!list) return;

  try {
    const data = await api("/dashboard/api/od-jobs/templates");
    const templates = data.templates || [];
    if (data.message && !templates.length) {
      list.innerHTML = `<p class="muted-text">${escapeHtml(data.message)}</p>`;
      return;
    }
    if (!templates.length) {
      list.innerHTML = "<p class=\"muted-text\">No saved templates yet. Create a manual OD job to save one.</p>";
      return;
    }

    list.innerHTML = "";
    templates.forEach((template) => {
      const req = template.request || {};
      const row = document.createElement("div");
      row.className = "od-job-row";
      const range = formatOdDateTimeRange(req.start_date, req.end_date);
      row.innerHTML = `
        <div class="od-job-info">
          <div class="od-job-title">${escapeHtml(template.name || `Template #${template.id}`)}</div>
          <div class="od-job-meta">
            ${escapeHtml(template.platform || req.platform || "?")}
            · limit=${req.limit ?? "?"}
            · ttl=${req.ttl_minutes ?? "?"}m
            ${range ? ` · ${escapeHtml(range)}` : ""}
          </div>
          <div class="od-job-meta muted-text">template #${template.id}</div>
        </div>
        <div class="btn-row">
          <button type="button" class="secondary od-template-fill">Fill form</button>
          <button type="button" class="secondary od-template-create">Create job</button>
          <button type="button" class="danger od-template-delete">Delete</button>
        </div>
      `;
      row.querySelector(".od-template-fill")?.addEventListener("click", () => {
        applyOdTemplateToForm(req);
        showToast("Template loaded into manual form");
      });
      row.querySelector(".od-template-create")?.addEventListener("click", async () => {
        try {
          const result = await api(
            `/dashboard/api/od-jobs/templates/${template.id}/create`,
            { method: "POST", body: JSON.stringify({}) }
          );
          showToast(`OD job created from template: ${result.id || "ok"}`);
          loadOdJobs();
          loadOdSubmissions();
        } catch (e) {
          showToast("Create from template failed: " + e.message, true);
        }
      });
      row.querySelector(".od-template-delete")?.addEventListener("click", async () => {
        if (!window.confirm(`Delete template #${template.id}?`)) return;
        try {
          await api(`/dashboard/api/od-jobs/templates/${template.id}`, { method: "DELETE" });
          showToast("Template deleted");
          loadOdTemplates();
        } catch (e) {
          showToast("Delete template failed: " + e.message, true);
        }
      });
      list.appendChild(row);
    });
  } catch (e) {
    list.innerHTML = `<p class="muted-text">Failed to load templates: ${escapeHtml(e.message)}</p>`;
  }
}

function applyOdTemplateToForm(req) {
  const platform = req.platform || "x";
  document.getElementById("manual-od-platform").value = platform;
  syncOdPlatformFields();
  document.getElementById("manual-od-keywords").value = (req.keywords || []).join(", ");
  document.getElementById("manual-od-limit").value = req.limit ?? 50;
  document.getElementById("manual-od-ttl").value = req.ttl_minutes ?? 30;
  document.getElementById("manual-od-keyword-mode").value = req.keyword_mode || "any";
  if (platform === "reddit") {
    document.getElementById("manual-od-subreddit").value = req.subreddit || "";
  } else {
    document.getElementById("manual-od-usernames").value = (req.usernames || []).join(", ");
  }
  setOdDateFields(
    "manual-od-start-date",
    "manual-od-end-date",
    req.start_date || "",
    req.end_date || ""
  );
}

async function loadOdJobs() {
  const list = document.getElementById("od-jobs-list");
  if (!list) return;

  try {
    const data = await api("/dashboard/api/od-jobs");
    const jobs = (data.jobs || []).slice().sort((a, b) => {
      const ta = a.created_at || "";
      const tb = b.created_at || "";
      return tb.localeCompare(ta);
    });

    if (!jobs.length) {
      list.innerHTML = "<p class=\"muted-text\">No OD jobs yet.</p>";
      return;
    }

    list.innerHTML = "";
    jobs.forEach((job) => {
      const row = document.createElement("div");
      row.className = "od-job-row";
      const platform = job.job?.platform || job.job?.job?.platform || "?";
      const mode = job.keyword_mode || "any";
      const range =
        job.start_date && job.end_date
          ? formatOdDateTimeRange(job.start_date, job.end_date)
          : "";
      const createdLabel = job.created_at
        ? formatOdDateTimeRange(job.created_at, null).split(" → ")[0]
        : "";
      const jobId = job.id || "";
      row.innerHTML = `
        <div class="od-job-info">
          <div class="od-job-title">${escapeHtml(jobId.slice(0, 8))}… [${escapeHtml(platform)}]</div>
          <div class="od-job-meta">
            mode=${escapeHtml(mode)} · limit=${job.limit ?? "?"}
            ${range ? ` · posts ${escapeHtml(range)}` : ""}
            ${createdLabel ? ` · created ${escapeHtml(createdLabel)}` : ""}
            · expires ${escapeHtml(formatTime(job.expire_at))}
          </div>
          <div class="od-job-meta muted-text">${escapeHtml(jobId)}</div>
        </div>
        <button type="button" class="danger od-job-delete-btn">Delete</button>
      `;
      row.querySelector(".od-job-delete-btn").addEventListener("click", () => {
        deleteOdJob(jobId);
      });
      list.appendChild(row);
    });
  } catch (e) {
    list.innerHTML = `<p class="muted-text">Failed to load jobs: ${escapeHtml(e.message)}</p>`;
  }
}

async function deleteOdJob(jobId) {
  const includeSubmissions =
    document.getElementById("clear-job-submissions")?.checked ?? true;
  const submissionNote = includeSubmissions
    ? "\n\nLinked miner submissions for this job will also be deleted."
    : "\n\nMiner submissions for this job will be kept.";
  const confirmed = window.confirm(
    `Delete OD job ${jobId}?${submissionNote}`
  );
  if (!confirmed) return;

  try {
    const result = await api(
      `/dashboard/api/od-jobs/${encodeURIComponent(jobId)}?include_submissions=${includeSubmissions}`,
      { method: "DELETE" }
    );
    showToast(result.message || `Deleted OD job ${jobId}.`);
    await loadOdJobs();
    if (includeSubmissions) {
      await loadOdSubmissions();
      await checkLocalApi();
    }
  } catch (e) {
    showToast("Delete failed: " + e.message, true);
  }
}

async function clearOdJobs() {
  const includeSubmissions =
    document.getElementById("clear-job-submissions")?.checked ?? true;
  const submissionNote = includeSubmissions
    ? " and all miner submission files under on_demand/submissions/"
    : "";
  const confirmed = window.confirm(
    "Delete all local OD job definitions" +
      submissionNote +
      "?\n\nThis does not reset scores or validation reports."
  );
  if (!confirmed) return;

  try {
    const result = await api("/dashboard/api/od-jobs/clear", {
      method: "POST",
      body: JSON.stringify({ include_submissions: includeSubmissions }),
    });
    showToast(result.message || "OD jobs cleared.");
    await loadOdJobs();
    if (includeSubmissions) {
      await loadOdSubmissions();
      await checkLocalApi();
    }
  } catch (e) {
    showToast("Clear failed: " + e.message, true);
  }
}

function formatBytes(bytes) {
  const n = Number(bytes) || 0;
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MB`;
}

async function loadOdSubmissions() {
  const list = document.getElementById("od-submissions-list");
  const dirEl = document.getElementById("od-submissions-dir");
  const statsEl = document.getElementById("od-submissions-stats");
  const preview = document.getElementById("od-submission-preview");
  if (!list) return;

  try {
    const data = await api("/dashboard/api/od-submissions?limit=30");
    if (dirEl) {
      dirEl.textContent = data.submissions_root
        ? `Storage: ${data.submissions_root}`
        : "Storage path not configured";
    }
    if (statsEl && data.stats) {
      const s = data.stats;
      statsEl.textContent = [
        `${s.od_submissions ?? 0} OD submission(s)`,
        `${s.parquet_files ?? 0} parquet file(s)`,
        formatBytes(s.total_bytes ?? 0),
      ].join(" · ");
    }
    const dataDirInput = document.getElementById("local-api-data-dir");
    if (dataDirInput && data.data_dir) dataDirInput.value = data.data_dir;

    const submissions = data.submissions || [];
    if (!submissions.length) {
      list.innerHTML = "<p class=\"muted-text\">No submissions yet.</p>";
      if (preview) {
        preview.classList.add("od-hidden");
        preview.textContent = "";
      }
      return;
    }

    list.innerHTML = "";
    submissions.forEach((sub) => {
      const row = document.createElement("div");
      row.className = "od-submission-row";
      const keywords = (sub.job_keywords || []).join(", ");
      const users = (sub.job_usernames || []).join(", ");
      const jobHint = [sub.job_platform, keywords, users].filter(Boolean).join(" · ");
      const parseBadge = sub.parse_ok
        ? `<span class="od-submission-badge ok">JSON OK</span>`
        : `<span class="od-submission-badge bad">JSON error</span>`;
      row.innerHTML = `
        <div class="od-submission-main">
          <strong>${escapeHtml(sub.job_id.slice(0, 8))}…</strong>
          ${parseBadge}
          <span class="muted-text">${escapeHtml(sub.miner_hotkey.slice(0, 14))}…</span>
        </div>
        <div class="od-submission-meta muted-text">
          ${escapeHtml(formatTime(sub.modified_at))}
          · ${sub.entity_count} entities
          · ${Math.round((sub.size_bytes || 0) / 1024)} KB
          ${jobHint ? `· ${escapeHtml(jobHint)}` : ""}
        </div>
        <div class="od-submission-path muted-text">${escapeHtml(sub.relative_path)}</div>
        <button type="button" class="secondary od-submission-view-btn">View JSON</button>
      `;
      row.querySelector(".od-submission-view-btn").addEventListener("click", () => {
        previewOdSubmission(sub.job_id, sub.miner_hotkey);
      });
      list.appendChild(row);
    });
  } catch (e) {
    list.innerHTML = `<p class="muted-text">Failed to load submissions: ${escapeHtml(e.message)}</p>`;
  }
}

async function clearMinerSubmissions() {
  const includeOd = document.getElementById("clear-od-submissions")?.checked ?? true;
  const includeParquet = document.getElementById("clear-parquet-uploads")?.checked ?? true;
  if (!includeOd && !includeParquet) {
    showToast("Select at least one category to clear.", true);
    return;
  }

  const parts = [];
  if (includeOd) parts.push("OD submission JSON files");
  if (includeParquet) parts.push("parquet uploads (data/)");
  const confirmed = window.confirm(
    "Delete all miner upload history on disk?\n\n" +
      `This will remove: ${parts.join(" and ")}.\n\n` +
      "OD job definitions are kept. Local scores and validation reports are NOT reset."
  );
  if (!confirmed) return;

  try {
    const result = await api("/dashboard/api/od-submissions/clear", {
      method: "POST",
      body: JSON.stringify({
        include_od_submissions: includeOd,
        include_parquet: includeParquet,
      }),
    });
    showToast(result.message || "Miner submission history cleared.");
    await loadOdSubmissions();
    await checkLocalApi();
  } catch (e) {
    showToast("Clear failed: " + e.message, true);
  }
}

async function previewOdSubmission(jobId, minerHotkey) {
  const preview = document.getElementById("od-submission-preview");
  if (!preview) return;
  try {
    const data = await api(
      `/dashboard/api/od-submissions/${encodeURIComponent(jobId)}/${encodeURIComponent(minerHotkey)}`
    );
    preview.classList.remove("od-hidden");
    preview.textContent = JSON.stringify(data.submission, null, 2);
    preview.scrollIntoView({ behavior: "smooth", block: "nearest" });
  } catch (e) {
    showToast("Preview failed: " + e.message, true);
  }
}

async function checkLocalApi() {
  try {
    const data = await api("/dashboard/api/local-api/health");
    const el = document.getElementById("local-api-status");
    const storageBits = [];
    if (data.storage?.parquet_files != null) {
      storageBits.push(`${data.storage.parquet_files} parquet`);
    }
    if (data.storage?.od_submissions != null) {
      storageBits.push(`${data.storage.od_submissions} OD submissions`);
    }
    const storageText = storageBits.length ? ` — ${storageBits.join(", ")}` : "";
    el.textContent = data.status === "healthy"
      ? `Local API OK${storageText}`
      : `Local API: ${data.status || data.error || "unknown"}`;
    const dataDirInput = document.getElementById("local-api-data-dir");
    if (dataDirInput && data.storage?.data_dir) {
      dataDirInput.value = data.storage.data_dir;
    }
  } catch (e) {
    document.getElementById("local-api-status").textContent = "Local API unreachable";
  }
}

function showToast(msg, isError = false) {
  const toast = document.getElementById("toast");
  toast.textContent = msg;
  toast.style.background = isError ? "var(--danger)" : "var(--success)";
  toast.style.display = "block";
  setTimeout(() => { toast.style.display = "none"; }, 3000);
}

function initTabs() {
  document.querySelectorAll(".tab").forEach((tab) => {
    tab.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
      document.querySelectorAll(".tab-content").forEach((c) => c.classList.remove("active"));
      tab.classList.add("active");
      document.getElementById(tab.dataset.tab).classList.add("active");
      if (tab.dataset.tab === "tab-charts") onChartsTabShown();
      if (tab.dataset.tab === "tab-failures") loadValidationFailures();
      if (tab.dataset.tab === "tab-stats") loadValidationStats();
      if (tab.dataset.tab === "tab-od") {
        loadOdJobs();
        loadOdSubmissions();
        loadSchedulerStatus();
      }
    });
  });
}

document.addEventListener("DOMContentLoaded", () => {
  initTabs();
  initFeedToggle();
  initFeedFilter();
  updateEvalControlsUI();
  connectSSE();
  loadStatus();
  loadMiners();
  loadScores();
  loadSettings();
  loadOdJobs();
  loadOdTemplates();
  loadOdSubmissions();
  checkLocalApi();
  loadSchedulerStatus();
  loadValidationFailures();
  loadValidationStats();

  document.getElementById("refresh-stats")?.addEventListener("click", loadValidationStats);
  document.getElementById("refresh-scores")?.addEventListener("click", loadScores);
  document.getElementById("chart-uid")?.addEventListener("change", () => {
    refreshCharts();
    requestAnimationFrame(() => resizeCharts());
  });

  document.getElementById("auto-od-enabled").addEventListener("change", (e) => {
    const enabled = e.target.checked;
    document.getElementById("auto-od-interval").disabled = !enabled;
    if (enabled && parseInt(document.getElementById("auto-od-interval").value, 10) < 1) {
      document.getElementById("auto-od-interval").value = 5;
    }
  });

  document.getElementById("auto-od-keyword-source")?.addEventListener("change", toggleKeywordSourceUI);
  document.getElementById("manual-od-platform")?.addEventListener("change", syncOdPlatformFields);
  document.getElementById("auto-od-platform")?.addEventListener("change", syncOdPlatformFields);
  document.getElementById("preview-od-labels")?.addEventListener("click", previewLabelFile);
  document.getElementById("use-od-example-file")?.addEventListener("click", useOdExampleFile);
  document.getElementById("od-label-upload")?.addEventListener("change", (e) => {
    const file = e.target.files && e.target.files[0];
    if (file) uploadLabelFile(file).catch((err) => showToast(err.message, true));
  });
  document.getElementById("save-od-settings")?.addEventListener("click", saveOdSettings);
  document.getElementById("create-od-from-file")?.addEventListener("click", createOdJobsFromFile);

  document.getElementById("save-settings").addEventListener("click", saveSettings);
  document.getElementById("trigger-eval").addEventListener("click", triggerEval);
  document.getElementById("eval-start-btn")?.addEventListener("click", startEvaluation);
  document.getElementById("eval-stop-btn")?.addEventListener("click", stopEvaluation);
  document.getElementById("eval-once-btn")?.addEventListener("click", triggerEval);
  document.getElementById("reset-scores")?.addEventListener("click", resetAllScores);
  document.getElementById("create-od-job").addEventListener("click", createOdJob);
  document.getElementById("refresh-od-templates")?.addEventListener("click", loadOdTemplates);
  document.getElementById("refresh-od-jobs")?.addEventListener("click", loadOdJobs);
  document.getElementById("clear-od-jobs")?.addEventListener("click", clearOdJobs);
  document.getElementById("refresh-od-submissions")?.addEventListener("click", loadOdSubmissions);
  document.getElementById("clear-miner-submissions")?.addEventListener("click", clearMinerSubmissions);
  document.getElementById("refresh-failures")?.addEventListener("click", () => loadValidationFailures(true));
  document.getElementById("load-more-failures")?.addEventListener("click", loadMoreValidationFailures);
  document.getElementById("clear-validation-failures")?.addEventListener("click", clearValidationFailures);
  document.getElementById("clear-feed")?.addEventListener("click", clearFeedHistory);
  document.getElementById("failure-type-filter")?.addEventListener("change", () => loadValidationFailures(true));
  document.getElementById("failure-uid-filter")?.addEventListener("change", () => loadValidationFailures(true));
});
